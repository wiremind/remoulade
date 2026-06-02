# This file is a part of Remoulade.
#
# Copyright (C) 2026 WIREMIND SAS <dev@wiremind.io>
#
# Remoulade is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Remoulade is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import json
import time
from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Event, Lock, Thread, local
from typing import TYPE_CHECKING, Any, Final, override

import psycopg
from pgmq import SQLAlchemyPGMQueue
from pgmq.messages import Message as PgmqQueueMessage
from psycopg import sql as psycopg_sql
from sqlalchemy import Column, Connection, Integer, MetaData, Table, func, select

from ..broker import Broker, Consumer, MessageProxy
from ..errors import QueueJoinTimeout, QueueNotFound, UnsupportedMessageEncoding
from ..message import Message

if TYPE_CHECKING:
    from ..middleware import Middleware

PgmqPayload = dict[str, Any]
LISTEN_NOTIFY_THROTTLE_MS: Final[int] = 250


class PgmqBroker(Broker):
    """A broker backed by PostgreSQL via the PGMQ extension."""

    def __init__(
        self,
        *,
        url: str,
        middleware: list["Middleware"] | None = None,
        group_transaction: bool = False,
        archive_partition_interval: str = "1 day",
        archive_retention_interval: str = "7 days",
        visibility_timeout_in_second: int = 30,
        heartbeat_interval_in_second: float = 10.0,
    ) -> None:
        """Initialize a PostgreSQL-backed broker using the PGMQ extension.

        Parameters:
          url(str): PostgreSQL URL in plain format (`postgresql://...`), used both by SQLAlchemy and psycopg.
          middleware(list[Middleware] | None): Middleware stack applied to this broker.
          group_transaction(bool): If True, wraps group and pipeline operations in a single transaction.
          archive_partition_interval(str): Partition interval passed to PGMQ when creating partitioned queues.
          archive_retention_interval(str): Retention interval passed to PGMQ for archive partitions.
          visibility_timeout_in_second(int): Message visibility timeout in seconds after read; must be greater than 0.
          heartbeat_interval_in_second(float): Heartbeat interval in seconds used to extend in-flight message visibility
            must be greater than 0 and lower than visibility_timeout_in_second
        """
        super().__init__(middleware=middleware)
        if visibility_timeout_in_second <= 0:
            raise ValueError("visibility_timeout_in_second must be greater than 0")
        if heartbeat_interval_in_second <= 0 or heartbeat_interval_in_second >= visibility_timeout_in_second:
            raise ValueError("heartbeat_interval must be greater than 0 and lower than visibility_timeout")

        self.url = url
        self.state = local()
        self.group_transaction = group_transaction
        self.archive_partition_interval = archive_partition_interval
        self.archive_retention_interval = archive_retention_interval
        self.visibility_timeout_in_second = visibility_timeout_in_second
        self.heartbeat_interval_in_second = heartbeat_interval_in_second
        self.supports_native_delay = True

        self.client = SQLAlchemyPGMQueue(conn_string=url, init_extension=False, vt=self.visibility_timeout_in_second)

    @override
    @contextmanager
    def tx(self) -> Iterator[None]:
        """Run broker operations inside a single SQL transaction.

        The active SQLAlchemy connection is stored on thread-local state so
        queue operations can reuse it while the context is open.
        """
        with self.client.engine.begin() as connection:
            self.state.transaction_connection = connection
            try:
                yield
            finally:
                self.state.transaction_connection = None

    @property
    def _current_connection(self) -> Connection | None:
        """Return the transactional connection, if one is active."""
        return getattr(self.state, "transaction_connection", None)

    def _try_enable_notify(self, queue_name: str) -> None:
        """Try to enable LISTEN/NOTIFY for a queue.

        Failures are logged and the consumer later falls back to polling.
        """
        try:
            self.client.enable_notify(
                queue_name,
                throttle_interval_ms=LISTEN_NOTIFY_THROTTLE_MS,
                conn=self._current_connection,
            )
        except Exception as e:
            self.logger.warning(
                "Failed to enable LISTEN/NOTIFY for queue %s; consumer will fall back to polling. Error: %s",
                queue_name,
                e,
                exc_info=True,
            )

    @override
    def close(self) -> None:
        """Dispose the underlying PGMQ client."""
        self.client.dispose()

    @override
    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 30000) -> "_PgmqConsumer":
        """Create a consumer for a declared queue.

        Raises:
          QueueNotFound: If the queue has not been declared.
        """
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)
        return _PgmqConsumer(self, queue_name=queue_name, prefetch=prefetch, timeout=timeout)

    @override
    def declare_queue(self, queue_name: str) -> None:
        """Create a partitioned PGMQ queue if it does not already exist."""
        if queue_name in self.queues:
            return

        self.client.validate_queue_name(queue_name, conn=self._current_connection)
        self.emit_before("declare_queue", queue_name)
        self.client.create_partitioned_queue(
            queue_name,
            partition_interval=self.archive_partition_interval,
            retention_interval=self.archive_retention_interval,
            conn=self._current_connection,
        )
        self._try_enable_notify(queue_name)
        self.queues[queue_name] = None
        self.emit_after("declare_queue", queue_name)

    def _encode_message(self, message: "Message") -> PgmqPayload:
        """Decode a Remoulade message into a JSON object payload for PGMQ.

        Raises:
          UnsupportedMessageEncoding: If the encoded payload is not valid JSON
            or is not a JSON object.
        """
        try:
            payload = json.loads(message.encode().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise UnsupportedMessageEncoding(
                "PgmqBroker only supports encoders that serialize messages as JSON objects."
            ) from e

        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PgmqBroker requires message encoders to produce JSON objects.")

        return payload

    @override
    def _enqueue(self, message: "Message", *, delay: int | None = None) -> "Message":
        """Send a message to PGMQ, optionally delayed by milliseconds."""
        if message.queue_name not in self.queues:
            raise QueueNotFound(message.queue_name)

        payload = self._encode_message(message)
        visible_at = datetime.now(UTC) + timedelta(milliseconds=delay) if delay is not None else None
        self.client.send(
            message.queue_name,
            payload,
            conn=self._current_connection,
            delay=visible_at,
        )

        return message

    @override
    def flush(self, queue_name: str) -> None:
        """Remove every message currently stored in a queue."""
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        self.client.purge(queue_name, conn=self._current_connection)

    @override
    def flush_all(self) -> None:
        """Purge every declared queue."""
        for queue_name in self.queues:
            self.flush(queue_name)

    def _count_enqueued_messages(self, queue_name: str) -> int:
        """Count rows in the underlying PGMQ queue table."""
        queue_table = Table(f"q_{queue_name}", MetaData(), Column("msg_id", Integer), schema="pgmq")
        count_query = select(func.count()).select_from(queue_table)

        with self.client.session() as session:
            return session.execute(count_query).scalar_one()

    @override
    def join(
        self,
        queue_name: str,
        min_successes: int = 10,
        idle_time: int = 100,
        *,
        timeout: int | None = None,
    ) -> None:
        """Wait for all the messages on the given queue to be processed.

        This method checks the full PGMQ queue table and therefore waits for
        all states: visible messages, invisible in-flight messages and native
        delayed messages.

        Parameters:
          queue_name(str): The queue to wait on.
          min_successes(int): The minimum number of times the queue should be
            observed as empty.
          idle_time(int): The number of milliseconds to wait between checks.
          timeout(Optional[int]): The max amount of time, in milliseconds, to
            wait on this queue.

        Raises:
          QueueNotFound: If the given queue was never declared.
          QueueJoinTimeout: When the timeout elapses.
        """
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        deadline = time.monotonic() + timeout / 1000 if timeout is not None else None
        successes = 0

        while successes < min_successes:
            if deadline and time.monotonic() >= deadline:
                raise QueueJoinTimeout(queue_name)

            total_messages = self._count_enqueued_messages(queue_name)
            if total_messages == 0:
                successes += 1
                if successes >= min_successes:
                    return
            else:
                successes = 0

            time.sleep(idle_time / 1000)


class _PgmqConsumer(Consumer):
    def __init__(self, broker: PgmqBroker, *, queue_name: str, prefetch: int, timeout: int) -> None:
        """Initialize a consumer for a PGMQ queue.

        Parameters:
          broker(PgmqBroker): Broker instance that owns the queue and database client.
          queue_name(str): Name of the declared queue to consume from.
          prefetch(int): Maximum number of messages fetched per read call; values lower than 1 are coerced to 1.
          timeout(int): Idle wait timeout in milliseconds when polling for messages; values lower than 0 are coerced to
          0. A value of 0 performs non-blocking reads.
        """
        self.broker = broker
        self.client = broker.client
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout = timeout
        self.visibility_timeout = broker.visibility_timeout_in_second
        self.heartbeat_interval = broker.heartbeat_interval_in_second
        self.messages: deque[PgmqQueueMessage] = deque()
        self._notify_event = Event()
        self._listener_stop = Event()
        self._listener_thread: Thread | None = None
        self._listener_connection: psycopg.Connection[Any] | None = None
        self._listener_available = False
        self._heartbeat_stop = Event()
        self._heartbeat_thread: Thread | None = None
        self._heartbeat_msg_ids_lock = Lock()
        self._heartbeat_msg_ids: set[int] = set()

        self._start_listener()
        self._start_heartbeat()

    @property
    def wait_timeout_seconds(self) -> float:
        """Return the listener wait timeout in seconds."""
        return self.timeout / 1000 if self.timeout > 0 else 0

    def _normalize_messages(self, messages: PgmqQueueMessage | list[PgmqQueueMessage] | None) -> list[PgmqQueueMessage]:
        """Normalize a PGMQ read result into a list."""
        if messages is None:
            return []
        return [messages] if not isinstance(messages, list) else messages

    def _read_immediate(self) -> list[PgmqQueueMessage]:
        """Read up to ``prefetch`` messages without polling."""
        return self._normalize_messages(
            self.client.read(
                self.queue_name,
                vt=self.visibility_timeout,
                qty=self.prefetch,
            )
        )

    def _read_with_poll(self) -> list[PgmqQueueMessage]:
        """Read up to ``prefetch`` messages using PGMQ polling."""
        return self._normalize_messages(
            self.client.read_with_poll(
                self.queue_name,
                vt=self.visibility_timeout,
                qty=self.prefetch,
                max_poll_seconds=max((self.timeout + 999) // 1000, 1),
                poll_interval_ms=max(min(self.timeout, 1000), 1),
            )
        )

    def _start_heartbeat(self) -> None:
        """Start the background visibility-timeout renewal thread."""
        self._heartbeat_thread = Thread(
            target=self._run_heartbeat,
            name=f"pgmq-heartbeat-{self.queue_name}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _run_heartbeat(self) -> None:
        """Periodically renew the lease of in-flight messages.

        The loop sleeps for ``heartbeat_interval`` seconds, unless shutdown is
        requested through ``_heartbeat_stop``. On each tick it snapshots the
        current set of tracked message ids under a lock, then calls
        ``set_vt`` to push their visibility timeout back to
        ``self.visibility_timeout``.

        This prevents long-running handlers from becoming visible again and
        being consumed twice before they are explicitly acked, nacked or
        requeued. Failures are logged and retried on the next heartbeat tick.
        """
        while not self._heartbeat_stop.wait(self.heartbeat_interval):
            with self._heartbeat_msg_ids_lock:
                msg_ids = list(self._heartbeat_msg_ids)
            if not msg_ids:
                continue
            try:
                self.client.set_vt(self.queue_name, msg_ids, self.visibility_timeout)
            except Exception as e:
                self.broker.logger.warning(
                    "Failed to extend visibility timeout heartbeat for queue %s. Error: ",
                    self.queue_name,
                    str(e),
                    exc_info=True,
                )

    def _register_heartbeat_msg(self, message: "_PgmqMessage") -> None:
        """Track a message so the heartbeat can renew it."""
        with self._heartbeat_msg_ids_lock:
            self._heartbeat_msg_ids.add(message._pgmq_message.msg_id)

    def _unregister_heartbeat_msg_id(self, msg_id: int) -> None:
        """Stop tracking a message for heartbeat renewal."""
        with self._heartbeat_msg_ids_lock:
            self._heartbeat_msg_ids.discard(msg_id)

    def _start_listener(self) -> None:
        """Start a LISTEN/NOTIFY listener for the queue when available."""
        channel = f"pgmq.q_{self.queue_name}.INSERT"
        try:
            self._listener_connection = psycopg.connect(self.broker.url, autocommit=True)
            self._listener_connection.execute(psycopg_sql.SQL("LISTEN {}").format(psycopg_sql.Identifier(channel)))
            self._listener_available = True
        except Exception as e:
            self._listener_available = False
            self._listener_connection = None
            self.broker.logger.warning(
                "Failed to start LISTEN/NOTIFY listener for queue %s; falling back to polling. Error: %s",
                self.queue_name,
                str(e),
                exc_info=True,
            )
            return

        self._listener_thread = Thread(
            target=self._run_listener,
            name=f"pgmq-listener-{self.queue_name}",
            daemon=True,
        )
        self._listener_thread.start()

    def _run_listener(self) -> None:
        """Wake the consumer when a notification arrives, or disable the listener on errors."""
        while not self._listener_stop.is_set():
            try:
                if self._listener_connection is None:
                    break
                for _ in self._listener_connection.notifies(timeout=0.5, stop_after=1):
                    self._notify_event.set()
            except Exception as e:
                if self._listener_stop.is_set():
                    break
                self._listener_available = False
                self._notify_event.set()
                self.broker.logger.warning(
                    "LISTEN/NOTIFY listener crashed for queue %s; falling back to polling. Error: %s",
                    self.queue_name,
                    str(e),
                    exc_info=True,
                )
                break

        self._listener_available = False

    @override
    def ack(self, message: "MessageProxy") -> None:
        """Archive a processed message."""
        if not isinstance(message, _PgmqMessage):
            raise ValueError("It must be a PgmqMessage")
        self._unregister_heartbeat_msg_id(message._pgmq_message.msg_id)
        self.client.archive(self.queue_name, message._pgmq_message.msg_id)

    @override
    def nack(self, message: "MessageProxy") -> None:
        """Archive a failed message."""
        if not isinstance(message, _PgmqMessage):
            raise ValueError("It must be a PgmqMessage")
        self._unregister_heartbeat_msg_id(message._pgmq_message.msg_id)
        self.client.archive(self.queue_name, message._pgmq_message.msg_id)

    @override
    def requeue(self, messages: Iterable["MessageProxy"]) -> None:
        """Make messages visible again immediately by resetting their visibility timeout."""
        msg_ids = [message._pgmq_message.msg_id for message in messages if isinstance(message, _PgmqMessage)]
        if msg_ids:
            for msg_id in msg_ids:
                self._unregister_heartbeat_msg_id(msg_id)
            self.client.set_vt(self.queue_name, msg_ids, 0)

    def _build_message(self, pgmq_message: PgmqQueueMessage) -> "_PgmqMessage":
        """Wrap a raw PGMQ row and register it for heartbeat renewal."""
        message = _PgmqMessage(pgmq_message)
        self._register_heartbeat_msg(message)
        return message

    @override
    def __next__(self) -> "_PgmqMessage | None":
        """Return the next available message, or ``None`` if the queue stays empty."""
        if self.messages:
            return self._build_message(self.messages.popleft())

        if self._listener_available:
            self._notify_event.wait(self.wait_timeout_seconds)
            messages = self._read_immediate()
            if not messages:
                messages = self._read_with_poll()
                self._listener_available = False
            self._notify_event.clear()
        else:
            messages = self._read_with_poll()

        if not messages:
            return None

        self.messages.extend(messages)
        return self._build_message(self.messages.popleft())

    @override
    def close(self) -> None:
        """Stop background threads and close the listener connection."""
        self._listener_stop.set()
        self._heartbeat_stop.set()
        self._notify_event.set()
        if self._listener_connection is not None:
            try:
                self._listener_connection.close()
            except Exception as e:
                self.broker.logger.error("Listener not joinable: %s", str(e))
            finally:
                self._listener_connection = None
        if self._listener_thread is not None:
            self._listener_thread.join(timeout=1)
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=1)
        return


class _PgmqMessage(MessageProxy):
    def __init__(self, pgmq_message: PgmqQueueMessage) -> None:
        """Wrap a PGMQ message row as a Remoulade message proxy."""
        payload = pgmq_message.message
        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PGMQ messages must contain JSON objects.")

        try:
            # Re-run the global message decoder so custom encoders (e.g. PydanticEncoder)
            # can rehydrate actor args/kwargs to their typed schemas.
            message = Message.decode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        except TypeError as exc:
            raise UnsupportedMessageEncoding("PGMQ message payload is not a valid Remoulade message envelope.") from exc

        super().__init__(message)
        self._pgmq_message = pgmq_message
