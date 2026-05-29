# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 WIREMIND SAS <dev@wiremind.fr>
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
from typing import TYPE_CHECKING, Any

import psycopg
from pgmq import SQLAlchemyPGMQueue
from pgmq.messages import Message as PgmqQueueMessage
from psycopg import sql as psycopg_sql
from sqlalchemy import Column, Integer, MetaData, Table, bindparam, create_engine, func, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker as sqlalchemy_sessionmaker

from ..broker import Broker, Consumer, MessageProxy
from ..errors import QueueJoinTimeout, QueueNotFound, UnsupportedMessageEncoding
from ..message import Message

if TYPE_CHECKING:
    from ..middleware import Middleware

PgmqPayload = dict[str, Any]

DELAYED_SEND_SQL = text(
    """
    SELECT *
    FROM pgmq.send(
        queue_name => :queue_name,
        msg => :message,
        delay => :delay
    )
    """
).bindparams(bindparam("message", type_=JSONB))
LISTEN_NOTIFY_THROTTLE_MS = 250


class PgmqBroker(Broker):
    """A broker backed by PostgreSQL via the PGMQ extension."""

    def __init__(
        self,
        *,
        url: str,
        middleware: list["Middleware"] | None = None,
        group_transaction: bool = False,
        partition_archive_on_queue_init: bool = False,
        archive_partition_interval: int | str = "1 day",
        archive_retention_interval: int | str = "7 days",
        listen_notify_enabled: bool = True,
        visibility_timeout: int = 30,
        heartbeat_interval: float = 10.0,
    ) -> None:
        """Initialize a PostgreSQL-backed broker using the PGMQ extension.

        Parameters:
          url(str): PostgreSQL URL in plain format (`postgresql://...`), used both by SQLAlchemy and psycopg.
          middleware(list[Middleware] | None): Middleware stack applied to this broker.
          group_transaction(bool): If True, wraps group and pipeline operations in a single transaction.
          partition_archive_on_queue_init(bool): If True, creates partitioned queues when declaring queues.
          archive_partition_interval(int | str): Partition interval passed to PGMQ when creating partitioned queues.
          archive_retention_interval(int | str): Retention interval passed to PGMQ for archive partitions.
          listen_notify_enabled(bool): If True, enables LISTEN/NOTIFY to wake consumers when new messages arrive.
          visibility_timeout(int): Message visibility timeout in seconds after read; must be greater than 0.
          heartbeat_interval(float): Heartbeat interval in seconds used to extend in-flight message visibility;
            must be greater than 0 and lower than visibility_timeout.

        Raises:
          ValueError: If visibility_timeout or heartbeat_interval values are invalid.
        """
        super().__init__(middleware=middleware)
        if visibility_timeout <= 0:
            raise ValueError("visibility_timeout must be greater than 0")
        if heartbeat_interval <= 0 or heartbeat_interval >= visibility_timeout:
            raise ValueError("heartbeat_interval must be greater than 0 and lower than visibility_timeout")

        self.url = url
        self.state = local()
        self.group_transaction = group_transaction
        self.partition_archive_on_queue_init = partition_archive_on_queue_init
        self.archive_partition_interval = archive_partition_interval
        self.archive_retention_interval = archive_retention_interval
        self.listen_notify_enabled = listen_notify_enabled
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval

        self.engine = create_engine(self.url, pool_pre_ping=True)
        self.sessionmaker = sqlalchemy_sessionmaker(bind=self.engine)
        self.supports_native_delay = True

        self.client = SQLAlchemyPGMQueue(engine=self.engine, init_extension=False, vt=self.visibility_timeout)

    @contextmanager
    def tx(self) -> Iterator[None]:
        with self.sessionmaker.begin() as session:
            self.state.transaction_session = session
            self.state.transaction_connection = session.connection()
            try:
                yield
            finally:
                self.state.transaction_session = None
                self.state.transaction_connection = None

    @property
    def _current_connection(self) -> Any | None:
        return getattr(self.state, "transaction_connection", None)

    def _try_enable_notify(self, queue_name: str) -> None:
        if not self.listen_notify_enabled:
            return

        try:
            self.client.enable_notify(
                queue_name,
                throttle_interval_ms=LISTEN_NOTIFY_THROTTLE_MS,
                conn=self._current_connection,
            )
        except Exception:
            self.logger.warning(
                "Failed to enable LISTEN/NOTIFY for queue %s; consumer will fall back to polling.",
                queue_name,
                exc_info=True,
            )

    def close(self) -> None:
        self.engine.dispose()

    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 30000) -> "_PgmqConsumer":
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)
        return _PgmqConsumer(self, queue_name=queue_name, prefetch=prefetch, timeout=timeout)

    def declare_queue(self, queue_name: str) -> None:
        if queue_name in self.queues:
            return

        self.client.validate_queue_name(queue_name, conn=self._current_connection)
        self.emit_before("declare_queue", queue_name)
        if self.partition_archive_on_queue_init:
            self.client.create_partitioned_queue(
                queue_name,
                partition_interval=self.archive_partition_interval,
                retention_interval=self.archive_retention_interval,
                conn=self._current_connection,
            )
        else:
            self.client.create_queue(queue_name, conn=self._current_connection)
        self._try_enable_notify(queue_name)
        self.queues[queue_name] = None
        self.emit_after("declare_queue", queue_name)

    def _apply_delay(self, message: "Message", delay: int | None = None) -> "Message":
        return message

    def _encode_message(self, message: "Message") -> PgmqPayload:
        try:
            payload = json.loads(message.encode().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnsupportedMessageEncoding(
                "PgmqBroker only supports encoders that serialize messages as JSON objects."
            ) from exc

        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PgmqBroker requires message encoders to produce JSON objects.")

        return payload

    def _send_with_delay(self, queue_name: str, payload: PgmqPayload, delay: int) -> None:
        visible_at = datetime.now(UTC) + timedelta(milliseconds=delay)
        parameters = {"queue_name": queue_name, "message": payload, "delay": visible_at}

        if self._current_connection is not None:
            self._current_connection.execute(DELAYED_SEND_SQL, parameters)
            return

        with self.sessionmaker.begin() as session:
            session.connection().execute(DELAYED_SEND_SQL, parameters)

    def _enqueue(self, message: "Message", *, delay: int | None = None) -> "Message":
        if message.queue_name not in self.queues:
            raise QueueNotFound(message.queue_name)

        payload = self._encode_message(message)

        if delay is None:
            self.client.send(message.queue_name, payload, conn=self._current_connection)
        else:
            self._send_with_delay(message.queue_name, payload, delay)

        return message

    def flush(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)

        self.client.purge(queue_name, conn=self._current_connection)

    def purge_queue(self, queue_name: str) -> None:
        self.flush(queue_name)

    def flush_all(self) -> None:
        for queue_name in self.queues:
            self.flush(queue_name)

    def _count_enqueued_messages(self, queue_name: str) -> int:
        queue_table = Table(f"q_{queue_name}", MetaData(), Column("msg_id", Integer), schema="pgmq")
        count_query = select(func.count()).select_from(queue_table)

        with self.sessionmaker.begin() as session:
            return session.execute(count_query).scalar_one()

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
        self.prefetch = max(prefetch, 1)
        self.timeout = max(timeout, 0)
        self.visibility_timeout = broker.visibility_timeout
        self.heartbeat_interval = broker.heartbeat_interval
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

        if self.broker.listen_notify_enabled:
            self._start_listener()
        self._start_heartbeat()

    @property
    def max_poll_seconds(self) -> int:
        return max((self.timeout + 999) // 1000, 1)

    @property
    def poll_interval_ms(self) -> int:
        return max(min(self.timeout, 1000), 1)

    @property
    def wait_timeout_seconds(self) -> float:
        return self.timeout / 1000 if self.timeout > 0 else 0

    def _normalize_messages(self, messages: PgmqQueueMessage | list[PgmqQueueMessage] | None) -> list[PgmqQueueMessage]:
        if messages is None:
            return []
        return [messages] if not isinstance(messages, list) else messages

    def _read_immediate(self) -> list[PgmqQueueMessage]:
        return self._normalize_messages(
            self.client.read(
                self.queue_name,
                vt=self.visibility_timeout,
                qty=self.prefetch,
            )
        )

    def _read_with_poll(self) -> list[PgmqQueueMessage]:
        return self._normalize_messages(
            self.client.read_with_poll(
                self.queue_name,
                vt=self.visibility_timeout,
                qty=self.prefetch,
                max_poll_seconds=self.max_poll_seconds,
                poll_interval_ms=self.poll_interval_ms,
            )
        )

    def _start_heartbeat(self) -> None:
        self._heartbeat_thread = Thread(
            target=self._run_heartbeat,
            name=f"pgmq-heartbeat-{self.queue_name}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _run_heartbeat(self) -> None:
        while not self._heartbeat_stop.wait(self.heartbeat_interval):
            with self._heartbeat_msg_ids_lock:
                msg_ids = list(self._heartbeat_msg_ids)
            if not msg_ids:
                continue
            try:
                self.client.set_vt(self.queue_name, msg_ids, self.visibility_timeout)
            except Exception:
                self.broker.logger.warning(
                    "Failed to extend visibility timeout heartbeat for queue %s.",
                    self.queue_name,
                    exc_info=True,
                )

    def _register_heartbeat_msg(self, message: "_PgmqMessage") -> None:
        with self._heartbeat_msg_ids_lock:
            self._heartbeat_msg_ids.add(message._pgmq_message.msg_id)

    def _unregister_heartbeat_msg_id(self, msg_id: int) -> None:
        with self._heartbeat_msg_ids_lock:
            self._heartbeat_msg_ids.discard(msg_id)

    def _start_listener(self) -> None:
        channel = f"pgmq.q_{self.queue_name}.INSERT"
        try:
            self._listener_connection = psycopg.connect(self.broker.url, autocommit=True)
            self._listener_connection.execute(psycopg_sql.SQL("LISTEN {}").format(psycopg_sql.Identifier(channel)))
            self._listener_available = True
        except Exception:
            self._listener_available = False
            self._listener_connection = None
            self.broker.logger.warning(
                "Failed to start LISTEN/NOTIFY listener for queue %s; falling back to polling.",
                self.queue_name,
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
        while not self._listener_stop.is_set():
            try:
                if self._listener_connection is None:
                    break
                for _ in self._listener_connection.notifies(timeout=0.5, stop_after=1):
                    self._notify_event.set()
            except Exception:
                if self._listener_stop.is_set():
                    break
                self._listener_available = False
                self._notify_event.set()
                self.broker.logger.warning(
                    "LISTEN/NOTIFY listener crashed for queue %s; falling back to polling.",
                    self.queue_name,
                    exc_info=True,
                )
                break

        self._listener_available = False

    def ack(self, message: "MessageProxy") -> None:
        if not isinstance(message, _PgmqMessage):
            raise ValueError("It must be a PgmqMessage")
        self._unregister_heartbeat_msg_id(message._pgmq_message.msg_id)
        self.client.archive(self.queue_name, message._pgmq_message.msg_id)

    def nack(self, message: "MessageProxy") -> None:
        if not isinstance(message, _PgmqMessage):
            raise ValueError("It must be a PgmqMessage")
        self._unregister_heartbeat_msg_id(message._pgmq_message.msg_id)
        self.client.archive(self.queue_name, message._pgmq_message.msg_id)

    def requeue(self, messages: Iterable["MessageProxy"]) -> None:
        msg_ids = [message._pgmq_message.msg_id for message in messages if isinstance(message, _PgmqMessage)]
        if msg_ids:
            for msg_id in msg_ids:
                self._unregister_heartbeat_msg_id(msg_id)
            self.client.set_vt(self.queue_name, msg_ids, 0)

    def _build_message(self, pgmq_message: PgmqQueueMessage) -> "_PgmqMessage":
        message = _PgmqMessage(pgmq_message)
        self._register_heartbeat_msg(message)
        return message

    def __next__(self) -> "_PgmqMessage | None":
        if self.messages:
            return self._build_message(self.messages.popleft())

        if self._listener_available:
            self._notify_event.clear()
            messages = self._read_immediate()
            if not messages:
                self._notify_event.wait(self.wait_timeout_seconds)
                messages = self._read_with_poll() if not self._listener_available else self._read_immediate()
        else:
            messages = self._read_with_poll()

        if not messages:
            return None

        self.messages.extend(messages)
        return self._build_message(self.messages.popleft())

    def close(self) -> None:
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
