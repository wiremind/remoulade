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
import logging
import math
import time
from collections import deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import Event, Lock, Thread, local
from typing import TYPE_CHECKING, Any, Final, override
from urllib.parse import urlparse

import psycopg
from pgmq import SQLAlchemyPGMQueue
from pgmq.messages import Message as PostgresQueueMessage
from psycopg import sql as psycopg_sql
from sqlalchemy import Column, Connection, Integer, MetaData, Table, func, select

from ..broker import Broker, Consumer, MessageProxy
from ..errors import QueueJoinTimeout, QueueNotFound, UnsupportedMessageEncoding
from ..message import Message

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from ..middleware import Middleware

PostgresPayload = dict[str, Any]
LISTEN_NOTIFY_THROTTLE_MS: Final[int] = 250
LISTENER_RECONNECT_BACKOFF_MIN_S: Final[float] = 0.5
LISTENER_RECONNECT_BACKOFF_MAX_S: Final[float] = 30.0


def _milliseconds_to_seconds(milliseconds: int) -> int:
    """Convert milliseconds to whole seconds, rounding up for PGMQ inputs."""
    return max(1, math.ceil(milliseconds / 1000))


class PostgresBroker(Broker):
    """A broker backed by PostgreSQL via the PGMQ extension.

    PGMQ handles delayed messages natively, so delayed sends stay in
    PostgreSQL instead of being staged in worker memory.

    Connection budget (per worker process):
      * the shared SQLAlchemy pool (``pool_size``), used by reads, acks and
        the heartbeat, bounded regardless of the number of consumed queues;
      * a single shared LISTEN/NOTIFY connection (one per process, not one per
        queue) when ``enable_listen_notify`` is True.

    Set ``enable_listen_notify=False`` for a poll-only mode that opens no
    dedicated listener connection. This is required behind a connection pooler
    in transaction pooling mode (e.g. pgbouncer), where LISTEN/NOTIFY is not
    supported, and useful to cap connections on very large fan-outs.
    """

    supports_native_delay = True

    def __init__(
        self,
        *,
        url: str,
        middleware: list["Middleware"] | None = None,
        group_transaction: bool = False,
        archive_partition_interval_in_days: int = 1,
        archive_retention_interval_in_days: int = 7,
        visibility_timeout_ms: int = 30_000,
        heartbeat_interval_ms: int = 10_000,
        enable_listen_notify: bool = True,
        pool_size: int = 10,
        engine: "Engine | None" = None,
    ) -> None:
        """Initialize a PostgreSQL-backed broker using the PGMQ extension.

        Parameters:
          url(str): PostgreSQL URL in plain format (`postgresql://...`), used both by SQLAlchemy and psycopg.
          The url must be the creds for a user who can create and delete tables
          middleware(list[Middleware] | None): Middleware stack applied to this broker.
          group_transaction(bool): If True, wraps group and pipeline operations in a single transaction.
          archive_partition_interval_in_days(int): Partition interval passed to PGMQ when creating partitioned queues.
          archive_retention_interval_in_days(int): Retention interval passed to PGMQ for archive partitions.
          visibility_timeout_ms(int): Message visibility timeout in milliseconds after read; must be greater than 0.
          heartbeat_interval_ms(int): Heartbeat interval in milliseconds used to extend in-flight message visibility
            must be greater than 0 and lower than visibility_timeout_ms.
          enable_listen_notify(bool): If True (default), consumers are woken by a single process-wide LISTEN/NOTIFY
            connection. If False, consumers poll only and no dedicated listener connection is opened (required behind
            a transaction-pooling connection pooler such as pgbouncer).
          pool_size(int): Size of the shared SQLAlchemy connection pool. Ignored when ``engine`` is provided.
          engine(Engine | None): A pre-configured SQLAlchemy engine to reuse instead of letting PGMQ build one, so
            the pool can be sized and shared by the caller.
        """
        super().__init__(middleware=middleware)
        if visibility_timeout_ms <= 0:
            raise ValueError("visibility_timeout_ms must be greater than 0")
        if heartbeat_interval_ms <= 0 or heartbeat_interval_ms >= visibility_timeout_ms:
            raise ValueError("heartbeat_interval_ms must be greater than 0 and lower than visibility_timeout_ms")

        self.url = urlparse(url).geturl()
        self.state = local()
        self.group_transaction = group_transaction
        self.archive_partition_interval = self.convert_days_in_partman_syntax(archive_partition_interval_in_days)
        self.archive_retention_interval = self.convert_days_in_partman_syntax(archive_retention_interval_in_days)
        self.visibility_timeout_ms = visibility_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.visibility_timeout_seconds = _milliseconds_to_seconds(visibility_timeout_ms)
        self.heartbeat_interval_seconds = heartbeat_interval_ms / 1000
        self.enable_listen_notify = enable_listen_notify

        self.client = SQLAlchemyPGMQueue(
            conn_string=url,
            init_extension=False,
            vt=self.visibility_timeout_seconds,
            pool_size=pool_size,
            engine=engine,
        )

        self._listener = _PostgresListener(self.url, self.logger) if enable_listen_notify else None

    def convert_days_in_partman_syntax(self, interval_in_day: int) -> str:
        """Convert int into partman syntax"""
        if interval_in_day <= 0:
            raise ValueError("interval_in_day must be greater than 0")
        if interval_in_day == 1:
            return "1 day"
        return f"{interval_in_day} days"

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

    def _queue_exists(self, queue_name: str) -> bool:
        """Return whether the queue already exists in PostgreSQL."""
        return queue_name in {queue.queue_name for queue in self.client.list_queues()}

    @override
    def close(self) -> None:
        """Stop the shared listener and dispose the underlying PGMQ client."""
        if self._listener is not None:
            self._listener.close()
        self.client.dispose()

    @override
    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 30000) -> "_PostgresConsumer":
        """Create a consumer for a declared queue.

        Raises:
          QueueNotFound: If the queue has not been declared.
        """
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)
        return _PostgresConsumer(self, queue_name=queue_name, prefetch=prefetch, timeout=timeout)

    @override
    def declare_queue(self, queue_name: str) -> None:
        """Create a partitioned PGMQ queue if it does not already exist."""
        if queue_name in self.queues:
            return

        self.client.validate_queue_name(queue_name, conn=self._current_connection)
        queue_exists = self._queue_exists(queue_name)

        if not queue_exists:
            self.emit_before("declare_queue", queue_name)
            self.client.create_partitioned_queue(
                queue_name,
                partition_interval=self.archive_partition_interval,
                retention_interval=self.archive_retention_interval,
                conn=self._current_connection,
            )
            if self.enable_listen_notify:
                self._try_enable_notify(queue_name)

        self.queues[queue_name] = None

        if not queue_exists:
            self.emit_after("declare_queue", queue_name)

    def _encode_message(self, message: "Message") -> PostgresPayload:
        """Encode a Remoulade message into a JSON object payload for PGMQ.

        The encoder is responsible for validating that the payload is valid
        JSON; here we only enforce the PGMQ-specific requirement that it be a
        JSON object.

        Raises:
          UnsupportedMessageEncoding: If the encoded payload is not valid JSON
            or is not a JSON object.
        """
        try:
            payload = message.encode_in_json()
        except (TypeError, ValueError, UnsupportedMessageEncoding) as exc:
            raise UnsupportedMessageEncoding("PGMQ messages must contain JSON objects.") from exc

        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PGMQ messages must contain JSON objects.")

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


class _PostgresListener:
    """Process-wide LISTEN/NOTIFY dispatcher shared by all consumers of a broker.

    A single psycopg connection LISTENs on every consumed queue's
    ``pgmq.q_<name>.INSERT`` channel and a single background thread routes each
    notification to the matching consumer's wake event. This keeps the number
    of dedicated listener connections at one per process instead of one per
    consumed queue.

    The connection is opened synchronously when the first consumer registers.
    On any connection failure ``available`` flips to False and
    consumers transparently fall back to polling, while the thread keeps
    retrying (with capped backoff) to reopen the connection and re-LISTEN on
    every registered channel. ``available`` flips back to True as soon as a
    reconnection succeeds, so consumers resume LISTEN/NOTIFY automatically
    instead of polling for the rest of the process lifetime.
    """

    def __init__(self, url: str, logger: logging.Logger) -> None:
        self._url = url
        self._logger = logger
        self._lock = Lock()
        self._stop = Event()
        self._connection: psycopg.Connection[Any] | None = None
        self._thread: Thread | None = None
        self._started = False
        self.available = False
        self._events: dict[str, Event] = {}
        self._channel_to_queue: dict[str, str] = {}
        self._pending_listen: set[str] = set()

    @staticmethod
    def _channel_for(queue_name: str) -> str:
        return f"pgmq.q_{queue_name}.INSERT"

    def register(self, queue_name: str, event: Event) -> None:
        """Register a consumer's wake event and ensure its channel is listened to."""
        with self._lock:
            self._events[queue_name] = event
            self._channel_to_queue[self._channel_for(queue_name)] = queue_name
            self._pending_listen.add(queue_name)
            starting = not self._started
            if starting:
                self._started = True
        # Start outside the lock: the initial connection attempt acquires the
        # same lock to snapshot channels, and would otherwise deadlock.
        if starting:
            self._start()

    def unregister(self, queue_name: str) -> None:
        """Stop routing notifications to a consumer that is shutting down."""
        with self._lock:
            self._events.pop(queue_name, None)
            self._channel_to_queue.pop(self._channel_for(queue_name), None)
            self._pending_listen.discard(queue_name)

    def _start(self) -> None:
        """Open the shared connection (best effort) and start the dispatch thread.

        The initial connection attempt is synchronous so ``available`` reflects
        a real outcome by the time the first consumer starts reading. If it
        fails, the dispatch thread keeps retrying with backoff, reopening the
        connection and re-LISTENing on every registered channel, so a transient
        database outage degrades consumers to polling instead of disabling
        LISTEN/NOTIFY for the rest of the process lifetime.
        """
        self._open_connection()
        self._thread = Thread(target=self._run, name="postgres-listener", daemon=True)
        self._thread.start()

    def _open_connection(self) -> bool:
        """Open the shared connection and LISTEN on every registered channel.

        Called once synchronously when the listener starts and then from the
        dispatch thread on every reconnection. Must not be called while holding
        ``self._lock``, which it acquires to snapshot the registered channels.
        Returns True when the connection is ready and all channels are listened
        to, False when the connection could not be opened (the caller then backs
        off and retries).
        """
        connection = None
        try:
            connection = psycopg.connect(self._url, autocommit=True)
            with self._lock:
                # A fresh connection listens to nothing, so re-LISTEN every
                # channel currently registered rather than only those pending
                # since the last loop.
                channels = list(self._channel_to_queue)
                self._pending_listen.clear()
            for channel in channels:
                connection.execute(psycopg_sql.SQL("LISTEN {}").format(psycopg_sql.Identifier(channel)))
        except Exception as e:
            self._logger.warning(
                "Failed to open shared LISTEN/NOTIFY connection; consumers will fall back to polling. Error: %s",
                str(e),
                exc_info=True,
            )
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    self._logger.debug("Failed to close partially-opened listener connection", exc_info=True)
            return False
        self._connection = connection
        self.available = True
        return True

    def _drop_connection(self) -> None:
        """Mark the listener unavailable and discard the current connection.

        Waiting consumers are woken so they fall back to polling immediately
        instead of blocking on their wake event for the full timeout.
        """
        self.available = False
        self._wake_all()
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as e:
                self._logger.error("Failed to close shared listener connection: %s", str(e))
            self._connection = None

    def _drain_pending_listen(self) -> None:
        """Issue LISTEN for queues registered since the last loop (listener thread only)."""
        with self._lock:
            pending = list(self._pending_listen)
            self._pending_listen.clear()
        if self._connection is None:
            return
        for queue_name in pending:
            channel = self._channel_for(queue_name)
            self._connection.execute(psycopg_sql.SQL("LISTEN {}").format(psycopg_sql.Identifier(channel)))

    def _wake_channel(self, channel: str) -> None:
        """Wake the single consumer registered for a notification channel."""
        with self._lock:
            queue_name = self._channel_to_queue.get(channel)
            event = self._events.get(queue_name) if queue_name is not None else None
        if event is not None:
            event.set()

    def _wake_all(self) -> None:
        """Wake every registered consumer (used on shutdown or listener failure)."""
        with self._lock:
            events = list(self._events.values())
        for event in events:
            event.set()

    def _run(self) -> None:
        """Route notifications to consumer events, reconnecting on errors.

        When the connection is down it is reopened with an exponential backoff
        capped at ``LISTENER_RECONNECT_BACKOFF_MAX_S``; the backoff resets on
        every successful reconnection. Consumers poll while the connection is
        unavailable and resume LISTEN/NOTIFY once it is restored.
        """
        backoff = LISTENER_RECONNECT_BACKOFF_MIN_S
        while not self._stop.is_set():
            if self._connection is None:
                if not self._open_connection():
                    self._stop.wait(backoff)
                    backoff = min(backoff * 2, LISTENER_RECONNECT_BACKOFF_MAX_S)
                    continue
                backoff = LISTENER_RECONNECT_BACKOFF_MIN_S
            try:
                self._drain_pending_listen()
                for notify in self._connection.notifies(timeout=0.5, stop_after=1):
                    self._wake_channel(notify.channel)
            except Exception as e:
                if self._stop.is_set():
                    break
                self._logger.warning(
                    "Shared LISTEN/NOTIFY listener error; consumers will fall back to polling while it "
                    "reconnects. Error: %s",
                    str(e),
                    exc_info=True,
                )
                self._drop_connection()

        self._drop_connection()

    def close(self) -> None:
        """Stop the dispatch thread and close the shared connection."""
        self._stop.set()
        self._wake_all()
        if self._thread is not None:
            self._thread.join(timeout=1)
        self._drop_connection()


class _PostgresConsumer(Consumer):
    def __init__(self, broker: PostgresBroker, *, queue_name: str, prefetch: int, timeout: int) -> None:
        """Initialize a consumer for a PGMQ queue.

        Parameters:
          broker(PostgresBroker): Broker instance that owns the queue and database client.
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
        self.visibility_timeout_seconds = broker.visibility_timeout_seconds
        self.heartbeat_interval_seconds = broker.heartbeat_interval_seconds
        self.messages: deque[PostgresQueueMessage] = deque()
        self._notify_event = Event()
        self._heartbeat_stop = Event()
        self._heartbeat_thread: Thread | None = None
        self._heartbeat_message_ids_lock = Lock()
        self._heartbeat_message_ids: set[int] = set()

        if broker._listener is not None:
            broker._listener.register(queue_name, self._notify_event)
        self._start_heartbeat()

    @property
    def wait_timeout_seconds(self) -> float:
        """Return the listener wait timeout in seconds."""
        return self.timeout / 1000 if self.timeout > 0 else 0

    @property
    def _listener_available(self) -> bool:
        """Whether the broker's shared LISTEN/NOTIFY listener is currently usable."""
        return self.broker._listener is not None and self.broker._listener.available

    def _normalize_messages(
        self, messages: PostgresQueueMessage | list[PostgresQueueMessage] | None
    ) -> list[PostgresQueueMessage]:
        """Normalize a PGMQ read result into a list."""
        if messages is None:
            return []
        return [messages] if not isinstance(messages, list) else messages

    def _read_immediate(self) -> list[PostgresQueueMessage]:
        """Read up to ``prefetch`` messages without polling."""
        return self._normalize_messages(
            self.client.read(
                self.queue_name,
                vt=self.visibility_timeout_seconds,
                qty=self.prefetch,
            )
        )

    def _read_with_poll(self) -> list[PostgresQueueMessage]:
        """Read up to ``prefetch`` messages using PGMQ polling."""
        return self._normalize_messages(
            self.client.read_with_poll(
                self.queue_name,
                vt=self.visibility_timeout_seconds,
                qty=self.prefetch,
                max_poll_seconds=max((self.timeout + 999) // 1000, 1),
                poll_interval_ms=max(min(self.timeout, 1000), 1),
            )
        )

    def _read_next_batch(self) -> list[PostgresQueueMessage]:
        """Read the next batch, favoring LISTEN/NOTIFY when available."""
        if not self._listener_available:
            return self._read_with_poll()

        messages = self._read_immediate()
        if messages:
            self._notify_event.clear()
            return messages

        self._notify_event.wait(self.wait_timeout_seconds)
        self._notify_event.clear()
        return self._read_immediate() if self._listener_available else self._read_with_poll()

    def _start_heartbeat(self) -> None:
        """Start the background visibility-timeout renewal thread."""
        self._heartbeat_thread = Thread(
            target=self._run_heartbeat,
            name=f"postgres-heartbeat-{self.queue_name}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _run_heartbeat(self) -> None:
        """Periodically renew the lease of in-flight messages.

        The loop sleeps for ``heartbeat_interval_seconds`` seconds, derived from the
        millisecond broker input, unless shutdown is requested through
        ``_heartbeat_stop``. On each tick it snapshots the current set of
        tracked message ids under a lock, then calls ``set_vt`` to push their
        visibility timeout back to ``self.visibility_timeout_seconds`` seconds.

        This prevents long-running handlers from becoming visible again and
        being consumed twice before they are explicitly acked, nacked or
        requeued. Failures are logged and retried on the next heartbeat tick.
        """
        while not self._heartbeat_stop.wait(self.heartbeat_interval_seconds):
            with self._heartbeat_message_ids_lock:
                message_ids = list(self._heartbeat_message_ids)
            if not message_ids:
                continue
            try:
                self.client.set_vt(self.queue_name, message_ids, self.visibility_timeout_seconds)
            except Exception as e:
                self.broker.logger.warning(
                    "Failed to extend visibility timeout heartbeat for queue %s. Error: %s",
                    self.queue_name,
                    str(e),
                    exc_info=True,
                )

    def _unregister_heartbeat_message_id(self, message_id: int) -> None:
        """Stop tracking a message for heartbeat renewal."""
        with self._heartbeat_message_ids_lock:
            self._heartbeat_message_ids.discard(message_id)

    def _requeue_message_ids(self, message_ids: list[int]) -> None:
        """Make a batch of message ids visible again immediately."""
        for message_id in message_ids:
            self._unregister_heartbeat_message_id(message_id)
        if len(message_ids) > 0:
            self.client.set_vt(self.queue_name, message_ids, 0)

    @override
    def ack(self, message: "MessageProxy") -> None:
        """Archive a processed message."""
        if not isinstance(message, _PostgresMessage):
            raise ValueError("It must be a PostgresMessage")
        self._unregister_heartbeat_message_id(message._postgres_message.msg_id)
        self.client.archive(self.queue_name, message._postgres_message.msg_id)

    @override
    def nack(self, message: "MessageProxy") -> None:
        """Archive a failed message."""
        if not isinstance(message, _PostgresMessage):
            raise ValueError("It must be a PostgresMessage")
        self._unregister_heartbeat_message_id(message._postgres_message.msg_id)
        self.client.archive(self.queue_name, message._postgres_message.msg_id)

    @override
    def requeue(self, messages: Iterable["MessageProxy"]) -> None:
        """Make messages visible again immediately by resetting their visibility timeout."""
        message_ids = [
            message._postgres_message.msg_id for message in messages if isinstance(message, _PostgresMessage)
        ]
        self._requeue_message_ids(message_ids)

    def _build_message(self, postgres_message: PostgresQueueMessage) -> "_PostgresMessage":
        """Wrap a raw PGMQ row as a Remoulade message proxy."""
        return _PostgresMessage(postgres_message)

    @override
    def __next__(self) -> "_PostgresMessage | None":
        """Return the next available message, or ``None`` if the queue stays empty."""
        if self.messages:
            return self._build_message(self.messages.popleft())

        messages = self._read_next_batch()

        if not messages:
            return None
        message_ids = [message.msg_id for message in messages]
        with self._heartbeat_message_ids_lock:
            self._heartbeat_message_ids.update(message_ids)
        self.messages.extend(messages)
        return self._build_message(self.messages.popleft())

    @override
    def close(self) -> None:
        """Stop the heartbeat, unregister from the shared listener and requeue buffered messages."""
        self._heartbeat_stop.set()
        self._notify_event.set()
        if self.broker._listener is not None:
            self.broker._listener.unregister(self.queue_name)
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=1)
        self._requeue_message_ids([message.msg_id for message in self.messages])
        self.messages.clear()


class _PostgresMessage(MessageProxy):
    def __init__(self, postgres_message: PostgresQueueMessage) -> None:
        """Wrap a PGMQ message row as a Remoulade message proxy."""
        payload = postgres_message.message
        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PGMQ messages must contain JSON objects.")
        try:
            # Re-run the global message decoder so custom encoders (e.g. PydanticEncoder)
            # can rehydrate actor args/kwargs to their typed schemas.
            message = Message.decode_json(payload)
        except TypeError as exc:
            raise UnsupportedMessageEncoding("PGMQ message payload is not a valid Remoulade message envelope.") from exc
        if message.options.get("eta", None) is not None:
            raise UnsupportedMessageEncoding("eta option isn't supported with postgres broker")
        super().__init__(message)
        self._postgres_message = postgres_message
