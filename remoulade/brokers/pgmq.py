# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
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
from collections import deque
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import local
from typing import TYPE_CHECKING

from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker as sqlalchemy_sessionmaker

from ..broker import Broker, Consumer, MessageProxy
from ..errors import QueueNotFound, UnsupportedMessageEncoding
from ..message import Message

if TYPE_CHECKING:
    from ..middleware import Middleware


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
    ):
        super().__init__(middleware=middleware)

        self.url = url
        self.state = local()
        self.group_transaction = group_transaction
        self.partition_archive_on_queue_init = partition_archive_on_queue_init
        self.archive_partition_interval = archive_partition_interval
        self.archive_retention_interval = archive_retention_interval

        self.engine = create_engine(self.url, pool_pre_ping=True)
        self.sessionmaker = sqlalchemy_sessionmaker(bind=self.engine)
        self.supports_native_delay = True

        self.client = SQLAlchemyPGMQueue(engine=self.engine, init_extension=False)

    @contextmanager
    def tx(self):
        with self.sessionmaker.begin() as session:
            self.state.transaction_session = session
            self.state.transaction_connection = session.connection()
            try:
                yield
            finally:
                self.state.transaction_session = None
                self.state.transaction_connection = None

    @property
    def _current_connection(self):
        return getattr(self.state, "transaction_connection", None)

    def close(self):
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
        self.queues[queue_name] = None
        self.emit_after("declare_queue", queue_name)

    def _apply_delay(self, message: "Message", delay: int | None = None) -> "Message":
        return message

    def _encode_message(self, message: "Message") -> dict:
        try:
            payload = json.loads(message.encode().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnsupportedMessageEncoding(
                "PgmqBroker only supports encoders that serialize messages as JSON objects."
            ) from exc

        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PgmqBroker requires message encoders to produce JSON objects.")

        return payload

    def _send_with_delay(self, queue_name: str, payload: dict, delay: int) -> None:
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

    def join(self, queue_name: str, *, timeout: int | None = None) -> None:
        raise NotImplementedError("PgmqBroker.join() will be implemented in jalon 3.")


class _PgmqConsumer(Consumer):
    def __init__(self, broker: PgmqBroker, *, queue_name: str, prefetch: int, timeout: int):
        self.broker = broker
        self.client = broker.client
        self.queue_name = queue_name
        self.prefetch = max(prefetch, 1)
        self.timeout = max(timeout, 0)
        self.messages = deque()

    @property
    def max_poll_seconds(self) -> int:
        return max((self.timeout + 999) // 1000, 1)

    @property
    def poll_interval_ms(self) -> int:
        return max(min(self.timeout, 1000), 1)

    def ack(self, message):
        if not isinstance(message, _PgmqMessage):
            raise ValueError("It must be a PgmqMessage")
        self.client.delete(self.queue_name, message._pgmq_message.msg_id)

    def nack(self, message):
        if not isinstance(message, _PgmqMessage):
            raise ValueError("It must be a PgmqMessage")
        self.client.archive(self.queue_name, message._pgmq_message.msg_id)

    def requeue(self, messages):
        msg_ids = [message._pgmq_message.msg_id for message in messages if isinstance(message, _PgmqMessage)]
        if msg_ids:
            self.client.set_vt(self.queue_name, msg_ids, 0)

    def __next__(self):
        if self.messages:
            return _PgmqMessage(self.messages.popleft())

        messages = self.client.read_with_poll(
            self.queue_name,
            qty=self.prefetch,
            max_poll_seconds=self.max_poll_seconds,
            poll_interval_ms=self.poll_interval_ms,
        )
        if not messages:
            return None

        self.messages.extend(messages)
        return _PgmqMessage(self.messages.popleft())

    def close(self):
        return


class _PgmqMessage(MessageProxy):
    def __init__(self, pgmq_message):
        payload = pgmq_message.message
        if not isinstance(payload, dict):
            raise UnsupportedMessageEncoding("PGMQ messages must contain JSON objects.")

        try:
            message = Message(**payload)
        except TypeError as exc:
            raise UnsupportedMessageEncoding("PGMQ message payload is not a valid Remoulade message envelope.") from exc

        super().__init__(message)
        self._pgmq_message = pgmq_message
