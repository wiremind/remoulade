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
import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from threading import local
from typing import TYPE_CHECKING

from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker as sqlalchemy_sessionmaker
from sqlalchemy.orm.session import sessionmaker

from ..broker import Broker
from ..errors import QueueNotFound, UnsupportedMessageEncoding
from ..state.backends.postgres import DEFAULT_POSTGRES_URI

if TYPE_CHECKING:
    from ..message import Message
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
        url: str | None = None,
        middleware: list["Middleware"] | None = None,
        sessionmaker: sessionmaker | None = None,
        engine: Engine | None = None,
        group_transaction: bool = False,
    ):
        super().__init__(middleware=middleware)

        if engine is not None and sessionmaker is not None:
            raise ValueError("Only one of engine or sessionmaker can be provided.")

        self.url = (
            url
            or os.getenv("REMOULADE_PGMQ_URL")
            or os.getenv("REMOULADE_POSTGRESQL_URL")
            or DEFAULT_POSTGRES_URI
        )
        self.state = local()
        self.group_transaction = group_transaction
        self._owns_engine = engine is None and sessionmaker is None

        if sessionmaker is not None:
            bound_engine = sessionmaker.kw.get("bind")
            if bound_engine is None or not isinstance(bound_engine, Engine):
                raise ValueError("sessionmaker must be bound to a SQLAlchemy engine.")
            self.engine = bound_engine
            self.sessionmaker = sessionmaker
        else:
            self.engine = engine or create_engine(self.url, pool_pre_ping=True)
            self.sessionmaker = sqlalchemy_sessionmaker(bind=self.engine)

        self.client = SQLAlchemyPGMQueue(engine=self.engine, init_extension=False)

    @contextmanager
    def tx(self):
        if self._has_transaction:
            self.state.transaction_depth += 1
            try:
                yield
            finally:
                self.state.transaction_depth -= 1
            return

        with self.sessionmaker.begin() as session:
            self.state.transaction_depth = 1
            self.state.transaction_session = session
            self.state.transaction_connection = session.connection()
            try:
                yield
            finally:
                self.state.transaction_depth = 0
                self.state.transaction_session = None
                self.state.transaction_connection = None

    @property
    def _has_transaction(self) -> bool:
        return getattr(self.state, "transaction_connection", None) is not None

    @property
    def _current_connection(self):
        return getattr(self.state, "transaction_connection", None)

    def close(self):
        if self._owns_engine:
            self.engine.dispose()

    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 30000):
        raise NotImplementedError("PgmqBroker.consume() will be implemented in jalon 2.")

    def declare_queue(self, queue_name: str) -> None:
        if queue_name in self.queues:
            return

        self.client.validate_queue_name(queue_name, conn=self._current_connection)
        self.emit_before("declare_queue", queue_name)
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
