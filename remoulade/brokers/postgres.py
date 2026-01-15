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
import hashlib
import os
import time
import uuid
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta
from threading import local
from typing import TYPE_CHECKING, Deque, Dict, List, Optional, cast

from pytz import utc
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    LargeBinary,
    SmallInteger,
    String,
    create_engine,
    func,
    inspect,
    text,
)
from sqlalchemy import delete, or_, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker

from ..broker import Broker, Consumer, MessageProxy
from ..common import current_millis
from ..errors import ConnectionClosed, QueueJoinTimeout, QueueNotFound
from ..helpers.queues import dq_name, xq_name
from ..logging import get_logger
from ..message import Message

if TYPE_CHECKING:
    from ..middleware import Middleware  # noqa

DEFAULT_POSTGRES_URI = "postgresql://remoulade@localhost:5432/remoulade"
DEAD_MESSAGE_TTL = 86400000 * 7
DEFAULT_ACK_RETENTION = 86400000 * 7
DEFAULT_POLL_INTERVAL = 500
DEFAULT_VISIBILITY_TIMEOUT = 3600000
DB_VERSION = 2
ACK_RETENTION_UNSET = object()

Base = declarative_base()


class BrokerVersion(Base):
    __tablename__ = "remoulade_broker_version"

    version = Column(SmallInteger, primary_key=True)


class StoredMessage(Base):
    __tablename__ = "remoulade_messages"
    __table_args__ = {"postgresql_partition_by": "LIST (queue_name)"}

    queue_name = Column(String(length=200), primary_key=True, nullable=False)
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    message_id = Column(String(length=36), index=True, nullable=False)
    priority = Column(SmallInteger, index=True, nullable=False, default=0)
    message = Column(LargeBinary, nullable=False)
    enqueued_at = Column(
        DateTime(timezone=True), index=True, nullable=False, server_default=func.now()
    )
    locked_by = Column(String(length=36), index=True)
    locked_at = Column(DateTime(timezone=True), index=True)
    expires_at = Column(DateTime(timezone=True), index=True)


class ArchivedMessage(Base):
    __tablename__ = "remoulade_messages_archive"
    __table_args__ = {"postgresql_partition_by": "LIST (queue_name)"}

    queue_name = Column(String(length=200), primary_key=True, nullable=False)
    id = Column(BigInteger, primary_key=True, autoincrement=False)
    message_id = Column(String(length=36), index=True, nullable=False)
    priority = Column(SmallInteger, index=True, nullable=False, default=0)
    message = Column(LargeBinary, nullable=False)
    enqueued_at = Column(DateTime(timezone=True), index=True, nullable=False)
    archived_at = Column(DateTime(timezone=True), index=True, nullable=False)
    expires_at = Column(DateTime(timezone=True), index=True)


class PostgresBroker(Broker):
    """A broker that can be used with PostgreSQL.

    Examples:

      >>> PostgresBroker(url="postgresql://remoulade@localhost:5432/remoulade")

    Messages are stored in list-partitioned tables keyed by queue name.
    Acked messages are moved to an archive table for replay.

    Parameters:
      url(str): The optional connection URL to use.
      middleware(list[Middleware]): The set of middleware that apply
        to this broker.
      max_priority(int): Configure maximum supported priority value.
      dead_queue_max_length(int|None): Max size of the dead queue. If None,
        no max size, if 0 disable dead queue.
      dead_queue_ttl(int): Max time to keep dead-lettered messages in milliseconds.
      ack_retention(int|None): Time to keep acked messages in milliseconds (0 disables archiving).
        When unset, uses REMOULADE_POSTGRESQL_ACK_RETENTION or defaults to 7 days.
      poll_interval(int): Delay in milliseconds between receive polls.
      visibility_timeout(int|None): Max lock duration in milliseconds.
      group_transaction(bool): If true, use transactions by default when running group and pipelines.
    """

    def __init__(
        self,
        *,
        url: Optional[str] = None,
        middleware: Optional[List["Middleware"]] = None,
        client: Optional[sessionmaker] = None,
        max_priority: Optional[int] = None,
        dead_queue_max_length: Optional[int] = None,
        dead_queue_ttl: int = DEAD_MESSAGE_TTL,
        ack_retention: Optional[int] | object = ACK_RETENTION_UNSET,
        poll_interval: int = DEFAULT_POLL_INTERVAL,
        visibility_timeout: Optional[int] = DEFAULT_VISIBILITY_TIMEOUT,
        group_transaction: bool = False,
        future: bool = False,
    ) -> None:
        super().__init__(middleware=middleware)

        if max_priority is not None and not (0 < max_priority <= 255):
            raise ValueError("max_priority must be a value between 0 and 255")

        if dead_queue_max_length is not None and dead_queue_max_length < 0:
            raise ValueError("dead_queue_max_length must be above or equal to 0")

        if ack_retention is ACK_RETENTION_UNSET:
            env_value = os.getenv("REMOULADE_POSTGRESQL_ACK_RETENTION")
            if env_value is not None:
                try:
                    ack_retention = int(env_value)
                except ValueError as exc:
                    raise ValueError(
                        "REMOULADE_POSTGRESQL_ACK_RETENTION must be an integer"
                    ) from exc
            else:
                ack_retention = DEFAULT_ACK_RETENTION

        ack_retention_value = cast(Optional[int], ack_retention)
        if ack_retention_value is not None and ack_retention_value < 0:
            raise ValueError("ack_retention must be above or equal to 0")

        if poll_interval <= 0:
            raise ValueError("poll_interval must be above 0")

        if visibility_timeout is not None and visibility_timeout < 0:
            raise ValueError("visibility_timeout must be above or equal to 0")

        self.url = url or os.getenv("REMOULADE_POSTGRESQL_URL") or DEFAULT_POSTGRES_URI
        if client is None:
            self.engine = create_engine(self.url, pool_pre_ping=True, future=future)
            self.client = sessionmaker(self.engine)
        else:
            self.client = client
            self.engine = client.kw.get("bind") if hasattr(client, "kw") else None
        self.max_priority = max_priority
        self.dead_queue_max_length = dead_queue_max_length
        self.dead_queue_ttl = dead_queue_ttl
        self.ack_retention = ack_retention_value
        self.visibility_timeout = visibility_timeout
        self.poll_interval = poll_interval / 1000
        self.group_transaction = group_transaction
        self.state = local()
        self.dead_queues = set()
        self.init_db()

    @property
    def dead_queue_enabled(self) -> bool:
        return self.dead_queue_max_length != 0

    def _partition_name(self, table_name: str, queue_name: str) -> str:
        digest = hashlib.sha1(queue_name.encode("utf-8")).hexdigest()[:16]
        return f"{table_name}_p_{digest}"

    def _ensure_partition(self, session, table_name: str, queue_name: str) -> None:
        partition_name = self._partition_name(table_name, queue_name)
        session.execute(
            text(
                f"CREATE TABLE IF NOT EXISTS {partition_name} PARTITION OF {table_name} "
                "FOR VALUES IN (:queue_name)"
            ),
            {"queue_name": queue_name},
        )

    def _ensure_partitions(self, queue_name: str) -> None:
        with self._get_session() as session:
            self._ensure_partition(session, StoredMessage.__tablename__, queue_name)
            self._ensure_partition(session, ArchivedMessage.__tablename__, queue_name)

    def _register_queue(self, queue_name: str) -> None:
        if (
            queue_name in self.queues
            or queue_name in self.delay_queues
            or queue_name in self.dead_queues
        ):
            return

        if queue_name.endswith(".DQ"):
            self.delay_queues.add(queue_name)
        elif queue_name.endswith(".XQ"):
            self.dead_queues.add(queue_name)
        else:
            self.queues[queue_name] = None

    def init_db(self) -> None:
        with self.client.begin() as session:
            bind = session.get_bind()
            inspector = inspect(bind)

            if not inspector.has_table(BrokerVersion.__tablename__):
                Base.metadata.create_all(bind=bind, tables=[BrokerVersion.__table__])

            version = session.query(BrokerVersion).first()
            if version is None:
                session.add(BrokerVersion(version=DB_VERSION))

            has_messages = inspector.has_table(StoredMessage.__tablename__)
            has_archive = inspector.has_table(ArchivedMessage.__tablename__)

            if not has_messages or not has_archive:
                Base.metadata.create_all(
                    bind=bind,
                    tables=[StoredMessage.__table__, ArchivedMessage.__table__],
                )
            elif version is None or version.version != DB_VERSION:
                session.execute(
                    text(
                        f"DROP TABLE IF EXISTS {ArchivedMessage.__tablename__} CASCADE"
                    )
                )
                session.execute(
                    text(f"DROP TABLE IF EXISTS {StoredMessage.__tablename__} CASCADE")
                )
                Base.metadata.create_all(
                    bind=bind,
                    tables=[StoredMessage.__table__, ArchivedMessage.__table__],
                )
                if version is not None:
                    version.version = DB_VERSION

    def close(self) -> None:
        self.logger.debug("Closing PostgreSQL broker connections...")
        try:
            if self.engine is not None:
                self.engine.dispose()
        except Exception:  # pragma: no cover
            self.logger.debug(
                "Encountered an error while closing PostgreSQL connections.",
                exc_info=True,
            )
        self.logger.debug("PostgreSQL broker connections closed.")

    def consume(
        self, queue_name: str, prefetch: int = 1, timeout: int = 5000
    ) -> "_PostgresConsumer":
        if queue_name not in self.queues and queue_name not in self.delay_queues:
            raise QueueNotFound(queue_name)
        return _PostgresConsumer(self, queue_name, prefetch, timeout)

    def declare_queue(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            self.emit_before("declare_queue", queue_name)
            self.queues[queue_name] = None
            self.emit_after("declare_queue", queue_name)
            self._ensure_partitions(queue_name)

            delayed_name = dq_name(queue_name)
            self.delay_queues.add(delayed_name)
            self.emit_after("declare_delay_queue", delayed_name)
            self._ensure_partitions(delayed_name)

            if self.dead_queue_enabled:
                dead_name = xq_name(queue_name)
                self.dead_queues.add(dead_name)
                self._ensure_partitions(dead_name)

    def _apply_delay(
        self, message: "Message", delay: Optional[int] = None
    ) -> "Message":
        if delay is not None:
            message_eta = current_millis() + delay
            queue_name = (
                message.queue_name if delay is None else dq_name(message.queue_name)
            )
            message = message.copy(queue_name=queue_name, options={"eta": message_eta})

        return message

    @contextmanager
    def tx(self):
        with self.client.begin() as session:
            self.state.session = session
            try:
                yield
            finally:
                self.state.session = None

    @contextmanager
    def _get_session(self):
        session = getattr(self.state, "session", None)
        if session is not None:
            yield session
        else:
            with self.client.begin() as session:
                yield session

    def _enqueue(self, message: "Message", *, delay: Optional[int] = None) -> "Message":
        queue_name = message.queue_name
        if (
            queue_name not in self.queues
            and queue_name not in self.delay_queues
            and queue_name not in self.dead_queues
        ):
            raise QueueNotFound(queue_name)

        actor = self.get_actor(message.actor_name)
        priority = message.options.get("priority", actor.priority)
        if self.max_priority is not None:
            priority = min(max(priority, 0), self.max_priority)

        stored_message = StoredMessage(
            queue_name=queue_name,
            message_id=message.message_id,
            priority=priority,
            message=message.encode(),
            enqueued_at=datetime.now(utc),
        )
        try:
            with self._get_session() as session:
                session.add(stored_message)
            return message
        except SQLAlchemyError as exc:
            raise ConnectionClosed(exc) from None

    def _available_clause(self, queue_name: str, now: datetime):
        if self.visibility_timeout is None:
            lock_clause = StoredMessage.locked_at.is_(None)
        else:
            cutoff = now - timedelta(milliseconds=self.visibility_timeout)
            lock_clause = or_(
                StoredMessage.locked_at.is_(None), StoredMessage.locked_at < cutoff
            )
        return [
            StoredMessage.queue_name == queue_name,
            lock_clause,
            or_(StoredMessage.expires_at.is_(None), StoredMessage.expires_at > now),
        ]

    def _fetch_messages(
        self, queue_name: str, prefetch: int, consumer_id: str
    ) -> List["_PostgresMessage"]:
        limit = max(prefetch, 1)
        now = datetime.now(utc)
        subquery = (
            select(StoredMessage.id)
            .where(*self._available_clause(queue_name, now))
            .order_by(StoredMessage.priority.desc(), StoredMessage.id.asc())
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
        statement = (
            update(StoredMessage)
            .where(StoredMessage.queue_name == queue_name)
            .where(StoredMessage.id.in_(subquery))
            .values(locked_by=consumer_id, locked_at=now)
            .returning(StoredMessage.id, StoredMessage.message)
        )
        try:
            with self._get_session() as session:
                rows = session.execute(statement).all()
        except SQLAlchemyError as exc:
            raise ConnectionClosed(exc) from None

        messages = []
        for row in rows:
            message = Message.decode(row.message)
            messages.append(_PostgresMessage(message, row.id, consumer_id))
        return messages

    def _ack_message(self, db_id: int, queue_name: str, consumer_id: str) -> None:
        try:
            with self._get_session() as session:
                row = (
                    session.execute(
                        select(StoredMessage).where(
                            StoredMessage.id == db_id,
                            StoredMessage.queue_name == queue_name,
                            StoredMessage.locked_by == consumer_id,
                        )
                    )
                    .scalars()
                    .first()
                )
                if row is None:
                    return

                if self.ack_retention == 0:
                    session.delete(row)
                    return

                self._ensure_partition(
                    session, ArchivedMessage.__tablename__, row.queue_name
                )
                archived_at = datetime.now(utc)
                expires_at = None
                if self.ack_retention is not None:
                    expires_at = archived_at + timedelta(
                        milliseconds=self.ack_retention
                    )

                archive = ArchivedMessage(
                    queue_name=row.queue_name,
                    id=row.id,
                    message_id=row.message_id,
                    priority=row.priority,
                    message=row.message,
                    enqueued_at=row.enqueued_at,
                    archived_at=archived_at,
                    expires_at=expires_at,
                )
                session.add(archive)
                session.delete(row)
        except SQLAlchemyError as exc:
            raise ConnectionClosed(exc) from None

    def _truncate_dead_queue(self, session, queue_name: str) -> None:
        if self.dead_queue_max_length is None or self.dead_queue_max_length <= 0:
            return

        subquery = (
            select(StoredMessage.id)
            .where(StoredMessage.queue_name == queue_name)
            .order_by(StoredMessage.id.desc())
            .limit(self.dead_queue_max_length)
        )
        session.execute(
            delete(StoredMessage).where(
                StoredMessage.queue_name == queue_name, ~StoredMessage.id.in_(subquery)
            )
        )

    def _nack_message(self, db_id: int, queue_name: str, consumer_id: str) -> None:
        try:
            with self._get_session() as session:
                if not self.dead_queue_enabled:
                    session.execute(
                        delete(StoredMessage).where(
                            StoredMessage.id == db_id,
                            StoredMessage.queue_name == queue_name,
                            StoredMessage.locked_by == consumer_id,
                        )
                    )
                    return

                dead_queue_name = xq_name(queue_name)
                expires_at = datetime.now(utc) + timedelta(
                    milliseconds=self.dead_queue_ttl
                )
                session.execute(
                    update(StoredMessage)
                    .where(
                        StoredMessage.id == db_id,
                        StoredMessage.queue_name == queue_name,
                        StoredMessage.locked_by == consumer_id,
                    )
                    .values(
                        queue_name=dead_queue_name,
                        locked_by=None,
                        locked_at=None,
                        expires_at=expires_at,
                    )
                )
                self._truncate_dead_queue(session, dead_queue_name)
        except SQLAlchemyError as exc:
            raise ConnectionClosed(exc) from None

    def _requeue_messages(self, messages: List["_PostgresMessage"]) -> None:
        if not messages:
            return

        grouped: Dict[str, List[int]] = {}
        for message in messages:
            grouped.setdefault(message.queue_name, []).append(message.db_id)

        try:
            with self._get_session() as session:
                for queue_name, db_ids in grouped.items():
                    session.execute(
                        update(StoredMessage)
                        .where(StoredMessage.queue_name == queue_name)
                        .where(StoredMessage.id.in_(db_ids))
                        .values(locked_by=None, locked_at=None)
                    )
        except SQLAlchemyError as exc:
            raise ConnectionClosed(exc) from None

    def requeue_archived(
        self, message_id: str, queue_name: Optional[str] = None
    ) -> Optional[Message]:
        with self._get_session() as session:
            query = select(ArchivedMessage).where(
                ArchivedMessage.message_id == message_id
            )
            if queue_name is not None:
                query = query.where(ArchivedMessage.queue_name == queue_name)
            row = (
                session.execute(query.order_by(ArchivedMessage.archived_at.desc()))
                .scalars()
                .first()
            )
            if row is None:
                return None

            message_data = row.message
            self._register_queue(row.queue_name)
            self._ensure_partition(session, StoredMessage.__tablename__, row.queue_name)
            session.add(
                StoredMessage(
                    queue_name=row.queue_name,
                    message_id=row.message_id,
                    priority=row.priority,
                    message=message_data,
                    enqueued_at=datetime.now(utc),
                )
            )
            session.delete(row)

        return Message.decode(message_data)

    def get_queue_message_counts(self, queue_name: str):
        with self._get_session() as session:
            base_query = session.query(func.count(StoredMessage.id))
            queue_count = (
                base_query.filter(StoredMessage.queue_name == queue_name).scalar() or 0
            )
            dq_queue_count = (
                base_query.filter(
                    StoredMessage.queue_name == dq_name(queue_name)
                ).scalar()
                or 0
            )
            xq_queue_count = 0
            if self.dead_queue_enabled:
                xq_queue_count = (
                    base_query.filter(
                        StoredMessage.queue_name == xq_name(queue_name)
                    ).scalar()
                    or 0
                )

        return queue_count, dq_queue_count, xq_queue_count

    def flush(self, queue_name: str) -> None:
        if queue_name not in self.queues:
            raise QueueNotFound(queue_name)
        queues = [queue_name, dq_name(queue_name)]
        if self.dead_queue_enabled:
            queues.append(xq_name(queue_name))
        with self._get_session() as session:
            session.execute(
                delete(StoredMessage).where(StoredMessage.queue_name.in_(queues))
            )
            session.execute(
                delete(ArchivedMessage).where(ArchivedMessage.queue_name.in_(queues))
            )

    def flush_all(self) -> None:
        for queue_name in self.queues:
            self.flush(queue_name)

    def join(
        self,
        queue_name: str,
        min_successes: int = 10,
        idle_time: int = 100,
        *,
        timeout: Optional[int] = None,
    ) -> None:
        deadline = timeout and time.monotonic() + timeout / 1000
        successes = 0
        while successes < min_successes:
            if deadline and time.monotonic() >= deadline:
                raise QueueJoinTimeout(queue_name)

            total_messages = sum(self.get_queue_message_counts(queue_name)[:-1])
            if total_messages == 0:
                successes += 1
            else:
                successes = 0

            time.sleep(idle_time / 1000)

    def clean(self) -> int:
        now = datetime.now(utc)
        with self._get_session() as session:
            result = session.execute(
                delete(StoredMessage).where(
                    StoredMessage.expires_at.isnot(None),
                    StoredMessage.expires_at <= now,
                )
            )
            archive_result = session.execute(
                delete(ArchivedMessage).where(
                    ArchivedMessage.expires_at.isnot(None),
                    ArchivedMessage.expires_at <= now,
                )
            )
        return (result.rowcount or 0) + (archive_result.rowcount or 0)


class _PostgresConsumer(Consumer):
    def __init__(
        self, broker: PostgresBroker, queue_name: str, prefetch: int, timeout: int
    ) -> None:
        self.logger = get_logger(__name__, type(self))
        self.broker = broker
        self.queue_name = queue_name
        self.prefetch = prefetch
        self.timeout = timeout
        self.consumer_id = uuid.uuid4().hex
        self.buffer: Deque[_PostgresMessage] = deque()

    def ack(self, message: "_PostgresMessage") -> None:
        self.broker._ack_message(message.db_id, message.queue_name, self.consumer_id)

    def nack(self, message: "_PostgresMessage") -> None:
        self.broker._nack_message(message.db_id, message.queue_name, self.consumer_id)

    def requeue(self, messages) -> None:
        pending = [
            message for message in messages if isinstance(message, _PostgresMessage)
        ]
        self.broker._requeue_messages(pending)

    def __next__(self):
        if not self.buffer:
            deadline = time.monotonic() + self.timeout / 1000
            while True:
                self.buffer.extend(
                    self.broker._fetch_messages(
                        self.queue_name, self.prefetch, self.consumer_id
                    )
                )
                if self.buffer:
                    break
                if time.monotonic() >= deadline:
                    return None
                time.sleep(self.broker.poll_interval)

        return self.buffer.popleft()


class _PostgresMessage(MessageProxy):
    def __init__(self, message: Message, db_id: int, consumer_id: str) -> None:
        super().__init__(message)
        self.db_id = db_id
        self.consumer_id = consumer_id
