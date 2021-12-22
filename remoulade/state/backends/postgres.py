import datetime
import os
import sys
import threading
from typing import List, Optional, Type, TypeVar

from pytz import utc
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    LargeBinary,
    SmallInteger,
    String,
    create_engine,
    distinct,
    inspect,
    or_,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.sql.functions import coalesce, count, max, min

from remoulade import Encoder
from remoulade.state import State, StateBackend

Base = declarative_base()

DEFAULT_POSTGRES_URI = "postgresql://remoulade@localhost:5432/remoulade"
DB_VERSION = 3
T = TypeVar("T", bound="StoredState")


class StoredState(Base):

    __tablename__ = "states"

    message_id = Column(String(length=36), primary_key=True, index=True)
    status = Column(String(length=10), index=True)
    actor_name = Column(String(length=79), index=True)
    args = Column(LargeBinary)
    kwargs = Column(LargeBinary)
    options = Column(LargeBinary)
    priority = Column(SmallInteger)
    progress = Column(Float)
    enqueued_datetime = Column(DateTime(timezone=True), index=True)
    started_datetime = Column(DateTime(timezone=True), index=True)
    end_datetime = Column(DateTime(timezone=True), index=True)
    queue_name = Column(String(length=60))
    composition_id = Column(String)

    def as_state(self, encoder: Encoder) -> State:
        state_dict = {}
        mapper = inspect(StoredState)
        for column in mapper.attrs:
            column_value = getattr(self, column.key)
            if column_value is None:
                continue
            if column.key in ["args", "kwargs", "options"]:
                column_value = encoder.decode(column_value)
            state_dict[column.key] = column_value
        return State.from_dict(state_dict)

    @classmethod
    def from_state(cls: Type[T], state: State, max_size: int, encoder: Encoder) -> T:
        state_dict = state.as_dict()
        for key in ["args", "kwargs", "options"]:
            if key in state_dict:
                encoded_value = encoder.encode(state_dict[key])
                state_dict[key] = encoded_value if sys.getsizeof(encoded_value) <= max_size else None
        return cls(**state_dict)


class StateVersion(Base):

    __tablename__ = "version"

    version = Column(SmallInteger, primary_key=True)


def filter_query(
    *,
    query,
    selected_actors: Optional[List[str]],
    selected_statuses: Optional[List[str]],
    selected_message_ids: Optional[List[str]],
    selected_composition_ids: Optional[List[str]],
    start_datetime: Optional[datetime.datetime],
    end_datetime: Optional[datetime.datetime],
):
    if selected_actors is not None:
        query = query.filter(StoredState.actor_name.in_(selected_actors))
    if selected_statuses is not None:
        query = query.filter(StoredState.status.in_(selected_statuses))
    if selected_message_ids is not None:
        query = query.filter(StoredState.message_id.in_(selected_message_ids))
    if selected_composition_ids is not None:
        query = query.filter(StoredState.composition_id.in_(selected_composition_ids))
    if start_datetime is not None:
        query = query.filter(StoredState.enqueued_datetime >= start_datetime)
    if end_datetime is not None:
        query = query.filter(StoredState.enqueued_datetime <= end_datetime)

    return query


class PostgresBackend(StateBackend):
    def __init__(
        self,
        *,
        namespace: str = "remoulade-state",
        encoder: Optional[Encoder] = None,
        client: Optional[sessionmaker] = None,
        max_size: int = 2000000,
        url: Optional[str] = None,
    ):
        self.url = url or os.getenv("REMOULADE_POSTGRESQL_URL") or DEFAULT_POSTGRES_URI
        super().__init__(namespace=namespace, encoder=encoder)
        self.client = client or sessionmaker(create_engine(self.url))
        self.init_db()
        self.max_size = max_size
        self.lock = threading.Lock()

    def init_db(self):
        with self.client.begin() as session:
            bind = session.get_bind()
            insp = inspect(bind)

            if not insp.has_table("version"):
                Base.metadata.create_all(bind=bind, tables=[StateVersion.__table__])

            state_version = session.query(StateVersion).first()
            if state_version is None:
                session.add(StateVersion(version=DB_VERSION))

            if not insp.has_table("states"):
                Base.metadata.create_all(bind=bind, tables=[StoredState.__table__])
            elif state_version is None or state_version.version != DB_VERSION:
                StoredState.__table__.drop(bind=bind)
                Base.metadata.create_all(bind=bind, tables=[StoredState.__table__])
                if state_version is not None:
                    state_version.version = DB_VERSION

    def get_state(self, message_id: str):
        with self.client.begin() as session:
            state = session.query(StoredState).filter_by(message_id=message_id).first()
            if state is None:
                return None
            return state.as_state(self.encoder)

    def set_state(self, state: State, ttl=3600):
        with self.lock:
            with self.client.begin() as session:
                session.merge(StoredState.from_state(state, self.max_size, self.encoder))

    def get_states(
        self,
        *,
        size: Optional[int] = None,
        offset: int = 0,
        selected_actors: Optional[List[str]] = None,
        selected_statuses: Optional[List[str]] = None,
        selected_message_ids: Optional[List[str]] = None,
        selected_composition_ids: Optional[List[str]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        sort_column: Optional[str] = None,
        sort_direction: Optional[str] = None,
    ):
        sort_column = sort_column or "enqueued_datetime"
        sort_direction = sort_direction or "desc"

        with self.client.begin() as session:
            query = session.query(StoredState)
            query = filter_query(
                query=query,
                selected_actors=selected_actors,
                selected_statuses=selected_statuses,
                selected_message_ids=selected_message_ids,
                selected_composition_ids=selected_composition_ids,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )
            if size is not None:
                query = query.subquery()
                query_group = (
                    session.query(
                        max(query.c.composition_id).label("grouped_composition_id"),
                        max(query.c.message_id).label("grouped_message_id"),
                        max(query.c.status).label("grouped_status"),
                        max(query.c.actor_name).label("grouped_actor_name"),
                        max(query.c.priority).label("grouped_priority"),
                        func.avg(query.c.progress).label("grouped_progress"),
                        min(query.c.enqueued_datetime).label("grouped_enqueued_datetime"),
                        min(query.c.started_datetime).label("grouped_started_datetime"),
                        max(query.c.end_datetime).label("grouped_end_datetime"),
                        max(query.c.queue_name).label("grouped_queue_name"),
                    )
                    .group_by(coalesce(query.c.composition_id, query.c.message_id))
                    .order_by(text(f"grouped_{sort_column} {sort_direction}"))
                )
                query_group = query_group.offset(offset).limit(size).subquery()
                query = (
                    session.query(StoredState)
                    .select_from(StoredState)
                    .join(
                        query_group,
                        or_(
                            StoredState.message_id == query_group.c.grouped_message_id,
                            StoredState.composition_id == query_group.c.grouped_composition_id,
                        ),
                    )
                )
            query = query.order_by(text(f"{sort_column} {sort_direction}"))
            return [state_model.as_state(self.encoder) for state_model in query]

    def get_states_count(
        self,
        *,
        selected_actors: Optional[List[str]] = None,
        selected_statuses: Optional[List[str]] = None,
        selected_messages_ids: Optional[List[str]] = None,
        selected_composition_ids: Optional[List[str]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        **kwargs,
    ):
        with self.client.begin() as session:
            query = session.query(count(distinct(coalesce(StoredState.composition_id, StoredState.message_id))))

            query = filter_query(
                query=query,
                selected_actors=selected_actors,
                selected_statuses=selected_statuses,
                selected_message_ids=selected_messages_ids,
                selected_composition_ids=selected_composition_ids,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )
            return query.first()[0]

    def clean(self, max_age: Optional[int] = None, not_started: bool = False):
        with self.client.begin() as session:
            query = session.query(StoredState)
            if max_age:
                now = datetime.datetime.now(utc)
                min_datetime = now - datetime.timedelta(minutes=max_age)
                query = session.query(StoredState).filter(StoredState.end_datetime < min_datetime)
            if not_started:
                query = session.query(StoredState).filter(StoredState.started_datetime.is_(None))
            query.delete()
