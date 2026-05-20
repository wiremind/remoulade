import json
import time
from unittest.mock import Mock

import pytest
from sqlalchemy import JSON, Column, Integer, MetaData, Table, func
from sqlalchemy.sql import select, text

import remoulade
from remoulade import Message, Middleware, UnsupportedMessageEncoding
from remoulade.brokers.pgmq import PgmqBroker


class RecorderMiddleware(Middleware):
    def __init__(self):
        self.before_messages = []
        self.after_messages = []

    def before_enqueue(self, broker, message, delay):
        self.before_messages.append((message, delay))

    def after_enqueue(self, broker, message, delay, exception=None):
        self.after_messages.append((message, delay, exception))


@pytest.fixture
def sqlite_pgmq_broker():
    broker = PgmqBroker(url="sqlite://", middleware=[])
    remoulade.set_broker(broker)
    yield broker
    broker.close()


def _count_messages(broker, queue_name="default"):
    queue_table = Table(f"q_{queue_name}", MetaData(), Column("msg_id", Integer), schema="pgmq")
    with broker.sessionmaker.begin() as session:
        return session.execute(select(func.count()).select_from(queue_table)).scalar_one()


def _first_payload(broker, queue_name="default"):
    queue_table = Table(
        f"q_{queue_name}",
        MetaData(),
        Column("msg_id", Integer),
        Column("message", JSON),
        schema="pgmq",
    )
    with broker.sessionmaker.begin() as session:
        row = session.execute(select(queue_table.c.message).order_by(queue_table.c.msg_id).limit(1)).one()
    return row[0]


def _queue_exists(broker, queue_name):
    with broker.sessionmaker.begin() as session:
        query = text("SELECT EXISTS(SELECT 1 FROM pgmq.list_queues() WHERE queue_name = :queue_name)")
        return session.execute(query, {"queue_name": queue_name}).scalar_one()


def _expected_payload(message):
    return json.loads(message.encode().decode("utf-8"))


def test_pgmq_broker_uses_provided_url():
    broker_url = "postgresql://pgmq-user@localhost/pgmq"
    broker = PgmqBroker(url=broker_url)

    assert broker.url == broker_url


def test_pgmq_broker_partitions_archive_table_on_postgresql_queue_init():
    broker = PgmqBroker(
        url="postgresql://pgmq-user@localhost/pgmq",
        middleware=[],
        partition_archive_on_queue_init=True,
    )
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.create_queue = Mock()

    broker.declare_queue("default")

    broker.client.create_partitioned_queue.assert_called_once_with(
        "default",
        partition_interval="1 day",
        retention_interval="7 days",
        conn=None,
    )
    broker.client.create_queue.assert_not_called()


def test_pgmq_broker_uses_non_partitioned_queue_creation_when_disabled():
    broker = PgmqBroker(
        url="postgresql://pgmq-user@localhost/pgmq",
        middleware=[],
        partition_archive_on_queue_init=False,
    )
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.create_queue = Mock()

    broker.declare_queue("default")

    broker.client.create_queue.assert_called_once_with("default", conn=None)
    broker.client.create_partitioned_queue.assert_not_called()


def test_pgmq_broker_uses_non_partitioned_queue_creation_on_non_postgresql():
    broker = PgmqBroker(url="sqlite://", middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.create_queue = Mock()

    broker.declare_queue("default")

    broker.client.create_queue.assert_called_once_with("default", conn=None)
    broker.client.create_partitioned_queue.assert_not_called()


def test_pgmq_broker_declares_queue_idempotently(sqlite_pgmq_broker):
    sqlite_pgmq_broker.client.validate_queue_name = Mock()
    sqlite_pgmq_broker.client.create_queue = Mock()
    sqlite_pgmq_broker.declare_queue("default")
    sqlite_pgmq_broker.declare_queue("default")

    assert sqlite_pgmq_broker.get_declared_queues() == {"default"}
    assert sqlite_pgmq_broker.get_declared_delay_queues() == set()
    assert sqlite_pgmq_broker.client.validate_queue_name.call_count == 1
    assert sqlite_pgmq_broker.client.create_queue.call_count == 1


def test_pgmq_broker_rejects_too_long_queue_names(sqlite_pgmq_broker):
    sqlite_pgmq_broker.client.validate_queue_name = Mock(side_effect=ValueError("queue name too long"))

    with pytest.raises(ValueError):
        sqlite_pgmq_broker.declare_queue("q" * 49)


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_broker_enqueue_stores_a_standard_remoulade_payload_as_jsonb(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(1, 2), kwargs={"debug": True}, options={})
    pgmq_broker.declare_queue(message.queue_name)

    pgmq_broker.enqueue(message)

    assert _count_messages(pgmq_broker) == 1
    assert _first_payload(pgmq_broker) == _expected_payload(message)


def test_pgmq_broker_rejects_non_json_encoders(sqlite_pgmq_broker, pickle_encoder):
    message = Message(queue_name="default", actor_name="do_work", args=(1,), kwargs={}, options={})
    sqlite_pgmq_broker.client.validate_queue_name = Mock()
    sqlite_pgmq_broker.client.create_queue = Mock()
    sqlite_pgmq_broker.declare_queue(message.queue_name)
    sqlite_pgmq_broker.client.send = Mock()

    with pytest.raises(UnsupportedMessageEncoding):
        sqlite_pgmq_broker.enqueue(message)

    sqlite_pgmq_broker.client.send.assert_not_called()


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_broker_uses_native_visibility_delay_without_delay_queue(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})
    pgmq_broker.declare_queue(message.queue_name)

    pgmq_broker.enqueue(message, delay=250)

    assert pgmq_broker.client.read("default", vt=1) is None
    assert not _queue_exists(pgmq_broker, "default.DQ")

    time.sleep(0.35)
    delayed = pgmq_broker.client.read("default", vt=1)

    assert delayed is not None
    assert delayed.message == _expected_payload(message)


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_broker_transactions_commit_and_rollback_messages(pgmq_broker):
    @remoulade.actor
    def do_work():
        return 1

    pgmq_broker.declare_actor(do_work)

    with pgmq_broker.tx():
        do_work.send()

    with pytest.raises(ValueError), pgmq_broker.tx():
        do_work.send()
        raise ValueError("rollback")

    assert _count_messages(pgmq_broker) == 1


def test_pgmq_broker_flush_purges_messages(sqlite_pgmq_broker):
    sqlite_pgmq_broker.client.validate_queue_name = Mock()
    sqlite_pgmq_broker.client.create_queue = Mock()
    sqlite_pgmq_broker.client.purge = Mock()

    sqlite_pgmq_broker.declare_queue("default")
    sqlite_pgmq_broker.flush("default")

    sqlite_pgmq_broker.client.purge.assert_called_once_with("default", conn=None)


def test_pgmq_broker_middleware_receives_standard_messages(sqlite_pgmq_broker):
    middleware = RecorderMiddleware()
    sqlite_pgmq_broker.client.validate_queue_name = Mock()
    sqlite_pgmq_broker.client.create_queue = Mock()
    sqlite_pgmq_broker.client.send = Mock()
    sqlite_pgmq_broker.add_middleware(middleware)

    @remoulade.actor
    def do_work():
        return 1

    sqlite_pgmq_broker.declare_actor(do_work)
    message = do_work.send()

    assert middleware.before_messages == [(message, None)]
    assert middleware.after_messages == [(message, None, None)]


def test_pgmq_broker_worker_entrypoints_fail_explicitly_until_jalon_two(sqlite_pgmq_broker):
    with pytest.raises(NotImplementedError, match="jalon 2"):
        sqlite_pgmq_broker.consume("default")

    with pytest.raises(NotImplementedError, match="jalon 3"):
        sqlite_pgmq_broker.join("default")
