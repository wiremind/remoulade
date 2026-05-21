import json
import threading
import time
from unittest.mock import Mock

import pytest
from sqlalchemy import JSON, Column, Integer, MetaData, Table, func
from sqlalchemy.sql import select, text

import remoulade
from remoulade import Message, Worker
from remoulade.brokers.pgmq import PgmqBroker


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


def _count_archived_messages(broker, queue_name="default"):
    archive_table = Table(f"a_{queue_name}", MetaData(), Column("msg_id", Integer), schema="pgmq")
    with broker.sessionmaker.begin() as session:
        return session.execute(select(func.count()).select_from(archive_table)).scalar_one()


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


def test_pgmq_broker_enables_notify_on_postgresql_queue_init():
    broker = PgmqBroker(url="postgresql://pgmq-user@localhost/pgmq", middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_queue = Mock()
    broker.client.enable_notify = Mock()

    broker.declare_queue("default")

    broker.client.enable_notify.assert_called_once_with("default", throttle_interval_ms=250, conn=None)


def test_pgmq_broker_does_not_fail_when_enable_notify_raises(caplog):
    broker = PgmqBroker(url="postgresql://pgmq-user@localhost/pgmq", middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_queue = Mock()
    broker.client.enable_notify = Mock(side_effect=RuntimeError("notify unavailable"))

    broker.declare_queue("default")

    assert "default" in broker.queues
    assert "Failed to enable LISTEN/NOTIFY" in caplog.text


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_broker_enqueue_stores_a_standard_remoulade_payload_as_jsonb(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(1, 2), kwargs={"debug": True}, options={})
    pgmq_broker.declare_queue(message.queue_name)

    pgmq_broker.enqueue(message)

    assert _count_messages(pgmq_broker) == 1
    assert _first_payload(pgmq_broker) == _expected_payload(message)


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


def test_pgmq_consumer_uses_notification_path_when_listener_is_available(monkeypatch):
    def _fake_start_listener(self):
        self._listener_available = True

    monkeypatch.setattr("remoulade.brokers.pgmq._PgmqConsumer._start_listener", _fake_start_listener)

    broker = PgmqBroker(url="postgresql://pgmq-user@localhost/pgmq", middleware=[])
    broker.queues["default"] = None

    message = Message(queue_name="default", actor_name="do_work", args=(1,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(side_effect=[None, Mock(msg_id=1, message=payload)])
    broker.client.read_with_poll = Mock(return_value=[])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumer._notify_event.set()

    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == message.message_id
    assert broker.client.read.call_count == 2
    broker.client.read_with_poll.assert_not_called()
    consumer.close()


def test_pgmq_consumer_falls_back_to_polling_when_listener_is_unavailable(monkeypatch):
    def _fake_start_listener(self):
        self._listener_available = False

    monkeypatch.setattr("remoulade.brokers.pgmq._PgmqConsumer._start_listener", _fake_start_listener)

    broker = PgmqBroker(url="postgresql://pgmq-user@localhost/pgmq", middleware=[])
    broker.queues["default"] = None

    message = Message(queue_name="default", actor_name="do_work", args=(2,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=2, message=payload)])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == message.message_id
    broker.client.read.assert_not_called()
    broker.client.read_with_poll.assert_called_once_with(
        "default",
        qty=1,
        max_poll_seconds=1,
        poll_interval_ms=200,
    )
    consumer.close()


def test_pgmq_consumer_falls_back_to_polling_when_listener_stops_during_wait(monkeypatch):
    def _fake_start_listener(self):
        self._listener_available = True

    monkeypatch.setattr("remoulade.brokers.pgmq._PgmqConsumer._start_listener", _fake_start_listener)

    broker = PgmqBroker(url="postgresql://pgmq-user@localhost/pgmq", middleware=[])
    broker.queues["default"] = None

    message = Message(queue_name="default", actor_name="do_work", args=(3,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=3, message=payload)])

    consumer = broker.consume("default", prefetch=1, timeout=200)

    def _wait_and_stop(_timeout):
        consumer._listener_available = False
        return False

    consumer._notify_event.wait = Mock(side_effect=_wait_and_stop)

    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == message.message_id
    broker.client.read.assert_called_once_with("default", qty=1)
    broker.client.read_with_poll.assert_called_once_with(
        "default",
        qty=1,
        max_poll_seconds=1,
        poll_interval_ms=200,
    )
    consumer.close()


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_consumer_reads_messages_and_acks_with_delete(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(42,), kwargs={}, options={})
    pgmq_broker.declare_queue(message.queue_name)
    pgmq_broker.enqueue(message)

    consumer = pgmq_broker.consume("default", prefetch=2, timeout=200)
    consumed_message = next(consumer)
    assert consumed_message is not None
    assert consumed_message.message_id == message.message_id

    consumer.ack(consumed_message)
    consumer.close()

    assert _count_messages(pgmq_broker) == 0


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_consumer_nack_archives_messages(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})
    pgmq_broker.declare_queue(message.queue_name)
    pgmq_broker.enqueue(message)

    consumer = pgmq_broker.consume("default", prefetch=1, timeout=200)
    consumed_message = next(consumer)

    assert consumed_message is not None
    consumer.nack(consumed_message)
    consumer.close()

    assert _count_messages(pgmq_broker) == 0
    assert _count_archived_messages(pgmq_broker) == 1


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_consumer_requeue_restores_visibility_with_set_vt(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})
    pgmq_broker.declare_queue(message.queue_name)
    pgmq_broker.enqueue(message)

    consumer = pgmq_broker.consume("default", prefetch=1, timeout=200)
    consumed_message = next(consumer)

    assert consumed_message is not None
    consumer.requeue([consumed_message])

    replayed_message = next(consumer)
    assert replayed_message is not None
    assert replayed_message.message_id == message.message_id

    consumer.ack(replayed_message)
    consumer.close()

    assert _count_messages(pgmq_broker) == 0


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_worker_processes_native_delayed_messages_without_delay_queue(pgmq_broker):
    seen = []

    @remoulade.actor
    def do_work(value):
        seen.append(value)

    pgmq_broker.declare_actor(do_work)
    worker = Worker(pgmq_broker, worker_timeout=100, worker_threads=2)
    worker.start()
    try:
        do_work.send_with_options(args=(3,), delay=150)
        deadline = time.monotonic() + 10
        while not seen and time.monotonic() < deadline:
            time.sleep(0.05)
        assert seen == [3]
        worker.join()
    finally:
        worker.stop()

    assert not _queue_exists(pgmq_broker, "default.DQ")


@pytest.mark.usefixtures("pgmq_broker")
def test_pgmq_consumer_listener_wakes_on_enqueue_with_listen_notify(pgmq_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(99,), kwargs={}, options={})
    pgmq_broker.declare_queue(message.queue_name)
    consumer = pgmq_broker.consume(message.queue_name, prefetch=1, timeout=1500)

    if not consumer._listener_available:
        pytest.skip("LISTEN/NOTIFY listener unavailable in this environment.")

    consumed_messages = []

    def _consume_once():
        consumed_messages.append(next(consumer))

    thread = threading.Thread(target=_consume_once)
    thread.start()
    try:
        time.sleep(0.15)
        pgmq_broker.enqueue(message)
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert consumed_messages
        consumed = consumed_messages[0]
        assert consumed is not None
        assert consumed.message_id == message.message_id
        consumer.ack(consumed)
    finally:
        consumer.close()
