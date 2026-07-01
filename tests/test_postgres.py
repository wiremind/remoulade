import json
import logging
import os
import threading
import time
from unittest.mock import Mock

import pytest
from pydantic import BaseModel
from sqlalchemy import JSON, Column, Integer, MetaData, Table, func
from sqlalchemy.sql import select, text

import remoulade
from remoulade import Message, QueueJoinTimeout, UnsupportedMessageEncoding, Worker, group
from remoulade.brokers.postgres import PostgresBroker, _PostgresListener
from remoulade.encoder import Encoder, MessageData
from remoulade.results import Results
from remoulade.results.backends import StubBackend

TEST_POSTGRES_URL = os.getenv("REMOULADE_TEST_DB_URL") or "postgresql://remoulade@localhost:5544/test"


def _count_messages(broker, queue_name="default"):
    queue_table = Table(f"q_{queue_name}", MetaData(), Column("msg_id", Integer), schema="pgmq")
    with broker.client.session() as session:
        return session.execute(select(func.count()).select_from(queue_table)).scalar_one()


def _first_payload(broker, queue_name="default"):
    queue_table = Table(
        f"q_{queue_name}",
        MetaData(),
        Column("msg_id", Integer),
        Column("message", JSON),
        schema="pgmq",
    )
    with broker.client.session() as session:
        row = session.execute(select(queue_table.c.message).order_by(queue_table.c.msg_id).limit(1)).one()
    return row[0]


def _count_archived_messages(broker, queue_name="default"):
    archive_table = Table(f"a_{queue_name}", MetaData(), Column("msg_id", Integer), schema="pgmq")
    with broker.client.session() as session:
        return session.execute(select(func.count()).select_from(archive_table)).scalar_one()


def _queue_exists(broker, queue_name):
    with broker.client.session() as session:
        query = text("SELECT EXISTS(SELECT 1 FROM pgmq.list_queues() WHERE queue_name = :queue_name)")
        return session.execute(query, {"queue_name": queue_name}).scalar_one()


def _expected_payload(message):
    return json.loads(message.encode_in_bytes().decode("utf-8"))


class _StubTransaction:
    """Stand-in for ``engine.begin()`` that yields a controllable connection.

    ``declare_queue`` always opens its own transaction and runs queue
    operations against that connection, so tests inject a fake connection to
    assert on the ``conn`` argument forwarded to the PGMQ client.
    """

    def __init__(self, connection):
        self._connection = connection

    def __enter__(self):
        return self._connection

    def __exit__(self, exc_type, exc, tb):
        return None


class _FakeListener:
    """Stand-in for the broker's shared LISTEN/NOTIFY listener.

    Lets tests control listener availability deterministically without opening
    a real psycopg connection or starting the dispatch thread.
    """

    def __init__(self, available):
        self.available = available

    def register(self, queue_name, event):
        pass

    def unregister(self, queue_name, event):
        pass

    def close(self):
        pass


def _install_listener(broker, *, available):
    """Replace the broker's shared listener with a controllable fake."""
    listener = _FakeListener(available)
    broker._listener = listener
    return listener


def test_postgres_broker_uses_provided_url():
    broker_url = TEST_POSTGRES_URL
    broker = PostgresBroker(url=broker_url)

    assert broker.url == broker_url


def test_postgres_broker_creates_partitioned_queue_with_default_intervals():
    broker = PostgresBroker(
        url=TEST_POSTGRES_URL,
        middleware=[],
    )
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.create_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=False)

    conn = Mock()
    broker.client.engine.begin = Mock(return_value=_StubTransaction(conn))

    broker.declare_queue("default")

    broker.client.validate_queue_name.assert_called_once_with("default", conn=conn)
    broker.client.create_partitioned_queue.assert_called_once_with(
        "default",
        partition_interval="1 day",
        retention_interval="7 days",
        conn=conn,
    )
    broker.client.enable_notify.assert_called_once_with("default", throttle_interval_ms=250, conn=conn)
    broker.client.create_queue.assert_not_called()


def test_postgres_broker_uses_current_transaction_connection_for_queue_creation():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=False)

    transaction_connection = Mock()

    broker.client.engine.begin = Mock(return_value=_StubTransaction(transaction_connection))

    with broker.tx():
        broker.declare_queue("default")

    broker.client.validate_queue_name.assert_called_once_with("default", conn=transaction_connection)
    broker.client.create_partitioned_queue.assert_called_once_with(
        "default",
        partition_interval="1 day",
        retention_interval="7 days",
        conn=transaction_connection,
    )
    broker.client.enable_notify.assert_called_once_with(
        "default",
        throttle_interval_ms=250,
        conn=transaction_connection,
    )


def test_postgres_broker_enables_notify_on_postgresql_queue_init():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=False)

    conn = Mock()
    broker.client.engine.begin = Mock(return_value=_StubTransaction(conn))

    broker.declare_queue("default")

    broker.client.create_partitioned_queue.assert_called_once_with(
        "default",
        partition_interval="1 day",
        retention_interval="7 days",
        conn=conn,
    )
    broker.client.enable_notify.assert_called_once_with("default", throttle_interval_ms=250, conn=conn)


def test_postgres_broker_enable_infinite_time_partitions_runs_update_and_returns_tables():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    conn = Mock()
    conn.execute.return_value.all.return_value = [("pgmq.q_default",), ("pgmq.a_default",)]
    broker.client.engine.begin = Mock(return_value=_StubTransaction(conn))

    tables = broker.enable_infinite_time_partitions()

    assert tables == ["pgmq.q_default", "pgmq.a_default"]
    sql = str(conn.execute.call_args.args[0])
    assert "infinite_time_partitions = true" in sql
    assert "part_config" in sql


def test_postgres_broker_run_partition_maintenance_calls_run_maintenance_proc():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    conn = Mock()
    broker.client.engine.begin = Mock(return_value=_StubTransaction(conn))

    broker.run_partition_maintenance()

    sql = str(conn.execute.call_args.args[0])
    assert "run_maintenance_proc()" in sql


def test_postgres_broker_does_not_fail_when_enable_notify_raises(caplog):
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock(side_effect=RuntimeError("notify unavailable"))
    broker._queue_exists = Mock(return_value=False)

    broker.declare_queue("default")

    assert "default" in broker.queues
    assert "Failed to enable LISTEN/NOTIFY" in caplog.text


def test_postgres_broker_declare_queue_is_idempotent_when_queue_already_exists():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=True)

    broker.declare_queue("default")

    broker.client.create_partitioned_queue.assert_not_called()
    broker.client.enable_notify.assert_not_called()
    assert "default" in broker.queues


def test_postgres_broker_poll_only_mode_opens_no_listener_and_skips_enable_notify():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], enable_listen_notify=False)
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=False)

    broker.declare_queue("default")

    assert broker._listener is None
    broker.client.enable_notify.assert_not_called()


def test_postgres_broker_poll_only_consumer_never_reports_listener_available():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], enable_listen_notify=False)
    broker.queues["default"] = None

    message = Message(queue_name="default", actor_name="do_work", args=(9,), kwargs={}, options={})
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=9, message=_expected_payload(message))])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumed = next(consumer)

    assert consumed is not None
    assert consumer._listener_available is False
    broker.client.read.assert_not_called()
    broker.client.read_with_poll.assert_called_once()
    consumer.close()


def test_postgres_consumer_rejects_negative_timeout():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], enable_listen_notify=False)
    broker.queues["default"] = None

    with pytest.raises(ValueError, match="timeout must be greater than or equal to 0"):
        broker.consume("default", prefetch=1, timeout=-1)

    broker.close()


def test_postgres_consumer_rejects_prefetch_below_one():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], enable_listen_notify=False)
    broker.queues["default"] = None

    with pytest.raises(ValueError, match="prefetch must be greater than or equal to 1"):
        broker.consume("default", prefetch=0, timeout=200)

    broker.close()


def test_postgres_poll_only_consumer_reads_immediately_when_timeout_is_zero():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], enable_listen_notify=False)
    broker.queues["default"] = None
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=None)

    consumer = broker.consume("default", prefetch=1, timeout=0)

    # A zero timeout is non-blocking: the polling fallback must do a single
    # immediate read and return None at once instead of polling for ~1s.
    assert next(consumer) is None
    broker.client.read.assert_called_once()
    broker.client.read_with_poll.assert_not_called()
    consumer.close()


def test_postgres_broker_shares_a_single_listener_across_consumers():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["first"] = None
    broker.queues["second"] = None
    listener = _install_listener(broker, available=True)
    listener.register = Mock()
    listener.unregister = Mock()

    first = broker.consume("first", prefetch=1, timeout=0)
    second = broker.consume("second", prefetch=1, timeout=0)

    assert first.broker._listener is second.broker._listener
    assert listener.register.call_count == 2
    registered_queues = {call.args[0] for call in listener.register.call_args_list}
    assert registered_queues == {"first", "second"}

    first.close()
    second.close()
    assert listener.unregister.call_count == 2


def test_postgres_broker_forwards_pool_size_to_client():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], pool_size=3)

    assert broker.client.engine.pool.size() == 3


def test_postgres_broker_uses_custom_partition_settings_when_provided():
    broker = PostgresBroker(
        url=TEST_POSTGRES_URL,
        middleware=[],
        archive_partition_interval_in_days=2,
        archive_retention_interval_in_days=14,
    )
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=False)

    conn = Mock()
    broker.client.engine.begin = Mock(return_value=_StubTransaction(conn))

    broker.declare_queue("default")

    broker.client.create_partitioned_queue.assert_called_once_with(
        "default",
        partition_interval="2 days",
        retention_interval="14 days",
        conn=conn,
    )


def test_postgres_broker_rejects_non_json_message_encoders():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    class _MessageWithInvalidJson(Encoder):
        def _encode_in_json(self, data):
            raise TypeError("not json")

        def decode_json(self, data):
            return data

        def encode_in_bytes(self, data: MessageData) -> bytes:
            return b""

        def decode_bytes(self, data: bytes) -> MessageData:
            return {}

    with pytest.raises(UnsupportedMessageEncoding):
        broker._encode_message(_MessageWithInvalidJson())


def test_postgres_broker_rejects_nested_non_json_safe_payloads():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    old_encoder = remoulade.get_encoder()

    class _NestedInvalidJsonEncoder(Encoder):
        def _encode_in_json(self, data):
            return {**data, "options": {"nested": {1}}}

        def decode_json(self, data):
            return data

        def encode_in_bytes(self, data: MessageData) -> bytes:
            return b""

        def decode_bytes(self, data: bytes) -> MessageData:
            return {}

    remoulade.set_encoder(_NestedInvalidJsonEncoder())
    try:
        message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})

        with pytest.raises(UnsupportedMessageEncoding):
            broker._encode_message(message)
    finally:
        remoulade.set_encoder(old_encoder)


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_broker_enqueue_stores_a_standard_remoulade_payload_as_jsonb(postgres_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(1, 2), kwargs={"debug": True}, options={})
    postgres_broker.declare_queue(message.queue_name)

    postgres_broker.enqueue(message)

    assert _count_messages(postgres_broker) == 1
    assert _first_payload(postgres_broker) == _expected_payload(message)


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_broker_uses_native_visibility_delay_without_delay_queue(postgres_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})
    postgres_broker.declare_queue(message.queue_name)

    postgres_broker.enqueue(message, delay=250)

    assert postgres_broker.client.read("default", vt=1) is None

    time.sleep(0.35)
    delayed = postgres_broker.client.read("default", vt=1)

    assert delayed is not None
    assert delayed.message == _expected_payload(message)


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_broker_transactions_commit_and_rollback_messages(postgres_broker):
    @remoulade.actor
    def do_work():
        return 1

    postgres_broker.declare_actor(do_work)

    with postgres_broker.tx():
        do_work.send()

    with pytest.raises(ValueError), postgres_broker.tx():
        do_work.send()
        raise ValueError("rollback")

    assert _count_messages(postgres_broker) == 1


def test_postgres_consumer_uses_notification_path_when_listener_is_available():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    _install_listener(broker, available=True)

    message = Message(queue_name="default", actor_name="do_work", args=(1,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=Mock(msg_id=1, message=payload))
    broker.client.read_with_poll = Mock(return_value=[])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumer._notify_event.wait = Mock(return_value=False)

    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == message.message_id
    broker.client.read.assert_called_once_with("default", vt=30, qty=1)
    broker.client.read_with_poll.assert_not_called()
    consumer._notify_event.wait.assert_not_called()
    assert consumer._listener_available is True
    consumer.close()


def test_postgres_consumer_falls_back_to_polling_when_listener_is_unavailable():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    _install_listener(broker, available=False)

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
        vt=30,
        qty=1,
        max_poll_seconds=1,
        poll_interval_ms=200,
    )
    consumer.close()


def test_postgres_consumer_keeps_listener_path_after_an_empty_cycle():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    _install_listener(broker, available=True)

    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumer._notify_event.wait = Mock(return_value=False)

    consumed = next(consumer)

    assert consumed is None
    assert consumer._listener_available is True
    assert broker.client.read.call_count == 2
    broker.client.read_with_poll.assert_not_called()
    consumer._notify_event.wait.assert_called_once_with(0.2)
    consumer.close()


def test_postgres_consumer_uses_broker_visibility_timeout_for_reads():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[], visibility_timeout_ms=17_000)
    broker.queues["default"] = None
    _install_listener(broker, available=False)

    message = Message(queue_name="default", actor_name="do_work", args=(5,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=5, message=payload)])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumed = next(consumer)

    assert consumed is not None
    broker.client.read_with_poll.assert_called_once_with(
        "default",
        vt=17,
        qty=1,
        max_poll_seconds=1,
        poll_interval_ms=200,
    )
    consumer.close()


def test_postgres_consumer_tracks_all_prefetched_messages_for_heartbeat(monkeypatch):
    def _fake_start_heartbeat(self):
        self._heartbeat_thread = None

    monkeypatch.setattr("remoulade.brokers.postgres._PostgresConsumer._start_heartbeat", _fake_start_heartbeat)

    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    _install_listener(broker, available=False)

    first_message = Message(queue_name="default", actor_name="do_work", args=(1,), kwargs={}, options={})
    second_message = Message(queue_name="default", actor_name="do_work", args=(2,), kwargs={}, options={})
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(
        return_value=[
            Mock(msg_id=1, message=_expected_payload(first_message)),
            Mock(msg_id=2, message=_expected_payload(second_message)),
        ]
    )
    broker.client.set_vt = Mock()

    consumer = broker.consume("default", prefetch=2, timeout=200)
    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == first_message.message_id
    with consumer._heartbeat_message_ids_lock:
        assert consumer._heartbeat_message_ids == {1, 2}

    consumer.close()


def test_postgres_consumer_close_requeues_buffered_prefetched_messages(monkeypatch):
    def _fake_start_heartbeat(self):
        self._heartbeat_thread = None

    monkeypatch.setattr("remoulade.brokers.postgres._PostgresConsumer._start_heartbeat", _fake_start_heartbeat)

    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    _install_listener(broker, available=False)

    first_message = Message(queue_name="default", actor_name="do_work", args=(1,), kwargs={}, options={})
    second_message = Message(queue_name="default", actor_name="do_work", args=(2,), kwargs={}, options={})
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(
        return_value=[
            Mock(msg_id=1, message=_expected_payload(first_message)),
            Mock(msg_id=2, message=_expected_payload(second_message)),
        ]
    )
    broker.client.set_vt = Mock()

    consumer = broker.consume("default", prefetch=2, timeout=200)
    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == first_message.message_id

    consumer.close()

    broker.client.set_vt.assert_called_once_with("default", [2], 0)


def test_postgres_consumer_falls_back_to_polling_when_listener_stops_during_wait():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    listener = _install_listener(broker, available=True)

    message = Message(queue_name="default", actor_name="do_work", args=(3,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=3, message=payload)])

    consumer = broker.consume("default", prefetch=1, timeout=200)

    def _wait_and_stop(_timeout):
        listener.available = False
        return False

    consumer._notify_event.wait = Mock(side_effect=_wait_and_stop)

    consumed = next(consumer)

    assert consumed is not None
    assert consumed.message_id == message.message_id
    broker.client.read.assert_called_once_with("default", vt=30, qty=1)
    broker.client.read_with_poll.assert_called_once_with(
        "default",
        vt=30,
        qty=1,
        max_poll_seconds=1,
        poll_interval_ms=200,
    )
    consumer.close()


def test_postgres_consumer_heartbeat_extends_inflight_message_visibility():
    broker = PostgresBroker(
        url=TEST_POSTGRES_URL,
        middleware=[],
        visibility_timeout_ms=2_000,
        heartbeat_interval_ms=50,
    )
    broker.queues["default"] = None
    _install_listener(broker, available=False)

    message = Message(queue_name="default", actor_name="do_work", args=(7,), kwargs={}, options={})
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=7, message=payload)])
    broker.client.set_vt = Mock()
    broker.client.archive = Mock()

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumed = next(consumer)
    assert consumed is not None

    deadline = time.monotonic() + 1.0
    while broker.client.set_vt.call_count == 0 and time.monotonic() < deadline:
        time.sleep(0.01)

    assert broker.client.set_vt.call_count >= 1
    queue_name, msg_ids, vt = broker.client.set_vt.call_args.args
    assert queue_name == "default"
    assert msg_ids == [7]
    assert vt == 2

    consumer.ack(consumed)
    consumer.close()


def test_postgres_consumer_decodes_payload_with_global_encoder(pydantic_encoder):
    class InputSchema(BaseModel):
        value: int

    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    remoulade.set_broker(broker)
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker._queue_exists = Mock(return_value=False)
    _install_listener(broker, available=False)

    @remoulade.actor(actor_name="typed.actor", queue_name="default")
    def typed_actor(payload: InputSchema):
        return payload.value

    broker.declare_actor(typed_actor)

    message = Message(
        queue_name="default",
        actor_name="typed.actor",
        args=(InputSchema(value=42),),
        kwargs={},
        options={},
    )
    payload = _expected_payload(message)
    broker.client.read = Mock(return_value=None)
    broker.client.read_with_poll = Mock(return_value=[Mock(msg_id=1, message=payload)])

    consumer = broker.consume("default", prefetch=1, timeout=200)
    consumed = next(consumer)

    assert consumed is not None
    assert isinstance(consumed.args[0], InputSchema)
    assert consumed.args[0].value == 42
    consumer.close()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_consumer_reads_messages_and_acks_with_delete(postgres_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(42,), kwargs={}, options={})
    postgres_broker.declare_queue(message.queue_name)
    postgres_broker.enqueue(message)

    consumer = postgres_broker.consume("default", prefetch=2, timeout=200)
    consumed_message = next(consumer)
    assert consumed_message is not None
    assert consumed_message.message_id == message.message_id

    consumer.ack(consumed_message)
    consumer.close()

    assert _count_messages(postgres_broker) == 0


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_consumer_nack_archives_messages(postgres_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})
    postgres_broker.declare_queue(message.queue_name)
    postgres_broker.enqueue(message)

    consumer = postgres_broker.consume("default", prefetch=1, timeout=200)
    consumed_message = next(consumer)

    assert consumed_message is not None
    consumer.nack(consumed_message)
    consumer.close()

    assert _count_messages(postgres_broker) == 0
    assert _count_archived_messages(postgres_broker) == 1


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_consumer_requeue_restores_visibility_with_set_vt(postgres_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(), kwargs={}, options={})
    postgres_broker.declare_queue(message.queue_name)
    postgres_broker.enqueue(message)

    consumer = postgres_broker.consume("default", prefetch=1, timeout=200)
    consumed_message = next(consumer)

    assert consumed_message is not None
    consumer.requeue([consumed_message])

    replayed_message = next(consumer)
    assert replayed_message is not None
    assert replayed_message.message_id == message.message_id

    consumer.ack(replayed_message)
    consumer.close()

    assert _count_messages(postgres_broker) == 0


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_worker_processes_native_delayed_messages_without_delay_queue(postgres_broker):
    seen = []

    @remoulade.actor
    def do_work(value):
        seen.append(value)

    postgres_broker.declare_actor(do_work)
    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=2)
    worker.start()
    try:
        do_work.send_with_options(args=(3,), delay=150)
        postgres_broker.join(do_work.queue_name, timeout=10_000)
        assert seen == [3]
        worker.join()
    finally:
        worker.stop()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_broker_join_times_out_while_processing_invisible_message(postgres_broker):
    started = threading.Event()
    release = threading.Event()

    @remoulade.actor
    def do_work():
        started.set()
        release.wait(timeout=5)

    postgres_broker.declare_actor(do_work)

    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        do_work.send()
        assert started.wait(timeout=2)

        with pytest.raises(QueueJoinTimeout):
            postgres_broker.join(do_work.queue_name, timeout=100)

        release.set()
        postgres_broker.join(do_work.queue_name, timeout=5_000)
        worker.join()
    finally:
        release.set()
        worker.stop()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_worker_processes_a_two_actor_pipeline(postgres_broker):
    seen: list[tuple[str, int]] = []

    @remoulade.actor
    def first_actor(value):
        seen.append(("first", value))
        return value + 1

    @remoulade.actor
    def second_actor(value):
        seen.append(("second", value))

    postgres_broker.declare_actor(first_actor)
    postgres_broker.declare_actor(second_actor)

    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        remoulade.pipeline([first_actor.message(1), second_actor.message()]).run()

        postgres_broker.join(second_actor.queue_name, timeout=10_000)
        worker.join()

        assert seen == [("first", 1), ("second", 2)]
    finally:
        worker.stop()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_consumer_listener_wakes_on_enqueue_with_listen_notify(postgres_broker):
    message = Message(queue_name="default", actor_name="do_work", args=(99,), kwargs={}, options={})
    postgres_broker.declare_queue(message.queue_name)
    consumer = postgres_broker.consume(message.queue_name, prefetch=1, timeout=1500)

    if not consumer._listener_available:
        pytest.skip("LISTEN/NOTIFY listener unavailable in this environment.")

    consumed_messages = []

    def _consume_once():
        consumed_messages.append(next(consumer))

    thread = threading.Thread(target=_consume_once)
    thread.start()
    try:
        time.sleep(0.15)
        postgres_broker.enqueue(message)
        thread.join(timeout=3)
        assert not thread.is_alive()
        assert consumed_messages
        consumed = consumed_messages[0]
        assert consumed is not None
        assert consumed.message_id == message.message_id
        consumer.ack(consumed)
    finally:
        consumer.close()


class _FakeConnection:
    """Minimal psycopg connection stand-in for listener reconnection tests."""

    def __init__(self, *, raise_on_notify=False, on_notify=None):
        self.raise_on_notify = raise_on_notify
        self.on_notify = on_notify
        self.listened = []
        self.closed = False

    def execute(self, statement):
        self.listened.append(statement)

    def notifies(self, timeout=0.5, stop_after=1):
        if self.raise_on_notify:
            self.raise_on_notify = False
            raise RuntimeError("connection lost")
        if self.on_notify is not None:
            self.on_notify()
        return []

    def close(self):
        self.closed = True


def test_listener_open_connection_relistens_every_registered_channel(monkeypatch):
    listener = _PostgresListener("postgresql://localhost/test", logging.getLogger("test"))
    listener._channel_to_queue = {
        listener._channel_for("first"): "first",
        listener._channel_for("second"): "second",
    }
    connection = _FakeConnection()
    monkeypatch.setattr("remoulade.brokers.postgres.psycopg.connect", Mock(return_value=connection))

    assert listener._open_connection() is True
    assert listener.available is True
    assert listener._connection is connection
    assert len(connection.listened) == 2


def test_listener_open_connection_failure_keeps_listener_unavailable(monkeypatch, caplog):
    listener = _PostgresListener("postgresql://localhost/test", logging.getLogger("test"))
    monkeypatch.setattr("remoulade.brokers.postgres.psycopg.connect", Mock(side_effect=OSError("database is down")))

    with caplog.at_level(logging.WARNING):
        assert listener._open_connection() is False

    assert listener.available is False
    assert listener._connection is None
    assert "Failed to open shared LISTEN/NOTIFY connection" in caplog.text


def test_listener_reconnects_after_a_connection_drop(monkeypatch):
    monkeypatch.setattr("remoulade.brokers.postgres.LISTENER_RECONNECT_BACKOFF_MIN_S", 0.01)
    monkeypatch.setattr("remoulade.brokers.postgres.LISTENER_RECONNECT_BACKOFF_MAX_S", 0.01)

    recovered = threading.Event()
    healthy_connection = _FakeConnection(on_notify=recovered.set)
    connections = [_FakeConnection(raise_on_notify=True), healthy_connection]
    connect = Mock(side_effect=connections)
    monkeypatch.setattr("remoulade.brokers.postgres.psycopg.connect", connect)

    listener = _PostgresListener("postgresql://localhost/test", logging.getLogger("test"))
    listener.register("default", threading.Event())
    try:
        assert recovered.wait(timeout=2)
        assert listener.available is True
        assert connect.call_count >= 2
        assert connections[0].closed is True
        assert len(healthy_connection.listened) == 1
    finally:
        listener.close()


def test_listener_recovers_when_initial_connection_fails(monkeypatch):
    monkeypatch.setattr("remoulade.brokers.postgres.LISTENER_RECONNECT_BACKOFF_MIN_S", 0.01)
    monkeypatch.setattr("remoulade.brokers.postgres.LISTENER_RECONNECT_BACKOFF_MAX_S", 0.01)

    recovered = threading.Event()
    healthy_connection = _FakeConnection(on_notify=recovered.set)
    connect = Mock(side_effect=[OSError("database is down"), healthy_connection])
    monkeypatch.setattr("remoulade.brokers.postgres.psycopg.connect", connect)

    listener = _PostgresListener("postgresql://localhost/test", logging.getLogger("test"))
    listener.register("default", threading.Event())
    try:
        assert recovered.wait(timeout=2)
        assert listener.available is True
        assert connect.call_count >= 2
    finally:
        listener.close()


def test_listener_close_stops_the_dispatch_thread(monkeypatch):
    connection = _FakeConnection()
    monkeypatch.setattr("remoulade.brokers.postgres.psycopg.connect", Mock(return_value=connection))

    listener = _PostgresListener("postgresql://localhost/test", logging.getLogger("test"))
    listener.register("default", threading.Event())

    listener.close()

    assert listener.available is False
    assert connection.closed is True
    assert listener._thread is not None
    assert listener._thread.is_alive() is False


# End-to-end tests running a real Worker against PGMQ, exercising the
# middleware-driven behaviours (retries, results, groups) that the unit tests
# above stub out. These validate the broker's ack/nack/requeue/heartbeat
# interplay through the full message lifecycle, not just in isolation.


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_worker_retries_failed_message_then_succeeds(postgres_broker):
    attempts = []

    @remoulade.actor(max_retries=3, min_backoff=50, max_backoff=50, jitter=False)
    def flaky():
        attempts.append(1)
        if len(attempts) < 2:
            raise RuntimeError("boom")

    postgres_broker.declare_actor(flaky)

    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        flaky.send()
        postgres_broker.join(flaky.queue_name, timeout=10_000)
        worker.join()

        # The actor ran twice: the failed attempt, then the retry that succeeded.
        assert len(attempts) == 2
        # The failed attempt is archived (acked) and a delayed retry enqueued;
        # the successful retry is archived too. Nothing is left in the queue.
        assert _count_messages(postgres_broker) == 0
        assert _count_archived_messages(postgres_broker) == 2
    finally:
        worker.stop()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_worker_archives_message_when_retries_exhausted(postgres_broker):
    attempts = []

    @remoulade.actor(max_retries=1, min_backoff=50, max_backoff=50, jitter=False)
    def always_fails():
        attempts.append(1)
        raise RuntimeError("boom")

    postgres_broker.declare_actor(always_fails)

    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        always_fails.send()
        postgres_broker.join(always_fails.queue_name, timeout=10_000)
        worker.join()

        # max_retries=1 -> one initial attempt plus one retry, then it is failed.
        assert len(attempts) == 2
        # Both the acked retry-source and the nacked exhausted message end up
        # archived; an empty queue proves the nack does not leave the message
        # invisible to be redelivered forever (PostgresBroker has no DLQ).
        assert _count_messages(postgres_broker) == 0
        assert _count_archived_messages(postgres_broker) == 2
    finally:
        worker.stop()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_worker_stores_and_retrieves_actor_result(postgres_broker):
    postgres_broker.add_middleware(Results(backend=StubBackend()))

    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    postgres_broker.declare_actor(do_work)

    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        message = do_work.send()
        postgres_broker.join(do_work.queue_name, timeout=10_000)
        worker.join()

        assert message.result.get(block=True) == 42
    finally:
        worker.stop()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_worker_runs_group_with_results(postgres_broker):
    postgres_broker.add_middleware(Results(backend=StubBackend()))

    @remoulade.actor(store_results=True)
    def square(value):
        return value * value

    postgres_broker.declare_actor(square)

    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=4)
    worker.start()
    try:
        g = group([square.message(value) for value in range(4)])
        g.run()
        postgres_broker.join(square.queue_name, timeout=10_000)
        worker.join()

        assert sorted(g.results.get(block=True)) == [0, 1, 4, 9]
    finally:
        worker.stop()


def test_listener_wakes_all_consumers_on_a_queue_and_unregister_spares_siblings():
    listener = _PostgresListener("postgresql://localhost/test", logging.getLogger("test"))
    # Skip starting the dispatch thread: this test only exercises wake routing.
    listener._started = True

    first, second = threading.Event(), threading.Event()
    listener.register("default", first)
    listener.register("default", second)
    channel = listener._channel_for("default")

    # A notification on the shared queue wakes every registered consumer.
    listener._wake_channel(channel)
    assert first.is_set()
    assert second.is_set()
    first.clear()
    second.clear()

    # One consumer leaving must not stop notifications for its sibling.
    listener.unregister("default", first)
    listener._wake_channel(channel)
    assert not first.is_set()
    assert second.is_set()

    # The channel routing is dropped only once the last consumer leaves.
    listener.unregister("default", second)
    assert channel not in listener._channel_to_queue
    assert "default" not in listener._events
