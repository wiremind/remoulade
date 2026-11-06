"""Tests for the PGMQ client's hand-written SQL (``remoulade.helpers.postgres_client``)."""

import os
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import text

from remoulade.brokers.postgres import PostgresBroker
from remoulade.helpers.postgres_client import assert_valid_queue_name
from remoulade.state import State, StateStatusesEnum
from remoulade.state.backends import PostgresBackend

TEST_POSTGRES_URL = os.getenv("REMOULADE_TEST_DB_URL") or "postgresql://remoulade@localhost:5544/test"


def _index_exists(broker, index_name):
    with broker.client.engine.connect() as connection:
        return connection.execute(
            text("SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE schemaname = 'pgmq' AND indexname = :name)"),
            {"name": index_name},
        ).scalar_one()


def test_postgres_client_create_indexes_emits_every_index():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    conn = Mock()

    broker.client.create_indexes("default", conn)

    executed = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert executed == [
        'CREATE INDEX IF NOT EXISTS "q_default_msg_id_idx" ON pgmq."q_default" (msg_id)',
        'CREATE INDEX IF NOT EXISTS "a_default_msg_id_idx" ON pgmq."a_default" (msg_id)',
    ]


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_client_patch_headers_merges_into_the_existing_headers(postgres_broker):
    postgres_broker.declare_queue("default")
    postgres_broker.client.send("default", {"message_id": "m1"}, headers={"progress": 0.5})

    with postgres_broker.client.engine.connect() as connection:
        msg_id = connection.execute(text('SELECT msg_id FROM pgmq."q_default"')).scalar_one()

    assert postgres_broker.client.patch_headers("default", msg_id, {"status": "Success"}) is True

    with postgres_broker.client.engine.connect() as connection:
        headers = connection.execute(text('SELECT headers FROM pgmq."q_default"')).scalar_one()

    # Merged, not replaced: a header written earlier survives the outcome.
    assert headers == {"progress": 0.5, "status": "Success"}


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_client_patched_headers_survive_the_archive(postgres_broker):
    """What makes a status written before the ack durable: pgmq.archive carries headers over."""
    postgres_broker.declare_queue("default")
    postgres_broker.client.send("default", {"message_id": "m1"})

    with postgres_broker.client.engine.connect() as connection:
        msg_id = connection.execute(text('SELECT msg_id FROM pgmq."q_default"')).scalar_one()

    postgres_broker.client.patch_headers("default", msg_id, {"status": "Success"})
    assert postgres_broker.client.archive("default", msg_id) is True

    with postgres_broker.client.engine.connect() as connection:
        assert connection.execute(text('SELECT count(*) FROM pgmq."q_default"')).scalar_one() == 0
        assert connection.execute(text('SELECT headers FROM pgmq."a_default"')).scalar_one() == {"status": "Success"}


def test_postgres_client_patch_headers_reports_a_missing_message(postgres_broker):
    postgres_broker.declare_queue("default")

    assert postgres_broker.client.patch_headers("default", 123456, {"status": "Success"}) is False


def test_postgres_client_patch_headers_refuses_a_queue_name_it_cannot_quote():
    """The name reaches the client from a State, so the gate has to be here too."""
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    with patch.object(broker.client, "_run") as run, pytest.raises(ValueError, match="not a usable queue name"):
        broker.client.patch_headers('default" ; DROP TABLE pgmq.q_default; --', 7, {"status": "Success"})

    run.assert_not_called()


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_client_declaring_an_existing_queue_backfills_its_indexes(postgres_broker):
    """How a queue that predates an index gains it.

    Nothing warns when one is missing and nothing fails — the queue just seq scans
    every partition on each archive — so the declaration has to be what repairs it,
    rather than a manual call an operator has to know about.
    """
    postgres_broker.declare_queue("default")

    with postgres_broker.client.engine.begin() as connection:
        connection.execute(text('DROP INDEX pgmq."q_default_msg_id_idx"'))
        connection.execute(text('DROP INDEX pgmq."a_default_msg_id_idx"'))
    assert not _index_exists(postgres_broker, "q_default_msg_id_idx")
    assert not _index_exists(postgres_broker, "a_default_msg_id_idx")

    # Re-declaring the queue is enough: the broker caches what it declared, so this
    # is what a restart against an existing database does.
    postgres_broker.queues.pop("default")
    postgres_broker.declare_queue("default")

    assert _index_exists(postgres_broker, "q_default_msg_id_idx")
    assert _index_exists(postgres_broker, "a_default_msg_id_idx")


@pytest.mark.parametrize("queue_name", ["default", "sales", "sales_eu", "sales.eu", "sales-eu", "_x", "q" * 47])
def test_postgres_client_accepts_usable_queue_names(queue_name):
    assert assert_valid_queue_name(queue_name) is None


@pytest.mark.parametrize(
    "queue_name",
    [
        "",
        "q" * 48,  # would truncate to the same index name as any other 48-character name
        'default" ; DROP TABLE pgmq.q_default; --',
        "default'",
        "with space",
        "with\nnewline",
        "accentué",
        "semi;colon",
        "back\\slash",
        "1leading-digit",
        "sales.DQ",
        "sales__eu",
    ],
)
def test_postgres_client_rejects_queue_names_it_cannot_quote(queue_name):
    with pytest.raises(ValueError, match="not a usable queue name"):
        assert_valid_queue_name(queue_name)


def test_postgres_client_create_indexes_names_the_pgmq_table_of_a_dotted_queue():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    conn = Mock()

    broker.client.create_indexes("etl__low", conn)

    executed = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert executed == [
        'CREATE INDEX IF NOT EXISTS "q_etl__low_msg_id_idx" ON pgmq."q_etl__low" (msg_id)',
        'CREATE INDEX IF NOT EXISTS "a_etl__low_msg_id_idx" ON pgmq."a_etl__low" (msg_id)',
    ]


@pytest.mark.parametrize("method", ["create_indexes", "patch_headers"])
def test_postgres_client_refuses_a_logical_dotted_queue_name(method):
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    conn = Mock()
    args = (conn,) if method == "create_indexes" else (7, {"status": "Success"}, conn)

    with pytest.raises(ValueError, match="not a usable queue name"):
        getattr(broker.client, method)("etl.low", *args)

    conn.execute.assert_not_called()


@pytest.mark.parametrize(("method", "kwargs"), [("delete_messages", {}), ("find_last_attempts", {"status": "Failure"})])
def test_postgres_client_archive_queries_refuse_a_logical_dotted_queue_name(method, kwargs):
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    with patch.object(broker.client, "engine") as engine, pytest.raises(ValueError, match="not a usable queue name"):
        getattr(broker.client, method)("etl.low", **kwargs)

    engine.connect.assert_not_called()


def test_postgres_client_patch_headers_updates_the_pgmq_table_of_a_dotted_queue():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    conn = Mock()
    conn.execute.return_value.rowcount = 1

    assert broker.client.patch_headers("etl__low", 7, {"status": "Success"}, conn) is True

    (statement, params) = conn.execute.call_args.args
    assert 'UPDATE pgmq."q_etl__low"' in str(statement)
    assert params["msg_id"] == 7


def test_postgres_client_create_indexes_refuses_a_queue_name_it_cannot_quote():
    """The gate sits on the method that interpolates the name, not on its callers."""
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    conn = Mock()

    with pytest.raises(ValueError, match="not a usable queue name"):
        broker.client.create_indexes('default" ; DROP TABLE pgmq.q_default; --', conn)

    conn.execute.assert_not_called()


def test_postgres_broker_declares_no_queue_it_cannot_name():
    """create_indexes rejects the name, and the transaction declare_queue opened rolls back."""
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.client.validate_queue_name = Mock()
    broker.client.create_partitioned_queue = Mock()
    broker.client.enable_notify = Mock()
    broker.client.list_queues = Mock(return_value=[])
    broker.client.engine.begin = MagicMock()

    with pytest.raises(ValueError, match="not a usable queue name"):
        broker.declare_queue('default" ; DROP TABLE pgmq.q_default; --')

    assert broker.queues == {}


def test_postgres_backend_ignores_a_terminal_status_without_a_delivery_id():
    """Nothing names the row to write on, so there is nothing to write.

    That is the failed-enqueue case: MessageState reports a Failure, but an enqueue
    that raised wrote no row to record it on.
    """
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    backend = PostgresBackend(broker=broker)

    with patch.object(broker.client, "patch_headers") as patch_headers:
        backend.set_state(State("mid", StateStatusesEnum.Success, queue_name="default"))

    patch_headers.assert_not_called()


def test_postgres_backend_ignores_a_state_the_pgmq_row_already_implies():
    """Those are not written even when the row is known."""
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    backend = PostgresBackend(broker=broker)

    with patch.object(broker.client, "patch_headers") as patch_headers:
        backend.set_state(State("mid", StateStatusesEnum.Pending, queue_name="default", delivery_id=7))
        backend.set_state(State("mid", StateStatusesEnum.Started, queue_name="default", delivery_id=7))
        backend.set_state(State("mid", progress=0.5, queue_name="default", delivery_id=7))

    patch_headers.assert_not_called()


def test_postgres_backend_patches_the_pgmq_name_of_a_dotted_queue():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    backend = PostgresBackend(broker=broker)

    with patch.object(broker.client, "patch_headers", return_value=True) as patch_headers:
        backend.set_state(State("mid", StateStatusesEnum.Success, queue_name="etl.low", delivery_id=7))

    patch_headers.assert_called_once_with("etl__low", 7, {"status": "Success"})
