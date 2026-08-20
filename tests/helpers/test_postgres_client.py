"""Tests for the PGMQ client's hand-written SQL (``remoulade.helpers.postgres_client``)."""

import os
from unittest.mock import Mock, patch

import pytest
from pgmq import SQLAlchemyPGMQueue
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
    # Only the queue table: nothing remoulade runs reads the archive back, so an
    # index there would be paid for on every archive and never used.
    assert executed == [
        'CREATE INDEX IF NOT EXISTS "q_default_msg_id_idx" ON pgmq."q_default" (msg_id)',
        # patch_headers finds a message by remoulade's own id, in the payload.
        'CREATE INDEX IF NOT EXISTS "q_default_rmsgid_idx" ON pgmq."q_default" ((message->>\'message_id\'))',
    ]


def test_postgres_client_archive_without_headers_uses_pgmq_archive():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    with patch.object(SQLAlchemyPGMQueue, "archive", return_value=True) as archive:
        assert broker.client.archive("default", 7) is True
        assert broker.client.archive("default", 7, headers={}) is True

    # An empty patch is nothing to merge, so it must not take the custom path.
    assert archive.call_count == 2


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_client_archive_with_headers_merges_them_in_one_statement(postgres_broker):
    postgres_broker.declare_queue("default")
    postgres_broker.client.send("default", {"message_id": "m1"}, headers={"progress": 0.5})

    with postgres_broker.client.engine.connect() as connection:
        msg_id = connection.execute(text('SELECT msg_id FROM pgmq."q_default"')).scalar_one()

    assert postgres_broker.client.archive("default", msg_id, headers={"status": "Success"}) is True

    with postgres_broker.client.engine.connect() as connection:
        assert connection.execute(text('SELECT count(*) FROM pgmq."q_default"')).scalar_one() == 0
        archived = connection.execute(text('SELECT headers FROM pgmq."a_default"')).scalar_one()

    # Merged, not replaced: a progress written earlier survives the outcome.
    assert archived == {"progress": 0.5, "status": "Success"}


def test_postgres_client_archive_with_headers_reports_a_missing_message(postgres_broker):
    postgres_broker.declare_queue("default")

    assert postgres_broker.client.archive("default", 123456, headers={"status": "Success"}) is False


def test_postgres_client_patch_headers_merges_into_a_live_message(postgres_broker):
    postgres_broker.declare_queue("default")
    postgres_broker.client.send("default", {"message_id": "m1"}, headers={"progress": 0.5})

    assert postgres_broker.client.patch_headers(["default"], "m1", {"status": "Canceled"}) is True

    with postgres_broker.client.engine.connect() as connection:
        headers = connection.execute(text('SELECT headers FROM pgmq."q_default"')).scalar_one()

    # Merged, not replaced, like the archive path.
    assert headers == {"progress": 0.5, "status": "Canceled"}


def test_postgres_client_patch_headers_leaves_archived_messages_alone(postgres_broker):
    postgres_broker.declare_queue("default")
    postgres_broker.client.send("default", {"message_id": "m1"})
    with postgres_broker.client.engine.connect() as connection:
        msg_id = connection.execute(text('SELECT msg_id FROM pgmq."q_default"')).scalar_one()
    postgres_broker.client.archive("default", msg_id, headers={"status": "Success"})

    assert postgres_broker.client.patch_headers(["default"], "m1", {"status": "Failure"}) is False

    with postgres_broker.client.engine.connect() as connection:
        headers = connection.execute(text('SELECT headers FROM pgmq."a_default"')).scalar_one()

    assert headers == {"status": "Success"}


def test_postgres_client_patch_headers_reports_an_unknown_message(postgres_broker):
    postgres_broker.declare_queue("default")

    assert postgres_broker.client.patch_headers(["default"], "nope", {"status": "Success"}) is False


def test_postgres_client_patch_headers_walks_the_queues_it_is_given(postgres_broker):
    postgres_broker.declare_queue("default")
    postgres_broker.declare_queue("other")
    postgres_broker.client.send("other", {"message_id": "m1"})

    # One statement per queue until one matches, so the caller names as few as
    # it can — but several are allowed when it does not know which one holds it.
    assert postgres_broker.client.patch_headers(["default", "other"], "m1", {"status": "Success"}) is True

    with postgres_broker.client.engine.connect() as connection:
        headers = connection.execute(text('SELECT headers FROM pgmq."q_other"')).scalar_one()

    assert headers == {"status": "Success"}


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_client_declaring_an_existing_queue_backfills_its_indexes(postgres_broker):
    """How a queue that predates an index gains it.

    Nothing warns when one is missing and nothing fails — the queue just seq scans
    every partition on each archive — so the declaration has to be what repairs it,
    rather than a manual call an operator has to know about.
    """
    postgres_broker.declare_queue("default")

    with postgres_broker.client.engine.begin() as connection:
        connection.execute(text('DROP INDEX pgmq."q_default_rmsgid_idx"'))
    assert not _index_exists(postgres_broker, "q_default_rmsgid_idx")

    # Re-declaring the queue is enough: the broker caches what it declared, so this
    # is what a restart against an existing database does.
    postgres_broker.queues.pop("default")
    postgres_broker.declare_queue("default")

    assert _index_exists(postgres_broker, "q_default_rmsgid_idx")


@pytest.mark.parametrize("queue_name", ["default", "sales", "sales_eu", "sales.DQ", "sales-eu", "_x", "q" * 47])
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
    ],
)
def test_postgres_client_rejects_queue_names_it_cannot_quote(queue_name):
    with pytest.raises(ValueError, match="not a usable queue name"):
        assert_valid_queue_name(queue_name)


def test_postgres_broker_declares_no_queue_it_cannot_name():
    """The gate rejects the name before any statement is built, so nothing hits the database."""
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])

    with patch.object(broker, "tx") as tx, pytest.raises(ValueError, match="not a usable queue name"):
        broker.declare_queue('default" ; DROP TABLE pgmq.q_default; --')

    tx.assert_not_called()
    assert broker.queues == {}


def test_postgres_backend_patches_headers_on_the_brokers_transaction():
    """Without the broker's connection the UPDATE runs outside its transaction.

    Mocked so it is checked without a database: the DB-backed counterpart lives in
    ``tests/state/test_postgres_backend.py``.
    """
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None  # what declare_queue records, without a database
    backend = PostgresBackend(broker=broker)
    connection = Mock()

    with patch.object(broker.client, "patch_headers") as patch_headers:
        broker.state.transaction_connection = connection
        try:
            backend.set_state(State("mid", StateStatusesEnum.Success, queue_name="default"))
        finally:
            broker.state.transaction_connection = None

    assert patch_headers.call_args.kwargs["conn"] is connection


def test_postgres_backend_patches_headers_on_its_own_connection_outside_a_transaction():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    broker.queues["default"] = None
    backend = PostgresBackend(broker=broker)

    with patch.object(broker.client, "patch_headers") as patch_headers:
        backend.set_state(State("mid", StateStatusesEnum.Success, queue_name="default"))

    # None lets the client open a transaction for the single statement.
    assert patch_headers.call_args.kwargs["conn"] is None


def test_postgres_backend_drops_a_status_for_a_queue_the_broker_never_declared():
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    backend = PostgresBackend(broker=broker)

    with patch.object(broker.client, "patch_headers") as patch_headers:
        backend.set_state(State("mid", StateStatusesEnum.Success, queue_name="never-declared"))
        backend.set_state(State("mid", StateStatusesEnum.Success))

    patch_headers.assert_not_called()
