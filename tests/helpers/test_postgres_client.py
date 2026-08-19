"""Tests for the PGMQ client's hand-written SQL (``remoulade.helpers.postgres_client``)."""

import os
from unittest.mock import Mock, patch

import pytest
from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import text

from remoulade.brokers.postgres import PostgresBroker

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
    assert 'CREATE INDEX IF NOT EXISTS "q_default_msg_id_idx" ON pgmq."q_default" (msg_id)' in executed
    # Remoulade looks messages up by its own id, which lives in the payload, on
    # both the live and the archived side; and hunts failures in the archive.
    assert (
        'CREATE INDEX IF NOT EXISTS "q_default_rmsgid_idx" ON pgmq."q_default" ((message->>\'message_id\'))' in executed
    )
    assert (
        'CREATE INDEX IF NOT EXISTS "a_default_rmsgid_idx" ON pgmq."a_default" ((message->>\'message_id\'))' in executed
    )
    assert 'CREATE INDEX IF NOT EXISTS "a_default_rstatus_idx" ON pgmq."a_default" ((headers->>\'status\'))' in executed


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


def test_postgres_client_creating_a_queue_brings_its_indexes():
    # A queue without these indexes works but degrades badly, so creating one
    # must not leave them to a separate call a caller could forget.
    broker = PostgresBroker(url=TEST_POSTGRES_URL, middleware=[])
    conn = Mock()
    broker.client.create_indexes = Mock()

    with patch.object(SQLAlchemyPGMQueue, "create_partitioned_queue") as create:
        broker.client.create_partitioned_queue("default", "1 day", "7 days", conn=conn)

    create.assert_called_once()
    broker.client.create_indexes.assert_called_once_with("default", conn)


@pytest.mark.usefixtures("postgres_broker")
def test_postgres_client_create_indexes_backfills_an_existing_queue(postgres_broker):
    """The supported way to give an index to a queue that predates it.

    Declaring a queue that already exists does nothing, so a queue created by a
    version of remoulade that did not declare one of these indexes only gains it
    through an explicit create_indexes call.
    """
    postgres_broker.declare_queue("default")

    with postgres_broker.client.engine.begin() as connection:
        connection.execute(text('DROP INDEX pgmq."q_default_msg_id_idx"'))

    # Re-declaring is deliberately not enough.
    postgres_broker.queues.pop("default")
    postgres_broker.declare_queue("default")
    assert not _index_exists(postgres_broker, "q_default_msg_id_idx")

    postgres_broker.client.create_indexes("default")

    assert _index_exists(postgres_broker, "q_default_msg_id_idx")
