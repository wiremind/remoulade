# This file is a part of Remoulade.
#
# Copyright (C) 2026 WIREMIND SAS <dev@wiremind.io>
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
"""Remoulade's hand-written SQL on top of the PGMQ client.

Every statement remoulade writes itself lives here, so ``PostgresBroker`` and the
pgmq state backend never build SQL of their own.
"""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import Connection, text

from .pgmq_names import (
    QUEUE_NAME_MAX_LENGTH as QUEUE_NAME_MAX_LENGTH,
    assert_valid_pgmq_queue_name,
    assert_valid_queue_name as assert_valid_queue_name,
    pgmq_table_name,
)


class RemouladePostgresClient(SQLAlchemyPGMQueue):
    """A PGMQ client that also knows how to patch a remoulade message's headers.

    Inherits the whole PGMQ surface (``send``, ``read``, ``archive``,
    ``set_vt``, ``metrics``, ...) unchanged and adds the statements remoulade
    needs on top of it.
    """

    def create_indexes(self, queue_name: str, conn: Connection | None = None) -> None:
        """Ensure the queue and archive tables carry the ``msg_id`` indexes remoulade needs.

        Partitioned by time, neither table has one from pgmq; the archive's serves
        :meth:`delete_messages`, whose batches look rows up by ``msg_id``.
        ``queue_name`` is the pgmq name of the queue, see
        :func:`~remoulade.helpers.pgmq_names.physical_queue_name`.

        Raises:
          ValueError: If ``queue_name`` is not the pgmq name of a usable queue name.
        """
        assert_valid_pgmq_queue_name(queue_name)
        with self._connection(conn) as connection:
            for table in (pgmq_table_name(queue_name, "q"), pgmq_table_name(queue_name, "a")):
                connection.execute(text(f'CREATE INDEX IF NOT EXISTS "{table}_msg_id_idx" ON pgmq."{table}" (msg_id)'))

    def patch_headers(self, queue: str, msg_id: int, patch: dict[str, Any], conn: Connection | None = None) -> bool:
        """Merge ``patch`` into an enqueued message's headers, key by key.

        ``queue`` is the pgmq name of the queue, see
        :func:`~remoulade.helpers.pgmq_names.physical_queue_name`. Only reaches a
        message still in ``pgmq.q_<queue>``; once archived, its headers are out of
        reach. ``pgmq.archive`` carries them over.

        Returns:
          bool: Whether a row was patched.

        Raises:
          ValueError: If ``queue`` is not the pgmq name of a usable queue name.
        """
        assert_valid_pgmq_queue_name(queue)
        statement = text(f"""
            UPDATE pgmq."{pgmq_table_name(queue)}"
            SET headers = coalesce(headers, '{{}}'::jsonb) || CAST(:patch AS jsonb)
            WHERE msg_id = :msg_id
        """)  # noqa: S608
        return self._run(statement, {"msg_id": msg_id, "patch": json.dumps(patch)}, conn) > 0

    def delete_messages(
        self,
        queue: str,
        *,
        include_active: bool = False,
        statuses: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        batch_size: int = 10_000,
        lock_timeout: str = "3s",
    ) -> dict[str, int]:
        """Delete the archived messages of a queue, and its enqueued ones too when ``include_active``.

        Filters combine with AND and are skipped when None: a timestamp in ``[start, end)``
        -- ``archived_at`` on the archive, ``enqueued_at`` on the queue, each table's
        indexed partition key -- and ``headers->>'status'`` in ``statuses``.

        Counts the matching rows first, then deletes them ``batch_size`` per transaction; rows
        that start matching after the count are left in place. A batch
        skips rows another transaction has locked (``SKIP LOCKED``) rather than wait on them,
        and they stay in place; ``lock_timeout`` bounds any other lock wait.

        Returns:
          dict[str, int]: The number of deleted rows, by table name.

        Raises:
          ValueError: If ``queue`` cannot be used as a SQL identifier.
          sqlalchemy.exc.OperationalError: If a batch waits on a lock past ``lock_timeout``,
            e.g. while pg_partman maintenance drops a partition.
        """
        assert_valid_queue_name(queue)
        params = {"start": start, "end": end, "statuses": statuses, "batch_size": batch_size}
        time_column_by_table = {f"a_{queue}": "archived_at"}
        if include_active:
            time_column_by_table[f"q_{queue}"] = "enqueued_at"

        set_lock_timeout = text("SELECT set_config('lock_timeout', :lock_timeout, true)")
        deleted_count_by_table = {}
        for table, time_column in time_column_by_table.items():
            conditions = ["TRUE"]
            if start is not None:
                conditions.append(f"{time_column} >= :start")
            if end is not None:
                conditions.append(f"{time_column} < :end")
            if statuses is not None:
                conditions.append("headers->>'status' = ANY(:statuses)")
            where = " AND ".join(conditions)
            statement = text(f"""
                DELETE FROM pgmq."{table}" WHERE msg_id IN (
                    SELECT msg_id FROM pgmq."{table}"
                    WHERE {where}
                    LIMIT :batch_size
                    FOR UPDATE SKIP LOCKED
                )
            """)  # noqa: S608
            with self.engine.connect() as connection:
                with connection.begin():
                    connection.execute(set_lock_timeout, {"lock_timeout": lock_timeout})
                    matching_count = connection.execute(
                        text(f'SELECT count(*) FROM pgmq."{table}" WHERE {where}'),  # noqa: S608
                        params,
                    ).scalar_one()
                deleted_count_by_table[table] = 0
                for _ in range(0, matching_count, batch_size):
                    with connection.begin():
                        connection.execute(set_lock_timeout, {"lock_timeout": lock_timeout})
                        deleted_count_by_table[table] += connection.execute(statement, params).rowcount
        return deleted_count_by_table

    def find_last_attempts(
        self,
        queue: str,
        *,
        status: str,
        start: datetime | None = None,
        end: datetime | None = None,
        actor_name: str | None = None,
        message_ids: list[str] | None = None,
    ) -> list[tuple[int, dict[str, Any]]]:
        """Find the messages whose last archived attempt ended with ``status`` and that nothing will run again.

        A retry re-enqueues the same ``message_id`` as a new row, so only the archived row
        with the highest ``msg_id`` per ``message_id`` counts, and a message with an attempt
        still in the queue is left out. So is any message of a pipeline or a group (it has a
        ``composition_id``): running it again would run, or count, part of a composition
        that already ended. Filters apply to that last attempt
        and are skipped when None: ``archived_at`` in ``[start, end)``, ``actor_name``,
        ``message_id`` in ``message_ids``.

        Reads the whole archive, which has no ``message_id`` index, unless ``actor_name`` or
        ``message_ids`` narrow it first; a plain read never blocks the workers.

        Returns:
          list[tuple[int, dict[str, Any]]]: ``(archived msg_id, message payload)``, by ``msg_id``.

        Raises:
          ValueError: If ``queue`` cannot be used as a SQL identifier.
        """
        assert_valid_queue_name(queue)
        # Every attempt of a message shares its message_id and actor_name, so these filters
        # can narrow the archive before the DISTINCT ON sorts it without changing the result.
        attempt_conditions = ["TRUE"]
        if actor_name is not None:
            attempt_conditions.append("message->>'actor_name' = :actor_name")
        if message_ids is not None:
            attempt_conditions.append("message->>'message_id' = ANY(:message_ids)")
        conditions = [
            "headers->>'status' = :status",
            "message->'options'->'composition_id' IS NULL",
            f"""NOT EXISTS (
                SELECT 1 FROM pgmq."q_{queue}" AS active
                WHERE active.message->>'message_id' = last_attempt.message->>'message_id'
            )""",  # noqa: S608
        ]
        if start is not None:
            conditions.append("archived_at >= :start")
        if end is not None:
            conditions.append("archived_at < :end")
        statement = text(f"""
            WITH last_attempt AS (
                SELECT DISTINCT ON (message->>'message_id') msg_id, archived_at, message, headers
                FROM pgmq."a_{queue}"
                WHERE {" AND ".join(attempt_conditions)}
                ORDER BY message->>'message_id', msg_id DESC
            )
            SELECT msg_id, message FROM last_attempt
            WHERE {" AND ".join(conditions)}
            ORDER BY msg_id
        """)  # noqa: S608
        params = {"status": status, "start": start, "end": end, "actor_name": actor_name, "message_ids": message_ids}
        with self.engine.connect() as connection:
            return [(row.msg_id, row.message) for row in connection.execute(statement, params)]

    @contextmanager
    def _connection(self, conn: Connection | None) -> Iterator[Connection]:
        """Yield the caller's connection, or open a transaction of our own."""
        if conn is not None:
            yield conn
            return
        with self.engine.begin() as connection:
            yield connection

    def _run(self, statement: Any, params: dict[str, Any], conn: Connection | None) -> int:
        """Execute a write statement, returning the number of affected rows."""
        with self._connection(conn) as connection:
            return connection.execute(statement, params).rowcount
