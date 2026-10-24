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
        """Ensure the queue table carries the indexes remoulade needs.

        ``queue_name`` is the pgmq name of the queue, see
        :func:`~remoulade.helpers.pgmq_names.physical_queue_name`.

        Raises:
          ValueError: If ``queue_name`` is not the pgmq name of a usable queue name.
        """
        assert_valid_pgmq_queue_name(queue_name)
        table_name = pgmq_table_name(queue_name)
        with self._connection(conn) as connection:
            connection.execute(
                text(f'CREATE INDEX IF NOT EXISTS "{table_name}_msg_id_idx" ON pgmq."{table_name}" (msg_id)')
            )

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
