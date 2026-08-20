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

PGMQ exposes a message's ``headers jsonb`` column but offers no way to patch it
after the fact, which is what remoulade needs to record a message's outcome in
the message itself. Every statement remoulade writes itself lives here, so
``PostgresBroker`` and the pgmq state backend never build SQL of their own.

The terminal outcome (success vs failure vs skipped vs canceled) is the only
thing stored: the rest of a message's lifecycle is *already* recorded by PGMQ
through ``enqueued_at``, ``last_read_at``, ``read_ct`` and — crucially — whether
the row sits in ``pgmq.q_<queue>`` or has been moved to ``pgmq.a_<queue>``.
"""

import json
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any, override

from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import Connection, text

from ..actor import QUEUE_NAME_PATTERN

#: pgmq's own limit on a queue name. It also keeps the longest identifier remoulade
#: derives from one (``q_<queue>_msg_id_idx``) under PostgreSQL's 63-byte cap, past
#: which PostgreSQL truncates -- and two long queue names would collide on one index.
QUEUE_NAME_MAX_LENGTH = 47


def assert_valid_queue_name(queue_name: str) -> None:
    """Check that ``queue_name`` is safe to interpolate as a SQL identifier.

    The character set is remoulade's own (:data:`~remoulade.actor.QUEUE_NAME_PATTERN`,
    already enforced on every actor declaration), which holds nothing needing quotes
    or escaping; only the length bound is specific to PostgreSQL.

    Parameters:
      queue_name(str): The name to check.

    Raises:
      ValueError: If the name does not match :data:`~remoulade.actor.QUEUE_NAME_PATTERN`
        or is longer than :data:`QUEUE_NAME_MAX_LENGTH`.
    """
    if not QUEUE_NAME_PATTERN.fullmatch(queue_name) or len(queue_name) > QUEUE_NAME_MAX_LENGTH:
        raise ValueError(
            f"{queue_name!r} is not a usable queue name for a PostgresBroker: it becomes a SQL identifier, so it "
            f"must start with a letter or an underscore, hold only letters, digits, dashes, dots and underscores, "
            f"and be at most {QUEUE_NAME_MAX_LENGTH} characters long."
        )


class RemouladePostgresClient(SQLAlchemyPGMQueue):
    """A PGMQ client that also knows how to patch a remoulade message's headers.

    Inherits the whole PGMQ surface (``send``, ``read``, ``archive``,
    ``set_vt``, ``metrics``, ...) unchanged and adds the statements remoulade
    needs on top of it.
    """

    def create_indexes(self, queue_name: str, conn: Connection | None = None) -> None:
        """Ensure the queue table carries the indexes remoulade needs."""
        # (table prefix, index name suffix, indexed expression including its parentheses)
        indexes = [
            ("q", "msg_id_idx", "(msg_id)"),
            ("q", "rmsgid_idx", "((message->>'message_id'))"),
        ]
        with self._connection(conn) as connection:
            for table_prefix, suffix, expression in indexes:
                index = f"{table_prefix}_{queue_name}_{suffix}"
                connection.execute(
                    text(f'CREATE INDEX IF NOT EXISTS "{index}" ON pgmq."{table_prefix}_{queue_name}" {expression}')
                )

    @override
    def archive(
        self,
        queue: str,
        msg_id: int,
        conn: Connection | None = None,
        *,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        """Archive a message, merging ``headers`` into its stored headers if given.
        Returns:
          bool: Whether a message was archived.
        """
        if not headers:
            return super().archive(queue, msg_id, conn=conn)

        statement = text(f"""
            WITH archived AS (
                DELETE FROM pgmq."q_{queue}"
                WHERE msg_id = :msg_id
                RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
            )
            INSERT INTO pgmq."a_{queue}" (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
            SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message,
                   coalesce(headers, '{{}}'::jsonb) || CAST(:patch AS jsonb)
            FROM archived
        """)  # noqa: S608
        return self._run(statement, {"msg_id": msg_id, "patch": json.dumps(headers)}, conn) > 0

    def patch_headers(
        self,
        queue_names: Iterable[str],
        message_id: str,
        patch: dict[str, Any],
        conn: Connection | None = None,
    ) -> bool:
        """Merge ``patch`` into the headers of a message still in a queue table.
        Returns:
          bool: Whether a row was updated.
        """
        for queue_name in queue_names:
            statement = text(f"""
                UPDATE pgmq."q_{queue_name}"
                SET headers = coalesce(headers, '{{}}'::jsonb) || CAST(:patch AS jsonb)
                WHERE message->>'message_id' = :message_id
            """)  # noqa: S608
            if self._run(statement, {"patch": json.dumps(patch), "message_id": message_id}, conn) > 0:
                return True
        return False

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
