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
after the fact, and no way to query a queue's rows as remoulade states. Every
statement remoulade writes itself lives here, so ``PostgresBroker`` and the pgmq
state backend never build SQL of their own.

The central idea is that a message's lifecycle is *already* recorded by PGMQ:
``enqueued_at``, ``last_read_at``, ``read_ct`` and — crucially — whether the row
sits in ``pgmq.q_<queue>`` or has been moved to ``pgmq.a_<queue>``. Only the
terminal outcome (success vs failure vs skipped vs canceled) cannot be derived,
so that is the only thing stored in ``headers``.
"""

import datetime
import json
import re
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from typing import Any, Final, override

from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import Connection, Row, text

#: PGMQ refuses queue names longer than this (``pgmq.validate_queue_name``), so
#: that ``template_pgmq_q_<name>`` fits in PostgreSQL's 63 byte identifier limit.
MAX_QUEUE_NAME_LENGTH: Final[int] = 47
MAX_IDENTIFIER_LENGTH: Final[int] = 63

_QUEUE_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

#: ``jsonb`` is untyped, so a hand-edited header or a foreign producer can put a
#: non-numeric value where a number is expected. A bare ``::int`` cast would then
#: abort the whole query, taking the dashboard down over one bad row, so numeric
#: fields are guarded and read as NULL when they do not look like numbers.
_INTEGER_RE: Final[str] = r"^-?[0-9]+$"


def _safe_cast(expression: str, sql_type: str, pattern: str) -> str:
    """Cast a jsonb-extracted text value, yielding NULL instead of failing."""
    return f"CASE WHEN {expression} ~ '{pattern}' THEN ({expression})::{sql_type} END"


#: Columns of the state projection that are read straight out of the message
#: envelope or out of PGMQ's own bookkeeping. Shared by the queue and the
#: archive branch of the union so both stay in lockstep.
_SHARED_STATE_COLUMNS: Final[str] = f"""
           message->>'message_id'                AS message_id,
           message->>'actor_name'                AS actor_name,
           message->'args'                       AS args,
           message->'kwargs'                     AS kwargs,
           message->'options'                    AS options,
           message->>'queue_name'                AS queue_name,
           message->'options'->>'composition_id' AS composition_id,
           {_safe_cast("message->'options'->>'priority'", "int", _INTEGER_RE)} AS priority,
           headers                               AS headers,
           NULL::float8                          AS progress,
           enqueued_at                           AS enqueued_datetime,
           last_read_at                          AS started_datetime,
           read_ct                               AS read_ct,
           msg_id                                AS msg_id
"""

#: State columns a caller may sort on. Interpolated into ORDER BY, hence the
#: whitelist. Mirrors ``StatesParamsSchema.sort_column`` in ``remoulade.api``.
SORTABLE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "message_id",
        "status",
        "actor_name",
        "priority",
        # Always NULL: the pgmq state backend does not store progress. Kept
        # sortable because the HTTP API accepts it as a sort column.
        "progress",
        "enqueued_datetime",
        "started_datetime",
        "end_datetime",
        "queue_name",
        "composition_id",
    }
)

#: How to pick the current row among a message's attempts. The live row always
#: wins over an archived one; among archived rows the one that finished last
#: does. ``msg_id`` is only a final tiebreak and cannot be the main key: it is a
#: per-queue identity sequence, so it is not comparable across queues, and
#: ``escalation_queue_mapping`` does move a retried message to another queue.
_CURRENT_ATTEMPT_ORDER: Final[str] = "archived ASC, end_datetime DESC NULLS FIRST, enqueued_datetime DESC, msg_id DESC"

#: Filters that hold the same value for every attempt of a message, and can
#: therefore be applied before deduplicating attempts (cheaper). ``status`` and
#: the datetime bounds differ per attempt and must be applied after, so that
#: "show me the failures" means "whose *current* state is a failure".
_PUSHDOWN_FILTERS: Final[dict[str, str]] = {
    "selected_actors": "actor_name",
    "selected_message_ids": "message_id",
    "selected_composition_ids": "composition_id",
}


def validated_queue_name(queue_name: str) -> str:
    """Return ``queue_name`` if it is safe to interpolate as an SQL identifier.

    PGMQ table names are built by string interpolation (``pgmq.q_<name>``), so
    remoulade cannot bind them as parameters. ``PostgresBroker.declare_queue``
    already calls ``pgmq.validate_queue_name`` server-side; this is the local,
    round-trip-free guard used on every read path.

    Raises:
      ValueError: If the name is not a bare identifier or is too long.
    """
    if not _QUEUE_NAME_RE.match(queue_name):
        raise ValueError(f"unsafe pgmq queue name: {queue_name!r}")
    if len(queue_name) > MAX_QUEUE_NAME_LENGTH:
        raise ValueError(f"pgmq queue name is too long (max {MAX_QUEUE_NAME_LENGTH}): {queue_name!r}")
    return queue_name


#: Every ``noqa: S608`` below is a query whose only interpolated parts are a
#: queue name checked by :func:`validated_queue_name` and, for the ordering, a
#: column and direction checked against :data:`SORTABLE_COLUMNS`. PGMQ names its
#: tables ``pgmq.q_<queue>``, so the queue cannot be a bind parameter; every
#: value a caller supplies is bound.


def _index_name(prefix: str, queue_name: str, suffix: str) -> str:
    """Build an index identifier, refusing to let PostgreSQL silently truncate it.

    Truncation would be worse than an error: two long queue names could collapse
    onto the same index name.
    """
    name = f"{prefix}_{queue_name}_{suffix}"
    if len(name) > MAX_IDENTIFIER_LENGTH:
        raise ValueError(f"index name is too long for PostgreSQL: {name!r}")
    return name


class RemouladePostgresClient(SQLAlchemyPGMQueue):
    """A PGMQ client that also knows how to read and patch remoulade state.

    Inherits the whole PGMQ surface (``send``, ``read``, ``archive``,
    ``set_vt``, ``metrics``, ...) unchanged and adds the statements remoulade
    needs on top of it.
    """

    # -- write paths ----------------------------------------------------------

    @override
    def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int | str = 10000,
        retention_interval: int | str = 100000,
        conn: Connection | None = None,
    ) -> None:
        """Create a partitioned PGMQ queue, with the indexes remoulade needs.

        The indexes belong to creating the queue: a queue without them works but
        degrades badly, so they are not left to a separate call a caller could
        forget.

        Only queues being created go through here: ``pgmq.create_partitioned`` is
        not idempotent — it calls ``pg_partman.create_parent``, which raises once
        the parent is configured — so it cannot be re-run on an existing queue.
        Backfilling an existing queue is :meth:`create_indexes`' job.
        """
        super().create_partitioned_queue(
            queue,
            partition_interval=partition_interval,
            retention_interval=retention_interval,
            conn=conn,
        )
        self.create_indexes(queue, conn)

    def create_indexes(self, queue_name: str, conn: Connection | None = None) -> None:
        """Ensure the queue and archive tables carry the indexes remoulade needs.

        Queues created by remoulade get these through
        :meth:`create_partitioned_queue`, which is the only caller on the normal
        path. It is also safe to call directly, and is how you backfill a queue
        created by a version of remoulade that did not declare one of these
        indexes yet — without it, lookups on that queue seq scan every partition::

            for queue in broker.get_declared_queues():
                broker.client.create_indexes(queue)

        Created on the partitioned parents so they propagate to existing and
        future partitions. On a large existing queue the initial build locks the
        table for its duration.

        * ``q_<queue> (msg_id)`` — PGMQ's time-partitioned tables ship without
          one, so every ``archive`` (ack/nack) and ``set_vt`` (heartbeat,
          requeue) lookup would seq scan all partitions.
        * ``q_/a_<queue> ((message->>'message_id'))`` — remoulade looks messages
          up by *its* id, which lives inside the payload. Indexing the payload
          rather than ``headers`` keeps updates of ``headers``, ``vt`` and
          ``read_ct`` eligible for HOT, since none of those are indexed.
        * ``a_<queue> ((headers->>'status'))`` — hunting failures in the
          archive. Free on the archive side: an archived row is inserted once
          and never updated, so there is no HOT concern there.
        """
        queue = validated_queue_name(queue_name)
        # (table prefix, index name suffix, indexed expression including its parentheses)
        indexes = [
            ("q", "msg_id_idx", "(msg_id)"),
            ("q", "rmsgid_idx", "((message->>'message_id'))"),
            ("a", "rmsgid_idx", "((message->>'message_id'))"),
            ("a", "rstatus_idx", "((headers->>'status'))"),
        ]
        with self._connection(conn, write=True) as connection:
            for table_prefix, suffix, expression in indexes:
                index = _index_name(table_prefix, queue, suffix)
                connection.execute(
                    text(f'CREATE INDEX IF NOT EXISTS "{index}" ON pgmq."{table_prefix}_{queue}" {expression}')
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

        Without a patch this is PGMQ's own archive. With one, it is a faithful
        clone of it with the header merge folded into the same statement, so
        recording a message's outcome costs nothing on top of the archive that
        ack/nack performs anyway. ``archived_at`` is left to the archive table's
        ``DEFAULT now()`` either way.

        Returns:
          bool: Whether a message was archived.
        """
        if not headers:
            return super().archive(queue, msg_id, conn=conn)

        queue_name = validated_queue_name(queue)
        statement = text(f"""
            WITH archived AS (
                DELETE FROM pgmq."q_{queue_name}"
                WHERE msg_id = :msg_id
                RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
            )
            INSERT INTO pgmq."a_{queue_name}" (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
            SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message,
                   coalesce(headers, '{{}}'::jsonb) || CAST(:patch AS jsonb)
            FROM archived
        """)  # noqa: S608
        return self._run(statement, {"msg_id": msg_id, "patch": json.dumps(headers)}, conn) > 0

    def patch_headers(
        self,
        message_id: str,
        patch: dict[str, Any],
        conn: Connection | None = None,
        *,
        queue_names: Iterable[str] | None = None,
    ) -> bool:
        """Merge ``patch`` into the headers of a message still in a queue table.

        Used when the patch cannot ride along with the archive: a terminal state
        recorded without the message proxy at hand. Archived messages are
        immutable here, so this is a no-op for them.

        With several candidate queues the message is located first, to keep this
        at two statements instead of one per queue; pass ``queue_names`` with a
        single queue when the caller already knows it, to stay at one.

        Returns:
          bool: Whether a row was updated.
        """
        candidates = self._target_queues(queue_names)
        if not candidates:
            return False
        if len(candidates) > 1:
            located = self.find_state(message_id, conn=conn, queue_names=candidates)
            if located is None or located.archived:
                return False
            candidates = [located.pgmq_queue]

        for queue_name in candidates:
            queue = validated_queue_name(queue_name)
            statement = text(f"""
                UPDATE pgmq."q_{queue}"
                SET headers = coalesce(headers, '{{}}'::jsonb) || CAST(:patch AS jsonb)
                WHERE message->>'message_id' = :message_id
            """)  # noqa: S608
            if self._run(statement, {"patch": json.dumps(patch), "message_id": message_id}, conn) > 0:
                return True
        return False

    # -- read paths -----------------------------------------------------------

    def _target_queues(self, queue_names: Iterable[str] | None = None) -> list[str]:
        """The queues to look in, defaulting to every PGMQ queue in the database.

        Read from PostgreSQL rather than from a broker's declared queues, so a
        process that only serves a dashboard can find states without having
        declared the actors. Queues remoulade does not own contribute rows with a
        NULL message_id, which the projection discards.
        """
        if queue_names is not None:
            return list(queue_names)
        return [queue.queue_name for queue in self.list_queues()]

    def find_state(
        self,
        message_id: str,
        conn: Connection | None = None,
        *,
        queue_names: Iterable[str] | None = None,
    ) -> Row[Any] | None:
        """Return the current state row of a message, or None if it is unknown.

        A retried message leaves one row per attempt, possibly spread over two
        queues when ``escalation_queue_mapping`` moved it. The live row wins
        over any archived one, and among archived rows the most recent attempt
        wins — which matches Redis, where the last write wins.
        """
        union = self._states_union(queue_names)
        if union is None:
            return None
        statement = text(f"""
            SELECT * FROM ({union}) AS states
            WHERE states.message_id = :message_id
            ORDER BY {_CURRENT_ATTEMPT_ORDER}
            LIMIT 1
        """)  # noqa: S608
        return self._fetch_one(statement, {"message_id": message_id}, conn)

    def select_states(
        self,
        *,
        queue_names: Iterable[str] | None = None,
        size: int | None = None,
        offset: int = 0,
        sort_column: str | None = None,
        sort_direction: str | None = None,
        conn: Connection | None = None,
        **filters: Any,
    ) -> list[Row[Any]]:
        """Return state rows, filtered, sorted and paginated by composition.

        Pagination counts *compositions* rather than messages — a page holds
        ``size`` pipelines/groups and every message belonging to them — which is
        the contract the dashboard was built against and what
        :meth:`count_states` counts.
        """
        current, params = self._current_states_cte(queue_names, filters)
        if current is None:
            return []
        row_order = self._order_by(sort_column, sort_direction, table="filtered")

        if size is None:
            statement = text(f"""
                {current}
                SELECT * FROM filtered ORDER BY {row_order} OFFSET :offset
            """)  # noqa: S608
            params["offset"] = offset
        else:
            # Group by composition (falling back to the message itself), take one
            # page of those, then bring back every message of the selected ones.
            statement = text(f"""
                {current},
                grouped AS (
                    SELECT max(composition_id)      AS grouped_composition_id,
                           max(message_id)          AS grouped_message_id,
                           max(status)              AS grouped_status,
                           max(actor_name)          AS grouped_actor_name,
                           max(priority)            AS grouped_priority,
                           avg(progress)            AS grouped_progress,
                           min(enqueued_datetime)   AS grouped_enqueued_datetime,
                           min(started_datetime)    AS grouped_started_datetime,
                           max(end_datetime)        AS grouped_end_datetime,
                           max(queue_name)          AS grouped_queue_name
                    FROM filtered
                    GROUP BY coalesce(composition_id, message_id)
                    ORDER BY {self._order_by(sort_column, sort_direction, prefix="grouped_")}
                    OFFSET :offset LIMIT :size
                )
                SELECT filtered.* FROM filtered
                JOIN grouped ON filtered.message_id = grouped.grouped_message_id
                             OR filtered.composition_id = grouped.grouped_composition_id
                ORDER BY {row_order}
            """)  # noqa: S608
            params["offset"] = offset
            params["size"] = size

        return self._fetch_all(statement, params, conn)

    def count_states(
        self, conn: Connection | None = None, *, queue_names: Iterable[str] | None = None, **filters: Any
    ) -> int:
        """Count the compositions matching ``filters``, to pair with :meth:`select_states`."""
        current, params = self._current_states_cte(queue_names, filters)
        if current is None:
            return 0
        statement = text(f"""
            {current}
            SELECT count(DISTINCT coalesce(composition_id, message_id)) FROM filtered
        """)  # noqa: S608
        row = self._fetch_one(statement, params, conn)
        return int(row[0]) if row is not None else 0

    # -- SQL construction -----------------------------------------------------

    def _state_projection(self, queue_name: str) -> str:
        """Project one queue's live and archived rows as remoulade states.

        The single source of truth for how a pgmq row becomes a ``State``.
        ``Pending``/``Started`` come from ``read_ct``, which ``pgmq.read()``
        increments for us; the terminal statuses are the only ones stored.

        An archived row without a stored status was archived outside the normal
        middleware path (no ``MessageState``, or an archive replay), and is
        reported as a success since ack is the successful path.
        """
        queue = validated_queue_name(queue_name)
        return f"""
            SELECT {_SHARED_STATE_COLUMNS},
                   NULL::timestamptz AS end_datetime,
                   false AS archived,
                   CASE WHEN read_ct = 0 THEN 'Pending' ELSE 'Started' END AS status,
                   '{queue}' AS pgmq_queue
            FROM pgmq."q_{queue}"
            UNION ALL
            SELECT {_SHARED_STATE_COLUMNS},
                   archived_at AS end_datetime,
                   true AS archived,
                   coalesce(headers->>'status', 'Success') AS status,
                   '{queue}' AS pgmq_queue
            FROM pgmq."a_{queue}"
        """  # noqa: S608

    def _states_union(self, queue_names: Iterable[str] | None = None) -> str | None:
        """Union every queue's projection, or None when there is no queue to read.

        Resolving the queue set here rather than at each call site means no read
        path can forget to default it.
        """
        projections = [self._state_projection(queue_name) for queue_name in self._target_queues(queue_names)]
        if not projections:
            return None
        return "\nUNION ALL\n".join(projections)

    def _current_states_cte(
        self, queue_names: Iterable[str] | None, filters: dict[str, Any]
    ) -> tuple[str | None, dict[str, Any]]:
        """Build the ``WITH ... filtered`` prelude shared by the two query paths.

        Three steps, in this order for a reason:

        1. ``matched`` — the union, with the filters that hold for every attempt
           of a message pushed down so they run before the sort.
        2. ``current`` — one row per message id, resolved like
           :meth:`find_state` does. A retried message has several rows and the
           dashboard must see only its current state.
        3. ``filtered`` — the remaining filters, applied *after* deduplication so
           that filtering on ``status`` matches the current status rather than
           any past attempt's.
        """
        union = self._states_union(queue_names)
        if union is None:
            return None, {}

        params: dict[str, Any] = {}
        # A PGMQ database may hold queues that remoulade does not own; their rows
        # project to a NULL message_id and must not surface as states.
        pushdown = ["message_id IS NOT NULL"]
        for filter_name, column in _PUSHDOWN_FILTERS.items():
            values = filters.get(filter_name)
            if values:
                params[filter_name] = list(values)
                pushdown.append(f"{column} = ANY(CAST(:{filter_name} AS text[]))")
        pushdown_clause = f"WHERE {' AND '.join(pushdown)}"

        late = []
        statuses = filters.get("selected_statuses")
        if statuses:
            params["selected_statuses"] = list(statuses)
            late.append("status = ANY(CAST(:selected_statuses AS text[]))")
        # Both bounds apply to enqueued_datetime, as the previous SQL state
        # backend did, so the dashboard's date range keeps its meaning.
        for filter_name, operator in (("start_datetime", ">="), ("end_datetime", "<=")):
            bound = filters.get(filter_name)
            if bound is not None:
                params[filter_name] = _as_aware(bound)
                late.append(f"enqueued_datetime {operator} :{filter_name}")
        late_clause = f"WHERE {' AND '.join(late)}" if late else ""

        cte = f"""
            WITH matched AS (
                SELECT * FROM ({union}) AS states
                {pushdown_clause}
            ),
            current AS (
                SELECT DISTINCT ON (message_id) *
                FROM matched
                ORDER BY message_id, {_CURRENT_ATTEMPT_ORDER}
            ),
            filtered AS (
                SELECT * FROM current
                {late_clause}
            )
        """  # noqa: S608
        return cte, params

    @staticmethod
    def _order_by(sort_column: str | None, sort_direction: str | None, *, table: str = "", prefix: str = "") -> str:
        """Return a validated ``ORDER BY`` fragment.

        Column and direction are interpolated, not bound, so both are
        whitelisted. The default matches the previous SQL state backend: newest
        first. ``NULLS LAST`` keeps rows that never reached a given milestone
        (an unstarted message has no ``started_datetime``) out of the way.
        """
        column = sort_column or "enqueued_datetime"
        if column not in SORTABLE_COLUMNS:
            raise ValueError(f"cannot sort states on {column!r}")
        direction = (sort_direction or "desc").lower()
        if direction not in ("asc", "desc"):
            raise ValueError(f"invalid sort direction: {sort_direction!r}")
        qualifier = f"{table}." if table else ""
        return f"{qualifier}{prefix}{column} {direction} NULLS LAST"

    # -- execution ------------------------------------------------------------

    @contextmanager
    def _connection(self, conn: Connection | None, *, write: bool) -> Iterator[Connection]:
        """Yield the caller's connection, or borrow one from the pool.

        Writes need a transaction; reads do not, and should not pay for one.
        """
        if conn is not None:
            yield conn
            return
        opener = self.engine.begin if write else self.engine.connect
        with opener() as connection:
            yield connection

    def _run(self, statement: Any, params: dict[str, Any], conn: Connection | None) -> int:
        """Execute a write statement, returning the number of affected rows."""
        with self._connection(conn, write=True) as connection:
            return connection.execute(statement, params).rowcount

    def _fetch_one(self, statement: Any, params: dict[str, Any], conn: Connection | None) -> Row[Any] | None:
        """Execute a read statement, returning its first row."""
        with self._connection(conn, write=False) as connection:
            return connection.execute(statement, params).first()

    def _fetch_all(self, statement: Any, params: dict[str, Any], conn: Connection | None) -> list[Row[Any]]:
        """Execute a read statement, returning every row."""
        with self._connection(conn, write=False) as connection:
            return list(connection.execute(statement, params).all())


def _as_aware(value: datetime.datetime) -> datetime.datetime:
    """Assume naive datetimes are UTC, so comparisons against timestamptz behave."""
    if value.tzinfo is None:
        return value.replace(tzinfo=datetime.UTC)
    return value
