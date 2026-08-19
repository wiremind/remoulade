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
"""A state backend that keeps message state inside the pgmq message itself."""

import datetime
from typing import Any, Final

from sqlalchemy import Row

from ...broker import Broker
from ...encoder import Encoder
from ...errors import ActorNotFound
from ...helpers.postgres_client import RemouladePostgresClient
from ...logging import get_logger
from ..backend import State, StateBackend, StateStatusesEnum

#: The only statuses ever written to a message's ``headers``. ``Pending`` and
#: ``Started`` are derived at read time from PGMQ's own ``read_ct`` and from
#: which table the row sits in.
TERMINAL_STATUSES: Final[frozenset[StateStatusesEnum]] = frozenset(
    {
        StateStatusesEnum.Success,
        StateStatusesEnum.Failure,
        StateStatusesEnum.Skipped,
        StateStatusesEnum.Canceled,
    }
)


class PostgresBackend(StateBackend):
    """A state backend backed by the pgmq message a state describes.

    Requires a :class:`~remoulade.brokers.postgres.PostgresBroker`, because it
    stores nothing of its own: a message's state *is* its pgmq row. PGMQ already
    records when a message was enqueued (``enqueued_at``), when it was last
    handed to a worker (``last_read_at``), how many times
    (``read_ct``), when it finished (``archived_at``), and — by moving the row
    from ``pgmq.q_<queue>`` to ``pgmq.a_<queue>`` — whether it finished at all.
    Everything else lives in the message payload.

    That leaves exactly one thing to store: which terminal state a finished
    message reached. That goes into the message's ``headers`` column, and the
    write is handed to the broker so it rides along with the archive that ack/nack
    performs anyway. The net cost of tracking a message's whole lifecycle is
    therefore **no extra statement at all**.

    Progress is not supported: storing it would mean an ``UPDATE`` on the
    broker's queue table per :meth:`remoulade.Message.set_progress` call, so that
    method raises with this backend.

    Consequences worth knowing:

    * ``ttl`` is ignored. Retention is the archive's, set by the broker's
      ``archive_retention_interval_in_days`` and enforced by pg_partman, so
      state history is bounded by how long archived messages are kept.
    * Purging or dropping a queue destroys its state along with its messages.
    * A retried message leaves one row per attempt, so the archive keeps a
      per-attempt trail. Reads always resolve to the current attempt.

    Parameters:
      broker(Broker): The PostgreSQL broker whose queues hold the state. When
        omitted, the global broker is resolved on first use.
      namespace(str): Unused; kept for interface compatibility. State is keyed
        by the message itself, not by a namespaced key.
      encoder(Encoder): Unused; ``args``/``kwargs``/``options`` are read back
        straight from the message's ``jsonb`` payload.
      max_size(int): Unused; nothing this backend stores in a header is unbounded.
    """

    def __init__(
        self,
        broker: Broker,
        *,
        namespace: str = "remoulade-state",
        encoder: Encoder | None = None,
        max_size: float = 2e6,
    ) -> None:
        """Build a backend reading through ``broker``.

        The broker is required, and checked here: a misconfiguration must raise
        while the application is being wired up. Failing later would be much
        worse, since the processing hooks run inside ``emit_before``/
        ``emit_after``, which log and swallow anything that is not a
        ``MiddlewareError`` — this backend would silently record nothing.

        Raises:
          ValueError: If ``broker`` is not a
            :class:`~remoulade.brokers.postgres.PostgresBroker`.
        """
        super().__init__(namespace=namespace, encoder=encoder, max_size=max_size)
        self.logger = get_logger(__name__, type(self))

        from ...brokers.postgres import PostgresBroker

        if not isinstance(broker, PostgresBroker):
            raise ValueError(
                f"{type(broker).__name__} cannot be used with PostgresBackend, which stores state inside the "
                "pgmq message; use a PostgresBroker, or pick another state backend."
            )
        self.broker = broker

    # -- broker plumbing ------------------------------------------------------

    @property
    def client(self) -> RemouladePostgresClient:
        """The broker's PGMQ client, which owns every statement this backend runs."""
        return self.broker.client

    # -- writes ---------------------------------------------------------------

    def set_state(self, state: State, ttl: int = 3600, *, message: Any = None) -> None:
        """Record whatever part of ``state`` is not already implied by the pgmq row.

        Pending and Started, along with all three timestamps, are derived at read
        time, so those calls do no I/O at all. A terminal state is staged on the
        message and persisted by the broker's archive, costing nothing on top of
        the ack. Only a terminal state reported without the message at hand costs
        a statement of its own.

        Raises:
          NotImplementedError: If the state carries a progress. This backend does
            not store progress, and accepting the call would mean silently
            dropping it.
        """
        if state.progress is not None:
            raise NotImplementedError(
                "PostgresBackend does not store progress: it would mean an UPDATE on the broker's queue table "
                "for every Message.set_progress call. Track long-running work through your metrics instead, "
                "or use another state backend."
            )

        # Pending/Started are read back from the pgmq row itself, so they are
        # nothing to write. The patch is merged with ``headers || patch``, hence
        # a status key only when there is a status: {"status": null} would wipe
        # a status already stored rather than leave it alone.
        if state.status not in TERMINAL_STATUSES:
            return
        patch: dict[str, Any] = {"status": state.status.value}

        # PostgresBackend refuses any other broker, so an in-flight proxy is
        # always a _PostgresMessage: it carries the patch to the archive for free.
        from ...brokers.postgres import _PostgresMessage

        if isinstance(message, _PostgresMessage) and message.stage_headers(patch):
            return

        queue_names = [state.queue_name] if state.queue_name else None
        self.client.patch_headers(state.message_id, patch, queue_names=queue_names)

    # -- reads ----------------------------------------------------------------

    def get_state(self, message_id: str) -> State | None:
        """Return a message's current state, or None when it is unknown.

        A message is unknown once its archived row falls outside the archive's
        retention window, so this is also how state expires.
        """
        row = self.client.find_state(message_id)
        return None if row is None else self._to_state(row)

    def get_states(
        self,
        *,
        size: int | None = None,
        offset: int = 0,
        selected_actors: list[str] | None = None,
        selected_statuses: list[str] | None = None,
        selected_message_ids: list[str] | None = None,
        selected_composition_ids: list[str] | None = None,
        start_datetime: datetime.datetime | None = None,
        end_datetime: datetime.datetime | None = None,
        sort_column: str | None = None,
        sort_direction: str | None = None,
    ) -> list[State]:
        """Return states matching the given filters.

        Unlike the Redis and stub backends, which ignore every filter and
        paginate in Python, filtering, sorting and pagination all happen in SQL.
        ``size`` counts compositions rather than messages: a page holds that many
        pipelines/groups and all of their messages, matching what
        :meth:`get_states_count` counts.
        """
        rows = self.client.select_states(
            size=size,
            offset=offset,
            sort_column=sort_column,
            sort_direction=sort_direction,
            selected_actors=selected_actors,
            selected_statuses=selected_statuses,
            selected_message_ids=selected_message_ids,
            selected_composition_ids=selected_composition_ids,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        return [self._to_state(row) for row in rows]

    def get_states_count(
        self,
        *,
        selected_actors: list[str] | None = None,
        selected_statuses: list[str] | None = None,
        selected_messages_ids: list[str] | None = None,
        selected_composition_ids: list[str] | None = None,
        start_datetime: datetime.datetime | None = None,
        end_datetime: datetime.datetime | None = None,
        **kwargs: Any,
    ) -> int:
        """Count the compositions matching the filters, to pair with :meth:`get_states`.

        The base class spells this filter ``selected_messages_ids`` while
        :meth:`get_states` spells it ``selected_message_ids``; both are accepted
        so a caller passing either reaches the same query.
        """
        return self.client.count_states(
            selected_actors=selected_actors,
            selected_statuses=selected_statuses,
            selected_message_ids=selected_messages_ids or kwargs.get("selected_message_ids"),
            selected_composition_ids=selected_composition_ids,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )

    def clean(self, max_age: int | None = None, not_started: bool = False) -> None:
        """Not supported: retention belongs to the archive, not to this backend.

        pg_partman drops archive partitions older than the broker's
        ``archive_retention_interval_in_days``, which is what expires state here.
        Deleting states independently would mean deleting messages.
        """
        raise NotImplementedError(
            "PostgresBackend does not clean states: they expire with the pgmq archive partitions, "
            "sized by PostgresBroker(archive_retention_interval_in_days=...)."
        )

    # -- row mapping ----------------------------------------------------------

    def _to_state(self, row: Row[Any]) -> State:
        """Turn a projected pgmq row into a :class:`~remoulade.state.State`.

        The status comes from the SQL projection, which is also what filtering and
        sorting run on. ``progress`` is always None: this backend does not store
        it.
        """
        return State(
            row.message_id,
            self._parse_status(row.status),
            actor_name=row.actor_name,
            args=row.args,
            kwargs=row.kwargs,
            options=row.options,
            priority=self._resolve_priority(row),
            enqueued_datetime=row.enqueued_datetime,
            started_datetime=row.started_datetime,
            end_datetime=row.end_datetime,
            queue_name=row.queue_name,
            composition_id=row.composition_id,
        )

    def _parse_status(self, status: str | None) -> StateStatusesEnum | None:
        """Read the projected status, tolerating a header written by something else."""
        if status is None:
            return None
        try:
            return StateStatusesEnum(status)
        except ValueError:
            self.logger.warning("Ignoring unknown status %r stored in a pgmq message header", status)
            return None

    def _resolve_priority(self, row: Row[Any]) -> int | None:
        """Return the message's priority, falling back to its actor's.

        A message only carries a priority in its options once ``Retries``
        escalated it, so for everything else the actor's configured priority is
        the answer.
        """
        if row.priority is not None:
            return int(row.priority)
        try:
            return self.broker.get_actor(row.actor_name).priority
        except ActorNotFound:
            return None
