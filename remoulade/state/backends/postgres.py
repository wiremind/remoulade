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
"""A state backend that records a message's status inside the pgmq message itself."""

from typing import Any, Final

from ...broker import Broker
from ...encoder import Encoder
from ...helpers.postgres_client import RemouladePostgresClient
from ..backend import State, StateBackend, StateStatusesEnum

#: The only statuses this backend writes. ``Pending`` and ``Started`` are already
#: implied by PGMQ's own ``read_ct`` and by which table the row sits in, so they
#: are nothing to store.
TERMINAL_STATUSES: Final[frozenset[StateStatusesEnum]] = frozenset(
    {
        StateStatusesEnum.Success,
        StateStatusesEnum.Failure,
        StateStatusesEnum.Skipped,
        StateStatusesEnum.Canceled,
    }
)


class PostgresBackend(StateBackend):
    """A write-only state backend that stores a message's status in its pgmq row.

    Requires a :class:`~remoulade.brokers.postgres.PostgresBroker`, because it
    keeps no store of its own: a message's status is written into the ``headers``
    column of the very pgmq row that carries the message.

    Only :meth:`set_state` is implemented, and only for the terminal statuses:
    everything else about a message's lifecycle is already recorded by PGMQ
    (``enqueued_at``, ``last_read_at``, ``read_ct``, ``archived_at``, and which of
    ``pgmq.q_<queue>``/``pgmq.a_<queue>`` the row sits in) or by the message
    payload

    Consequences worth knowing:

    * ``ttl`` is ignored. Retention is the archive's, set by the broker's
      ``archive_retention_interval_in_days`` and enforced by pg_partman, so how
      long a status is kept is how long its archived message is kept.
    * Purging or dropping a queue destroys the statuses along with the messages.
    * A retried message keeps its ``message_id`` and is re-enqueued as a new row,
      so the archive ends up with one row per attempt, each with its own status.

    Parameters:
      broker(Broker): The PostgreSQL broker whose queues hold the messages.
      namespace(str): Unused; kept for interface compatibility. A status is
        stored on the message itself, not under a namespaced key.
      encoder(Encoder): Unused; the only thing stored is a status string.
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
        """Build a backend writing through ``broker``.

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

        from ...brokers.postgres import PostgresBroker

        if not isinstance(broker, PostgresBroker):
            raise ValueError(
                f"{type(broker).__name__} cannot be used with PostgresBackend, which stores state inside the "
                "pgmq message; use a PostgresBroker, or pick another state backend."
            )
        self.broker = broker

    @property
    def client(self) -> RemouladePostgresClient:
        """The broker's PGMQ client, which owns every statement this backend runs."""
        return self.broker.client

    def set_state(self, state: State, ttl: int = 3600, *, message: Any = None) -> None:
        """Record ``state``'s status on the message it belongs to.

        This backend runs no statement of its own: the status is staged on the
        in-flight message and written by the archive the broker performs on ack or
        nack anyway. It therefore needs that message, and ``message`` is where it
        comes from.

        ``Pending`` and ``Started`` do no I/O at all -- PGMQ's own ``enqueued_at``,
        ``read_ct`` and the table the row sits in already record them -- and a progress
        the state carries is ignored, since storing one would mean an ``UPDATE`` on the
        broker's queue table per ``Message.set_progress`` call.

        Raises:
          NotImplementedError: If no ``message`` is given. Without it there is nothing
            to stage the status on, and a message id is not enough to find the row:
            a retry is the same message re-enqueued, so an id can name several rows at
            once and nothing here would say which one the status belongs to.
        """
        # Pending/Started are already implied by the pgmq row itself, so they are
        # nothing to write.
        if state.status not in TERMINAL_STATUSES:
            return
        patch: dict[str, Any] = {"status": state.status.value}

        # PostgresBackend refuses any other broker, so an in-flight proxy is always a
        # PostgresMessage: it carries the patch to the archive for free.
        from ...brokers.postgres import PostgresMessage

        if isinstance(message, PostgresMessage):
            message.stage_headers(patch)
            return

        if message is not None:
            # A plain Message comes from the enqueue hooks, where MessageState records a
            # terminal status only when the enqueue raised -- and an enqueue that raised
            # wrote no row, so there is nothing to record it on.
            return

        raise NotImplementedError(
            f"PostgresBackend cannot record {state.status.value} for message {state.message_id!r} without the "
            "message itself: it stores a status inside the pgmq message rather than in a table of its own. Pass "
            "message= (the middleware always does), or use another state backend."
        )
