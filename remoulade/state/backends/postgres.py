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

import json
from typing import ClassVar, Final, override

from ...broker import Broker
from ...encoder import Encoder
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
    keeps no store of its own: a status is written into the ``headers`` column of
    the very pgmq row that carries the message, which bounds what it can do.

    * Only the terminal statuses are stored, and only while the message is still
      enqueued. The rest of a lifecycle is already recorded by PGMQ
      (``enqueued_at``, ``last_read_at``, ``read_ct``, ``archived_at``, and which
      of ``pgmq.q_<queue>``/``pgmq.a_<queue>`` the row sits in).
    * Reads are not implemented: query the pgmq tables instead.
    * ``ttl`` is ignored. Retention is the archive's, set by the broker's
      ``archive_retention_interval_in_days``, so a status lives as long as its
      archived message. Purging or dropping a queue destroys the statuses too.
    * A retried message keeps its ``message_id`` and is re-enqueued as a new row,
      so the archive ends up with one row per attempt, each with its own status.

    Parameters:
      broker(Broker): The PostgreSQL broker whose queues hold the messages.
      namespace(str): Unused; kept for interface compatibility. A status is
        stored on the message itself, not under a namespaced key.
      encoder(Encoder): Unused; the only thing stored is a status string.
      max_size(int): Largest header patch this backend will write, in bytes. It caps
        the patch itself, not the ``headers`` column it is merged into: reading the
        column back would cost a statement the backend is built not to spend. A
        status alone never comes close to the default.
    """

    requires_ttl: ClassVar[bool] = False

    def __init__(
        self,
        broker: Broker,
        *,
        namespace: str = "remoulade-state",
        encoder: Encoder | None = None,
        max_size: float = 2e6,
    ) -> None:
        """Build a backend writing through ``broker``.

        The broker is checked here rather than on first write: the processing hooks
        run inside ``emit_before``/``emit_after``, which log and swallow anything
        that is not a ``MiddlewareError``, so a misconfiguration found later would
        silently record nothing.

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

    @override
    def set_state(self, state: State, ttl: int = 3600) -> None:
        """Record ``state``'s status on the pgmq row it was observed on.

        One ``UPDATE`` on the broker's queue table, on top of the archive that
        ack/nack performs anyway. A progress and the ``Pending``/``Started`` the row
        already implies write nothing at all.
        """
        if state.status not in TERMINAL_STATUSES:
            return

        if state.delivery_id is None:
            # Every in-flight hook reports a MessageProxy, so a terminal status with no
            # delivery id comes from the enqueue hooks, where MessageState only records
            # a Failure. Any other status here means the state was built by hand.
            if state.status is not StateStatusesEnum.Failure:
                self.broker.logger.warning(
                    "Could not record status %s for message %s: it carries no delivery_id, so there is no pgmq "
                    "row to record it on.",
                    state.status.value,
                    state.message_id,
                )
            return

        patch = {"status": state.status.value}
        if not self._fits(json.dumps(patch).encode()):
            self.broker.logger.warning(
                "Could not record status %s for message %s: its header patch is over the backend's max_size "
                "of %s bytes.",
                state.status.value,
                state.message_id,
                self.max_size,
            )
            return

        patched = self.broker.client.patch_headers(
            state.queue_name,
            state.delivery_id,
            patch,
        )
        if not patched:
            self.broker.logger.warning(
                "Could not record status %s for message %s: pgmq message %s is gone from queue %s, so it was "
                "either already archived or redelivered elsewhere.",
                state.status.value,
                state.message_id,
                state.delivery_id,
                state.queue_name,
            )
