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
from collections.abc import Collection
from datetime import datetime
from typing import Any, ClassVar, Final, override

from ...broker import Broker
from ...encoder import Encoder
from ...message import Message
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


def _check_range(start: datetime | None, end: datetime | None) -> None:
    if start is not None and end is not None and start >= end:
        raise ValueError(
            f"start ({start}) must be earlier than end ({end}): the range is half-open, so it would match nothing."
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

    def purge(
        self,
        queue_name: str,
        *,
        include_active: bool = False,
        statuses: Collection[StateStatusesEnum] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        batch_size: int = 10_000,
    ) -> dict[str, int]:
        """Delete the archived messages of a queue, and its enqueued ones too when ``include_active``.

        Filters combine with AND: a timestamp in ``[start, end)`` -- when the message was
        archived for the archive, enqueued for the queue -- and a recorded status in
        ``statuses``; a row with no recorded status never matches a status filter.
        Deleting enqueued messages drops in-flight ones too: their ack then archives
        nothing, so they leave no trace.

        Runs in batches of ``batch_size`` rows, one transaction each; see
        :meth:`~remoulade.helpers.postgres_client.RemouladePostgresClient.delete_messages`
        for what that means under live workers.

        Returns:
          dict[str, int]: The number of deleted rows, by pgmq table name.

        Raises:
          ValueError: If ``queue_name`` cannot be used as a SQL identifier, if ``start``
            is not earlier than ``end`` (an empty range), or if ``statuses`` holds a
            status this backend never writes (both would match nothing), or if
            ``batch_size`` is below 1.
        """
        _check_range(start, end)
        if batch_size < 1:
            raise ValueError(f"batch_size must be at least 1, got {batch_size}.")
        if statuses is not None and not TERMINAL_STATUSES.issuperset(statuses):
            unwritten = sorted(status.value for status in set(statuses) - TERMINAL_STATUSES)
            raise ValueError(f"PostgresBackend never records {', '.join(unwritten)}, so no message can match them.")

        return self.broker.client.delete_messages(
            queue_name,
            include_active=include_active,
            statuses=None if statuses is None else [status.value for status in statuses],
            start=start,
            end=end,
            batch_size=batch_size,
        )

    def replay_failures(
        self,
        queue_name: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        actor_name: str | None = None,
        message_ids: Collection[str] | None = None,
        dry_run: bool = False,
    ) -> dict[int, Message[Any]]:
        """Enqueue again the messages of a queue whose last attempt failed.

        A replay keeps its ``message_id`` and starts over with no retry used. Never replays
        a message that already succeeded on a retry, one with an attempt still enqueued --
        so replaying twice in a row enqueues once -- nor any message of a pipeline or a
        group, whose composition already ended. Each message is replayed once, from its last
        attempt; ``start``/``end`` bound when that attempt was archived.

        Assumes a message's retries stay in its queue: with ``escalation_queue_mapping``, a
        message whose retry is pending or succeeded in the escalation queue is replayed anyway.

        Never run two replays of the same queue at once: both would read the same failures
        before either enqueues, and replay them twice.

        Returns:
          dict[int, Message]: The messages enqueued, or that would be with ``dry_run``, by the
            archived ``msg_id`` of their last attempt.

        Raises:
          ValueError: If ``queue_name`` cannot be used as a SQL identifier, or if ``start``
            is not earlier than ``end``.
        """
        _check_range(start, end)
        last_attempts = self.broker.client.find_last_attempts(
            queue_name,
            status=StateStatusesEnum.Failure.value,
            start=start,
            end=end,
            actor_name=actor_name,
            message_ids=None if message_ids is None else list(message_ids),
        )
        messages_by_msg_id = {
            msg_id: Message[Any].decode_json(payload).copy(queue_name=queue_name, options={"retries": 0})
            for msg_id, payload in last_attempts
        }
        if not dry_run and messages_by_msg_id:
            self.broker.enqueue_many(list(messages_by_msg_id.values()))
        return messages_by_msg_id
