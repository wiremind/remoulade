"""Tests for the state backend that records a message's status in the pgmq message.

The point of this backend is that a message's lifecycle is already recorded by
PGMQ, so these tests assert on what is *not* written as much as on what is. It
implements no read path, so what was written is checked against the pgmq tables
directly.
"""

import re
from datetime import UTC, datetime

import pytest
from sqlalchemy import event, text

import remoulade
from remoulade import Worker
from remoulade.brokers.stub import StubBroker
from remoulade.errors import QueueNotFound
from remoulade.helpers.pgmq_names import pgmq_table_name
from remoulade.middleware import CurrentMessage, SkipMessage
from remoulade.state import MessageState, State, StateStatusesEnum
from remoulade.state.backends import PostgresBackend


def _attach_state_middleware(broker, backend):
    broker.add_middleware(MessageState(backend))


def _drain(broker, queue_name="default", timeout=10_000):
    worker = Worker(broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        broker.join(queue_name, timeout=timeout)
        worker.join()
    finally:
        worker.stop()


def _archived_rows(broker, queue_name="default"):
    with broker.client.engine.begin() as connection:
        return connection.execute(
            text(
                f"SELECT msg_id, read_ct, headers, archived_at, message->>'message_id' AS message_id "  # noqa: S608
                f'FROM pgmq."{pgmq_table_name(queue_name, "a")}" ORDER BY msg_id'
            )
        ).all()


def _archived_status(broker, message_id, queue_name="default"):
    """The status stored on a message's last archived attempt."""
    statuses = [row.headers["status"] for row in _archived_rows(broker, queue_name) if row.message_id == message_id]
    return statuses[-1] if statuses else None


class _StatementRecorder:
    """Record the SQL a broker's engine actually runs."""

    def __init__(self, broker):
        self.statements: list[str] = []
        event.listen(broker.client.engine, "before_cursor_execute", self._record)
        self._broker = broker

    def _record(self, conn, cursor, statement, parameters, context, executemany):
        self.statements.append(" ".join(statement.split()))

    def stop(self):
        event.remove(self._broker.client.engine, "before_cursor_execute", self._record)

    @property
    def writes(self) -> list[str]:
        """Statements that touch a queue or archive table, ignoring bookkeeping.

        Remoulade's own SQL quotes the table name (``pgmq."q_default"``) while
        PGMQ's functions do not, so both spellings have to be matched.
        """
        table = re.compile(r'pgmq\."?[qa]_|pgmq\.archive')
        return [statement for statement in self.statements if table.search(statement)]


@pytest.mark.usefixtures("postgres_broker")
class TestWriteCost:
    """What tracking a status costs on the broker's queue table."""

    def test_tracking_state_adds_one_statement_per_message(self, postgres_broker, postgres_state_backend):
        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)

        do_work.send()
        recorder = _StatementRecorder(postgres_broker)
        try:
            _drain(postgres_broker)
            without_state = list(recorder.writes)
        finally:
            recorder.stop()

        _attach_state_middleware(postgres_broker, postgres_state_backend)
        do_work.send()
        recorder = _StatementRecorder(postgres_broker)
        try:
            _drain(postgres_broker)
            with_state = list(recorder.writes)
        finally:
            recorder.stop()

        assert len(without_state) == 1
        assert "pgmq.archive" in without_state[0]
        # The status is its own UPDATE, run before the ack archives the row.
        assert len(with_state) == 2
        assert 'UPDATE pgmq."q_default"' in with_state[0]
        assert "pgmq.archive" in with_state[1]

    def test_pending_and_started_write_nothing(self, postgres_broker, postgres_state_backend):
        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        recorder = _StatementRecorder(postgres_broker)
        try:
            do_work.send()
            # Only the enqueue itself; before_enqueue's Pending is implied by the row.
            assert [s for s in recorder.writes if "UPDATE" in s] == []
        finally:
            recorder.stop()


@pytest.mark.usefixtures("postgres_broker")
class TestTerminalStates:
    def test_failure_is_recorded(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor(max_retries=0)
        def boom():
            raise ValueError("kaboom")

        postgres_broker.declare_actor(boom)
        message = boom.send()
        _drain(postgres_broker)

        (row,) = _archived_rows(postgres_broker)
        assert row.message_id == message.message_id
        assert row.headers["status"] == "Failure"
        # archived_at is the archive table's own DEFAULT now().
        assert row.archived_at is not None

    def test_status_is_recorded_on_a_dotted_queue(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor(queue_name="etl.low")
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        message = do_work.send()
        recorder = _StatementRecorder(postgres_broker)
        try:
            _drain(postgres_broker, queue_name="etl.low")
        finally:
            recorder.stop()

        assert _archived_status(postgres_broker, message.message_id, queue_name="etl.low") == "Success"
        assert any('UPDATE pgmq."q_etl__low"' in statement for statement in recorder.writes)

    def test_success_is_recorded(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        message = do_work.send()
        _drain(postgres_broker)

        assert _archived_status(postgres_broker, message.message_id) == "Success"

    def test_skipped_is_not_reported_as_success(self, postgres_broker, postgres_state_backend):
        # ack means "success or skipped or canceled or being retried", so the
        # broker alone cannot tell these apart; the middleware can.
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def skipper():
            raise SkipMessage()

        postgres_broker.declare_actor(skipper)
        message = skipper.send()
        _drain(postgres_broker)

        assert _archived_status(postgres_broker, message.message_id) == "Skipped"

    def test_canceled_is_not_reported_as_success(self, postgres_broker, postgres_state_backend, cancel_backend):
        from remoulade.cancel import Cancel

        postgres_broker.add_middleware(Cancel(backend=cancel_backend))
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        message = do_work.send()
        message.cancel()
        _drain(postgres_broker)

        assert _archived_status(postgres_broker, message.message_id) == "Canceled"

    def test_a_terminal_status_without_a_delivery_id_is_dropped_and_logged(
        self, postgres_broker, postgres_state_backend, caplog
    ):
        """The status is written on a row named by its delivery id, so it is required.

        The middleware fills it from the in-flight message. A message id would not do:
        a retry is the same message re-enqueued, so one id can name several rows at once
        and nothing here would say which of them the status belongs to.
        """
        postgres_broker.declare_queue("default")
        recorder = _StatementRecorder(postgres_broker)
        try:
            postgres_state_backend.set_state(State("does-not-exist", StateStatusesEnum.Success, queue_name="default"))
        finally:
            recorder.stop()

        assert recorder.writes == []
        assert "carries no delivery_id" in caplog.text

    def test_a_patch_over_max_size_is_not_written(self, postgres_broker, postgres_state_backend, caplog):
        """``max_size`` caps the header patch, as it caps a stored field on the other backends.

        A status alone never comes near the default cap, so the guard is here to make the
        parameter mean the same thing on every backend -- and, unlike the field the other
        backends drop, it says in the log that it gave up.
        """
        postgres_broker.declare_queue("default")
        postgres_state_backend.max_size = 1

        recorder = _StatementRecorder(postgres_broker)
        try:
            postgres_state_backend.set_state(
                State("a-message", StateStatusesEnum.Success, queue_name="default", delivery_id=1)
            )
        finally:
            recorder.stop()

        assert recorder.writes == []
        assert "is over the backend's max_size" in caplog.text

    def test_a_failed_enqueue_records_nothing(self, postgres_broker, postgres_state_backend, caplog):
        """The one place the middleware reports a status without an in-flight message.

        ``MessageState.after_enqueue`` only saves a status when the enqueue raised, and
        an enqueue that raised wrote no row -- so there is nothing to record it on, and
        nothing to warn about either.
        """
        _attach_state_middleware(postgres_broker, postgres_state_backend)
        postgres_broker.declare_queue("default")

        @remoulade.actor(queue_name="default")
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        recorder = _StatementRecorder(postgres_broker)
        try:
            with pytest.raises(QueueNotFound):
                postgres_broker.enqueue(do_work.message().copy(queue_name="never-declared"))
        finally:
            recorder.stop()

        assert recorder.writes == []
        assert "carries no delivery_id" not in caplog.text


@pytest.mark.usefixtures("postgres_broker")
class TestRetries:
    def test_archive_keeps_one_row_per_attempt(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)
        attempts = []

        @remoulade.actor(max_retries=3, min_backoff=50, max_backoff=50, jitter=False)
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError(f"boom {len(attempts)}")

        postgres_broker.declare_actor(flaky)
        message = flaky.send()
        _drain(postgres_broker)

        assert len(attempts) == 3
        rows = _archived_rows(postgres_broker)
        # A retry reuses the message_id, so the archive becomes a per-attempt trail.
        assert [row.message_id for row in rows] == [message.message_id] * 3
        assert [row.headers["status"] for row in rows] == ["Failure", "Failure", "Success"]


@pytest.mark.usefixtures("postgres_broker")
class TestProgress:
    """This backend drops progress: storing it would cost an UPDATE per call."""

    def test_a_state_carrying_only_a_progress_runs_no_statement(self, postgres_broker, postgres_state_backend):
        postgres_broker.declare_queue("default")
        recorder = _StatementRecorder(postgres_broker)
        try:
            postgres_state_backend.set_state(State("some-id", progress=0.5))
        finally:
            recorder.stop()

        assert recorder.writes == []

    def test_an_actor_reporting_its_progress_keeps_working(self, postgres_broker, postgres_state_backend):
        """Raising here would fail the message mid-work and retry it forever."""
        postgres_broker.add_middleware(CurrentMessage())
        _attach_state_middleware(postgres_broker, postgres_state_backend)
        reported = []

        @remoulade.actor(max_retries=0)
        def reporting():
            for step in (0.25, 0.5, 1):
                CurrentMessage.get_current_message().set_progress(step)
                reported.append(step)

        postgres_broker.declare_actor(reporting)
        message = reporting.send()
        _drain(postgres_broker)

        assert reported == [0.25, 0.5, 1]
        assert _archived_status(postgres_broker, message.message_id) == "Success"


class TestConfiguration:
    def test_rejects_a_broker_that_cannot_carry_state(self):
        with pytest.raises(ValueError, match="cannot be used with PostgresBackend"):
            PostgresBackend(StubBroker())

    def test_the_broker_is_required(self):
        # Without it the backend could only resolve one later, and a
        # misconfiguration would surface as silently missing states.
        with pytest.raises(TypeError):
            PostgresBackend()  # type: ignore[call-arg]

    def test_keeps_the_broker_it_was_given(self, postgres_broker):
        assert PostgresBackend(postgres_broker).broker is postgres_broker

    @pytest.mark.usefixtures("postgres_broker")
    @pytest.mark.parametrize(
        ("method", "args"),
        [
            ("get_state", ("some-id",)),
            ("get_states", ()),
            ("get_states_count", ()),
            ("clean", ()),
        ],
    )
    def test_reads_are_not_supported(self, postgres_state_backend, method, args):
        # This backend only writes: reading a state back means querying the pgmq
        # tables, so the base class's NotImplementedError is the answer.
        with pytest.raises(NotImplementedError, match=f"PostgresBackend does not implement {method}"):
            getattr(postgres_state_backend, method)(*args)


def _send(broker, *, status=None, at=None, archive=True, queue_name="default"):
    """Write one pgmq row by hand: enqueued, or archived with ``status`` recorded on it.

    ``at`` overrides the timestamp the purge filters on: ``archived_at`` for an
    archived row, ``enqueued_at`` for an enqueued one.
    """
    msg_id = broker.client.send(queue_name, {}, headers={"status": status} if status else None)
    if archive:
        broker.client.archive(queue_name, msg_id)
    if at is not None:
        table, column = (f"a_{queue_name}", "archived_at") if archive else (f"q_{queue_name}", "enqueued_at")
        with broker.client.engine.begin() as connection:
            connection.execute(
                text(f'UPDATE pgmq."{table}" SET {column} = :at WHERE msg_id = :msg_id'),  # noqa: S608
                {"at": at, "msg_id": msg_id},
            )
    return msg_id


def _msg_ids(broker, table):
    with broker.client.engine.begin() as connection:
        return [row.msg_id for row in connection.execute(text(f'SELECT msg_id FROM pgmq."{table}" ORDER BY msg_id'))]  # noqa: S608


@pytest.mark.usefixtures("postgres_broker")
class TestPurge:
    @pytest.fixture(autouse=True)
    def _declare_queue(self, postgres_broker):
        postgres_broker.declare_queue("default")

    def test_only_archives_by_default(self, postgres_broker, postgres_state_backend):
        _send(postgres_broker, status="Success")
        _send(postgres_broker, status="Failure")
        active = _send(postgres_broker, archive=False)

        assert postgres_state_backend.purge("default") == {"a_default": 2}
        assert _msg_ids(postgres_broker, "a_default") == []
        assert _msg_ids(postgres_broker, "q_default") == [active]

    def test_include_active_also_empties_the_queue(self, postgres_broker, postgres_state_backend):
        _send(postgres_broker, status="Success")
        _send(postgres_broker, archive=False)
        _send(postgres_broker, archive=False)

        assert postgres_state_backend.purge("default", include_active=True) == {"a_default": 1, "q_default": 2}
        assert _msg_ids(postgres_broker, "a_default") == []
        assert _msg_ids(postgres_broker, "q_default") == []

    def test_purges_a_dotted_queue_under_its_pgmq_name(self, postgres_broker, postgres_state_backend):
        postgres_broker.declare_queue("etl.low")
        _send(postgres_broker, status="Success", queue_name="etl__low")
        _send(postgres_broker, archive=False, queue_name="etl__low")

        assert postgres_state_backend.purge("etl.low", include_active=True) == {"a_etl__low": 1, "q_etl__low": 1}
        assert _msg_ids(postgres_broker, "a_etl__low") == []
        assert _msg_ids(postgres_broker, "q_etl__low") == []

    def test_leaves_other_queues_alone(self, postgres_broker, postgres_state_backend):
        postgres_broker.declare_queue("other")
        archived = _send(postgres_broker, queue_name="other")
        active = _send(postgres_broker, archive=False, queue_name="other")

        postgres_state_backend.purge("default", include_active=True)

        assert _msg_ids(postgres_broker, "a_other") == [archived]
        assert _msg_ids(postgres_broker, "q_other") == [active]

    def test_status_filter_keeps_other_statuses_and_unrecorded_ones(self, postgres_broker, postgres_state_backend):
        _send(postgres_broker, status="Success")
        failure = _send(postgres_broker, status="Failure")
        unrecorded = _send(postgres_broker)

        assert postgres_state_backend.purge("default", statuses=[StateStatusesEnum.Success]) == {"a_default": 1}
        assert _msg_ids(postgres_broker, "a_default") == [failure, unrecorded]

    def test_archive_range_is_on_archived_at_and_half_open(self, postgres_broker, postgres_state_backend):
        before = _send(postgres_broker, at=datetime(2026, 1, 1, tzinfo=UTC))
        _send(postgres_broker, at=datetime(2026, 2, 1, tzinfo=UTC))
        at_end = _send(postgres_broker, at=datetime(2026, 3, 1, tzinfo=UTC))

        deleted = postgres_state_backend.purge(
            "default", start=datetime(2026, 2, 1, tzinfo=UTC), end=datetime(2026, 3, 1, tzinfo=UTC)
        )

        assert deleted == {"a_default": 1}
        assert _msg_ids(postgres_broker, "a_default") == [before, at_end]

    def test_queue_range_is_on_enqueued_at(self, postgres_broker, postgres_state_backend):
        _send(postgres_broker, archive=False, at=datetime(2026, 2, 1, tzinfo=UTC))
        outside = _send(postgres_broker, archive=False, at=datetime(2026, 3, 1, tzinfo=UTC))

        deleted = postgres_state_backend.purge(
            "default",
            include_active=True,
            start=datetime(2026, 2, 1, tzinfo=UTC),
            end=datetime(2026, 3, 1, tzinfo=UTC),
        )

        assert deleted == {"a_default": 0, "q_default": 1}
        assert _msg_ids(postgres_broker, "q_default") == [outside]

    def test_deletes_across_several_batches(self, postgres_broker, postgres_state_backend):
        for _ in range(5):
            _send(postgres_broker, status="Success")
        kept = _send(postgres_broker, status="Failure")

        deleted = postgres_state_backend.purge("default", statuses=[StateStatusesEnum.Success], batch_size=2)

        assert deleted == {"a_default": 5}
        assert _msg_ids(postgres_broker, "a_default") == [kept]

    def test_skips_rows_locked_by_a_worker(self, postgres_broker, postgres_state_backend):
        """A row a worker holds is left for it rather than waited on."""
        locked = _send(postgres_broker, archive=False)
        _send(postgres_broker, archive=False)

        with postgres_broker.client.engine.begin() as connection:
            connection.execute(
                text('SELECT 1 FROM pgmq."q_default" WHERE msg_id = :msg_id FOR UPDATE'), {"msg_id": locked}
            )
            deleted = postgres_state_backend.purge("default", include_active=True)

        assert deleted == {"a_default": 0, "q_default": 1}
        assert _msg_ids(postgres_broker, "q_default") == [locked]

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"queue_name": "bad name"}, "not a usable queue name"),
            (
                {
                    "queue_name": "default",
                    "start": datetime(2026, 2, 1, tzinfo=UTC),
                    "end": datetime(2026, 1, 1, tzinfo=UTC),
                },
                "must be earlier than",
            ),
            (
                {
                    "queue_name": "default",
                    "start": datetime(2026, 1, 1, tzinfo=UTC),
                    "end": datetime(2026, 1, 1, tzinfo=UTC),
                },
                "must be earlier than",
            ),
            ({"queue_name": "default", "statuses": [StateStatusesEnum.Pending]}, "never records Pending"),
            ({"queue_name": "default", "batch_size": 0}, "batch_size must be at least 1"),
        ],
    )
    def test_rejects_invalid_arguments(self, postgres_state_backend, kwargs, match):
        with pytest.raises(ValueError, match=match):
            postgres_state_backend.purge(**kwargs)


def _archived_attempts(broker, queue_name="default"):
    """``(msg_id, message_id, status)`` of every archived attempt, by ``msg_id``."""
    return [
        (row.msg_id, row.message_id, (row.headers or {}).get("status")) for row in _archived_rows(broker, queue_name)
    ]


def _enqueued_payloads(broker, queue_name="default"):
    with broker.client.engine.begin() as connection:
        return list(
            connection.execute(
                text(f'SELECT message FROM pgmq."{pgmq_table_name(queue_name, "q")}" ORDER BY msg_id')  # noqa: S608
            ).scalars()
        )


@pytest.mark.usefixtures("postgres_broker")
class TestReplayFailures:
    @pytest.fixture(autouse=True)
    def _track_states(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

    @pytest.fixture
    def actors(self, postgres_broker):
        """``boom`` fails until ``fixed`` is set, ``ok`` always succeeds."""
        fixed = []

        @remoulade.actor(max_retries=0)
        def boom(*args):
            if not fixed:
                raise RuntimeError("kaboom")

        @remoulade.actor(max_retries=0)
        def ok(*args):
            return None

        postgres_broker.declare_actor(boom)
        postgres_broker.declare_actor(ok)
        return boom, ok, fixed

    def test_replays_a_final_failure_with_its_message_id_and_no_retry_used(
        self, postgres_broker, postgres_state_backend, actors
    ):
        boom, _, _ = actors
        message = boom.send("an-argument")
        _drain(postgres_broker)
        ((archived_msg_id, _, _),) = _archived_attempts(postgres_broker)

        replayed = postgres_state_backend.replay_failures("default")

        assert list(replayed) == [archived_msg_id]
        (payload,) = _enqueued_payloads(postgres_broker)
        assert payload["message_id"] == message.message_id
        assert payload["args"] == ["an-argument"]
        assert payload["options"]["retries"] == 0

    def test_replays_a_failure_of_a_dotted_queue_onto_that_queue(self, postgres_broker, postgres_state_backend):
        @remoulade.actor(queue_name="etl.low", max_retries=0)
        def etl_boom():
            raise RuntimeError("kaboom")

        postgres_broker.declare_actor(etl_boom)
        message = etl_boom.send()
        _drain(postgres_broker, queue_name="etl.low")
        ((archived_msg_id, _, _),) = _archived_attempts(postgres_broker, queue_name="etl.low")

        replayed = postgres_state_backend.replay_failures("etl.low")

        assert list(replayed) == [archived_msg_id]
        (payload,) = _enqueued_payloads(postgres_broker, queue_name="etl.low")
        assert payload["message_id"] == message.message_id
        assert payload["queue_name"] == "etl.low"
        assert postgres_state_backend.replay_failures("etl.low") == {}

    def test_a_replay_runs_and_is_not_replayed_again(self, postgres_broker, postgres_state_backend, actors):
        boom, _, fixed = actors
        boom.send()
        _drain(postgres_broker)

        postgres_state_backend.replay_failures("default")
        # Still enqueued: a second run must not enqueue it twice.
        assert postgres_state_backend.replay_failures("default") == {}

        fixed.append(True)
        _drain(postgres_broker)
        assert [status for _, _, status in _archived_attempts(postgres_broker)] == ["Failure", "Success"]
        assert postgres_state_backend.replay_failures("default") == {}

    def test_a_message_that_succeeded_on_a_retry_is_not_replayed(self, postgres_broker, postgres_state_backend):
        attempts = []

        @remoulade.actor(max_retries=3, min_backoff=50, max_backoff=50, jitter=False)
        def flaky():
            attempts.append(1)
            if len(attempts) < 2:
                raise RuntimeError("boom")

        postgres_broker.declare_actor(flaky)
        flaky.send()
        _drain(postgres_broker)

        assert [status for _, _, status in _archived_attempts(postgres_broker)] == ["Failure", "Success"]
        assert postgres_state_backend.replay_failures("default") == {}

    def test_filters_on_actor_message_ids_and_archived_at(self, postgres_broker, postgres_state_backend, actors):
        boom, _, _ = actors

        @remoulade.actor(max_retries=0)
        def other_boom():
            raise RuntimeError("kaboom")

        postgres_broker.declare_actor(other_boom)
        boom.send()
        boom.send()
        other_boom.send()
        _drain(postgres_broker)
        (first, _, _), (second, second_message_id, _), (other, _, _) = _archived_attempts(postgres_broker)
        with postgres_broker.client.engine.begin() as connection:
            connection.execute(
                text('UPDATE pgmq."a_default" SET archived_at = :archived_at WHERE msg_id = :msg_id'),
                {"archived_at": datetime(2026, 1, 1, tzinfo=UTC), "msg_id": first},
            )

        assert list(postgres_state_backend.replay_failures("default", actor_name="other_boom", dry_run=True)) == [other]
        assert list(
            postgres_state_backend.replay_failures("default", message_ids=[second_message_id], dry_run=True)
        ) == [second]
        assert list(
            postgres_state_backend.replay_failures(
                "default", start=datetime(2026, 1, 1, tzinfo=UTC), end=datetime(2026, 1, 2, tzinfo=UTC), dry_run=True
            )
        ) == [first]

    def test_a_message_failed_on_every_retry_is_replayed_once(self, postgres_broker, postgres_state_backend):
        @remoulade.actor(max_retries=1, min_backoff=50, max_backoff=50, jitter=False)
        def always_fails():
            raise RuntimeError("kaboom")

        postgres_broker.declare_actor(always_fails)
        message = always_fails.send()
        _drain(postgres_broker)
        attempts = _archived_attempts(postgres_broker)
        assert [status for _, _, status in attempts] == ["Failure", "Failure"]

        replayed = postgres_state_backend.replay_failures("default", message_ids=[message.message_id], dry_run=True)

        assert list(replayed) == [attempts[-1][0]]

    def test_dry_run_enqueues_nothing(self, postgres_broker, postgres_state_backend, actors):
        boom, _, _ = actors
        boom.send()
        _drain(postgres_broker)

        assert len(postgres_state_backend.replay_failures("default", dry_run=True)) == 1
        assert _enqueued_payloads(postgres_broker) == []

    def test_messages_of_a_composition_are_never_replayed(
        self, postgres_broker, postgres_state_backend, actors, stub_result_backend
    ):
        from remoulade.results import Results

        postgres_broker.add_middleware(Results(backend=stub_result_backend))
        boom, ok, _ = actors
        standalone = boom.send()
        middle_of_pipeline = boom.message()
        last_of_pipeline = boom.message()
        member_of_simple_group = boom.message()
        member_of_group_then_step = boom.message()
        remoulade.pipeline((ok.message(), middle_of_pipeline, ok.message())).run()
        remoulade.pipeline((ok.message(), last_of_pipeline)).run()
        remoulade.group([member_of_simple_group]).run()
        (remoulade.group([member_of_group_then_step, ok.message()]) | ok.message()).run()
        _drain(postgres_broker)

        replayed = postgres_state_backend.replay_failures("default", dry_run=True)

        failed_message_ids = {
            message_id for _, message_id, status in _archived_attempts(postgres_broker) if status == "Failure"
        }
        composition_message_ids = {
            message.message_id
            for message in (middle_of_pipeline, last_of_pipeline, member_of_simple_group, member_of_group_then_step)
        }
        assert composition_message_ids <= failed_message_ids
        assert [message.message_id for message in replayed.values()] == [standalone.message_id]

    def test_rejects_an_empty_range(self, postgres_state_backend):
        moment = datetime(2026, 1, 1, tzinfo=UTC)
        with pytest.raises(ValueError, match="must be earlier than"):
            postgres_state_backend.replay_failures("default", start=moment, end=moment)
