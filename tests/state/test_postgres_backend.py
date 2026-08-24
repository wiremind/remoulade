"""Tests for the state backend that records a message's status in the pgmq message.

The point of this backend is that a message's lifecycle is already recorded by
PGMQ, so these tests assert on what is *not* written as much as on what is. It
implements no read path, so what was written is checked against the pgmq tables
directly.
"""

import re

import pytest
from sqlalchemy import event, text

import remoulade
from remoulade import Worker
from remoulade.brokers.stub import StubBroker
from remoulade.errors import QueueNotFound
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
                f'FROM pgmq."a_{queue_name}" ORDER BY msg_id'
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

    def test_a_terminal_status_without_a_delivery_id_writes_nothing(self, postgres_broker, postgres_state_backend):
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

    def test_a_failed_enqueue_records_nothing(self, postgres_broker, postgres_state_backend):
        """The one place the middleware reports a status without an in-flight message.

        ``MessageState.after_enqueue`` only saves a status when the enqueue raised, and
        an enqueue that raised wrote no row -- so there is nothing to record it on, and
        nothing to refuse either.
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
