"""Tests for the state backend that stores state inside the pgmq message itself.

The point of this backend is that a message's lifecycle is already recorded by
PGMQ, so most of these tests assert on what is *not* written as much as on what
is read back.
"""

import datetime
import os
import re

import pytest
from sqlalchemy import event, text

import remoulade
from remoulade import Worker
from remoulade.brokers.stub import StubBroker
from remoulade.middleware import CurrentMessage, SkipMessage
from remoulade.state import MessageState, State, StateStatusesEnum
from remoulade.state.backends import PostgresBackend

TEST_POSTGRES_URL = os.getenv("REMOULADE_TEST_DB_URL") or "postgresql://remoulade@localhost:5544/test"


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
                f"SELECT msg_id, read_ct, headers, message->>'message_id' AS message_id "  # noqa: S608
                f'FROM pgmq."a_{queue_name}" ORDER BY msg_id'
            )
        ).all()


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
class TestDerivedState:
    """Pending, Started and the timestamps are read off pgmq's own columns."""

    def test_pending_is_derived_from_an_unread_message(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def do_work(x, key=None):
            return x

        postgres_broker.declare_actor(do_work)
        message = do_work.send_with_options(args=(21,), kwargs={"key": "value"})

        state = postgres_state_backend.get_state(message.message_id)
        assert state.status is StateStatusesEnum.Pending
        assert state.actor_name == "do_work"
        assert state.args == [21]
        assert state.kwargs == {"key": "value"}
        assert state.queue_name == "default"
        # enqueued_at is PGMQ's own column, so it is set without remoulade writing it.
        assert state.enqueued_datetime is not None
        assert state.started_datetime is None
        assert state.end_datetime is None

    def test_priority_falls_back_to_the_actor(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor(priority=7)
        def important():
            return None

        postgres_broker.declare_actor(important)
        message = important.send()

        # A message only carries a priority in its options once Retries escalated it.
        assert postgres_state_backend.get_state(message.message_id).priority == 7

    def test_success_and_timestamps_after_processing(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        message = do_work.send()
        _drain(postgres_broker)

        state = postgres_state_backend.get_state(message.message_id)
        assert state.status is StateStatusesEnum.Success
        assert state.started_datetime is not None  # PGMQ's last_read_at
        assert state.end_datetime is not None  # the archive's archived_at

    def test_unknown_message_has_no_state(self, postgres_state_backend):
        assert postgres_state_backend.get_state("does-not-exist") is None


@pytest.mark.usefixtures("postgres_broker")
class TestWriteCost:
    """The terminal state must not cost a statement of its own."""

    def test_tracking_state_adds_no_statement(self, postgres_broker, postgres_state_backend):
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

        assert len(with_state) == len(without_state) == 1
        # The one statement changed shape rather than gaining a sibling: the
        # header patch rides along with the archive ack performs anyway.
        assert "pgmq.archive" in without_state[0]
        assert "DELETE FROM pgmq." in with_state[0]

    def test_pending_and_started_write_nothing(self, postgres_broker, postgres_state_backend):
        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        recorder = _StatementRecorder(postgres_broker)
        try:
            do_work.send()
            # Only the enqueue itself; before_enqueue's Pending is derived.
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

        state = postgres_state_backend.get_state(message.message_id)
        assert state.status is StateStatusesEnum.Failure
        assert state.end_datetime is not None

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

        assert postgres_state_backend.get_state(message.message_id).status is StateStatusesEnum.Skipped

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

        assert postgres_state_backend.get_state(message.message_id).status is StateStatusesEnum.Canceled


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

    def test_current_state_is_the_latest_attempt(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)
        attempts = []

        @remoulade.actor(max_retries=3, min_backoff=50, max_backoff=50, jitter=False)
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise RuntimeError("boom")

        postgres_broker.declare_actor(flaky)
        message = flaky.send()
        _drain(postgres_broker)

        # The two archived failures must not shadow the successful attempt, and
        # must not leak into a filter on the current status either.
        assert postgres_state_backend.get_state(message.message_id).status is StateStatusesEnum.Success
        assert postgres_state_backend.get_states_count(selected_statuses=["Failure"]) == 0
        assert postgres_state_backend.get_states_count(selected_statuses=["Success"]) == 1

    def test_resolves_across_queues_after_an_escalation(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)
        attempts = []

        @remoulade.actor(
            max_retries=1,
            min_backoff=50,
            max_backoff=50,
            jitter=False,
            escalation_queue_mapping={"default": "escalated"},
        )
        def escalating():
            attempts.append(1)
            if len(attempts) < 2:
                raise RuntimeError("boom")

        postgres_broker.declare_actor(escalating)
        postgres_broker.declare_queue("escalated")
        message = escalating.send()
        _drain(postgres_broker)
        _drain(postgres_broker, "escalated")

        # The same message_id now exists in two queues; the lookup spans both.
        state = postgres_state_backend.get_state(message.message_id)
        assert state.status is StateStatusesEnum.Success
        assert state.queue_name == "escalated"


@pytest.mark.usefixtures("postgres_broker")
class TestProgress:
    """This backend does not store progress, and says so rather than dropping it."""

    def test_set_state_refuses_a_progress(self, postgres_state_backend):
        with pytest.raises(NotImplementedError, match="does not store progress"):
            postgres_state_backend.set_state(State("some-id", progress=0.5))

    def test_set_progress_from_an_actor_raises(self, postgres_broker, postgres_state_backend):
        postgres_broker.add_middleware(CurrentMessage())
        _attach_state_middleware(postgres_broker, postgres_state_backend)
        failures = []

        @remoulade.actor(max_retries=0)
        def reporting():
            try:
                CurrentMessage.get_current_message().set_progress(0.5)
            except NotImplementedError:
                failures.append(True)

        postgres_broker.declare_actor(reporting)
        message = reporting.send()
        _drain(postgres_broker)

        assert failures == [True]
        # The message itself still completes and is recorded normally.
        assert postgres_state_backend.get_state(message.message_id).status is StateStatusesEnum.Success

    def test_states_never_carry_a_progress(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        message = do_work.send()

        # Still sortable, because the HTTP API accepts progress as a sort column.
        assert postgres_state_backend.get_state(message.message_id).progress is None
        assert postgres_state_backend.get_states(sort_column="progress") != []


@pytest.mark.usefixtures("postgres_broker")
class TestQueries:
    @pytest.fixture
    def populated(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def alpha(x):
            return x

        @remoulade.actor
        def beta(x):
            return x

        postgres_broker.declare_actor(alpha)
        postgres_broker.declare_actor(beta)
        messages = [alpha.send(1), alpha.send(2), beta.send(3)]
        return postgres_state_backend, messages

    def test_filters_by_actor(self, populated):
        backend, _ = populated
        assert backend.get_states_count(selected_actors=["alpha"]) == 2
        assert backend.get_states_count(selected_actors=["beta"]) == 1
        assert backend.get_states_count(selected_actors=["gamma"]) == 0
        assert {state.actor_name for state in backend.get_states(selected_actors=["beta"])} == {"beta"}

    def test_filters_by_message_id(self, populated):
        backend, messages = populated
        states = backend.get_states(selected_message_ids=[messages[0].message_id])
        assert [state.message_id for state in states] == [messages[0].message_id]

    def test_filters_by_status(self, populated):
        backend, _ = populated
        assert backend.get_states_count(selected_statuses=["Pending"]) == 3
        assert backend.get_states_count(selected_statuses=["Success"]) == 0

    def test_filters_by_enqueued_datetime(self, populated):
        backend, _ = populated
        now = datetime.datetime.now(datetime.UTC)
        assert backend.get_states_count(start_datetime=now - datetime.timedelta(minutes=1)) == 3
        assert backend.get_states_count(start_datetime=now + datetime.timedelta(minutes=1)) == 0
        assert backend.get_states_count(end_datetime=now - datetime.timedelta(minutes=1)) == 0

    def test_sorts_and_paginates(self, populated):
        backend, messages = populated
        ascending = backend.get_states(sort_column="enqueued_datetime", sort_direction="asc")
        assert [state.message_id for state in ascending] == [message.message_id for message in messages]

        descending = backend.get_states(sort_column="enqueued_datetime", sort_direction="desc")
        assert [state.message_id for state in descending] == [message.message_id for message in reversed(messages)]

        first_page = backend.get_states(size=2, sort_column="enqueued_datetime", sort_direction="asc")
        assert [state.message_id for state in first_page] == [messages[0].message_id, messages[1].message_id]
        assert [state.message_id for state in backend.get_states(size=2, offset=2)] == [messages[0].message_id]

    def test_rejects_an_unknown_sort_column(self, populated):
        backend, _ = populated
        with pytest.raises(ValueError, match="cannot sort states"):
            backend.get_states(sort_column="args; DROP TABLE pgmq.meta")

    def test_rejects_an_unknown_sort_direction(self, populated):
        backend, _ = populated
        with pytest.raises(ValueError, match="sort direction"):
            backend.get_states(sort_column="status", sort_direction="sideways")

    def test_ignores_queues_remoulade_does_not_own(self, postgres_broker, postgres_state_backend):
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def alpha():
            return None

        postgres_broker.declare_actor(alpha)
        alpha.send()

        # A shared database may hold pgmq queues written by something else;
        # their rows are not remoulade states.
        postgres_broker.client.create_queue("foreign_queue")
        postgres_broker.client.send("foreign_queue", {"totally": "unrelated"})

        assert postgres_state_backend.get_states_count() == 1
        assert [state.actor_name for state in postgres_state_backend.get_states()] == ["alpha"]


@pytest.mark.usefixtures("postgres_broker")
class TestCompositions:
    def test_pagination_counts_compositions_not_messages(self, postgres_broker, postgres_state_backend, result_backend):
        from remoulade import group
        from remoulade.results import Results

        postgres_broker.add_middleware(Results(backend=result_backend))
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor(store_results=True)
        def child(x):
            return x

        postgres_broker.declare_actor(child)
        composition = group([child.message(1), child.message(2), child.message(3)])
        composition.run()

        states = postgres_state_backend.get_states()
        assert len(states) == 3
        composition_ids = {state.composition_id for state in states}
        assert len(composition_ids) == 1 and None not in composition_ids

        # One composition, so one page entry -- but the page carries all of its
        # messages, which is the contract the dashboard was built against.
        assert postgres_state_backend.get_states_count() == 1
        assert len(postgres_state_backend.get_states(size=1)) == 3

    def test_filters_by_composition_id(self, postgres_broker, postgres_state_backend, result_backend):
        from remoulade import group
        from remoulade.results import Results

        postgres_broker.add_middleware(Results(backend=result_backend))
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor(store_results=True)
        def child(x):
            return x

        postgres_broker.declare_actor(child)
        group([child.message(1), child.message(2)]).run()
        composition_id = postgres_state_backend.get_states()[0].composition_id

        assert postgres_state_backend.get_states_count(selected_composition_ids=[composition_id]) == 1
        assert postgres_state_backend.get_states_count(selected_composition_ids=["nope"]) == 0


@pytest.mark.usefixtures("postgres_broker")
class TestIndexes:
    def test_message_id_lookup_can_use_an_index_on_every_partition(self, postgres_broker, postgres_state_backend):
        """A missing index here degrades quietly, then catastrophically at scale.

        The test tables are tiny, so the planner would rightly prefer a seq scan;
        disabling it asserts what matters, that a usable index exists on every
        partition of both the queue and the archive.
        """
        _attach_state_middleware(postgres_broker, postgres_state_backend)

        @remoulade.actor
        def do_work():
            return None

        postgres_broker.declare_actor(do_work)
        do_work.send()
        _drain(postgres_broker)

        union = postgres_broker.client._states_union(["default"])
        with postgres_broker.client.engine.begin() as connection:
            # SET LOCAL, not SET: a plain SET would follow the connection back
            # into the pool and silently change later queries' plans.
            connection.execute(text("SET LOCAL enable_seqscan = off"))
            plan = "\n".join(
                row[0]
                for row in connection.execute(
                    text(f"EXPLAIN SELECT * FROM ({union}) AS s WHERE s.message_id = 'whatever'")  # noqa: S608
                ).all()
            )

        assert "Seq Scan" not in plan
        # The filter is pushed down through the UNION ALL into each partition.
        assert plan.count("(message ->> 'message_id'::text) = 'whatever'::text") >= 2


@pytest.mark.usefixtures("postgres_broker")
class TestDashboardApi:
    """The API marshals states strictly, so it is worth exercising for real."""

    @pytest.fixture
    def api_client(self, postgres_broker, postgres_state_backend):
        from remoulade.api.main import app

        _attach_state_middleware(postgres_broker, postgres_state_backend)
        with app.test_client() as client:
            yield client

    def test_lists_and_fetches_states(self, postgres_broker, api_client):
        @remoulade.actor
        def do_work(x):
            return x

        postgres_broker.declare_actor(do_work)
        message = do_work.send(1)

        listed = api_client.post("/messages/states", json={})
        assert listed.status_code == 200
        assert listed.json["count"] == 1
        assert listed.json["data"][0]["message_id"] == message.message_id
        assert listed.json["data"][0]["status"] == "Pending"

        fetched = api_client.get(f"/messages/states/{message.message_id}")
        assert fetched.status_code == 200
        assert fetched.json["args"] == [1]
        assert api_client.get("/messages/states/unknown").status_code == 404


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
    def test_clean_is_not_supported(self, postgres_state_backend):
        # Retention belongs to the archive partitions, not to the backend.
        with pytest.raises(NotImplementedError, match="archive_retention_interval_in_days"):
            postgres_state_backend.clean(max_age=10)
