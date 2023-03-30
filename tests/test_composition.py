import threading
import time
from threading import Condition
from unittest import mock
from unittest.mock import call, patch

import pytest

import remoulade
from remoulade import CollectionResults, group, pipeline
from remoulade.results import ErrorStored, MessageIdsMissing, ResultMissing, Results, ResultTimeout
from remoulade.results.backends import StubBackend
from tests.common import get_logs
from tests.conftest import fast_backoff, mock_func


def test_messages_can_be_piped(stub_broker):
    # Given an actor that adds two numbers together
    @remoulade.actor
    def add(x, y):
        return x + y

    # And this actor is declared
    stub_broker.declare_actor(add)

    # When I pipe some messages intended for that actor together
    pipe = add.message(1, 2) | add.message(3) | add.message(4)

    # Then I should get back a pipeline object
    assert isinstance(pipe, pipeline)

    def filter_options(message):
        return {key: value for (key, value) in message.items() if key != "options"}

    # If I build a pipeline
    targets = pipe.build()
    assert len(targets) == 1
    first_target = targets[0]
    assert len(first_target.options["pipe_target"]) == 1
    # And each message in the pipeline should reference the next message in line
    assert filter_options(first_target.options["pipe_target"][0]) == filter_options(pipe.children[1].asdict())

    assert len(first_target.options["pipe_target"]) == 1
    second_target = first_target.options["pipe_target"][0]
    assert filter_options(second_target["options"]["pipe_target"][0]) == filter_options(pipe.children[2].asdict())

    assert len(second_target["options"]["pipe_target"]) == 1
    third_target = second_target["options"]["pipe_target"][0]
    assert "pipe_target" not in third_target["options"]


def test_pipelines_flatten_child_pipelines(stub_broker):
    # Given an actor that adds two numbers together
    @remoulade.actor
    def add(x, y):
        return x + y

    # And this actor is declared
    stub_broker.declare_actor(add)

    # When I pipe a message intended for that actor and another pipeline together
    pipe = pipeline([add.message(1, 2), add.message(3) | add.message(4), add.message(5)])

    # Then the inner pipeline should be flattened into the outer pipeline
    assert len(pipe) == 4
    assert pipe.children[0].args == (1, 2)
    assert pipe.children[1].args == (3,)
    assert pipe.children[2].args == (4,)
    assert pipe.children[3].args == (5,)


def test_pipe_ignore_message_options(stub_broker, stub_worker, result_backend):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that return something
    @remoulade.actor(store_results=True)
    def do_nothing():
        return 0

    # Nothing should be sent to pipe ignored
    @remoulade.actor(store_results=True)
    def pipe_ignored(*args):
        assert len(args) == 0
        return 1

    # And these actors are declared
    stub_broker.declare_actor(do_nothing)
    stub_broker.declare_actor(pipe_ignored)

    pipe = do_nothing.message() | pipe_ignored.message_with_options(pipe_ignore=True)
    pipe.run()

    assert pipe.result.get(block=True) == 1


def test_pipe_ignore_actor_options(stub_broker, stub_worker, result_backend):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that return something
    @remoulade.actor(store_results=True)
    def do_nothing():
        return 0

    # Nothing should be sent to pipe ignored
    @remoulade.actor(store_results=True, pipe_ignore=True)
    def pipe_ignored(*args):
        assert len(args) == 0
        return 1

    # And these actors are declared
    stub_broker.declare_actor(do_nothing)
    stub_broker.declare_actor(pipe_ignored)

    pipe = do_nothing.message() | pipe_ignored.message()
    pipe.run()

    assert pipe.result.get(block=True) == 1


def test_pipeline_results_can_be_retrieved(stub_broker, stub_worker, result_backend):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that adds two numbers together and stores the result
    @remoulade.actor(store_results=True)
    def add(x, y):
        return x + y

    # And this actor is declared
    stub_broker.declare_actor(add)

    # When I pipe some messages intended for that actor together and run the pipeline
    pipe = add.message(1, 2) | (add.message(3) | add.message(4))
    pipe.run()

    # Then the pipeline result should be the sum of 1, 2, 3 and 4
    assert pipe.result.get(block=True) == 10

    # And I should be able to retrieve individual results
    assert list(pipe.results.get()) == [3, 6, 10]


def test_pipeline_results_respect_timeouts(stub_broker, stub_worker, result_backend):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that waits some amount of time then doubles that amount
    @remoulade.actor(store_results=True)
    def wait(n):
        time.sleep(n)
        return n * 2

    # And this actor is declared
    stub_broker.declare_actor(wait)

    # When I pipe some messages intended for that actor together and run the pipeline
    pipe = wait.message(1) | wait.message() | wait.message()
    pipe.run()

    # And get the results with a lower timeout than the tasks can complete in
    # Then a ResultTimeout error should be raised
    with pytest.raises(ResultTimeout):
        for _ in pipe.results.get(block=True, timeout=1000):
            pass


def test_pipelines_expose_completion_stats(stub_broker, stub_worker, result_backend):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that waits some amount of time
    result_backend.store_results, event_result = mock_func(result_backend.store_results)
    event_count = [threading.Event() for _ in range(4)]

    @remoulade.actor(store_results=True)
    def wait(n):
        event_count[n].wait(3)
        return n + 1

    # And this actor is declared
    stub_broker.declare_actor(wait)

    # When I pipe some messages intended for that actor together and run the pipeline
    pipe = wait.message(0) | wait.message() | wait.message() | wait.message()
    pipe.run()

    # Then every time a job in the pipeline completes, the completed_count should increase
    for count in range(0, len(pipe)):
        event_count[count].set()
        event_result.wait(2)
        event_result.clear()
        assert pipe.results.completed_count == count + 1

    # Finally, completed should be true
    assert pipe.results.completed


def test_pipelines_can_be_incomplete(stub_broker, result_backend):
    # Given that I am not running a worker
    # And I have a result backend
    stub_broker.add_middleware(Results(backend=result_backend))

    # And I have an actor that does nothing
    @remoulade.actor(store_results=True)
    def do_nothing():
        return None

    # And this actor is declared
    stub_broker.declare_actor(do_nothing)

    # And I've run a pipeline
    pipe = do_nothing.message() | do_nothing.message_with_options(pipe_ignore=True)
    pipe.run()

    # When I check if the pipeline has completed
    # Then it should return False
    assert not pipe.results.completed


@pytest.mark.parametrize("store_results", [True, False])
def test_pipelines_store_results_error(stub_broker, result_backend, stub_worker, store_results):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that fail
    @remoulade.actor(store_results=store_results)
    def do_work_fail():
        raise ValueError()

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And these actors are declared
    stub_broker.declare_actor(do_work_fail)
    stub_broker.declare_actor(do_work)

    # And I've run a pipeline
    g = group([do_work.message(), do_work.message(), do_work.message()])
    pipe = do_work_fail.message() | do_work.message() | g | do_work.message()
    pipe.run()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # I get an error
    if store_results:
        with pytest.raises(ErrorStored) as e:
            pipe.children[0].result.get()
        assert str(e.value) == "ValueError()"

    for i in [1, 3]:
        with pytest.raises(ErrorStored) as e:
            pipe.children[i].result.get()
        assert str(e.value).startswith("ParentFailed")

    for child in g.children:
        with pytest.raises(ErrorStored) as e:
            child.result.get()
        assert str(e.value).startswith("ParentFailed")


def test_groups_execute_jobs_in_parallel(stub_broker, stub_worker, result_backend):
    # Given that I have a result backend
    stub_broker.add_middleware(Results(backend=result_backend))

    # And I have an actor that sleeps for 100ms
    @remoulade.actor(store_results=True)
    def wait():
        time.sleep(0.1)

    # And this actor is declared
    stub_broker.declare_actor(wait)

    # When I group multiple of these actors together and run them
    t = time.monotonic()
    g = group([wait.message() for _ in range(5)])
    g.run()

    # group message_ids are no stored if not needed
    with pytest.raises(MessageIdsMissing):
        result_backend.get_group_message_ids(g.group_id)

    # And wait on the group to complete
    results = list(g.results.get(block=True))

    # Then the total elapsed time should be less than 500ms
    assert time.monotonic() - t <= 0.5

    # And I should get back as many results as there were jobs in the group
    assert len(results) == len(g)

    # And the group should be completed
    assert g.results.completed
    assert isinstance(g.results, CollectionResults)


def test_inner_groups_forbidden(stub_broker, stub_worker, result_backend):
    # Given that I have a result backend
    stub_broker.add_middleware(Results(backend=result_backend))

    # And I have an actor
    @remoulade.actor()
    def do_work():
        return 1

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # groups of groups are forbidden
    with pytest.raises(ValueError):
        group(group(do_work.message() for _ in range(2)) for _ in range(3))


def test_groups_can_time_out(stub_broker, stub_worker, result_backend):
    # Given that I have a result backend
    stub_broker.add_middleware(Results(backend=result_backend))

    # And I have an actor that sleeps for 300ms
    @remoulade.actor(store_results=True)
    def wait():
        time.sleep(0.3)

    # And this actor is declared
    stub_broker.declare_actor(wait)

    # When I group a few jobs together and run it
    g = group(wait.message() for _ in range(2))
    g.run()

    # And wait for the group to complete with a timeout
    # Then a ResultTimeout error should be raised
    with pytest.raises(ResultTimeout):
        g.results.wait(timeout=100)

    # And the group should not be completed
    assert not g.results.completed


def test_groups_expose_completion_stats(stub_broker, stub_worker, result_backend):
    # Given that I have a result backend
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that waits some amount of time
    condition = Condition()

    @remoulade.actor(store_results=True)
    def wait(n):
        time.sleep(n)
        with condition:
            condition.notify_all()
            return n

    # And this actor is declared
    stub_broker.declare_actor(wait)

    # When I group messages of varying durations together and run the group
    g = group(wait.message(n) for n in range(1, 4))
    g.run()

    # Then every time a job in the group completes, the completed_count should increase
    for count in range(1, len(g) + 1):
        with condition:
            condition.wait(5)
            time.sleep(0.1)  # give the worker time to set the result
            assert g.results.completed_count == count

    # Finally, completed should be true
    assert g.results.completed


@pytest.mark.parametrize("block", [True, False])
def test_group_forget(stub_broker, result_middleware, stub_worker, block):
    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # And I've run a group
    messages = [do_work.message() for _ in range(5)]
    g = group(messages)
    g.run()

    # If i wait for the group to be completed
    if not block:
        stub_broker.join(do_work.queue_name)
        stub_worker.join()

    # If i forget the results
    results = g.results.get(block=block, forget=True)
    assert list(results) == [42] * 5

    # All messages have been forgotten
    results = g.results.get()
    assert list(results) == [None] * 5


def test_group_wait_forget(stub_broker, result_backend, stub_worker):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # And I've run a group
    messages = [do_work.message() for _ in range(5)]
    g = group(messages)
    g.run()

    # If i forget the results
    g.results.wait(forget=True)

    # All messages have been forgotten
    assert list(g.results.get()) == [None] * 5


def test_pipelines_with_groups(stub_broker, stub_worker, result_middleware):
    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work(a):
        return a

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_sum(results):
        return sum(results)

    # And this actor is declared
    stub_broker.declare_actor(do_work)
    stub_broker.declare_actor(do_sum)

    # When I pipe some messages intended for that actor together and run the pipeline
    g = group([do_work.message(12), do_work.message(15)])
    pipe = g | do_sum.message()

    pipe.build()
    assert result_middleware.backend.get_group_message_ids(g.group_id) == list(g.message_ids)

    pipe.run()

    result = pipe.result.get(block=True)

    assert 12 + 15 == result

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # the group result has been forgotten
    assert list(g.results.get()) == [None, None]

    # the message_ids has been forgotten
    with pytest.raises(MessageIdsMissing):
        result_middleware.backend.get_group_message_ids(g.group_id)

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def add(a, b):
        return a + b

    stub_broker.declare_actor(add)

    pipe = do_work.message(13) | group([add.message(12), add.message(15)])
    pipe.run()

    result = pipe.result.get(block=True)

    assert [13 + 12, 13 + 15] == list(result)


def test_group_one_not_finished(stub_broker, stub_worker, result_middleware):
    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    stub_broker.declare_actor(do_work)

    messages = [do_work.send(), do_work.message(), do_work.send()]
    results = CollectionResults([m.result for m in messages])

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    with pytest.raises(ResultMissing):
        list(results.get())
    assert messages[0].result.get() == messages[2].result.get() == 42


def test_complex_pipelines(stub_broker, stub_worker, result_middleware):
    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def add(a):
        return 1 + a

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_sum(results):
        return sum(results)

    # And this actor is declared
    stub_broker.declare_actor(do_work)
    stub_broker.declare_actor(do_sum)
    stub_broker.declare_actor(add)

    pipe = do_work.message_with_options(pipe_ignore=True) | add.message() | add.message()  # return 3 [1, 2, 3] ?
    g = group([pipe, add.message(), add.message(), do_work.message_with_options(pipe_ignore=True)])  # return [3,2,2,1]
    final_pipe = do_work.message() | g | do_sum.message() | add.message()  # return 9
    final_pipe.run()

    result = final_pipe.result.get(block=True)

    assert 9 == result


def test_pipeline_with_groups_and_pipe_ignore(stub_broker, stub_worker, result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that do not stores results
    @remoulade.actor()
    def do_work():
        return 1

    @remoulade.actor(store_results=True)
    def do_other_work():
        return 2

    # And this actor is declared
    stub_broker.declare_actor(do_work)
    stub_broker.declare_actor(do_other_work)

    # When I pipe a group with another actor
    pipe = group([do_work.message(), do_work.message()]) | do_other_work.message_with_options(pipe_ignore=True)
    pipe.run()

    # I don't get any error as long the second actor has pipe_ignore=True
    result = pipe.result.get(block=True)

    assert 2 == result

    # But if it don't, the pipeline cannot finish
    pipe = group([do_work.message(), do_work.message()]) | do_other_work.message()
    pipe.run()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    with pytest.raises(ResultMissing):
        pipe.result.get()


def test_multiple_groups_pipelines(stub_broker, stub_worker, result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_sum(results):
        return sum(results)

    # And this actor is declared
    stub_broker.declare_actor(do_work)
    stub_broker.declare_actor(do_sum)

    pipe = pipeline(
        [group([do_work.message(), do_work.message()]), group([do_sum.message(), do_sum.message()]), do_sum.message()]
    ).run()

    result = pipe.result.get(block=True)

    assert 4 == result


def test_pipeline_does_not_continue_to_next_actor_when_message_is_marked_as_failed(stub_broker, stub_worker):
    # Given that I have an actor that fails messages
    class FailMessageMiddleware(remoulade.middleware.Middleware):
        def after_process_message(self, broker, message, *, result=None, exception=None):
            message.fail()

    stub_broker.add_middleware(FailMessageMiddleware())

    has_run = False

    @remoulade.actor
    def do_nothing():
        pass

    @remoulade.actor
    def should_never_run():
        nonlocal has_run
        has_run = True

    # And this actor is declared
    stub_broker.declare_actor(do_nothing)
    stub_broker.declare_actor(should_never_run)

    # When I pipe some messages intended for that actor together and run the pipeline
    pipe = do_nothing.message_with_options(pipe_ignore=True) | should_never_run.message()
    pipe.run()

    stub_broker.join(should_never_run.queue_name, timeout=10 * 1000)
    stub_worker.join()

    # Then the second message in the pipe should never have run
    assert not has_run


@mock.patch("remoulade.results.backend.compute_backoff", fast_backoff)
def test_retry_if_increment_group_completion_fail(stub_broker, stub_worker, caplog):
    with patch.object(StubBackend, "increment_group_completion") as mock_increment_group_completion:
        mock_increment_group_completion.side_effect = Exception("Cannot increment")
        middleware = Results(backend=StubBackend())
        stub_broker.add_middleware(middleware)

        attempts = []

        # And an actor that stores results
        @remoulade.actor(store_results=True)
        def do_work(*args):
            attempts.append(1)

        # And this actor is declared
        stub_broker.declare_actor(do_work)

        # When I send that actor a message
        (group([do_work.message(), do_work.message()]) | do_work.message()).run()

        # And wait for a result
        stub_broker.join(do_work.queue_name)
        stub_worker.join()

        # The actor has been tried 8 times (4 time each do_work and never the last one)
        assert len(attempts) == 8

        records = get_logs(caplog, "Unexpected failure in after_process_message")
        assert len(records) == 0


def test_composition_id_override(stub_broker, do_work):
    group_messages = group([pipeline([do_work.message(), do_work.message()])]).build(options={"composition_id": "id"})
    assert group_messages[0].options["composition_id"] == "id"
    assert group_messages[0].options["pipe_target"][0]["options"]["composition_id"] == "id"
    pipeline_messages = pipeline([group([do_work.message(), do_work.message()])]).build(composition_id="id")
    assert all(message.options["composition_id"] == "id" for message in pipeline_messages)


@mock.patch("remoulade.brokers.rabbitmq.RabbitmqBroker._get_channel")
@pytest.mark.confirm_delivery(True)
@pytest.mark.group_transaction(True)
def test_pipeline_with_delivery_confirmation_and_group_transaction(mocked_channel, rabbitmq_broker, rabbitmq_worker):
    @remoulade.actor(pipe_ignore=True)
    def do_work():
        return 42

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # When I pipe some messages intended for that actor together and run the pipeline
    pipe = do_work.message() | do_work.message()
    pipe.build()

    pipe.run()
    assert mocked_channel.call_count == 1
    assert mocked_channel.call_args == call(False)

    pipe.run(transaction=False)
    assert mocked_channel.call_count == 2
    assert mocked_channel.call_args == call(True)


@mock.patch("remoulade.brokers.rabbitmq.RabbitmqBroker._get_channel")
@pytest.mark.confirm_delivery(True)
@pytest.mark.group_transaction(True)
def test_group_with_delivery_confirmation_and_group_transaction(mocked_channel, rabbitmq_broker, rabbitmq_worker):
    @remoulade.actor()
    def do_work():
        return 42

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # And I've run a group
    messages = [do_work.message()]
    g = group(messages)

    g.run()
    assert mocked_channel.call_count == 1
    assert mocked_channel.call_args == call(False)

    g.run(transaction=False)
    assert mocked_channel.call_count == 2
    assert mocked_channel.call_args == call(True)


def test_pipeline_pipe_on_error(stub_broker, stub_worker, stub_result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=stub_result_backend))

    @remoulade.actor(pipe_on_error=True, store_results=True)
    def do_raise():
        raise MemoryError("BOOM")

    @remoulade.actor(store_results=True)
    def rcv_error(data):
        return data

    # And this actor is declared
    stub_broker.declare_actor(do_raise)
    stub_broker.declare_actor(rcv_error)

    pipe = pipeline([do_raise.message(), rcv_error.message()])
    pipe.run()

    stub_broker.join(do_raise.queue_name)
    stub_broker.join(rcv_error.queue_name)
    stub_worker.join()

    assert pipe.result.get(block=True, raise_on_error=False) == "MemoryError('BOOM')"


def test_complex_pipeline_pipe_on_error(stub_broker, stub_worker, stub_result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=stub_result_backend))

    @remoulade.actor(pipe_on_error=True, store_results=True)
    def do_one():
        return 1

    @remoulade.actor(pipe_on_error=True, store_results=True)
    def do_raise():
        raise MemoryError("BOOM")

    @remoulade.actor(store_results=True)
    def rcv_error(data):
        return data

    # And this actor is declared
    stub_broker.declare_actor(do_one)
    stub_broker.declare_actor(do_raise)
    stub_broker.declare_actor(rcv_error)

    pipe = pipeline([group([do_one.message(), do_raise.message(), do_one.message()]), rcv_error.message()])
    pipe.run()

    stub_worker.join()

    assert pipe.result.get(block=True, raise_on_error=False) == [1, "MemoryError('BOOM')", 1]
