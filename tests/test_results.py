import time

import pytest

import remoulade
from remoulade import Result
from remoulade.middleware import Retries
from remoulade.results import ErrorStored, ResultMissing, Results, ResultTimeout


@pytest.mark.parametrize("forget", [True, False])
@pytest.mark.parametrize("block", [True, False])
def test_actors_can_store_results(stub_broker, stub_worker, result_backend, forget, block):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    if not block:
        stub_broker.join(do_work.queue_name)
        stub_worker.join()

    result = message.result.get(block=block, forget=forget)
    assert isinstance(message.result, Result)

    # Then the result should be what the actor returned
    assert result == 42


@pytest.mark.parametrize("forget", [True, False])
def test_retrieving_a_result_can_raise_result_missing(stub_broker, stub_worker, result_backend, forget):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that sleeps for a long time before it stores a result
    @remoulade.actor(store_results=True)
    def do_work():
        time.sleep(0.2)
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And get the result without blocking
    # Then a ResultMissing error should be raised
    with pytest.raises(ResultMissing):
        result_backend.get_result(message, forget=forget)


@pytest.mark.parametrize("forget", [True, False])
def test_retrieving_a_result_can_time_out(stub_broker, stub_worker, result_backend, forget):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that sleeps for a long time before it stores a result
    @remoulade.actor(store_results=True)
    def do_work():
        time.sleep(0.2)
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    # Then a ResultTimeout error should be raised
    with pytest.raises(ResultTimeout):
        result_backend.get_result(message, block=True, timeout=1000, forget=forget)


@pytest.mark.parametrize("forget", [True, False])
def test_messages_can_get_results_from_backend(stub_broker, stub_worker, result_backend, forget):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that stores a result
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    # Then I should get that result back
    assert message.result.get(block=True, forget=forget) == 42


def test_messages_results_can_get_results_from_backend(stub_broker, stub_worker, result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that stores a result
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And create a message result
    result = Result(message_id=message.message_id)

    # And wait for a result
    # Then I should get that result back
    assert result.get(block=True) == 42


def test_messages_can_get_results_from_inferred_backend(stub_broker, stub_worker, redis_result_backend):
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=redis_result_backend))

    # And an actor that stores a result
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    # Then I should get that result back
    assert message.result.get(block=True) == 42


def test_result_default_before_retries(stub_broker, result_backend, stub_worker):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    retries_index, results_index = None, None

    for i, middleware in enumerate(stub_broker.middleware):
        if isinstance(middleware, Retries):
            retries_index = i
        if isinstance(middleware, Results):
            results_index = i

    assert results_index is not None
    assert retries_index is not None
    # The Results middleware should be before the Retries middleware
    assert retries_index > results_index


@pytest.mark.parametrize("block", [True, False])
def test_raise_on_error(stub_broker, result_backend, stub_worker, block):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that store a result and fail
    @remoulade.actor(store_results=True)
    def do_work():
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    if not block:
        stub_broker.join(do_work.queue_name)
        stub_worker.join()

    # It should raise an error
    with pytest.raises(ErrorStored) as e:
        message.result.get(block=block)
    assert str(e.value) == "ValueError()"


@pytest.mark.parametrize("block", [True, False])
def test_store_errors(stub_broker, result_backend, stub_worker, block):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that store a result and fail
    @remoulade.actor(store_results=True)
    def do_work():
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    if not block:
        stub_broker.join(do_work.queue_name)
        stub_worker.join()

    # Then I should get an ErrorStored
    error_stored = message.result.get(block=block, raise_on_error=False)
    assert isinstance(error_stored, ErrorStored)
    assert str(error_stored) == "ValueError()"


def test_store_errors_after_no_more_retry(stub_broker, result_backend, stub_worker):
    # Given that I have a database
    failures = []

    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that stores results
    @remoulade.actor(max_retries=3, store_results=True, min_backoff=10, max_backoff=100)
    def do_work():
        failures.append(1)
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message,
    message = do_work.send()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # I get an error
    with pytest.raises(Exception) as e:
        message.result.get(block=True)
    assert str(e.value) == "ValueError()"

    # all the retries have been made
    assert sum(failures) == 4


@pytest.mark.parametrize("block", [True, False])
def test_messages_can_get_results_and_forget(stub_broker, stub_worker, result_backend, block):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that stores a result
    @remoulade.actor(store_results=True)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    if not block:
        stub_broker.join(do_work.queue_name)
        stub_worker.join()

    # Then I should get that result back
    assert message.result.get(block=block, forget=True) == 42

    # If I ask again for the same result it should have been forgotten
    assert message.result.get() is None


@pytest.mark.parametrize("block", [True, False])
@pytest.mark.parametrize("forget", [True, False])
def test_redis_backend_keep_ttl_all_time(stub_broker, stub_worker, redis_result_backend, block, forget):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=redis_result_backend))

    # And an actor that stores a result with a result_ttl
    result_ttl = 100 * 1000

    @remoulade.actor(store_results=True, result_ttl=result_ttl)
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    if not block:
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        # The result should have a TTL in redis
        assert redis_result_backend.client.ttl(redis_result_backend.build_message_key(message.message_id)) > 0

    # Then I should get that result back
    assert message.result.get(block=block, forget=forget) == 42
    # The forgotten result should still have a TTL in redis
    assert redis_result_backend.client.ttl(redis_result_backend.build_message_key(message.message_id)) > 0


@pytest.mark.parametrize("error", [True, False])
def test_messages_can_get_completed(stub_broker, stub_worker, result_backend, error):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        if error:
            raise ValueError()
        else:
            return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # And wait for a result
    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    result = message.result
    # we can get the completion
    assert result.completed

    result.get(forget=True, raise_on_error=False)

    # even after a forget
    assert result.completed


def test_result_get_forget_not_store_if_no_result(stub_broker, stub_worker, result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # And an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        time.sleep(1)
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    result = message.result
    # If I get the result and forget it
    with pytest.raises(ResultMissing):
        result.get(forget=True)

    # It should not store a forgotten result if there is no key
    assert result.get(block=True) == 42
