import pytest

import remoulade
from remoulade import RemouladeError, Worker, group
from remoulade.cancel import Cancel
from remoulade.results import Results
from remoulade.results.backends import LocalBackend


def test_local_broker_cannot_have_non_local_backend(local_broker, result_backend):
    # Which is not a LocalBackend
    assert not isinstance(result_backend, LocalBackend)

    # Cannot be used with LocalBroker
    with pytest.raises(RuntimeError):
        local_broker.add_middleware(Results(backend=result_backend))


def test_local_broker_get_result_in_message(local_broker, local_result_backend):
    local_broker.add_middleware(Results(backend=local_result_backend))

    # Given that I have an actor that stores its results
    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    # And this actor is declared
    local_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # I should get the right result
    assert message.result.get() == 1


def test_local_broker_with_pipes(local_broker, local_result_backend):
    local_broker.add_middleware(Results(backend=local_result_backend))

    # Given that I have an actor that stores its results
    @remoulade.actor(store_results=True)
    def add(a, b):
        return a + b

    # And this actor is declared
    local_broker.declare_actor(add)

    # When I run a pipe
    pipe = add.message(1, 2) | add.message(3)
    pipe.run()

    # I should get the right result
    assert pipe.result.get() == 6


def test_local_broker_with_groups(local_broker, local_result_backend):
    local_broker.add_middleware(Results(backend=local_result_backend))

    # Given that I have an actor that stores its results
    @remoulade.actor(store_results=True)
    def add(a, b):
        return a + b

    # And this actor is declared
    local_broker.declare_actor(add)

    # When I run a group
    g = group([add.message(1, 2), add.message(3, 4), add.message(4, 5)])
    g.run()

    assert list(g.results.get()) == [3, 7, 9]


def test_local_broker_forget(local_broker, local_result_backend):
    local_broker.add_middleware(Results(backend=local_result_backend))

    # Given that I have an actor that stores its results
    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    # And this actor is declared
    local_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # I if forget the result
    assert message.result.get(forget=True) == 1

    # the result is forgotten
    assert message.result.get() is None


def test_local_broker_cancel(local_broker, stub_cancel_backend):
    local_broker.add_middleware(Cancel(backend=stub_cancel_backend))

    @remoulade.actor()
    def do_work():
        return 1

    # And this actor is declared
    local_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # I can call cancel
    message.cancel()


def test_local_broker_worker_forbidden(local_broker):
    with pytest.raises(RemouladeError):
        Worker(broker=local_broker)


def test_local_raise_error(local_broker):
    # Given that I have an actor that raise an exception
    @remoulade.actor()
    def raise_error():
        raise ValueError()

    local_broker.declare_actor(raise_error)

    # When I send that actor a message, a get an exception
    with pytest.raises(ValueError):
        raise_error.send()
