import time

import pytest

import remoulade
from remoulade import group, pipeline
from remoulade.cancel import Cancel
from remoulade.errors import NoCancelBackend


@pytest.mark.parametrize("cancel", [True, False])
def test_actors_can_be_canceled(stub_broker, stub_worker, cancel_backend, cancel):
    # Given a cancel backend
    # And a broker with the cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    has_been_called = []

    # And an actor that is cancelable
    @remoulade.actor(cancelable=True)
    def do_work():
        has_been_called.append(1)

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.send()

    # If I cancel the message
    if cancel:
        message.cancel()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # It should not be called
    assert bool(has_been_called) != cancel


def test_cancellations_not_stored_forever(cancel_backend):
    # Given a cancel backend which store the cancellations 1 second
    cancel_backend.cancellation_ttl = 1

    # If we cancel some messages
    cancel_backend.cancel('a')
    # Then wait some time
    time.sleep(1)
    # And call again cancel
    cancel_backend.cancel('c')

    # the first cancellation should have been forgotten
    assert not cancel_backend.is_canceled('a')


def test_group_are_canceled_on_actor_failure(stub_broker, stub_worker, cancel_backend):
    # Given a cancel backend
    # And a broker with the cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    has_been_called = []

    # And an actor that is cancelable
    @remoulade.actor(cancelable=True)
    def do_work():
        has_been_called.append(1)
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    g = group((do_work.message() for _ in range(4)), cancel_on_error=True)

    # When I group a few jobs together and run it
    g.run()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # All actors should have been canceled
    assert all(cancel_backend.is_canceled(child.message_id) for child in g.children)


def test_cannot_cancel_on_error_if_no_cancel(stub_broker):
    # Given an actor
    @remoulade.actor()
    def do_work():
        return 42

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    with pytest.raises(NoCancelBackend):
        group((do_work.message() for _ in range(4)), cancel_on_error=True)


@pytest.mark.parametrize("with_pipeline", [True, False])
def test_cancel_pipeline_or_groups(stub_broker, stub_worker, cancel_backend, with_pipeline):
    # Given a cancel backend
    # And a broker with the cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    has_been_called = []

    # And an actor that is cancelable
    @remoulade.actor(cancelable=True)
    def do_work():
        has_been_called.append(1)
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    if with_pipeline:
        g = pipeline((do_work.message() for _ in range(4)))
    else:
        g = group((do_work.message() for _ in range(4)))

    g.cancel()
    g.run()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # All actors should have been canceled
    assert all(cancel_backend.is_canceled(child.message_id) for child in g.children)
    assert len(has_been_called) == 0
