from unittest.mock import patch

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

    # And an actor
    @remoulade.actor()
    def do_work():
        has_been_called.append(1)

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # When I send that actor a message
    message = do_work.message()

    # If I cancel the message
    if cancel:
        message.cancel()

    stub_broker.enqueue(message)

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # It should not be called
    assert bool(has_been_called) != cancel


def test_cancellations_not_stored_forever(cancel_backend, frozen_datetime):
    # Given a cancel backend which store the cancellations 1 second
    cancel_backend.cancellation_ttl = 1

    # If we cancel some messages
    cancel_backend.cancel("a")
    # Then wait some time
    frozen_datetime.tick(delta=2)
    # And call again cancel
    cancel_backend.cancel("c")

    # the first cancellation should have been forgotten
    assert not cancel_backend.is_canceled("a", None)


@pytest.mark.parametrize("cancel", [True, False])
def test_compositions_are_canceled_on_actor_failure(stub_broker, stub_worker, cancel_backend, cancel):
    # Given a cancel backend
    # And a broker with the cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    # And an actor who doesn't fail
    @remoulade.actor
    def do_work(arg=None):
        return 1

    # And an actor who fails
    @remoulade.actor
    def do_fail(arg):
        raise Exception

    # And those actors are declared
    remoulade.declare_actors([do_work, do_fail])

    g = group([do_work.message(), do_fail.message() | do_work.message()], cancel_on_error=cancel)
    p = pipeline([g, do_work.message()], cancel_on_error=cancel)

    # When I group a few jobs together and run it
    with patch("remoulade.composition.generate_unique_id") as mocked_obj:
        mocked_obj.return_value = "mocked_composition_id"
        p.run()

    stub_broker.join(do_fail.queue_name)
    stub_worker.join()

    # All actors should have been canceled
    assert cancel_backend.is_canceled("", "mocked_composition_id") == cancel


def test_compositions_are_canceled_on_message_cancel(stub_broker, cancel_backend, state_middleware, api_client):
    # Given a cancel backend
    # And a broker with the cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    # And an actor
    @remoulade.actor
    def do_work(arg=None):
        return 1

    # And those actors are declared
    stub_broker.declare_actor(do_work)

    message_to_cancel = do_work.message()

    # And a group that I enqueue
    g = group([message_to_cancel | do_work.message(), do_work.message()], cancel_on_error=True)
    g.run()

    # When I cancel a message of this group
    api_client.post("messages/cancel/" + message_to_cancel.message_id)

    # The whole composition should be canceled
    assert cancel_backend.is_canceled("", g.group_id)


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

    # And an actor
    @remoulade.actor()
    def do_work():
        has_been_called.append(1)
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    if with_pipeline:
        g = pipeline(do_work.message() for _ in range(4))
    else:
        g = group(do_work.message() for _ in range(4))

    g.cancel()
    g.run()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # All actors should have been canceled
    assert all(cancel_backend.is_canceled(child.message_id, None) for child in g.children)
    assert len(has_been_called) == 0


def test_composition_can_be_canceled(stub_broker, stub_worker, cancel_backend):
    # Given a cancel backend
    # And a broker with the cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    calls_count = 0

    # And an actor
    @remoulade.actor()
    def do_work():
        nonlocal calls_count
        calls_count += 1
        raise ValueError()

    # And this actor is declared
    stub_broker.declare_actor(do_work)

    # And a composition
    g = group([do_work.message() | do_work.message() for _ in range(2)])

    # If the composition is canceled
    cancel_backend.cancel([g.group_id])

    g.run()

    stub_broker.join(do_work.queue_name)
    stub_worker.join()

    # It messages should not have runMessageSchema
    assert calls_count == 0


def test_raise_error_if_unknown_id(stub_broker, cancel_backend, api_client):
    # Given a cancel middleware
    stub_broker.add_middleware(Cancel(backend=cancel_backend))

    # If I try to cancel a id that is not a id of a message or composition
    res = api_client.post("messages/cancel/invalid_id")

    assert res.status_code == 400
