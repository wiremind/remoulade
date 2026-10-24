import logging
import time
from unittest.mock import Mock

import pytest

import remoulade
from remoulade import Message, Worker
from remoulade.errors import QueueNotFound, UnsupportedMessageEncoding
from remoulade.middleware import CatchError


class _NotJsonEncodable:
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return f"not json: {self.reason}"

    def __eq__(self, other):
        return isinstance(other, _NotJsonEncodable) and other.reason == self.reason

    def __hash__(self):
        return hash(self.reason)


def test_on_failure(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal on_failure_count
        on_failure_count += 1

    @remoulade.actor
    def fail_actor():
        raise Exception()

    remoulade.declare_actors([on_failure, fail_actor])

    fail_actor.send_with_options(on_failure=on_failure)

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    # The on_failure should have run
    assert on_failure_count == 1


def test_on_failure_message(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal on_failure_count
        on_failure_count += 1

    @remoulade.actor
    def fail_actor():
        raise Exception()

    remoulade.declare_actors([on_failure, fail_actor])

    fail_actor.send_with_options(on_failure=on_failure.message())

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    # The on_failure should have run
    assert on_failure_count == 1


def test_on_failure_message_in_actor_options(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal on_failure_count
        on_failure_count += 1

    stub_broker.declare_actor(on_failure)

    @remoulade.actor(on_failure=on_failure.message())
    def fail_actor():
        raise Exception()

    stub_broker.declare_actor(fail_actor)

    fail_actor.send()
    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    # The message should not have run
    assert on_failure_count == 0


def test_on_failure_runs_only_once(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal on_failure_count
        on_failure_count += 1

    @remoulade.actor
    def fail_actor():
        raise Exception()

    remoulade.declare_actors([on_failure, fail_actor])

    fail_actor.send_with_options(on_failure=on_failure, max_retries=3, min_backoff=1, backoff_strategy="constant")

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    # The on_failure should have run only once
    assert on_failure_count == 1


def test_on_failure_passes_exception_args(stub_broker, stub_worker):
    failure_exception_args = None

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal failure_exception_args
        failure_exception_args = exception_args

    @remoulade.actor
    def fail_actor():
        raise Exception("message", "missing.csv")

    remoulade.declare_actors([on_failure, fail_actor])

    fail_actor.send_with_options(on_failure=on_failure)

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    assert failure_exception_args == ["message", "missing.csv"]


def test_on_failure_keeps_existing_exception_args(stub_broker, stub_worker):
    failure_message_kwargs = None

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal failure_message_kwargs
        failure_message_kwargs = kwargs

    @remoulade.actor
    def fail_actor(exception_args=None):
        raise Exception("message", "missing.csv")

    remoulade.declare_actors([on_failure, fail_actor])

    stub_broker.enqueue(
        fail_actor.message_with_options(
            kwargs={"exception_args": ["existing-value"]},
            on_failure=on_failure,
        )
    )

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    assert failure_message_kwargs == {"exception_args": ["existing-value"]}


def test_clean_runs_on_timeout(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal on_failure_count
        on_failure_count += 1

    @remoulade.actor
    def do_work():
        time.sleep(1)

    remoulade.declare_actors([on_failure, do_work])

    do_work.send_with_options(on_failure=on_failure, time_limit=1)

    stub_broker.join(do_work.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    # The on_failure should have run as messages should not be passed in on_failure as actor options
    assert on_failure_count == 1


@pytest.mark.parametrize("as_message", [False, True])
def test_on_failure_coerces_exception_args_the_encoder_rejects(stub_broker, stub_worker, as_message):
    failure_exception_args = None

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal failure_exception_args
        failure_exception_args = exception_args

    @remoulade.actor
    def fail_actor():
        raise Exception("message", _NotJsonEncodable("boom"), 3, {"a": [1]})

    remoulade.declare_actors([on_failure, fail_actor])

    fail_actor.send_with_options(on_failure=on_failure.message() if as_message else on_failure)

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    assert failure_exception_args == ["message", "not json: boom", 3, {"a": [1]}]


def test_on_failure_keeps_exception_args_the_encoder_accepts(pickle_encoder, stub_broker, stub_worker):
    failure_exception_args = None

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        nonlocal failure_exception_args
        failure_exception_args = exception_args

    @remoulade.actor
    def fail_actor():
        raise Exception("message", _NotJsonEncodable("boom"))

    remoulade.declare_actors([on_failure, fail_actor])

    fail_actor.send_with_options(on_failure=on_failure)

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(on_failure.queue_name)
    stub_worker.join()

    assert failure_exception_args == ("message", _NotJsonEncodable("boom"))


def test_on_failure_coerces_exception_args_on_postgres(postgres_broker):
    received = []

    @remoulade.actor
    def on_failure(actor_name, exception_name, exception_args, args, kwargs):
        received.append((actor_name, exception_name, exception_args, args, kwargs))

    @remoulade.actor(max_retries=0)
    def fail_actor(value):
        raise ConnectionError(_NotJsonEncodable("max retries"), "https://rs.example/x")

    remoulade.declare_actors([on_failure, fail_actor])
    worker = Worker(postgres_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    try:
        fail_actor.send_with_options(args=(1,), on_failure=on_failure)
        postgres_broker.join(fail_actor.queue_name, timeout=10_000)
        worker.join()
    finally:
        worker.stop()

    assert received == [
        ("fail_actor", "ConnectionError", ["not json: max retries", "https://rs.example/x"], [1], {}),
    ]


def _failed_message(on_failure):
    return Mock(failed=True, actor_name="fail_actor", args=[1], kwargs={}, options={"on_failure": on_failure})


def _broker(enqueue_side_effect=None):
    broker = Mock()
    broker.get_actor.return_value.message.side_effect = lambda *args: Message(
        queue_name="default", actor_name="on_failure", args=args, kwargs={}, options={}
    )
    broker.enqueue.side_effect = enqueue_side_effect
    return broker


def _enqueued_exception_args(broker):
    return [call.args[0].args[2] for call in broker.enqueue.call_args_list]


def test_catch_error_retries_an_actor_message_once_with_coerced_exception_args():
    broker = _broker([TypeError("Object of type _NotJsonEncodable is not JSON serializable"), None])

    CatchError().after_process_message(
        broker, _failed_message("on_failure"), exception=Exception(_NotJsonEncodable("boom"), "kept")
    )

    assert _enqueued_exception_args(broker) == [
        (_NotJsonEncodable("boom"), "kept"),
        ("not json: boom", "kept"),
    ]
    first, retry = (call.args[0] for call in broker.enqueue.call_args_list)
    assert retry.message_id == first.message_id
    assert retry.args == ("fail_actor", "Exception", ("not json: boom", "kept"), [1], {})


def test_catch_error_logs_which_exception_args_it_coerces(caplog):
    broker = _broker([TypeError("Object of type _NotJsonEncodable is not JSON serializable"), None])

    with caplog.at_level(logging.WARNING, logger="remoulade.middleware.catch_error"):
        CatchError().after_process_message(
            broker, _failed_message("on_failure"), exception=Exception("kept", _NotJsonEncodable("boom"))
        )

    assert (
        "Could not encode the on_failure message to actor on_failure after fail_actor failed "
        "(TypeError: Object of type _NotJsonEncodable is not JSON serializable); "
        "retrying with exception args {1: '_NotJsonEncodable'} replaced by their str."
    ) in caplog.text


def test_catch_error_enqueues_once_and_logs_nothing_when_the_first_enqueue_works(caplog):
    broker = _broker()

    with caplog.at_level(logging.WARNING, logger="remoulade.middleware.catch_error"):
        CatchError().after_process_message(
            broker, _failed_message("on_failure"), exception=Exception(_NotJsonEncodable("boom"))
        )

    assert _enqueued_exception_args(broker) == [(_NotJsonEncodable("boom"),)]
    assert caplog.text == ""


def test_catch_error_retries_a_message_enqueue_once_with_coerced_exception_args():
    broker = _broker([UnsupportedMessageEncoding("PGMQ messages must contain JSON objects."), None])
    on_failure = Message(queue_name="default", actor_name="on_failure", args=(), kwargs={}, options={}).asdict()

    CatchError().after_process_message(
        broker, _failed_message(on_failure), exception=Exception(_NotJsonEncodable("boom"))
    )

    assert _enqueued_exception_args(broker) == [
        (_NotJsonEncodable("boom"),),
        ("not json: boom",),
    ]
    first, retry = (call.args[0] for call in broker.enqueue.call_args_list)
    assert retry.message_id == first.message_id == on_failure["message_id"]


def test_catch_error_re_raises_when_every_exception_arg_is_already_json():
    broker = _broker(TypeError("something else is not JSON serializable"))

    with pytest.raises(TypeError, match="something else"):
        CatchError().after_process_message(broker, _failed_message("on_failure"), exception=Exception("plain", 1))

    broker.enqueue.assert_called_once()


def test_catch_error_re_raises_when_the_coerced_retry_fails_too():
    broker = _broker(ValueError("still not encodable"))

    with pytest.raises(ValueError, match="still not encodable"):
        CatchError().after_process_message(
            broker, _failed_message("on_failure"), exception=Exception(_NotJsonEncodable("boom"))
        )

    assert broker.enqueue.call_count == 2


def test_catch_error_does_not_retry_an_error_that_is_not_about_encoding():
    broker = _broker(QueueNotFound("default"))

    with pytest.raises(QueueNotFound):
        CatchError().after_process_message(
            broker, _failed_message("on_failure"), exception=Exception(_NotJsonEncodable("boom"))
        )

    broker.enqueue.assert_called_once()
