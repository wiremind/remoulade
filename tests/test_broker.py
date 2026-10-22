import platform

import pytest

import remoulade
import remoulade.broker
from remoulade import group
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.brokers.stub import StubBroker
from remoulade.helpers.queues import dq_name
from remoulade.middleware import AgeLimit, Middleware, ShutdownNotifications, TimeLimit

CURRENT_OS = platform.system()
skip_on_windows = pytest.mark.skipif(CURRENT_OS == "Windows", reason="test skipped on Windows")


class EmptyMiddleware(Middleware):
    pass


def test_broker_uses_rabbitmq_if_not_set():
    # Given that no global broker is set
    remoulade.broker.global_broker = None

    # If I try to get the global broker
    # I expect a ValueError to be raised
    with pytest.raises(ValueError) as e:
        remoulade.get_broker()

    assert str(e.value) == "Broker not found, are you sure you called set_broker(broker) ?"


def test_change_broker(stub_broker):
    # Given that some actors
    @remoulade.actor
    def add(x, y):
        return x + y

    @remoulade.actor
    def modulo(x, y):
        return x % y

    # And these actors are declared
    stub_broker.declare_actor(add)
    stub_broker.declare_actor(modulo)

    # Given a new broker
    new_broker = StubBroker()

    remoulade.change_broker(new_broker)

    # I expect them to have the same actors
    assert stub_broker.actors == new_broker.actors


def test_declare_actors(stub_broker):
    # Given that some actors
    @remoulade.actor
    def add(x, y):
        return x + y

    @remoulade.actor
    def modulo(x, y):
        return x % y

    actors = [add, modulo]
    remoulade.declare_actors(actors)

    assert set(stub_broker.actors.values()) == set(actors)


def test_declare_actors_no_broker():
    remoulade.broker.global_broker = None
    with pytest.raises(ValueError):
        remoulade.declare_actors([])


def test_middleware_is_inserted_correctly():
    broker = StubBroker(middleware=[])

    assert len(broker.middleware) == 0

    broker.add_middleware(AgeLimit())

    assert len(broker.middleware) == 1

    broker.add_middleware(ShutdownNotifications())

    assert len(broker.middleware) == 2

    broker.add_middleware(TimeLimit())

    assert len(broker.middleware) == 3
    assert isinstance(broker.middleware[1], TimeLimit)


def test_custom_middleware_is_appended(stub_broker):
    class TestMiddleware(Middleware):
        pass

    middleware_count = len(stub_broker.middleware)

    stub_broker.add_middleware(TestMiddleware())

    assert len(stub_broker.middleware) == middleware_count + 1
    assert isinstance(stub_broker.middleware[middleware_count], TestMiddleware)


class _EnqueueRecorder(Middleware):
    def __init__(self):
        self.events = []

    def before_enqueue(self, broker, message, delay):
        self.events.append(("before", message.args[0]))

    def after_enqueue(self, broker, message, delay, exception=None):
        self.events.append(("after", message.args[0], exception is not None))


def test_enqueue_many_groups_before_calls_then_after_calls(stub_broker):
    @remoulade.actor
    def do_work(index):
        return index

    stub_broker.declare_actor(do_work)
    recorder = _EnqueueRecorder()
    stub_broker.add_middleware(recorder)

    messages = [do_work.message(0), do_work.message(1)]
    stub_broker.enqueue_many(messages)

    # Every message goes through the enqueue middleware, but all before_enqueue calls happen before the
    # (batched) write and all after_enqueue calls after it, instead of being interleaved per message.
    assert recorder.events == [
        ("before", 0),
        ("before", 1),
        ("after", 0, False),
        ("after", 1, False),
    ]


def test_enqueue_many_emits_after_with_exception_only_for_started_messages(stub_broker):
    @remoulade.actor
    def do_work(index):
        return index

    stub_broker.declare_actor(do_work)
    recorder = _EnqueueRecorder()
    stub_broker.add_middleware(recorder)

    boom = RuntimeError("backend down")

    def failing_enqueue_many(messages, *, delay=None):
        raise boom

    stub_broker._enqueue_many = failing_enqueue_many

    messages = [do_work.message(0), do_work.message(1)]
    with pytest.raises(RuntimeError):
        stub_broker.enqueue_many(messages)

    # before ran for both, and after (with exception) is emitted for both so nothing leaks a dangling span/state
    assert recorder.events == [
        ("before", 0),
        ("before", 1),
        ("after", 0, True),
        ("after", 1, True),
    ]


class _DelayRecorder(Middleware):
    def __init__(self):
        self.delays = []

    def before_enqueue(self, broker, message, delay):
        self.delays.append((message.args[0], delay))


@pytest.fixture
def do_work(stub_broker):
    @remoulade.actor
    def do_work(index):
        return index

    stub_broker.declare_actor(do_work)
    return do_work


@pytest.fixture
def enqueue_delays(stub_broker):
    """Record the delay each message is announced with to the ``before_enqueue`` middleware."""
    recorder = _DelayRecorder()
    stub_broker.add_middleware(recorder)
    return recorder.delays


@pytest.fixture
def enqueue_many_batches(stub_broker, monkeypatch):
    """Record the messages and the delay of every ``_enqueue_many`` call of the stub broker."""
    batches = []
    enqueue_many = stub_broker._enqueue_many

    def recording_enqueue_many(messages, *, delay=None):
        batches.append(([message.args[0] for message in messages], delay))
        return enqueue_many(messages, delay=delay)

    monkeypatch.setattr(stub_broker, "_enqueue_many", recording_enqueue_many)
    return batches


def test_enqueue_adds_the_message_delay_option_to_the_enqueue_delay(stub_broker, do_work, enqueue_delays):
    message = stub_broker.enqueue(do_work.message_with_options(args=(0,), delay=1000), delay=500)

    assert enqueue_delays == [(0, 1500)]
    assert message.queue_name == dq_name(do_work.queue_name)
    # the option is consumed, so enqueuing the message again (delay queue, retries) does not delay it twice
    assert "delay" not in message.options


def test_enqueue_keeps_the_delay_option_of_the_given_message(stub_broker, do_work):
    message = do_work.message_with_options(args=(0,), delay=1000)

    stub_broker.enqueue(message)

    # the option is consumed on a copy, the caller's message is left untouched
    assert message.options["delay"] == 1000


def test_enqueue_many_writes_one_batch_per_total_delay(stub_broker, do_work, enqueue_delays, enqueue_many_batches):
    messages = [
        do_work.message_with_options(args=(0,), delay=1000),
        do_work.message(1),
        do_work.message_with_options(args=(2,), delay=1000),
    ]
    enqueued_messages = stub_broker.enqueue_many(messages, delay=500)

    assert enqueue_many_batches == [([0, 2], 1500), ([1], 500)]
    assert enqueue_delays == [(0, 1500), (1, 500), (2, 1500)]
    assert [message.args[0] for message in enqueued_messages] == [0, 1, 2]
    assert all("delay" not in message.options for message in enqueued_messages)


def test_enqueue_many_without_message_delay_writes_a_single_batch(stub_broker, do_work, enqueue_many_batches):
    stub_broker.enqueue_many([do_work.message(0), do_work.message(1)])

    assert enqueue_many_batches == [([0, 1], None)]


@pytest.mark.parametrize(
    ("option_delay", "enqueue_delay"),
    [(0, None), (None, 0), (0, 0)],
    ids=["zero_option", "zero_enqueue_delay", "zero_both"],
)
def test_enqueue_sends_a_zero_delay_message_without_delay(
    stub_broker, do_work, enqueue_delays, option_delay, enqueue_delay
):
    options = {} if option_delay is None else {"delay": option_delay}

    message = stub_broker.enqueue(do_work.message_with_options(args=(0,), **options), delay=enqueue_delay)

    # a zero delay goes straight to the queue instead of through the delay queue
    assert enqueue_delays == [(0, None)]
    assert message.queue_name == do_work.queue_name
    assert "eta" not in message.options


def test_enqueue_many_writes_zero_delay_and_no_delay_messages_in_one_batch(stub_broker, do_work, enqueue_many_batches):
    stub_broker.enqueue_many([do_work.message_with_options(args=(0,), delay=0), do_work.message(1)])

    assert enqueue_many_batches == [([0, 1], None)]


def test_group_runs_each_message_once_after_its_delay_option(stub_broker, stub_worker):
    results = []

    @remoulade.actor
    def append(value):
        results.append(value)

    stub_broker.declare_actor(append)

    group(
        [
            append.message_with_options(args=(1,), delay=300),
            append.message_with_options(args=(2,), delay=100),
        ]
    ).run()

    stub_broker.join(append.queue_name)
    stub_worker.join()

    assert results == [2, 1]


def test_can_instantiate_brokers_without_middleware():
    # Given that I have an empty list of middleware
    # When I pass that to the RMQ Broker
    broker = RabbitmqBroker(middleware=[])

    # Then I should get back a broker with not middleware
    assert not broker.middleware


def test_duplicate_middleware(stub_broker):
    # Given that I have a broker with the default middleware
    middleware_count = len(stub_broker.middleware)

    # When I add a middleware that is already added with options
    stub_broker.add_middleware(AgeLimit(max_age=2))

    # Then I should have the same amount of middleware, and the Age Limit middleware should be the new one
    assert len(stub_broker.middleware) == middleware_count
    assert stub_broker.get_middleware(AgeLimit).max_age == 2


def test_remove_middleware(stub_broker):
    # Given that I have a broker with multiple middlewares
    stub_broker.add_middleware(AgeLimit())
    stub_broker.add_middleware(ShutdownNotifications())
    stub_broker.add_middleware(TimeLimit())
    middleware_count = len(stub_broker.middleware)

    # When I add a middleware that is already added with options
    stub_broker.remove_middleware(ShutdownNotifications)

    # Then I should have the right amount of middleware, and the ShutdownNotifications middleware should not be there
    assert len(stub_broker.middleware) == middleware_count - 1
    assert stub_broker.get_middleware(ShutdownNotifications) is None
