import time
from threading import Event
from unittest import mock
from unittest.mock import Mock, call

import pytest

import remoulade
from remoulade import ActorNotFound, Message, QueueJoinTimeout, Worker
from remoulade.common import current_millis
from remoulade.errors import MessageNotDelivered
from remoulade.middleware import CurrentMessage
from remoulade.results import Results


def test_rabbitmq_actors_can_be_sent_messages(rabbitmq_broker, rabbitmq_worker):
    # Given that I have a database
    database = {}

    # And an actor that can write data to that database
    @remoulade.actor
    def put(key, value):
        database[key] = value

    # And this actor is declared
    rabbitmq_broker.declare_actor(put)

    # If I send that actor many async messages
    for i in range(100):
        assert put.send("key-%d" % i, i)

    # And I give the workers time to process the messages
    rabbitmq_broker.join(put.queue_name)
    rabbitmq_worker.join()

    # I expect the database to be populated
    assert len(database) == 100


def test_rabbitmq_queues_created_lazily(rabbitmq_broker):
    # Given that rabbitMQ has no open connection
    rabbitmq_broker.close()

    # Given that I have an actor
    @remoulade.actor
    def add(a, b):
        return a + b

    # And this actor is declared
    rabbitmq_broker.declare_actor(add)

    # queue_name should be in prepared_queues
    assert add.queue_name in rabbitmq_broker.queues

    # nothing is sent so RabbitMQ before sending a message
    assert rabbitmq_broker._connection is None

    # If I send that actor an async message
    add.send(1, 2)

    # RabbitMQ is connected to
    assert rabbitmq_broker._connection is not None


def test_rabbitmq_actors_retry_with_backoff_on_failure(rabbitmq_broker, rabbitmq_worker):
    # Given that I have a database
    failure_time, success_time = None, None
    succeeded = Event()

    # And an actor that fails the first time it's called
    @remoulade.actor(max_retries=3, min_backoff=1000, max_backoff=5000)
    def do_work():
        nonlocal failure_time, success_time
        if not failure_time:
            failure_time = current_millis()
            raise RuntimeError("First failure.")
        else:
            success_time = current_millis()
            succeeded.set()

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # If I send it a message
    do_work.send()

    # Then wait for the actor to succeed
    succeeded.wait(timeout=30)

    # I expect backoff time to have passed between sucesss and failure
    assert 800 <= success_time - failure_time <= 1200


def test_rabbitmq_actors_retry_with_queue_escalation_on_failure(rabbitmq_broker, rabbitmq_worker):
    for queue_name in ["default.high", "default.medium"]:
        rabbitmq_broker.declare_queue(queue_name)

    incoming_queues = []
    escalation_queue_mapping_arg = {"default": "default.medium", "default.medium": "default.high"}

    @remoulade.actor(max_retries=4, escalation_queue_mapping=escalation_queue_mapping_arg, max_backoff=1)
    def do_work():
        nonlocal incoming_queues
        msg = CurrentMessage.get_current_message()
        incoming_queues.append(msg.queue_name)
        raise RuntimeError("Failure")

    rabbitmq_broker.declare_actor(do_work)

    do_work.send()

    rabbitmq_broker.join(do_work.queue_name, min_successes=40)
    rabbitmq_worker.join()

    # Queue should be escalated only once
    assert incoming_queues == ["default", "default.medium", "default.medium", "default.medium", "default.medium"]


def test_rabbitmq_actors_retry_without_queue_escalation(rabbitmq_broker, rabbitmq_worker):
    for queue_name in ["default.high", "default.medium"]:
        rabbitmq_broker.declare_queue(queue_name)

    incoming_queues = []
    escalation_queue_mapping_arg = {}

    @remoulade.actor(max_retries=4, escalation_queue_mapping=escalation_queue_mapping_arg, max_backoff=1)
    def do_work():
        nonlocal incoming_queues
        msg = CurrentMessage.get_current_message()
        incoming_queues.append(msg.queue_name)
        raise RuntimeError("Failure")

    rabbitmq_broker.declare_actor(do_work)

    do_work.send()

    rabbitmq_broker.join(do_work.queue_name, min_successes=40)
    rabbitmq_worker.join()

    # queue should stay the same
    assert incoming_queues == ["default"] * 5


def test_rabbitmq_actors_retry_with_priority_elevation_on_failure(rabbitmq_broker, rabbitmq_worker):
    incoming_priorities = []

    @remoulade.actor(max_retries=4, increase_priority_on_retry=True, max_backoff=1)
    def do_work():
        nonlocal incoming_priorities
        msg = CurrentMessage.get_current_message()
        incoming_priorities.append(msg._rabbitmq_message.priority)
        raise RuntimeError("Failure")

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)
    # If I send it a message
    do_work.send()

    # Then join on the queue
    rabbitmq_broker.join(do_work.queue_name, min_successes=40)
    rabbitmq_worker.join()

    # I expect the priority to increase once
    assert incoming_priorities == [0, 1, 1, 1, 1]


def test_rabbitmq_actors_retry_without_priority_elevation(rabbitmq_broker, rabbitmq_worker):
    incoming_priorities = []

    @remoulade.actor(max_retries=4, increase_priority_on_retry=False, max_backoff=1)
    def do_work():
        nonlocal incoming_priorities
        msg = CurrentMessage.get_current_message()
        incoming_priorities.append(msg._rabbitmq_message.priority)
        raise RuntimeError("Failure")

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)
    # If I send it a message
    do_work.send()

    # Then join on the queue
    rabbitmq_broker.join(do_work.queue_name, min_successes=40)
    rabbitmq_worker.join()

    # I expect the priority to stay none for all attempts
    assert incoming_priorities == [0] * 5


def test_rabbitmq_actors_can_retry_multiple_times(rabbitmq_broker, rabbitmq_worker):
    # Given that I have a database
    attempts = []

    # And an actor that fails 3 times then succeeds
    @remoulade.actor(max_retries=3, max_backoff=1)
    def do_work():
        attempts.append(1)
        if sum(attempts) < 4:
            raise RuntimeError("Failure #%d" % sum(attempts))

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # If I send it a message
    do_work.send()

    # Then join on the queue
    rabbitmq_broker.join(do_work.queue_name, min_successes=40)
    rabbitmq_worker.join()

    # I expect it to have been attempted 4 times
    assert sum(attempts) == 4


def test_rabbitmq_actors_can_have_their_messages_delayed(rabbitmq_broker, rabbitmq_worker):
    # Given that I have a database
    start_time, run_time = current_millis(), None

    # And an actor that records the time it ran
    @remoulade.actor
    def record():
        nonlocal run_time
        run_time = current_millis()

    # And this actor is declared
    rabbitmq_broker.declare_actor(record)

    # If I send it a delayed message
    record.send_with_options(delay=1000)

    # Then join on the queue
    rabbitmq_broker.join(record.queue_name)
    rabbitmq_worker.join()

    # I expect that message to have been processed at least delayed milliseconds later
    assert run_time - start_time >= 1000


def test_rabbitmq_actors_can_delay_messages_independent_of_each_other(rabbitmq_broker, rabbitmq_worker):
    # Given that I have a database
    results = []

    # And an actor that appends a number to the database
    @remoulade.actor
    def append(x):
        results.append(x)

    # And this actor is declared
    rabbitmq_broker.declare_actor(append)

    # And I send it a delayed message
    append.send_with_options(args=(1,), delay=1500)

    # And then another delayed message with a smaller delay
    append.send_with_options(args=(2,), delay=1000)

    # Then join on the queue
    rabbitmq_broker.join(append.queue_name, min_successes=20)
    rabbitmq_worker.join()

    # I expect the latter message to have been run first
    assert results == [2, 1]


def test_rabbitmq_actors_can_have_retry_limits(rabbitmq_broker, rabbitmq_worker):
    # Given that I have an actor that always fails
    @remoulade.actor(max_retries=0)
    def do_work():
        raise RuntimeError("failed")

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # If I send it a message
    do_work.send()

    # Then join on its queue
    rabbitmq_broker.join(do_work.queue_name)
    rabbitmq_worker.join()

    # I expect the message to get moved to the dead letter queue
    _, _, xq_count = rabbitmq_broker.get_queue_message_counts(do_work.queue_name)
    assert xq_count == 1


def test_rabbitmq_messages_belonging_to_missing_actors_are_rejected(rabbitmq_broker, rabbitmq_worker):
    # Given that I have a broker without actors
    # If I send it a message
    message = Message(queue_name="some-queue", actor_name="some-actor", args=(), kwargs={}, options={})
    rabbitmq_broker.declare_queue(message.queue_name)
    with pytest.raises(ActorNotFound):
        rabbitmq_broker.enqueue(message)


def test_rabbitmq_broker_reconnects_after_enqueue_failure(rabbitmq_broker):
    # Given that I have an actor
    @remoulade.actor
    def do_nothing():
        pass

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_nothing)

    # If I close my connection
    rabbitmq_broker.connection.close()

    # Then send my actor a message
    # I expect the message to be enqueued
    assert do_nothing.send()

    # And the connection be reopened
    assert rabbitmq_broker.connection.is_open


@mock.patch("amqpstorm.basic.Basic.publish")
@pytest.mark.confirm_delivery(True)
def test_rabbitmq_broker_retry_to_enqueue_message(publish, rabbitmq_broker):
    # Given that I have an actor
    @remoulade.actor
    def do_nothing():
        pass

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_nothing)

    publish.return_value = False
    with pytest.raises(MessageNotDelivered):
        assert do_nothing.send()

    assert publish.call_count == 6


def test_rabbitmq_connections_can_be_deleted_multiple_times(rabbitmq_broker):
    del rabbitmq_broker.connection
    del rabbitmq_broker.connection


def test_rabbitmq_channels_can_be_deleted_multiple_times(rabbitmq_broker):
    rabbitmq_broker.clear_channel_pools()
    rabbitmq_broker.clear_channel_pools()


def test_rabbitmq_consumers_ignore_unknown_messages_in_ack_and_nack(rabbitmq_broker):
    # Given that I have a RabbitmqConsumer
    consumer = rabbitmq_broker.consume("default")

    # If I attempt to ack a Message that wasn't consumed off of it
    # I expect nothing to happen
    assert consumer.ack(Mock(_tag=1)) is None

    # Likewise for nack
    assert consumer.nack(Mock(_tag=1)) is None


def test_rabbitmq_broker_can_join_with_timeout(rabbitmq_broker, rabbitmq_worker):
    # Given that I have an actor that takes a long time to run
    @remoulade.actor
    def do_work():
        time.sleep(1)

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # When I send that actor a message
    do_work.send()

    # And join on its queue with a timeout
    # Then I expect a QueueJoinTimeout to be raised
    with pytest.raises(QueueJoinTimeout):
        rabbitmq_broker.join(do_work.queue_name, timeout=500)


def test_rabbitmq_broker_can_flush_queues(rabbitmq_broker):
    # Given that I have an actor
    @remoulade.actor
    def do_work():
        pass

    # And this actor is declared
    rabbitmq_broker.declare_actor(do_work)

    # When I send that actor a message
    do_work.send()

    # And then tell the broker to flush all queues
    rabbitmq_broker.flush_all()

    # And then join on the actors's queue
    # Then it should join immediately
    assert rabbitmq_broker.join(do_work.queue_name, min_successes=1, timeout=200) is None


def test_rabbitmq_broker_can_enqueue_messages_with_priority(rabbitmq_broker):
    max_priority = 10
    message_processing_order = []
    queue_name = "prioritized"

    # Given that I have an actor that store priorities
    @remoulade.actor(queue_name=queue_name)
    def do_work(message_priority):
        message_processing_order.append(message_priority)

    remoulade.declare_actors([do_work])

    worker = Worker(rabbitmq_broker, worker_threads=1)
    worker.queue_prefetch = 1

    try:
        # When I send that actor messages with increasing priorities
        for priority in range(max_priority):
            do_work.send_with_options(args=(priority,), priority=priority)

        worker.start()
        # And then tell the broker to wait for all messages
        rabbitmq_broker.join(queue_name, timeout=5000)
        worker.join()

        # I expect the stored priorities to be saved in decreasing order
        assert message_processing_order == list(reversed(range(max_priority)))
    finally:
        worker.stop()


def test_rabbitmq_broker_disable_delivery_confirmation(rabbitmq_broker, rabbitmq_worker, stub_result_backend):
    rabbitmq_broker.add_middleware(Results(backend=stub_result_backend))

    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    rabbitmq_broker.declare_actor(do_work)

    message = do_work.send_with_options(confirm_delivery=False)

    # Then join on the queue
    rabbitmq_broker.join(do_work.queue_name)
    rabbitmq_worker.join()

    assert message.result.get() == 1


def test_rabbitmq_broker_can_use_transactions(rabbitmq_broker, rabbitmq_worker):
    call_count = 0

    @remoulade.actor()
    def do_work():
        nonlocal call_count
        call_count += 1

    rabbitmq_broker.declare_actor(do_work)

    # check than a message is executed
    with rabbitmq_broker.tx():
        do_work.send()

    # then enqueue message but raise in the transaction
    with pytest.raises(ValueError):
        with rabbitmq_broker.tx():
            [do_work.send() for _ in range(10)]
            raise ValueError()

    # Then join on the queue
    rabbitmq_broker.join(do_work.queue_name)
    rabbitmq_worker.join()

    # messages have been rollback (but not the one outside the transaction)
    assert call_count == 1


@mock.patch("remoulade.brokers.rabbitmq.RabbitmqBroker._get_channel")
@pytest.mark.confirm_delivery(True)
@pytest.mark.group_transaction(True)
def test_rabbitmq_broker_delivery_confirmation_and_group_transaction(mocked_channel, rabbitmq_broker, rabbitmq_worker):
    @remoulade.actor()
    def do_work():
        return 1

    rabbitmq_broker.declare_actor(do_work)

    do_work.send()
    assert mocked_channel.call_count == 1
    assert mocked_channel.call_args == call(True)

    remoulade.group([do_work.message()]).run()
    assert mocked_channel.call_count == 2
    assert mocked_channel.call_args == call(False)
