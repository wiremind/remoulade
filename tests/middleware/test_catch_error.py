import time

import remoulade


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
