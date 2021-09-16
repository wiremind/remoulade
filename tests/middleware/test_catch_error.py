import time

import remoulade


def test_on_failure(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, args, kwargs):
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
    def on_failure(actor_name, exception_name, args, kwargs):
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
    def on_failure(actor_name, exception_name, args, kwargs):
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
    def on_failure(actor_name, exception_name, args, kwargs):
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


def test_clean_runs_on_timeout(stub_broker, stub_worker):
    on_failure_count = 0

    @remoulade.actor
    def on_failure(actor_name, exception_name, args, kwargs):
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
