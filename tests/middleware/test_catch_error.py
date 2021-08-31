import time

import remoulade
from remoulade.middleware.catch_error import CatchError


def test_cleanup_actor(stub_broker, stub_worker):
    cleanup_count = 0

    @remoulade.actor
    def cleanup(actor_name, exception_name, args, kwargs):
        nonlocal cleanup_count
        cleanup_count += 1

    @remoulade.actor
    def fail_actor():
        raise Exception()

    remoulade.declare_actors([cleanup, fail_actor])

    stub_broker.add_middleware(CatchError())
    fail_actor.send_with_options(cleanup_actor=cleanup)

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(cleanup.queue_name)
    stub_worker.join()

    # The cleanup should have run
    assert cleanup_count == 1


def test_cleanup_runs_only_once(stub_broker, stub_worker):
    cleanup_count = 0

    @remoulade.actor
    def cleanup(actor_name, exception_name, args, kwargs):
        nonlocal cleanup_count
        cleanup_count += 1

    @remoulade.actor
    def fail_actor():
        raise Exception()

    remoulade.declare_actors([cleanup, fail_actor])

    stub_broker.add_middleware(CatchError())
    fail_actor.send_with_options(cleanup_actor=cleanup, max_retries=3, min_backoff=1, retry_strategy="constant")

    stub_broker.join(fail_actor.queue_name)
    stub_broker.join(cleanup.queue_name)
    stub_worker.join()

    # The cleanup should have run only once
    assert cleanup_count == 1


def test_clean_runs_on_timeout(stub_broker, stub_worker):
    cleanup_count = 0

    @remoulade.actor
    def cleanup(actor_name, exception_name, args, kwargs):
        nonlocal cleanup_count
        cleanup_count += 1

    @remoulade.actor
    def do_work():
        time.sleep(1)

    remoulade.declare_actors([cleanup, do_work])

    stub_broker.add_middleware(CatchError())
    do_work.send_with_options(cleanup_actor=cleanup, time_limit=1)

    stub_broker.join(do_work.queue_name)
    stub_broker.join(cleanup.queue_name)
    stub_worker.join()

    # The cleanup should have run
    assert cleanup_count == 1
