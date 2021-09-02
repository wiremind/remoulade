import time
from unittest import mock

import pytest

import remoulade
from remoulade import Worker
from remoulade.__main__ import main
from remoulade.middleware import MaxTasks


def test_max_tasks(stub_broker):
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=1)
    worker.start()
    stub_broker.add_middleware(MaxTasks(max_tasks=20))

    result = 0

    @remoulade.actor
    def do_work():
        nonlocal result
        result += 1

    stub_broker.declare_actor(do_work)

    for _ in range(30):
        do_work.send()

    time.sleep(0.1)

    # The worker should have run tasks until he reached max_tasks and then stopped
    assert result == 20
    assert worker.worker_stopped


@pytest.mark.timeout(5)
@mock.patch("sys.argv", ["", "tests.middleware.test_max_tasks"])
def test_max_tasks_multiple_threads(stub_broker):
    stub_broker.add_middleware(MaxTasks(max_tasks=1))

    result = 0

    @remoulade.actor
    def do_work():
        nonlocal result
        result += 1
        time.sleep(1)

    stub_broker.declare_actor(do_work)

    for _ in range(300):
        do_work.send()

    ret_code = main()

    # There are 8 worker threads
    # All worker threads should stop after the first one is finished
    # The amount of message processed may go up to 15 since every thread except the stopped one might start processing
    # another message before the main function stops them
    assert 8 <= result <= 15
    assert ret_code == 0
