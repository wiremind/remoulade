import time
from unittest import mock

from remoulade.middleware import MaxMemory


@mock.patch("remoulade.middleware.max_memory.resource.getrusage")
def test_below_max_memory(getrusage, stub_broker, stub_worker, do_work):
    getrusage.return_value = mock.MagicMock(ru_maxrss=50)

    # Given a broker with a MaxMemory middleware
    stub_broker.add_middleware(MaxMemory(max_memory=100))

    do_work.send()
    stub_broker.join(do_work.queue_name)
    stub_worker.join()
    time.sleep(0.01)

    assert not stub_worker.worker_stopped


@mock.patch("remoulade.middleware.max_memory.resource.getrusage")
def test_above_max_memory(getrusage, stub_broker, stub_worker, do_work):
    getrusage.return_value = mock.MagicMock(ru_maxrss=200)

    # Given a broker with a MaxMemory middleware
    stub_broker.add_middleware(MaxMemory(max_memory=100))

    do_work.send()
    stub_broker.join(do_work.queue_name)
    stub_worker.join()
    time.sleep(0.01)  # wait a bit for worker thread to finish

    assert stub_worker.worker_stopped
