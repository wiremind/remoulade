import threading
import time
from queue import Queue

import pytest
from redis import ConnectionError as RedisConnectionError
from redis.asyncio.retry import Retry as AsyncRetry
from redis.retry import Retry

import remoulade
from remoulade import QueueJoinTimeout
from remoulade.helpers import compute_backoff, get_actor_arguments
from remoulade.helpers.queues import join_queue
from remoulade.helpers.redis_client import HEALTH_CHECK_INTERVAL, async_redis_client, redis_client
from remoulade.helpers.reduce import reduce
from remoulade.results import Results

SENTINEL_URL = "sentinel://:secret@localhost:26379/mymaster"
PLAIN_URL = "redis://localhost:6379/0"


def test_reduce_messages(stub_broker, stub_worker, result_backend):
    # Given a result backend
    # And a broker with the results middleware
    stub_broker.add_middleware(Results(backend=result_backend))

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def do_work():
        return 1

    # Given an actor that stores results
    @remoulade.actor(store_results=True)
    def merge(results):
        return sum(results)

    # And this actor is declared
    stub_broker.declare_actor(do_work)
    stub_broker.declare_actor(merge)

    merged_message = reduce((do_work.message() for _ in range(10)), merge)
    merged_message.run()

    result = merged_message.result.get(block=True)

    assert result == 10


def test_actor_arguments():
    @remoulade.actor
    def do_work(a: int | None = None):
        return 1

    assert get_actor_arguments(do_work) == [{"default": "", "name": "a", "type": "int | None"}]


def test_compute_backoff_exponential():
    assert compute_backoff(4, min_backoff=10, jitter=False, max_backoff=50, max_retries=4) == (5, 50)
    assert compute_backoff(2, min_backoff=10, jitter=False, max_backoff=100, max_retries=4) == (3, 40)
    assert compute_backoff(5, min_backoff=10, jitter=False, max_backoff=100, max_retries=3) == (6, 40)


def test_compute_backoff_constant():
    for retry in range(5):
        assert compute_backoff(retry, min_backoff=10, jitter=False, backoff_strategy="constant") == (retry + 1, 10)


def test_compute_backoff_linear():
    assert compute_backoff(3, min_backoff=10, jitter=False, backoff_strategy="linear") == (4, 40)
    assert compute_backoff(4, min_backoff=10, jitter=False, backoff_strategy="linear") == (5, 50)


def test_compute_backoff_spread_linear():
    assert compute_backoff(
        2, min_backoff=10, max_backoff=210, max_retries=5, jitter=False, backoff_strategy="spread_linear"
    ) == (3, 110)
    assert compute_backoff(
        3, min_backoff=10, max_backoff=210, max_retries=5, jitter=False, backoff_strategy="spread_linear"
    ) == (4, 160)
    assert compute_backoff(
        3, min_backoff=10, max_backoff=410, max_retries=5, jitter=False, backoff_strategy="spread_linear"
    ) == (4, 310)


def test_compute_backoff_spread_exponential():
    assert compute_backoff(
        0, min_backoff=10, jitter=False, max_backoff=100, max_retries=2, backoff_strategy="spread_exponential"
    ) == (1, 10)
    assert compute_backoff(
        1, min_backoff=10, jitter=False, max_backoff=160, max_retries=3, backoff_strategy="spread_exponential"
    ) == (2, 40)
    assert compute_backoff(
        4, min_backoff=10, jitter=False, max_backoff=160, max_retries=3, backoff_strategy="spread_exponential"
    ) == (5, 160)


class _GeventStyleQueue:
    def __init__(self, result):
        self.result = result
        self.received_timeout = None

    def join(self, timeout=None):
        self.received_timeout = timeout
        return self.result


def test_join_queue_gevent_style_timeout_raises_queue_join_timeout():
    queue = _GeventStyleQueue(result=False)

    with pytest.raises(QueueJoinTimeout):
        join_queue(queue, timeout=0.1)


def test_join_queue_gevent_style_success():
    queue = _GeventStyleQueue(result=True)
    join_queue(queue, timeout=0.1)
    assert queue.received_timeout == 0.1


def test_join_queue_stdlib_fallback_success():
    queue = Queue()
    queue.put(1)

    def worker():
        _ = queue.get()
        time.sleep(0.01)
        queue.task_done()

    thread = threading.Thread(target=worker)
    thread.start()
    join_queue(queue, timeout=0.2)
    thread.join()


def test_join_queue_stdlib_fallback_timeout_raises_queue_join_timeout():
    queue = Queue()
    queue.put(1)

    with pytest.raises(QueueJoinTimeout):
        join_queue(queue, timeout=0.01)


@pytest.mark.parametrize("url", [SENTINEL_URL, PLAIN_URL])
def test_redis_client_sets_resilient_connection_parameters(url):
    connection_kwargs = redis_client(url, socket_timeout=5.0).connection_pool.connection_kwargs

    assert connection_kwargs["health_check_interval"] == HEALTH_CHECK_INTERVAL
    assert connection_kwargs["socket_keepalive"] is True
    assert isinstance(connection_kwargs["retry"], Retry)
    assert RedisConnectionError in connection_kwargs["retry_on_error"]


@pytest.mark.parametrize("url", [SENTINEL_URL, PLAIN_URL])
def test_async_redis_client_sets_resilient_connection_parameters(url):
    connection_kwargs = async_redis_client(url, socket_timeout=5.0).connection_pool.connection_kwargs

    assert connection_kwargs["health_check_interval"] == HEALTH_CHECK_INTERVAL
    assert connection_kwargs["socket_keepalive"] is True
    assert isinstance(connection_kwargs["retry"], AsyncRetry)
    assert RedisConnectionError in connection_kwargs["retry_on_error"]


def test_redis_client_is_resilient_without_socket_timeout():
    # The scheduler builds its client without a socket_timeout; it must still get health checks and retries.
    connection_kwargs = redis_client(PLAIN_URL).connection_pool.connection_kwargs

    assert connection_kwargs["health_check_interval"] == HEALTH_CHECK_INTERVAL
    assert isinstance(connection_kwargs["retry"], Retry)
    assert RedisConnectionError in connection_kwargs["retry_on_error"]
