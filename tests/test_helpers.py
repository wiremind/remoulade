import remoulade
from remoulade.helpers import compute_backoff, get_actor_arguments
from remoulade.helpers.reduce import reduce
from remoulade.results import Results


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

    assert 10 == result


def test_actor_arguments():
    @remoulade.actor
    def do_work(a: int = None):
        return 1

    assert get_actor_arguments(do_work) == [{"default": "", "name": "a", "type": "int"}]


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
