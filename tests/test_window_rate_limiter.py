import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

from remoulade.rate_limits import WindowRateLimiter


def test_window_rate_limiter_limits_per_window(rate_limiter_backend):
    # Given that I have a bucket rate limiter and a call database
    limiter = WindowRateLimiter(rate_limiter_backend, "window-test", limit=2, window=1)
    calls = Counter()

    # And a function that increments keys over the span of 3 seconds
    def work():
        for _ in range(15):
            for _ in range(8):
                with limiter.acquire(raise_on_failure=False) as acquired:
                    if not acquired:
                        continue

                    calls[int(time.time())] += 1

            time.sleep(0.2)

    # If I run that function multiple times concurrently
    with ThreadPoolExecutor(max_workers=8) as e:
        futures = []
        for _ in range(8):
            futures.append(e.submit(work))

        for future in futures:
            future.result()

    # I expect between 6 and 10 calls to have been made in total
    assert 6 <= sum(calls.values()) <= 10
