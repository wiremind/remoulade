# ruff: noqa: S106
# ruff: noqa: S105
import time


def test_redis_backend_acquire_and_release(redis_concurrency_backend):
    backend = redis_concurrency_backend
    key = "concurrent-test"

    lease = backend.acquire(key=key, limit=1, ttl_ms=1000, token="token1")
    assert lease is not None
    assert lease.key == key
    assert lease.expires_at_ms >= int(time.time() * 1000)

    assert backend.acquire(key=key, limit=1, ttl_ms=1000, token="token2") is None

    backend.release(lease)

    lease_again = backend.acquire(key=key, limit=1, ttl_ms=1000, token="token2")
    assert lease_again is not None
    assert lease_again.token == "token2"


def test_redis_backend_acquire_and_release_multiple_tokens(redis_concurrency_backend):
    backend = redis_concurrency_backend
    key = "concurrent-test"

    lease = backend.acquire(key=key, limit=2, ttl_ms=1000, token="token1")
    assert lease is not None
    assert lease.key == key
    assert lease.expires_at_ms >= int(time.time() * 1000)

    second_lease = backend.acquire(key=key, limit=2, ttl_ms=1000, token="token2")
    assert second_lease is not None
    assert second_lease.key == key
    assert second_lease.expires_at_ms >= int(time.time() * 1000)

    assert backend.acquire(key=key, limit=2, ttl_ms=1000, token="token3") is None

    backend.release(lease)

    lease_again = backend.acquire(key=key, limit=2, ttl_ms=1000, token="token4")
    assert lease_again is not None
    assert lease_again.token == "token4"
