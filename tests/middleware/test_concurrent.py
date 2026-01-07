import pytest

import remoulade
from remoulade.concurrent import ConcurrencyLimitExceeded, Concurrent


def test_concurrent_blocks_when_limit_reached(stub_broker, stub_concurrency_backend):
    middleware = Concurrent(backend=stub_concurrency_backend, default_ttl_ms=1000)
    stub_broker.add_middleware(middleware)

    @remoulade.actor(concurrency_limit=1)
    def limited():
        return "ok"

    stub_broker.declare_actor(limited)

    first = limited.message_with_options()
    middleware.before_process_message(stub_broker, first)

    with pytest.raises(ConcurrencyLimitExceeded):
        middleware.before_process_message(stub_broker, limited.message_with_options())

    middleware.after_process_message(stub_broker, first)
    middleware.before_process_message(stub_broker, limited.message_with_options())


def test_concurrent_supports_custom_key_and_bypass(stub_broker, stub_concurrency_backend):
    middleware = Concurrent(backend=stub_concurrency_backend, default_ttl_ms=1000)
    stub_broker.add_middleware(middleware)

    @remoulade.actor(concurrency_limit=1, concurrency_key="{args[0]}")
    def limited(key):
        return key

    stub_broker.declare_actor(limited)

    first = limited.message_with_options(args=("a",))
    middleware.before_process_message(stub_broker, first)

    second = limited.message_with_options(args=("b",))
    middleware.before_process_message(stub_broker, second)

    lease_keys = {lease.key for leases in middleware._leases.values() for lease in leases}
    assert lease_keys == {"a", "b"}

    bypassed = limited.message_with_options(args=("a",), bypass_concurrency_limits=True)
    middleware.before_process_message(stub_broker, bypassed)

    middleware.after_process_message(stub_broker, first)
    middleware.after_process_message(stub_broker, second)
