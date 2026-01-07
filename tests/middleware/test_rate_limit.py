from unittest import mock

import pytest

import remoulade

pytest.importorskip("limits")

from remoulade.rate_limits import RateLimitEnqueue, RateLimitExceeded, RateLimitSpecificationError
from remoulade.rate_limits.backend import RateLimitBackend
from remoulade.rate_limits.backends import StubBackend


def test_rate_limit_blocks_excess_enqueues(stub_broker, rate_limit_backend):
    stub_broker.add_middleware(RateLimitEnqueue(backend=rate_limit_backend))

    @remoulade.actor(enqueue_rate_limits="2 per second")
    def limited():
        return "ok"

    stub_broker.declare_actor(limited)

    limited.send()
    limited.send()

    with pytest.raises(RateLimitExceeded):
        limited.send()

    assert stub_broker.queues[limited.queue_name].qsize() == 2


def test_rate_limit_rejects_invalid_spec(stub_broker, rate_limit_backend):
    stub_broker.add_middleware(RateLimitEnqueue(backend=rate_limit_backend))

    @remoulade.actor(enqueue_rate_limits="definitely-not-a-limit")
    def limited():
        return "ok"

    stub_broker.declare_actor(limited)

    with pytest.raises(RateLimitSpecificationError, match="Invalid rate_limits specification"):
        limited.send()

    assert stub_broker.queues[limited.queue_name].qsize() == 0


def test_rate_limit_uses_message_override(stub_broker, rate_limit_backend):
    stub_broker.add_middleware(RateLimitEnqueue(backend=rate_limit_backend))

    @remoulade.actor(enqueue_rate_limits="10 per second")
    def limited():
        return "ok"

    stub_broker.declare_actor(limited)

    limited.send_with_options(enqueue_rate_limits="1 per second")

    with pytest.raises(RateLimitExceeded):
        limited.send_with_options(enqueue_rate_limits="1 per second")

    assert stub_broker.queues[limited.queue_name].qsize() == 1


def test_rate_limit_ignored_when_option_missing(stub_broker):
    backend = mock.create_autospec(RateLimitBackend, instance=True, spec_set=True)
    backend.hit.return_value = True
    stub_broker.add_middleware(RateLimitEnqueue(backend=backend))

    @remoulade.actor
    def do_work():
        return "ok"

    stub_broker.declare_actor(do_work)
    do_work.send()

    backend.hit.assert_not_called()


def test_rate_limit_unknown_strategy():
    with pytest.raises(ValueError, match="Unknown rate limit strategy"):
        StubBackend(strategy="unknown")
