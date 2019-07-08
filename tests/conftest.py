import logging
import os
import random
import subprocess
import sys
from unittest.mock import Mock

import pytest
import redis

import remoulade
from remoulade import Worker
from remoulade.brokers.local import LocalBroker
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.brokers.stub import StubBroker
from remoulade.rate_limits import backends as rl_backends
from remoulade.results import backends as res_backends
from remoulade.cancel import backends as cl_backends


logfmt = "[%(asctime)s] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=logfmt)

random.seed(1337)

CI = os.getenv("CI") == "true"


def check_rabbitmq(broker):
    try:
        broker.connection
    except Exception as e:
        raise e if CI else pytest.skip("No connection to RabbmitMQ server.")


def check_redis(client):
    try:
        client.ping()
    except redis.ConnectionError as e:
        raise e if CI else pytest.skip("No connection to Redis server.")
    client.flushall()


@pytest.fixture()
def stub_broker():
    broker = StubBroker()
    broker.emit_after("process_boot")
    remoulade.set_broker(broker)
    yield broker
    broker.flush_all()
    broker.emit_before("process_stop")
    broker.close()


@pytest.fixture()
def rabbitmq_broker():
    broker = RabbitmqBroker(max_priority=10)
    check_rabbitmq(broker)
    broker.emit_after("process_boot")
    remoulade.set_broker(broker)
    yield broker
    broker.flush_all()
    broker.emit_before("process_stop")
    broker.close()


@pytest.fixture()
def local_broker():
    broker = LocalBroker()
    broker.emit_after("process_boot")
    remoulade.set_broker(broker)
    yield broker
    broker.flush_all()
    broker.emit_before("process_stop")
    broker.close()


@pytest.fixture()
def stub_worker(stub_broker):
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture()
def rabbitmq_worker(rabbitmq_broker):
    worker = Worker(rabbitmq_broker, worker_timeout=100, worker_threads=32)
    worker.start()
    yield worker
    worker.stop()


@pytest.fixture
def info_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    yield
    logger.setLevel(logging.DEBUG)


@pytest.fixture
def start_cli():
    proc = None

    def run(broker_module, *, extra_args=None, **kwargs):
        nonlocal proc
        args = [sys.executable, "-m", "remoulade", broker_module]
        proc = subprocess.Popen(args + (extra_args or []), **kwargs)
        return proc

    yield run

    if proc is not None:
        proc.terminate()
        proc.wait()


@pytest.fixture
def start_scheduler():
    proc = None

    def run(broker_module):
        nonlocal proc
        args = ["remoulade-scheduler", broker_module]
        proc = subprocess.Popen(args)
        return proc

    yield run

    if proc is not None:
        proc.terminate()
        proc.wait()


@pytest.fixture
def redis_rate_limiter_backend():
    backend = rl_backends.RedisBackend()
    check_redis(backend.client)
    return backend


@pytest.fixture
def stub_rate_limiter_backend():
    return rl_backends.StubBackend()


@pytest.fixture
def rate_limiter_backends(redis_rate_limiter_backend, stub_rate_limiter_backend):
    return {
        "redis": redis_rate_limiter_backend,
        "stub": stub_rate_limiter_backend,
    }


@pytest.fixture(params=["redis", "stub"])
def rate_limiter_backend(request, rate_limiter_backends):
    return rate_limiter_backends[request.param]


@pytest.fixture
def redis_result_backend():
    backend = res_backends.RedisBackend()
    check_redis(backend.client)
    return backend


@pytest.fixture
def stub_result_backend():
    return res_backends.StubBackend()


@pytest.fixture
def local_result_backend():
    return res_backends.LocalBackend()


@pytest.fixture
def result_backends(redis_result_backend, stub_result_backend):
    return {
        "redis": redis_result_backend,
        "stub": stub_result_backend,
    }


@pytest.fixture(params=["redis", "stub"])
def result_backend(request, result_backends):
    return result_backends[request.param]


@pytest.fixture
def redis_cancel_backend():
    backend = cl_backends.RedisBackend()
    check_redis(backend.client)
    return backend


@pytest.fixture
def stub_cancel_backend():
    return cl_backends.StubBackend()


@pytest.fixture
def cancel_backends(redis_cancel_backend, stub_cancel_backend):
    return {
        "redis": redis_cancel_backend,
        "stub": stub_cancel_backend,
    }


@pytest.fixture(params=["redis", "stub"])
def cancel_backend(request, cancel_backends):
    return cancel_backends[request.param]


@pytest.fixture
def mock_channel_factory():
    mock_generator = (Mock(id=x, is_closed=False) for x in range(10000))

    def factory():
        return next(mock_generator)

    return factory
