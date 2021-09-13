import logging
import os
import random
import subprocess
import sys
import threading
from unittest.mock import Mock

import pytest
import redis
from freezegun import freeze_time

import remoulade
from remoulade import Worker
from remoulade.brokers.local import LocalBroker
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.brokers.stub import StubBroker
from remoulade.cancel import backends as cl_backends
from remoulade.rate_limits import backends as rl_backends
from remoulade.results import Results
from remoulade.results import backends as res_backends
from remoulade.scheduler import Scheduler
from remoulade.state import MessageState
from remoulade.state import backends as st_backends

logfmt = "[%(asctime)s] [%(threadName)s] [%(name)s] [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=logfmt)

random.seed(1337)

CI = os.getenv("CI") == "true"


def check_rabbitmq(broker):
    try:
        broker.connection
    except Exception as e:
        raise e from e if CI else pytest.skip("No connection to RabbmitMQ server.")


def check_redis(client):
    try:
        client.ping()
    except redis.ConnectionError as e:
        raise e from e if CI else pytest.skip("No connection to Redis server.")
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
    rmq_url = os.getenv("REMOULADE_TEST_RABBITMQ_URL") or "amqp://guest:guest@localhost:5784"
    broker = RabbitmqBroker(max_priority=10, url=rmq_url)
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
def scheduler_thread():
    scheduler = remoulade.get_scheduler()
    thread = threading.Thread(target=scheduler.start)
    yield thread
    scheduler.stop()


@pytest.fixture
def redis_rate_limiter_backend():
    redis_url = os.getenv("REMOULADE_TEST_REDIS_URL") or "redis://localhost:6481/0"
    backend = rl_backends.RedisBackend(url=redis_url)
    check_redis(backend.client)
    return backend


@pytest.fixture
def stub_rate_limiter_backend():
    return rl_backends.StubBackend()


@pytest.fixture
def rate_limiter_backends(redis_rate_limiter_backend, stub_rate_limiter_backend):
    return {"redis": redis_rate_limiter_backend, "stub": stub_rate_limiter_backend}


@pytest.fixture(params=["redis", "stub"])
def rate_limiter_backend(request, rate_limiter_backends):
    return rate_limiter_backends[request.param]


@pytest.fixture
def redis_result_backend():
    redis_url = os.getenv("REMOULADE_TEST_REDIS_URL") or "redis://localhost:6481/0"
    backend = res_backends.RedisBackend(url=redis_url)
    check_redis(backend.client)
    return backend


@pytest.fixture
def redis_state_backend():
    redis_url = os.getenv("REMOULADE_TEST_REDIS_URL") or "redis://localhost:6481/0"
    backend = st_backends.RedisBackend(url=redis_url)
    check_redis(backend.client)
    return backend


@pytest.fixture
def stub_state_backend():
    return st_backends.StubBackend()


@pytest.fixture
def state_backends(redis_state_backend, stub_state_backend):
    return {"redis": redis_state_backend, "stub": stub_state_backend}


@pytest.fixture(params=["redis", "stub"])
def state_backend(request, state_backends):
    return state_backends[request.param]


@pytest.fixture
def state_middleware(state_backend):
    broker = remoulade.get_broker()
    middleware = MessageState(backend=state_backend)
    broker.add_middleware(middleware)
    return middleware


@pytest.fixture
def result_middleware(result_backend):
    broker = remoulade.get_broker()
    middleware = Results(backend=result_backend)
    broker.add_middleware(middleware)
    return middleware


@pytest.fixture
def do_work():
    broker = remoulade.get_broker()

    @remoulade.actor
    def do_work():
        return 1

    broker.declare_actor(do_work)
    return do_work


@pytest.fixture
def add():
    broker = remoulade.get_broker()

    @remoulade.actor()
    def add(x, y):
        return x + y

    broker.declare_actor(add)
    return add


@pytest.fixture
def mul():
    broker = remoulade.get_broker()

    @remoulade.actor()
    def mul(x, y):
        return x * y

    broker.declare_actor(mul)
    return mul


@pytest.fixture
def stub_result_backend():
    return res_backends.StubBackend()


@pytest.fixture
def local_result_backend():
    return res_backends.LocalBackend()


@pytest.fixture
def result_backends(redis_result_backend, stub_result_backend):
    return {"redis": redis_result_backend, "stub": stub_result_backend}


@pytest.fixture(params=["redis", "stub"])
def result_backend(request, result_backends):
    return result_backends[request.param]


@pytest.fixture
def redis_cancel_backend():
    redis_url = os.getenv("REMOULADE_TEST_REDIS_URL") or "redis://localhost:6481/0"
    backend = cl_backends.RedisBackend(url=redis_url)
    check_redis(backend.client)
    return backend


@pytest.fixture
def stub_cancel_backend():
    return cl_backends.StubBackend()


@pytest.fixture
def cancel_backends(redis_cancel_backend, stub_cancel_backend):
    return {"redis": redis_cancel_backend, "stub": stub_cancel_backend}


@pytest.fixture(params=["redis", "stub"])
def cancel_backend(request, cancel_backends):
    return cancel_backends[request.param]


@pytest.fixture
def mock_channel_factory():
    mock_generator = (Mock(id=x, is_closed=False) for x in range(10000))

    def factory():
        return next(mock_generator)

    return factory


@pytest.fixture
def frozen_datetime():
    with freeze_time("2020-02-03") as frozen_datetime:
        yield frozen_datetime


def new_scheduler(stub_broker):
    redis_url = os.getenv("REMOULADE_TEST_REDIS_URL") or "redis://localhost:6481/0"
    scheduler = Scheduler(stub_broker, [], period=0.1, url=redis_url)
    remoulade.set_scheduler(scheduler)
    return scheduler


@pytest.fixture
def scheduler(stub_broker):
    scheduler = new_scheduler(stub_broker)
    check_redis(scheduler.client)
    return scheduler


@pytest.fixture
def pickle_encoder():
    old_encoder = remoulade.get_encoder()
    new_encoder = remoulade.PickleEncoder()
    remoulade.set_encoder(new_encoder)
    yield new_encoder
    remoulade.set_encoder(old_encoder)
