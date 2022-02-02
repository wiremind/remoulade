from time import sleep
from unittest import mock
from urllib import request as request

import pytest

from remoulade.brokers.stub import StubBroker

prometheus = pytest.importorskip("remoulade.middleware.prometheus")


def test_prometheus_middleware_exposes_metrics():
    try:
        # Given a broker
        broker = StubBroker()

        # And an instance of the prometheus middleware
        prom = prometheus.Prometheus()
        prom.before_worker_boot(broker, None)

        # If I wait for the server to start
        sleep(0.01)

        # When I request metrics via HTTP
        with request.urlopen("http://127.0.0.1:9191") as resp:
            # Then the response should be successful
            assert resp.getcode() == 200
    finally:
        prom.after_worker_shutdown(broker, None)


@mock.patch("prometheus_client.start_http_server")
def test_prometheus_process_message(_, stub_broker, do_work):
    prom = prometheus.Prometheus()

    stub_broker.emit_before("worker_boot", mock.Mock())
    prom.before_worker_boot(stub_broker, mock.Mock(consumer_whitelist=None))

    message = do_work.message()

    assert not prom.message_start_times
    prom.before_process_message(stub_broker, message)
    assert prom.message_start_times
    prom.after_process_message(stub_broker, message)
    assert not prom.message_start_times
