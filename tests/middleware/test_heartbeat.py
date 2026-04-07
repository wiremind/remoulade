# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 WIREMIND SAS <dev@wiremind.fr>
#
# Remoulade is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Remoulade is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import contextlib
import logging
import threading
import time
from datetime import timedelta
from unittest import mock
from urllib import error, request

import pytest
from freezegun import freeze_time

from remoulade.brokers.stub import StubBroker
from remoulade.middleware import Heartbeat
from remoulade.worker import Worker


def _build_url(heartbeat: Heartbeat, query: str = "") -> str:
    suffix = f"?{query}" if query else ""
    return f"http://127.0.0.1:{heartbeat.server.server_port}/{suffix}"


def _get_status(url: str) -> int:
    try:
        with request.urlopen(url, timeout=1) as response:  # noqa: S310
            return response.getcode()
    except error.HTTPError as exc:
        return exc.code


def _wait_for_threads(heartbeat: Heartbeat, count: int, timeout: float = 1) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with heartbeat.lock:
            if len(heartbeat.heartbeats) == count:
                return
        time.sleep(0.01)
    raise AssertionError(f"Timed out waiting for {count} heartbeat threads.")


def _wait_for_response(url: str, timeout: float = 1) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return _get_status(url)
        except error.URLError:
            time.sleep(0.01)
    return _get_status(url)


@contextlib.contextmanager
def _heartbeat(**kwargs):
    heartbeat = Heartbeat(http_port=0, **kwargs)
    try:
        yield heartbeat
    finally:
        with contextlib.suppress(Exception):
            heartbeat.server.server_close()


@contextlib.contextmanager
def _running_worker(stub_broker: StubBroker, heartbeat: Heartbeat, *, worker_threads: int = 1):
    stub_broker.add_middleware(heartbeat)
    worker = Worker(stub_broker, worker_timeout=100, worker_threads=worker_threads)
    worker.start()
    _wait_for_threads(heartbeat, worker_threads)
    _wait_for_response(_build_url(heartbeat))
    try:
        yield worker
    finally:
        worker.stop()


@contextlib.contextmanager
def _running_server(stub_broker: StubBroker, heartbeat: Heartbeat):
    worker = mock.Mock()
    heartbeat.before_worker_boot(stub_broker, worker)
    _wait_for_response(_build_url(heartbeat))
    try:
        yield
    finally:
        heartbeat.after_worker_shutdown(stub_broker, worker)


def test_heartbeat_exposes_http_liveness(stub_broker: StubBroker):
    with _heartbeat() as heartbeat, _running_worker(stub_broker, heartbeat):
        assert _get_status(_build_url(heartbeat)) == 200


def test_heartbeat_uses_default_threshold_when_request_does_not_override_it(stub_broker: StubBroker):
    with (
        _heartbeat(threshold=1) as heartbeat,
        _running_server(stub_broker, heartbeat),
        mock.patch.object(heartbeat, "healthy", side_effect=lambda threshold: threshold == 3) as healthy,
    ):
        assert _get_status(_build_url(heartbeat)) == 500
        assert _get_status(_build_url(heartbeat, "threshold=3")) == 200
        assert healthy.call_args_list == [mock.call(1), mock.call(3)]


def test_heartbeat_returns_500_without_any_worker_threads(stub_broker: StubBroker):
    with _heartbeat() as heartbeat, _running_server(stub_broker, heartbeat):
        assert _wait_for_response(_build_url(heartbeat)) == 500


@pytest.mark.parametrize("query", ["threshold=abc", "threshold=0"])
def test_heartbeat_logs_and_returns_200_on_invalid_threshold(
    stub_broker: StubBroker,
    caplog: pytest.LogCaptureFixture,
    query: str,
):
    with _heartbeat() as heartbeat, _running_worker(stub_broker, heartbeat):
        with caplog.at_level(logging.ERROR, logger="remoulade.middleware.heartbeat.HeartbeatMiddleware"):
            assert _get_status(_build_url(heartbeat, query)) == 200

        assert any(record.message == "Heartbeat probe failed." for record in caplog.records)


def test_heartbeat_logs_and_returns_200_on_probe_exception(
    stub_broker: StubBroker,
    caplog: pytest.LogCaptureFixture,
):
    with (
        _heartbeat() as heartbeat,
        _running_worker(stub_broker, heartbeat),
        mock.patch.object(heartbeat, "healthy", side_effect=RuntimeError("boom")),
        caplog.at_level(logging.ERROR, logger="remoulade.middleware.heartbeat.HeartbeatMiddleware"),
    ):
        assert _get_status(_build_url(heartbeat)) == 200

    assert any(record.message == "Heartbeat probe failed." for record in caplog.records)


def test_heartbeat_server_shuts_down_cleanly(stub_broker: StubBroker):
    with _heartbeat() as heartbeat:
        with _running_worker(stub_broker, heartbeat):
            url = _build_url(heartbeat)

        with pytest.raises(error.URLError):
            request.urlopen(url, timeout=1)  # noqa: S310


def test_healthy_returns_false_when_any_worker_thread_is_stale():
    with _heartbeat() as heartbeat, freeze_time("2020-02-03 00:00:00") as frozen_time:
        with heartbeat.lock:
            heartbeat.heartbeats[1] = time.time()
        frozen_time.tick(delta=timedelta(seconds=2))
        with heartbeat.lock:
            heartbeat.heartbeats[2] = time.time()

        assert not heartbeat.healthy(1)


def test_healthy_returns_false_without_any_worker_threads():
    with _heartbeat() as heartbeat:
        assert not heartbeat.healthy(1)


def test_healthy_raises_on_non_positive_threshold():
    with _heartbeat() as heartbeat, pytest.raises(ValueError, match="strictly positive"):
        heartbeat.healthy(0)


def test_heartbeat_debounces_publication():
    with _heartbeat() as heartbeat, freeze_time("2020-02-03 00:00:00") as frozen_time:
        start_time = time.time()
        heartbeat.local.last_beat = start_time
        thread_id = threading.get_ident()
        with heartbeat.lock:
            heartbeat.heartbeats[thread_id] = start_time

        frozen_time.tick(delta=timedelta(seconds=9))
        heartbeat.beat()
        with heartbeat.lock:
            assert heartbeat.heartbeats[thread_id] == start_time

        frozen_time.tick(delta=timedelta(seconds=1))
        heartbeat.beat()
        with heartbeat.lock:
            assert heartbeat.heartbeats[thread_id] == time.time()
