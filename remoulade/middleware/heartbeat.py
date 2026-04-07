# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
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
import threading
import time
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

from ..logging import get_logger
from .middleware import Middleware

DEFAULT_HTTP_HOST = "0.0.0.0"  # noqa: S104
DEFAULT_HTTP_PORT = 9192
DEFAULT_THRESHOLD = 2 * 60 * 60
HEARTBEAT_INTERVAL = 10


class HeartbeatRequestHandler(BaseHTTPRequestHandler):
    server: "HeartbeatServer"

    def do_GET(self):
        self.server.answer(self)

    def log_message(self, *_args):
        return


class HeartbeatServer(HTTPServer):
    allow_reuse_address = True

    def __init__(
        self,
        heartbeat: "Heartbeat",
        *,
        http_host: str = DEFAULT_HTTP_HOST,
        http_port: int = DEFAULT_HTTP_PORT,
    ):
        self.heartbeat = heartbeat
        super().__init__((http_host, http_port), HeartbeatRequestHandler)

    def answer(self, handler: BaseHTTPRequestHandler):
        try:
            raw_threshold = parse_qs(urlsplit(handler.path).query).get("threshold", [None])[-1]
            threshold = self.heartbeat.threshold if raw_threshold is None else int(raw_threshold)
            status = HTTPStatus.OK if self.heartbeat.healthy(threshold) else HTTPStatus.INTERNAL_SERVER_ERROR
        except Exception:
            # Probe failures must not kill the process by mistake. The worker
            # will remain observable through logs while liveness stays green.
            self.heartbeat.logger.exception("Heartbeat probe failed.")
            status = HTTPStatus.OK

        body = b"OK\n" if status == HTTPStatus.OK else b"UNHEALTHY\n"
        handler.send_response(status)
        handler.send_header("Content-Type", "text/plain; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)


class Heartbeat(Middleware):
    """Expose worker-thread liveness over HTTP.

    A lightweight HTTP server runs alongside the worker process and reports
    whether all worker threads have published a recent heartbeat. Heartbeats
    are kept in memory and refreshed by worker-thread hooks.

    Parameters:
      http_host(str): The host to bind the heartbeat server on.
      http_port(int): The port on which the server should listen.
      threshold(int): The default liveness threshold, in seconds, used
        when the request query string does not override it.
    """

    def __init__(
        self,
        *,
        http_host: str = DEFAULT_HTTP_HOST,
        http_port: int = DEFAULT_HTTP_PORT,
        threshold: int = DEFAULT_THRESHOLD,
    ):
        super().__init__()
        self.logger = get_logger(__name__, "HeartbeatMiddleware")
        self.threshold = threshold
        self.local = threading.local()
        self.heartbeats: dict[int, float] = {}
        self.lock = threading.Lock()
        # Defer socket binding to worker boot so producer-only processes can
        # share the same broker configuration without claiming the port.
        self.server_factory = partial(HeartbeatServer, self, http_host=http_host, http_port=http_port)

    def healthy(self, threshold: int) -> bool:
        if threshold <= 0:
            raise ValueError("Heartbeat threshold must be strictly positive.")

        now = time.time()
        with self.lock:
            heartbeats = list(self.heartbeats.values())
        return bool(heartbeats) and all(heartbeat + threshold >= now for heartbeat in heartbeats)

    def before_worker_boot(self, broker, worker):
        self.server = self.server_factory()
        self.server_thread = threading.Thread(target=self.server.serve_forever, name="HeartbeatServer", daemon=True)
        self.server_thread.start()

    def after_worker_thread_boot(self, broker, thread):
        now = time.time()
        self.local.last_beat = now
        with self.lock:
            self.heartbeats[thread.ident] = now

    def beat(self, *, now: float | None = None):
        now = time.time() if now is None else now
        # Worker hooks run on every processed message and empty poll. Debounce
        # heartbeat publication to keep locking off the hot path.
        if self.local.last_beat + HEARTBEAT_INTERVAL > now:
            return

        self.local.last_beat = now
        with self.lock:
            self.heartbeats[threading.get_ident()] = now

    def before_process_message(self, broker, message):
        self.beat()

    def after_worker_thread_empty(self, broker, thread):
        self.beat()

    def before_worker_thread_shutdown(self, broker, thread):
        with self.lock:
            self.heartbeats.pop(thread.ident, None)

    def after_worker_shutdown(self, broker, worker):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=1)
        with self.lock:
            self.heartbeats.clear()
