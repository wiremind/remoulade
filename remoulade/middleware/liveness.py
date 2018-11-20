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
import os
from http.server import BaseHTTPRequestHandler

from .middleware import Middleware
from .utils import ExpositionServer
from ..logging import get_logger

#: The path to the file to use to race Exposition servers against one another.
LOCK_PATH = os.getenv("remoulade_prom_lock", "/tmp/remoulade-liveness.lock")

#: The default HTTP host the exposition server should bind to.
DEFAULT_HTTP_HOST = os.getenv("remoulade_liveness_host", "127.0.0.1")

#: The default HTTP port the exposition server should listen on.
DEFAULT_HTTP_PORT = int(os.getenv("remoulade_liveness_port", "8080"))


class Liveness(Middleware):
    """A middleware that act as a liveness probe.
    # TODO: docd

    Parameters:
      http_host(str): The host to bind the Prometheus exposition
        server on.  This parameter can also be configured via the
        ``remoulade_prom_host`` environment variable.
      http_port(int): The port on which the server should listen.
        This parameter can also be configured via the
        ``remoulade_prom_port`` environment variable.
    """

    def __init__(self, *, http_host=DEFAULT_HTTP_HOST, http_port=DEFAULT_HTTP_PORT):
        self.logger = get_logger(__name__, type(self))
        self.http_host = http_host
        self.http_port = http_port
        self.server = None

    def after_process_boot(self, broker):
        self.logger.debug("Starting liveness exposition server...")
        self.server = ExpositionServer(
            http_host=self.http_host,
            http_port=self.http_port,
            lockfile=LOCK_PATH,
            handler=LivenessHandler
        )

        self.server.start()

    def after_worker_shutdown(self, broker, worker):
        self.logger.debug("Shutting down exposition server...")
        self.server.stop()


class LivenessHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.end_headers()
        self.wfile.write('ok'.encode('utf-8'))

    def log_message(self, fmt, *args):
        logger = get_logger(__name__, type(self))
        logger.debug(fmt, *args)
