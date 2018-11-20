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
import fcntl
from contextlib import contextmanager
from http.server import HTTPServer
from threading import Thread

from ...logging import get_logger


class ExpositionServer(Thread):
    """Exposition servers race against a POSIX lock in order to bind
    an HTTP server that can expose Prometheus metrics in the
    background.
    """

    def __init__(self, *, http_host, http_port, lockfile, handler):
        super().__init__(daemon=True)

        self.logger = get_logger(__name__, type(self))
        self.address = (http_host, http_port)
        self.httpd = None
        self.lockfile = lockfile
        self.handler = handler

    def run(self):
        with flock(self.lockfile) as acquired:
            if not acquired:
                self.logger.debug("Failed to acquire lock file.")
                return

            self.logger.debug("Lock file acquired. Running exposition server.")

            try:
                self.httpd = HTTPServer(self.address, self.handler)
                self.httpd.serve_forever()
            except OSError:
                self.logger.warning("Failed to bind exposition server.", exc_info=True)

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.join()


@contextmanager
def flock(path):
    """Attempt to acquire a POSIX file lock.
    """
    with open(path, "w+") as lf:
        try:
            fcntl.flock(lf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
            yield acquired

        except OSError:
            acquired = False
            yield acquired

        finally:
            if acquired:
                fcntl.flock(lf, fcntl.LOCK_UN)