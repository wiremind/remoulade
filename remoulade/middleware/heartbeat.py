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
import os
import shutil
import tempfile
import threading
import time
from typing import Union

from ..logging import get_logger
from .middleware import Middleware


class Heartbeat(Middleware):
    """Make Remoulade's heart beats.

    This middleware makes each worker thread writes a file
    in the specified directory containing the timestamp they started their latest task.
    You can specify a minimal interval in seconds (default to 1 minute) between each write.
    The directory is created when starting if it does not exist
    (default to your temporary directory+/remouladebeat).
    """

    # This implementation is rather simple and naive.
    # There is nothing to ensure the actual state of the filesystem corresponds
    # to what we think about it here.
    # In particular, if something is killed unexpectedly, some files may be left
    # and you could think that a thread is stuck because a file is not updated anymore.

    def __init__(self, dir: Union[str, None] = None, interval: int = 60):
        super().__init__()
        self.basedir = dir
        self.interval = interval
        self.log = get_logger(__name__, "HeartbeatMiddleware")
        # process-specific
        self.subdirs: dict[int, str] = {}
        # thread-specific
        self.beats: dict[int, float] = {}
        self.files: dict[int, str] = {}

    def after_process_boot(self, broker):
        if not self.basedir:
            self.basedir = tempfile.gettempdir() + "/remouladebeat"
        os.makedirs(self.basedir, exist_ok=True)
        self.log.debug("Created directory %s", self.basedir)

    def before_worker_boot(self, broker, worker):
        pid = os.getpid()
        self.subdirs[pid] = tempfile.mkdtemp(dir=self.basedir, prefix=f"pid-{pid}-")
        self.log.debug("Created directory %s", self.subdirs[pid])

    def after_worker_thread_boot(self, broker, thread):
        fd, self.files[thread.ident] = tempfile.mkstemp(dir=self.subdirs[os.getpid()], prefix=f"th-{thread.ident}-")
        os.close(fd)

    def heartbeat(self):
        ident = threading.get_ident()
        if self.beats.get(ident, 0) + self.interval < (beat := time.time()):
            with open(self.files[ident], "w") as f:
                f.write(f"{beat}")
            self.beats[ident] = beat

    def before_process_message(self, broker, message):
        self.heartbeat()

    def after_worker_thread_empty(self, broker, thread):
        self.heartbeat()

    def before_worker_thread_shutdown(self, broker, thread):
        try:
            os.remove(self.files[thread.ident])
        except FileNotFoundError:
            pass
        del self.beats[thread.ident]

    def before_worker_shutdown(self, broker, worker):
        shutil.rmtree(self.subdirs[os.getpid()], ignore_errors=True)
