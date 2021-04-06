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

import resource

from ..logging import get_logger
from .middleware import Middleware


class MaxMemory(Middleware):
    """Middleware that stop a worker if its amount of resident memory exceed max_memory (in kilobytes)
    If a task causes a worker to exceed this limit, the task will be completed, and the worker will be
    stopped afterwards.

    Parameters:
      max_memory(int): The maximum amount of resident memory (in kilobytes)
    """

    def __init__(self, *, max_memory: int):
        self.logger = get_logger(__name__, type(self))
        self.max_memory = max_memory

    def after_worker_thread_process_message(self, broker, thread):
        used_memory = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if used_memory <= 0:
            self.logger.error("Worker unable to determine memory usage")
        if used_memory > self.max_memory:
            self.logger.warning(
                f"Stopping worker thread as used_memory ({used_memory:.2E}Kb) > max_memory ({self.max_memory:.2E}Kb)"
            )
            # stopping the worker thread will in time stop the worker process which check if all workers are still
            # running (via Worker.worker_stopped)
            thread.stop()
