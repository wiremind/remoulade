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
import signal
import threading
import time
import warnings

from ..errors import NoCancelBackend
from ..logging import get_logger
from .middleware import Middleware
from .threading import Interrupt, current_platform, raise_thread_exception, supported_platforms


class TimeLimitExceeded(Interrupt):
    """Exception used to interrupt worker threads when actors exceed
    their time limits.
    """


class TimeLimit(Middleware):
    """Middleware that cancels actors that run for too long.
    Currently, this is only available on CPython.

    Note:
      This works by setting an async exception in the worker thread
      that runs the actor.  This means that the exception will only get
      called the next time that thread acquires the GIL.  Concretely,
      this means that this middleware can't cancel system calls.

    Parameters:
      time_limit(int): The maximum number of milliseconds actors may run for.
      interval(int): The interval (in milliseconds) with which to check for actors that have exceeded the limit.
      exit_delay(int): The delay (in milliseconds) after with we stop (SystemExit) to the worker if the exception failed
       to stop the message (ie. system calls). None to disable, disabled by default.
    """

    def __init__(self, *, time_limit=1800000, interval=1000, exit_delay=None):
        self.logger = get_logger(__name__, type(self))
        self.time_limit = time_limit
        self.interval = interval
        self.deadlines = {}
        self.exit_delay = exit_delay
        self.lock = threading.Lock()

    def _handle(self, *_):
        # Do no try to acquire the lock for more than the interval as handler will be called again
        acquired = self.lock.acquire(timeout=self.interval / 1000)
        if not acquired:
            return
        try:
            current_time = time.monotonic()
            for thread_id, row in self.deadlines.items():
                if row is None:
                    continue

                deadline, exception_raised, message = row
                if exception_raised and self.exit_delay and current_time >= deadline + self.exit_delay / 1000:
                    self.deadlines[thread_id] = None
                    try:
                        message.cancel()
                        self.logger.critical(
                            "Could not stop message %s execution with TimeLimitExceeded, "
                            "cancel message and exit the worker",
                            message.message_id,
                        )
                        raise SystemExit(1)
                    except NoCancelBackend:
                        # Sending SIGKILL would requeue the message via RabbitMQ
                        self.logger.error(
                            "Could not stop message %s execution with TimeLimitExceeded, "
                            "but do not stop the worker as the message would be requeue",
                            message.message_id,
                        )
                elif not exception_raised and current_time >= deadline:
                    self.logger.warning("Time limit exceeded. Raising exception in worker thread %r.", thread_id)
                    self.deadlines[thread_id] = deadline, True, message
                    raise_thread_exception(thread_id, TimeLimitExceeded)
        finally:
            self.lock.release()

    @property
    def actor_options(self):
        return {"time_limit"}

    def after_process_boot(self, _):
        # Used because only the main thread can set a signal handler
        self.logger.debug("Setting up timers...")
        signal.setitimer(signal.ITIMER_REAL, self.interval / 1000, self.interval / 1000)
        signal.signal(signal.SIGALRM, self._handle)

        if current_platform not in supported_platforms:  # pragma: no cover
            msg = "TimeLimit cannot kill threads on your current platform (%r)."
            warnings.warn(msg % current_platform, category=RuntimeWarning, stacklevel=2)

    def before_process_stop(self, _):
        self.logger.debug("Clear timers...")
        signal.setitimer(signal.ITIMER_REAL, 0)

    def before_process_message(self, broker, message):
        actor = broker.get_actor(message.actor_name)
        limit = message.options.get("time_limit") or actor.options.get("time_limit", self.time_limit)
        deadline = time.monotonic() + limit / 1000
        with self.lock:
            self.deadlines[threading.get_ident()] = deadline, False, message

    def after_process_message(self, broker, message, *, result=None, exception=None):
        with self.lock:
            self.deadlines[threading.get_ident()] = None

    after_skip_message = after_process_message
    after_message_canceled = after_process_message
