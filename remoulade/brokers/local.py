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
from typing import Optional

from ..broker import Broker, MessageProxy
from ..common import current_millis
from ..results import Results
from ..results.backends import LocalBackend


class LocalBroker(Broker):
    """Broker that calculate the message result immediately

    It can only be used with LocalBackend as a result backend
    """

    def __init__(self, middleware=None):
        super().__init__(middleware)

    @property
    def local(self):
        return True

    def add_middleware(self, middleware, *, before=None, after=None):
        if isinstance(middleware, Results) and not isinstance(middleware.backend, LocalBackend):
            raise RuntimeError("LocalBroker can only be used with LocalBackend.")
        super().add_middleware(middleware)

    def emit_before(self, signal, *args, **kwargs):
        # A local broker should not catch any exception because we are not in a worker but in the main thread
        for middleware in self.middleware:
            getattr(middleware, "before_" + signal)(self, *args, **kwargs)

    def emit_after(self, signal, *args, **kwargs):
        for middleware in reversed(self.middleware):
            getattr(middleware, "after_" + signal)(self, *args, **kwargs)

    def consume(self, queue_name, prefetch=1, timeout=100):
        raise ValueError("LocalBroker is not destined to use with a Worker")

    def declare_queue(self, queue_name):
        self.queues[queue_name] = None

    def _apply_delay(self, message, delay: Optional[int] = None):
        if delay is not None:
            message_eta = current_millis() + delay
            message = message.copy(options={"eta": message_eta})
        return message

    def _enqueue(self, message, *, delay=None):
        """Enqueue and compute a message.

        Parameters:
          message(Message): The message to enqueue
          delay(int): ignored
        """
        actor = self.get_actor(message.actor_name)
        message_proxy = MessageProxy(message)
        self.emit_before("process_message", message_proxy)
        res = actor(*message_proxy.args, **message_proxy.kwargs)
        self.emit_after("process_message", message_proxy, result=res)
        return message_proxy

    def flush(self, _):
        pass

    def flush_all(self):
        pass

    def join(self, *_):
        return
