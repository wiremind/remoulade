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

from typing import TYPE_CHECKING, Any, override

from ..broker import Broker, Consumer, MessageProxy
from ..common import current_millis
from ..message import Message
from ..middleware import SkipMessage
from ..results import Results
from ..results.backends import LocalBackend

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..middleware import Middleware


class LocalBroker(Broker):
    """Broker that calculate the message result immediately

    It can only be used with LocalBackend as a result backend
    """

    def __init__(self, middleware: "Iterable[Middleware] | None" = None) -> None:
        super().__init__(middleware)

    @property
    @override
    def local(self) -> bool:
        return True

    @override
    def add_middleware(
        self, middleware: "Middleware", *, before: "Middleware | None" = None, after: "Middleware | None" = None
    ) -> None:
        if isinstance(middleware, Results) and not isinstance(middleware.backend, LocalBackend):
            raise RuntimeError("LocalBroker can only be used with LocalBackend.")
        super().add_middleware(middleware)

    @override
    def emit_before(self, signal: str, *args: Any, **kwargs: Any) -> None:
        # A local broker should not catch any exception because we are not in a worker but in the main thread
        for middleware in self.middleware:
            getattr(middleware, "before_" + signal)(self, *args, **kwargs)

    @override
    def emit_after(self, signal: str, *args: Any, **kwargs: Any) -> None:
        for middleware in reversed(self.middleware):
            getattr(middleware, "after_" + signal)(self, *args, **kwargs)

    @override
    def consume(self, queue_name: str, prefetch: int = 1, timeout: int = 100) -> "Consumer":
        raise ValueError("LocalBroker is not destined to use with a Worker")

    @override
    def declare_queue(self, queue_name: str) -> None:
        self.emit_before("declare_queue", queue_name)
        self.queues[queue_name] = None
        self.emit_after("declare_queue", queue_name)

    @override
    def _apply_delay(self, message: "Message", delay: int | None = None) -> "Message":
        if delay is not None:
            message_eta = current_millis() + delay
            message = message.copy(options={"eta": message_eta})
        return message

    @override
    def enqueue(self, message: "Message", *, delay: int | None = None) -> "Message":  # pragma: no cover
        """Enqueue a message on this broker.

        Parameters:
          message(Message): The message to enqueue.
          delay(int): The number of milliseconds to delay the message for.

        Returns:
          Message: Either the original message or a copy of it.
        """

        message = self._apply_delay(message, delay)
        self.emit_before("enqueue", message, delay)
        self.emit_after("enqueue", message, delay)

        message = self._enqueue(message, delay=delay)
        return message

    @override
    def _enqueue(self, message: "Message", *, delay: int | None = None):
        """Enqueue and compute a message.

        Parameters:
          message(Message): The message to enqueue
          delay(int): ignored
        """
        actor = self.get_actor(message.actor_name)
        message_proxy = MessageProxy(message)
        try:
            self.emit_before("process_message", message_proxy)

            res = None
            if not message_proxy.failed:
                res = actor(*message_proxy.args, **message_proxy.kwargs)

            self.emit_after("process_message", message_proxy, result=res)
        except SkipMessage:
            self.emit_after("skip_message", message)

        except BaseException as e:
            self.emit_after("process_message", message_proxy, exception=e)
            raise
        finally:
            if message_proxy.failed:
                self.emit_before("nack", message)
                self.emit_after("nack", message)
            else:
                self.emit_before("ack", message)
                self.emit_after("ack", message)

        return message_proxy

    @override
    def _enqueue_many(self, messages: list["Message[Any]"], *, delay: int | None = None) -> list["Message[Any]"]:
        return [self._enqueue(message, delay=delay) for message in messages]

    @override
    def flush(self, _: str) -> None:
        pass

    @override
    def flush_all(self) -> None:
        pass

    @override
    def join(self, queue_name: str, *, timeout: int | None = None) -> None:
        return
