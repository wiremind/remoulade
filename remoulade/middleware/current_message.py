# SPDX-License-Identifier: LPGL-3.0
# Copyright (C) 2019 CLEARTYPE SRL <bogdan@cleartype.io>
# Based in current_message.py from the dramatiq project

from threading import local
from typing import TYPE_CHECKING

from .middleware import Middleware

if TYPE_CHECKING:
    from .. import Broker, Message


class CurrentMessage(Middleware):

    local_data = local()

    @classmethod
    def get_current_message(cls) -> "Message":
        """Get the message that triggered the current actor.  Messages
        are thread local so this returns ``None`` when called outside
        of actor code.
        """
        return getattr(cls.local_data, "current_message", None)

    def before_process_message(self, broker: "Broker", message: "Message") -> None:
        self.local_data.current_message = message

    def after_process_message(self, broker: "Broker", message: "Message", *, result=None, exception=None) -> None:
        self.local_data.current_message = None
