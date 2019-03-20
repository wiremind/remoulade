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
from ..logging import get_logger
from ..middleware import Middleware
from .errors import MessageCanceled


class Cancel(Middleware):
    """Middleware that check if a message has been canceled before processing it
    If the message has been canceled raise a MessageCanceled to prevent message processing.

    Example:

      >>> from remoulade.cancel import Cancel
      >>> from remoulade.cancel.backends import RedisBackend
      >>> backend = RedisBackend()
      >>> broker.add_middleware(Cancel(backend=backend))

      >>> @remoulade.actor(store_results=True)
      ... def add(x, y):
      ...   return x + y

      >>> broker.declare_actor(add)
      >>> message = add.send(1, 2)
      >>> message.cancel()
      3

    Parameters:
      backend(CancelBackend): The cancel backend to use to check
        cancellations.
        Defaults to False and can be set on a per-actor basis.
    """

    def __init__(self, *, backend=None):
        self.logger = get_logger(__name__, type(self))
        self.backend = backend

    def before_process_message(self, broker, message):
        group_id = message.options.get("group_info", {}).get("group_id")

        if self.backend.is_canceled(message.message_id, group_id):
            raise MessageCanceled("Message %s has been canceled" % message.message_id)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        """ Cancel all the messages in the group if one of the message of the group fail"""
        from ..composition import GroupInfo

        if exception is None:
            return

        group_info = message.options.get("group_info")
        if not group_info:
            return

        group_info = GroupInfo(**group_info)
        if group_info.cancel_on_error:
            self.backend.cancel([group_info.group_id])
