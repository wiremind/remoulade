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
from typing import Iterable

DEFAULT_CANCELLATION_TTL = 3600


class CancelBackend:
    """ABC for cancel backends.

        Parameters:
          cancellation_ttl(int): The minimal amount of seconds message cancellations
            should be kept in the backend (default 1h).
        """

    def __init__(self, *, cancellation_ttl=None):
        self.cancellation_ttl = cancellation_ttl or DEFAULT_CANCELLATION_TTL

    def is_canceled(self, message_id: str, group_id: str) -> bool:
        """ Return true if the message has been canceled """
        raise NotImplementedError(
            "%(classname)r does not implement is_canceled" % {"classname": type(self).__name__,}
        )

    def cancel(self, message_ids: Iterable[str]) -> None:
        """ Mark a message as canceled """
        raise NotImplementedError(
            "%(classname)r does not implement cancel" % {"classname": type(self).__name__,}
        )
