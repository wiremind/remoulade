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
import time
from typing import Dict, Iterable, Optional

from ..backend import CancelBackend


class StubBackend(CancelBackend):
    """An in-memory cancel backend.  For use in unit tests.

    Parameters:
      cancellation_ttl(int): The minimal amount of seconds message cancellations
        should be kept in the backend.
    """

    def __init__(self, *, cancellation_ttl: Optional[int] = None) -> None:
        super().__init__(cancellation_ttl=cancellation_ttl)
        self.cancellations: Dict[str, float] = {}

    def is_canceled(self, message_id: str, composition_id: str) -> bool:
        return any(
            self.cancellations.get(key, -float("inf")) > time.time() - self.cancellation_ttl
            for key in [message_id, composition_id]
            if key
        )

    def cancel(self, message_ids: Iterable[str]) -> None:
        timestamp = time.time()
        for message_id in message_ids:
            self.cancellations[message_id] = timestamp
