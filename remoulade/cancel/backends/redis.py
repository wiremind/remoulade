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
from typing import Iterable

import redis

from ..backend import CancelBackend


class RedisBackend(CancelBackend):
    """A cancel backend for Redis_.

    It uses a sorted set with the message_id as member and the timestamp of the addition as a score.
    We can check if message has been canceled if it belongs to the set.
    And on each message cancel we delete all the cancellations that are older than cancellation_ttl (ZREMRANGEBYSCORE).
    This prevents unlimited set growth.

    Parameters:
      cancellation_ttl(int): The minimal amount of seconds message cancellations
        should be kept in the backend.
      key(str): A string to serve as key for the sorted set.
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      url(str): An optional connection URL.  If both a URL and
        connection parameters are provided, the URL is used.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    .. _redis: https://redis.io
    """

    def __init__(self, *, cancellation_ttl=None, key="remoulade-cancellations", client=None, url=None, **parameters):
        super().__init__(cancellation_ttl=cancellation_ttl)

        if url:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url)

        self.key = key
        self.client = client or redis.Redis(**parameters)

    def is_canceled(self, message_id: str, group_id: str) -> bool:
        with self.client.pipeline() as pipe:
            [pipe.zscore(self.key, key) for key in [message_id, group_id] if key]
            results = pipe.execute()
        return any(result is not None for result in results)

    def cancel(self, message_ids: Iterable[str]) -> None:
        timestamp = time.time()
        with self.client.pipeline() as pipe:
            pipe.zadd(self.key, {message_id: timestamp for message_id in message_ids})
            pipe.zremrangebyscore(self.key, "-inf", timestamp - self.cancellation_ttl)
            pipe.execute()
