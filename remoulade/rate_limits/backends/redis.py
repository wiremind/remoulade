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

from limits.storage import RedisSentinelStorage, RedisStorage, Storage

from ..backend import RateLimitBackend
from .utils import build_limiter


class RedisBackend(RateLimitBackend):
    """Redis backend using ``limits`` RedisStorage.

    Parameters:
      url(str): An optional connection URL.  If both a URL and
        connection parameters are provided, the URL is used.
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      key_prefix(str): A prefix to prepend to all keys used for rate limiting.
      strategy(str): The rate limiting strategy to use.  One of
        ``fixed_window``, ``moving_window``, or ``sliding_window``.
        See :ref:`limits strategies <limits-strategies>` for details.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    .. _redis: https://redis.io
    .. _limits: https://limits.readthedocs.io/en/stable/
    """

    def __init__(
        self,
        *,
        url: str | None = None,
        client=None,
        key_prefix: str = "remoulade-rate-limit:",
        strategy: str = "sliding_window",
        **parameters,
    ):
        super().__init__()

        if client is None and url is None:
            raise ValueError("Either url or client must be provided")

        storage: Storage
        if client:
            storage = RedisStorage("redis://localhost", connection_pool=client.connection_pool, key_prefix=key_prefix)
        elif "sentinel" in url:
            storage = RedisSentinelStorage(url, key_prefix=key_prefix, **parameters)
        else:
            storage = RedisStorage(url, key_prefix=key_prefix, **parameters)

        self.limiter = build_limiter(storage, strategy=strategy)

    def hit(self, limit, key: str) -> bool:
        return bool(self.limiter.hit(limit, key))
