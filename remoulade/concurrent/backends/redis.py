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

import redis

from ...helpers.redis_client import redis_client
from ..backend import ConcurrencyBackend
from ..lease import Lease

# The acquire Lua script will first purge expired tokens, then check the current
# count of active tokens. If the limit has not been reached, it adds the new token
# with its expiration timestamp and resets the key's TTL.
ACQUIRE_LUA = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local token = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
local current = redis.call('ZCARD', key)
if current >= limit then
  return 0
end

local expires_at = now + ttl
redis.call('ZADD', key, expires_at, token)
redis.call('PEXPIRE', key, ttl)
return expires_at
"""

# The release Lua script simply removes the token from the sorted set.
RELEASE_LUA = """
local key = KEYS[1]
local token = ARGV[1]
return redis.call('ZREM', key, token)
"""


class RedisBackend(ConcurrencyBackend):
    """Redis-backed distributed semaphore for concurrency control.

    Parameters:
      url(str): An optional connection URL.  If both a URL and
        connection parameters are provided, the URL is used.
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      key_prefix(str): A prefix to prepend to all keys used for concurrency.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    """

    def __init__(
        self,
        *,
        url: str | None = None,
        client: redis.Redis | None = None,
        key_prefix: str = "remoulade-concurrency:",
        socket_timeout: float = 5.0,
        **parameters,
    ) -> None:
        super().__init__()
        self.client = client or redis_client(url=url, socket_timeout=socket_timeout, **parameters)
        if self.client is None:
            raise ValueError("A redis client or url must be provided for RedisBackend")
        self.key_prefix = key_prefix

        self._acquire_script = self.client.register_script(ACQUIRE_LUA)
        self._release_script = self.client.register_script(RELEASE_LUA)

    def acquire(self, *, key: str, limit: int, ttl_ms: int, token: str) -> Lease | None:
        now = int(time.time() * 1000)
        try:
            expires_at = self._acquire_script(keys=[self._build_key(key)], args=[limit, now, ttl_ms, token])
        except redis.exceptions.RedisError:
            return None

        if not expires_at:
            return None

        return Lease(key=key, token=token, ttl_ms=ttl_ms, expires_at_ms=int(expires_at))

    def release(self, lease: Lease) -> None:
        try:
            self._release_script(keys=[self._build_key(lease.key)], args=[lease.token])
        except redis.exceptions.RedisError:
            return

    def _build_key(self, key: str) -> str:
        return f"{self.key_prefix}{key}"
