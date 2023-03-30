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
import os
import time
from typing import Iterable, List

import redis

from ...helpers.backoff import BackoffStrategy, compute_backoff
from ..backend import BackendResult, ForgottenResult, Missing, ResultBackend, ResultMissing, ResultTimeout


class RedisBackend(ResultBackend):
    """A result backend for Redis_.  This is the recommended result
    backend as waiting for a result is resource efficient.

    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      url(str): An optional connection URL.  If both a URL and
        connection paramters are provided, the URL is used.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    .. _redis: https://redis.io
    """

    def __init__(
        self,
        *,
        namespace="remoulade-results",
        encoder=None,
        client=None,
        url=None,
        default_timeout=None,
        max_retries=3,
        min_backoff=500,
        max_backoff=5000,
        backoff_strategy: BackoffStrategy = "spread_exponential",
        **parameters,
    ):
        super().__init__(namespace=namespace, encoder=encoder, default_timeout=default_timeout)

        url = url or os.getenv("REMOULADE_REDIS_URL")
        if url:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url)

        self.client = client or redis.Redis(**parameters)
        self.max_retries = max_retries
        self.min_backoff = min_backoff
        self.max_backoff = max_backoff
        self.backoff_strategy = backoff_strategy

    def get_results(
        self,
        message_ids: List[str],
        *,
        block: bool = False,
        timeout: int = None,
        forget: bool = False,
        raise_on_error: bool = True,
    ) -> Iterable[BackendResult]:
        if block:
            yield from super().get_results(message_ids, block=block, timeout=timeout, forget=forget, raise_on_error=raise_on_error)
        else:
            with self.client.pipeline() as pipe:
                for message_id in message_ids:
                    message_key = self.build_message_key(message_id)
                    if forget:
                        pipe.rpushx(message_key, self.encoder.encode(ForgottenResult.asdict()))
                        pipe.lpop(message_key)
                    else:
                        pipe.rpoplpush(message_key, message_key)
                data = pipe.execute()
            for i, row in enumerate(data):
                if i % 2 == 0 and forget:
                    continue  # skip one row in two if forget as there is two commands
                if row is None:
                    raise ResultMissing(message_id)
                yield self.process_result(BackendResult(**self.encoder.decode(row)), raise_on_error)

    def get_result(self, message_id: str, *, block=False, timeout=None, forget=False, raise_on_error=True):
        """Get a result from the backend.

        Warning:
          Sub-second timeouts are not respected by this backend.

        Parameters:
          message_id(str)
          block(bool): Whether or not to block until a result is set.
          timeout(int): The maximum amount of time, in ms, to wait for
            a result when block is True.  Defaults to 10 seconds.
          forget(bool): Whether or not the result need to be kept.
          raise_on_error(bool): raise an error if the result stored in
            an error

        Raises:
          ResultMissing: When block is False and the result isn't set.
          ResultTimeout: When waiting for a result times out.

        Returns:
          object: The result.
        """
        if timeout is None:
            timeout = self.default_timeout

        message_key = self.build_message_key(message_id)
        timeout = int(timeout / 1000)
        deadline = time.monotonic() + timeout
        retry_count, data = 0, None
        while data is None:
            try:
                if block and timeout > 0:
                    data = self.client.brpoplpush(message_key, message_key, timeout=timeout)
                    if forget and data is not None:
                        with self.client.pipeline() as pipe:
                            pipe.lpushx(message_key, self.encoder.encode(ForgottenResult.asdict()))
                            pipe.ltrim(message_key, 0, 0)
                            pipe.execute()

                else:
                    if forget:
                        with self.client.pipeline() as pipe:
                            pipe.rpushx(message_key, self.encoder.encode(ForgottenResult.asdict()))
                            pipe.lpop(message_key)
                            data = pipe.execute()[1]
                    else:
                        data = self.client.rpoplpush(message_key, message_key)

                if data is None:
                    if block:
                        raise ResultTimeout(message_id)
                    else:
                        raise ResultMissing(message_id)

            except (redis.ConnectionError, redis.TimeoutError):
                # if data is not None, it means the second step of block+forget has failed, we can live without a forget
                if data is not None:
                    break
                if retry_count >= self.max_retries:
                    raise
                _, backoff = compute_backoff(
                    retry_count,
                    min_backoff=self.min_backoff,
                    max_backoff=self.max_backoff,
                    max_retries=self.max_retries,
                )
                retry_count += 1
                time.sleep(backoff / 1000)
                timeout = deadline - time.monotonic()
                if block and timeout <= 0:  # do not retry is timeout is expired
                    raise

        result = BackendResult(**self.encoder.decode(data))
        return self.process_result(result, raise_on_error)

    def _store(self, message_keys, results, ttl):
        with self.client.pipeline() as pipe:
            for (message_key, result) in zip(message_keys, results):
                pipe.delete(message_key)
                pipe.lpush(message_key, self.encoder.encode(result))
                pipe.pexpire(message_key, ttl)
            pipe.execute()

    def _get(self, key, forget=False):
        data = self.client.rpoplpush(key, key)
        if data:
            return self.encoder.decode(data)
        return Missing

    def _delete(self, key):
        self.client.delete(key)

    def increment_group_completion(self, group_id: str, message_id: str, ttl: int) -> int:
        group_completion_key = self.build_group_completion_key(group_id)
        with self.client.pipeline() as pipe:
            pipe.sadd(group_completion_key, message_id)
            pipe.pexpire(group_completion_key, ttl)
            pipe.scard(group_completion_key)
            group_completion = pipe.execute()[2]

        return group_completion

    def get_status(self, message_ids: List[str]) -> int:  # type: ignore
        if not message_ids:
            return 0
        return self.client.exists(*[self.build_message_key(message_id) for message_id in message_ids])
