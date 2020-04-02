import time
from typing import Iterable

import redis

from ..backend import Missing, StateBackend


class RedisBackend(StateBackend):

    def __init__(self, *, namespace="remoulade-results", encoder=None, client=None, url=None, **parameters):
        super().__init__(namespace=namespace, encoder=encoder)
        if url:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url)
        self.client = client or redis.Redis(**parameters)
        self.cancellation_ttl = 1000

    def get_value(self, message_id):
        """ Given a message_id returns the
        value in the database"""
        key = self.build_message_key(message_id)
        print(key)
        data = self.client.rpoplpush(key, key)
        if data:
            return self.encoder.decode(data)
        return Missing

    def _delete(self, key):
        self.client.delete(key)

    def _store(self, message_keys, results, ttl):
        with self.client.pipeline() as pipe:
            for (message_key, result) in zip(message_keys, results):
                pipe.delete(message_key)
                pipe.lpush(message_key, self.encoder.encode(result))
                pipe.pexpire(message_key, ttl)
            pipe.execute()

    def cancel(self, message_ids: Iterable[str]) -> None:
        # TODO: delete or find a way to avoid redundance in cancel
        timestamp = time.time()
        with self.client.pipeline() as pipe:
            pipe.zadd(self.namespace, {message_id: timestamp for message_id in message_ids})
            pipe.zremrangebyscore(self.namespace, '-inf', timestamp - self.cancellation_ttl)
            pipe.execute()

    def is_canceled(self, message_id: str, group_id: str) -> bool:
        with self.client.pipeline() as pipe:
            [pipe.zscore(self.namespace, key) for key in [message_id, group_id] if key]
            results = pipe.execute()
        return any(result is not None for result in results)
