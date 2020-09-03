import datetime
import time

import pytz
import redis

from remoulade.common import chunk

from ..backend import State, StateBackend


class RedisBackend(StateBackend):
    """A state backend for Redis_.

    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
      client(Redis): An optional client.  If this is passed,
        then all other parameters are ignored.
      url(str): An optional connection URL.  If both a URL and
        connection parameters are provided, the URL is used.
      **parameters(dict): Connection parameters are passed directly
        to :class:`redis.Redis`.

    .. _redis: https://redis.io
    """

    def __init__(self, *, namespace="remoulade-state", encoder=None, client=None, url=None, **parameters):
        super().__init__(namespace=namespace, encoder=encoder)
        if url:
            parameters["connection_pool"] = redis.ConnectionPool.from_url(url)
        self.client = client or redis.Redis(**parameters)
        self.index_key = namespace + "-index"
        self.index_keys = {
            'group': namespace + "-index-group",
            'group-id'
        }

    def get_state(self, message_id):
        key = self._build_message_key(message_id)
        data = self.client.hgetall(key)
        if not data:
            return None
        return self._parse_state(data)

    def get_index_name(self, key):
        return self.namespace + '-' + key

    def set_state(self, state, ttl=3600):
        message_key = self._build_message_key(state.message_id)
        timestamp = time.time()
        # What to do with groups
        with self.client.pipeline() as pipe:
            encoded_state = self._encode_dict(state.as_dict())
            pipe.hset(message_key, mapping=encoded_state)
            pipe.expire(message_key, ttl)

            self._set_sorted_index(pipe, self.get_index_name("message-id"), state.message_id, ttl)
            if state.group_id:
                self._set_sorted_index(pipe, self.get_index_name("group"), state.group_id, ttl)
                self._set_lexicographical_index(pipe, self.get_index_name("group-id"), state, "group_id", ttl)

            self._set_lexicographical_index(pipe, self.get_index_name("name"), state, "name", ttl)
            self._set_lexicographical_index(pipe, self.get_index_name("actor-name"), state, "actor_name", ttl)

            pipe.execute(raise_on_error=False)

    @staticmethod
    def _set_sorted_index(pipe, index_name, value, ttl):
        timestamp = time.time()
        pipe.zadd(index_name, {value: timestamp})
        pipe.zremrangebyscore(index_name, "-inf", timestamp - ttl)
        pipe.expire(index_name, ttl)

    def _set_lexicographical_index(self, pipe, index_name, state, key, ttl):
        timestamp = time.time()
        index_name = index_name + "-" + self.today.isoformat()
        value = getattr(state, key) + ":" + state.message_id
        pipe.zadd(index_name, {value: 0})
        pipe.zremrangebyscore(index_name, "-inf", timestamp - ttl)
        pipe.expire(index_name, ttl)

    @property
    def today(self):
        return pytz.UTC.localize(datetime.utcnow()).date()

    @property
    def yesterday(self):
        return pytz.UTC.localize(datetime.utcnow()).date() - datetime.timedelta(days=1)

    def get_states(self, after=None):
        # return count with zcard
        # make index with date inside and an expire
        size = 1000  # maybe better size ?
        start = 0 if after is None else self.client.zrank(self.index_key, after)
        message_ids = self.client.zrange(self.index_key, start, start + size)
        # use a while or something like this
        for message_ids in chunk(message_ids, size):
            with self.client.pipeline() as pipe:
                for message_id in message_ids:
                    pipe.hgetall(self._build_message_key(message_id))
                data = pipe.execute()
                for state in data:
                    if state:
                        yield self._parse_state(state)

    def _parse_state(self, data):
        decoded_state = self._decode_dict(data)
        return State.from_dict(decoded_state)
