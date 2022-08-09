import datetime
from typing import List, Optional

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

    def get_state(self, message_id):
        key = self._build_message_key(message_id)
        data = self.client.hgetall(key)
        if not data:
            return None
        return self._parse_state(data)

    def set_state(self, state, ttl=3600):
        message_key = self._build_message_key(state.message_id)
        with self.client.pipeline() as pipe:
            encoded_state = self._encode_dict(state.as_dict())
            pipe.hset(message_key, mapping=encoded_state)
            pipe.expire(message_key, ttl)
            pipe.execute()

    def get_states(
        self,
        *,
        size: Optional[int] = None,
        offset: int = 0,
        selected_actors: Optional[List[str]] = None,
        selected_statuses: Optional[List[str]] = None,
        selected_message_ids: Optional[List[str]] = None,
        selected_composition_ids: Optional[List[str]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        sort_column: Optional[str] = None,
        sort_direction: Optional[str] = None,
    ):
        states = []
        for keys in chunk(self.client.scan_iter(match=f"{StateBackend.namespace}*", count=size), size):
            with self.client.pipeline() as pipe:
                for key in keys:
                    pipe.hgetall(key)
                data = pipe.execute()
                for state_dict in data:
                    if state_dict:
                        states.append(self._parse_state(state_dict))

        if size is None:
            return states[offset:]

        return states[offset : size + offset]

    def get_states_count(
        self,
        *,
        selected_actors: Optional[List[str]] = None,
        selected_statuses: Optional[List[str]] = None,
        selected_messages_ids: Optional[List[str]] = None,
        selected_composition_ids: Optional[List[str]] = None,
        start_datetime: Optional[datetime.datetime] = None,
        end_datetime: Optional[datetime.datetime] = None,
        **kwargs,
    ) -> int:
        return len(list(self.client.scan_iter(match=f"{StateBackend.namespace}*")))

    def _parse_state(self, data):
        decoded_state = self._decode_dict(data)
        return State.from_dict(decoded_state)
