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
        data = self.client.get(key)
        if data:
            data = self._parse_state(data)
        return data

    def set_state(self, state, ttl):
        message_key = self._build_message_key(state.message_id)
        self.client.set(message_key, self.encoder.encode(state.asdict()), ex=ttl)

    def get_states(self):
        size = 1000
        for keys in chunk(self.client.scan_iter(match="{}*".format(StateBackend.namespace), count=size), size):
            for data in self.client.mget(keys):
                yield self._parse_state(data)

    def _parse_state(self, data):
        json_data = self.encoder.decode(data)
        return State.from_dict(json_data)
