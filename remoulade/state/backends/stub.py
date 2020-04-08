import time

from ..backend import State, StateBackend


class StubBackend(StateBackend):
    """An in-memory state backend.  For use in unit tests.

    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
    """

    states = {}

    def get_state(self, message_id):
        message_key = self._build_message_key(message_id)
        data = self.states.get(message_key)
        if data:
            data = self.encoder.decode(data)
            if time.monotonic() > data["expiration"]:
                self._delete(message_key)
                data = None
            else:
                data = State.from_dict(**data["state"])
        return data

    def set_state(self, message_id, state, ttl):
        message_key = self._build_message_key(message_id)
        ttl = ttl + time.monotonic()
        payload = {"state": state.asdict(), "expiration": ttl}
        self.states[message_key] = self.encoder.encode(payload)

    def _delete(self, message_key):
        del self.states[message_key]
