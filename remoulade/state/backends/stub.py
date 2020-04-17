import time
from typing import Dict

from ..backend import State, StateBackend


class StubBackend(StateBackend):
    """An in-memory state backend.  For use in unit tests.

    Parameters:
      namespace(str): A string with which to prefix result keys.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.states = {}  # type: Dict[str, bytes]

    def get_state(self, message_id):
        message_key = self._build_message_key(message_id)
        data = self.states.get(message_key)
        state = None
        if data:
            decoded_data = self.encoder.decode(data)
            if time.monotonic() > decoded_data["expiration"]:
                self._delete(message_key)
            else:
                state = State.from_dict(decoded_data["state"])
        return state

    def set_state(self, state, ttl):
        message_key = self._build_message_key(state.message_id)
        ttl = ttl + time.monotonic()
        payload = {"state": state.asdict(), "expiration": ttl}
        self.states[message_key] = self.encoder.encode(payload)

    def _delete(self, message_key):
        del self.states[message_key]

    def get_states(self):
        time_now = time.monotonic()
        for (message_key, data) in self.states.items():
            decoded_data = self.encoder.decode(data)
            if time_now > decoded_data["expiration"]:
                self._delete(message_key)
                continue
            state = State.from_dict(decoded_data["state"])
            yield state
