import datetime
import time
from typing import Dict, List, Optional

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
        self.states: Dict[str, Dict[str, str]] = {}

    def get_state(self, message_id):
        message_key = self._build_message_key(message_id)
        data = self.states.get(message_key)
        state = None
        if data:
            if time.monotonic() > float(data["expiration"]):
                self._delete(message_key)
            else:
                state = State.from_dict(self._decode_dict(data["state"]))
        return state

    def set_state(self, state, ttl=3600):
        message_key = self._build_message_key(state.message_id)
        ttl = ttl + time.monotonic()
        encoded_state = self._encode_dict(state.as_dict())
        if message_key not in self.states.keys():
            payload = {"state": encoded_state, "expiration": ttl}
            self.states[message_key] = payload
        else:
            state = self.states[message_key]["state"]
            for (key, value) in encoded_state.items():
                state[key] = value
            self.states[message_key]["state"] = state
            self.states[message_key]["expiration"] = ttl

    def _delete(self, message_key):
        del self.states[message_key]

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
        time_now = time.monotonic()
        states = []
        for message_key in list(self.states.keys()):
            data = self.states[message_key]
            if time_now > float(data["expiration"]):
                self._delete(message_key)
                continue
            state = State.from_dict(self._decode_dict(data["state"]))
            states.append(state)
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
        return len(self.states)
