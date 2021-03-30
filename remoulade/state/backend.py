import sys
from collections import namedtuple
from enum import Enum
from typing import Dict, List

from dateutil.parser import parse

from ..encoder import Encoder
from .errors import InvalidStateError


class StateNamesEnum(Enum):
    """Contains the possible states that can have a message"""

    Started = "Started"  # a `Message` that has not been processed
    Pending = "Pending"  # a `Message` that has been enqueued
    Skipped = "Skipped"  # a `Message` that has been skipped
    Canceled = "Canceled"  # a `Message` that has been canceled
    Failure = "Failure"  # a `Message` that has been processed and raised an Exception
    Success = "Success"  # a `Message` that has been processed and does not raise an Exception


#: A type alias representing states in the database
class State(
    namedtuple(
        "State",
        (
            "message_id",
            "name",
            "actor_name",
            "args",
            "kwargs",
            "options",
            "priority",
            "progress",
            "enqueued_datetime",
            "started_datetime",
            "end_datetime",
            "group_id",
        ),
    )
):
    """Catalog Class, it storages the state
    Parameters:
        name: Name of the state
        args: List of arguments in the state(name)
    """

    def __new__(
        cls,
        message_id,
        name=None,
        *,
        actor_name=None,
        args=None,
        kwargs=None,
        options=None,
        priority=None,
        progress=None,
        enqueued_datetime=None,
        started_datetime=None,
        end_datetime=None,
        group_id=None,
    ):

        if name and name not in list(StateNamesEnum):
            raise InvalidStateError("The {} State is not defined".format(name))
        return super().__new__(
            cls,
            message_id,
            name,
            actor_name,
            args,
            kwargs,
            options,
            priority,
            progress,
            enqueued_datetime,
            started_datetime,
            end_datetime,
            group_id,
        )

    def as_dict(self, exclude_keys=(), encode_args=False):
        """ Transform a State into a dict, can exclude some keys """
        as_dict = {
            key: value for (key, value) in self._asdict().items() if value is not None and key not in exclude_keys
        }
        datetime_keys = ["enqueued_datetime", "started_datetime", "end_datetime"]
        for key in datetime_keys:
            if key in as_dict:
                as_dict[key] = as_dict[key].isoformat()
        if self.name:
            as_dict["name"] = self.name.value
        if encode_args:
            from ..message import get_encoder

            for key in (item for item in ["args", "kwargs", "options"] if item in as_dict):
                try:
                    as_dict[key] = get_encoder().encode(as_dict[key]).decode("utf-8")
                except (UnicodeDecodeError, TypeError):
                    as_dict[key] = "encoded_data"
        return as_dict

    @classmethod
    def from_dict(cls, input_dict: Dict) -> "State":
        if "name" in input_dict:
            input_dict["name"] = StateNamesEnum(input_dict["name"])
        datetime_keys = ["enqueued_datetime", "started_datetime", "end_datetime"]
        for key in datetime_keys:
            if key in input_dict:
                input_dict[key] = parse(input_dict[key])
        return cls(**input_dict)


class StateBackend:
    """ABC for  state backends.

    Parameters:
      namespace(str): The logical namespace under which the data
        should be stored.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
      max_size(int): Maximum size of arguments allow to storage
        in the database, default 2MB
    """

    namespace = "remoulade-state*"

    def __init__(self, *, namespace: str = "remoulade-state", encoder: Encoder = None, max_size=2e6):
        from ..message import get_encoder

        self.namespace = namespace
        self.encoder = encoder or get_encoder()
        self.max_size = max_size

    def _build_message_key(self, message_id: str) -> str:  # noqa: F821
        """Given a message id, return its globally-unique key.

        Parameters:
          message_id(str)

        Returns:
          str
        """
        return f"{self.namespace}:{message_id}"

    def get_state(self, message_id: str) -> State:
        """Get the state with a message_id from the backend.

        Parameters:
            message_id(str)

        """
        raise NotImplementedError(f"{type(self).__name__} does not implement get_state")

    def set_state(self, state: State, ttl: int = 3600) -> None:
        """Save a message in the backend if it does not exist,
            otherwise update it.

        Parameters:
            state(State)
            ttl(seconds): The time to keep that state in the backend
             default is one hour(3600 seconds)
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement set_state")

    def get_states(self) -> List[State]:
        """Return all the states in the backend"""
        raise NotImplementedError(f"{type(self).__name__} does not implement get_all_messages")

    def _encode_dict(self, data):
        """Return the (keys, values) of a dictionary encoded"""
        encoded_data = {}
        for (key, value) in data.items():
            encoded_value = self.encoder.encode(value)
            if sys.getsizeof(encoded_value) <= self.max_size:
                encoded_data[self.encoder.encode(key)] = self.encoder.encode(value)
        return encoded_data

    def _decode_dict(self, data):
        """Return the (keys, values) of a dictionary decoded"""
        decoded_data = {}
        for (key, value) in data.items():
            decoded_data[self.encoder.decode(key)] = self.encoder.decode(value)
        return decoded_data
