from typing import Iterable

from ..encoder import Encoder


class State:
    """Catalog Class, that
    storages the state payload
    """
    def __init__(self, state):
        self.state = state

    def _asdict(self):
        return {"state": self.state}


Missing = type("Missing", (object,), {})()


class StateBackend:
    """ABC for  state backends.

    Parameters:
      namespace(str): The logical namespace under which the data
        should be stored.
      encoder(Encoder): The encoder to use when storing and retrieving
        result data.  Defaults to :class:`.JSONEncoder`.
    """

    def __init__(self, *, namespace: str = "remoulade-results", encoder: Encoder = None):
        from ..message import get_encoder
        self.namespace = namespace
        self.encoder = encoder or get_encoder()

    def store_result(self, message_id: str, result: State, ttl: int) -> None:  # noqa: F821
        """Store a result in the backend.

        Parameters:
          message_id(str)
          result(object): Must be serializable.
          ttl(int): The maximum amount of time the result may be
            stored in the backend for.
        """
        message_key = self.build_message_key(message_id)
        return self._store([message_key], [result._asdict()], ttl)

    def build_message_key(self, message_id: str) -> str:  # noqa: F821
        """Given a message, return its globally-unique key.

        Parameters:
          message_id(str)

        Returns:
          str
        """
        return "{}:{}".format(self.namespace, message_id)

    def cancel(self, message_ids: Iterable[str]) -> None:
        """ Mark a message as canceled """
        raise NotImplementedError("%(classname)r does not implement cancel" % {
            "classname": type(self).__name__,
        })
