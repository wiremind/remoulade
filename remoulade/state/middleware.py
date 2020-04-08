import sys

from ..middleware import Middleware
from .backend import State, StateNamesEnum


class MessageState(Middleware):

    """Middleware use to storage and update the state
    of the messages.
    Parameters
    state_ttl(int):Time(seconds) that the state will be storage
                    in the database
    max_size(int): Maximum size of arguments allow to storage
                    in the database, default 2MB
    """

    def __init__(self, backend, state_ttl=3600, max_size=2e6):
        self.backend = backend
        self.state_ttl = state_ttl
        self.max_size = max_size

    def save(self, message, state):
        args = message.args
        kwargs = message.kwargs
        if sys.getsizeof(args) > self.max_size:
            # Arguments exceed maximum size to display
            #  do not save them.
            args = []
        if sys.getsizeof(kwargs) > self.max_size:
            # Keyword arguments exceed maximum size to
            #  display do not save them
            kwargs = {}
        self.backend.set_state(
            message.message_id, State(state, args, kwargs), self.state_ttl
        )

    def after_enqueue(self, broker, message, delay):
        self.save(message, state=StateNamesEnum.Pending)

    def after_skip_message(self, broker, message):
        self.save(message, state=StateNamesEnum.Skipped)

    def after_message_canceled(self, broker, message):
        self.save(message, state=StateNamesEnum.Canceled)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        self.save(
            message,
            state=StateNamesEnum.Success
            if exception is None
            else StateNamesEnum.Failure,
        )

    def before_process_message(self, broker, message):
        self.save(message, state=StateNamesEnum.Started)
