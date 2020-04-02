from ..cancel import Cancel
from .backend import State


class Decorator:
    """Decorator to save the
    state in a backend
    """
    def save(state):
        def wrapper(decorated):
            def save_state(*args, **kwargs):
                key = args[2].message_id
                args[0].backend.store_result(key,
                                             State(state),
                                             args[0].result_ttl)
            return save_state
        return wrapper


class MessageState(Cancel):

    """Middleware use to storage and update the state
    of the messages.
    """
    def __init__(self, backend, result_ttl=86400):
        super().__init__(backend=backend)
        self.backend = backend
        self.result_ttl = result_ttl

    @Decorator.save(state="Pending")
    def after_enqueue(self, broker, message, delay):
        pass

    @Decorator.save(state="Success")
    def after_process_message(self, broker, message, *, result=None, exception=None):
        pass

    @Decorator.save(state="Skipped")
    def after_skip_message(self, broker, message):
        pass

    @Decorator.save(state="Canceled")
    def after_message_canceled(self, broker, message):
        pass
