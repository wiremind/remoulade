from .middleware import Middleware


class CatchError(Middleware):
    """Middleware that lets you enqueue another message on message failure.

    Parameters:
      on_failure(str): A Message to enqueue on failure.
    """

    @property
    def actor_options(self):
        return {"on_failure"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        from .. import Message

        if message.failed:
            on_failure = self.get_option("on_failure", broker=broker, message=message)
            if on_failure:
                on_failure_message = Message(**on_failure)
                on_failure_message = on_failure_message.copy(
                    args=[message.actor_name, type(exception).__name__, message.args, message.kwargs]
                )
                broker.enqueue(on_failure_message)

    def update_options_before_create_message(self, options, broker, actor_name):
        from .. import Message

        on_failure = options.get("on_failure")
        if isinstance(on_failure, Message):
            options["on_failure"] = on_failure.asdict()

        elif on_failure is not None:
            raise TypeError(f"on_failure must be an Message, got {type(on_failure)} instead")

        return options
