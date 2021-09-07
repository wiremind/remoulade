from .middleware import Middleware


class CatchError(Middleware):
    """Middleware that lets you enqueue an actor on message failure.

    Parameters:
      on_failure(str): The name of an actor to enqueue on failure.
    """

    @property
    def actor_options(self):
        return {"on_failure"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if message.failed:
            on_failure = self.get_option("on_failure", broker=broker, message=message)
            if on_failure:
                actor = broker.get_actor(on_failure)
                actor.send(message.actor_name, type(exception).__name__, message.args, message.kwargs)

    def update_options_before_create_message(self, options, broker, actor_name):
        from ..actor import Actor

        on_failure = options.get("on_failure")
        if isinstance(on_failure, Actor):
            options["on_failure"] = on_failure.actor_name

        elif on_failure is not None and not isinstance(on_failure, str):
            raise TypeError(f"on_failure must be an Actor or a string, got {type(on_failure)} instead")

        return options
