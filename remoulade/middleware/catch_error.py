from remoulade import Middleware
from remoulade.middleware import Retries


class CatchError(Middleware):
    """Middleware that lets you enqueue an actor on message failure.

    Parameters:
      cleanup_actor(str): The name of an actor to enqueue on failure.
    """

    default_before = Retries

    @property
    def actor_options(self):
        return {"cleanup_actor"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if message.failed:
            cleanup_actor = self.get_option("cleanup_actor", broker=broker, message=message)
            if cleanup_actor:
                actor = broker.get_actor(cleanup_actor)
                actor.send(message.actor_name, type(exception).__name__, message.args, message.kwargs)

    def update_options_before_create_message(self, options, broker, actor_name):
        from ..actor import Actor

        cleanup_actor = options.get("cleanup_actor")
        if isinstance(cleanup_actor, Actor):
            options["cleanup_actor"] = cleanup_actor.actor_name

        elif cleanup_actor is not None and not isinstance(cleanup_actor, str):
            raise TypeError(f"cleanup_actor must be an Actor or a string, got {type(cleanup_actor)} instead")

        return options
