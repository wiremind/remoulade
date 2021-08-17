from remoulade import Middleware
from remoulade.middleware import Retries


class CatchError(Middleware):
    """Middleware that lets you enqueue an actor on message failure.

    Parameters:
      cleanup_actor(str): The name of an actor to enqueue on failure.
    """

    default_after = Retries

    @property
    def actor_options(self):
        return {"cleanup_actor"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if message.failed:
            actor_name = self.get_option("cleanup_actor", broker=broker, message=message)
            if actor_name:
                actor = broker.get_actor(actor_name)
                actor.send(message.asdict(), {"type": type(exception).__name__, "message": str(exception)})
