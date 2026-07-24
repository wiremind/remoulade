from typing import Any

from ..common import current_millis, generate_unique_id
from .middleware import Middleware


class CatchError(Middleware):
    """Middleware that lets you enqueue another actor or message on message failure.

    Parameters:
      on_failure(Message|Actor|str): A Message, Actor or Actor name to enqueue on failure.
    """

    @property
    def actor_options(self):
        return {"on_failure"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        from .. import Message

        if message.failed:
            on_failure = self.get_option("on_failure", broker=broker, message=message)

            if isinstance(on_failure, str):
                actor = broker.get_actor(on_failure)
                actor.send(message.actor_name, type(exception).__name__, exception.args, message.args, message.kwargs)
            elif isinstance(on_failure, dict):
                on_failure_message = Message[Any](**on_failure)
                # The on_failure message may have been built long before the failure it reports (e.g. attached to a
                # message that retried/aged for hours). Re-stamp it with a fresh id and timestamp so it is treated as
                # freshly enqueued and cannot be dropped by AgeLimit against its original build time.
                on_failure_message = on_failure_message.copy(
                    args=[message.actor_name, type(exception).__name__, exception.args, message.args, message.kwargs],
                    message_id=generate_unique_id(),
                    message_timestamp=current_millis(),
                )
                broker.enqueue(on_failure_message)

    def update_options_before_create_message(self, options, broker, actor_name):
        from .. import Message
        from ..actor import Actor

        on_failure = options.get("on_failure")
        if isinstance(on_failure, Message):
            options["on_failure"] = on_failure.asdict()
        elif isinstance(on_failure, Actor):
            options["on_failure"] = on_failure.actor_name
        elif on_failure is not None and not isinstance(on_failure, str):
            raise TypeError(f"on_failure must be an Message, an Actor or a string, got {type(on_failure)} instead")

        return options
