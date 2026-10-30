import json
from typing import Any

from ..errors import UnsupportedMessageEncoding
from ..logging import get_logger
from .middleware import Middleware

ENCODING_ERRORS = (UnsupportedMessageEncoding, TypeError, ValueError)


def _is_json_encodable(value: Any) -> bool:
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return False
    return True


class CatchError(Middleware):
    """Middleware that lets you enqueue another actor or message on message failure.

    Parameters:
      on_failure(Message|Actor|str): A Message, Actor or Actor name to enqueue on failure.
    """

    def __init__(self) -> None:
        self.logger = get_logger(__name__, type(self))

    @property
    def actor_options(self):
        return {"on_failure"}

    def after_process_message(self, broker, message, *, result=None, exception=None):
        from .. import Message

        if message.failed:
            on_failure = self.get_option("on_failure", broker=broker, message=message)

            def on_failure_args(exception_args):
                return [message.actor_name, type(exception).__name__, exception_args, message.args, message.kwargs]

            if isinstance(on_failure, str):
                on_failure_message = broker.get_actor(on_failure).message(*on_failure_args(exception.args))
            elif isinstance(on_failure, dict):
                on_failure_message = Message[Any](**on_failure).copy(args=on_failure_args(exception.args))
            else:
                return

            try:
                broker.enqueue(on_failure_message)
            except ENCODING_ERRORS as error:
                unencodable_arg_types = {
                    index: type(arg).__name__ for index, arg in enumerate(exception.args) if not _is_json_encodable(arg)
                }
                if not unencodable_arg_types:
                    raise
                self.logger.warning(
                    "Could not encode the on_failure message to actor %s after %s failed (%s: %s); retrying with "
                    "exception args %s replaced by their str.",
                    on_failure_message.actor_name,
                    message.actor_name,
                    type(error).__name__,
                    error,
                    unencodable_arg_types,
                )
                coerced_exception_args = tuple(
                    str(arg) if index in unencodable_arg_types else arg for index, arg in enumerate(exception.args)
                )
                broker.enqueue(on_failure_message.copy(args=on_failure_args(coerced_exception_args)))

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
