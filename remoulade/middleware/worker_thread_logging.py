import logging

from ..errors import RateLimitExceeded
from ..logging import get_logger
from .middleware import Middleware


class WorkerThreadLogging(Middleware):
    def __init__(self):
        self.logger = get_logger(__name__, type(self))

    def build_extra(self, message, max_input_size: int = 1000):
        return {
            **message.options.get("logging_metadata", {}),
            "message_id": message.message_id,
            "input": {"args": str(message.args)[:max_input_size], "kwargs": str(message.kwargs)[:max_input_size]},
        }

    def before_actor_execution(self, broker, message):
        self.logger.info("Started Actor %s", message, extra=self.build_extra(message))

    def after_actor_execution(self, broker, message, *, runtime=0):
        extra = self.build_extra(message)
        extra["runtime"] = runtime
        self.logger.info("Finished Actor %s after %.02fms.", message, runtime, extra=extra)

    def before_process_message(self, broker, message):
        self.logger.debug(
            "Received message %s with id %r.", message, message.message_id, extra=self.build_extra(message)
        )

    def after_process_message(self, broker, message, *, result=None, exception=None):
        if exception is None:
            return
        if isinstance(exception, RateLimitExceeded):
            self.logger.warning(
                "Rate limit exceeded in message %s: %s.", message, exception, extra=self.build_extra(message)
            )
        else:
            self.logger.log(
                logging.ERROR if message.failed else logging.WARNING,
                "Failed to process message %s with unhandled %s",
                message,
                exception.__class__.__name__,
                exc_info=True,
                extra=self.build_extra(message, 5000),
            )

    def after_skip_message(self, broker, message):
        self.logger.warning("Message %s was skipped.", message, extra=self.build_extra(message))

    def after_message_canceled(self, broker, message):
        self.logger.warning("Message %s has been canceled", message, extra=self.build_extra(message))
