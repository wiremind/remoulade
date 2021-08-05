from typing import Callable, Dict, Optional

from ..logging import get_logger
from .middleware import Middleware


class LoggingMetadata(Middleware):
    """Middleware that lets you add logging_metadata to messages.

    Parameters:
      logging_metadata(dict): The metadata to add to messages
      logging_metadata_getter(Callable[[], dict]): A callback that returns
    """

    def __init__(
        self, *, logging_metadata: Optional[Dict] = None, logging_metadata_getter: Optional[Callable[[], Dict]] = None
    ):
        self.logger = get_logger(__name__, type(self))
        self.logging_metadata = logging_metadata
        self.logging_metadata_getter = logging_metadata_getter

    @property
    def actor_options(self):
        return {
            "logging_metadata",
            "logging_metadata_getter",
        }

    def merge_metadata(self, total_logging_metadata, logging_metadata, logging_metadata_getter):
        if logging_metadata is not None:
            total_logging_metadata = {**total_logging_metadata, **logging_metadata}
        if logging_metadata_getter is not None:
            if callable(logging_metadata_getter):
                total_logging_metadata = {**total_logging_metadata, **logging_metadata_getter()}
            else:
                raise TypeError("logging_metadata_getter must be callable.")

        return total_logging_metadata

    def update_options_before_create_message(self, options, broker, actor_name):

        total_logging_metadata: dict = {}

        # getting logging_metadata at middleware level
        logging_metadata = self.logging_metadata
        logging_metadata_getter = self.logging_metadata_getter
        total_logging_metadata = self.merge_metadata(total_logging_metadata, logging_metadata, logging_metadata_getter)

        # getting logging_metadata at actor level
        actor_options = broker.get_actor(actor_name).options
        logging_metadata = actor_options.get("logging_metadata", None)
        logging_metadata_getter = actor_options.get("logging_metadata_getter", None)
        total_logging_metadata = self.merge_metadata(total_logging_metadata, logging_metadata, logging_metadata_getter)

        # getting logging_metadata at message level
        logging_metadata = options.get("logging_metadata", None)
        logging_metadata_getter = options.get("logging_metadata_getter", None)
        total_logging_metadata = self.merge_metadata(total_logging_metadata, logging_metadata, logging_metadata_getter)
        if logging_metadata_getter is not None:
            del options["logging_metadata_getter"]

        if total_logging_metadata != {}:
            options["logging_metadata"] = total_logging_metadata

        for name in ["message_id", "input"]:
            if name in total_logging_metadata:
                self.logger.error(f"'{name}' cannot be used as a logging_metadata key. It will be overwritten.")

        return options
