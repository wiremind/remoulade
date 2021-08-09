from datetime import datetime, timezone

from ..middleware import Middleware
from .backend import State, StateStatusesEnum


class MessageState(Middleware):

    """Middleware use to storage and update the state
    of the messages.
    Parameters
    state_ttl(int):Time(seconds) that the state will be storage
                    in the database
    """

    def __init__(self, backend, state_ttl=3600):
        self.backend = backend
        self.state_ttl = state_ttl

    def save(self, message, status, priority=None, **kwargs):
        if self.state_ttl is None or self.state_ttl <= 0:
            return
        args = message.args
        options = message.options
        kwargs_state = message.kwargs
        message_id = message.message_id
        actor_name = message.actor_name
        queue_name = message.queue_name
        self.backend.set_state(
            State(
                message_id,
                status,
                actor_name=actor_name,
                args=args,
                priority=priority,
                kwargs=kwargs_state,
                options=options,
                queue_name=queue_name,
                **kwargs,
            ),
            self.state_ttl,
        )

    def _get_current_time(self):
        return datetime.now(timezone.utc)

    def before_enqueue(self, broker, message, delay):
        priority = broker.get_actor(message.actor_name).priority
        composition_id = self.get_option("composition_id", broker=broker, message=message)
        self.save(
            message,
            status=StateStatusesEnum.Pending,
            enqueued_datetime=self._get_current_time(),
            priority=priority,
            composition_id=composition_id,
        )

    def after_enqueue(self, broker, message, delay, exception=None):
        if exception is not None:
            self.save(message, status=StateStatusesEnum.Failure, end_datetime=self._get_current_time())

    def after_skip_message(self, broker, message):
        self.save(message, status=StateStatusesEnum.Skipped)

    def after_message_canceled(self, broker, message):
        self.save(message, status=StateStatusesEnum.Canceled)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        self.save(
            message,
            status=StateStatusesEnum.Success if exception is None else StateStatusesEnum.Failure,
            end_datetime=self._get_current_time(),
        )

    def before_process_message(self, broker, message):
        self.save(message, status=StateStatusesEnum.Started, started_datetime=self._get_current_time())
