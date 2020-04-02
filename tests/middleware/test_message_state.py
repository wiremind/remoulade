import remoulade
from remoulade.state.message_state import MessageState


class TestMessageState:
    """Class to test the middleware
    MessageState
    """

    @staticmethod
    @remoulade.actor
    def add(x, y):
        return x + y

    def test_pending_state_message(self, rabbitmq_broker_state, redis_state_backend):
        rabbitmq_broker_state.declare_actor(TestMessageState.add)
        rabbitmq_broker_state.add_middleware(MessageState(backend=redis_state_backend))
        msg = TestMessageState.add.send(5, 7)
        assert redis_state_backend.get_value(msg.message_id)['state'] == "Pending"

    def test_success_state_message(self, local_broker, redis_state_backend):
        local_broker.declare_actor(TestMessageState.add)
        local_broker.add_middleware(MessageState(backend=redis_state_backend))
        msg = TestMessageState.add.send(5, 7).result
        assert redis_state_backend.get_value(msg.message_id)['state'] == "Success"

    def test_cancel_state_message(self, local_broker, redis_state_backend):
        local_broker.declare_actor(TestMessageState.add)
        middleware = MessageState(backend=redis_state_backend)
        local_broker.add_middleware(middleware)
        msg = TestMessageState.add.send(5, 7)
        # TODO: this should be msg.cancel()
        middleware.after_message_canceled(local_broker, msg)
        assert redis_state_backend.get_value(msg.message_id)['state'] == "Canceled"
