import remoulade
from remoulade.middleware import CurrentMessage


class TestCurrentMessage:
    """Class to test the middleware
    CurrentMessage"""

    def test_middleware_in_actor(self, stub_broker, stub_worker):
        message_ids = set()

        @remoulade.actor
        def do_work():
            msg = CurrentMessage.get_current_message()
            message_ids.add(msg.message_id)

        stub_broker.declare_actor(do_work)
        messages = [do_work.send() for _ in range(20)]

        stub_broker.join(do_work.queue_name)
        stub_worker.join()

        assert message_ids == {m.message_id for m in messages}
        # verify no current message outside of actor
        assert CurrentMessage.get_current_message() is None

    def test_middleware_not_in_actor(self, stub_broker, stub_worker):

        stub_broker.middleware.remove(next(m for m in stub_broker.middleware if type(m) == CurrentMessage))
        message_ids = set()

        @remoulade.actor
        def do_work():
            msg = CurrentMessage.get_current_message()
            if msg is not None:
                message_ids.add(msg.message_id)

        stub_broker.declare_actor(do_work)
        for _ in range(10):
            do_work.send()

        stub_broker.join(do_work.queue_name)
        stub_worker.join()

        assert len(message_ids) == 0
