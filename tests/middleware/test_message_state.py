import time

import pytest

import remoulade
from remoulade.cancel import Cancel
from remoulade.middleware import Middleware, SkipMessage
from remoulade.state.backend import State, StateNamesEnum
from remoulade.state.middleware import MessageState


class TestMessageState:
    """Class to test the middleware
    MessageState
    """

    def test_pending_state_message(self, stub_broker, state_middleware, do_work):
        msg = do_work.send()
        assert state_middleware.backend.get_state(msg.message_id).name == StateNamesEnum.Pending

    def test_success_state_message(self, stub_broker, stub_worker, state_middleware, do_work):
        msg = do_work.send()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        assert state_middleware.backend.get_state(msg.message_id).name == StateNamesEnum.Success

    def test_started_state_message(self, stub_broker, state_middleware, stub_worker):
        @remoulade.actor
        def wait():
            time.sleep(0.3)

        stub_broker.declare_actor(wait)
        msg = wait.send()
        # We wait the message be emited
        time.sleep(0.1)
        assert state_middleware.backend.get_state(msg.message_id).name == StateNamesEnum.Started

    def test_failure_state_message(self, stub_broker, stub_worker, state_middleware):
        @remoulade.actor
        def error():
            raise Exception()

        remoulade.declare_actors([error])
        msg = error.send()
        stub_broker.join(error.queue_name)
        stub_worker.join()
        assert state_middleware.backend.get_state(msg.message_id).name == StateNamesEnum.Failure

    def test_cancel_state_message(
        self, stub_broker, stub_worker, cancel_backend, state_middleware, do_work,
    ):
        stub_broker.add_middleware(Cancel(backend=cancel_backend))
        #  Pause the worker to be able to cancel the  message, and
        #   this does not been processed  when it enters the queue
        stub_worker.pause()
        msg = do_work.send()
        msg.cancel()
        stub_worker.resume()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        assert state_middleware.backend.get_state(msg.message_id).name == StateNamesEnum.Canceled

    def test_skip_state_message(self, stub_broker, stub_worker, state_middleware, do_work):
        class SkipMiddleware(Middleware):
            def before_process_message(self, broker, message):
                raise SkipMessage()

        stub_broker.add_middleware(SkipMiddleware())
        msg = do_work.send()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        assert state_middleware.backend.get_state(msg.message_id).name == StateNamesEnum.Skipped

    @pytest.mark.parametrize(
        "ttl, result_type", [pytest.param(1000, State), pytest.param(1, type(None)),],
    )
    def test_expiration_data_backend(self, ttl, result_type, stub_broker, state_backend):
        @remoulade.actor
        def wait():
            time.sleep(0.3)

        stub_broker.add_middleware(MessageState(backend=state_backend, state_ttl=ttl))
        stub_broker.declare_actor(wait)
        msg = wait.send()
        time.sleep(2)
        data = state_backend.get_state(msg.message_id)
        # if the ttl is  greater than the expiration, the data should be None
        assert type(data) == result_type

    @pytest.mark.parametrize(
        "max_size,expected", [pytest.param(0, []), pytest.param(1000, [5])],
    )
    def test_maximum_size(self, max_size, expected, stub_broker, state_backend, do_work):
        @remoulade.actor
        def do_work(x):
            return x

        stub_broker.add_middleware(MessageState(backend=state_backend, max_size=max_size))
        stub_broker.declare_actor(do_work)
        msg = do_work.send(5)
        args = state_backend.get_state(msg.message_id).args
        # if the max_size = 0, then should not storage nothing
        assert args == expected
