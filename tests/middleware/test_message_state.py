import time
from unittest.mock import Mock

import pytest

import remoulade
from remoulade import group
from remoulade.cancel import Cancel
from remoulade.middleware import Middleware, SkipMessage
from remoulade.state.backend import State, StateStatusesEnum
from remoulade.state.middleware import MessageState, State


class TestMessageState:
    """Class to test the middleware
    MessageState
    """

    def test_pending_state_message(self, stub_broker, state_middleware, do_work, frozen_datetime):
        msg = do_work.send()
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.message_id == msg.message_id
        assert state.status == StateStatusesEnum.Pending
        assert state.enqueued_datetime.isoformat() == "2020-02-03T00:00:00+00:00"

    def test_success_state_message(self, stub_broker, stub_worker, state_middleware, frozen_datetime):
        @remoulade.actor
        def do_work():
            frozen_datetime.tick(delta=15)

        stub_broker.declare_actor(do_work)
        stub_worker.pause()
        msg = do_work.send()
        frozen_datetime.tick(delta=15)
        stub_worker.resume()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.status == StateStatusesEnum.Success
        assert state.enqueued_datetime.isoformat() == "2020-02-03T00:00:00+00:00"
        assert state.started_datetime.isoformat() == "2020-02-03T00:00:15+00:00"
        assert state.end_datetime.isoformat() == "2020-02-03T00:00:30+00:00"

    def test_started_state_message(self, stub_broker, state_middleware, stub_worker, frozen_datetime):
        @remoulade.actor
        def wait():
            time.sleep(0.3)

        stub_broker.declare_actor(wait)
        msg = wait.send()
        # We wait the message be emited
        time.sleep(0.1)
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.status == StateStatusesEnum.Started
        assert state.started_datetime.isoformat() == "2020-02-03T00:00:00+00:00"

    def test_failure_state_message(self, stub_broker, stub_worker, state_middleware, frozen_datetime):
        @remoulade.actor
        def error():
            raise Exception()

        remoulade.declare_actors([error])
        msg = error.send()
        stub_broker.join(error.queue_name)
        stub_worker.join()
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.status == StateStatusesEnum.Failure
        assert state.end_datetime.isoformat() == "2020-02-03T00:00:00+00:00"

    def test_cancel_state_message(self, stub_broker, stub_worker, cancel_backend, state_middleware, do_work):
        stub_broker.add_middleware(Cancel(backend=cancel_backend))
        #  Pause the worker to be able to cancel the  message, and
        #   this does not been processed  when it enters the queue
        stub_worker.pause()
        msg = do_work.send()
        msg.cancel()
        stub_worker.resume()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.status == StateStatusesEnum.Canceled
        # should not finish, since is cancelled
        assert state.end_datetime is None

    def test_skip_state_message(self, stub_broker, stub_worker, state_middleware, do_work):
        class SkipMiddleware(Middleware):
            def before_process_message(self, broker, message):
                raise SkipMessage()

        stub_broker.add_middleware(SkipMiddleware())
        msg = do_work.send()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.status == StateStatusesEnum.Skipped
        # should not finish, since is skipped and does not
        # try again
        assert state.end_datetime is None

    @pytest.mark.parametrize("ttl, result_type", [pytest.param(1000, State), pytest.param(1, type(None))])
    def test_expiration_data_backend(self, ttl, result_type, stub_broker, state_backend):
        @remoulade.actor
        def wait():
            pass

        stub_broker.add_middleware(MessageState(backend=state_backend, state_ttl=ttl))
        stub_broker.declare_actor(wait)
        msg = wait.send()
        time.sleep(2)
        data = state_backend.get_state(msg.message_id)
        # if the ttl is  greater than the expiration, the data should be None
        assert type(data) == result_type

    @pytest.mark.parametrize("max_size", [200, 1000])
    def test_maximum_size_args(self, max_size, stub_broker, state_backend, do_work):
        @remoulade.actor
        def do_work(x):
            return x

        state_backend.max_size = max_size
        stub_broker.add_middleware(MessageState(backend=state_backend))
        stub_broker.declare_actor(do_work)
        long_string = "".join("a" for _ in range(256))
        msg = do_work.send(long_string)
        args = state_backend.get_state(msg.message_id).args
        # if the max_size == 0, then should not storage nothing

        if max_size > 200:
            assert list(args) == [long_string]
        else:
            assert args is None

    def test_save_group_id_in_message(self, stub_broker, state_middleware, do_work):
        msg = do_work.message()
        group_id = group([msg]).run().group_id
        state = state_middleware.backend.get_state(msg.message_id)
        assert state.message_id == msg.message_id
        assert state.group_id == group_id

    @pytest.mark.parametrize("state_ttl", [0, -1, None])
    def test_backend_not_called_if_no_state_ttl(self, stub_broker, do_work, state_ttl):
        backend = Mock()
        stub_broker.add_middleware(MessageState(backend=backend, state_ttl=state_ttl))
        do_work.send()
        assert backend.set_state.call_count == 0
