import random
from typing import Dict

import pytest

import remoulade
from remoulade.errors import InvalidProgress
from remoulade.middleware import CurrentMessage


class TestProgressMessage:
    """Class to test the actions related the
    progress of the message"""

    def test_set_progress_message(self, stub_broker, stub_worker, state_middleware):
        progress_messages: Dict[str, float] = {}

        @remoulade.actor
        def do_work():
            msg = CurrentMessage.get_current_message()
            progress = random.uniform(0, 1)
            msg.set_progress(progress)
            progress_messages[msg.message_id] = progress

        stub_broker.declare_actor(do_work)

        messages = [do_work.send() for _ in range(10)]
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        for m in messages:
            state = state_middleware.backend.get_state(m.message_id)
            assert state.progress == progress_messages[m.message_id]

    @pytest.mark.parametrize("progress", [-1, 2])
    def test_invalid_progress(self, progress, stub_broker, stub_worker, state_middleware):
        error = []

        @remoulade.actor
        def do_work():
            try:
                msg = CurrentMessage.get_current_message()
                msg.set_progress(progress)
            except InvalidProgress:
                error.append(True)

        stub_broker.declare_actor(do_work)
        do_work.send()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        assert error == [True]
