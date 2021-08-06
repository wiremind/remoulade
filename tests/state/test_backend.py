from remoulade.state import State, StateStatusesEnum


class TestStateBackend:
    """This class test the different methods of state
    backend"""

    def test_change_existence_state(self, state_backend):
        message_id = "14271"
        state = State(
            message_id, StateStatusesEnum.Pending, actor_name="do_work", args=[1, 2], kwargs={"status": "work"}
        )
        state_backend.set_state(state, ttl=100)
        # the state status changes
        state = State(message_id, StateStatusesEnum.Success)
        state_backend.set_state(state, ttl=100)
        # if the states exist, then it update the new fields
        # then args and kwargs should hold
        # and the state status should change
        state = state_backend.get_state(message_id)
        assert state.status == StateStatusesEnum.Success
        assert state.args == [1, 2]
        assert state.kwargs == {"status": "work"}
