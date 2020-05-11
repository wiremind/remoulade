from remoulade.state import State, StateNamesEnum


class TestStateBackend:
    """This class test the different methods of state
    backend"""

    def test_change_existance_state(self, state_backend):
        message_id = "14271"
        state = State(message_id, StateNamesEnum.Pending, actor_name="do_work", args=[1, 2], kwargs={"name": "work"})
        state_backend.set_state(state, ttl=100)
        # the state name changes
        state = State(message_id, StateNamesEnum.Success)
        state_backend.set_state(state, ttl=100)
        # if the states exist, then it update the new fields
        # then args and kwargs should hold
        # and the state name should change
        state = state_backend.get_state(message_id)
        assert state.name == StateNamesEnum.Success
        assert state.args == [1, 2]
        assert state.kwargs == {"name": "work"}
