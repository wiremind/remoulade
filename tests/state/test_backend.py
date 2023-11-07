import pytest

from remoulade.state import State, StateStatusesEnum
from remoulade.state.backends import PostgresBackend


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

    def test_count_messages(self, stub_broker, state_middleware):
        backend = state_middleware.backend
        for i in range(3):
            backend.set_state(State(f"id{i}"))

        assert backend.get_states_count() == 3

    def test_count_compositions(self, stub_broker, state_middleware):
        if not isinstance(state_middleware.backend, PostgresBackend):
            pytest.skip()

        backend = state_middleware.backend

        for i in range(3):
            for j in range(2):
                backend.set_state(State(f"id{i * j}", composition_id=f"id{j}"))

        assert backend.get_states_count() == 2

    def test_sort_with_offset(self, stub_broker, state_middleware):
        if not isinstance(state_middleware.backend, PostgresBackend):
            pytest.skip()
        backend = state_middleware.backend
        for i in range(8):
            backend.set_state(State(f"id{i}", actor_name=f"{3 + 4 * (i // 4) - i % 4}"))

        res = backend.get_states(size=3, sort_column="actor_name", sort_direction="desc")
        assert res[0].actor_name == "7"
        assert res[1].actor_name == "6"
        assert res[2].actor_name == "5"
        res = backend.get_states(size=3, sort_column="actor_name", sort_direction="asc")
        assert res[0].actor_name == "0"
        assert res[1].actor_name == "1"
        assert res[2].actor_name == "2"
