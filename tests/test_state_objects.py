import pytest

from remoulade.state.backend import State, StateNamesEnum
from remoulade.state.errors import InvalidStateError


class TestState:
    """Class to test State Objects, a data transfer object
    that represents the state of a message"""

    @pytest.mark.parametrize("defined_state", StateNamesEnum)
    def test_create_valid_state(self, defined_state):
        assert State(defined_state.name, [], {})

    @pytest.mark.parametrize("undefined_state", ["UndefinedState", "pending"])
    def test_raise_exception_when_invalid_state(self, undefined_state):
        # if I send a state not defined in StateNamesEnum
        #  it should raise an exception
        with pytest.raises(InvalidStateError):
            State(undefined_state, [], {})

    def test_check_conversion_object_to_dict(self):
        dict_state = State(StateNamesEnum.Success, [1, 2, 3], {"key": "value"}).asdict()
        assert dict_state["name"] == StateNamesEnum.Success
        assert dict_state["args"] == [1, 2, 3]
        assert dict_state["kwargs"] == {"key": "value"}

    def test_check_conversion_dict_to_object(self):
        dict_state = {"name": "Success", "args": [1, 2, 3], "kwargs": {"key": "value"}}
        stateobj = State.from_dict(**dict_state)
        assert stateobj.asdict() == dict_state
