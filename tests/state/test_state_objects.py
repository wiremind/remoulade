import pytest

from remoulade.state.backend import State, StateNamesEnum
from remoulade.state.errors import InvalidStateError


class TestState:
    """Class to test State Objects, a data transfer object
    that represents the state of a message"""

    @pytest.mark.parametrize("defined_state", StateNamesEnum)
    def test_create_valid_state(self, defined_state):
        assert State("id", defined_state, args=[], kwargs={})

    @pytest.mark.parametrize("undefined_state", ["UndefinedState", "pending"])
    def test_raise_exception_when_invalid_state(self, undefined_state):
        # if I send a state not defined in StateNamesEnum
        #  it should raise an exception
        with pytest.raises(InvalidStateError):
            State("id", undefined_state, args=[], kwargs={})

    def test_check_conversion_object_to_dict(self):
        dict_state = State("id", StateNamesEnum.Success, args=[1, 2, 3], kwargs={"key": "value"}).as_dict()
        assert dict_state["name"] == StateNamesEnum.Success.name
        assert dict_state["args"] == [1, 2, 3]
        assert dict_state["kwargs"] == {"key": "value"}

    def test_check_conversion_dict_to_object(self):
        dict_state = {"name": "Success", "args": [1, 2, 3], "kwargs": {"key": "value"}, "message_id": "idtest"}
        stateobj = State.from_dict(dict_state)
        assert stateobj.message_id == dict_state["message_id"]
        assert stateobj.name == dict_state["name"]
        assert stateobj.kwargs == dict_state["kwargs"]
        assert stateobj.args == dict_state["args"]

    def test_exclude_keys_from_serialization(self):
        dict_state = State("id", StateNamesEnum.Success, args=[1, 2, 3], kwargs={"key": "value"}).as_dict(
            exclude_keys=("kwargs", "id")
        )
        assert "kwargs" not in dict_state
        assert "id" not in dict_state
