from random import choice, randint, sample

import pytest

from remoulade.api.main import app
from remoulade.cancel import Cancel
from remoulade.state import State, StateNamesEnum


@pytest.fixture
def api_client(state_middleware):
    with app.test_client() as client:
        yield client


class TestMessageStateAPI:
    """ Class Responsible to do the test of the API """

    def test_no_messages(self, stub_broker, state_middleware, api_client):
        res = api_client.get("/messages/states")
        assert res.status_code == 200
        assert len(res.json["result"]) == 0

    def test_invalid_url(self, stub_broker, state_middleware, api_client):
        res = api_client.get("/invalid_url/")
        assert res.status_code == 404

    def test_invalid_state_message(self, stub_broker, api_client):
        res = api_client.get("/messages/states?name=invalid_state")
        assert res.status_code == 400

    @pytest.mark.parametrize("name_state", [StateNamesEnum.Skipped, StateNamesEnum.Success])
    def test_get_state_by_name(self, name_state, stub_broker, state_middleware, api_client):
        state = State("3141516", name_state, [], {})
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.get("/messages/states?name={}".format(name_state.value))
        assert res.json["result"] == [state.asdict()]

    def test_get_by_message_id(self, stub_broker, state_middleware, api_client):
        message_id = "2718281"
        state = State(message_id, StateNamesEnum.Pending, [], {})
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.get("/messages/state/{}".format(message_id))
        assert res.json == state.asdict()

    @pytest.mark.parametrize("n", [0, 10, 50, 100])
    def test_get_states_by_name(self, n, stub_broker, state_middleware, api_client):
        # generate random test
        random_states = []
        for i in range(n):
            random_state = State(
                "id{}".format(i),
                choice(list(StateNamesEnum)),  # random state
                sample(range(1, 10), randint(0, 5)),  # random args
                {str(i): str(i) for i in range(randint(0, 5))},  # random kwargs
            )
            random_states.append(random_state.asdict())
            state_middleware.backend.set_state(random_state, ttl=1000)

        # check storage
        res = api_client.get("/messages/states")
        states = res.json["result"]
        assert len(states) == n
        # check integrity
        for state in states:
            assert state in random_states

    def test_request_cancel(self, stub_broker, api_client, cancel_backend):
        message_id = "3141516"
        stub_broker.add_middleware(Cancel(backend=cancel_backend))
        api_client.post("/messages/cancel/{}".format(message_id))
        assert cancel_backend.is_canceled(message_id, None)

    def test_request_cancel_without_backend(self, stub_broker, api_client):
        res = api_client.post("/messages/cancel/{}".format("id"))
        # if there is not cancel backend should raise an error
        assert res.json["error"] == "The default broker doesn't have a cancel backend."
        assert res.status_code == 500
