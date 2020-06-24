import datetime
import json
from random import choice, randint, sample
from unittest import mock
from unittest.mock import MagicMock

import pytest
import pytz

import remoulade
from remoulade.api.main import app
from remoulade.cancel import Cancel
from remoulade.message import Message
from remoulade.scheduler import ScheduledJob
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
        assert len(res.json["data"]) == 0

    def test_invalid_url(self, stub_broker, state_middleware, api_client):
        res = api_client.get("/invalid_url/")
        assert res.status_code == 404

    def test_invalid_state_message(self, stub_broker, api_client):
        res = api_client.get("/messages/states?name=invalid_state")
        assert res.status_code == 400

    @pytest.mark.parametrize("name_state", [StateNamesEnum.Skipped, StateNamesEnum.Success])
    def test_get_state_by_name(self, name_state, stub_broker, state_middleware, api_client):
        state = State("1", name_state)
        state_middleware.backend.set_state(state, ttl=1000)
        args = {"search_value": name_state.value}
        res = api_client.get("/messages/states", data=args)
        assert res.json == {"count": 1, "data": [state.as_dict()]}

    def test_get_by_message_id(self, stub_broker, state_middleware, api_client):
        message_id = "1"
        state = State(message_id, StateNamesEnum.Pending)
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.get("/messages/state/{}".format(message_id))
        assert res.json == state.as_dict()

    @pytest.mark.parametrize("n", [0, 10, 50, 100])
    def test_get_states_by_name(self, n, stub_broker, state_middleware, api_client):
        # generate random test
        random_states = []
        for i in range(n):
            random_state = State(
                "id{}".format(i),
                choice(list(StateNamesEnum)),  # random state
                args=sample(range(1, 10), randint(0, 5)),  # random args
                kwargs={str(i): str(i) for i in range(randint(0, 5))},  # random kwargs
            )
            random_states.append(random_state.as_dict())
            state_middleware.backend.set_state(random_state, ttl=1000)

        # check storage
        res = api_client.get("/messages/states")
        states = res.json["data"]
        assert len(states) == n
        # check integrity
        for state in states:
            assert state in random_states

    def test_request_cancel(self, stub_broker, api_client, cancel_backend):
        message_id = "1"
        stub_broker.add_middleware(Cancel(backend=cancel_backend))
        api_client.post("/messages/cancel/{}".format(message_id))
        assert cancel_backend.is_canceled(message_id, None)

    def test_request_cancel_without_backend(self, stub_broker, api_client):
        res = api_client.post("/messages/cancel/{}".format("id"))
        # if there is not cancel backend should raise an error
        assert res.json["error"] == "The default broker doesn't have a cancel backend."
        assert res.status_code == 500

    def test_no_scheduler(self, stub_broker, api_client):
        res = api_client.get("/scheduled/jobs")
        assert res.json["result"] == []

    def test_scheduled_jobs(self, scheduler, api_client, do_work, frozen_datetime):
        timezone = pytz.timezone("Europe/Paris")
        scheduler.schedule = [
            ScheduledJob(
                actor_name=do_work.actor_name, daily_time=(datetime.datetime.now(timezone)).time(), tz="Europe/Paris",
            )
        ]
        scheduler.sync_config()

        res = api_client.get("/scheduled/jobs")
        jobs = res.json["result"]
        assert jobs == [
            {
                "actor_name": "do_work",
                "args": [],
                "daily_time": "01:00:00",
                "enabled": True,
                "interval": 86400,
                "iso_weekday": None,
                "kwargs": {},
                "last_queued": None,
                "tz": "Europe/Paris",
            }
        ]

    def test_enqueue_message(self, stub_broker, do_work, api_client):
        data = {
            "actor_name": "do_work",
            "args": ["1", "2"],
            "kwargs": {},
            "options": {"time_limit": 1000},
            "delay": None,
        }
        stub_broker.enqueue = MagicMock()
        stub_broker.join(do_work.queue_name)
        res = api_client.post("/messages", data=json.dumps(data), content_type="application/json")
        message = Message(
            queue_name="default",
            actor_name="do_work",
            args=("1", "2"),
            kwargs={},
            options={"time_limit": 1000},
            message_id=mock.ANY,
            message_timestamp=mock.ANY,
        )
        assert stub_broker.enqueue.call_count == 1
        assert stub_broker.enqueue.call_args == mock.call(message, delay=None)
        assert res.status_code == 200

    @pytest.mark.parametrize(
        "actor_name,error",
        [
            (None, "Field may not be null."),
            ("", "Shorter than minimum length 1."),
            (111, "Not a valid string."),
            ([], "Not a valid string."),
        ],
    )
    def test_invalid_actor_name_to_enqueue(self, actor_name, error, stub_broker, do_work, api_client):
        data = {
            "actor_name": actor_name,
        }
        res = api_client.post("/messages", data=json.dumps(data), content_type="application/json")
        validation_error = res.json["error"]
        assert validation_error["actor_name"] == [error]
        assert res.status_code == 400

    @pytest.mark.parametrize(
        "delay,error", [(-1, "Must be greater than or equal to 1."), ("str", "Not a valid number.")]
    )
    def test_invalid_delay_to_enqueue(self, delay, error, stub_broker, do_work, api_client):
        data = {
            "actor_name": "some_actor_name",
            "delay": delay,
        }
        res = api_client.post("/messages", data=json.dumps(data), content_type="application/json")
        validation_error = res.json["error"]
        assert validation_error["delay"] == [error]
        assert res.status_code == 400

    def test_get_declared_actors(self, stub_broker, do_work, api_client):
        @remoulade.actor(queue_name="foo", priority=10)
        def do_job():
            pass

        stub_broker.declare_actor(do_job)
        res = api_client.get("/actors")
        assert res.json["result"] == [
            {"name": "do_work", "priority": 0, "queue_name": "default"},
            {"name": "do_job", "priority": 10, "queue_name": "foo"},
        ]

    def test_filter_messages(self, stub_broker, api_client, state_middleware):
        state = State("some_message_id",)
        state_middleware.backend.set_state(state, ttl=1000)
        data = {
            "sort_column": "message_id",
            "search_value": "some_mes",
        }
        res = api_client.get("/messages/states", query_string=data)
        assert res.json == {"count": 1, "data": [state.as_dict()]}

    @pytest.mark.parametrize("offset", [0, 1, 5, 100])
    def test_get_states_offset(self, offset, stub_broker, api_client, state_middleware):
        for i in range(0, 10):
            state_middleware.backend.set_state(State("id{}".format(i)), ttl=1000)
        res = api_client.get("/messages/states", query_string={"offset": offset})
        if offset >= 10:
            assert res.json["data"] == []
        else:
            assert len(res.json["data"]) + offset == 10

    @pytest.mark.parametrize("size", [1, 5, 100])
    def test_get_states_page_size(self, size, stub_broker, api_client, state_middleware):
        for i in range(0, 10):
            state_middleware.backend.set_state(State("id{}".format(i)), ttl=1000)
        res = api_client.get("/messages/states", query_string={"size": size})
        if size >= 10:
            assert res.json["count"] == 10
        else:
            assert len(res.json["data"]) == size
