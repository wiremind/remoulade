import datetime
import json
from datetime import date
from operator import itemgetter
from random import choice
from unittest import mock
from unittest.mock import MagicMock

import pytest
import pytz
from dateutil.parser import parse

import remoulade
from remoulade import set_scheduler
from remoulade.api.main import app
from remoulade.cancel import Cancel
from remoulade.message import Message
from remoulade.scheduler import ScheduledJob
from remoulade.state import State, StateStatusesEnum


class TestMessageStateAPI:
    """Class Responsible to do the test of the API"""

    def test_no_messages(self, stub_broker, state_middleware, api_client):
        res = api_client.post("/messages/states")
        assert res.status_code == 200
        assert len(res.json["data"]) == 0

    def test_invalid_url(self, stub_broker, state_middleware, api_client):
        res = api_client.get("/invalid_url/")
        assert res.status_code == 404

    def test_invalid_state_message(self, stub_broker, api_client):
        res = api_client.post(
            "/messages/states", data=json.dumps({"status": "invalid_state"}), content_type="application/json"
        )
        assert res.status_code == 400

    @pytest.mark.parametrize("status", [StateStatusesEnum.Skipped, StateStatusesEnum.Success])
    def test_get_state_by_name(self, status, stub_broker, state_middleware, api_client):
        state = State("1", status)
        state_middleware.backend.set_state(state, ttl=1000)
        args = {"selected_statuses": [status.value]}
        res = api_client.post("/messages/states", data=json.dumps(args), content_type="application/json")
        assert res.json == {"count": 1, "data": [state.as_dict()]}

    def test_get_by_message_id(self, stub_broker, state_middleware, api_client):
        message_id = "1"
        state = State(message_id, StateStatusesEnum.Pending)
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.get("/messages/states/{}".format(message_id))
        assert res.json == state.as_dict()

    @pytest.mark.parametrize("n", [0, 10, 50, 100])
    def test_get_states_by_name(self, n, stub_broker, state_middleware, api_client):
        # generate random test
        random_states = []
        for i in range(n):
            random_state = State(f"id{i}", choice(list(StateStatusesEnum)))  # random state
            random_states.append(random_state.as_dict())
            state_middleware.backend.set_state(random_state, ttl=1000)

        # check storage
        res = api_client.post("/messages/states", data=json.dumps({"size": 100}), content_type="application/json")
        states = res.json["data"]
        assert len(states) == n
        # check integrity
        for state in states:
            assert state in random_states

    def test_request_cancel(self, stub_broker, cancel_backend):
        with app.test_client() as client:
            message_id = "1"
            stub_broker.add_middleware(Cancel(backend=cancel_backend))
            client.post(f"/messages/cancel/{message_id}")
            assert cancel_backend.is_canceled(message_id, None)

    def test_request_cancel_without_backend(self, stub_broker, api_client):
        res = api_client.post("/messages/cancel/{}".format("id"))
        # if there is not cancel backend should raise an error
        assert res.json["error"] == "The default broker doesn't have a cancel backend."
        assert res.status_code == 500

    def test_no_scheduler(self, stub_broker, api_client):
        set_scheduler(None)
        res = api_client.get("/scheduled/jobs")
        assert res.status_code == 400

    def test_scheduled_jobs(self, scheduler, api_client, do_work, frozen_datetime):
        timezone = pytz.timezone("Europe/Paris")
        scheduler.schedule = [
            ScheduledJob(
                actor_name=do_work.actor_name, daily_time=(datetime.datetime.now(timezone)).time(), tz="Europe/Paris"
            )
        ]
        scheduler.sync_config()

        res = api_client.get("/scheduled/jobs")
        jobs = res.json["result"]
        assert jobs == [
            {
                "hash": jobs[0]["hash"],
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
            "actor_name": "do_work",
            "delay": delay,
        }
        res = api_client.post("/messages", data=json.dumps(data), content_type="application/json")
        validation_error = res.json["error"]
        assert validation_error["delay"] == [error]
        assert res.status_code == 400

    def test_enqueue_invalid_actor_name(self, stub_broker, api_client):
        data = {"actor_name": "invalid_actor"}
        res = api_client.post("/messages", data=json.dumps(data), content_type="application/json")

        assert res.status_code == 400

    def test_get_declared_actors(self, stub_broker, do_work, api_client):
        @remoulade.actor(queue_name="foo", priority=10)
        def do_job():
            pass

        stub_broker.declare_actor(do_job)
        res = api_client.get("/actors")
        expected = [
            {"name": "do_work", "priority": 0, "queue_name": "default"},
            {"name": "do_job", "priority": 10, "queue_name": "foo"},
        ].sort(key=itemgetter("name"))
        assert res.json["result"].sort(key=itemgetter("name")) == expected

    def test_filter_messages(self, stub_broker, api_client):
        state = State("some_message_id")
        state_backend = stub_broker.get_state_backend()
        state_backend.set_state(state, ttl=1000)
        data = {"sort_column": "message_id", "selected_message_ids": ["some_message_id"]}
        res = api_client.post("/messages/states", data=json.dumps(data), content_type="application/json")
        assert res.json == {"count": 1, "data": [state.as_dict()]}

    @pytest.mark.parametrize("offset", [0, 1, 5, 100])
    def test_get_states_offset(self, offset, stub_broker, api_client, state_middleware):
        for i in range(0, 10):
            state_middleware.backend.set_state(State(f"id{i}"), ttl=1000)
        res = api_client.post(
            "/messages/states", data=json.dumps({"offset": offset, "size": 50}), content_type="application/json"
        )
        if offset >= 10:
            assert res.json["data"] == []
        else:
            assert len(res.json["data"]) + offset == 10

    @pytest.mark.parametrize("size", [1, 5, 100])
    def test_get_states_page_size(self, size, stub_broker, api_client, state_middleware):
        for i in range(0, 10):
            state_middleware.backend.set_state(State("id{}".format(i)), ttl=1000)
        res = api_client.post("/messages/states", data=json.dumps({"size": size}), content_type="application/json")
        if size >= 10:
            assert res.json["count"] == 10
        else:
            assert len(res.json["data"]) == size

    def test_not_raise_error_with_pickle_and_non_serializable(
        self, pickle_encoder, stub_broker, api_client, state_middleware
    ):
        state = State("id1", args=["some_status", date(2020, 12, 12)])
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.post("messages/states")
        assert res.json == {
            "count": 1,
            "data": [{"args": ["some_status", "Sat, 12 Dec 2020 00:00:00 GMT"], "message_id": "id1"}],
        }

    def test_requeue_message(self, stub_broker, do_work, api_client, state_middleware):
        stub_broker.enqueue = MagicMock()
        state = State("id1", StateStatusesEnum.Success, actor_name="do_work", options={"time_limit": 1000})
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.post("messages/requeue/id1")
        message = Message(
            queue_name="default",
            actor_name="do_work",
            args=(),
            kwargs={},
            options={"time_limit": 1000},
            message_id=mock.ANY,
            message_timestamp=mock.ANY,
        )
        assert stub_broker.enqueue.call_count == 1
        assert stub_broker.enqueue.call_args == mock.call(message, delay=None)
        assert res.status_code == 200

    def test_requeue_invalid_id(self, stub_broker, api_client, state_middleware):
        res = api_client.post("messages/requeue/invalid_id")
        assert res.status_code == 400

    def test_requeue_message_with_pipeline(self, stub_broker, do_work, api_client, state_middleware):
        stub_broker.enqueue = MagicMock()
        state = State("id1", StateStatusesEnum.Success, actor_name="do_work", options={"pipe_target": "some_pipe"})
        state_middleware.backend.set_state(state, ttl=1000)
        res = api_client.post("messages/requeue/id1")
        assert res.json == {"error": "requeue message in a pipeline not supported"}
        assert res.status_code == 400

    def test_get_result_message(self, stub_broker, stub_worker, api_client, state_middleware, result_middleware):
        @remoulade.actor(store_results=True)
        def do_work():
            return 42

        stub_broker.declare_actor(do_work)
        message = do_work.send()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        res = api_client.get(f"/messages/result/{message.message_id}")
        assert res.json["result"] == "42"

    def test_no_result_backend(self, stub_broker, stub_worker, api_client, do_work):
        message = do_work.send()
        res = api_client.get(f"/messages/result/{message.message_id}")
        assert res.json["result"] == "no result backend"

    def test_result_not_serializable(
        self, pickle_encoder, stub_broker, stub_worker, api_client, state_middleware, result_middleware
    ):
        @remoulade.actor(store_results=True)
        def do_work():
            return {"date": date(2020, 10, 10)}

        stub_broker.declare_actor(do_work)
        message = do_work.send()
        stub_broker.join(do_work.queue_name)
        stub_worker.join()
        res = api_client.get(f"/messages/result/{message.message_id}")
        assert res.json["result"] == "non serializable result"

    def test_select_actors(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        for i in range(2):
            backend.set_state(State(f"id{i}", actor_name=f"actor{i}"))
        res = backend.get_states(selected_actors=["actor1"])
        assert len(res) == 1
        assert res[0].actor_name == "actor1"

    def test_select_statuses(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        for i in range(2):
            backend.set_state(State(f"id{i}", StateStatusesEnum.Success if i else StateStatusesEnum.Skipped))
        res = backend.get_states(selected_statuses=["Success"])
        assert len(res) == 1
        assert res[0].status == StateStatusesEnum.Success

    def test_select_ids(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        for i in range(2):
            backend.set_state(State(f"id{i}"))
        res = backend.get_states(selected_message_ids=["id1"])
        assert len(res) == 1
        assert res[0].message_id == "id1"

    def test_select_datetimes(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        for i in range(2):
            backend.set_state(State(f"id{i}", enqueued_datetime=parse(f"2020-08-08 1{i}:00:00")))

        res = backend.get_states(start_datetime=parse("2020-08-08 10:30:00"))
        assert len(res) == 1
        assert res[0].message_id == "id1"

        res = backend.get_states(end_datetime=parse("2020-08-08 10:30:00"))
        assert len(res) == 1
        assert res[0].message_id == "id0"

    def test_clean(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        backend.set_state(State("id0"))

        assert len(backend.get_states()) == 1
        backend.clean()
        assert len(backend.get_states()) == 0

    def test_clean_max_age(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        backend.set_state(State("id0", end_datetime=datetime.datetime.now(pytz.utc)))
        backend.set_state(State("id1", end_datetime=datetime.datetime.now(pytz.utc) - datetime.timedelta(minutes=50)))

        assert len(backend.get_states()) == 2
        backend.clean(max_age=25)
        res = backend.get_states()
        assert len(res) == 1
        assert res[0].message_id == "id0"

    def test_clean_not_started(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        backend.set_state(State("id0", started_datetime=datetime.datetime.now(pytz.utc)))
        backend.set_state(State("id1"))

        assert len(backend.get_states()) == 2
        backend.clean(not_started=True)
        res = backend.get_states()
        assert len(res) == 1
        assert res[0].message_id == "id0"

    def test_clean_route(self, stub_broker, postgres_state_middleware):
        client = app.test_client()
        backend = postgres_state_middleware.backend
        backend.set_state(State("id0"))

        assert len(backend.get_states()) == 1
        client.delete("/messages/states")
        assert len(backend.get_states()) == 0

    def test_cant_sort_by_args_kwargs_options(self, stub_broker, state_middleware, api_client):
        res = api_client.post(
            "/messages/states", data=json.dumps({"sort_column": "args"}), content_type="application/json"
        )
        assert res.status_code == 400
        res = api_client.post(
            "/messages/states", data=json.dumps({"sort_column": "kwargs"}), content_type="application/json"
        )
        assert res.status_code == 400
        res = api_client.post(
            "/messages/states", data=json.dumps({"sort_column": "options"}), content_type="application/json"
        )
        assert res.status_code == 400
