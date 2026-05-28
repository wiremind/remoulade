import datetime

import pytest
from dateutil.parser import parse

from remoulade.api.main import app
from remoulade.state import State, StateStatusesEnum


@pytest.fixture
def local_api_client():
    with app.test_client() as client:
        yield client


@pytest.mark.parametrize(
    "method,path",
    [
        ("post", "/messages/states"),
        ("get", "/messages/states/some-id"),
        ("delete", "/messages/states"),
        ("post", "/messages/cancel/some-id"),
        ("post", "/messages/requeue/some-id"),
        ("get", "/messages/result/some-id"),
        ("post", "/messages"),
        ("get", "/actors"),
        ("get", "/options"),
        ("get", "/scheduled/jobs"),
        ("post", "/scheduled/jobs"),
        ("put", "/scheduled/jobs/some-id"),
        ("delete", "/scheduled/jobs/some-id"),
    ],
)
def test_superbowl_routes_are_removed(local_api_client, method, path):
    response = getattr(local_api_client, method)(path)
    assert response.status_code == 404


class TestMessageStateBackend:
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
        backend.set_state(State("id0", end_datetime=datetime.datetime.now(datetime.UTC)))
        backend.set_state(
            State("id1", end_datetime=datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=50))
        )

        assert len(backend.get_states()) == 2
        backend.clean(max_age=25)
        res = backend.get_states()
        assert len(res) == 1
        assert res[0].message_id == "id0"

    def test_clean_not_started(self, stub_broker, postgres_state_middleware):
        backend = postgres_state_middleware.backend
        backend.set_state(State("id0", started_datetime=datetime.datetime.now(datetime.UTC)))
        backend.set_state(State("id1"))

        assert len(backend.get_states()) == 2
        backend.clean(not_started=True)
        res = backend.get_states()
        assert len(res) == 1
        assert res[0].message_id == "id0"
