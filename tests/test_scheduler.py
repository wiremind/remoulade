import datetime
import json
import threading
import time

import pytest
import pytz

import remoulade
from remoulade.scheduler import ScheduledJob
from tests.conftest import check_redis, mock_func, new_scheduler


def test_simple_interval_scheduler(stub_broker, stub_worker, scheduler, scheduler_thread, mul, add):
    result = 0

    @remoulade.actor()
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)
    write_loaded_at.send, event_write = mock_func(write_loaded_at.send)
    mul.send, event_mul = mock_func(mul.send)
    start = time.time()

    # Run scheduler
    scheduler.schedule = [
        ScheduledJob(
            actor_name="write_loaded_at",
            interval=1,
        ),
        ScheduledJob(actor_name="mul", kwargs={"x": 1, "y": 2}, interval=3600),
    ]
    scheduler_thread.start()

    event_write.wait(10)
    event_mul.wait(10)

    stub_broker.join(mul.queue_name)
    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    end = time.time()
    # should have written ~1 line per second
    assert end - start - 1 <= result <= end - start + 1

    # get the last_queued date for this slow task, this should not change when reloading schedule with new config
    tasks = scheduler.get_redis_schedule().values()
    (slow_task,) = (job for job in tasks if job.actor_name == "mul")
    last_queued = slow_task.last_queued
    assert {j.actor_name for j in tasks} == {"mul", "write_loaded_at"}

    scheduler.schedule = [
        ScheduledJob(actor_name="add", kwargs={"x": 1, "y": 2}, interval=1),
        ScheduledJob(actor_name="mul", kwargs={"x": 1, "y": 2}, interval=3600),
    ]
    scheduler.sync_config()

    tasks = scheduler.get_redis_schedule().values()
    # One item was deleted
    assert {j.actor_name for j in tasks} == {"add", "mul"}
    # The other one was not updated
    (slow_task,) = (job for job in tasks if job.actor_name == "mul")
    assert slow_task.last_queued == last_queued


def test_multiple_schedulers(stub_broker, stub_worker):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)
    schedule = [
        ScheduledJob(
            actor_name="write_loaded_at",
            interval=3600,
        )
    ]

    scheduler_list = []
    event_list = []
    thread_list = []

    for _ in range(5):
        sch = new_scheduler(stub_broker)
        sch.get_redis_schedule, event = mock_func(sch.get_redis_schedule)
        event_list.append(event)
        if not scheduler_list:
            check_redis(sch.client)
        sch.schedule = schedule
        scheduler_list.append(sch)
        t = threading.Thread(target=sch.start)
        thread_list.append(t)
        t.start()

    for _ in range(2):
        for event in event_list:
            event.wait(2)
            event.clear()

    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    # slow task should run exactly once, even if we launched 2 schedulers
    assert result == 1

    for scheduler in scheduler_list:
        scheduler.stop()

    for thread in thread_list:
        thread.join(10)


@pytest.mark.parametrize("tz", [None, "Europe/Paris"])
def test_scheduler_daily_time(stub_broker, stub_worker, scheduler, scheduler_thread, tz, frozen_datetime):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)
    scheduler.get_redis_schedule, event_sch = mock_func(scheduler.get_redis_schedule)
    write_loaded_at.send, event_send = mock_func(write_loaded_at.send)

    scheduler.schedule = [
        ScheduledJob(
            actor_name="write_loaded_at",
            daily_time=(
                datetime.datetime.now(pytz.timezone(tz) if tz else None) + datetime.timedelta(seconds=1)
            ).time(),
            tz=tz,
        )
    ]
    scheduler_thread.start()
    # Wait for sync_config + a complete scheduler iteration
    for _ in range(3):
        event_sch.wait(10)
        event_sch.clear()
    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()
    # should not have run yet
    assert result == 0
    frozen_datetime.tick(2)
    # Wait for the ScheduledJob to be sent
    event_send.wait(1)
    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()
    assert result == 1

    # Wait for a complete scheduler iteration
    for _ in range(2):
        event_sch.wait(10)
        event_sch.clear()
    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    # should not rerun
    assert result == 1


def test_scheduler_new_daily_time(stub_broker, stub_worker, scheduler, scheduler_thread):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)

    scheduler.schedule = [
        ScheduledJob(
            actor_name="write_loaded_at",
            daily_time=(datetime.datetime.utcnow() - datetime.timedelta(seconds=1)).time(),
        )
    ]
    scheduler.get_redis_schedule, event = mock_func(scheduler.get_redis_schedule)
    scheduler_thread.start()
    event.wait(3)
    event.clear()
    event.wait(2)

    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    # should not have ran, will run tomorrow
    assert result == 0


def test_scheduler_wrong_weekday(stub_broker, stub_worker, scheduler, scheduler_thread):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)

    scheduler.schedule = [
        ScheduledJob(
            actor_name="write_loaded_at",
            iso_weekday=datetime.datetime.now().isoweekday() + 1,
        )
    ]
    scheduler.get_redis_schedule, event = mock_func(scheduler.get_redis_schedule)
    scheduler_thread.start()

    event.wait(2)
    event.clear()
    event.wait(2)

    # do nothing
    assert result == 0


def test_scheduler_right_weekday(stub_broker, stub_worker, scheduler, scheduler_thread):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)

    scheduler.schedule = [
        ScheduledJob(
            actor_name="write_loaded_at",
            iso_weekday=datetime.datetime.now().isoweekday(),
        )
    ]
    write_loaded_at.send, event = mock_func(write_loaded_at.send)
    scheduler_thread.start()
    event.wait(2)
    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    # Should have ran
    assert result == 1


def test_add_job(scheduler):
    job = ScheduledJob(actor_name="do_work")
    scheduler.add_job(job)
    hashes = list(scheduler.get_redis_schedule().keys())
    assert len(hashes) == 1
    assert hashes[0] == job.get_hash()


def test_delete_job(scheduler):
    scheduler.schedule = [ScheduledJob(actor_name="do_work")]
    scheduler.sync_config()
    scheduler.delete_job(scheduler.schedule[0].get_hash())
    assert scheduler.get_redis_schedule() == {}


def test_get_scheduled_jobs(scheduler, api_client):
    scheduler.schedule = [ScheduledJob(actor_name="do_work")]
    scheduler.sync_config()
    res = api_client.get("/scheduled/jobs")
    assert res.status_code == 200
    assert res.json == {
        "result": [
            {
                "hash": scheduler.schedule[0].get_hash(),
                "actor_name": "do_work",
                "args": [],
                "daily_time": None,
                "enabled": True,
                "interval": 86400,
                "iso_weekday": None,
                "kwargs": {},
                "last_queued": None,
                "tz": "UTC",
            }
        ]
    }


def test_api_add_job(scheduler, api_client, do_work):
    res = api_client.post(
        "/scheduled/jobs", data=json.dumps({"actor_name": "do_work"}), content_type="application/json"
    )
    assert res.status_code == 200
    assert res.json == {
        "result": [
            {
                "hash": res.json["result"][0]["hash"],
                "actor_name": "do_work",
                "args": [],
                "daily_time": None,
                "enabled": True,
                "interval": 86400,
                "iso_weekday": None,
                "kwargs": {},
                "last_queued": None,
                "tz": "UTC",
            }
        ]
    }


def test_api_delete_job(scheduler, api_client):
    scheduler.schedule = [ScheduledJob(actor_name="do_work")]
    scheduler.sync_config()
    res = api_client.delete(f"/scheduled/jobs/{scheduler.schedule[0].get_hash()}")
    assert res.status_code == 200
    assert res.json == {"result": []}


def test_update_job(scheduler, api_client, do_work):
    scheduler.schedule = [ScheduledJob(actor_name="do_work")]
    scheduler.sync_config()
    res = api_client.put(
        f"/scheduled/jobs/{scheduler.schedule[0].get_hash()}",
        data=json.dumps({"actor_name": "do_work", "enabled": False}),
        content_type="application/json",
    )
    assert res.status_code == 200
    assert res.json == {
        "result": [
            {
                "hash": res.json["result"][0]["hash"],
                "actor_name": "do_work",
                "args": [],
                "daily_time": None,
                "enabled": False,
                "interval": 86400,
                "iso_weekday": None,
                "kwargs": {},
                "last_queued": None,
                "tz": "UTC",
            }
        ]
    }


def test_api_update_jobs(scheduler, api_client, do_work):
    scheduler.schedule = [ScheduledJob(actor_name="do_work"), ScheduledJob(actor_name="do_other_work")]
    scheduler.sync_config()
    res = api_client.put(
        "/scheduled/jobs",
        data=json.dumps(
            {
                "jobs": {
                    scheduler.schedule[0].get_hash(): {"actor_name": "do_work", "enabled": False},
                    scheduler.schedule[1].get_hash(): {"actor_name": "do_work", "enabled": False, "interval": "55"},
                }
            }
        ),
        content_type="application/json",
    )
    assert res.status_code == 200
    assert len(res.json["result"]) == 2
    for i in range(2):
        assert not res.json["result"][i]["enabled"]


def test_daily_time_wrong_interval(scheduler, api_client):
    res = api_client.post(
        "/scheduled/jobs",
        data=json.dumps({"actor_name": "do_work", "daily_time": "00:00", "interval": 1000}),
        content_type="application/json",
    )
    assert res.status_code == 400


def test_invalid_timezone(scheduler, api_client):
    res = api_client.post(
        "/scheduled/jobs",
        data=json.dumps({"actor_name": "do_work", "tz": "invalid_tz"}),
        content_type="application/json",
    )
    assert res.status_code == 400


def test_invalid_actor_name(scheduler, api_client):
    res = api_client.post(
        "/scheduled/jobs", data=json.dumps({"actor_name": "invalid_actor"}), content_type="application/json"
    )
    assert res.status_code == 400


def test_tz_aware_last_queued(scheduler, api_client, do_work):
    res = api_client.post(
        "scheduled/jobs",
        data=json.dumps({"actor_name": "do_work", "last_queued": "2020-10-10 10:00:00Z"}),
        content_type="application/json",
    )

    assert res.status_code == 400
