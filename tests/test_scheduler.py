import datetime
import threading
import time

import pytest
import pytz

import remoulade
from remoulade.scheduler import ScheduledJob
from tests.conftest import check_redis, new_scheduler


def test_simple_interval_scheduler(stub_broker, stub_worker, scheduler, scheduler_thread, mul, add):

    result = 0

    @remoulade.actor()
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)
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

    time.sleep(3)

    stub_broker.join(mul.queue_name)
    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    end = time.time()
    # should have written ~1 line per second
    assert end - start - 1 <= result <= end - start + 1

    # get the last_queued date for this slow task, this should not change when reloading schedule with new config
    tasks = scheduler.get_redis_schedule().values()
    (slow_task,) = [job for job in tasks if job.actor_name == "mul"]
    last_queued = slow_task.last_queued
    assert {j.actor_name for j in tasks} == {"mul", "write_loaded_at"}

    scheduler.schedule = [
        ScheduledJob(actor_name="add", kwargs={"x": 1, "y": 2}, interval=1),
        ScheduledJob(actor_name="mul", kwargs={"x": 1, "y": 2}, interval=3600),
    ]
    scheduler.sync_config()

    time.sleep(1)

    tasks = scheduler.get_redis_schedule().values()
    # One item was deleted
    assert {j.actor_name for j in tasks} == {"add", "mul"}
    # The other one was not updated
    (slow_task,) = [job for job in tasks if job.actor_name == "mul"]
    assert slow_task.last_queued == last_queued


def test_multiple_schedulers(stub_broker, stub_worker):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)

    scheduler_list = []

    for _ in range(5):
        sch = new_scheduler(stub_broker)
        if not scheduler_list:
            check_redis(sch.client)
        sch.schedule = [
            ScheduledJob(
                actor_name="write_loaded_at",
                interval=3600,
            )
        ]
        scheduler_list.append(sch)
        threading.Thread(target=sch.start).start()

    time.sleep(0.2)

    stub_broker.join(write_loaded_at.queue_name)
    stub_worker.join()

    # slow task should run exactly once, even if we launched 2 schedulers
    assert result == 1

    for scheduler in scheduler_list:
        scheduler.stop()


@pytest.mark.parametrize("tz", [True, False])
def test_scheduler_daily_time(stub_broker, stub_worker, scheduler, scheduler_thread, tz):
    result = 0

    @remoulade.actor
    def write_loaded_at():
        nonlocal result
        result += 1

    stub_broker.declare_actor(write_loaded_at)

    if tz:
        scheduler.schedule = [
            ScheduledJob(
                actor_name="write_loaded_at",
                daily_time=(
                    datetime.datetime.now(pytz.timezone("Europe/Paris")) + datetime.timedelta(seconds=2)
                ).time(),
                tz="Europe/Paris",
            )
        ]
    else:
        scheduler.schedule = [
            ScheduledJob(
                actor_name="write_loaded_at",
                daily_time=(datetime.datetime.utcnow() + datetime.timedelta(seconds=2)).time(),
            )
        ]
    scheduler_thread.start()

    time.sleep(1)

    # should not have run yet
    assert result == 0

    # should run now
    time.sleep(1.2)

    assert result == 1

    time.sleep(0.5)

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
    scheduler_thread.start()
    time.sleep(1)

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
    scheduler_thread.start()

    time.sleep(0.1)

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
    scheduler_thread.start()
    time.sleep(0.1)

    # Should have ran
    assert result == 1
