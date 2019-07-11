import os
import time

import pytest
import redis

from remoulade.scheduler import ScheduledJob

# For this to work you need to make sure:
# - your redis is empty (flushall)
# - There are no /tmp/schedul* files


@pytest.mark.skipif(os.getenv("TRAVIS") == "1", reason="test skipped on Travis")
def test_simple_interval_scheduler(start_scheduler, start_cli):
    from tests.scheduler_configs.simple_1 import broker, write_loaded_at
    client = redis.Redis()

    start = time.time()

    # Run scheduler
    sch = start_scheduler("tests.scheduler_configs.simple_1")

    # And worker
    start_cli("tests.scheduler_configs.simple_1", extra_args=[
        "--processes", "1",
        "--threads", "1",
        "--watch", "tests"
    ])

    time.sleep(3)

    broker.join(write_loaded_at.queue_name)

    with open("/tmp/scheduler-1", "r") as f:
        text = [x for x in f.read().split("\n") if x]

    end = time.time()

    # should have written ~1 line per second
    assert end - start - 2 <= len(text) <= end - start + 5

    sch.terminate()
    sch.wait()

    # get the last_queued date for this slow task, this should not change when reloading schedule with new config
    tasks = [ScheduledJob.decode(v) for v in client.hgetall("remoulade-schedule").values()]
    slow_task, = [job for job in tasks if job.actor_name == "mul"]
    last_queued = slow_task.last_queued
    assert {j.actor_name for j in tasks} == {"mul", "write_loaded_at"}

    start_scheduler("tests.scheduler_configs.simple_2")

    time.sleep(1)

    tasks = [ScheduledJob.decode(v) for v in client.hgetall("remoulade-schedule").values()]
    # One item was deleted
    assert {j.actor_name for j in tasks} == {"add", "mul"}
    # The other one was not updated
    slow_task, = [job for job in tasks if job.actor_name == "mul"]
    assert slow_task.last_queued == last_queued


@pytest.mark.skipif(os.getenv("TRAVIS") == "1", reason="test skipped on Travis")
def test_multiple_schedulers(start_scheduler, start_cli):
    from tests.scheduler_configs.simple_1 import broker, write_loaded_at

    # we launch 5 schedulers
    for _ in range(5):
        start_scheduler("tests.scheduler_configs.simple_slow")

    start_cli("tests.scheduler_configs.simple_slow", extra_args=[
        "--processes", "1",
        "--threads", "1",
        "--watch", "tests"
    ])

    time.sleep(5)

    broker.join(write_loaded_at.queue_name)

    with open("/tmp/scheduler-2", "r") as f:
        text = [x for x in f.read().split("\n") if x]

    # slow task should run exactly once, even if we launched 2 schedulers
    assert len(text) == 1


@pytest.mark.skipif(os.getenv("TRAVIS") == "1", reason="test skipped on Travis")
@pytest.mark.parametrize("suffix", ["", "_tz"])
def test_scheduler_daily_time(start_scheduler, start_cli, suffix):
    start_scheduler("tests.scheduler_configs.daily{}".format(suffix))

    start_cli("tests.scheduler_configs.daily{}".format(suffix), extra_args=[
        "--processes", "1",
        "--threads", "1",
        "--watch", "tests"
    ])

    time.sleep(3)

    fpath = "/tmp/scheduler-daily{}".format(suffix)

    # should not have run yet
    with pytest.raises(FileNotFoundError):
        with open(fpath, "r"):
            ...

    # should run now
    time.sleep(5)

    with open(fpath, "r") as f:
        text = [x for x in f.read().split("\n") if x]

    assert len(text) == 1

    time.sleep(2)

    # should not rerun
    with open(fpath, "r") as f:
        text = [x for x in f.read().split("\n") if x]

    assert len(text) == 1


@pytest.mark.skipif(os.getenv("TRAVIS") == "1", reason="test skipped on Travis")
def test_scheduler_new_daily_time(start_scheduler, start_cli):
    start_scheduler("tests.scheduler_configs.daily")

    start_cli("tests.scheduler_configs.daily_new", extra_args=[
        "--processes", "1",
        "--threads", "1",
        "--watch", "tests"
    ])

    time.sleep(10)

    # should not have ran, will run tomorrow
    with pytest.raises(FileNotFoundError):
        with open("/tmp/scheduler-daily-new", "r"):
            ...


@pytest.mark.skipif(os.getenv("TRAVIS") == "1", reason="test skipped on Travis")
def test_scheduler_wrong_weekday(start_scheduler, start_cli):
    start_scheduler("tests.scheduler_configs.weekday_1")

    start_cli("tests.scheduler_configs.weekday_1", extra_args=[
        "--processes", "1",
        "--threads", "1",
        "--watch", "tests"
    ])

    time.sleep(3)

    # do nothing
    with pytest.raises(FileNotFoundError):
        with open("/tmp/scheduler-weekday-1", "r"):
            ...


@pytest.mark.skipif(os.getenv("TRAVIS") == "1", reason="test skipped on Travis")
def test_scheduler_right_weekday(start_scheduler, start_cli):
    start_scheduler("tests.scheduler_configs.weekday_2")

    start_cli("tests.scheduler_configs.weekday_2", extra_args=[
        "--processes", "1",
        "--threads", "1",
        "--watch", "tests"
    ])

    time.sleep(3)

    with open("/tmp/scheduler-weekday-2", "r") as f:
        text = [x for x in f.read().split("\n") if x]

    # Should have ran
    assert len(text) == 1
