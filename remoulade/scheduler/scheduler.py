import datetime
import json
import time
from typing import Dict, List, Union

import pytz
import redis

from remoulade import Broker, get_encoder, get_logger

DEFAULT_JOB_INTERVAL = 3600 * 24
DEFAULT_JOB_STATUS = True
DEFAULT_TZ = "UTC"

DEFAULT_SCHEDULER_NAMESPACE = "remoulade-schedule"
DEFAULT_SCHEDULER_LOCK_KEY = "remoulade-scheduler-tick"
DEFAULT_SCHEDULER_PERIOD = 1


class ScheduledJob:
    def __init__(
        self,
        actor_name: str,
        args: List = None,
        kwargs: Dict = None,
        interval: int = None,
        daily_time: datetime.time = None,
        iso_weekday: int = None,
        enabled: bool = None,
        last_queued: datetime.datetime = None,
        tz: str = None,
    ) -> None:
        """
        Describes a job that should be run through the scheduler
        :param actor_name: The name of the actor that should be ran
        :param args: args that should be passed
        :param kwargs: kwargs that should be passed
        :param interval: at which interval (in seconds) this should run
        :param daily_time: at which time of day this should run (this assumes interval=24H)
        :param iso_weekday: the isoweekday this should be running at
        :param enabled: allows to disable jobs
        :param last_queued: when is the last time this job was run through the scheduler ?
        you should not be setting this yourself
        :param tz: timezone your daily_time is expressed in
        """
        if (daily_time is not None and daily_time.tzinfo is not None) or (
            last_queued is not None and last_queued.tzinfo is not None
        ):
            raise ValueError("Datetimes should be naive")
        if daily_time is not None and interval not in (3600 * 24, None):
            raise ValueError("daily_time should be used with a 24h interval")
        self.actor_name = actor_name
        self.interval = interval if interval is not None else DEFAULT_JOB_INTERVAL
        self.daily_time = daily_time
        self.iso_weekday = iso_weekday
        self.enabled = enabled if enabled is not None else DEFAULT_JOB_STATUS
        self.last_queued = last_queued
        self.tz = tz if tz is not None else DEFAULT_TZ
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}

    def get_hash(self) -> str:
        d = self.as_dict()
        args = json.dumps(d["args"])
        kwargs = json.dumps(sorted(list(d["kwargs"].items()), key=lambda x: x[0]))

        path = [str(d[k]) for k in ("actor_name", "interval", "daily_time", "iso_weekday", "enabled", "tz")] + [
            args,
            kwargs,
        ]

        return " ".join(path)

    def as_dict(self) -> Dict:
        return {
            "actor_name": self.actor_name,
            "interval": self.interval,
            "daily_time": None if self.daily_time is None else self.daily_time.strftime("%H:%M:%S"),
            "iso_weekday": self.iso_weekday,
            "enabled": self.enabled,
            "last_queued": None if self.last_queued is None else self.last_queued.strftime("%Y-%m-%dT%H:%M:%S"),
            "tz": self.tz,
            "args": self.args,
            "kwargs": self.kwargs,
        }

    def encode(self) -> bytes:
        return get_encoder().encode(self.as_dict())

    @classmethod
    def decode(cls, data: bytes) -> "ScheduledJob":
        data = get_encoder().decode(data)
        return ScheduledJob(
            actor_name=data["actor_name"],
            interval=data["interval"],
            daily_time=None
            if data["daily_time"] is None
            else datetime.datetime.strptime(data["daily_time"], "%H:%M:%S").time(),
            iso_weekday=data["iso_weekday"],
            enabled=data["enabled"],
            last_queued=None
            if data["last_queued"] is None
            else datetime.datetime.strptime(data["last_queued"], "%Y-%m-%dT%H:%M:%S"),
            tz=data["tz"],
            args=data["args"],
            kwargs=data["kwargs"],
        )


class Scheduler:
    def __init__(
        self,
        broker: Broker,
        schedule: List[ScheduledJob],
        *,
        namespace: str = None,
        lock_key: str = None,
        period: Union[int, float] = None,
        client: redis.Redis = None,
        url: str = None,
        **redis_parameters,
    ) -> None:
        """
        The class that takes care of scheduling tasks
        :param broker: The broker for the remoulade app
        :param schedule: A map that gives the schedule you want to be running
        :param namespace: Where we should store our scheduled tasks on redis
        :param lock_key: The redis key that we use as a lock to make sure only one scheduler is working at a time
        :param period: At which period (in seconds) the scheduler should be running, default is 1 second
        :param client: The redis client we should use to access redis
        :param url: The URL of the redis client
        :param redis_parameters: Additional params to pass to redis
        """
        self.schedule = schedule
        self.namespace = namespace if namespace is not None else DEFAULT_SCHEDULER_NAMESPACE
        self.period = period if period is not None else DEFAULT_SCHEDULER_PERIOD
        self.broker = broker
        self.lock_key = lock_key if lock_key is not None else DEFAULT_SCHEDULER_LOCK_KEY

        if url:
            redis_parameters["connection_pool"] = redis.ConnectionPool.from_url(url)

        self.client = client or redis.Redis(**redis_parameters)
        self.logger = get_logger(__name__, type(self))

    def flush(self, job: ScheduledJob) -> None:
        self.client.hset(self.namespace, job.get_hash().encode("utf-8"), job.encode())

    def get_redis_schedule(self) -> Dict[str, ScheduledJob]:
        r = {}
        for json_schedule in self.client.hgetall(self.namespace).values():
            job = ScheduledJob.decode(json_schedule)
            r[job.get_hash()] = job
        return r

    def sync_config(self) -> None:
        redis_schedule = self.get_redis_schedule()
        config_schedule = {job.get_hash(): job for job in self.schedule}

        # loop over redis schedule
        for job_hash in redis_schedule.keys():
            # if job is no longer in configured schedule, remove it
            if job_hash not in config_schedule:
                self.logger.info("Dropping %s because it's no longer in the config", job_hash)
                self.client.hdel(self.namespace, job_hash.encode("utf-8"))

        # add newly configured jobs
        for job_hash, job in config_schedule.items():
            if job_hash not in redis_schedule:
                # Do not queue task if daily time is already passed
                if job.daily_time is not None and job.daily_time < datetime.datetime.now(pytz.timezone(job.tz)).time():
                    self.logger.info(
                        "Will not run %s today, because daily time has already passed. Wait for tomorrow", job_hash
                    )
                    job.last_queued = datetime.datetime.utcnow()
                # Add to redis
                self.logger.info("Adding new job %s to schedule", job_hash)
                self.flush(job)

    def start(self):
        self.logger.debug("Syncing config")
        self.sync_config()

        while True:
            with self.client.lock(self.lock_key, timeout=10, blocking_timeout=20):
                schedule = self.get_redis_schedule()
                for job_hash, job in schedule.items():
                    now = datetime.datetime.now(pytz.timezone(job.tz))
                    now_utc = datetime.datetime.utcnow()
                    curr_isodow = now.isoweekday()

                    if not job.enabled:
                        continue

                    # Tasks that are supposed to run at a specific time/day
                    if job.iso_weekday is not None or job.daily_time is not None:
                        if job.iso_weekday is not None and job.iso_weekday != curr_isodow:
                            continue

                        if job.daily_time is not None and now.time() < job.daily_time:
                            continue

                        if job.last_queued is not None:
                            # if task already ran today, skip it
                            last_queued_date = (
                                job.last_queued.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone(job.tz)).date()
                            )
                            if now.date() == last_queued_date:
                                continue
                    # Task that should run each X seconds
                    else:
                        if job.last_queued is not None and (
                            datetime.timedelta(seconds=0)
                            < now_utc - job.last_queued
                            < datetime.timedelta(seconds=job.interval)
                        ):
                            continue

                    if job.actor_name not in self.broker.actors:
                        self.logger.error(f"Unknown {job_hash}. Cannot queue it.")
                        continue

                    self.broker.actors[job.actor_name].send(*job.args, **job.kwargs)

                    job.last_queued = datetime.datetime.utcnow()
                    self.flush(job)

            time.sleep(self.period)
