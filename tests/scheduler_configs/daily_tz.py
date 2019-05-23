import datetime

import pytz

import remoulade
from remoulade.scheduler import Scheduler, ScheduledJob
from remoulade.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker(max_priority=10)
remoulade.set_broker(broker)
timezone = pytz.timezone("Europe/Paris")
remoulade.set_scheduler(
    Scheduler(broker,
              [
                  ScheduledJob(
                      actor_name="write_loaded_at",
                      kwargs={"filename": "/tmp/scheduler-daily_tz", "text": "simple schedule\n"},
                      daily_time=(datetime.datetime.now(timezone) + datetime.timedelta(seconds=5)).time(),
                      tz="Europe/Paris"
                  )
              ], period=0.1)
)


@remoulade.actor()
def write_loaded_at(filename, text):
    with open(filename, "a+") as f:
        f.write(text)


broker.declare_actor(write_loaded_at)
