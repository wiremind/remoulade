import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.scheduler import ScheduledJob, Scheduler

broker = RabbitmqBroker(max_priority=10)
remoulade.set_broker(broker)
remoulade.set_scheduler(
    Scheduler(
        broker,
        [
            ScheduledJob(
                actor_name="write_loaded_at",
                kwargs={"filename": "/tmp/scheduler-2", "text": "simple schedule\n"},
                interval=3600,
            )
        ],
        period=0.1,
    )
)


@remoulade.actor()
def write_loaded_at(filename, text):
    with open(filename, "a+") as f:
        f.write(text)


broker.declare_actor(write_loaded_at)
