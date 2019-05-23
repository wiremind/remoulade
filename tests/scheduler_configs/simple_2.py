import remoulade
from remoulade.scheduler import Scheduler, ScheduledJob
from remoulade.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker(max_priority=10)
remoulade.set_broker(broker)
remoulade.set_scheduler(
    Scheduler(broker,
              [
                  ScheduledJob(
                      actor_name="add", kwargs={"x": 1, "y": 2},
                      interval=1
                  ),
                  ScheduledJob(
                      actor_name="mul", kwargs={"x": 1, "y": 2}, interval=3600
                  )
              ], period=0.1)
)


@remoulade.actor()
def add(x, y):
    return x + y


@remoulade.actor()
def mul(x, y):
    return x * y


broker.declare_actor(add)
broker.declare_actor(mul)
