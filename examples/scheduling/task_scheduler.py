"""
This file shows the task that you want to schedule
count_words of github.com each second
count_words of gitlab.com each 10 seconds
"""
import requests

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.scheduler import ScheduledJob, Scheduler

broker = RabbitmqBroker()

remoulade.set_broker(broker)
remoulade.set_scheduler(
    Scheduler(
        broker,
        [
            ScheduledJob(actor_name="count_words", kwargs={"url": "https://github.com"}, interval=1),
            ScheduledJob(actor_name="count_words", kwargs={"url": "https://gitlab.com"}, interval=10),
        ],
        period=0.1,  # scheduler run each 0.1 (second)
    )
)


@remoulade.actor()
def count_words(url):
    response = requests.get(url).text
    print("There are {} in {}".format(len(response), url))


broker.declare_actor(count_words)
