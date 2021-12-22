import requests

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.scheduler import ScheduledJob, Scheduler

broker = RabbitmqBroker()
remoulade.set_broker(broker)


@remoulade.actor()
def count_words(url):
    response = requests.get(url).text
    print(f"There are {len(response)} words in {url}")


broker.declare_actor(count_words)

if __name__ == "__main__":
    scheduler = Scheduler(
        broker,
        [
            ScheduledJob(actor_name="count_words", kwargs={"url": "https://github.com"}, interval=1),
            ScheduledJob(actor_name="count_words", kwargs={"url": "https://gitlab.com"}, interval=10),
        ],
        period=0.1,  # scheduler run each 0.1 (second)
    )
    remoulade.set_scheduler(scheduler)
    scheduler.start()
