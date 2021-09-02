import random
import sys
import time

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker()
remoulade.set_broker(broker)


def fib(n):
    x, y = 1, 1
    while n > 2:
        x, y = x + y, x
        n -= 1
    return x


@remoulade.actor(time_limit=86400000)
def long_running(duration):
    deadline = time.monotonic() + duration
    while time.monotonic() < deadline:
        long_running.logger.info("%d seconds remaining.", deadline - time.monotonic())

        n = random.randint(1000, 1000000)
        long_running.logger.info("Computing fib(%d).", n)

        fib(n)
        long_running.logger.info("Computed fib(%d).", n)

        sleep = random.randint(1, 30)
        long_running.logger.info("Sleeping for %d seconds...", sleep)
        time.sleep(sleep)


broker.declare_actor(long_running)


def main():
    for _ in range(1000):
        long_running.send(random.randint(3600, 14400))
        time.sleep(random.randint(60, 3600))


if __name__ == "__main__":
    sys.exit(main())
