import logging
import sys
import time

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker()
remoulade.set_broker(broker)


@remoulade.actor(time_limit=5000)
def long_running():
    logger = logging.getLogger("long_running")

    while True:
        logger.info("Sleeping...")
        time.sleep(1)


@remoulade.actor(time_limit=5000)
def long_running_with_catch():
    logger = logging.getLogger("long_running_with_catch")

    try:
        while True:
            logger.info("Sleeping...")
            time.sleep(1)
    except remoulade.middleware.time_limit.TimeLimitExceeded:
        logger.warning("Time limit exceeded. Aborting...")


remoulade.declare_actors([long_running, long_running_with_catch])


def main():
    long_running.send()
    long_running_with_catch.send()


if __name__ == "__main__":
    sys.exit(main())
