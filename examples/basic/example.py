import random
import sys

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker()
remoulade.set_broker(broker)


@remoulade.actor
def add(x, y):
    add.logger.info("The sum of %d and %d is %d.", x, y, x + y)


broker.declare_actor(add)


def main(count):
    count = int(count)
    for _ in range(count):
        add.send(random.randint(0, 1000), random.randint(0, 1000))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
