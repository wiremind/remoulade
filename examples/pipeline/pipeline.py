import remoulade
from remoulade import pipeline
from remoulade.brokers.rabbitmq import RabbitmqBroker

broker = RabbitmqBroker()
remoulade.set_broker(broker)


@remoulade.actor
def add(x, y=0):
    print("sum({},{}) = {}".format(x, y, x + y))
    return x + y


broker.declare_actor(add)

if __name__ == "__main__":
    message = add.send(1, 2)
    g = pipeline((add.message(i) for i in range(0, 11)))
    g.run()
