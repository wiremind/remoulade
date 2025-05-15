import remoulade
from remoulade.brokers.stub import StubBroker

broker = StubBroker()
remoulade.set_broker(broker)
