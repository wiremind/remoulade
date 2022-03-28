# Here, we define all the actors and the remoulade broker

import requests

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.encoder import PickleEncoder
from remoulade.results import Results
from remoulade.results.backends import RedisBackend
from remoulade.state import MessageState
from remoulade.state.backends import PostgresBackend


encoder = PickleEncoder()
backend = RedisBackend(encoder=encoder)
broker = RabbitmqBroker()
broker.add_middleware(Results(backend=backend))
remoulade.set_broker(broker)
remoulade.set_encoder(encoder)
remoulade.get_broker().add_middleware(MessageState(backend=PostgresBackend()))


@remoulade.actor(store_results=True)
def request(uri):
    return requests.get(uri).text


@remoulade.actor(store_results=True)
def count_words(response):
    return len(response.split(" "))


remoulade.declare_actors([request, count_words])
