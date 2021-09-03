import argparse
import sys

import requests

import remoulade
from remoulade import group
from remoulade.brokers.rabbitmq import RabbitmqBroker
from remoulade.encoder import PickleEncoder
from remoulade.results import Results
from remoulade.results.backends import RedisBackend

encoder = PickleEncoder()
backend = RedisBackend(encoder=encoder)
broker = RabbitmqBroker()
broker.add_middleware(Results(backend=backend))
remoulade.set_broker(broker)
remoulade.set_encoder(encoder)


@remoulade.actor(store_results=True)
def request(uri):
    return requests.get(uri)


@remoulade.actor(store_results=True)
def count_words(response):
    return len(response.text.split(" "))


remoulade.declare_actors([request, count_words])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", nargs="+", help="A website URI.")

    arguments = parser.parse_args()
    jobs = group([request.message(uri) | count_words.message() for uri in arguments.uri]).run()
    for uri, count in zip(arguments.uri, jobs.results.get(block=True)):
        print(" * {uri} has {count} words".format(uri=uri, count=count))

    return 0


if __name__ == "__main__":
    sys.exit(main())
