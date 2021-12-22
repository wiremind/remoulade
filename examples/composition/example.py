import argparse
import sys

import requests

import remoulade
from remoulade import group
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


def ask_count_words(uri_list):
    jobs = group([request.message(uri) | count_words.message() for uri in uri_list]).run()
    for uri, count in zip(arguments.uri, jobs.results.get(block=True)):
        print(f" * {uri} has {count} words")

    return 0


def serve():
    from remoulade.api import app
    app.run(debug=True, host="0.0.0.0", port=5000, reloader_type="stat")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', action='store_true')
    parser.add_argument("uri", nargs="*", help="A website URI.")
    arguments = parser.parse_args()

    if arguments.server:
        serve()
    else:
        if not arguments.uri:
            raise Exception("Please specify URI(s) to count")
        sys.exit(ask_count_words(arguments.uri))
