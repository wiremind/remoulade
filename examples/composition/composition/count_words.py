# Here, we enqueue a new task that will be processed by workers.
# For the sake of the example, we are going to run synchronously

import argparse

from remoulade import group

from .actors import request, count_words


def ask_count_words():
    parser = argparse.ArgumentParser()
    parser.add_argument("uri", nargs="*", help="A website URI.")
    arguments = parser.parse_args()
    if not arguments.uri:
        raise Exception("Please specify URI(s) to count")

    jobs = group([request.message(uri) | count_words.message() for uri in arguments.uri]).run()
    for uri, count in zip(arguments.uri, jobs.results.get(block=True)):
        print(f" * {uri} has {count} words")
