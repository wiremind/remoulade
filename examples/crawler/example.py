import argparse
import logging
import re
import sys
from contextlib import closing
from threading import local

import requests

import remoulade
from remoulade.brokers.rabbitmq import RabbitmqBroker

logger = logging.getLogger("example")
anchor_re = re.compile(rb'<a href="([^"]+)">')
state = local()

broker = RabbitmqBroker()
remoulade.set_broker(broker)

urls = []


def get_session():
    session = getattr(state, "session", None)
    if session is None:
        session = state.session = requests.Session()
    return session


@remoulade.actor(max_retries=3, time_limit=10000)
def crawl(url):
    if url in urls:
        logger.warning("URL %r has already been visited. Skipping...", url)
        return

    urls.append(url)
    logger.info("Crawling %r...", url)
    matches = 0
    session = get_session()
    with closing(session.get(url, timeout=(3.05, 5), stream=True)) as response:
        if not response.headers.get("content-type", "").startswith("text/html"):
            logger.warning("Skipping URL %r since it's not HTML.", url)
            return

        for match in anchor_re.finditer(response.content):
            anchor = match.group(1).decode("utf-8")
            if anchor.startswith("http://") or anchor.startswith("https://"):
                crawl.send(anchor)
                matches += 1

        logger.info("Done crawling %r. Found %d anchors.", url, matches)


broker.declare_actor(crawl)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", type=str, help="a URL to crawl")
    args = parser.parse_args()
    crawl.send(args.url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
