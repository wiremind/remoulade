<img src="https://remoulade.readthedocs.io/en/latest/_static/logo.png" align="right" width="131" />

# remoulade

[![CircleCI](https://circleci.com/gh/wiremind/remoulade.svg?style=svg)](https://circleci.com/gh/wiremind/remoulade)
[![PyPI version](https://badge.fury.io/py/remoulade.svg)](https://badge.fury.io/py/remoulade)
[![Documentation](https://img.shields.io/badge/doc-latest-brightgreen.svg)](http://remoulade.readthedocs.io)

*A fast and reliable distributed task processing library for Python 3.* Fork of dramatiq.io

<hr/>

**Changelog**: https://remoulade.readthedocs.io/en/latest/changelog.html <br/>
**Documentation**: https://remoulade.readthedocs.io

<hr/>


## Installation

If you want to use it with [RabbitMQ]
```console
    $ pipenv install 'remoulade[rabbitmq]'
```

or if you want to use it with [Redis]

```console
   $ pipenv install 'remoulade[redis]'
```

## Quickstart

1. Make sure you've got [RabbitMQ] running, then create a new file called
`example.py`:

``` python
from remoulade.brokers.rabbitmq import RabbitmqBroker
import remoulade
import requests
import sys

broker = RabbitmqBroker()
remoulade.set_broker(broker)


@remoulade.actor
def count_words(url):
    response = requests.get(url)
    count = len(response.text.split(" "))
    print(f"There are {count} words at {url!r}.")


broker.declare_actor(count_words)

if __name__ == "__main__":
    count_words.send(sys.argv[1])
```

2. In one terminal, run your workers:
```console
   $ remoulade example
```

3. In another, start enqueueing messages:
```console
   $ python3 example.py http://example.com
   $ python3 example.py https://github.com
   $ python3 example.py https://news.ycombinator.com
```

Visit the [user guide] to see more features!.

## Dashboard

Check out [SuperBowl](https://github.com/wiremind/super-bowl) a dashboard for real-time monitoring and administrating all your Remoulade tasks.
***See the current progress, enqueue, requeue, cancel and more ...***
Super easy to use !.

## Kubernetes

Remoulade is tailored to run transparently in containers on [Kubernetes](https://kubernetes.io/) and to make the most of their features. This does not mean it cannot run outside of Kubernetes ;)

## Monitoring/Prometheus

Remoulade provides a `Prometheus` Middleware that exposes various metrics (processed messages, retries, fails etc.), [Check it out](https://github.com/wiremind/remoulade/blob/master/remoulade/middleware/prometheus.py).

## Tracing/OpenTelemtry

Take a look at [opentelemetry-instrumentation-remoulade](https://github.com/open-telemetry/opentelemetry-python-contrib/tree/main/instrumentation/opentelemetry-instrumentation-remoulade) if you want to enable tracing for Remoulade operations (enqueue, message processing, retries etc.). See [here](https://github.com/open-telemetry/opentelemetry-python-contrib/blob/main/instrumentation/opentelemetry-instrumentation-remoulade/src/opentelemetry/instrumentation/remoulade/__init__.py) for an example.
The library follows [OpenTelemetry](https://opentelemetry.io/) standards.

## License

remoulade is licensed under the LGPL.  Please see [COPYING] and
[COPYING.LESSER] for licensing details.

[COPYING.LESSER]: https://github.com/wiremind/remoulade/blob/master/COPYING.LESSER
[COPYING]: https://github.com/wiremind/remoulade/blob/master/COPYING
[RabbitMQ]: https://www.rabbitmq.com/
[Redis]: https://redis.io
[user guide]: https://remoulade.readthedocs.io/en/latest/guide.html
