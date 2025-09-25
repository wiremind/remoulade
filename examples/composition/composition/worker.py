# Here, we launch the worker, which will process tasks

from prometheus_client.registry import REGISTRY

from remoulade import Middleware
from remoulade.__main__ import main
from remoulade.middleware import Prometheus

from .actors import broker

prometheus: Middleware = Prometheus(
    http_host="0.0.0.0",  # noqa: S104
    http_port=9191,
    registry=REGISTRY,
)
broker.add_middleware(prometheus)


# Run worker
def run():
    main()
