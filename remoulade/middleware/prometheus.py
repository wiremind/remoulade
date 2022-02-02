# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Remoulade is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Remoulade is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import os
import time
from threading import local

import prometheus_client as prom

from ..logging import get_logger
from .middleware import Middleware

#: The default HTTP host the exposition server should bind to.
DEFAULT_HTTP_HOST = os.getenv("remoulade_prom_host", "127.0.0.1")

#: The default HTTP port the exposition server should listen on.
DEFAULT_HTTP_PORT = int(os.getenv("remoulade_prom_port", "9191"))

#: The default HTTP port the exposition server should listen on.
DEFAULT_LABEL = "default"


class Prometheus(Middleware):
    """A middleware that exports stats via Prometheus_.

    Parameters:
      http_host(str): The host to bind the Prometheus exposition
        server on.  This parameter can also be configured via the
        ``remoulade_prom_host`` environment variable.
      http_port(int): The port on which the server should listen.
        This parameter can also be configured via the
        ``remoulade_prom_port`` environment variable.
      registry(CollectorRegistry): the prometheus registry to use, if None, use a new registry.

    .. _Prometheus: https://prometheus.io
    """

    def __init__(
        self,
        *,
        http_host=DEFAULT_HTTP_HOST,
        http_port=DEFAULT_HTTP_PORT,
        registry=None,
        use_default_label=False,
    ):
        self.logger = get_logger(__name__, type(self))
        self.http_host = http_host
        self.http_port = http_port
        self.local_data = local()
        self.registry = registry
        self.use_default_label = use_default_label

        self.worker_busy = None
        self.total_errored_messages = None
        self.total_retried_messages = None
        self.total_rejected_messages = None
        self.message_durations = None

    @property
    def actor_options(self):
        """The set of options that may be configured on each actor."""
        return {"prometheus_label", "use_default_prometheus_label"}

    def _get_label(self, actor):
        use_default_label = actor.options.get("use_default_prometheus_label", self.use_default_label)
        if use_default_label:
            return DEFAULT_LABEL
        return actor.options.get("prometheus_label", actor.actor_name)

    def _get_labels(self, broker, message):
        actor = broker.get_actor(message.actor_name)
        return message.queue_name, self._get_label(actor)

    def _init_labels(self, actor, worker=None):
        # initialize the metrics for all labels to 0
        metrics = [
            self.total_errored_messages,
            self.total_retried_messages,
            self.total_rejected_messages,
            self.message_durations,
        ]
        worker_queues = worker.consumer_whitelist if worker else None
        for metric in metrics:
            if metric:  # metric can be None if actor is declared before worker boot
                label = self._get_label(actor)

                queues = set(actor.queue_names)
                if worker_queues:
                    queues &= worker_queues

                for queue_name in queues:
                    metric.labels(queue_name, label)

    def before_worker_boot(self, broker, worker):
        self.logger.debug("Setting up metrics...")
        if self.registry is None:
            self.registry = prom.CollectorRegistry()
        self.worker_busy = prom.Gauge(
            "remoulade_worker_busy", "1 if the worker is processing a message, 0 if not", registry=self.registry
        )
        self.total_errored_messages = prom.Counter(
            "remoulade_message_errors_total",
            "The total number of errored messages.",
            ["queue_name", "actor_name"],
            registry=self.registry,
        )
        self.total_retried_messages = prom.Counter(
            "remoulade_message_retries_total",
            "The total number of retried messages.",
            ["queue_name", "actor_name"],
            registry=self.registry,
        )
        self.total_rejected_messages = prom.Counter(
            "remoulade_message_rejects_total",
            "The total number of dead-lettered messages.",
            ["queue_name", "actor_name"],
            registry=self.registry,
        )
        self.message_durations = prom.Summary(
            "remoulade_message_duration_milliseconds",
            "The time spent processing messages.",
            ["queue_name", "actor_name"],
            registry=self.registry,
        )
        for actor in broker.actors.values():
            self._init_labels(actor, worker)

        self.logger.debug("Starting exposition server...")
        prom.start_http_server(addr=self.http_host, port=self.http_port, registry=self.registry)

    def after_worker_boot(self, broker, worker):
        self.worker_busy.set(0)

    def after_worker_shutdown(self, broker, worker):
        self.worker_busy.set(0)
        self.logger.debug("Shutting down exposition server...")
        # Do not stop it actually

    def after_declare_actor(self, broker, actor):
        self._init_labels(actor)

    def after_nack(self, broker, message):
        labels = self._get_labels(broker, message)
        self.total_rejected_messages.labels(*labels).inc()

    def after_enqueue(self, broker, message, delay, exception=None):
        if "retries" in message.options:
            labels = self._get_labels(broker, message)
            self.total_retried_messages.labels(*labels).inc()

    @property
    def message_start_times(self):
        if not hasattr(self.local_data, "message_start_times"):
            self.local_data.message_start_times = {}
        return self.local_data.message_start_times

    def before_process_message(self, broker, message):
        self.message_start_times[message.message_id] = time.monotonic() * 1000
        self.worker_busy.set(1)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        labels = self._get_labels(broker, message)
        message_start_time = self.message_start_times.pop(message.message_id, None)
        message_duration = 0
        if message_start_time is not None:
            message_duration = (time.monotonic() * 1000) - message_start_time
        self.message_durations.labels(*labels).observe(message_duration)
        if exception is not None:
            self.total_errored_messages.labels(*labels).inc()
        if not self.message_start_times:
            self.worker_busy.set(0)

    after_skip_message = after_process_message
    after_message_canceled = after_process_message
