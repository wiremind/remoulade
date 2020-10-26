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

import prometheus_client as prom

from ..common import current_millis
from ..logging import get_logger
from .middleware import Middleware

#: The default HTTP host the exposition server should bind to.
DEFAULT_HTTP_HOST = os.getenv("remoulade_prom_host", "127.0.0.1")

#: The default HTTP port the exposition server should listen on.
DEFAULT_HTTP_PORT = int(os.getenv("remoulade_prom_port", "9191"))


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

    def __init__(self, *, http_host=DEFAULT_HTTP_HOST, http_port=DEFAULT_HTTP_PORT, registry=None):
        self.logger = get_logger(__name__, type(self))
        self.http_host = http_host
        self.http_port = http_port
        self.delayed_messages = set()
        self.message_start_times = {}
        self.registry = registry

    def before_worker_boot(self, broker, worker):
        self.logger.debug("Setting up metrics...")
        if self.registry is None:
            self.registry = prom.CollectorRegistry()
        self.worker_busy = prom.Gauge(
            "remoulade_worker_busy", "1 if the worker is processing a message, 0 if not", registry=self.registry,
        )
        self.total_messages = prom.Counter(
            "remoulade_messages_total",
            "The total number of messages processed.",
            ["queue_name", "actor_name"],
            registry=self.registry,
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
        self.inprogress_messages = prom.Gauge(
            "remoulade_messages_inprogress",
            "The number of messages in progress.",
            ["queue_name", "actor_name"],
            registry=self.registry,
        )
        self.inprogress_delayed_messages = prom.Gauge(
            "remoulade_delayed_messages_inprogress",
            "The number of delayed messages in memory.",
            ["queue_name", "actor_name"],
            registry=self.registry,
        )
        self.message_durations = prom.Histogram(
            "remoulade_message_duration_milliseconds",
            "The time spent processing messages.",
            ["queue_name", "actor_name"],
            buckets=(
                5,
                10,
                25,
                50,
                75,
                100,
                250,
                500,
                750,
                1000,
                2500,
                5000,
                7500,
                10000,
                30000,
                60000,
                600000,
                900000,
                float("inf"),
            ),
            registry=self.registry,
        )

        self.logger.debug("Starting exposition server...")
        prom.start_http_server(addr=self.http_host, port=self.http_port, registry=self.registry)

    def after_worker_boot(self, broker, worker):
        self.worker_busy.set(0)

    def after_worker_shutdown(self, broker, worker):
        self.worker_busy.set(0)
        self.logger.debug("Shutting down exposition server...")
        # Do not stop it actually

    def after_nack(self, broker, message):
        labels = (message.queue_name, message.actor_name)
        self.total_rejected_messages.labels(*labels).inc()

    def after_enqueue(self, broker, message, delay):
        if "retries" in message.options:
            labels = (message.queue_name, message.actor_name)
            self.total_retried_messages.labels(*labels).inc()

    def before_delay_message(self, broker, message):
        labels = (message.queue_name, message.actor_name)
        self.delayed_messages.add(message.message_id)
        self.inprogress_delayed_messages.labels(*labels).inc()

    def before_process_message(self, broker, message):
        labels = (message.queue_name, message.actor_name)
        if message.message_id in self.delayed_messages:
            self.delayed_messages.remove(message.message_id)
            self.inprogress_delayed_messages.labels(*labels).dec()

        self.inprogress_messages.labels(*labels).inc()
        self.message_start_times[message.message_id] = current_millis()
        self.worker_busy.set(1)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        labels = (message.queue_name, message.actor_name)
        message_start_time = self.message_start_times.pop(message.message_id, current_millis())
        message_duration = current_millis() - message_start_time
        self.message_durations.labels(*labels).observe(message_duration)
        self.inprogress_messages.labels(*labels).dec()
        self.total_messages.labels(*labels).inc()
        if exception is not None:
            self.total_errored_messages.labels(*labels).inc()
        if not self.message_start_times:
            self.worker_busy.set(0)

    after_skip_message = after_process_message
    after_message_canceled = after_process_message
