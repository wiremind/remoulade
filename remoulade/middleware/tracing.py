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

"""OpenTelemetry tracing middleware for Remoulade.

This middleware creates **new trace roots** on the consumer side with a
``Link`` back to the producer span, instead of propagating trace context
as parent-child (same ``trace_id``).

Why?
----
The default ``opentelemetry-instrumentation-remoulade`` package propagates
the full trace context from producer to consumer.  Combined with a
``ParentBased`` sampler, this means that when a single sampled request fans
out to thousands of Remoulade tasks, *every* task (and all its downstream
SQL / Redis / HTTP spans) ends up in the **same** trace.  A single batch
can easily produce 300MB+ traces that overwhelm Tempo / Jaeger ingesters.

This middleware breaks the parent-child chain:

* **Producer side** (``before_enqueue``): starts a ``PRODUCER`` span and
  ``inject()``s the current context into ``message.options["trace_ctx"]``.
* **Consumer side** (``before_process_message``): always starts a **new root**
  ``CONSUMER`` span with its own ``trace_id``, independently subject to the
  configured sampler.  When the message carries a ``trace_ctx`` dict
  (injected by the producer), the consumer ``extract()``s it and attaches a
  ``Link`` back to the producer span.  Messages without ``trace_ctx`` still
  get a span but with no link.

The causal relationship remains navigable in Tempo / Grafana via span
links.

Usage
-----
::

    from opentelemetry import trace
    from remoulade.middleware.tracing import OpenTelemetryMiddleware

    tracer = trace.get_tracer("my_app")
    broker.add_middleware(OpenTelemetryMiddleware(tracer))
"""

from __future__ import annotations

import threading
from typing import Any

from opentelemetry import (
    context as context_api,
    trace as trace_api,
)
from opentelemetry.propagate import extract, inject
from opentelemetry.trace import Status, StatusCode

from .middleware import Middleware

SpanRegistryValue = tuple[Any, Any]


class OpenTelemetryMiddleware(Middleware):
    """Remoulade middleware that instruments message enqueue / process
    with OpenTelemetry spans using **link-based** propagation.

    Each consumed message starts a new, independent trace with a
    ``Link`` back to the producer span rather than being a child of it.
    """

    def __init__(self, tracer=None):
        """
        Parameters
        ----------
        tracer:
            An ``opentelemetry.trace.Tracer`` instance.  If *None*, a
            default tracer named ``"remoulade"`` is created.
        """
        self._tracer = tracer or trace_api.get_tracer(
            "remoulade",
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )
        self._local = threading.local()

    @property
    def _span_registry(self) -> dict[tuple[str, bool], SpanRegistryValue]:
        if not hasattr(self._local, "spans"):
            self._local.spans = {}
        return self._local.spans

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _operation_name(hook: str, retry_count: int) -> str:
        base = f"remoulade/{hook}"
        if retry_count:
            return f"{base}(retry-{retry_count})"
        return base

    # ------------------------------------------------------------------
    # Consumer hooks
    # ------------------------------------------------------------------

    def before_process_message(self, broker, message):
        # Build an optional link back to the producer span.
        links: list[trace_api.Link] = []
        trace_ctx = message.options.get("trace_ctx")
        if isinstance(trace_ctx, dict):
            parent_ctx = extract(trace_ctx)
            parent_span = trace_api.get_current_span(parent_ctx)
            parent_span_ctx = parent_span.get_span_context() if parent_span else None
            if parent_span_ctx and parent_span_ctx.is_valid:
                links.append(trace_api.Link(parent_span_ctx))

        retry_count = message.options.get("retries", 0)

        # Start a NEW root span in an **empty** context so that any
        # active span on this thread is NOT used as parent.  This
        # guarantees the consumer span is always a root span with its
        # own trace_id.
        empty_ctx = context_api.Context()
        span = self._tracer.start_span(
            self._operation_name("process", retry_count),
            context=empty_ctx,
            kind=trace_api.SpanKind.CONSUMER,
            links=links,
            attributes={
                "remoulade.retry_count": retry_count,
                "remoulade.action": "process",
                "remoulade.actor_name": message.actor_name,
                "messaging.message.id": message.message_id,
            },
        )

        activation = trace_api.use_span(span, end_on_exit=True)
        activation.__enter__()
        self._span_registry[message.message_id, False] = (span, activation)

    def after_process_message(self, broker, message, *, result=None, exception=None):
        self._end_consumer_span(message, exception=exception)

    def after_skip_message(self, broker, message):
        self._end_consumer_span(message)

    def after_message_canceled(self, broker, message):
        self._end_consumer_span(message)

    def _end_consumer_span(self, message, *, exception=None):
        span_and_activation = self._span_registry.pop((message.message_id, False), None)
        if span_and_activation is None:
            return

        span, activation = span_and_activation
        if exception is not None:
            span.set_status(Status(StatusCode.ERROR, str(exception)))
            span.record_exception(exception)
        activation.__exit__(None, None, None)

    # ------------------------------------------------------------------
    # Producer hooks
    # ------------------------------------------------------------------

    def before_enqueue(self, broker, message, delay):
        retry_count = message.options.get("retries", 0)

        span = self._tracer.start_span(
            self._operation_name("send", retry_count),
            kind=trace_api.SpanKind.PRODUCER,
            attributes={
                "remoulade.retry_count": retry_count,
                "remoulade.action": "send",
                "remoulade.actor_name": message.actor_name,
                "messaging.message.id": message.message_id,
            },
        )

        activation = trace_api.use_span(span, end_on_exit=True)
        activation.__enter__()
        self._span_registry[message.message_id, True] = (span, activation)

        # Inject current context so the consumer can build a link back.
        trace_ctx = message.options.get("trace_ctx")
        if not isinstance(trace_ctx, dict):
            trace_ctx = {}
        inject(trace_ctx)
        message.options["trace_ctx"] = trace_ctx

    def after_enqueue(self, broker, message, delay, exception=None):
        span_and_activation = self._span_registry.pop((message.message_id, True), None)
        if span_and_activation is None:
            return

        span, activation = span_and_activation
        if exception is not None:
            span.set_status(Status(StatusCode.ERROR, str(exception)))
            span.record_exception(exception)
        activation.__exit__(None, None, None)
