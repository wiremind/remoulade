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
* **Consumer side** (``before_process_message``): ``extract()``s the
  propagated context solely to build a ``Link``, then starts a **new root**
  ``CONSUMER`` span.  The new span gets its own ``trace_id`` and is
  independently subject to the configured sampler.

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

import logging

from .middleware import Middleware

log = logging.getLogger(__name__)


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
        from opentelemetry import trace as trace_api

        self._tracer = tracer or trace_api.get_tracer(
            "remoulade",
            schema_url="https://opentelemetry.io/schemas/1.11.0",
        )
        self._span_registry: dict[tuple[str, bool], tuple] = {}

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
        from opentelemetry import trace as trace_api
        from opentelemetry.propagate import extract

        if "trace_ctx" not in message.options:
            return

        # Extract the parent context to build a link, but do NOT use it
        # as the parent of the new span.
        parent_ctx = extract(message.options["trace_ctx"])
        parent_span = trace_api.get_current_span(parent_ctx)
        parent_span_ctx = parent_span.get_span_context() if parent_span else None

        links: list[trace_api.Link] = []
        if parent_span_ctx and parent_span_ctx.is_valid:
            links.append(trace_api.Link(parent_span_ctx))

        retry_count = message.options.get("retries", 0)

        # Start a NEW root span (no parent context) with a link to the
        # producer.
        span = self._tracer.start_span(
            self._operation_name("process", retry_count),
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
        span_and_activation = self._span_registry.pop((message.message_id, False), None)
        if span_and_activation is None:
            return

        span, activation = span_and_activation
        if exception is not None:
            from opentelemetry.trace import StatusCode

            span.set_status(StatusCode.ERROR, str(exception))
            span.record_exception(exception)
        activation.__exit__(None, None, None)

    # ------------------------------------------------------------------
    # Producer hooks
    # ------------------------------------------------------------------

    def before_enqueue(self, broker, message, delay):
        from opentelemetry import trace as trace_api
        from opentelemetry.propagate import inject

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
        if "trace_ctx" not in message.options:
            message.options["trace_ctx"] = {}
        inject(message.options["trace_ctx"])

    def after_enqueue(self, broker, message, delay, exception=None):
        span_and_activation = self._span_registry.pop((message.message_id, True), None)
        if span_and_activation is None:
            return

        span, activation = span_and_activation
        if exception is not None:
            from opentelemetry.trace import StatusCode

            span.set_status(StatusCode.ERROR, str(exception))
            span.record_exception(exception)
        activation.__exit__(None, None, None)
