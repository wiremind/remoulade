# This file is a part of Remoulade.
#
# Copyright (C) 2017,2018 WIREMIND SAS <dev@wiremind.fr>
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
from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry import trace as trace_api
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from remoulade import Message
from remoulade.middleware.tracing import OpenTelemetryMiddleware

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def otel_setup():
    """Set up a TracerProvider with an in-memory exporter and yield
    (middleware, exporter) for test assertions.
    """
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test-remoulade")
    middleware = OpenTelemetryMiddleware(tracer=tracer)
    yield middleware, exporter
    provider.shutdown()


def _make_message(**options) -> Message:
    return Message(
        queue_name="default",
        actor_name="test_actor",
        args=(),
        kwargs={},
        options=options,
    )


# ---------------------------------------------------------------------------
# Producer tests
# ---------------------------------------------------------------------------


class TestProducer:
    def test_before_enqueue_injects_trace_ctx(self, otel_setup):
        middleware, exporter = otel_setup
        message = _make_message()

        middleware.before_enqueue(None, message, delay=0)
        middleware.after_enqueue(None, message, delay=0)

        # trace_ctx should be injected into message options
        assert "trace_ctx" in message.options
        assert isinstance(message.options["trace_ctx"], dict)

        # A PRODUCER span should have been created and finished
        assert len(exporter.get_finished_spans()) == 1
        span = exporter.get_finished_spans()[0]
        assert span.kind == trace_api.SpanKind.PRODUCER
        assert "send" in span.name
        assert span.attributes["remoulade.actor_name"] == "test_actor"
        assert span.attributes["messaging.message.id"] == message.message_id

    def test_after_enqueue_records_error(self, otel_setup):
        middleware, exporter = otel_setup
        message = _make_message()

        middleware.before_enqueue(None, message, delay=0)
        exc = RuntimeError("enqueue failed")
        middleware.after_enqueue(None, message, delay=0, exception=exc)

        assert len(exporter.get_finished_spans()) == 1
        span = exporter.get_finished_spans()[0]
        assert span.status.status_code == trace_api.StatusCode.ERROR
        assert "enqueue failed" in span.status.description
        assert len(span.events) == 1  # recorded exception


# ---------------------------------------------------------------------------
# Consumer tests
# ---------------------------------------------------------------------------


class TestConsumer:
    def _enqueue_and_get_trace_ctx(self, middleware, exporter):
        """Helper: simulate producer side and return the injected trace_ctx."""
        message = _make_message()
        middleware.before_enqueue(None, message, delay=0)
        middleware.after_enqueue(None, message, delay=0)
        assert len(exporter.get_finished_spans()) == 1
        producer_span = exporter.get_finished_spans()[0]
        return message.options["trace_ctx"], producer_span

    def test_consumer_creates_new_trace_with_link(self, otel_setup):
        middleware, exporter = otel_setup
        trace_ctx, producer_span = self._enqueue_and_get_trace_ctx(middleware, exporter)

        # Simulate consumer side
        consumer_message = _make_message(trace_ctx=trace_ctx)
        middleware.before_process_message(None, consumer_message)
        middleware.after_process_message(None, consumer_message, result=42)

        assert len(exporter.get_finished_spans()) == 2
        consumer_span = exporter.get_finished_spans()[1]

        # Consumer span should be a CONSUMER kind
        assert consumer_span.kind == trace_api.SpanKind.CONSUMER
        assert "process" in consumer_span.name

        # Consumer span must have a DIFFERENT trace_id (new root)
        assert consumer_span.context.trace_id != producer_span.context.trace_id

        # Consumer span must have a Link to the producer span
        assert len(consumer_span.links) == 1
        link = consumer_span.links[0]
        assert link.context.trace_id == producer_span.context.trace_id
        assert link.context.span_id == producer_span.context.span_id

    def test_consumer_without_trace_ctx_creates_span_without_link(self, otel_setup):
        middleware, exporter = otel_setup
        message = _make_message()  # no trace_ctx

        middleware.before_process_message(None, message)
        middleware.after_process_message(None, message, result=1)

        # A CONSUMER span is still created, but without any link
        assert len(exporter.get_finished_spans()) == 1
        consumer_span = exporter.get_finished_spans()[0]
        assert consumer_span.kind == trace_api.SpanKind.CONSUMER
        assert len(consumer_span.links) == 0

    def test_after_process_message_records_error(self, otel_setup):
        middleware, exporter = otel_setup
        trace_ctx, _ = self._enqueue_and_get_trace_ctx(middleware, exporter)

        consumer_message = _make_message(trace_ctx=trace_ctx)
        middleware.before_process_message(None, consumer_message)
        exc = ValueError("task failed")
        middleware.after_process_message(None, consumer_message, exception=exc)

        assert len(exporter.get_finished_spans()) == 2
        consumer_span = exporter.get_finished_spans()[1]
        assert consumer_span.status.status_code == trace_api.StatusCode.ERROR
        assert "task failed" in consumer_span.status.description
        assert len(consumer_span.events) == 1

    def test_after_skip_message_ends_span(self, otel_setup):
        middleware, exporter = otel_setup
        trace_ctx, _ = self._enqueue_and_get_trace_ctx(middleware, exporter)

        consumer_message = _make_message(trace_ctx=trace_ctx)
        middleware.before_process_message(None, consumer_message)
        middleware.after_skip_message(None, consumer_message)

        assert len(exporter.get_finished_spans()) == 2
        consumer_span = exporter.get_finished_spans()[1]
        # Span ended without error
        assert consumer_span.status.status_code == trace_api.StatusCode.UNSET

    def test_after_message_canceled_ends_span(self, otel_setup):
        middleware, exporter = otel_setup
        trace_ctx, _ = self._enqueue_and_get_trace_ctx(middleware, exporter)

        consumer_message = _make_message(trace_ctx=trace_ctx)
        middleware.before_process_message(None, consumer_message)
        middleware.after_message_canceled(None, consumer_message)

        assert len(exporter.get_finished_spans()) == 2
        consumer_span = exporter.get_finished_spans()[1]
        assert consumer_span.status.status_code == trace_api.StatusCode.UNSET


# ---------------------------------------------------------------------------
# Context isolation tests
# ---------------------------------------------------------------------------


class TestContextIsolation:
    def test_consumer_span_is_root_even_with_active_span(self, otel_setup):
        """Even if there is an active span on the current thread, the
        consumer span must start as a new root (different trace_id)."""
        middleware, exporter = otel_setup
        trace_ctx, producer_span = TestConsumer()._enqueue_and_get_trace_ctx(middleware, exporter)

        # Start an "ambient" span that would be the parent if we didn't
        # explicitly use an empty context.
        ambient_tracer = TracerProvider().get_tracer("ambient")
        with ambient_tracer.start_as_current_span("ambient_operation") as ambient_span:
            consumer_message = _make_message(trace_ctx=trace_ctx)
            middleware.before_process_message(None, consumer_message)
            middleware.after_process_message(None, consumer_message, result=1)

        consumer_span = exporter.get_finished_spans()[1]
        ambient_trace_id = ambient_span.get_span_context().trace_id
        # Consumer span must NOT be a child of the ambient span
        assert consumer_span.context.trace_id != ambient_trace_id
        assert consumer_span.context.trace_id != producer_span.context.trace_id
