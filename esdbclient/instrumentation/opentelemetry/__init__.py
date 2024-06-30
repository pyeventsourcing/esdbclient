# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Collection

# Note: namespace pacakge issue? don't understand why this and not e.g. utils.unwrap
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.schemas import Schemas
from opentelemetry.trace import Tracer, get_tracer

from esdbclient import AsyncEventStoreDBClient, EventStoreDBClient
from esdbclient.instrumentation.opentelemetry.grpc import (
    try_unwrap_opentelemetry_intercept_grpc_server_stream,
    try_wrap_opentelemetry_intercept_grpc_server_stream,
)
from esdbclient.instrumentation.opentelemetry.package import _instruments
from esdbclient.instrumentation.opentelemetry.spanners import (
    span_append_to_stream,
    span_catchup_subscription,
    span_get_stream,
    span_persistent_subscription,
    span_read_stream,
)
from esdbclient.instrumentation.opentelemetry.utils import apply_spanner
from esdbclient.instrumentation.opentelemetry.version import __version__


class _RedefinedBaseInstrumentor(BaseInstrumentor):  # type: ignore[misc]
    pass


class _BaseInstrumentor(_RedefinedBaseInstrumentor):
    instrument_get_and_read_stream = False

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        instrument_get_and_read_stream = bool(
            kwargs.get("instrument_get_and_read_stream")
        )
        self.instrument_get_and_read_stream = instrument_get_and_read_stream

    def _get_tracer(self, **kwargs: Any) -> Tracer:
        tracer_provider = kwargs.get("tracer_provider")
        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=Schemas.V1_25_0.value,
        )
        return tracer


class EventStoreDBClientInstrumentor(_BaseInstrumentor):
    def _instrument(self, **kwargs: Any) -> None:
        super()._instrument(**kwargs)

        tracer = self._get_tracer(**kwargs)

        apply_spanner(
            patched_class=EventStoreDBClient,
            spanned_func=EventStoreDBClient.append_to_stream,
            spanner_func=span_append_to_stream,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=EventStoreDBClient,
            spanned_func=EventStoreDBClient.subscribe_to_stream,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=EventStoreDBClient,
            spanned_func=EventStoreDBClient.subscribe_to_all,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=EventStoreDBClient,
            spanned_func=EventStoreDBClient.read_subscription_to_stream,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=EventStoreDBClient,
            spanned_func=EventStoreDBClient.read_subscription_to_all,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        if self.instrument_get_and_read_stream:
            apply_spanner(
                patched_class=EventStoreDBClient,
                spanned_func=EventStoreDBClient.read_stream,
                spanner_func=span_read_stream,
                tracer=tracer,
            )
            apply_spanner(
                patched_class=EventStoreDBClient,
                spanned_func=EventStoreDBClient.get_stream,
                spanner_func=span_get_stream,
                tracer=tracer,
            )

        # Because its server streaming wrapper doesn't return an
        # object with a cancel() method, so we can't stop them.
        try_wrap_opentelemetry_intercept_grpc_server_stream()

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(EventStoreDBClient, "append_to_stream")
        unwrap(EventStoreDBClient, "subscribe_to_stream")
        unwrap(EventStoreDBClient, "subscribe_to_all")
        unwrap(EventStoreDBClient, "read_subscription_to_stream")
        unwrap(EventStoreDBClient, "read_subscription_to_all")

        if self.instrument_get_and_read_stream:
            unwrap(EventStoreDBClient, "get_stream")
            unwrap(EventStoreDBClient, "read_stream")

        try_unwrap_opentelemetry_intercept_grpc_server_stream()


class AsyncEventStoreDBClientInstrumentor(_BaseInstrumentor):
    def _instrument(self, **kwargs: Any) -> None:
        super()._instrument(**kwargs)

        tracer = self._get_tracer(**kwargs)

        apply_spanner(
            patched_class=AsyncEventStoreDBClient,
            spanned_func=AsyncEventStoreDBClient.append_to_stream,
            spanner_func=span_append_to_stream,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncEventStoreDBClient,
            spanned_func=AsyncEventStoreDBClient.subscribe_to_stream,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncEventStoreDBClient,
            spanned_func=AsyncEventStoreDBClient.subscribe_to_all,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncEventStoreDBClient,
            spanned_func=AsyncEventStoreDBClient.read_subscription_to_stream,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncEventStoreDBClient,
            spanned_func=AsyncEventStoreDBClient.read_subscription_to_all,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        if self.instrument_get_and_read_stream:
            apply_spanner(
                patched_class=AsyncEventStoreDBClient,
                spanned_func=AsyncEventStoreDBClient.read_stream,
                spanner_func=span_read_stream,
                tracer=tracer,
            )
            apply_spanner(
                patched_class=AsyncEventStoreDBClient,
                spanned_func=AsyncEventStoreDBClient.get_stream,
                spanner_func=span_get_stream,
                tracer=tracer,
            )

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(AsyncEventStoreDBClient, "append_to_stream")
        unwrap(AsyncEventStoreDBClient, "subscribe_to_stream")
        unwrap(AsyncEventStoreDBClient, "subscribe_to_all")
        unwrap(AsyncEventStoreDBClient, "read_subscription_to_stream")
        unwrap(AsyncEventStoreDBClient, "read_subscription_to_all")

        if self.instrument_get_and_read_stream:
            unwrap(AsyncEventStoreDBClient, "get_stream")
            unwrap(AsyncEventStoreDBClient, "read_stream")
