# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Collection, Dict, Type

# Note: namespace pacakge issue? don't understand why this and not e.g. utils.unwrap
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.schemas import Schemas
from opentelemetry.trace import get_tracer

from esdbclient import AsyncEventStoreDBClient, EventStoreDBClient
from esdbclient.client import BaseEventStoreDBClient
from esdbclient.instrumentation.opentelemetry.grpc import (
    try_unwrap_opentelemetry_intercept_grpc_server_stream,
    try_wrap_opentelemetry_intercept_grpc_server_stream,
)
from esdbclient.instrumentation.opentelemetry.package import _instruments
from esdbclient.instrumentation.opentelemetry.utils import (
    SpannerType,
    apply_spanner,
    span_append_to_stream,
    span_catchup_subscription,
    span_method,
    span_persistent_subscription,
    span_read_stream,
)
from esdbclient.instrumentation.opentelemetry.version import __version__


class EventStoreDBClientInstrumentor(BaseInstrumentor):
    SPANNERS: Dict[str, SpannerType[Any]] = {
        "get_stream": span_method,
        "read_stream": span_read_stream,
        "append_to_stream": span_append_to_stream,
        "subscribe_to_all": span_catchup_subscription,
        "subscribe_to_stream": span_catchup_subscription,
        "read_subscription_to_all": span_persistent_subscription,
        "read_subscription_to_stream": span_persistent_subscription,
    }
    client_cls: Type[BaseEventStoreDBClient] = EventStoreDBClient
    instrument_get_and_read_stream = False

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        tracer_provider = kwargs.get("tracer_provider")

        instrument_get_and_read_stream = bool(
            kwargs.get("instrument_get_and_read_stream")
        )
        self.instrument_get_and_read_stream = instrument_get_and_read_stream

        tracer = get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=Schemas.V1_25_0.value,
        )

        for name, spanner in self.SPANNERS.items():
            if name in ["get_stream", "read_stream"]:
                if not self.instrument_get_and_read_stream:
                    continue

            apply_spanner(self.client_cls, name, spanner, tracer)

        # Because its server streaming wrapper doesn't return an
        # object with a cancel() method, so we can't stop them.
        try_wrap_opentelemetry_intercept_grpc_server_stream()

    def _uninstrument(self, **kwargs: Any) -> None:
        for name in self.SPANNERS.keys():
            if not self.instrument_get_and_read_stream:
                if name in ["get_stream", "read_stream"]:
                    continue
            unwrap(self.client_cls, name)

        try_unwrap_opentelemetry_intercept_grpc_server_stream()


class AsyncEventStoreDBClientInstrumentor(EventStoreDBClientInstrumentor):
    client_cls = AsyncEventStoreDBClient
