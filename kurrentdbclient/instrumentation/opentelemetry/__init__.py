from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Note: namespace pacakge issue? don't understand why this and not e.g. utils.unwrap
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.semconv.schemas import Schemas
from opentelemetry.trace import Tracer, get_tracer

from kurrentdbclient import AsyncKurrentDBClient, KurrentDBClient
from kurrentdbclient.instrumentation.opentelemetry.grpc import (
    try_unwrap_opentelemetry_intercept_grpc_server_stream,
    try_wrap_opentelemetry_intercept_grpc_server_stream,
)
from kurrentdbclient.instrumentation.opentelemetry.package import _instruments
from kurrentdbclient.instrumentation.opentelemetry.spanners import (
    span_append_to_stream,
    span_catchup_subscription,
    span_get_stream,
    span_multi_append_to_stream,
    span_persistent_subscription,
    span_read_stream,
)
from kurrentdbclient.instrumentation.opentelemetry.utils import apply_spanner
from kurrentdbclient.instrumentation.opentelemetry.version import __version__

if TYPE_CHECKING:
    from collections.abc import Collection


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
        return get_tracer(
            __name__,
            __version__,
            tracer_provider=tracer_provider,
            schema_url=Schemas.V1_25_0.value,
        )


class KurrentDBClientInstrumentor(_BaseInstrumentor):
    def _instrument(self, **kwargs: Any) -> None:
        super()._instrument(**kwargs)

        tracer = self._get_tracer(**kwargs)

        apply_spanner(
            patched_class=KurrentDBClient,
            spanned_func=KurrentDBClient.append_to_stream,
            spanner_func=span_append_to_stream,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=KurrentDBClient,
            spanned_func=KurrentDBClient.multi_append_to_stream,
            spanner_func=span_multi_append_to_stream,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=KurrentDBClient,
            spanned_func=KurrentDBClient.subscribe_to_stream,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=KurrentDBClient,
            spanned_func=KurrentDBClient.subscribe_to_all,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=KurrentDBClient,
            spanned_func=KurrentDBClient.read_subscription_to_stream,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=KurrentDBClient,
            spanned_func=KurrentDBClient.read_subscription_to_all,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        if self.instrument_get_and_read_stream:
            apply_spanner(
                patched_class=KurrentDBClient,
                spanned_func=KurrentDBClient.read_stream,
                spanner_func=span_read_stream,
                tracer=tracer,
            )
            apply_spanner(
                patched_class=KurrentDBClient,
                spanned_func=KurrentDBClient.get_stream,
                spanner_func=span_get_stream,
                tracer=tracer,
            )

        # Because its server streaming wrapper doesn't return an
        # object with a cancel() method, so we can't stop them.
        try_wrap_opentelemetry_intercept_grpc_server_stream()

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(KurrentDBClient, "append_to_stream")
        unwrap(KurrentDBClient, "multi_append_to_stream")
        unwrap(KurrentDBClient, "subscribe_to_stream")
        unwrap(KurrentDBClient, "subscribe_to_all")
        unwrap(KurrentDBClient, "read_subscription_to_stream")
        unwrap(KurrentDBClient, "read_subscription_to_all")

        if self.instrument_get_and_read_stream:
            unwrap(KurrentDBClient, "get_stream")
            unwrap(KurrentDBClient, "read_stream")

        try_unwrap_opentelemetry_intercept_grpc_server_stream()


class AsyncKurrentDBClientInstrumentor(_BaseInstrumentor):
    def _instrument(self, **kwargs: Any) -> None:
        super()._instrument(**kwargs)

        tracer = self._get_tracer(**kwargs)

        apply_spanner(
            patched_class=AsyncKurrentDBClient,
            spanned_func=AsyncKurrentDBClient.append_to_stream,
            spanner_func=span_append_to_stream,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncKurrentDBClient,
            spanned_func=AsyncKurrentDBClient.multi_append_to_stream,
            spanner_func=span_multi_append_to_stream,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncKurrentDBClient,
            spanned_func=AsyncKurrentDBClient.subscribe_to_stream,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncKurrentDBClient,
            spanned_func=AsyncKurrentDBClient.subscribe_to_all,
            spanner_func=span_catchup_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncKurrentDBClient,
            spanned_func=AsyncKurrentDBClient.read_subscription_to_stream,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        apply_spanner(
            patched_class=AsyncKurrentDBClient,
            spanned_func=AsyncKurrentDBClient.read_subscription_to_all,
            spanner_func=span_persistent_subscription,
            tracer=tracer,
        )
        if self.instrument_get_and_read_stream:
            apply_spanner(
                patched_class=AsyncKurrentDBClient,
                spanned_func=AsyncKurrentDBClient.read_stream,
                spanner_func=span_read_stream,
                tracer=tracer,
            )
            apply_spanner(
                patched_class=AsyncKurrentDBClient,
                spanned_func=AsyncKurrentDBClient.get_stream,
                spanner_func=span_get_stream,
                tracer=tracer,
            )

    def _uninstrument(self, **kwargs: Any) -> None:
        unwrap(AsyncKurrentDBClient, "append_to_stream")
        unwrap(AsyncKurrentDBClient, "multi_append_to_stream")
        unwrap(AsyncKurrentDBClient, "subscribe_to_stream")
        unwrap(AsyncKurrentDBClient, "subscribe_to_all")
        unwrap(AsyncKurrentDBClient, "read_subscription_to_stream")
        unwrap(AsyncKurrentDBClient, "read_subscription_to_all")

        if self.instrument_get_and_read_stream:
            unwrap(AsyncKurrentDBClient, "get_stream")
            unwrap(AsyncKurrentDBClient, "read_stream")
