# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import (
    Any,
    Dict,
    Generic,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    overload,
)
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.case import _AssertRaisesContext
from uuid import uuid4

import opentelemetry.sdk.trace as trace_sdk
import opentelemetry.trace as trace_api

# from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.grpc import (
    GrpcAioInstrumentorClient,
    GrpcInstrumentorClient,
)
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)

# from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import (
    INVALID_SPAN_CONTEXT,
    SpanContext,
    SpanKind,
    Tracer,
    get_tracer,
    set_tracer_provider,
)
from opentelemetry.util.types import AttributeValue

from esdbclient import (
    AsyncEventStoreDBClient,
    EventStoreDBClient,
    NewEvent,
    RecordedEvent,
    StreamState,
)
from esdbclient.client import BaseEventStoreDBClient
from esdbclient.exceptions import DiscoveryFailed, GrpcError, ServiceUnavailable
from esdbclient.instrumentation.opentelemetry import (
    AsyncEventStoreDBClientInstrumentor,
    EventStoreDBClientInstrumentor,
)
from esdbclient.instrumentation.opentelemetry.spanners import (
    _enrich_span,
    _set_context_in_events,
)
from esdbclient.instrumentation.opentelemetry.utils import (
    AsyncSpannerResponse,
    OverloadedSpannerResponse,
    SpannerResponse,
    _set_span_error,
    _set_span_ok,
    _start_span,
    apply_spanner,
)
from esdbclient.persistent import AsyncPersistentSubscription, PersistentSubscription
from esdbclient.streams import AsyncCatchupSubscription, CatchupSubscription

_in_memory_span_exporter = InMemorySpanExporter()


def init_tracer_provider(
    span_exporters: Sequence[SpanExporter] = (),
    span_processor_cls: Union[
        Type[BatchSpanProcessor], Type[SimpleSpanProcessor]
    ] = BatchSpanProcessor,
) -> None:
    resource = Resource.create(
        attributes={
            SERVICE_NAME: "eventstoredb",
        }
    )
    provider = TracerProvider(resource=resource)
    for span_exporter in span_exporters:
        provider.add_span_processor(span_processor_cls(span_exporter))
    set_tracer_provider(provider)


init_tracer_provider(
    span_exporters=[
        _in_memory_span_exporter,
        # ConsoleSpanExporter(),
        # OTLPSpanExporter(endpoint="http://127.0.0.1:4318/v1/traces"),
    ],
)


def _clear_in_memory_spans() -> None:
    _force_flush_spans()
    _in_memory_span_exporter.clear()


def _get_in_memory_spans() -> Tuple[trace_sdk.ReadableSpan, ...]:
    _force_flush_spans()
    return _in_memory_span_exporter.get_finished_spans()


def _force_flush_spans() -> None:
    tracer_provider = cast(trace_sdk.TracerProvider, trace_api.get_tracer_provider())
    assert isinstance(tracer_provider, trace_sdk.TracerProvider)
    tracer_provider.force_flush()


S = TypeVar("S")
T = TypeVar("T")


class TestApplySpanner(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        _clear_in_memory_spans()
        self.tracer = get_tracer(__name__)

    async def test(self) -> None:

        class Example(BaseEventStoreDBClient):
            def double(self, /, x: int, name: str = "") -> int:
                if x < 0:
                    raise ValueError(f"Negative value {name}")
                return 2 * x

            async def adouble(self, /, x: int, name: str = "") -> int:
                if x < 0:
                    raise ValueError(f"Negative value {name}")
                return 2 * x

            @property
            def connection_target(self) -> str:
                return ""

        self.check_spans(num_spans=0)

        # Check example class.
        example = Example("esdb://blah?Tls=False")
        self.assertEqual(6, example.double(3, name="test1"))
        self.assertEqual(6, await example.adouble(3, name="test2"))
        with self.assertRaises(ValueError):
            example.double(-3, name="test3")
        with self.assertRaises(ValueError):
            await example.adouble(-3, name="test4")

        self.check_spans(num_spans=0)

        class DoubleMethod(Protocol):
            def __call__(self, /, x: int, name: str = "") -> int: ...

        class AsyncDoubleMethod(Protocol):
            async def __call__(self, /, x: int, name: str = "") -> int: ...

        # Define example spanner.
        @overload
        def example_spanner(
            tracer: Tracer,
            instance: Example,
            spanned_func: DoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        @overload
        def example_spanner(
            tracer: Tracer,
            instance: Example,
            spanned_func: AsyncDoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        def example_spanner(
            tracer: Tracer,
            instance: Example,
            spanned_func: Union[DoubleMethod, AsyncDoubleMethod],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        # Apply instrumenting wrapper to sync and async methods.
        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example_spanner,
            tracer=self.tracer,
        )

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example_spanner,
            tracer=self.tracer,
        )

        self.check_spans(num_spans=0)

        x = example.double(3, name="test5")
        self.assertEqual(6, x)

        self.check_spans(num_spans=1, span_index=0, span_name="test5")

        x = await example.adouble(3, name="test6")
        self.assertEqual(6, x)

        self.check_spans(num_spans=2, span_index=1, span_name="test6")

        with self.assertRaises(ValueError) as cm:
            example.double(-3, name="test7")

        self.check_spans(
            num_spans=3,
            span_index=2,
            span_name="test7",
            error=cm.exception,
        )

        with self.assertRaises(ValueError) as cm:
            await example.adouble(-3, name="test8")

        self.check_spans(
            num_spans=4,
            span_index=3,
            span_name="test8",
            error=cm.exception,
        )

        #
        ## Check mypy raises errors from broken static typing.
        #

        # Mismatched receiver of the spanned func. The spanned_func
        # is defined on Example2, but the patched_class is Example.

        class Example2(BaseEventStoreDBClient):
            def double(self, /, x: int, name: str = "") -> int:
                if x < 0:
                    raise ValueError(f"Negative value {name}")
                return 2 * x

            async def adouble(self, /, x: int, name: str = "") -> int:
                if x < 0:
                    raise ValueError(f"Negative value {name}")
                return 2 * x

            @property
            def connection_target(self) -> str:
                return ""

        apply_spanner(
            patched_class=Example,
            spanned_func=Example2.double,  # type: ignore
            spanner_func=example_spanner,
            tracer=self.tracer,
        )

        apply_spanner(
            patched_class=Example,
            spanned_func=Example2.adouble,  # type: ignore
            spanner_func=example_spanner,
            tracer=self.tracer,
        )

        # Mismatched spanned function arg type. It has "y: int"
        # which doesn't match the type of its spanned_func arg.

        def example2_spanner(
            tracer: Tracer,
            instance: Example2,
            spanned_func: DoubleMethod,
            /,
            x: int,
            y: int,
            name: str = "",
        ) -> SpannerResponse[int]:

            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, y, name)  # type: ignore
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        # Mismatched spanner_func arg value. The spanner function
        # "example3_spanner()" supports DoubleMethod3 with its args,
        # and its instance type is Example, but its args and the method
        # signatures of DoubleMethod3 and AsyncDoubleMethod3 don't match
        # the signature of the spanned functions Example.double and
        # Example.adouble.

        class DoubleMethod3(Protocol):
            def __call__(self, /, x: int, y: int, name: str = "") -> int: ...

        class AsyncDoubleMethod3(Protocol):
            async def __call__(self, /, x: int, y: int, name: str = "") -> int: ...

        @overload
        def example3_spanner(
            tracer: Tracer,
            instance: Example,
            spanned_func: DoubleMethod3,
            /,
            x: int,
            y: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        @overload
        def example3_spanner(
            tracer: Tracer,
            instance: Example,
            spanned_func: AsyncDoubleMethod3,
            /,
            x: int,
            y: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        def example3_spanner(
            tracer: Tracer,
            instance: Example,
            spanned_func: Union[DoubleMethod3, AsyncDoubleMethod3],
            /,
            x: int,
            y: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:

            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, y, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example3_spanner,  # type: ignore
            tracer=self.tracer,
        )

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example3_spanner,  # type: ignore
            tracer=self.tracer,
        )

        # Invalid spanner func signature. The function "example4_spanner()"
        # isn't a SpannerFunc, because its tracer arg type is not Tracer.

        @overload
        def example4_spanner(
            tracer: int,
            instance: Example,
            spanned_func: DoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        @overload
        def example4_spanner(
            tracer: int,
            instance: Example,
            spanned_func: AsyncDoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        def example4_spanner(
            tracer: int,
            instance: Example,
            spanned_func: Union[DoubleMethod, AsyncDoubleMethod],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:
            with _start_span(
                tracer=cast(Tracer, tracer),
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example4_spanner,  # type: ignore
            tracer=self.tracer,
        )
        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example4_spanner,  # type: ignore
            tracer=self.tracer,
        )

        # Invalid spanner func signature. The function "example5_spanner()"
        # isn't a SpannerFunc, because its instance arg is not a superclass
        # of the receiver type of the spanned_func arg value (it's "int").

        @overload
        def example5_spanner(
            tracer: Tracer,
            instance: int,
            spanned_func: DoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        @overload
        def example5_spanner(
            tracer: Tracer,
            instance: int,
            spanned_func: AsyncDoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        def example5_spanner(
            tracer: Tracer,
            instance: int,
            spanned_func: Union[DoubleMethod, AsyncDoubleMethod],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example5_spanner,  # type: ignore
            tracer=self.tracer,
        )
        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example5_spanner,  # type: ignore
            tracer=self.tracer,
        )

        # This is okay because instance is "object".
        @overload
        def example6_spanner(
            tracer: Tracer,
            instance: object,
            spanned_func: DoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        @overload
        def example6_spanner(
            tracer: Tracer,
            instance: object,
            spanned_func: AsyncDoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        def example6_spanner(
            tracer: Tracer,
            instance: object,
            spanned_func: Union[DoubleMethod, AsyncDoubleMethod],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example6_spanner,
            tracer=self.tracer,
        )
        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example6_spanner,
            tracer=self.tracer,
        )

        # This is okay because instance is "BaseEventStoreDBClient".
        @overload
        def example7_spanner(
            tracer: Tracer,
            instance: BaseEventStoreDBClient,
            spanned_func: AsyncDoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        @overload
        def example7_spanner(
            tracer: Tracer,
            instance: BaseEventStoreDBClient,
            spanned_func: DoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        def example7_spanner(
            tracer: Tracer,
            instance: BaseEventStoreDBClient,
            spanned_func: Union[DoubleMethod, AsyncDoubleMethod],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example7_spanner,
            tracer=self.tracer,
        )
        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example7_spanner,
            tracer=self.tracer,
        )

        # This is not okay because spanner func instance arg is type "SubExample" - a subclass.
        class SubExample(Example):
            pass

        @overload
        def example8_spanner(
            tracer: Tracer,
            instance: SubExample,
            spanned_func: AsyncDoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[int]: ...

        @overload
        def example8_spanner(
            tracer: Tracer,
            instance: SubExample,
            spanned_func: DoubleMethod,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[int]: ...

        def example8_spanner(
            tracer: Tracer,
            instance: SubExample,
            spanned_func: Union[DoubleMethod, AsyncDoubleMethod],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[int, int]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example,
            spanned_func=Example.double,
            spanner_func=example8_spanner,  # type: ignore
            tracer=self.tracer,
        )
        apply_spanner(
            patched_class=Example,
            spanned_func=Example.adouble,
            spanner_func=example8_spanner,  # type: ignore
            tracer=self.tracer,
        )

        # Here we have different return values from the sync and async methods.
        #  - this should all work

        class SyncInt(int):
            pass

        class AsyncInt(int):
            pass

        class DoubleMethod9(Protocol):
            def __call__(self, /, x: int, name: str = "") -> SyncInt: ...

        class AsyncDoubleMethod9(Protocol):
            async def __call__(self, /, x: int, name: str = "") -> AsyncInt: ...

        class Example9:
            def double(self, /, x: int, name: str = "") -> SyncInt:
                return SyncInt(x * 2)

            async def adouble(self, /, x: int, name: str = "") -> AsyncInt:
                return AsyncInt(x * 2)

        @overload
        def example9_spanner(
            tracer: Tracer,
            instance: Example9,
            spanned_func: AsyncDoubleMethod9,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[AsyncInt]: ...

        @overload
        def example9_spanner(
            tracer: Tracer,
            instance: Example9,
            spanned_func: DoubleMethod9,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[SyncInt]: ...

        def example9_spanner(
            tracer: Tracer,
            instance: Example9,
            spanned_func: Union[DoubleMethod9, AsyncDoubleMethod9],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[SyncInt, AsyncInt]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example9,
            spanned_func=Example9.double,
            spanner_func=example9_spanner,
            tracer=self.tracer,
        )

        apply_spanner(
            patched_class=Example9,
            spanned_func=Example9.adouble,
            spanner_func=example9_spanner,
            tracer=self.tracer,
        )

        # The same again, but with the spanner's instance arg type as a subclass.
        #  - this should all work

        class DoubleMethod10(Protocol):
            def __call__(self, /, x: int, name: str = "") -> SyncInt: ...

        class AsyncDoubleMethod10(Protocol):
            async def __call__(self, /, x: int, name: str = "") -> AsyncInt: ...

        class Example10(EventStoreDBClient):
            def double(self, /, x: int, name: str = "") -> SyncInt:
                return SyncInt(x * 2)

            async def adouble(self, /, x: int, name: str = "") -> AsyncInt:
                return AsyncInt(x * 2)

        @overload
        def example10_spanner(
            tracer: Tracer,
            instance: EventStoreDBClient,
            spanned_func: AsyncDoubleMethod10,
            /,
            x: int,
            name: str = "",
        ) -> AsyncSpannerResponse[AsyncInt]: ...

        @overload
        def example10_spanner(
            tracer: Tracer,
            instance: EventStoreDBClient,
            spanned_func: DoubleMethod10,
            /,
            x: int,
            name: str = "",
        ) -> SpannerResponse[SyncInt]: ...

        def example10_spanner(
            tracer: Tracer,
            instance: EventStoreDBClient,
            spanned_func: Union[DoubleMethod10, AsyncDoubleMethod10],
            /,
            x: int,
            name: str = "",
        ) -> OverloadedSpannerResponse[SyncInt, AsyncInt]:
            with _start_span(
                tracer=tracer,
                span_name=name,
                span_kind=SpanKind.CLIENT,
                end_on_exit=False,
            ) as span:
                try:
                    assert isinstance(instance, Example), instance
                    yield spanned_func(x, name)
                except Exception as e:
                    _set_span_error(span, e)
                    raise
                else:
                    _set_span_ok(span)
                finally:
                    span.end()

        apply_spanner(
            patched_class=Example10,
            spanned_func=Example10.double,
            spanner_func=example10_spanner,
            tracer=self.tracer,
        )

        apply_spanner(
            patched_class=Example10,
            spanned_func=Example10.adouble,
            spanner_func=example10_spanner,
            tracer=self.tracer,
        )

    def check_spans(
        self,
        num_spans: Optional[int] = None,
        span_index: Optional[int] = None,
        span_name: str = "",
        span_kind: trace_api.SpanKind = trace_api.SpanKind.CLIENT,
        parent_span_index: Optional[int] = None,
        error: Optional[Exception] = None,
    ) -> None:
        # Get all finished spans.
        spans = _get_in_memory_spans()

        # Check the number of finished spans.
        if num_spans is not None:
            self.assertEqual(num_spans, len(spans), len(spans))

        if span_index is None:
            return

        # Get a finished span.
        span: ReadableSpan = spans[span_index]

        # Check the span name.
        self.assertEqual(span_name, span.name)

        # Check the span kind.
        self.assertEqual(span_kind, span.kind)

        # Check the span's instrumentation scope.
        self.assertIsNotNone(span.instrumentation_scope)
        assert span.instrumentation_scope is not None  # for mypy
        self.assertIn(__name__, span.instrumentation_scope.name)

        # Check the span's resource attributes.
        resource_attributes = {
            "service.name": "eventstoredb",
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.25.0",
        }
        self.assertEqual(resource_attributes, span.resource.attributes)

        # Check span parent.
        if parent_span_index is not None:
            self.assertIsNotNone(span.parent)
            assert span.parent is not None  # for mypy
            parent_span = spans[parent_span_index]
            self.assertIsNotNone(parent_span.context)
            assert parent_span.context is not None  # for mypy
            # Check "streams.subscribe" parent span ID is the "streams.append" span ID.
            self.assertEqual(parent_span.context.span_id, span.parent.span_id)
            # Check "streams.subscribe" parent trace ID is the "streams.append" trace ID.
            self.assertEqual(parent_span.context.trace_id, span.parent.trace_id)
        else:
            if span.parent is not None:
                current_span = cast(trace_sdk.Span, trace_api.get_current_span())
                self.assertTrue(
                    current_span.name.startswith("test_"), current_span.name
                )
                context: Optional[SpanContext] = current_span.get_span_context()  # type: ignore[no-untyped-call]
                self.assertIsNotNone(context)
                assert context is not None  # for mypy
                self.assertEqual(context.span_id, span.parent.span_id)

        # Check the span attributes.
        span_attributes: Dict[str, AttributeValue] = {}
        self.assertIsNotNone(span.attributes)
        assert span.attributes is not None  # for mypy
        self.assertEqual(span_attributes, dict(span.attributes))

        # Check span status and span events.
        if error is None:
            # Check the status code is OK.
            self.assertEqual(trace_api.StatusCode.OK, span.status.status_code)
            # Check the status description is None.
            self.assertIsNone(span.status.description)
            # Check there are zero logged events.
            self.assertEqual(0, len(span.events))
        else:
            # Check the status code is ERROR.
            self.assertEqual(trace_api.StatusCode.ERROR, span.status.status_code)
            # Check the status description represents the error.
            self.assertEqual(
                f"{type(error).__name__}: {error}", span.status.description
            )
            # Check there is one logged event that details the error.
            self.assertEqual(1, len(span.events))
            self.assertEqual("exception", span.events[0].name)
            # Check event attributes.
            event_attributes = span.events[0].attributes
            self.assertIsNotNone(event_attributes)
            assert event_attributes is not None  # for mypy

            self.assertEqual(4, len(event_attributes))
            module = type(error).__module__
            qualname = type(error).__qualname__
            exception_type = (
                f"{module}.{qualname}" if module and module != "builtins" else qualname
            )
            self.assertEqual(exception_type, event_attributes["exception.type"])
            self.assertEqual(str(error), event_attributes["exception.message"])
            self.assertEqual(str(True), event_attributes["exception.escaped"])
            self.assertIn("Traceback", str(event_attributes["exception.stacktrace"]))


TEventStoreDBClient = TypeVar("TEventStoreDBClient", bound=BaseEventStoreDBClient)


class BaseEventStoreDBClientTestCase(TestCase, ABC, Generic[TEventStoreDBClient]):
    skip_check_spans = False

    def setUp(self) -> None:
        _clear_in_memory_spans()
        self.uri_schema = "esdb"
        self.user_info = "admin:changeit"
        self.grpc_target = "localhost:2113"
        self.qs = (
            "Tls=false&MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"
        )

    def tearDown(self) -> None:
        _clear_in_memory_spans()

    def construct_uri(
        self,
        uri_schema: str = "",
        user_info: str = "",
        grpc_target: str = "",
        qs: str = "",
    ) -> str:
        return (
            f"{uri_schema or self.uri_schema}://"
            f"{user_info or self.user_info}@"
            f"{grpc_target or self.grpc_target}"
            f"?{qs or self.qs}"
        )

    @abstractmethod
    def construct_client(
        self,
        uri_schema: str = "",
        user_info: str = "",
        grpc_target: str = "",
        qs: str = "",
    ) -> TEventStoreDBClient:
        pass  # pragma: no cover

    def check_spans(
        self,
        num_spans: Optional[int] = None,
        span_index: Optional[int] = None,
        span_name: str = "",
        span_kind: trace_api.SpanKind = trace_api.SpanKind.CLIENT,
        parent_span_index: Optional[int] = None,
        instrumentation_scope_name: str = "esdbclient.instrumentation.opentelemetry",
        instrumentation_scope_version: str = "1.1",
        span_attributes: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
        server_port: Optional[str] = None,
        rpc_service: str = "",
        rpc_status_code: int = 0,
        rpc_method: str = "",
    ) -> None:

        if self.skip_check_spans:
            return
        # if (
        #     GrpcInstrumentorClient().is_instrumented_by_opentelemetry
        #     or GrpcAioInstrumentorClient().is_instrumented_by_opentelemetry
        # ):
        #     return
        if (
            not EventStoreDBClientInstrumentor().is_instrumented_by_opentelemetry
            and not AsyncEventStoreDBClientInstrumentor().is_instrumented_by_opentelemetry
        ):
            return

        # Get all finished spans.
        spans = _get_in_memory_spans()

        # Check the number of finished spans.
        if num_spans is not None:
            self.assertEqual(num_spans, len(spans), len(spans))

        if span_index is None:
            return

        # Get a finished span.
        span = spans[span_index]

        # Check the span name.
        self.assertEqual(span_name, span.name)

        # Check the span kind.
        self.assertEqual(span_kind, span.kind)

        # Check the span's instrumentation scope.
        self.assertIsNotNone(span.instrumentation_scope)
        assert span.instrumentation_scope is not None  # for mypy
        self.assertEqual(instrumentation_scope_name, span.instrumentation_scope.name)
        self.assertEqual(
            instrumentation_scope_version, span.instrumentation_scope.version
        )

        # Check the span's resource attributes.
        resource_attributes = {
            "service.name": "eventstoredb",
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.25.0",
        }
        self.assertEqual(resource_attributes, span.resource.attributes)

        # Check the span attributes.
        if span_attributes is None:
            span_attributes = {}
        else:
            span_attributes = deepcopy(span_attributes)
        if rpc_service == "":
            span_attributes.update(
                {
                    "db.system": "eventstoredb",
                    "db.user": "admin",
                    "server.address": "localhost",
                    "server.port": server_port or "2113",
                }
            )
        else:
            span_attributes.update(
                {
                    "rpc.grpc.status_code": rpc_status_code,
                    "rpc.method": rpc_method,
                    "rpc.service": rpc_service,
                    "rpc.system": "grpc",
                }
            )

        self.assertIsNotNone(span.attributes)
        assert span.attributes is not None  # for mypy
        self.assertEqual(span_attributes, dict(span.attributes))

        # Check span status and span events.
        if error is None:
            if rpc_service == "":
                # Check the status code is OK.
                self.assertEqual(trace_api.StatusCode.OK, span.status.status_code)
            else:
                # Check the status code is UNSET.
                self.assertEqual(trace_api.StatusCode.UNSET, span.status.status_code)

            # Check the status description is None.
            self.assertIsNone(span.status.description)
            # Check there are zero logged events.
            self.assertEqual(0, len(span.events))
        else:
            # Check the status code is ERROR.
            self.assertEqual(trace_api.StatusCode.ERROR, span.status.status_code)
            # Check the status description represents the error.
            self.assertEqual(
                f"{type(error).__name__}: {error}", span.status.description
            )
            # Check there is one logged event that details the error.
            self.assertEqual(1, len(span.events))
            self.assertEqual("exception", span.events[0].name)
            event_attributes = span.events[0].attributes
            self.assertIsNotNone(event_attributes)
            assert event_attributes is not None  # for mypy

            self.assertEqual(4, len(event_attributes))
            module = type(error).__module__
            qualname = type(error).__qualname__
            exception_type = (
                f"{module}.{qualname}" if module and module != "builtins" else qualname
            )
            self.assertEqual(exception_type, event_attributes["exception.type"])
            self.assertEqual(str(error), event_attributes["exception.message"])
            self.assertEqual(str(True), event_attributes["exception.escaped"])
            self.assertIn("Traceback", str(event_attributes["exception.stacktrace"]))

        # Check span parent.
        if parent_span_index is not None:
            self.assertIsNotNone(span.parent)
            assert span.parent is not None  # for mypy
            parent_span = spans[parent_span_index]
            self.assertIsNotNone(parent_span.context)
            assert parent_span.context is not None  # for mypy
            # Check "streams.subscribe" parent span ID is the "streams.append" span ID.
            self.assertEqual(parent_span.context.span_id, span.parent.span_id)
            # Check "streams.subscribe" parent trace ID is the "streams.append" trace ID.
            self.assertEqual(parent_span.context.trace_id, span.parent.trace_id)
        else:
            self.assertIsNone(span.parent)
            # if span.parent is not None:
            #     current_span = cast(trace_sdk.Span, trace_api.get_current_span())
            #     self.assertTrue(
            #         current_span.name.startswith("test_"), current_span.name
            #     )
            #     self.assertEqual(
            #         current_span.get_span_context().span_id, span.parent.span_id
            #     )


class EventStoreDBClientInstrumentorTestCase(
    BaseEventStoreDBClientTestCase[EventStoreDBClient]
):
    instrument_get_and_read_stream = False
    instrument_grpc = False

    @classmethod
    def setUpClass(cls) -> None:
        EventStoreDBClientInstrumentor().instrument(
            instrument_get_and_read_stream=cls.instrument_get_and_read_stream
        )
        if cls.instrument_grpc:
            instrumentor = GrpcInstrumentorClient()  # type: ignore[no-untyped-call]
            instrumentor.instrument()

        # tracer = trace_api.get_tracer(__name__)
        # for name, func in cls.__dict__.items():
        #     if isinstance(func, FunctionType) and name.startswith("test_"):
        #         decorated_func = tracer.start_as_current_span(name)(func)
        #         setattr(cls, name, decorated_func)

    @classmethod
    def tearDownClass(cls) -> None:
        EventStoreDBClientInstrumentor().uninstrument()
        if cls.instrument_grpc:
            instrumentor = GrpcInstrumentorClient()  # type: ignore[no-untyped-call]
            instrumentor.uninstrument()

    def construct_client(
        self,
        uri_schema: str = "",
        user_info: str = "",
        grpc_target: str = "",
        qs: str = "",
    ) -> EventStoreDBClient:
        uri = self.construct_uri(uri_schema, user_info, grpc_target, qs)
        return EventStoreDBClient(uri)

    def get_next_and_check_consumer_span(
        self,
        subscription: Union[CatchupSubscription, PersistentSubscription],
        new_event: NewEvent,
        parent_span_index: Optional[int] = 0,
    ) -> None:
        # Count the number of spans.
        num_spans = len(_get_in_memory_spans())

        # Receive a recorded event from the subscription.
        recorded_event = next(subscription)

        self.assertEqual(new_event.id, recorded_event.id)

        # Check the spans.
        self.check_spans(
            num_spans=num_spans + 1,
            span_index=-1,
            parent_span_index=parent_span_index,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
                "db.eventstoredb.stream": recorded_event.stream_name,
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
        )

    @staticmethod
    def break_client_connection(client: EventStoreDBClient) -> None:
        client._connection._grpc_channel.close()
        client.connection_spec._targets = ["localhost:1000"]


class AsyncEventStoreDBClientInstrumentorTestCase(
    BaseEventStoreDBClientTestCase[AsyncEventStoreDBClient], IsolatedAsyncioTestCase
):
    instrument_get_and_read_stream = False
    instrument_grpc = False

    @classmethod
    def setUpClass(cls) -> None:
        AsyncEventStoreDBClientInstrumentor().instrument(
            instrument_get_and_read_stream=cls.instrument_get_and_read_stream
        )
        if cls.instrument_grpc:
            instrumentor = GrpcAioInstrumentorClient()  # type: ignore[no-untyped-call]
            instrumentor.instrument()

        # tracer = trace_api.get_tracer(__name__)
        # for name, func in cls.__dict__.items():
        #     if isinstance(func, FunctionType) and name.startswith("test_"):
        #         decorated_func = tracer.start_as_current_span(name)(func)
        #         setattr(cls, name, decorated_func)

    @classmethod
    def tearDownClass(cls) -> None:
        AsyncEventStoreDBClientInstrumentor().uninstrument()
        if cls.instrument_grpc:
            instrumentor = GrpcAioInstrumentorClient()  # type: ignore[no-untyped-call]
            instrumentor.uninstrument()

    def construct_client(
        self,
        uri_schema: str = "",
        user_info: str = "",
        grpc_target: str = "",
        qs: str = "",
    ) -> AsyncEventStoreDBClient:
        uri = self.construct_uri(uri_schema, user_info, grpc_target, qs)
        return AsyncEventStoreDBClient(uri)

    def get_next_and_check_consumer_span(
        self,
        subscription: Union[CatchupSubscription, PersistentSubscription],
        new_event: NewEvent,
        parent_span_index: Optional[int] = 0,
    ) -> None:
        # Count the number of spans.
        num_spans = len(_get_in_memory_spans())

        # Receive a recorded event from the subscription.
        recorded_event = next(subscription)

        self.assertEqual(new_event.id, recorded_event.id)

        # Check the spans.
        self.check_spans(
            num_spans=num_spans + 1,
            span_index=-1,
            parent_span_index=parent_span_index,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
                "db.eventstoredb.stream": recorded_event.stream_name,
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
        )

    async def async_get_next_and_check_consumer_span(
        self,
        subscription: Union[AsyncCatchupSubscription, AsyncPersistentSubscription],
        new_event: NewEvent,
        parent_span_index: Optional[int] = 0,
    ) -> None:
        # Count the number of spans.
        num_spans = len(_get_in_memory_spans())

        # Receive a recorded event from the subscription.
        recorded_event = await subscription.__anext__()

        self.assertEqual(new_event.id, recorded_event.id)

        # Check the spans.
        self.check_spans(
            num_spans=num_spans + 1,
            span_index=-1,
            parent_span_index=parent_span_index,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
                "db.eventstoredb.stream": recorded_event.stream_name,
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
        )

    @staticmethod
    async def async_break_client_connection(client: AsyncEventStoreDBClient) -> None:
        await client._connection._grpc_channel.close(grace=None)
        client.connection_spec._targets = ["localhost:1000"]


class BaseUtilsTestCase(BaseEventStoreDBClientTestCase[TEventStoreDBClient]):
    def test_span_helpers(self) -> None:
        tracer = get_tracer("esdbclient.instrumentation.opentelemetry", "1.1")
        client = self.construct_client()
        self.check_spans(num_spans=0)
        with _start_span(tracer, "test_enrich_span1", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
                db_operation_name="test1",
            )
            _set_span_ok(span)
        self.check_spans(
            num_spans=1,
            span_index=0,
            span_name="test_enrich_span1",
            span_kind=SpanKind.INTERNAL,
            span_attributes={"db.operation": "test1"},
        )

        with _start_span(tracer, "test_enrich_span2", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
                stream_name="test1",
            )
            _set_span_ok(span)
        self.check_spans(
            num_spans=2,
            span_index=1,
            span_name="test_enrich_span2",
            span_kind=SpanKind.INTERNAL,
            span_attributes={"db.eventstoredb.stream": "test1"},
        )

        with _start_span(tracer, "test_enrich_span3", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
                subscription_id="test1",
            )
            _set_span_ok(span)
        self.check_spans(
            num_spans=3,
            span_index=2,
            span_name="test_enrich_span3",
            span_kind=SpanKind.INTERNAL,
            span_attributes={"db.eventstoredb.subscription.id": "test1"},
        )

        with _start_span(tracer, "test_enrich_span4", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
                event_id="test1",
            )
            _set_span_ok(span)
        self.check_spans(
            num_spans=4,
            span_index=3,
            span_name="test_enrich_span4",
            span_kind=SpanKind.INTERNAL,
            span_attributes={"db.eventstoredb.event.id": "test1"},
        )

        with _start_span(tracer, "test_enrich_span5", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
                event_type="test1",
            )
            _set_span_ok(span)
        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="test_enrich_span5",
            span_kind=SpanKind.INTERNAL,
            span_attributes={"db.eventstoredb.event.type": "test1"},
        )

        # Check doesn't enrich when span has ended.
        with _start_span(
            tracer, "test_enrich_span6", SpanKind.INTERNAL, end_on_exit=False
        ) as span:
            _enrich_span(
                span=span,
                client=client,
            )
            _set_span_ok(span)
            span.end()
            _enrich_span(
                span=span,
                client=client,
                db_operation_name="test1",
                stream_name="test1",
                subscription_id="test1",
                event_id="test1",
                event_type="test1",
            )
        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="test_enrich_span6",
            span_kind=SpanKind.INTERNAL,
            span_attributes={},
        )

        # Check doesn't enrich when span has ended.
        with _start_span(
            tracer, "test_enrich_span7", SpanKind.INTERNAL, end_on_exit=False
        ) as span:
            _enrich_span(
                span=span,
                client=client,
            )
            _set_span_ok(span)
            span.end()
            _enrich_span(
                span=span,
                client=client,
                db_operation_name="test1",
                stream_name="test1",
                subscription_id="test1",
                event_id="test1",
                event_type="test1",
            )
        self.check_spans(
            num_spans=7,
            span_index=6,
            span_name="test_enrich_span7",
            span_kind=SpanKind.INTERNAL,
            span_attributes={},
        )

        # Check _set_span_error.
        error: Optional[Exception] = None
        with _start_span(tracer, "test_enrich_span8", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
            )
            try:
                raise ValueError()
            except ValueError as e:
                error = e
                _set_span_error(span, e)
        self.check_spans(
            num_spans=8,
            span_index=7,
            span_name="test_enrich_span8",
            span_kind=SpanKind.INTERNAL,
            span_attributes={},
            error=error,
        )

        # Check child span started with parent context.
        with _start_span(tracer, "test_enrich_span9", SpanKind.INTERNAL) as parent:
            _enrich_span(
                span=parent,
                client=client,
            )
            with _start_span(tracer, "test_enrich_span10", SpanKind.INTERNAL) as span:
                _enrich_span(
                    span=span,
                    client=client,
                )
                _set_span_ok(span)
            _set_span_ok(parent)
        self.check_spans(
            num_spans=10,
            span_index=8,
            span_name="test_enrich_span10",
            span_kind=SpanKind.INTERNAL,
            parent_span_index=9,
        )
        self.check_spans(
            num_spans=10,
            span_index=9,
            span_name="test_enrich_span9",
            span_kind=SpanKind.INTERNAL,
        )

        # Cover "quality of life" alternative for service address and port.
        client = self.construct_client(uri_schema="esdb+discover")
        with _start_span(tracer, "test_enrich_span11", SpanKind.INTERNAL) as span:
            _enrich_span(
                span=span,
                client=client,
            )
            _set_span_ok(span)
        self.check_spans(
            num_spans=11,
            span_index=10,
            span_name="test_enrich_span11",
            span_kind=SpanKind.INTERNAL,
            span_attributes={},
        )


class TestUtils(
    EventStoreDBClientInstrumentorTestCase, BaseUtilsTestCase[EventStoreDBClient]
):
    def test_propagate_context_via_events(self) -> None:
        context = INVALID_SPAN_CONTEXT

        # Single event, empty metadata.
        event1 = NewEvent("SomethingHappened", b"{}", b"")
        events = _set_context_in_events(context, event1)
        expected_events = [
            NewEvent(
                type="SomethingHappened",
                data=b"{}",
                metadata=b'{"$spanId": "0x0", "$traceId": "0x0"}',
                id=event1.id,
            )
        ]
        self.assertEqual(repr(expected_events), repr(events))

        # Single event, json metadata.
        event1 = NewEvent("SomethingHappened", b"{}", b"{}")
        events = _set_context_in_events(context, event1)
        expected_events = [
            NewEvent(
                type="SomethingHappened",
                data=b"{}",
                metadata=b'{"$spanId": "0x0", "$traceId": "0x0"}',
                id=event1.id,
            )
        ]
        self.assertEqual(repr(expected_events), repr(events))

        # Single event, json metadata.
        event1 = NewEvent("SomethingHappened", b"{}", b'{"my-key": "my-value"}')
        events = _set_context_in_events(context, event1)
        expected_events = [
            NewEvent(
                type="SomethingHappened",
                data=b"{}",
                metadata=b'{"my-key": "my-value", "$spanId": "0x0", "$traceId": "0x0"}',
                id=event1.id,
            )
        ]
        self.assertEqual(repr(expected_events), repr(events))

        # Single event, non-json metadata.
        event1 = NewEvent("SomethingHappened", b"{}", b"12345")
        events = _set_context_in_events(context, event1)
        expected_events = [
            NewEvent(
                type="SomethingHappened",
                data=b"{}",
                metadata=b"12345",
                id=event1.id,
            )
        ]
        self.assertEqual(repr(expected_events), repr(events))

        # Multiple events.
        event1 = NewEvent("SomethingHappened", b"{}", b"")
        event2 = NewEvent("SomethingHappened", b"{}", b"12345")
        events = _set_context_in_events(context, [event1, event2])
        expected_events = [
            NewEvent(
                type="SomethingHappened",
                data=b"{}",
                metadata=b'{"$spanId": "0x0", "$traceId": "0x0"}',
                id=event1.id,
            ),
            NewEvent(
                type="SomethingHappened",
                data=b"{}",
                metadata=b"12345",
                id=event2.id,
            ),
        ]
        self.assertEqual(repr(expected_events), repr(events))


class AsyncTestUtils(
    AsyncEventStoreDBClientInstrumentorTestCase,
    BaseUtilsTestCase[AsyncEventStoreDBClient],
):
    def construct_client(
        self,
        uri_schema: str = "",
        user_info: str = "",
        grpc_target: str = "",
        qs: str = "",
    ) -> AsyncEventStoreDBClient:
        client = super().construct_client(uri_schema, user_info, grpc_target, qs)
        asyncio.get_event_loop().run_until_complete(client.connect())
        return client


class TestWhatAlexeyAskedFor(EventStoreDBClientInstrumentorTestCase):
    def test_append_to_stream(self) -> None:
        # Construct client.
        client = self.construct_client()

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())

        event_with_json_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b'{"my-key": "my-value"}',
        )
        event_with_empty_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b"",
        )
        event_with_non_json_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b"0123456",
        )
        client.append_to_stream(
            stream_name,
            events=[
                event_with_json_metadata,
                event_with_empty_metadata,
                event_with_non_json_metadata,
            ],
            current_version=StreamState.NO_STREAM,
        )

        # Check there is one "producer" span.
        self.check_spans(
            num_spans=1,
            span_index=0,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Get the span context.
        span_context = _get_in_memory_spans()[0].context

        # Get the recorded events.
        events = client.get_stream(stream_name)

        # Check we didn't generate any new spans by reading the stream.
        self.check_spans(num_spans=1)

        # Check recorded event metadata has correct span and trace IDs.
        def extract_metadata_span_id(event: RecordedEvent) -> int:
            return extract_metadata_int_from_hex(event, "$spanId")

        def extract_metadata_trace_id(event: RecordedEvent) -> int:
            return extract_metadata_int_from_hex(event, "$traceId")

        def extract_metadata_int_from_hex(event: RecordedEvent, key: str) -> int:
            return int(json.loads(event.metadata)[key], 16)

        # First event should have span context.
        self.assertEqual(event_with_json_metadata.id, events[0].id)
        self.assertEqual(span_context.span_id, extract_metadata_span_id(events[0]))
        self.assertEqual(span_context.trace_id, extract_metadata_trace_id(events[0]))
        # - original metadata should be conserved
        self.assertEqual("my-value", json.loads(events[0].metadata)["my-key"])

        # Second event should have span context.
        self.assertEqual(event_with_empty_metadata.id, events[1].id)
        self.assertEqual(span_context.span_id, extract_metadata_span_id(events[1]))
        self.assertEqual(span_context.trace_id, extract_metadata_trace_id(events[1]))

        # Third event should have original metadata (no span context).
        self.assertEqual(event_with_non_json_metadata.id, events[2].id)
        self.assertEqual(event_with_non_json_metadata.metadata, events[2].metadata)

        # Cover edge cases:
        # - events arg is a single NewEvent
        # - stream_name is keyword arg
        client.append_to_stream(
            stream_name=stream_name,
            events=NewEvent("SomethingHappened", b""),
            current_version=2,
        )
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check span after error.
        self.break_client_connection(client)
        with self.assertRaises(ServiceUnavailable) as cm:
            client.append_to_stream(
                stream_name=stream_name,
                events=NewEvent("SomethingHappened", b""),
                current_version=2,
            )
        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
            error=cm.exception,
        )

    def test_esdb_discover(self) -> None:
        # Construct client.
        client = self.construct_client(uri_schema="esdb+discover")

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())

        event_with_json_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b'{"my-key": "my-value"}',
        )
        event_with_empty_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b"",
        )
        event_with_non_json_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b"0123456",
        )
        client.append_to_stream(
            stream_name,
            events=[
                event_with_json_metadata,
                event_with_empty_metadata,
                event_with_non_json_metadata,
            ],
            current_version=StreamState.NO_STREAM,
        )

        # Check there is one "producer" span.
        self.check_spans(
            num_spans=1,
            span_index=0,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Get the span context.
        span_context = _get_in_memory_spans()[0].context

        # Get the recorded events.
        events = client.get_stream(stream_name)

        # Check we didn't generate any new spans by reading the stream.
        self.check_spans(num_spans=1)

        # Check recorded event metadata has correct span and trace IDs.
        def extract_metadata_span_id(event: RecordedEvent) -> int:
            return extract_metadata_int_from_hex(event, "$spanId")

        def extract_metadata_trace_id(event: RecordedEvent) -> int:
            return extract_metadata_int_from_hex(event, "$traceId")

        def extract_metadata_int_from_hex(event: RecordedEvent, key: str) -> int:
            return int(json.loads(event.metadata)[key], 16)

        # First event should have span context.
        self.assertEqual(event_with_json_metadata.id, events[0].id)
        self.assertEqual(span_context.span_id, extract_metadata_span_id(events[0]))
        self.assertEqual(span_context.trace_id, extract_metadata_trace_id(events[0]))
        # - original metadata should be conserved
        self.assertEqual("my-value", json.loads(events[0].metadata)["my-key"])

        # Second event should have span context.
        self.assertEqual(event_with_empty_metadata.id, events[1].id)
        self.assertEqual(span_context.span_id, extract_metadata_span_id(events[1]))
        self.assertEqual(span_context.trace_id, extract_metadata_trace_id(events[1]))

        # Third event should have original metadata (no span context).
        self.assertEqual(event_with_non_json_metadata.id, events[2].id)
        self.assertEqual(event_with_non_json_metadata.metadata, events[2].metadata)

        # Cover edge cases:
        # - events arg is a single NewEvent
        # - stream_name is keyword arg
        client.append_to_stream(
            stream_name=stream_name,
            events=NewEvent("SomethingHappened", b""),
            current_version=2,
        )
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check span after error.
        self.break_client_connection(client)
        with self.assertRaises(DiscoveryFailed) as cm:
            client.append_to_stream(
                stream_name=stream_name,
                events=NewEvent("SomethingHappened", b""),
                current_version=2,
            )
        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
            error=cm.exception,
            server_port="1000",
        )

    def test_subscribe_to_stream(self) -> None:
        client = self.construct_client()
        cm: _AssertRaisesContext[Any]

        # Subscribe to a stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        subscription = client.subscribe_to_stream(stream_name)

        # Check supports iter.
        self.assertIs(subscription, iter(subscription))

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        self.get_next_and_check_consumer_span(subscription, new_events[0])
        self.get_next_and_check_consumer_span(subscription, new_events[1])
        self.get_next_and_check_consumer_span(subscription, new_events[2], None)

        # Check wrapper supports context manager.
        with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopIteration):
            next(subscription)

        # Check wrapper supports stop().
        subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        subscription = client.subscribe_to_stream(stream_name)
        self.break_client_connection(client)

        with self.assertRaises(GrpcError) as cm:
            next(subscription)

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            client.subscribe_to_stream(stream_name)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )

    def test_subscribe_to_all(self) -> None:
        client = self.construct_client()
        cm: _AssertRaisesContext[Any]

        # Subscribe to all.
        commit_position = client.get_commit_position()
        subscription = client.subscribe_to_all(commit_position=commit_position)

        # Check supports iter.
        self.assertIs(subscription, iter(subscription))

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        self.get_next_and_check_consumer_span(subscription, new_events[0])
        self.get_next_and_check_consumer_span(subscription, new_events[1])
        self.get_next_and_check_consumer_span(subscription, new_events[2], None)

        # Check wrapper supports context manager.
        with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopIteration):
            next(subscription)

        # Check wrapper supports stop().
        subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        subscription = client.subscribe_to_all(commit_position=commit_position)
        self.break_client_connection(client)

        with self.assertRaises(GrpcError) as cm:
            next(subscription)

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            client.subscribe_to_all(from_end=True)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )

    def test_read_subscription_to_stream(self) -> None:
        client = self.construct_client()
        cm: _AssertRaisesContext[Any]

        # Create and read subscription to a stream.
        group_name = "instrumentation-test-" + str(uuid4())
        stream_name = "instrumentation-test-" + str(uuid4())
        client.create_subscription_to_stream(group_name, stream_name)
        subscription = client.read_subscription_to_stream(group_name, stream_name)

        # Check supports iter.
        self.assertIs(subscription, iter(subscription))

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        self.get_next_and_check_consumer_span(subscription, new_events[0])
        self.get_next_and_check_consumer_span(subscription, new_events[1])
        self.get_next_and_check_consumer_span(subscription, new_events[2], None)

        # Check wrapper supports ack and nack.
        subscription.nack(new_events[0].id, "retry")
        subscription.ack(new_events[0].id)

        # Check wrapper supports context manager.
        with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopIteration):
            next(subscription)

        # Check wrapper supports stop().
        subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        group_name = "instrumentation-test-" + str(uuid4())
        client.create_subscription_to_stream(group_name, stream_name)
        subscription = client.read_subscription_to_stream(group_name, stream_name)
        self.break_client_connection(client)

        with self.assertRaises(GrpcError) as cm:
            next(subscription)

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )
        subscription.stop()

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            client.read_subscription_to_stream(group_name, stream_name)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )

    def test_read_subscription_to_all(self) -> None:
        client = self.construct_client()
        cm: _AssertRaisesContext[Any]

        # Create and read subscription to all.
        group_name = "instrumentation-test-" + str(uuid4())
        client.create_subscription_to_all(group_name, from_end=True)
        subscription = client.read_subscription_to_all(group_name)

        # Check supports iter.
        self.assertIs(subscription, iter(subscription))

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        self.get_next_and_check_consumer_span(subscription, new_events[0])
        self.get_next_and_check_consumer_span(subscription, new_events[1])
        self.get_next_and_check_consumer_span(subscription, new_events[2], None)

        # Check wrapper supports ack and nack.
        subscription.nack(new_events[0].id, "retry")
        subscription.ack(new_events[0].id)

        # Check wrapper supports context manager.
        with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopIteration):
            next(subscription)

        # Check wrapper supports stop().
        subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        group_name = "instrumentation-test-" + str(uuid4())
        client.create_subscription_to_all(group_name, from_end=True)
        subscription = client.read_subscription_to_all(group_name)
        self.break_client_connection(client)

        with self.assertRaises(GrpcError) as cm:
            next(subscription)

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            client.read_subscription_to_all(group_name)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )


class AsyncTestWhatAlexeyAskedFor(AsyncEventStoreDBClientInstrumentorTestCase):
    # instrument_grpc = True

    async def test_append_to_stream(self) -> None:
        # Construct client.
        client = self.construct_client()
        await client.connect()

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())

        event_with_json_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b'{"my-key": "my-value"}',
        )
        event_with_empty_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b"",
        )
        event_with_non_json_metadata = NewEvent(
            type="SomethingHappened",
            data=b"{}",
            metadata=b"0123456",
        )
        await client.append_to_stream(
            stream_name,
            events=[
                event_with_json_metadata,
                event_with_empty_metadata,
                event_with_non_json_metadata,
            ],
            current_version=StreamState.NO_STREAM,
        )

        # Check there is one "producer" span.
        self.check_spans(
            num_spans=1,
            span_index=0,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Get the span context.
        spans = _get_in_memory_spans()
        span_context = spans[0].context

        # Get the recorded events.
        events = await client.get_stream(stream_name)

        # Check we didn't generate any new spans by reading the stream.
        self.check_spans(num_spans=1)

        # Check recorded event metadata has correct span and trace IDs.
        def extract_metadata_span_id(event: RecordedEvent) -> int:
            return extract_metadata_int_from_hex(event, "$spanId")

        def extract_metadata_trace_id(event: RecordedEvent) -> int:
            return extract_metadata_int_from_hex(event, "$traceId")

        def extract_metadata_int_from_hex(event: RecordedEvent, key: str) -> int:
            return int(json.loads(event.metadata)[key], 16)

        # First event should have span context.
        self.assertEqual(event_with_json_metadata.id, events[0].id)
        self.assertEqual(span_context.span_id, extract_metadata_span_id(events[0]))
        self.assertEqual(span_context.trace_id, extract_metadata_trace_id(events[0]))
        # - original metadata should be conserved
        self.assertEqual("my-value", json.loads(events[0].metadata)["my-key"])

        # Second event should have span context.
        self.assertEqual(event_with_empty_metadata.id, events[1].id)
        self.assertEqual(span_context.span_id, extract_metadata_span_id(events[1]))
        self.assertEqual(span_context.trace_id, extract_metadata_trace_id(events[1]))

        # Third event should have original metadata (no span context).
        self.assertEqual(event_with_non_json_metadata.id, events[2].id)
        self.assertEqual(event_with_non_json_metadata.metadata, events[2].metadata)

        # Cover edge cases:
        # - events arg is a single NewEvent
        # - stream_name is keyword arg
        await client.append_to_stream(
            stream_name=stream_name,
            events=NewEvent("SomethingHappened", b""),
            current_version=2,
        )
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check span after error.
        await self.async_break_client_connection(client)
        with self.assertRaises(ServiceUnavailable) as cm:
            await client.append_to_stream(
                stream_name=stream_name,
                events=NewEvent("SomethingHappened", b""),
                current_version=2,
            )
        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
            error=cm.exception,
        )

    async def test_subscribe_to_stream(self) -> None:
        client = self.construct_client()
        await client.connect()

        cm: _AssertRaisesContext[Any]

        # Subscribe to a stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        subscription = await client.subscribe_to_stream(stream_name)

        # Check supports iter.
        self.assertIs(subscription, subscription.__aiter__())

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        await client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        await self.async_get_next_and_check_consumer_span(subscription, new_events[0])
        await self.async_get_next_and_check_consumer_span(subscription, new_events[1])
        await self.async_get_next_and_check_consumer_span(
            subscription, new_events[2], None
        )

        # Check wrapper supports context manager.
        async with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopAsyncIteration):
            await subscription.__anext__()

        # Check wrapper supports stop().
        await subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        subscription = await client.subscribe_to_stream(stream_name)
        subscription._set_iter_error_for_testing()
        await self.async_break_client_connection(client)

        with self.assertRaises(GrpcError) as cm:
            await subscription.__anext__()

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            await client.subscribe_to_stream(stream_name)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )

    async def test_subscribe_to_all(self) -> None:
        client = self.construct_client()
        await client.connect()
        cm: _AssertRaisesContext[Any]

        # Subscribe to all.
        commit_position = await client.get_commit_position()
        subscription = await client.subscribe_to_all(commit_position=commit_position)

        # Check supports iter.
        self.assertIs(subscription, subscription.__aiter__())

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        await client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        await self.async_get_next_and_check_consumer_span(subscription, new_events[0])
        await self.async_get_next_and_check_consumer_span(subscription, new_events[1])
        await self.async_get_next_and_check_consumer_span(
            subscription, new_events[2], None
        )

        # Check wrapper supports context manager.
        async with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopAsyncIteration):
            await subscription.__anext__()

        # Check wrapper supports stop().
        await subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        subscription = await client.subscribe_to_all(commit_position=commit_position)
        subscription._set_iter_error_for_testing()
        await self.async_break_client_connection(client)

        with self.assertRaises(GrpcError) as cm:
            await subscription.__anext__()

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            await client.subscribe_to_all(from_end=True)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )

    async def test_read_subscription_to_stream(self) -> None:
        client = self.construct_client()
        await client.connect()
        cm: _AssertRaisesContext[Any]

        # Create and read subscription to a stream.
        group_name = "instrumentation-test-" + str(uuid4())
        stream_name = "instrumentation-test-" + str(uuid4())
        await client.create_subscription_to_stream(group_name, stream_name)
        subscription = await client.read_subscription_to_stream(group_name, stream_name)

        # Check supports iter.
        self.assertIs(subscription, subscription.__aiter__())

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        await client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        await self.async_get_next_and_check_consumer_span(subscription, new_events[0])
        await self.async_get_next_and_check_consumer_span(subscription, new_events[1])
        await self.async_get_next_and_check_consumer_span(
            subscription, new_events[2], None
        )

        # Check wrapper supports ack and nack.
        await subscription.nack(new_events[0].id, "retry")
        await subscription.ack(new_events[0].id)

        # Check wrapper supports context manager.
        async with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopAsyncIteration):
            await subscription.__anext__()

        # Check wrapper supports stop().
        await subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        group_name = "instrumentation-test-" + str(uuid4())
        await client.create_subscription_to_stream(group_name, stream_name)
        subscription = await client.read_subscription_to_stream(group_name, stream_name)
        subscription._set_iter_error_for_testing()

        with self.assertRaises(GrpcError) as cm:
            await subscription.__anext__()

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )
        await subscription.stop()

        await self.async_break_client_connection(client)

        # Check span after error during method call.
        with self.assertRaises(ServiceUnavailable) as cm:
            await client.read_subscription_to_stream(group_name, stream_name)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )

    async def test_read_subscription_to_all(self) -> None:
        client = self.construct_client()
        await client.connect()
        cm: _AssertRaisesContext[Any]

        # Create and read subscription to all.
        group_name = "instrumentation-test-" + str(uuid4())
        await client.create_subscription_to_all(group_name, from_end=True)
        subscription = await client.read_subscription_to_all(group_name)

        # Check supports iter.
        self.assertIs(subscription, subscription.__aiter__())

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        await client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        # Check consumer spans are created by subscription.
        await self.async_get_next_and_check_consumer_span(subscription, new_events[0])
        await self.async_get_next_and_check_consumer_span(subscription, new_events[1])
        await self.async_get_next_and_check_consumer_span(
            subscription, new_events[2], None
        )

        # Check wrapper supports ack and nack.
        await subscription.nack(new_events[0].id, "retry")
        await subscription.ack(new_events[0].id)

        # Check wrapper supports context manager.
        async with subscription as s:
            self.assertIs(subscription, s)

        with self.assertRaises(StopAsyncIteration):
            await subscription.__anext__()

        # Check wrapper supports stop().
        await subscription.stop()

        # Check there are still four spans.
        self.check_spans(num_spans=4)

        # Check span after error during iteration.
        group_name = "instrumentation-test-" + str(uuid4())
        await client.create_subscription_to_all(group_name)
        subscription = await client.read_subscription_to_all(group_name)
        subscription._set_iter_error_for_testing()
        with self.assertRaises(GrpcError) as cm:
            await subscription.__anext__()

        self.check_spans(
            num_spans=5,
            span_index=4,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
                "db.eventstoredb.subscription.id": subscription.subscription_id,
            },
            error=cm.exception,
        )

        # Check span after error during method call.
        await self.async_break_client_connection(client)
        with self.assertRaises(ServiceUnavailable) as cm:
            await client.read_subscription_to_all(group_name)

        self.check_spans(
            num_spans=6,
            span_index=5,
            span_name="streams.subscribe",
            span_kind=trace_api.SpanKind.CONSUMER,
            span_attributes={
                "db.operation": "streams.subscribe",
            },
            error=cm.exception,
            server_port="1000",
        )


class TestReadAndGetStream(EventStoreDBClientInstrumentorTestCase):
    instrument_get_and_read_stream = True

    def test_read_stream(self) -> None:
        client = self.construct_client()

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append three events.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_event1 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event2 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event3 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        client.append_to_stream(
            stream_name=stream_name,
            events=[new_event1, new_event2, new_event3],
            current_version=StreamState.NO_STREAM,
        )

        # Check the first span.
        self.check_spans(
            num_spans=1,
            span_index=0,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        read_response = client.read_stream(stream_name=stream_name)

        # Check the second span.
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="EventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        recorded_event = next(read_response)

        # Check the third span.
        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = next(read_response)

        # Check the fourth span.
        self.check_spans(
            num_spans=4,
            span_index=3,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = next(read_response)

        # Check the fifth span.
        self.check_spans(
            num_spans=5,
            span_index=4,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        with self.assertRaises(StopIteration):
            next(read_response)

        # Check the sixth span.
        self.check_spans(
            num_spans=6,
            span_index=5,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
            },
        )

        # Check span after error during iteration.
        read_response = client.read_stream(stream_name)
        self.break_client_connection(client)
        with self.assertRaises(GrpcError):
            next(read_response)
        # Todo: Actually check the span...
        self.check_spans(
            num_spans=8,
        )

    def test_get_stream(self) -> None:
        client = self.construct_client()

        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        self.check_spans(num_spans=1)

        # Get stream.
        recorded_events = client.get_stream(stream_name)

        self.assertEqual(3, len(recorded_events))

        # Check the second span.
        self.check_spans(
            num_spans=7,
            span_index=1,
            parent_span_index=6,
            span_name="EventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check the third span.
        self.check_spans(
            num_spans=7,
            span_index=2,
            parent_span_index=6,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[0].id),
                "db.eventstoredb.event.type": recorded_events[0].type,
            },
        )

        # Check the fourth span.
        self.check_spans(
            num_spans=7,
            span_index=3,
            parent_span_index=6,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[1].id),
                "db.eventstoredb.event.type": recorded_events[1].type,
            },
        )

        # Check the fifth span.
        self.check_spans(
            num_spans=7,
            span_index=4,
            parent_span_index=6,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[2].id),
                "db.eventstoredb.event.type": recorded_events[2].type,
            },
        )

        # Check the sixth span.
        self.check_spans(
            num_spans=7,
            span_index=5,
            parent_span_index=6,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
            },
        )

        # Check the seventh span.
        self.check_spans(
            num_spans=7,
            span_index=6,
            parent_span_index=None,
            span_name="EventStoreDBClient.get_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.get_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check span after error during method call.
        self.break_client_connection(client)
        with self.assertRaises(ServiceUnavailable):
            client.get_stream(stream_name)

        self.check_spans(
            num_spans=15,
        )


class AsyncTestReadAndGetStream(AsyncEventStoreDBClientInstrumentorTestCase):
    instrument_get_and_read_stream = True

    async def test_read_stream(self) -> None:
        client = self.construct_client()
        await client.connect()

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append three events.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_event1 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event2 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event3 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        await client.append_to_stream(
            stream_name=stream_name,
            events=[new_event1, new_event2, new_event3],
            current_version=StreamState.NO_STREAM,
        )

        # Check the first span.
        self.check_spans(
            num_spans=1,
            span_index=0,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        read_response = await client.read_stream(stream_name=stream_name)

        # Check the second span.
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="AsyncEventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncEventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        recorded_event = await read_response.__anext__()

        # Check the third span.
        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = await read_response.__anext__()

        # Check the fourth span.
        self.check_spans(
            num_spans=4,
            span_index=3,
            parent_span_index=None,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = await read_response.__anext__()

        # Check the fifth span.
        self.check_spans(
            num_spans=5,
            span_index=4,
            parent_span_index=None,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        with self.assertRaises(StopAsyncIteration):
            await read_response.__anext__()

        # Check the sixth span.
        self.check_spans(
            num_spans=6,
            span_index=5,
            parent_span_index=None,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
            },
        )

        # Check span after error during iteration.
        await self.async_break_client_connection(client)
        read_response = await client.read_stream(stream_name)
        with self.assertRaises(GrpcError):
            await read_response.__anext__()
        # Todo: Actually check the span...
        self.check_spans(
            num_spans=8,
        )

    async def test_get_stream(self) -> None:
        client = self.construct_client()
        await client.connect()

        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        await client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        self.check_spans(num_spans=1)

        # Read stream.
        recorded_events = await client.get_stream(stream_name)

        self.assertEqual(3, len(recorded_events))

        # Check the second span.
        self.check_spans(
            num_spans=7,
            span_index=1,
            parent_span_index=6,
            span_name="AsyncEventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncEventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check the third span.
        self.check_spans(
            num_spans=7,
            span_index=2,
            parent_span_index=6,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[0].id),
                "db.eventstoredb.event.type": recorded_events[0].type,
            },
        )

        # Check the fourth span.
        self.check_spans(
            num_spans=7,
            span_index=3,
            parent_span_index=6,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[1].id),
                "db.eventstoredb.event.type": recorded_events[1].type,
            },
        )

        # Check the fifth span.
        self.check_spans(
            num_spans=7,
            span_index=4,
            parent_span_index=6,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[2].id),
                "db.eventstoredb.event.type": recorded_events[2].type,
            },
        )

        # Check the sixth span.
        self.check_spans(
            num_spans=7,
            span_index=5,
            parent_span_index=6,
            span_name="AsyncReadResponse.__anext__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncReadResponse.__anext__",
            },
        )

        # Check the seventh span.
        self.check_spans(
            num_spans=7,
            span_index=6,
            parent_span_index=None,
            span_name="AsyncEventStoreDBClient.get_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "AsyncEventStoreDBClient.get_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check span after error during method call.
        await self.async_break_client_connection(client)
        with self.assertRaises(ServiceUnavailable):
            await client.get_stream(stream_name)

        self.check_spans(
            num_spans=16,
        )


class TestReadAndGetStreamWithGrpcInstrumentor(EventStoreDBClientInstrumentorTestCase):
    instrument_get_and_read_stream = True
    instrument_grpc = True

    def test_read_stream(self) -> None:
        client = self.construct_client()

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append three events.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_event1 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event2 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event3 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        client.append_to_stream(
            stream_name=stream_name,
            events=[new_event1, new_event2, new_event3],
            current_version=StreamState.NO_STREAM,
        )

        self.check_spans(
            num_spans=2,
            span_index=0,
            parent_span_index=1,
            span_name="/event_store.client.streams.Streams/BatchAppend",
            span_kind=trace_api.SpanKind.CLIENT,
            instrumentation_scope_name="opentelemetry.instrumentation.grpc",
            instrumentation_scope_version="0.46b0",
            rpc_service="event_store.client.streams.Streams",
            rpc_method="BatchAppend",
        )
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        read_response = client.read_stream(stream_name=stream_name)

        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="EventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        recorded_event = next(read_response)

        self.check_spans(
            num_spans=4,
            span_index=3,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = next(read_response)

        self.check_spans(
            num_spans=5,
            span_index=4,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = next(read_response)

        self.check_spans(
            num_spans=6,
            span_index=5,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        with self.assertRaises(StopIteration):
            next(read_response)

        self.check_spans(
            num_spans=8,
            span_index=6,
            parent_span_index=2,
            span_name="/event_store.client.streams.Streams/Read",
            span_kind=trace_api.SpanKind.CLIENT,
            instrumentation_scope_name="opentelemetry.instrumentation.grpc",
            instrumentation_scope_version="0.46b0",
            rpc_service="event_store.client.streams.Streams",
            rpc_method="Read",
        )

        self.check_spans(
            num_spans=8,
            span_index=7,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
            },
        )

    def test_get_stream(self) -> None:
        client = self.construct_client()

        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )

        self.check_spans(num_spans=2)

        recorded_events = client.get_stream(stream_name)

        self.assertEqual(3, len(recorded_events))

        self.check_spans(
            num_spans=9,
            span_index=2,
            parent_span_index=8,
            span_name="EventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        self.check_spans(
            num_spans=9,
            span_index=3,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[0].id),
                "db.eventstoredb.event.type": recorded_events[0].type,
            },
        )

        self.check_spans(
            num_spans=9,
            span_index=4,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[1].id),
                "db.eventstoredb.event.type": recorded_events[1].type,
            },
        )

        self.check_spans(
            num_spans=9,
            span_index=5,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[2].id),
                "db.eventstoredb.event.type": recorded_events[2].type,
            },
        )

        self.check_spans(
            num_spans=9,
            span_index=6,
            parent_span_index=2,
            instrumentation_scope_name="opentelemetry.instrumentation.grpc",
            instrumentation_scope_version="0.46b0",
            span_name="/event_store.client.streams.Streams/Read",
            span_kind=trace_api.SpanKind.CLIENT,
            rpc_service="event_store.client.streams.Streams",
            rpc_method="Read",
        )

        self.check_spans(
            num_spans=9,
            span_index=7,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
            },
        )

        self.check_spans(
            num_spans=9,
            span_index=8,
            parent_span_index=None,
            span_name="EventStoreDBClient.get_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.get_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Close channel and check we get an error.
        self.break_client_connection(client)

        with self.assertRaises(ServiceUnavailable):
            client.get_stream(stream_name)

        self.check_spans(
            num_spans=21,
            # span_index=8,
            # parent_span_index=None,
            # span_name="EventStoreDBClient.get_stream",
            # span_kind=trace_api.SpanKind.CLIENT,
            # span_attributes={
            #     "db.operation": "EventStoreDBClient.get_stream",
            # },
        )


class AsyncTestReadAndGetStreamWithGrpcInstrumentor(
    AsyncEventStoreDBClientInstrumentorTestCase
):
    instrument_get_and_read_stream = True
    instrument_grpc = True

    skip_check_spans = True  # Somehow the GRPC spans aren't exiting... not sure why!

    async def test_read_stream(self) -> None:
        client = self.construct_client()
        await client.connect()

        # Check there are zero spans.
        self.check_spans(num_spans=0)

        # Append three events.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_event1 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event2 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        new_event3 = NewEvent(
            type="SomethingHappened",
            data=b"{}",
        )
        await client.append_to_stream(
            stream_name=stream_name,
            events=[new_event1, new_event2, new_event3],
            current_version=StreamState.NO_STREAM,
        )

        self.check_spans(
            num_spans=2,
            span_index=0,
            parent_span_index=1,
            span_name="/event_store.client.streams.Streams/BatchAppend",
            span_kind=trace_api.SpanKind.CLIENT,
            instrumentation_scope_name="opentelemetry.instrumentation.grpc",
            instrumentation_scope_version="0.46b0",
            rpc_service="event_store.client.streams.Streams",
            rpc_method="BatchAppend",
        )
        self.check_spans(
            num_spans=2,
            span_index=1,
            parent_span_index=None,
            span_name="streams.append",
            span_kind=trace_api.SpanKind.PRODUCER,
            span_attributes={
                "db.operation": "streams.append",
                "db.eventstoredb.stream": stream_name,
            },
        )

        read_response = await client.read_stream(stream_name=stream_name)

        self.check_spans(
            num_spans=3,
            span_index=2,
            parent_span_index=None,
            span_name="EventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        recorded_event = await read_response.__anext__()

        self.check_spans(
            num_spans=4,
            span_index=3,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = await read_response.__anext__()

        self.check_spans(
            num_spans=5,
            span_index=4,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        recorded_event = await read_response.__anext__()

        self.check_spans(
            num_spans=6,
            span_index=5,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_event.id),
                "db.eventstoredb.event.type": recorded_event.type,
            },
        )

        with self.assertRaises(StopAsyncIteration):
            await read_response.__anext__()

        self.check_spans(
            num_spans=8,
            span_index=6,
            parent_span_index=2,
            span_name="/event_store.client.streams.Streams/Read",
            span_kind=trace_api.SpanKind.CLIENT,
            instrumentation_scope_name="opentelemetry.instrumentation.grpc",
            instrumentation_scope_version="0.46b0",
            rpc_service="event_store.client.streams.Streams",
            rpc_method="Read",
        )

        self.check_spans(
            num_spans=8,
            span_index=7,
            parent_span_index=None,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
            },
        )

    async def test_get_stream(self) -> None:
        client = self.construct_client()
        await client.connect()

        self.check_spans(num_spans=0)

        # Append to stream.
        stream_name = "instrumentation-test-" + str(uuid4())
        new_events = [
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}"),
            NewEvent("SomethingHappened", b"{}", b"012345"),
        ]
        commit_position = await client.append_to_stream(
            stream_name=stream_name,
            events=new_events,
            current_version=StreamState.NO_STREAM,
        )
        self.assertIsInstance(commit_position, int)

        self.check_spans(num_spans=2)

        # Read stream.
        recorded_events = await client.get_stream(stream_name)

        self.assertEqual(3, len(recorded_events))

        # Check the second span.
        self.check_spans(
            num_spans=9,
            span_index=2,
            parent_span_index=8,
            span_name="EventStoreDBClient.read_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.read_stream",
                "db.eventstoredb.stream": stream_name,
            },
        )

        # Check the third span.
        self.check_spans(
            num_spans=9,
            span_index=3,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[0].id),
                "db.eventstoredb.event.type": recorded_events[0].type,
            },
        )

        # Check the fourth span.
        self.check_spans(
            num_spans=9,
            span_index=4,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[1].id),
                "db.eventstoredb.event.type": recorded_events[1].type,
            },
        )

        # Check the fifth span.
        self.check_spans(
            num_spans=9,
            span_index=5,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
                "db.eventstoredb.stream": stream_name,
                "db.eventstoredb.event.id": str(recorded_events[2].id),
                "db.eventstoredb.event.type": recorded_events[2].type,
            },
        )

        # Check the sixth span.
        self.check_spans(
            num_spans=9,
            span_index=6,
            parent_span_index=2,
            instrumentation_scope_name="opentelemetry.instrumentation.grpc",
            instrumentation_scope_version="0.46b0",
            span_name="/event_store.client.streams.Streams/Read",
            span_kind=trace_api.SpanKind.CLIENT,
            rpc_service="event_store.client.streams.Streams",
        )

        # Check the seventh span.
        self.check_spans(
            num_spans=9,
            span_index=7,
            parent_span_index=8,
            span_name="ReadResponse.__next__",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "ReadResponse.__next__",
            },
        )

        # Check the eighth span.
        self.check_spans(
            num_spans=9,
            span_index=8,
            parent_span_index=None,
            span_name="EventStoreDBClient.get_stream",
            span_kind=trace_api.SpanKind.CLIENT,
            span_attributes={
                "db.operation": "EventStoreDBClient.get_stream",
            },
        )


del BaseUtilsTestCase
