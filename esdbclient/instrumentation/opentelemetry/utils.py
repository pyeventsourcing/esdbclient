# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect
import json
import re
import traceback
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Generic,
    Iterator,
    Literal,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from uuid import UUID

from opentelemetry.context import Context
from opentelemetry.trace import (
    NonRecordingSpan,
    Span,
    SpanContext,
    SpanKind,
    Status,
    StatusCode,
    TraceFlags,
    Tracer,
    set_span_in_context,
)
from opentelemetry.util.types import AttributeValue
from typing_extensions import Self
from wrapt import FunctionWrapper, apply_patch

from esdbclient import (
    AsyncCatchupSubscription,
    AsyncEventStoreDBClient,
    AsyncPersistentSubscription,
    AsyncReadResponse,
    CatchupSubscription,
    EventStoreDBClient,
    NewEvent,
    PersistentSubscription,
    ReadResponse,
    RecordedEvent,
)
from esdbclient.client import BaseEventStoreDBClient
from esdbclient.common import (
    AsyncRecordedEventIterator,
    AsyncRecordedEventSubscription,
    RecordedEventIterator,
    RecordedEventSubscription,
)
from esdbclient.connection_spec import URI_SCHEME_ESDB_DISCOVER
from esdbclient.instrumentation.opentelemetry.attributes import Attributes

STREAMS_APPEND = "streams.append"
STREAMS_SUBSCRIBE = "streams.subscribe"
SPAN_NAMES_BY_CLIENT_METHOD = {
    EventStoreDBClient.append_to_stream.__qualname__: STREAMS_APPEND,
    EventStoreDBClient.subscribe_to_all.__qualname__: STREAMS_SUBSCRIBE,
    EventStoreDBClient.subscribe_to_stream.__qualname__: STREAMS_SUBSCRIBE,
    EventStoreDBClient.read_subscription_to_all.__qualname__: STREAMS_SUBSCRIBE,
    EventStoreDBClient.read_subscription_to_stream.__qualname__: STREAMS_SUBSCRIBE,
    AsyncEventStoreDBClient.append_to_stream.__qualname__: STREAMS_APPEND,
    AsyncEventStoreDBClient.subscribe_to_all.__qualname__: STREAMS_SUBSCRIBE,
    AsyncEventStoreDBClient.subscribe_to_stream.__qualname__: STREAMS_SUBSCRIBE,
    AsyncEventStoreDBClient.read_subscription_to_all.__qualname__: STREAMS_SUBSCRIBE,
    AsyncEventStoreDBClient.read_subscription_to_stream.__qualname__: STREAMS_SUBSCRIBE,
}
SPAN_KINDS_BY_CLIENT_METHOD = {
    EventStoreDBClient.append_to_stream.__qualname__: SpanKind.PRODUCER,
    EventStoreDBClient.subscribe_to_all.__qualname__: SpanKind.CONSUMER,
    EventStoreDBClient.subscribe_to_stream.__qualname__: SpanKind.CONSUMER,
    EventStoreDBClient.read_subscription_to_all.__qualname__: SpanKind.CONSUMER,
    EventStoreDBClient.read_subscription_to_stream.__qualname__: SpanKind.CONSUMER,
    AsyncEventStoreDBClient.append_to_stream.__qualname__: SpanKind.PRODUCER,
    AsyncEventStoreDBClient.subscribe_to_all.__qualname__: SpanKind.CONSUMER,
    AsyncEventStoreDBClient.subscribe_to_stream.__qualname__: SpanKind.CONSUMER,
    AsyncEventStoreDBClient.read_subscription_to_all.__qualname__: SpanKind.CONSUMER,
    AsyncEventStoreDBClient.read_subscription_to_stream.__qualname__: SpanKind.CONSUMER,
}


def _get_span_kind(func: Callable[..., Any]) -> SpanKind:
    return SPAN_KINDS_BY_CLIENT_METHOD.get(func.__qualname__, SpanKind.CLIENT)


def _get_span_name(func: Callable[..., Any]) -> str:
    return SPAN_NAMES_BY_CLIENT_METHOD.get(func.__qualname__, func.__qualname__)


def _get_span_name_and_kind(func: Callable[..., Any]) -> Tuple[str, SpanKind]:
    return _get_span_name(func), _get_span_kind(func)


@contextmanager
def _start_span(
    tracer: Tracer,
    span_name: str,
    span_kind: SpanKind = SpanKind.CLIENT,
    context: Optional[Context] = None,
    end_on_exit: bool = True,
) -> Iterator[Span]:
    with tracer.start_as_current_span(
        name=span_name,
        kind=span_kind,
        record_exception=False,
        set_status_on_exception=False,
        context=context,
        end_on_exit=end_on_exit,
    ) as span:
        yield span


def _enrich_span(
    *,
    span: Span,
    client: BaseEventStoreDBClient,
    db_operation_name: Optional[str] = None,
    stream_name: Optional[str] = None,
    subscription_id: Optional[str] = None,
    event_id: Optional[str] = None,
    event_type: Optional[str] = None,
) -> None:
    if span.is_recording():

        # Gather attributes.
        attributes: Dict[str, AttributeValue] = {}

        # Gather db attributes.
        if db_operation_name is not None:
            attributes[Attributes.DB_OPERATION] = db_operation_name
        attributes[Attributes.DB_SYSTEM] = "eventstoredb"
        # Todo: Username from credentials passed in as arg (not just from URI).
        attributes[Attributes.DB_USER] = _extract_db_user(client)

        # Gather eventstoredb attributes.
        if event_id is not None:
            attributes[Attributes.EVENTSTOREDB_EVENT_ID] = str(event_id)
        if event_type is not None:
            attributes[Attributes.EVENTSTOREDB_EVENT_TYPE] = event_type
        if stream_name is not None:
            attributes[Attributes.EVENTSTOREDB_STREAM] = stream_name
        if subscription_id is not None:
            attributes[Attributes.EVENTSTOREDB_SUBSCRIPTION_ID] = subscription_id

        # Gather server attributes.
        server_address, server_port = _extract_server_address_and_port(client)
        attributes[Attributes.SERVER_ADDRESS] = server_address
        attributes[Attributes.SERVER_PORT] = server_port

        # Set attributes on span.
        span.set_attributes(attributes)


def _extract_db_user(client: BaseEventStoreDBClient) -> str:
    return client.connection_spec.username or ""


def _extract_server_address_and_port(client: BaseEventStoreDBClient) -> Tuple[str, str]:
    # For "quality of life" of readers of observability platforms, try to
    # maintain a constant server address (when using esdb+discover with
    # one target only).
    if (
        client.connection_spec.scheme == URI_SCHEME_ESDB_DISCOVER
        and len(client.connection_spec.targets) == 1
    ):
        # Signal server address as the DNS cluster name ("quality of life").
        server_address, server_port = client.connection_spec.targets[0].split(":")
    else:
        # Signal server address as the current connection address.
        server_address, server_port = client.connection_target.split(":")
    return server_address, server_port


def _set_span_ok(span: Span) -> None:
    span.set_status(StatusCode.OK)


def _set_span_error(span: Span, error: Exception) -> None:
    # Set span status.
    exc_type = type(error)
    span.set_status(
        Status(
            status_code=StatusCode.ERROR,
            description=f"{exc_type.__name__}: {error}",
        )
    )
    # Log an event for the error.
    exception_type = (
        f"{exc_type.__module__}.{exc_type.__qualname__}"
        if exc_type.__module__ and exc_type.__module__ != "builtins"
        else exc_type.__qualname__
    )
    exception_message = str(error)
    stack = ["Traceback (most recent call last):\n"]
    # stack += traceback.format_stack()
    stack += traceback.format_exception(exc_type, error, error.__traceback__)[1:]
    exception_stacktrace = "".join([line for line in stack if _stack_include(line)])
    exception_escaped = str(True)
    # Gather attributes.
    attributes: MutableMapping[str, AttributeValue] = {
        Attributes.EXCEPTION_TYPE: exception_type,
        Attributes.EXCEPTION_MESSAGE: exception_message,
        Attributes.EXCEPTION_STACKTRACE: exception_stacktrace,
        Attributes.EXCEPTION_ESCAPED: exception_escaped,
    }
    span.add_event(name="exception", attributes=attributes, timestamp=None)


_stack_exclude_patterns = [
    "opentelemetry",
    "retrygrpc_decorator",
    "autoreconnect_decorator",
    "unittest",
]
_stack_exclude_regex = re.compile(".*(" + "|".join(_stack_exclude_patterns) + ").*")


def _stack_include(line: str) -> bool:
    return _stack_exclude_regex.match(line) is None


T = TypeVar("T")

SpannerType = Callable[
    [Tracer, Callable[..., T], Any, Sequence[Any], Dict[str, Any]], ContextManager[T]
]


def apply_spanner(
    cls: type, func_name: str, spanner: SpannerType[Any], tracer: Tracer
) -> None:
    original_func = vars(cls)[func_name]

    if inspect.iscoroutinefunction(original_func):

        async def async_wrapper(
            func: Callable[..., Any],
            instance: Any,
            args: Sequence[Any],
            kwargs: Dict[str, Any],
        ) -> Any:
            with spanner(tracer, func, instance, args, kwargs) as result:
                return await result

        apply_patch(cls, func_name, FunctionWrapper(original_func, async_wrapper))

    else:

        def wrapper(
            func: Callable[..., Any],
            instance: Any,
            args: Sequence[Any],
            kwargs: Dict[str, Any],
        ) -> Any:
            with spanner(tracer, func, instance, args, kwargs) as result:
                return result

        apply_patch(cls, func_name, FunctionWrapper(original_func, wrapper))


@contextmanager
def span_method(
    tracer: Tracer,
    original: Callable[..., Any],
    instance: Any,
    args: Sequence[Any],
    kwargs: Dict[str, Any],
) -> Iterator[Any]:

    span_name, span_kind = _get_span_name_and_kind(original)

    with _start_span(tracer, span_name, span_kind) as span:
        _enrich_span(
            span=span,
            client=instance,
            db_operation_name=span_name,
        )
        try:
            yield original(*args, **kwargs)
        except Exception as e:
            _set_span_error(span, e)
            raise
        else:
            _set_span_ok(span)


@contextmanager
def span_read_stream(
    tracer: Tracer,
    original: Callable[..., Any],
    instance: Any,
    args: Sequence[Any],
    kwargs: Dict[str, Any],
) -> Iterator[Any]:

    span_name, span_kind = _get_span_name_and_kind(original)

    with _start_span(tracer, span_name, span_kind) as span:
        stream_name = kwargs.get("stream_name") or args[-1]  # Todo: Better :)

        _enrich_span(
            span=span,
            client=instance,
            db_operation_name=span_name,
            stream_name=stream_name,
        )
        try:
            response = original(*args, **kwargs)
            if inspect.iscoroutine(response):

                async def wrap_response() -> TracedAsyncReadResponse:
                    return TracedAsyncReadResponse(
                        client=instance,
                        response=await response,
                        tracer=tracer,
                        span_name=span_name,
                        span_kind=span_kind,
                    )

                yield wrap_response()
            else:
                yield TracedReadResponse(
                    client=instance,
                    response=response,
                    tracer=tracer,
                    span_name=span_name,
                    span_kind=span_kind,
                )
        except Exception as e:
            _set_span_error(span, e)
            raise
        else:
            _set_span_ok(span)


@contextmanager
def span_append_to_stream(
    tracer: Tracer,
    original: Callable[..., Any],
    instance: Any,
    args: Sequence[Any],
    kwargs: Dict[str, Any],
) -> Iterator[Any]:

    span_name, span_kind = _get_span_name_and_kind(original)
    stream_name = kwargs.get("stream_name") or args[-1]  # Todo: Better :)

    with _start_span(tracer, span_name, span_kind) as span:
        try:
            _enrich_span(
                span=span,
                client=instance,
                db_operation_name=span_name,
                stream_name=stream_name,
            )
            _add_trace_metadata_to_events(span, kwargs)
            yield original(*args, **kwargs)
        except Exception as e:
            _set_span_error(span, e)
            raise
        else:
            _set_span_ok(span)


def _add_trace_metadata_to_events(span: Span, kwargs: MutableMapping[str, Any]) -> None:
    events = cast(Union[NewEvent, Sequence[NewEvent]], kwargs["events"])
    reconstructed_events = []
    if isinstance(events, NewEvent):
        events = [events]
    for event in events:
        metadata_bytes = event.metadata
        if metadata_bytes == b"":
            metadata_bytes = b"{}"
        try:
            metadata_dict = json.loads(metadata_bytes.decode("utf8"))
            span_context = span.get_span_context()
            metadata_dict["$traceId"] = f"{span_context.trace_id:#x}"
            metadata_dict["$spanId"] = f"{span_context.span_id:#x}"
            metadata_bytes = json.dumps(metadata_dict).encode("utf8")
        except Exception:
            reconstructed_event = event
        else:
            reconstructed_event = NewEvent(
                id=event.id,
                type=event.type,
                data=event.data,
                content_type=event.content_type,
                metadata=metadata_bytes,
            )

        reconstructed_events.append(reconstructed_event)
    kwargs["events"] = reconstructed_events


@contextmanager
def span_catchup_subscription(
    tracer: Tracer,
    original: Callable[..., Any],
    instance: Any,
    args: Sequence[Any],
    kwargs: Dict[str, Any],
) -> Iterator[Any]:

    span_name, span_kind = _get_span_name_and_kind(original)

    try:
        response = original(*args, **kwargs)
        if inspect.iscoroutine(response):

            async def wrap_response() -> TracedAsyncCatchupSubscription:
                return TracedAsyncCatchupSubscription(
                    client=instance,
                    response=await response,
                    tracer=tracer,
                    span_name=span_name,
                    span_kind=span_kind,
                )

            yield wrap_response()
        else:
            yield TracedCatchupSubscription(
                client=instance,
                response=response,
                tracer=tracer,
                span_name=span_name,
                span_kind=span_kind,
            )
    except Exception as e:
        with _start_span(tracer, span_name, span_kind) as span:
            _enrich_span(
                span=span,
                client=instance,
                db_operation_name=span_name,
            )
            _set_span_error(span, e)
            raise


@contextmanager
def span_persistent_subscription(
    tracer: Tracer,
    original: Callable[..., Any],
    instance: Any,
    args: Sequence[Any],
    kwargs: Dict[str, Any],
) -> Iterator[Any]:
    span_name, span_kind = _get_span_name_and_kind(original)

    try:
        response = original(*args, **kwargs)
        if inspect.iscoroutine(response):

            async def wrap_response() -> TracedAsyncPersistentSubscription:
                return TracedAsyncPersistentSubscription(
                    client=instance,
                    response=await response,
                    tracer=tracer,
                    span_name=span_name,
                    span_kind=span_kind,
                )

            yield wrap_response()
        else:
            yield TracedPersistentSubscription(
                client=instance,
                response=response,
                tracer=tracer,
                span_name=span_name,
                span_kind=span_kind,
            )
    except Exception as e:
        with _start_span(tracer, span_name, span_kind) as span:
            _enrich_span(
                span=span,
                client=instance,
                db_operation_name=span_name,
            )
            _set_span_error(span, e)
            raise


TRecordedEventIterator = TypeVar("TRecordedEventIterator", bound=RecordedEventIterator)

TRecordedEventSubscription = TypeVar(
    "TRecordedEventSubscription", bound=RecordedEventSubscription
)


class TracedRecordedEventIterator(
    RecordedEventIterator, Generic[TRecordedEventIterator]
):
    def __init__(
        self,
        *,
        client: EventStoreDBClient,
        response: TRecordedEventIterator,
        tracer: Tracer,
        span_name: str,
        span_kind: SpanKind,
    ) -> None:
        self.client = client
        self.response = response
        self.tracer = tracer
        self.span_name = span_name
        self.span_kind = span_kind
        self._current_span: Optional[Span] = None

        # self.iterator_span: Optional[Span] = None
        # self.iterator_context: Optional[Context] = None

        # with _start_span(self.tracer, "ReadResponse", end_on_exit=False) as span:
        #     self.iterator_span = span
        #     _enrich_span(
        #         span=self.iterator_span,
        #         client=self.client,
        #     )
        # self.iterator_context = set_span_in_context(self.iterator_span, Context())

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> RecordedEvent:
        span_name = _get_span_name(self.response.__next__)
        span_kind = _get_span_kind(self.response.__next__)

        with _start_span(
            self.tracer,
            span_name,
            span_kind,
            # context=self.iterator_context,
            end_on_exit=False,
        ) as span:
            self._current_span = span
            try:
                recorded_event = next(self.response)
            except StopIteration:
                _enrich_span(
                    span=span,
                    client=self.client,
                    db_operation_name=span_name,
                )
                _set_span_ok(span)
                raise
            except Exception as e:
                _enrich_span(
                    span=span,
                    client=self.client,
                    db_operation_name=span_name,
                )
                _set_span_error(span, e)
                # if self.iterator_span is not None:
                #     _set_span_error(self.iterator_span, e)
                #     self.iterator_span.end()
                raise
            else:
                _enrich_span(
                    span=span,
                    client=self.client,
                    db_operation_name=span_name,
                    stream_name=recorded_event.stream_name,
                    event_id=str(recorded_event.id),
                    event_type=recorded_event.type,
                )
                _set_span_ok(span)
                return recorded_event
            finally:
                span.end()
                self._current_span = None

    # def _enrich_span(
    #     self,
    #     *,
    #     span: Span,
    #     stream_name: Optional[str] = None,
    #     event_id: Optional[str] = None,
    #     event_type: Optional[str] = None,
    # ) -> None:
    #     _enrich_span(
    #         span=span,
    #         client=self.client,
    #         db_operation_name=self.span_name,
    #         stream_name=stream_name,
    #         event_id=event_id,
    #         event_type=event_type,
    #     )

    def stop(self) -> None:
        self.response.stop()

    def __enter__(self) -> Self:
        self.response.__enter__()
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        return self.response.__exit__(*args, **kwargs)

    def __del__(self) -> None:
        current_span = self._current_span
        if current_span and current_span.is_recording():  # pragma: no cover
            _set_span_ok(current_span)
            current_span.end()
        # iterator_span = self.iterator_span
        # if iterator_span and iterator_span.is_recording():
        #     _set_span_ok(iterator_span)
        #     iterator_span.end()


class TracedReadResponse(TracedRecordedEventIterator[ReadResponse]):
    pass


class TracedRecordedEventSubscription(
    TracedRecordedEventIterator[TRecordedEventSubscription]
):
    def __next__(self) -> RecordedEvent:
        try:
            recorded_event = next(self.response)
        except StopIteration:
            raise
        except Exception as e:
            with _start_span(self.tracer, self.span_name, self.span_kind) as span:
                self._enrich_span(
                    span=span,
                )
                _set_span_error(span, e)
                raise
        else:
            # Set the span's parentId as the $spanId from recorded event metadata,
            # and the span's traceId as the $traceId from recorded event metadata.
            try:
                metadata_dict = json.loads(recorded_event.metadata.decode("utf8"))
                trace_id = metadata_dict["$traceId"]
                parent_span_id = metadata_dict["$spanId"]
            except Exception:
                context: Optional[Context] = None
            else:
                trace_flags = TraceFlags(TraceFlags.SAMPLED)
                span_context = SpanContext(
                    trace_id=int(trace_id, 16),
                    span_id=int(parent_span_id, 16),
                    is_remote=True,
                    trace_flags=trace_flags,
                )
                context = set_span_in_context(
                    NonRecordingSpan(span_context),
                    Context(),
                )

            with _start_span(
                self.tracer, self.span_name, self.span_kind, context=context
            ) as span:
                self._enrich_span(
                    span=span,
                    stream_name=recorded_event.stream_name,
                    event_id=str(recorded_event.id),
                    event_type=recorded_event.type,
                )

                span.set_status(StatusCode.OK)

                return recorded_event

    def _enrich_span(
        self,
        *,
        span: Span,
        stream_name: Optional[str] = None,
        event_id: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> None:
        _enrich_span(
            span=span,
            client=self.client,
            db_operation_name=self.span_name,
            stream_name=stream_name,
            subscription_id=self.subscription_id,
            event_id=event_id,
            event_type=event_type,
        )

    @property
    def subscription_id(self) -> str:
        return self.response.subscription_id


class TracedCatchupSubscription(TracedRecordedEventSubscription[CatchupSubscription]):
    pass


class TracedPersistentSubscription(
    TracedRecordedEventSubscription[PersistentSubscription]
):
    def ack(self, item: Union[UUID, RecordedEvent]) -> None:
        self.response.ack(item)

    def nack(
        self,
        item: Union[UUID, RecordedEvent],
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        self.response.nack(item, action)


TAsyncRecordedEventIterator = TypeVar(
    "TAsyncRecordedEventIterator", bound=AsyncRecordedEventIterator
)

TAsyncRecordedEventSubscription = TypeVar(
    "TAsyncRecordedEventSubscription", bound=AsyncRecordedEventSubscription
)


class TracedAsyncRecordedEventIterator(
    AsyncRecordedEventIterator, Generic[TAsyncRecordedEventIterator]
):
    def __init__(
        self,
        *,
        client: AsyncEventStoreDBClient,
        response: TAsyncRecordedEventIterator,
        tracer: Tracer,
        span_name: str,
        span_kind: SpanKind,
    ) -> None:
        self.client = client
        self.response = response
        self.tracer = tracer
        self.span_name = span_name
        self.span_kind = span_kind
        self._current_span: Optional[Span] = None

    async def __anext__(self) -> RecordedEvent:
        span_name = _get_span_name(self.response.__anext__)
        span_kind = _get_span_kind(self.response.__anext__)

        with _start_span(
            self.tracer,
            span_name,
            span_kind,
            end_on_exit=False,
        ) as span:
            self._current_span = span
            try:
                recorded_event = await self.response.__anext__()
            except StopAsyncIteration:
                _enrich_span(
                    span=span,
                    client=self.client,
                    db_operation_name=span_name,
                )
                _set_span_ok(span)
                raise
            except Exception as e:
                _enrich_span(
                    span=span,
                    client=self.client,
                    db_operation_name=span_name,
                )
                _set_span_error(span, e)
                raise
            else:
                _enrich_span(
                    span=span,
                    client=self.client,
                    db_operation_name=span_name,
                    stream_name=recorded_event.stream_name,
                    event_id=str(recorded_event.id),
                    event_type=recorded_event.type,
                )
                _set_span_ok(span)
                return recorded_event
            finally:
                span.end()
                self._current_span = None

    async def stop(self) -> None:
        await self.response.stop()

    async def __aenter__(self) -> Self:
        await self.response.__aenter__()
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        return await self.response.__aexit__(*args, **kwargs)

    def _set_iter_error_for_testing(self) -> None:
        self.response._set_iter_error_for_testing()

    def __del__(self) -> None:
        current_span = self._current_span
        if current_span and current_span.is_recording():  # pragma: no cover
            _set_span_ok(current_span)
            current_span.end()


class TracedAsyncReadResponse(TracedAsyncRecordedEventIterator[AsyncReadResponse]):
    pass


class TracedAsyncRecordedEventSubscription(
    TracedAsyncRecordedEventIterator[TAsyncRecordedEventSubscription]
):
    async def __anext__(self) -> RecordedEvent:
        try:
            recorded_event = await self.response.__anext__()
        except StopAsyncIteration:
            raise
        except Exception as e:
            with _start_span(self.tracer, self.span_name, self.span_kind) as span:
                self._enrich_span(
                    span=span,
                )
                _set_span_error(span, e)
                raise
        else:
            # Set the span's parentId as the $spanId from recorded event metadata,
            # and the span's traceId as the $traceId from recorded event metadata.
            try:
                metadata_dict = json.loads(recorded_event.metadata.decode("utf8"))
                trace_id = metadata_dict["$traceId"]
                parent_span_id = metadata_dict["$spanId"]
            except Exception:
                context: Optional[Context] = None
            else:
                trace_flags = TraceFlags(TraceFlags.SAMPLED)
                span_context = SpanContext(
                    trace_id=int(trace_id, 16),
                    span_id=int(parent_span_id, 16),
                    is_remote=True,
                    trace_flags=trace_flags,
                )
                context = set_span_in_context(
                    NonRecordingSpan(span_context),
                    Context(),
                )

            with _start_span(
                self.tracer, self.span_name, self.span_kind, context=context
            ) as span:
                self._enrich_span(
                    span=span,
                    stream_name=recorded_event.stream_name,
                    event_id=str(recorded_event.id),
                    event_type=recorded_event.type,
                )
                _set_span_ok(span)
                return recorded_event

    def _enrich_span(
        self,
        *,
        span: Span,
        stream_name: Optional[str] = None,
        event_id: Optional[str] = None,
        event_type: Optional[str] = None,
    ) -> None:
        _enrich_span(
            span=span,
            client=self.client,
            db_operation_name=self.span_name,
            stream_name=stream_name,
            subscription_id=self.subscription_id,
            event_id=event_id,
            event_type=event_type,
        )

    @property
    def subscription_id(self) -> str:
        return self.response.subscription_id


class TracedAsyncCatchupSubscription(
    TracedAsyncRecordedEventSubscription[AsyncCatchupSubscription]
):
    pass


class TracedAsyncPersistentSubscription(
    TracedAsyncRecordedEventSubscription[AsyncPersistentSubscription]
):
    async def ack(self, item: Union[UUID, RecordedEvent]) -> None:
        await self.response.ack(item)

    async def nack(
        self,
        item: Union[UUID, RecordedEvent],
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        await self.response.nack(item, action)
