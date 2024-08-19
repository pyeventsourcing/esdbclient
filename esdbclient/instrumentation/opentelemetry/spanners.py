# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect
import json
import sys
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    Literal,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)
from uuid import UUID

import grpc
from opentelemetry.context import Context
from opentelemetry.trace import (
    NonRecordingSpan,
    Span,
    SpanContext,
    SpanKind,
    StatusCode,
    TraceFlags,
    Tracer,
    set_span_in_context,
)
from opentelemetry.util.types import AttributeValue
from typing_extensions import Self

from esdbclient import (
    AsyncEventStoreDBClient,
    AsyncReadResponse,
    EventStoreDBClient,
    NewEvent,
    ReadResponse,
    RecordedEvent,
    StreamState,
)
from esdbclient.client import BaseEventStoreDBClient
from esdbclient.common import (
    AbstractAsyncCatchupSubscription,
    AbstractAsyncPersistentSubscription,
    AbstractAsyncReadResponse,
    AbstractCatchupSubscription,
    AbstractPersistentSubscription,
    AbstractReadResponse,
    AsyncRecordedEventIterator,
    AsyncRecordedEventSubscription,
    RecordedEventIterator,
    RecordedEventSubscription,
)
from esdbclient.connection_spec import URI_SCHEME_ESDB_DISCOVER
from esdbclient.instrumentation.opentelemetry.attributes import Attributes
from esdbclient.instrumentation.opentelemetry.utils import (
    AsyncSpannerResponse,
    OverloadedSpannerResponse,
    SpannerResponse,
    _set_span_error,
    _set_span_ok,
    _start_span,
)

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


class GetStreamMethod(Protocol):
    def __call__(
        self,
        /,
        stream_name: str,
        *,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[RecordedEvent]:
        pass  # pragma: no cover


class AsyncGetStreamMethod(Protocol):
    async def __call__(
        self,
        /,
        stream_name: str,
        *,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[RecordedEvent]:
        pass  # pragma: no cover


@overload
def span_get_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: AsyncGetStreamMethod,
    /,
    stream_name: str,
    *,
    stream_position: Optional[int] = None,
    backwards: bool = False,
    resolve_links: bool = False,
    limit: int = sys.maxsize,
    timeout: Optional[float] = None,
    credentials: Optional[grpc.CallCredentials] = None,
) -> AsyncSpannerResponse[Sequence[RecordedEvent]]:
    pass  # pragma: no cover


@overload
def span_get_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: GetStreamMethod,
    /,
    stream_name: str,
    *,
    stream_position: Optional[int] = None,
    backwards: bool = False,
    resolve_links: bool = False,
    limit: int = sys.maxsize,
    timeout: Optional[float] = None,
    credentials: Optional[grpc.CallCredentials] = None,
) -> SpannerResponse[Sequence[RecordedEvent]]:
    pass  # pragma: no cover


def span_get_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: Union[GetStreamMethod, AsyncGetStreamMethod],
    /,
    stream_name: str,
    *,
    stream_position: Optional[int] = None,
    backwards: bool = False,
    resolve_links: bool = False,
    limit: int = sys.maxsize,
    timeout: Optional[float] = None,
    credentials: Optional[grpc.CallCredentials] = None,
) -> OverloadedSpannerResponse[Sequence[RecordedEvent], Sequence[RecordedEvent]]:
    span_name, span_kind = _get_span_name_and_kind(spanned_func)

    with _start_span(tracer, span_name, span_kind) as span:
        _enrich_span(
            span=span,
            client=instance,
            db_operation_name=span_name,
            stream_name=stream_name,
        )
        try:
            yield spanned_func(
                stream_name,
                stream_position=stream_position,
                backwards=backwards,
                resolve_links=resolve_links,
                limit=limit,
                timeout=timeout,
                credentials=credentials,
            )
        except Exception as e:
            _set_span_error(span, e)
            raise
        else:
            _set_span_ok(span)


class ReadStreamMethod(Protocol):
    def __call__(
        self,
        /,
        stream_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> AbstractReadResponse:
        pass  # pragma: no cover


class AsyncReadStreamMethod(Protocol):
    async def __call__(
        self,
        /,
        stream_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncReadResponse:
        pass  # pragma: no cover


@overload
def span_read_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: AsyncReadStreamMethod,
    /,
    stream_name: str,
    *args: Any,
    **kwargs: Any,
) -> AsyncSpannerResponse[AsyncReadResponse]:
    pass  # pragma: no cover


@overload
def span_read_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: ReadStreamMethod,
    /,
    stream_name: str,
    *args: Any,
    **kwargs: Any,
) -> SpannerResponse[AbstractReadResponse]:
    pass  # pragma: no cover


def span_read_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: Union[ReadStreamMethod, AsyncReadStreamMethod],
    /,
    stream_name: str,
    *args: Any,
    **kwargs: Any,
) -> OverloadedSpannerResponse[AbstractReadResponse, AsyncReadResponse]:
    span_name, span_kind = _get_span_name_and_kind(spanned_func)
    with _start_span(tracer, span_name, span_kind) as span:

        _enrich_span(
            span=span,
            client=instance,
            db_operation_name=span_name,
            stream_name=stream_name,
        )
        try:
            response = spanned_func(stream_name, *args, **kwargs)
            if inspect.iscoroutine(response):

                async def wrap_response() -> AsyncReadResponse:
                    return cast(
                        AsyncReadResponse,
                        TracedAsyncReadResponse(
                            client=instance,
                            response=await response,
                            tracer=tracer,
                            span_name=span_name,
                            span_kind=span_kind,
                        ),
                    )

                yield wrap_response()
            else:
                # Because TypeGuard doesn't do type narrowing in negative case.
                assert isinstance(response, ReadResponse)

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


class AppendToStreamMethod(Protocol):
    def __call__(
        self,
        /,
        stream_name: str,
        *,
        current_version: Union[int, StreamState],
        events: Union[NewEvent, Iterable[NewEvent]],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        pass  # pragma: no cover


class AsyncAppendToStreamMethod(Protocol):
    async def __call__(
        self,
        /,
        stream_name: str,
        *,
        current_version: Union[int, StreamState],
        events: Union[NewEvent, Iterable[NewEvent]],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        pass  # pragma: no cover


@overload
def span_append_to_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: AsyncAppendToStreamMethod,
    /,
    stream_name: str,
    *,
    current_version: Union[int, StreamState],
    events: Union[NewEvent, Iterable[NewEvent]],
    timeout: Optional[float] = None,
    credentials: Optional[grpc.CallCredentials] = None,
) -> AsyncSpannerResponse[int]:
    pass  # pragma: no cover


@overload
def span_append_to_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: AppendToStreamMethod,
    /,
    stream_name: str,
    *,
    current_version: Union[int, StreamState],
    events: Union[NewEvent, Iterable[NewEvent]],
    timeout: Optional[float] = None,
    credentials: Optional[grpc.CallCredentials] = None,
) -> SpannerResponse[int]:
    pass  # pragma: no cover


def span_append_to_stream(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: Union[AppendToStreamMethod, AsyncAppendToStreamMethod],
    /,
    stream_name: str,
    *,
    current_version: Union[int, StreamState],
    events: Union[NewEvent, Iterable[NewEvent]],
    timeout: Optional[float] = None,
    credentials: Optional[grpc.CallCredentials] = None,
) -> OverloadedSpannerResponse[int, int]:

    span_name, span_kind = _get_span_name_and_kind(spanned_func)

    with _start_span(tracer, span_name, span_kind) as span:
        try:
            _enrich_span(
                span=span,
                client=instance,
                db_operation_name=span_name,
                stream_name=stream_name,
            )
            events = _set_context_in_events(span.get_span_context(), events)
            yield spanned_func(
                stream_name,
                current_version=current_version,
                events=events,
                timeout=timeout,
                credentials=credentials,
            )
        except Exception as e:
            _set_span_error(span, e)
            raise
        else:
            _set_span_ok(span)


class CatchupSubscriptionMethod(Protocol):
    def __call__(
        self,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> AbstractCatchupSubscription:
        pass  # pragma: no cover


class AsyncCatchupSubscriptionMethod(Protocol):
    async def __call__(
        self,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> AbstractAsyncCatchupSubscription:
        pass  # pragma: no cover


@overload
def span_catchup_subscription(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: AsyncCatchupSubscriptionMethod,
    /,
    *args: Any,
    **kwargs: Any,
) -> AsyncSpannerResponse[AbstractAsyncCatchupSubscription]:
    pass  # pragma: no cover


@overload
def span_catchup_subscription(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: CatchupSubscriptionMethod,
    /,
    *args: Any,
    **kwargs: Any,
) -> SpannerResponse[AbstractCatchupSubscription]:
    pass  # pragma: no cover


def span_catchup_subscription(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: Union[CatchupSubscriptionMethod, AsyncCatchupSubscriptionMethod],
    /,
    *args: Any,
    **kwargs: Any,
) -> OverloadedSpannerResponse[
    AbstractCatchupSubscription, AbstractAsyncCatchupSubscription
]:
    span_name, span_kind = _get_span_name_and_kind(spanned_func)
    try:
        response = spanned_func(*args, **kwargs)
        if inspect.isawaitable(response):

            async def wrap_response() -> AbstractAsyncCatchupSubscription:
                return TracedAsyncCatchupSubscription(
                    client=instance,
                    response=await response,
                    tracer=tracer,
                    span_name=span_name,
                    span_kind=span_kind,
                )

            yield wrap_response()
        else:
            # Because TypeGuard doesn't do type narrowing in negative case.
            assert isinstance(response, AbstractCatchupSubscription)

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


class ReadPersistentSubscriptionMethod(Protocol):
    def __call__(
        self,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> AbstractPersistentSubscription:
        pass  # pragma: no cover


class AsyncReadPersistentSubscriptionMethod(Protocol):
    async def __call__(
        self,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> AbstractAsyncPersistentSubscription:
        pass  # pragma: no cover


@overload
def span_persistent_subscription(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: AsyncReadPersistentSubscriptionMethod,
    /,
    *args: Any,
    **kwargs: Any,
) -> AsyncSpannerResponse[AbstractAsyncPersistentSubscription]:
    pass  # pragma: no cover


@overload
def span_persistent_subscription(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: ReadPersistentSubscriptionMethod,
    /,
    *args: Any,
    **kwargs: Any,
) -> SpannerResponse[AbstractPersistentSubscription]:
    pass  # pragma: no cover


def span_persistent_subscription(
    tracer: Tracer,
    instance: BaseEventStoreDBClient,
    spanned_func: Union[
        ReadPersistentSubscriptionMethod, AsyncReadPersistentSubscriptionMethod
    ],
    /,
    *args: Any,
    **kwargs: Any,
) -> OverloadedSpannerResponse[
    AbstractPersistentSubscription, AbstractAsyncPersistentSubscription
]:
    span_name, span_kind = _get_span_name_and_kind(spanned_func)
    try:
        response = spanned_func(*args, **kwargs)
        if inspect.isawaitable(response):

            async def wrap_response() -> AbstractAsyncPersistentSubscription:
                return TracedAsyncPersistentSubscription(
                    client=instance,
                    response=await response,
                    tracer=tracer,
                    span_name=span_name,
                    span_kind=span_kind,
                )

            yield wrap_response()

        else:
            # Because TypeGuard doesn't do type narrowing in negative case.
            assert isinstance(response, AbstractPersistentSubscription)

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
        client: BaseEventStoreDBClient,
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

    def __next__(self) -> RecordedEvent:
        span_name, span_kind = _get_span_name_and_kind(self.response.__next__)

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


class TracedReadResponse(
    TracedRecordedEventIterator[AbstractReadResponse], AbstractReadResponse
):
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
            context = _extract_context_from_event(recorded_event)

            if context is not None:
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
            else:
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


class TracedCatchupSubscription(
    TracedRecordedEventSubscription[AbstractCatchupSubscription],
    AbstractCatchupSubscription,
):
    pass


class TracedPersistentSubscription(
    TracedRecordedEventSubscription[AbstractPersistentSubscription],
    AbstractPersistentSubscription,
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
        client: BaseEventStoreDBClient,
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


class TracedAsyncReadResponse(
    TracedAsyncRecordedEventIterator[AsyncReadResponse],
    AbstractAsyncReadResponse,
):
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
            context = _extract_context_from_event(recorded_event)

            if context is not None:
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
            else:
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
    TracedAsyncRecordedEventSubscription[AbstractAsyncCatchupSubscription],
    AbstractAsyncCatchupSubscription,
):
    pass


class TracedAsyncPersistentSubscription(
    TracedAsyncRecordedEventSubscription[AbstractAsyncPersistentSubscription],
    AbstractAsyncPersistentSubscription,
):
    async def ack(self, item: Union[UUID, RecordedEvent]) -> None:
        await self.response.ack(item)

    async def nack(
        self,
        item: Union[UUID, RecordedEvent],
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        await self.response.nack(item, action)


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


METADATA_TRACE_ID = "$traceId"
METADATA_SPAN_ID = "$spanId"


def _set_context_in_events(
    context: SpanContext, events: Union[NewEvent, Iterable[NewEvent]]
) -> Sequence[NewEvent]:
    # Kind of propagate OpenTelemetry context in "standard EventStoreDB" style.
    reconstructed_events = []
    if isinstance(events, NewEvent):
        events = [events]
    for event in events:
        if event.content_type == "application/json":
            try:
                d = json.loads((event.metadata or b"{}").decode("utf8"))
                d[METADATA_SPAN_ID] = _int_to_hex(context.span_id)
                d[METADATA_TRACE_ID] = _int_to_hex(context.trace_id)
                metadata = json.dumps(d).encode("utf8")
            except Exception:
                pass
            else:
                event = NewEvent(
                    id=event.id,
                    type=event.type,
                    data=event.data,
                    content_type=event.content_type,
                    metadata=metadata,
                )
        reconstructed_events.append(event)
    return reconstructed_events


def _extract_context_from_event(
    recorded_event: Union[NewEvent, RecordedEvent],
) -> Optional[Context]:
    # Extract propagated OpenTelemetry context using "standard EventStoreDB" style.
    try:
        m = json.loads(recorded_event.metadata.decode("utf8"))
        parent_span_id = _hex_to_int(m[METADATA_SPAN_ID])
        trace_id = _hex_to_int(m[METADATA_TRACE_ID])
    except Exception:
        context: Optional[Context] = None
    else:
        trace_flags = TraceFlags(TraceFlags.SAMPLED)
        span_context = SpanContext(
            trace_id=trace_id,
            span_id=parent_span_id,
            is_remote=True,
            trace_flags=trace_flags,
        )
        context = set_span_in_context(
            NonRecordingSpan(span_context),
            Context(),
        )
    return context


def _int_to_hex(i: int) -> str:
    return f"{i:#x}"


def _hex_to_int(hex_string: str) -> int:
    return int(hex_string, 16)
