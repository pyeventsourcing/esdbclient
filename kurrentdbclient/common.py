from __future__ import annotations

import asyncio
import contextlib
import datetime
import os
import threading
from abc import ABC, abstractmethod
from base64 import b64encode
from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Literal,
    TypeVar,
    Union,
)
from uuid import UUID
from weakref import WeakValueDictionary

import grpc
import grpc.aio
from grpc_status import rpc_status
from typing_extensions import Self

import kurrentdbclient.protos.kurrent.rpc.errors_pb2 as kurrent_rpc_errors_pb2
import kurrentdbclient.protos.v2.streams.errors_pb2 as v2streams_errors_pb2
from kurrentdbclient.events import RecordedEvent
from kurrentdbclient.exceptions import (
    AbortedByServerError,
    AlreadyExistsError,
    CancelledByClientError,
    ConsumerTooSlowError,
    ExceptionThrownByHandlerError,
    FailedPreconditionError,
    GrpcDeadlineExceededError,
    GrpcError,
    InternalError,
    KurrentDBClientError,
    MaximumSubscriptionsReachedError,
    MultiAppendToSameStreamError,
    NodeIsNotLeaderError,
    NotFoundError,
    OperationFailedError,
    RecordMaxSizeExceededError,
    ServiceUnavailableError,
    SSLError,
    StreamTombstonedError,
    TransactionMaxSizeExceededError,
    UnauthenticatedError,
    UnknownError,
    WrongCurrentVersionError,
)
from kurrentdbclient.protos.v1 import persistent_pb2, streams_pb2
from kurrentdbclient.unpack_error_status import unpack_status_details

# Avoid ares resolver.
if "GRPC_DNS_RESOLVER" not in os.environ:
    os.environ["GRPC_DNS_RESOLVER"] = "native"


if TYPE_CHECKING:  # pragma: no cover
    from grpc import Metadata

    from kurrentdbclient.connection_spec import ConnectionSpec

else:
    Metadata = tuple[tuple[str, str], ...]

__all__ = [
    "handle_rpc_error",
    "BasicAuthCallCredentials",
    "KurrentDBService",
    "Metadata",
]

PROTOBUF_MAX_DEADLINE_SECONDS = 315576000000
DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER = 5
DEFAULT_WINDOW_SIZE = 30
DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT = 30.0
DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT = 10
DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT = 10
DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT = 1000
DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER = 2.0
DEFAULT_PERSISTENT_SUB_EVENT_BUFFER_SIZE = 150
DEFAULT_PERSISTENT_SUB_MAX_ACK_BATCH_SIZE = 50
DEFAULT_PERSISTENT_SUB_MAX_ACK_DELAY = 0.2
DEFAULT_PERSISTENT_SUB_STOPPING_GRACE = 0.2
DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT = 5
DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE = 500
DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE = 200
DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE = 500


GrpcOption = tuple[str, Union[str, int]]
GrpcOptions = tuple[GrpcOption, ...]


class BaseGrpcStreamer:
    pass


class GrpcStreamer(BaseGrpcStreamer, ABC):
    def __init__(self, grpc_streamers: GrpcStreamers) -> None:
        self._grpc_streamers = grpc_streamers
        self._grpc_streamers.add(self)
        self._is_stopped = False
        self._is_stopped_lock = threading.Lock()

    @abstractmethod
    def stop(self) -> None:
        """
        Stops the iterator(s) of streaming call.
        """

    def _set_is_stopped(self) -> bool:
        is_stopped = True
        if self._is_stopped is False:
            with self._is_stopped_lock:
                if self._is_stopped is False:
                    is_stopped = False
                    self._is_stopped = True
                else:  # pragma: no cover
                    pass
        return is_stopped


class AsyncGrpcStreamer(BaseGrpcStreamer, ABC):
    def __init__(self, grpc_streamers: AsyncGrpcStreamers) -> None:
        self._grpc_streamers = grpc_streamers
        self._grpc_streamers.add(self)
        self._is_stopped = False
        self._is_stopped_lock = asyncio.Lock()

    @abstractmethod
    async def stop(self) -> None:
        """
        Stops the iterator(s) of streaming call.
        """

    async def _set_is_stopped(self) -> bool:
        is_stopped = True
        if self._is_stopped is False:
            async with self._is_stopped_lock:
                if self._is_stopped is False:
                    is_stopped = False
                    self._is_stopped = True
                else:  # pragma: no cover
                    pass
        return is_stopped


TGrpcStreamer = TypeVar("TGrpcStreamer", bound=BaseGrpcStreamer)


class BaseGrpcStreamers(Generic[TGrpcStreamer]):
    def __init__(self) -> None:
        self.map: WeakValueDictionary[int, TGrpcStreamer] = WeakValueDictionary()
        self.lock = threading.Lock()

    def add(self, streamer: TGrpcStreamer) -> None:
        with self.lock:
            self.map[id(streamer)] = streamer

    def __iter__(self) -> Iterator[TGrpcStreamer]:
        with self.lock:
            return iter(tuple(self.map.values()))

    def remove(self, streamer: TGrpcStreamer) -> None:
        with self.lock, contextlib.suppress(KeyError):
            self.map.pop(id(streamer))


class GrpcStreamers(BaseGrpcStreamers[GrpcStreamer]):
    def close(self) -> None:
        for grpc_streamer in self:
            # print("closing streamer")
            grpc_streamer.stop()
            # print("closed streamer")


class AsyncGrpcStreamers(BaseGrpcStreamers[AsyncGrpcStreamer]):
    async def close(self) -> None:
        for async_grpc_streamer in self:
            # print("closing streamer")
            await async_grpc_streamer.stop()
            # print("closed streamer")


TGrpcStreamers = TypeVar("TGrpcStreamers", bound=BaseGrpcStreamers[Any])


class BasicAuthCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        credentials = b64encode(f"{username}:{password}".encode())
        self._metadata = (("authorization", (b"Basic " + credentials)),)

    def __call__(
        self,
        context: grpc.AuthMetadataContext,
        callback: grpc.AuthMetadataPluginCallback,
    ) -> None:
        callback(self._metadata, None)


def handle_rpc_error(e: grpc.RpcError) -> KurrentDBClientError:  # noqa: PLR0911
    """
    Converts gRPC errors to client exceptions.
    """
    if isinstance(e, (grpc.Call, grpc.aio.AioRpcError)):
        details_str = e.details() or ""

        rich_status = rpc_status.from_call(e)  # type: ignore[arg-type]
        if rich_status is not None:
            # Handle error by unpacking details messages.
            status_msg = rpc_status.to_status(rich_status)
            unpacked_details = unpack_status_details(rich_status)
            for unpacked_detail in unpacked_details:
                if status_msg.code == grpc.StatusCode.FAILED_PRECONDITION:
                    if isinstance(
                        unpacked_detail,
                        kurrent_rpc_errors_pb2.NotLeaderNodeErrorDetails,
                    ):
                        node_info = unpacked_detail.current_leader
                        return NodeIsNotLeaderError(
                            rich_status.message,
                            host=node_info.host,
                            port=node_info.port,
                            node_id=node_info.node_id,
                        )
                    if isinstance(
                        unpacked_detail,
                        v2streams_errors_pb2.StreamRevisionConflictErrorDetails,
                    ):
                        return WrongCurrentVersionError(
                            rich_status.message.replace("revision", "version").replace(
                                "actual", "current"
                            ),
                            stream_name=unpacked_detail.stream,
                            current_version=unpacked_detail.actual_revision,
                            expected_version=unpacked_detail.expected_revision,
                        )
                    if isinstance(
                        unpacked_detail,
                        v2streams_errors_pb2.StreamTombstonedErrorDetails,
                    ):
                        return StreamTombstonedError(
                            rich_status.message, stream_name=unpacked_detail.stream
                        )
                if status_msg.code == grpc.StatusCode.INVALID_ARGUMENT:  # noqa: SIM102
                    if isinstance(
                        unpacked_detail,
                        v2streams_errors_pb2.AppendRecordSizeExceededErrorDetails,
                    ):
                        return RecordMaxSizeExceededError(
                            rich_status.message,
                            stream_name=unpacked_detail.stream,
                            event_id=UUID(unpacked_detail.record_id),
                            size=unpacked_detail.size,
                            max_size=unpacked_detail.max_size,
                        )
                if status_msg.code == grpc.StatusCode.ABORTED:
                    if isinstance(
                        unpacked_detail,
                        v2streams_errors_pb2.AppendTransactionSizeExceededErrorDetails,
                    ):
                        return TransactionMaxSizeExceededError(
                            rich_status.message,
                            size=unpacked_detail.size,
                            max_size=unpacked_detail.max_size,
                        )
                    if isinstance(
                        unpacked_detail,
                        v2streams_errors_pb2.StreamAlreadyInAppendSessionErrorDetails,
                    ):
                        return MultiAppendToSameStreamError(
                            rich_status.message,
                            stream_name=unpacked_detail.stream,
                        )
            return KurrentDBClientError(details_str)

        if e.code() == grpc.StatusCode.UNKNOWN:
            if "Exception was thrown by handler" in details_str:
                return ExceptionThrownByHandlerError(details_str)
            if (
                "Envelope callback expected Updated, received Conflict instead"
                in details_str
            ):
                # Projections.Create does this....
                return AlreadyExistsError(details_str)
            if (
                "Envelope callback expected Updated, received NotFound instead"
                in details_str
            ):
                # Projections.Update and Projections.Delete does this in < v24.6
                return NotFoundError(details_str)  # pragma: no cover
            if (
                "Envelope callback expected Statistics, received NotFound instead"
                in details_str
            ):
                # Projections.Statistics does this in < v24.6
                return NotFoundError(details_str)  # pragma: no cover
            if (
                "Envelope callback expected ProjectionState, received NotFound instead"
                in details_str
            ):
                # Projections.State does this in < v24.6
                return NotFoundError(details_str)  # pragma: no cover
            if (
                "Envelope callback expected ProjectionResult, received NotFound instead"
                in details_str
            ):
                # Projections.Result does this in < v24.6
                return NotFoundError(details_str)  # pragma: no cover
            if (
                "Envelope callback expected Updated, received OperationFailed instead"
                in details_str
            ):
                # Projections.Delete does this....
                return OperationFailedError(details_str)
            return UnknownError(details_str)  # pragma: no cover

        if e.code() == grpc.StatusCode.ABORTED:
            if "Consumer too slow" in details_str:
                return ConsumerTooSlowError(details_str)
            return AbortedByServerError(details_str)
        if (
            e.code() == grpc.StatusCode.CANCELLED
            and details_str == "Locally cancelled by application!"
        ):
            return CancelledByClientError(details_str)
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return GrpcDeadlineExceededError(details_str)
        if e.code() == grpc.StatusCode.UNAUTHENTICATED:
            return UnauthenticatedError(details_str)
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            if "SSL_ERROR" in details_str:
                # root_certificates is None and CA cert not installed
                return SSLError(details_str)
            if "empty address list" in details_str:
                # given root_certificates is invalid
                return SSLError(details_str)
            return ServiceUnavailableError(details_str)
        if e.code() == grpc.StatusCode.ALREADY_EXISTS:
            return AlreadyExistsError(details_str)
        if e.code() == grpc.StatusCode.NOT_FOUND:
            if details_str == "Leader info available":
                trailing_metadata = {m[0]: m[1] for m in e.trailing_metadata()}  # type: ignore[index]
                leader_host = trailing_metadata.get("leader-endpoint-host")
                leader_port = trailing_metadata.get("leader-endpoint-port")
                return NodeIsNotLeaderError(
                    details_str,
                    host=leader_host,
                    port=(
                        int(leader_port)
                        if isinstance(leader_port, str) and leader_port.isdigit()
                        else None
                    ),
                )
            return NotFoundError()
        if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
            if details_str is not None and details_str.startswith(
                "Maximum subscriptions reached"
            ):
                return MaximumSubscriptionsReachedError(details_str)
            # no cover: start
            return FailedPreconditionError(details_str)
            # no cover: stop
        if e.code() == grpc.StatusCode.INTERNAL:  # pragma: no cover
            return InternalError(details_str)
    return GrpcError(str(e))


class KurrentDBService(Generic[TGrpcStreamers]):
    def __init__(
        self,
        connection_spec: ConnectionSpec,
        grpc_streamers: TGrpcStreamers,
    ):
        self._connection_spec = connection_spec
        self._grpc_streamers = grpc_streamers

    def _metadata(
        self, metadata: Metadata | None, *, requires_leader: bool = False
    ) -> Metadata:
        default = (
            "true"
            if self._connection_spec.options.node_preference == "leader"
            else "false"
        )
        requires_leader_metadata: Metadata = (
            ("requires-leader", "true" if requires_leader else default),
        )
        metadata = () if metadata is None else metadata
        return metadata + requires_leader_metadata


def construct_filter_include_regex(patterns: Sequence[str]) -> str:
    patterns = [patterns] if isinstance(patterns, str) else patterns
    return "^" + "|".join(patterns) + "$"


def construct_filter_exclude_regex(patterns: Sequence[str]) -> str:
    patterns = [patterns] if isinstance(patterns, str) else patterns
    return "^(?!(" + "|".join([s + "$" for s in patterns]) + "))"


def construct_recorded_event(
    read_event: streams_pb2.ReadResp.ReadEvent | persistent_pb2.ReadResp.ReadEvent,
) -> RecordedEvent | None:
    assert isinstance(
        read_event, (streams_pb2.ReadResp.ReadEvent, persistent_pb2.ReadResp.ReadEvent)
    )
    event = read_event.event
    assert isinstance(
        event,
        (
            streams_pb2.ReadResp.ReadEvent.RecordedEvent,
            persistent_pb2.ReadResp.ReadEvent.RecordedEvent,
        ),
    )
    link = read_event.link
    assert isinstance(
        link,
        (
            streams_pb2.ReadResp.ReadEvent.RecordedEvent,
            persistent_pb2.ReadResp.ReadEvent.RecordedEvent,
        ),
    )

    if event.id.string == "":  # pragma: no cover
        # Sometimes get here when resolving links after deleting a stream.
        # Sometimes never, e.g. when the test suite runs, don't know why.
        return None

    # Used to get "no_position" with EventStoreDB < 22.10 when reading a stream.
    position_oneof = read_event.WhichOneof("position")
    assert position_oneof == "commit_position", position_oneof

    if isinstance(read_event, persistent_pb2.ReadResp.ReadEvent):
        retry_count: int | None = read_event.retry_count
    else:
        retry_count = None

    if link.id.string == "":
        recorded_event_link: RecordedEvent | None = None
    else:
        try:
            recorded_at = datetime.datetime.fromtimestamp(
                int(event.metadata.get("created", "")) / 10000000.0,
                tz=datetime.timezone.utc,
            )
        except (TypeError, ValueError):  # pragma: no cover
            recorded_at = None

        recorded_event_link = RecordedEvent(
            id=UUID(link.id.string),
            type=link.metadata.get("type", ""),
            data=link.data,
            metadata=link.custom_metadata,
            content_type=link.metadata.get("content-type", ""),
            stream_name=link.stream_identifier.stream_name.decode("utf8"),
            stream_position=link.stream_revision,
            commit_position=link.commit_position,
            prepare_position=link.prepare_position,
            retry_count=retry_count,
            recorded_at=recorded_at,
        )

    try:
        recorded_at = datetime.datetime.fromtimestamp(
            int(event.metadata.get("created", "")) / 10000000.0,
            tz=datetime.timezone.utc,
        )
    except (TypeError, ValueError):  # pragma: no cover
        recorded_at = None

    return RecordedEvent(
        id=UUID(event.id.string),
        type=event.metadata.get("type", ""),
        data=event.data,
        metadata=event.custom_metadata,
        content_type=event.metadata.get("content-type", ""),
        stream_name=event.stream_identifier.stream_name.decode("utf8"),
        stream_position=event.stream_revision,
        commit_position=event.commit_position,
        prepare_position=event.prepare_position,
        retry_count=retry_count,
        link=recorded_event_link,
        recorded_at=recorded_at,
    )
    # if (
    #     recorded_event.commit_position
    #     and recorded_event.commit_position != recorded_event.prepare_position
    # ):
    #     raise Exception(
    #         f"Commit and prepare positions of recorded event are not equal:"
    #         f" {recorded_event}"
    #     )
    # if (
    #     recorded_event.link
    #     and recorded_event.link.commit_position
    #     and recorded_event.link.commit_position != recorded_event.link.prepare_position  # noqa: E501
    # ):
    #     raise Exception(
    #         f"Commit and prepare positions of recorded event link are not equal:"
    #         f" {recorded_event.link}"
    #     )


try:  # pragma: no cover
    _ContextManager = AbstractContextManager[Iterator[RecordedEvent]]
except TypeError:  # pragma: no cover
    # For Python <= v3.9.
    _ContextManager = AbstractContextManager  # type: ignore


class RecordedEventIterator(Iterator[RecordedEvent], _ContextManager):
    def __init__(self) -> None:
        self._is_context_manager_active = False

    def __iter__(self) -> Self:
        return self

    def __enter__(self) -> Self:
        self._is_context_manager_active = True
        return self

    def __exit__(self, *args: object, **kwargs: Any) -> None:
        self._is_context_manager_active = False
        self.stop()

    def __del__(self) -> None:
        self.stop()

    @abstractmethod
    def stop(self) -> None:
        pass  # pragma: no cover


class AbstractReadResponse(RecordedEventIterator):
    pass


class RecordedEventSubscription(RecordedEventIterator):
    @property
    @abstractmethod
    def subscription_id(self) -> str:
        pass  # pragma: no cover


class AbstractCatchupSubscription(RecordedEventSubscription, AbstractReadResponse):
    pass


class AbstractPersistentSubscription(RecordedEventSubscription):
    @abstractmethod
    def ack(self, item: UUID | RecordedEvent) -> None:
        pass  # pragma: no cover

    @abstractmethod
    def nack(
        self,
        item: UUID | RecordedEvent,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        pass  # pragma: no cover


try:  # pragma: no cover
    _AsyncContextManager = AbstractAsyncContextManager[AsyncIterator[RecordedEvent]]
except TypeError:  # pragma: no cover
    # For Python <= v3.9.
    _AsyncContextManager = AbstractAsyncContextManager  # type: ignore


class AsyncRecordedEventIterator(AsyncIterator[RecordedEvent], _AsyncContextManager):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._is_context_manager_active = False

    @abstractmethod
    async def stop(self) -> None:
        pass  # pragma: no cover

    def __aiter__(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        self._is_context_manager_active = True
        return self

    async def __aexit__(self, *args: object, **kwargs: Any) -> None:
        self._is_context_manager_active = False
        await self.stop()

    def _set_iter_error_for_testing(self) -> None:
        # This, because I can't find a good way to inspire an error during iterating
        # with catchup and persistent subscriptions after successfully
        # receiving confirmation response, tried closing the channel
        # but the async streaming response continues (unlike with sync call). Needed
        # to inspire this error to test instrumentation span error during iteration.
        self._iter_error_for_testing = True

    def _has_iter_error_for_testing(self) -> bool:
        return getattr(self, "_iter_error_for_testing", False)


class AbstractAsyncReadResponse(AsyncRecordedEventIterator):
    pass


class AsyncRecordedEventSubscription(AsyncRecordedEventIterator):
    @property
    @abstractmethod
    def subscription_id(self) -> str:
        pass  # pragma: no cover


class AbstractAsyncCatchupSubscription(AsyncRecordedEventSubscription):
    pass


class AbstractAsyncPersistentSubscription(AsyncRecordedEventSubscription):
    @abstractmethod
    async def ack(self, item: UUID | RecordedEvent) -> None:
        pass  # pragma: no cover

    @abstractmethod
    async def nack(
        self,
        item: UUID | RecordedEvent,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        pass  # pragma: no cover


def grpc_target(host: str, port: int | str) -> str:
    return f"{host}:{port}"
