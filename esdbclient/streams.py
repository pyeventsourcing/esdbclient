# -*- coding: utf-8 -*-
import sys
from abc import abstractmethod
from asyncio import CancelledError
from dataclasses import dataclass
from enum import Enum
from typing import (
    AsyncIterator,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Union,
    overload,
)
from uuid import UUID, uuid4

import grpc
import grpc.aio
from google.protobuf import duration_pb2, empty_pb2
from typing_extensions import Literal, Protocol, runtime_checkable

from esdbclient.common import (
    DEFAULT_BATCH_APPEND_REQUEST_DEADLINE,
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_WINDOW_SIZE,
    ESDBService,
    Metadata,
    construct_filter_exclude_regex,
    construct_filter_include_regex,
    construct_recorded_event,
    handle_rpc_error,
)
from esdbclient.connection_spec import ConnectionSpec
from esdbclient.events import Checkpoint, NewEvent, RecordedEvent
from esdbclient.exceptions import (
    AccessDeniedError,
    BadRequestError,
    CancelledByClient,
    EventStoreDBClientException,
    InvalidTransactionError,
    MaximumAppendSizeExceededError,
    NotFound,
    StreamIsDeleted,
    SubscriptionConfirmationError,
    TimeoutError,
    UnknownError,
    WrongCurrentVersion,
)
from esdbclient.protos.Grpc import shared_pb2, status_pb2, streams_pb2, streams_pb2_grpc


@runtime_checkable
class _ReadResps(Iterator[streams_pb2.ReadResp], Protocol):
    @abstractmethod
    def cancel(self) -> None:
        ...  # pragma: no cover


class StreamState(Enum):
    ANY = "ANY"
    NO_STREAM = "NO_STREAM"
    EXISTS = "EXISTS"


class BaseReadResponse:
    def __init__(
        self,
        stream_name: Optional[str],
    ):
        self.stream_name = stream_name

    def handle_stream_read_rpc_error(
        self, e: grpc.RpcError
    ) -> EventStoreDBClientException:
        if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
            details = e.details() or ""
            if self.stream_name and details and "is deleted" in details:
                return StreamIsDeleted()
            else:  # pragma: no cover
                return handle_rpc_error(e)
        else:
            return handle_rpc_error(e)

    def _convert_read_resp(
        self, read_resp: streams_pb2.ReadResp
    ) -> Optional[RecordedEvent]:
        content_oneof = read_resp.WhichOneof("content")
        if content_oneof == "stream_not_found":
            raise NotFound(f"Stream {self.stream_name!r} not found")
        elif content_oneof == "event":
            return construct_recorded_event(read_resp.event)
        elif content_oneof == "checkpoint":
            checkpoint = read_resp.checkpoint
            # print("Checkpoint commit position:", checkpoint.commit_position)
            return Checkpoint(
                commit_position=checkpoint.commit_position,
            )
        else:  # pragma: no cover
            return None
            # Todo: Maybe support other content_oneof values:
            # 		uint64 first_stream_position = 5;
            # 		uint64 last_stream_position = 6;
            # 		AllStreamPosition last_all_stream_position = 7;
            #
            # Todo: Not sure how to request to get first_stream_position,
            #   last_stream_position, first_all_stream_position.


class AsyncioReadResponse(AsyncIterator[RecordedEvent], BaseReadResponse):
    def __init__(
        self,
        aio_call: grpc.aio.UnaryStreamCall[streams_pb2.ReadReq, streams_pb2.ReadResp],
        stream_name: Optional[str],
    ):
        super().__init__(stream_name=stream_name)
        self.aio_call = aio_call
        self.read_resp_iter = aio_call.__aiter__()

    def __aiter__(self) -> AsyncIterator[RecordedEvent]:
        return self

    async def __anext__(self) -> RecordedEvent:
        while True:
            try:
                read_resp = await self._get_next_read_resp()
            except CancelledByClient as e:
                raise StopAsyncIteration() from e
            else:
                recorded_event = self._convert_read_resp(read_resp)
                if recorded_event is not None:
                    return recorded_event
                else:  # pragma: no cover
                    pass

    async def _get_next_read_resp(self) -> streams_pb2.ReadResp:
        try:
            read_resp = await self.read_resp_iter.__anext__()
        except grpc.RpcError as e:
            raise self.handle_stream_read_rpc_error(e) from e
        except CancelledError as e:
            raise CancelledByClient() from e
        else:
            assert isinstance(read_resp, streams_pb2.ReadResp)
            return read_resp

    def stop(self) -> None:
        self.aio_call.cancel()


class AsyncioCatchupSubscription(AsyncioReadResponse):
    def __init__(
        self,
        aio_call: grpc.aio.UnaryStreamCall[streams_pb2.ReadReq, streams_pb2.ReadResp],
        stream_name: Optional[str],
        include_checkpoints: bool = False,
    ):
        super().__init__(aio_call=aio_call, stream_name=stream_name)
        self.include_checkpoints = include_checkpoints

    async def check_confirmation(self) -> None:
        read_resp = await self._get_next_read_resp()
        content_oneof = read_resp.WhichOneof("content")
        if content_oneof != "confirmation":  # pragma: no cover
            raise SubscriptionConfirmationError(
                f"Expected subscription confirmation, got: {read_resp}"
            )

    async def __anext__(self) -> RecordedEvent:
        while True:
            recorded_event = await super().__anext__()
            if self.include_checkpoints or not isinstance(recorded_event, Checkpoint):
                return recorded_event


class ReadResponse(Iterator[RecordedEvent], BaseReadResponse):
    def __init__(
        self,
        read_resps: _ReadResps,
        stream_name: Optional[str],
    ):
        super().__init__(stream_name=stream_name)
        self.read_resps = read_resps

    def __iter__(self) -> "ReadResponse":
        return self

    def __next__(self) -> RecordedEvent:
        while True:
            try:
                read_resp = self._get_next_read_resp()
            except CancelledByClient as e:
                raise StopIteration() from e
            else:
                recorded_event = self._convert_read_resp(read_resp)
                if recorded_event is not None:
                    return recorded_event
                else:  # pragma: no cover
                    pass

    def _get_next_read_resp(self) -> streams_pb2.ReadResp:
        try:
            read_resp = next(self.read_resps)
        except grpc.RpcError as e:
            raise self.handle_stream_read_rpc_error(e) from e
        else:
            assert isinstance(read_resp, streams_pb2.ReadResp)
            return read_resp

    def stop(self) -> None:
        self.read_resps.cancel()

    def __del__(self) -> None:
        self.stop()
        del self


class CatchupSubscription(ReadResponse):
    def __init__(
        self,
        read_resps: _ReadResps,
        stream_name: Optional[str],
        include_checkpoints: bool = False,
    ):
        super().__init__(read_resps=read_resps, stream_name=stream_name)
        self.subscription_id: Optional[UUID] = None
        self.include_checkpoints = include_checkpoints
        first_read_resp = self._get_next_read_resp()
        content_oneof = first_read_resp.WhichOneof("content")
        if content_oneof == "confirmation":
            pass
            # Todo: What is '.confirmation.subscription_id' for?
        else:  # pragma: no cover
            raise SubscriptionConfirmationError(
                f"Expected subscription confirmation, got: {first_read_resp}"
            )

    def __next__(self) -> RecordedEvent:
        while True:
            recorded_event = super().__next__()
            if self.include_checkpoints or not isinstance(recorded_event, Checkpoint):
                return recorded_event


# @dataclass
# class BatchAppendRequest:
#     stream_name: str
#     current_version: Union[int, StreamState]
#     events: Iterable[NewEvent]
#     correlation_id: UUID = field(default_factory=uuid4)
#     deadline: int = DEFAULT_BATCH_APPEND_REQUEST_DEADLINE


# if TYPE_CHECKING:  # pragma: no cover
#
#     class _BatchAppendFuture(Future[BatchAppendResponse]):
#         pass
#
# else:
#
#     class _BatchAppendFuture(Future):
#         pass
#
#
# class BatchAppendFuture(_BatchAppendFuture):
#     def __init__(self, batch_append_request: BatchAppendRequest):
#         super().__init__()
#         self.batch_append_request = batch_append_request
#
#
# if TYPE_CHECKING:  # pragma: no cover
#     BatchAppendFutureQueue = Queue[BatchAppendFuture]
# else:
#     BatchAppendFutureQueue = Queue


@dataclass
class BatchAppendResponse:
    commit_position: int


# class BatchAppendFutureIterator(Iterator[streams_pb2.BatchAppendReq]):
#     def __init__(self, queue: BatchAppendFutureQueue):
#         self.queue = queue
#         self.futures_by_correlation_id: Dict[UUID, BatchAppendFuture] = {}
#
#     def __next__(self) -> streams_pb2.BatchAppendReq:
#         future = self.queue.get()
#         batch = future.batch_append_request
#         self.futures_by_correlation_id[batch.correlation_id] = future
#         return _construct_batch_append_req(batch)
#
#     def pop_future(self, correlation_id: UUID) -> BatchAppendFuture:
#         return self.futures_by_correlation_id.pop(correlation_id)


class BaseStreamsService(ESDBService):
    def __init__(
        self,
        channel: Union[grpc.Channel, grpc.aio.Channel],
        connection_spec: ConnectionSpec,
    ):
        super().__init__(connection_spec=connection_spec)
        self._stub = streams_pb2_grpc.StreamsStub(channel)

    @staticmethod
    def _generate_append_reqs(
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
    ) -> Iterator[streams_pb2.AppendReq]:
        # First, define append request that has 'content' as 'options'.
        options = streams_pb2.AppendReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            )
        )
        # Decide 'expected_stream_revision'.
        if isinstance(current_version, int):
            assert current_version >= 0
            options.revision = current_version
        else:
            assert isinstance(current_version, StreamState)
            if current_version is StreamState.EXISTS:
                options.stream_exists.CopyFrom(shared_pb2.Empty())
            elif current_version is StreamState.ANY:
                options.any.CopyFrom(shared_pb2.Empty())
            else:
                assert current_version is StreamState.NO_STREAM
                options.no_stream.CopyFrom(shared_pb2.Empty())

        yield streams_pb2.AppendReq(options=options)

        # Secondly, define append requests that has 'content' as 'proposed_message'.
        for event in events:
            proposed_message = streams_pb2.AppendReq.ProposedMessage(
                id=shared_pb2.UUID(string=str(event.id)),
                metadata={"type": event.type, "content-type": event.content_type},
                custom_metadata=event.metadata,
                data=event.data,
            )
            yield streams_pb2.AppendReq(proposed_message=proposed_message)

    @staticmethod
    def _construct_batch_append_req(
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
        deadline: int,
        correlation_id: UUID,
    ) -> streams_pb2.BatchAppendReq:
        # Construct batch request 'options'.
        options = streams_pb2.BatchAppendReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            ),
            deadline=duration_pb2.Duration(
                seconds=deadline,
                nanos=0,
            ),
        )
        # Decide options 'expected_stream_revision'.
        if isinstance(current_version, int):
            assert current_version >= 0
            options.stream_position = current_version
        else:
            assert isinstance(current_version, StreamState)
            if current_version is StreamState.EXISTS:
                options.stream_exists.CopyFrom(empty_pb2.Empty())
            elif current_version is StreamState.ANY:
                options.any.CopyFrom(empty_pb2.Empty())
            else:
                assert current_version is StreamState.NO_STREAM
                options.no_stream.CopyFrom(empty_pb2.Empty())

        # Construct batch request 'proposed_messages'.
        # Todo: Split batch.events into chunks of 20?
        proposed_messages = []
        for event in events:
            proposed_message = streams_pb2.BatchAppendReq.ProposedMessage(
                id=shared_pb2.UUID(string=str(event.id)),
                metadata={"type": event.type, "content-type": event.content_type},
                custom_metadata=event.metadata,
                data=event.data,
            )
            proposed_messages.append(proposed_message)
        return streams_pb2.BatchAppendReq(
            correlation_id=shared_pb2.UUID(string=str(correlation_id)),
            options=options,
            proposed_messages=proposed_messages,
            is_final=True,  # This specifies the end of an atomic transaction.
        )

    @staticmethod
    def _convert_batch_append_resp(
        response: streams_pb2.BatchAppendResp, stream_name: str
    ) -> Union[BatchAppendResponse, EventStoreDBClientException]:
        result: Union[BatchAppendResponse, EventStoreDBClientException]
        # Response 'result' is either 'success' or 'error'.
        result_oneof = response.WhichOneof("result")
        if result_oneof == "success":
            # Construct response object.
            result = BatchAppendResponse(
                commit_position=response.success.position.commit_position,
            )
        else:
            # Construct exception object.
            assert result_oneof == "error"
            assert isinstance(response.error, status_pb2.Status)

            error_details = response.error.details
            if error_details.Is(shared_pb2.WrongExpectedVersion.DESCRIPTOR):
                wrong_version = shared_pb2.WrongExpectedVersion()
                error_details.Unpack(wrong_version)

                csro_oneof = wrong_version.WhichOneof("current_stream_revision_option")
                if csro_oneof == "current_no_stream":
                    result = NotFound(f"Stream {stream_name !r} not found")
                else:
                    assert csro_oneof == "current_stream_revision"
                    psn = wrong_version.current_stream_revision
                    result = WrongCurrentVersion(f"Current position is {psn}")

            # Todo: Write tests to cover all of this:
            elif error_details.Is(
                shared_pb2.AccessDenied.DESCRIPTOR
            ):  # pragma: no cover
                result = AccessDeniedError()
            elif error_details.Is(shared_pb2.StreamDeleted.DESCRIPTOR):
                stream_deleted = shared_pb2.StreamDeleted()
                error_details.Unpack(stream_deleted)
                # Todo: Ask ESDB team if this is ever different from request value.
                # stream_name = stream_deleted.stream_identifier.stream_name
                result = StreamIsDeleted(f"Stream {stream_name !r} is deleted")
            elif error_details.Is(shared_pb2.Timeout.DESCRIPTOR):  # pragma: no cover
                result = TimeoutError()
            elif error_details.Is(shared_pb2.Unknown.DESCRIPTOR):  # pragma: no cover
                result = UnknownError()
            elif error_details.Is(
                shared_pb2.InvalidTransaction.DESCRIPTOR
            ):  # pragma: no cover
                result = InvalidTransactionError()
            elif error_details.Is(
                shared_pb2.MaximumAppendSizeExceeded.DESCRIPTOR
            ):  # pragma: no cover
                size_exceeded = shared_pb2.MaximumAppendSizeExceeded()
                error_details.Unpack(size_exceeded)
                size = size_exceeded.maxAppendSize
                result = MaximumAppendSizeExceededError(f"Max size is {size}")
            elif error_details.Is(shared_pb2.BadRequest.DESCRIPTOR):  # pragma: no cover
                bad_request = shared_pb2.BadRequest()
                error_details.Unpack(bad_request)
                result = BadRequestError(f"Bad request: {bad_request.message}")
            else:
                # Unexpected error details type.
                result = EventStoreDBClientException(error_details)  # pragma: no cover
        return result

    @staticmethod
    def _construct_read_request(
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        from_end: bool = False,
        backwards: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        subscribe: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    ) -> streams_pb2.ReadReq:
        # Construct ReadReq.Options.
        options = streams_pb2.ReadReq.Options()

        # Decide 'stream_option'.
        if isinstance(stream_name, str):
            assert isinstance(stream_name, str)
            assert commit_position is None
            stream_options = streams_pb2.ReadReq.Options.StreamOptions(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                revision=stream_position or 0,
            )

            # Decide 'revision_option'.
            if stream_position is not None:
                stream_options.revision = stream_position
            elif from_end is True:
                stream_options.end.CopyFrom(shared_pb2.Empty())
            elif backwards is False:
                stream_options.start.CopyFrom(shared_pb2.Empty())
            else:
                stream_options.end.CopyFrom(shared_pb2.Empty())
            options.stream.CopyFrom(stream_options)
        else:
            assert stream_position is None
            if commit_position is not None:
                all_options = streams_pb2.ReadReq.Options.AllOptions(
                    position=streams_pb2.ReadReq.Options.Position(
                        commit_position=commit_position,
                        # prepare_position=prepare_position or commit_position,
                        prepare_position=commit_position,
                    )
                )
            elif backwards or from_end:
                all_options = streams_pb2.ReadReq.Options.AllOptions(
                    end=shared_pb2.Empty()
                )
            else:
                all_options = streams_pb2.ReadReq.Options.AllOptions(
                    start=shared_pb2.Empty()
                )
            options.all.CopyFrom(all_options)

        # Decide 'read_direction'.
        if backwards is False:
            options.read_direction = streams_pb2.ReadReq.Options.Forwards
        else:
            options.read_direction = streams_pb2.ReadReq.Options.Backwards

        # Decide 'resolve_links'.
        options.resolve_links = resolve_links

        # Decide 'count_option'.
        if subscribe:
            subscription = streams_pb2.ReadReq.Options.SubscriptionOptions()
            options.subscription.CopyFrom(subscription)

        else:
            options.count = limit

        # Decide 'filter_option'.
        if filter_exclude or filter_include:
            filter_options = streams_pb2.ReadReq.Options.FilterOptions(
                max=window_size,
                checkpointIntervalMultiplier=checkpoint_interval_multiplier,
            )

            # Decide 'expression'
            if filter_include:
                regex = construct_filter_include_regex(filter_include)
            else:
                regex = construct_filter_exclude_regex(filter_exclude)

            expression = streams_pb2.ReadReq.Options.FilterOptions.Expression(
                regex=regex
            )

            if filter_by_stream_name:
                filter_options.stream_identifier.CopyFrom(expression)
            else:
                filter_options.event_type.CopyFrom(expression)

            options.filter.CopyFrom(filter_options)
        else:
            options.no_filter.CopyFrom(shared_pb2.Empty())

        # Decide 'uuid_option'.
        options.uuid_option.CopyFrom(
            streams_pb2.ReadReq.Options.UUIDOption(string=shared_pb2.Empty())
        )

        # Decide 'control_option'.
        # Todo: What does this do, and what value should it have?

        return streams_pb2.ReadReq(options=options)

    @staticmethod
    def _construct_delete_req(
        stream_name: str, current_version: Union[int, StreamState]
    ) -> streams_pb2.DeleteReq:
        options = streams_pb2.DeleteReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            )
        )
        # Decide 'expected_stream_revision'.
        if isinstance(current_version, int):
            assert current_version >= 0
            options.revision = current_version
        else:
            assert isinstance(current_version, StreamState)
            if current_version is StreamState.EXISTS:
                options.stream_exists.CopyFrom(shared_pb2.Empty())
            elif current_version is StreamState.ANY:
                options.any.CopyFrom(shared_pb2.Empty())
            else:
                assert current_version is StreamState.NO_STREAM
                options.no_stream.CopyFrom(shared_pb2.Empty())

        return streams_pb2.DeleteReq(options=options)

    @staticmethod
    def _construct_tombstone_req(
        stream_name: str, current_version: Union[int, StreamState]
    ) -> streams_pb2.TombstoneReq:
        options = streams_pb2.TombstoneReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            )
        )
        # Decide 'expected_stream_revision'.
        if isinstance(current_version, int):
            assert current_version >= 0
            # Stream position is expected to be a certain value.
            options.revision = current_version
        else:
            assert isinstance(current_version, StreamState)
            if current_version is StreamState.EXISTS:
                options.stream_exists.CopyFrom(shared_pb2.Empty())
            elif current_version is StreamState.ANY:
                options.any.CopyFrom(shared_pb2.Empty())
            else:
                assert current_version is StreamState.NO_STREAM
                options.no_stream.CopyFrom(shared_pb2.Empty())
        return streams_pb2.TombstoneReq(options=options)


class AsyncioStreamsService(BaseStreamsService):
    async def batch_append(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> BatchAppendResponse:
        # Call the gRPC method.
        try:
            req = self._construct_batch_append_req(
                stream_name=stream_name,
                current_version=current_version,
                events=events,
                deadline=DEFAULT_BATCH_APPEND_REQUEST_DEADLINE,
                correlation_id=uuid4(),
            )
            async for response in self._stub.BatchAppend(
                iter([req]),
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            ):
                assert isinstance(response, streams_pb2.BatchAppendResp)
                result = self._convert_batch_append_resp(response, stream_name)
                if isinstance(result, BatchAppendResponse):
                    return result
                else:
                    assert isinstance(result, EventStoreDBClientException)
                    raise result
            else:  # pragma: no cover
                raise EventStoreDBClientException("Batch append response not received")

        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e

    @overload
    async def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioReadResponse:
        """
        Signature for reading events from a stream.
        """

    @overload
    async def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        subscribe: Literal[True],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Signature for reading events from a stream with a catch-up subscription.
        """

    @overload
    async def read(
        self,
        *,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioReadResponse:
        """
        Signature for reading all events.
        """

    @overload
    async def read(
        self,
        *,
        commit_position: Optional[int] = None,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        subscribe: Literal[True],
        include_checkpoints: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Signature for reading all events with a catch-up subscription.
        """

    async def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        subscribe: bool = False,
        include_checkpoints: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Union[AsyncioReadResponse, AsyncioCatchupSubscription]:
        """
        Constructs and sends a gRPC 'ReadReq' to the 'Read' rpc.

        Returns a generator which yields RecordedEvent objects.
        """

        # Construct read request.
        read_req = self._construct_read_request(
            stream_name=stream_name,
            stream_position=stream_position,
            commit_position=commit_position,
            backwards=backwards,
            resolve_links=resolve_links,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=limit,
            subscribe=subscribe,
            window_size=window_size,
            checkpoint_interval_multiplier=checkpoint_interval_multiplier,
        )

        # Send the read request, and iterate over the response.
        unary_stream_call: grpc.aio.UnaryStreamCall[
            streams_pb2.ReadReq, streams_pb2.ReadResp
        ] = self._stub.Read(
            read_req,
            timeout=timeout,
            metadata=self._metadata(metadata),
            credentials=credentials,
        )

        if not subscribe:
            response = AsyncioReadResponse(
                aio_call=unary_stream_call,
                stream_name=stream_name,
            )
        else:
            response = AsyncioCatchupSubscription(
                aio_call=unary_stream_call,
                stream_name=stream_name,
                include_checkpoints=include_checkpoints,
            )
            await response.check_confirmation()
        return response

    async def delete(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        delete_req = self._construct_delete_req(stream_name, current_version)

        try:
            delete_resp = await self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongCurrentVersion() from e
                elif "is deleted" in details:
                    raise StreamIsDeleted() from e
                else:  # pragma: no cover
                    raise handle_rpc_error(e) from e
            else:
                raise handle_rpc_error(e) from e
        else:
            assert isinstance(delete_resp, streams_pb2.DeleteResp), delete_resp

    async def tombstone(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        tombstone_req = self._construct_tombstone_req(stream_name, current_version)

        try:
            tombstone_resp = await self._stub.Tombstone(
                tombstone_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongCurrentVersion() from e
                elif "is deleted" in details:
                    raise StreamIsDeleted() from e
                else:  # pragma: no cover
                    raise handle_rpc_error(e) from e
            else:
                raise handle_rpc_error(e) from e
        else:
            assert isinstance(tombstone_resp, streams_pb2.TombstoneResp)


class StreamsService(BaseStreamsService):
    """
    Encapsulates the 'streams.Streams' gRPC service.
    """

    @overload
    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ReadResponse:
        """
        Signature for reading events from a stream.
        """

    @overload
    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        from_end: bool = False,
        resolve_links: bool = False,
        subscribe: Literal[True],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> CatchupSubscription:
        """
        Signature for reading events from a stream with a catch-up subscription.
        """

    @overload
    def read(
        self,
        *,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ReadResponse:
        """
        Signature for reading all events.
        """

    @overload
    def read(
        self,
        *,
        commit_position: Optional[int] = None,
        from_end: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        subscribe: Literal[True],
        include_checkpoints: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> CatchupSubscription:
        """
        Signature for reading all events with a catch-up subscription.
        """

    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        from_end: bool = False,
        backwards: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        subscribe: bool = False,
        include_checkpoints: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Union[ReadResponse, CatchupSubscription]:
        """
        Constructs and sends a gRPC 'ReadReq' to the 'Read' rpc.

        Returns a generator which yields RecordedEvent objects.
        """

        # Construct read request.
        read_req = self._construct_read_request(
            stream_name=stream_name,
            stream_position=stream_position,
            commit_position=commit_position,
            from_end=from_end,
            backwards=backwards,
            resolve_links=resolve_links,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=limit,
            subscribe=subscribe,
            window_size=window_size,
            checkpoint_interval_multiplier=checkpoint_interval_multiplier,
        )

        # Send the read request, and iterate over the response.
        read_resps = self._stub.Read(
            read_req,
            timeout=timeout,
            metadata=self._metadata(metadata),
            credentials=credentials,
        )
        assert isinstance(read_resps, _ReadResps)  # a _MultiThreadedRendezvous

        if subscribe is False:
            return ReadResponse(read_resps, stream_name=stream_name)
        else:
            return CatchupSubscription(
                read_resps,
                stream_name=stream_name,
                include_checkpoints=include_checkpoints,
            )

    def append(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        """
        Constructs and sends a stream of gRPC 'AppendReq' to the 'Append' rpc.

        Returns the commit position of the last appended event.
        """
        try:
            append_reqs = self._generate_append_reqs(
                stream_name=stream_name,
                current_version=current_version,
                events=events,
            )
            append_resp = self._stub.Append(
                append_reqs,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e
        else:
            assert isinstance(append_resp, streams_pb2.AppendResp)
            # Response 'result' is either 'success' or 'wrong_expected_version'.
            result_oneof = append_resp.WhichOneof("result")
            if result_oneof == "success":
                return append_resp.success.position.commit_position
            else:
                assert result_oneof == "wrong_expected_version", result_oneof
                wev = append_resp.wrong_expected_version
                cro_oneof = wev.WhichOneof("current_revision_option")
                if cro_oneof == "current_revision":
                    raise WrongCurrentVersion(
                        f"Current version is {wev.current_revision}"
                    )
                else:
                    assert cro_oneof == "current_no_stream", cro_oneof
                    raise WrongCurrentVersion(f"Stream {stream_name!r} does not exist")

    # def batch_append_multiplexed(
    #     self,
    #     futures_queue: BatchAppendFutureQueue,
    #     timeout: Optional[float] = None,
    #     metadata: Optional[Metadata] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> None:
    #     # Construct batch append requests iterator.
    #     requests = BatchAppendFutureIterator(futures_queue)
    #
    #     # Call the gRPC method.
    #     try:
    #         for response in self._stub.BatchAppend(
    #             requests,
    #             timeout=timeout,
    #             metadata=self._metadata(metadata, requires_leader=True),
    #             credentials=credentials,
    #         ):
    #             # Use the correlation ID to get the future.
    #             assert isinstance(response, streams_pb2.BatchAppendResp)
    #             correlation_id = UUID(response.correlation_id.string)
    #             future = requests.pop_future(correlation_id)
    #
    #             # Convert the result.
    #             stream_name = future.batch_append_request.stream_name
    #             result = self._convert_batch_append_result(response, stream_name)
    #
    #             # Finish the future.
    #             if isinstance(result, BatchAppendResponse):
    #                 future.set_result(result)
    #             else:
    #                 assert isinstance(result, EventStoreDBClientException)
    #                 future.set_exception(result)
    #
    #         else:
    #             # The response stream ended without an RPC error.
    #             for (
    #                 correlation_id
    #             ) in requests.futures_by_correlation_id:  # pragma: no cover
    #                 future = requests.pop_future(correlation_id)
    #                 future.set_exception(
    #                     EventStoreDBClientException("Batch append response not received")
    #                 )
    #
    #     except grpc.RpcError as rpc_error:
    #         # The response stream ended with an RPC error.
    #         try:
    #             raise handle_rpc_error(rpc_error) from rpc_error
    #         except GrpcError as grpc_error:
    #             for (
    #                 correlation_id
    #             ) in requests.futures_by_correlation_id:  # pragma: no cover
    #                 future = requests.pop_future(correlation_id)
    #                 future.set_exception(grpc_error)
    #             raise

    def batch_append(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> BatchAppendResponse:
        # Call the gRPC method.
        try:
            req = self._construct_batch_append_req(
                stream_name=stream_name,
                current_version=current_version,
                events=events,
                deadline=DEFAULT_BATCH_APPEND_REQUEST_DEADLINE,
                correlation_id=uuid4(),
            )
            for response in self._stub.BatchAppend(
                iter([req]),
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            ):
                assert isinstance(response, streams_pb2.BatchAppendResp)
                result = self._convert_batch_append_resp(response, stream_name)
                if isinstance(result, BatchAppendResponse):
                    return result
                else:
                    assert isinstance(result, EventStoreDBClientException)
                    raise result
            else:  # pragma: no cover
                raise EventStoreDBClientException("Batch append response not received")

        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e

    def delete(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        delete_req = self._construct_delete_req(stream_name, current_version)

        try:
            delete_resp = self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongCurrentVersion() from e
                elif "is deleted" in details:
                    raise StreamIsDeleted() from e
                else:  # pragma: no cover
                    raise handle_rpc_error(e) from e
            else:
                raise handle_rpc_error(e) from e
        else:
            assert isinstance(delete_resp, streams_pb2.DeleteResp)
            # position_option_oneof = delete_resp.WhichOneof("position_option")
            # if position_option_oneof == "position":
            #     return delete_resp.position
            # else:
            #     return delete_resp.no_position

    def tombstone(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        tombstone_req = self._construct_tombstone_req(stream_name, current_version)

        try:
            tombstone_resp = self._stub.Tombstone(
                tombstone_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongCurrentVersion() from e
                elif "is deleted" in details:
                    raise StreamIsDeleted() from e
                else:  # pragma: no cover
                    raise handle_rpc_error(e) from e
            else:
                raise handle_rpc_error(e) from e
        else:
            assert isinstance(tombstone_resp, streams_pb2.TombstoneResp)
            # position_option_oneof = tombstone_resp.WhichOneof("position_option")
            # if position_option_oneof == "position":
            #     return tombstone_resp.position
            # else:
            #     return tombstone_resp.no_position
