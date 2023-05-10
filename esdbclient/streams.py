# -*- coding: utf-8 -*-
import sys
from dataclasses import dataclass
from typing import (
    AsyncIterable,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Union,
    overload,
)
from uuid import UUID, uuid4

import grpc.aio
from google.protobuf import duration_pb2, empty_pb2
from grpc import CallCredentials, RpcError, StatusCode
from typing_extensions import Literal

from esdbclient.esdbapibase import ESDBService, Metadata, handle_rpc_error
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    AccessDeniedError,
    BadRequestError,
    ESDBClientException,
    InvalidTransactionError,
    MaximumAppendSizeExceededError,
    NotFound,
    StreamIsDeleted,
    SubscriptionConfirmationError,
    TimeoutError,
    UnknownError,
    WrongExpectedPosition,
)
from esdbclient.protos.Grpc import shared_pb2, status_pb2, streams_pb2, streams_pb2_grpc


class AsyncioReadResponse:
    def __init__(
        self,
        read_resp_iter: AsyncIterable[streams_pb2.ReadResp],
        stream_name: Optional[str],
        is_subscription: bool,
    ):
        self.read_resp_iter = read_resp_iter.__aiter__()
        self.stream_name = stream_name
        self.subscription_id: Optional[UUID] = None
        self.is_subscription = is_subscription

    async def _get_next_read_resp(self) -> streams_pb2.ReadResp:
        try:
            read_resp = await self.read_resp_iter.__anext__()
        except RpcError as e:
            if e.code() == StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if self.stream_name and details and "is deleted" in details:
                    raise StreamIsDeleted() from e
                else:  # pragma: no cover
                    raise handle_rpc_error(e) from e
            else:
                raise handle_rpc_error(e) from e
        assert isinstance(read_resp, streams_pb2.ReadResp)
        return read_resp

    async def check_confirmation(self) -> None:
        if self.is_subscription:
            read_resp = await self._get_next_read_resp()
            content_oneof = read_resp.WhichOneof("content")
            if content_oneof != "confirmation":  # pragma: no cover
                raise SubscriptionConfirmationError(
                    f"Expected subscription confirmation, got: {read_resp}"
                )

    def __aiter__(self) -> "AsyncioReadResponse":
        return self

    async def __anext__(self) -> "RecordedEvent":
        while True:
            read_resp = await self._get_next_read_resp()
            content_oneof = read_resp.WhichOneof("content")
            if content_oneof == "event":
                event = read_resp.event.event
                position_oneof = read_resp.event.WhichOneof("position")
                if position_oneof == "commit_position":
                    commit_position = read_resp.event.commit_position
                else:  # pragma: no cover
                    # We only get here with EventStoreDB < 22.10.
                    assert position_oneof == "no_position", position_oneof
                    commit_position = None

                return RecordedEvent(
                    id=UUID(event.id.string),
                    type=event.metadata["type"],
                    data=event.data,
                    content_type=event.metadata["content-type"],
                    metadata=event.custom_metadata,
                    stream_name=event.stream_identifier.stream_name.decode("utf8"),
                    stream_position=event.stream_revision,
                    commit_position=commit_position,
                )
            elif content_oneof == "stream_not_found":
                raise NotFound(f"Stream {self.stream_name!r} not found")
            else:
                pass  # pragma: no cover
                # Todo: Maybe support other content_oneof values:
                #   oneof content {
                # 		ReadEvent event = 1;
                # 		SubscriptionConfirmation confirmation = 2;
                # 		Checkpoint checkpoint = 3;
                # 		StreamNotFound stream_not_found = 4;
                # 		uint64 first_stream_position = 5;
                # 		uint64 last_stream_position = 6;
                # 		AllStreamPosition last_all_stream_position = 7;
                # 	}
                #
                # Todo: Not sure how to request to get first_stream_position,
                #   last_stream_position, first_all_stream_position.


class ReadResponse(Iterable[RecordedEvent]):
    def __init__(
        self,
        read_resp_iter: Iterable[streams_pb2.ReadResp],
        stream_name: Optional[str],
        is_subscription: bool,
    ):
        self.read_resp_iter = iter(read_resp_iter)
        self.stream_name = stream_name
        self.subscription_id: Optional[UUID] = None
        if is_subscription:
            read_resp = self._get_next_read_resp()
            content_oneof = read_resp.WhichOneof("content")
            if content_oneof == "confirmation":
                pass
                # Todo: What is 'read_resp.confirmation.subscription_id' for?
            else:  # pragma: no cover
                raise SubscriptionConfirmationError(
                    f"Expected subscription confirmation, got: {read_resp}"
                )

    def __iter__(self) -> "ReadResponse":
        return self

    def __next__(self) -> RecordedEvent:
        while True:
            read_resp = self._get_next_read_resp()
            content_oneof = read_resp.WhichOneof("content")
            if content_oneof == "event":
                event = read_resp.event.event
                position_oneof = read_resp.event.WhichOneof("position")
                if position_oneof == "commit_position":
                    commit_position = read_resp.event.commit_position
                else:  # pragma: no cover
                    # We only get here with EventStoreDB < 22.10.
                    assert position_oneof == "no_position", position_oneof
                    commit_position = None

                return RecordedEvent(
                    id=UUID(event.id.string),
                    type=event.metadata["type"],
                    data=event.data,
                    content_type=event.metadata["content-type"],
                    metadata=event.custom_metadata,
                    stream_name=event.stream_identifier.stream_name.decode("utf8"),
                    stream_position=event.stream_revision,
                    commit_position=commit_position,
                )
            elif content_oneof == "stream_not_found":
                raise NotFound(f"Stream {self.stream_name!r} not found")
            else:
                pass  # pragma: no cover
                # Todo: Maybe support other content_oneof values:
                #   oneof content {
                # 		ReadEvent event = 1;
                # 		SubscriptionConfirmation confirmation = 2;
                # 		Checkpoint checkpoint = 3;
                # 		StreamNotFound stream_not_found = 4;
                # 		uint64 first_stream_position = 5;
                # 		uint64 last_stream_position = 6;
                # 		AllStreamPosition last_all_stream_position = 7;
                # 	}
                #
                # Todo: Not sure how to request to get first_stream_position,
                #   last_stream_position, first_all_stream_position.

    def _get_next_read_resp(self) -> streams_pb2.ReadResp:
        try:
            read_resp = next(self.read_resp_iter)
        except RpcError as e:
            if e.code() == StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if self.stream_name and details and "is deleted" in details:
                    raise StreamIsDeleted() from e
                else:  # pragma: no cover
                    raise handle_rpc_error(e) from e
            else:
                raise handle_rpc_error(e) from e
        assert isinstance(read_resp, streams_pb2.ReadResp)
        return read_resp


DEFAULT_BATCH_APPEND_REQUEST_DEADLINE = 315576000000

# @dataclass
# class BatchAppendRequest:
#     stream_name: str
#     expected_position: Optional[int]
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
    def __init__(self, channel: Union[grpc.Channel, grpc.aio.Channel]):
        self._stub = streams_pb2_grpc.StreamsStub(channel)

    def _construct_read_request(
        self,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        subscribe: bool = False,
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
                        prepare_position=commit_position,
                    )
                )
            elif backwards:
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
        options.resolve_links = False

        # Decide 'count_option'.
        if subscribe:
            subscription = streams_pb2.ReadReq.Options.SubscriptionOptions()
            options.subscription.CopyFrom(subscription)

        else:
            options.count = limit

        # Decide 'filter_option'.
        if filter_exclude or filter_include:
            if filter_include:
                filter_include = (
                    [filter_include]
                    if isinstance(filter_include, str)
                    else filter_include
                )
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_exclude = (
                    [filter_exclude]
                    if isinstance(filter_exclude, str)
                    else filter_exclude
                )
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter_expression = streams_pb2.ReadReq.Options.FilterOptions.Expression(
                regex=filter_regex
            )
            if filter_by_stream_name:
                stream_identifier_filter = filter_expression
                event_type_filter = None
            else:
                stream_identifier_filter = None
                event_type_filter = filter_expression

            filter_options = streams_pb2.ReadReq.Options.FilterOptions(
                stream_identifier=stream_identifier_filter,
                event_type=event_type_filter,
                # Todo: What does 'window' mean?
                # max=shared_pb2.Empty(),
                count=shared_pb2.Empty(),
                # Todo: What does 'checkpointIntervalMultiplier' mean?
                checkpointIntervalMultiplier=5,
            )
            options.filter.CopyFrom(filter_options)
        else:
            options.no_filter.CopyFrom(shared_pb2.Empty())

        # Decide 'uuid_option'.
        options.uuid_option.CopyFrom(
            streams_pb2.ReadReq.Options.UUIDOption(string=shared_pb2.Empty())
        )

        return streams_pb2.ReadReq(options=options)

    @staticmethod
    def _construct_batch_append_req(
        stream_name: str,
        expected_position: Optional[int],
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
        if isinstance(expected_position, int):
            if expected_position >= 0:
                # Stream is expected to exist.
                options.stream_position = expected_position
            else:
                # Disable optimistic concurrency control.
                options.any.CopyFrom(empty_pb2.Empty())
        else:
            # Stream is expected not to exist.
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

    def _convert_batch_append_resp(
        self, response: streams_pb2.BatchAppendResp, stream_name: str
    ) -> Union[BatchAppendResponse, ESDBClientException]:
        result: Union[BatchAppendResponse, ESDBClientException]
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
                    result = WrongExpectedPosition(f"Current position is {psn}")

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
                result = ESDBClientException(error_details)  # pragma: no cover
        return result

    def _construct_delete_req(
        self, stream_name: str, expected_position: Optional[int]
    ) -> streams_pb2.DeleteReq:
        options = streams_pb2.DeleteReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            )
        )
        # Decide 'expected_stream_revision'.
        if isinstance(expected_position, int):
            if expected_position >= 0:
                # Stream position is expected to be a certain value.
                options.revision = expected_position
            else:
                # Disable optimistic concurrency control.
                options.any.CopyFrom(shared_pb2.Empty())
        else:
            # Stream is expected to exist.
            options.stream_exists.CopyFrom(shared_pb2.Empty())
        return streams_pb2.DeleteReq(options=options)

    def _construct_tombstone_req(
        self, stream_name: str, expected_position: Optional[int]
    ) -> streams_pb2.TombstoneReq:
        options = streams_pb2.TombstoneReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            )
        )
        # Decide 'expected_stream_revision'.
        if isinstance(expected_position, int):
            if expected_position >= 0:
                # Stream position is expected to be a certain value.
                options.revision = expected_position
            else:
                # Disable optimistic concurrency control.
                options.any.CopyFrom(shared_pb2.Empty())
        else:
            # Stream is expected to exist.
            options.stream_exists.CopyFrom(shared_pb2.Empty())
        return streams_pb2.TombstoneReq(options=options)


class AsyncioStreamsService(BaseStreamsService):
    async def batch_append(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> BatchAppendResponse:
        # Call the gRPC method.
        try:
            req = self._construct_batch_append_req(
                stream_name=stream_name,
                expected_position=expected_position,
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
                    assert isinstance(result, ESDBClientException)
                    raise result
            else:  # pragma: no cover
                raise ESDBClientException("Batch append response not received")

        except RpcError as e:
            raise handle_rpc_error(e) from e

    @overload
    async def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> AsyncIterable[RecordedEvent]:
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
        credentials: Optional[CallCredentials] = None,
    ) -> AsyncIterable[RecordedEvent]:
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
        credentials: Optional[CallCredentials] = None,
    ) -> AsyncIterable[RecordedEvent]:
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
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> AsyncIterable[RecordedEvent]:
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
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        subscribe: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> AsyncIterable[RecordedEvent]:
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
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=limit,
            subscribe=subscribe,
        )

        # Send the read request, and iterate over the response.
        unary_stream_call: AsyncIterable[streams_pb2.ReadResp] = self._stub.Read(
            read_req,
            timeout=timeout,
            metadata=self._metadata(metadata),
            credentials=credentials,
        )

        response = AsyncioReadResponse(
            unary_stream_call, stream_name=stream_name, is_subscription=subscribe
        )
        await response.check_confirmation()
        return response

    async def delete(
        self,
        stream_name: str,
        expected_position: Optional[int],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        delete_req = self._construct_delete_req(stream_name, expected_position)

        try:
            delete_resp = await self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongExpectedPosition() from e
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
        expected_position: Optional[int],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        tombstone_req = self._construct_tombstone_req(stream_name, expected_position)

        try:
            tombstone_resp = await self._stub.Tombstone(
                tombstone_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongExpectedPosition() from e
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
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading events from a stream.
        """

    @overload
    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        subscribe: Literal[True],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading events from a stream with a catch-up subscription.
        """

    @overload
    def read(
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
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading all events.
        """

    @overload
    def read(
        self,
        *,
        commit_position: Optional[int] = None,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        subscribe: Literal[True],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading all events with a catch-up subscription.
        """

    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        subscribe: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
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
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=limit,
            subscribe=subscribe,
        )

        # Send the read request, and iterate over the response.
        read_resp = self._stub.Read(
            read_req,
            timeout=timeout,
            metadata=self._metadata(metadata),
            credentials=credentials,
        )

        return ReadResponse(
            read_resp, stream_name=stream_name, is_subscription=subscribe
        )

    def append(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> int:
        """
        Constructs and sends a stream of gRPC 'AppendReq' to the 'Append' rpc.

        Returns the commit position of the last appended event.
        """
        try:
            response = self._stub.Append(
                self._generate_append_requests(
                    stream_name=stream_name,
                    expected_position=expected_position,
                    events=events,
                ),
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            raise handle_rpc_error(e) from e
        else:
            assert isinstance(response, streams_pb2.AppendResp)
            # Response 'result' is either 'success' or 'wrong_expected_version'.
            result_oneof = response.WhichOneof("result")
            if result_oneof == "success":
                return response.success.position.commit_position
            else:
                assert result_oneof == "wrong_expected_version", result_oneof
                wev = response.wrong_expected_version
                cro_oneof = wev.WhichOneof("current_revision_option")
                if cro_oneof == "current_revision":
                    raise WrongExpectedPosition(
                        f"Current position is {wev.current_revision}"
                    )
                else:
                    assert cro_oneof == "current_no_stream", cro_oneof
                    raise WrongExpectedPosition(
                        f"Stream {stream_name!r} does not exist"
                    )

    def _generate_append_requests(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
    ) -> Iterator[streams_pb2.AppendReq]:
        # First, define append request that has 'content' as 'options'.
        options = streams_pb2.AppendReq.Options(
            stream_identifier=shared_pb2.StreamIdentifier(
                stream_name=stream_name.encode("utf8")
            )
        )
        # Decide 'expected_stream_revision'.
        if isinstance(expected_position, int):
            if expected_position >= 0:
                # Stream is expected to exist.
                options.revision = expected_position
            else:
                # Disable optimistic concurrency control.
                options.any.CopyFrom(shared_pb2.Empty())
        else:
            # Stream is expected not to exist.
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

    # def batch_append_multiplexed(
    #     self,
    #     futures_queue: BatchAppendFutureQueue,
    #     timeout: Optional[float] = None,
    #     metadata: Optional[Metadata] = None,
    #     credentials: Optional[CallCredentials] = None,
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
    #                 assert isinstance(result, ESDBClientException)
    #                 future.set_exception(result)
    #
    #         else:
    #             # The response stream ended without an RPC error.
    #             for (
    #                 correlation_id
    #             ) in requests.futures_by_correlation_id:  # pragma: no cover
    #                 future = requests.pop_future(correlation_id)
    #                 future.set_exception(
    #                     ESDBClientException("Batch append response not received")
    #                 )
    #
    #     except RpcError as rpc_error:
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
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> BatchAppendResponse:
        # Call the gRPC method.
        try:
            req = self._construct_batch_append_req(
                stream_name=stream_name,
                expected_position=expected_position,
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
                    assert isinstance(result, ESDBClientException)
                    raise result
            else:  # pragma: no cover
                raise ESDBClientException("Batch append response not received")

        except RpcError as e:
            raise handle_rpc_error(e) from e

    def delete(
        self,
        stream_name: str,
        expected_position: Optional[int],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        delete_req = self._construct_delete_req(stream_name, expected_position)

        try:
            delete_resp = self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongExpectedPosition() from e
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
        expected_position: Optional[int],
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        tombstone_req = self._construct_tombstone_req(stream_name, expected_position)

        try:
            tombstone_resp = self._stub.Tombstone(
                tombstone_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.FAILED_PRECONDITION:
                details = e.details() or ""
                if "WrongExpectedVersion" in details:
                    if "Actual version: -1" in details:
                        raise NotFound() from e
                    else:
                        raise WrongExpectedPosition() from e
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
