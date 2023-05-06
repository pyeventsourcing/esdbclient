# -*- coding: utf-8 -*-
import queue
import sys
from base64 import b64encode
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)
from uuid import UUID, uuid4

import grpc
from google.protobuf import duration_pb2, empty_pb2
from grpc import (
    AuthMetadataContext,
    AuthMetadataPluginCallback,
    Call,
    CallCredentials,
    Channel,
    RpcError,
    StatusCode,
)
from typing_extensions import Literal

if TYPE_CHECKING:  # pragma: no cover
    from grpc import Metadata
else:
    Metadata = Tuple[Tuple[str, str], ...]

from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    AccessDeniedError,
    BadRequestError,
    DeadlineExceeded,
    ESDBClientException,
    ExceptionThrownByHandler,
    ExpectedPositionError,
    GrpcError,
    InvalidTransactionError,
    MaximumAppendSizeExceededError,
    NodeIsNotLeader,
    ServiceUnavailable,
    StreamDeletedError,
    StreamNotFound,
    SubscriptionNotFound,
    TimeoutError,
    UnknownError,
)
from esdbclient.protos.Grpc import (
    cluster_pb2,
    cluster_pb2_grpc,
    gossip_pb2,
    gossip_pb2_grpc,
    persistent_pb2,
    persistent_pb2_grpc,
    shared_pb2,
    status_pb2,
    streams_pb2,
    streams_pb2_grpc,
)


class BasicAuthCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        credentials = b64encode(f"{username}:{password}".encode())
        self._metadata = (("authorization", (b"Basic " + credentials)),)

    def __call__(
        self, context: AuthMetadataContext, callback: AuthMetadataPluginCallback
    ) -> None:
        callback(self._metadata, None)


def handle_rpc_error(e: RpcError) -> ESDBClientException:
    """
    Converts gRPC errors to client exceptions.
    """
    if isinstance(e, Call):
        if (
            e.code() == StatusCode.UNKNOWN
            and "Exception was thrown by handler" in e.details()
        ):
            return ExceptionThrownByHandler(e)
        elif e.code() == StatusCode.UNAVAILABLE:
            return ServiceUnavailable(e)
        elif e.code() == StatusCode.DEADLINE_EXCEEDED:
            return DeadlineExceeded(e)
        if e.code() == StatusCode.NOT_FOUND and e.details() == "Leader info available":
            return NodeIsNotLeader(e)
    return GrpcError(e)


@dataclass
class BatchAppendRequest:
    stream_name: str
    expected_position: Optional[int]
    events: Iterable[NewEvent]
    correlation_id: UUID = field(default_factory=uuid4)
    deadline: int = 315576000000


@dataclass
class BatchAppendResponse:
    commit_position: int


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


class ESDBService:
    def _metadata(
        self, metadata: Optional[Metadata], requires_leader: bool = False
    ) -> Metadata:
        requires_leader_metadata: Metadata = (
            ("requires-leader", "true" if requires_leader else "false"),
        )
        metadata = tuple() if metadata is None else metadata
        return metadata + requires_leader_metadata


class StreamsService(ESDBService):
    """
    Encapsulates the 'streams.Streams' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = streams_pb2_grpc.StreamsStub(channel)

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

    def _construct_read_request(
        self,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
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
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter = streams_pb2.ReadReq.Options.FilterOptions(
                event_type=streams_pb2.ReadReq.Options.FilterOptions.Expression(
                    regex=filter_regex
                ),
                # Todo: What does 'window' mean?
                # max=shared_pb2.Empty(),
                count=shared_pb2.Empty(),
                # Todo: What does 'checkpointIntervalMultiplier' mean?
                checkpointIntervalMultiplier=5,
            )
            options.filter.CopyFrom(filter)
        else:
            options.no_filter.CopyFrom(shared_pb2.Empty())

        # Decide 'uuid_option'.
        options.uuid_option.CopyFrom(
            streams_pb2.ReadReq.Options.UUIDOption(string=shared_pb2.Empty())
        )

        return streams_pb2.ReadReq(options=options)

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
                    raise ExpectedPositionError(
                        f"Current position is {wev.current_revision}"
                    )
                else:
                    assert cro_oneof == "current_no_stream", cro_oneof
                    raise ExpectedPositionError(
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

    def _convert_batch_append_result(
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
                    result = StreamNotFound(f"Stream {stream_name !r} not found")
                else:
                    assert csro_oneof == "current_stream_revision"
                    psn = wrong_version.current_stream_revision
                    result = ExpectedPositionError(f"Current position is {psn}")

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
                result = StreamDeletedError(f"Stream {stream_name !r} is deleted")
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

    def batch_append(
        self,
        batch_append_request: BatchAppendRequest,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> BatchAppendResponse:
        # Call the gRPC method.
        try:
            for response in self._stub.BatchAppend(
                iter([_construct_batch_append_req(batch_append_request)]),
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            ):
                assert isinstance(response, streams_pb2.BatchAppendResp)
                stream_name = batch_append_request.stream_name
                result = self._convert_batch_append_result(response, stream_name)
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

        delete_req = streams_pb2.DeleteReq(options=options)

        try:
            delete_resp = self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            # Todo: Raise WrongExceptedVersion if expected_position is wrong...
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

        tombstone_req = streams_pb2.TombstoneReq(options=options)

        try:
            tombstone_resp = self._stub.Tombstone(
                tombstone_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            # Todo: Raise WrongExceptedVersion if expected_position is wrong...
            raise handle_rpc_error(e) from e

        else:
            assert isinstance(tombstone_resp, streams_pb2.TombstoneResp)
            # position_option_oneof = tombstone_resp.WhichOneof("position_option")
            # if position_option_oneof == "position":
            #     return tombstone_resp.position
            # else:
            #     return tombstone_resp.no_position


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


def _construct_batch_append_req(
    batch: BatchAppendRequest,
) -> streams_pb2.BatchAppendReq:
    # Construct batch request 'options'.
    options = streams_pb2.BatchAppendReq.Options(
        stream_identifier=shared_pb2.StreamIdentifier(
            stream_name=batch.stream_name.encode("utf8")
        ),
        deadline=duration_pb2.Duration(seconds=batch.deadline, nanos=0),
    )
    # Decide options 'expected_stream_revision'.
    if isinstance(batch.expected_position, int):
        if batch.expected_position >= 0:
            # Stream is expected to exist.
            options.stream_position = batch.expected_position
        else:
            # Disable optimistic concurrency control.
            options.any.CopyFrom(empty_pb2.Empty())
    else:
        # Stream is expected not to exist.
        options.no_stream.CopyFrom(empty_pb2.Empty())
    # Construct batch request 'proposed_messages'.
    # Todo: Split batch.events into chunks of 20?
    proposed_messages = []
    for event in batch.events:
        proposed_message = streams_pb2.BatchAppendReq.ProposedMessage(
            id=shared_pb2.UUID(string=str(event.id)),
            metadata={"type": event.type, "content-type": event.content_type},
            custom_metadata=event.metadata,
            data=event.data,
        )
        proposed_messages.append(proposed_message)
    batch_append_req = streams_pb2.BatchAppendReq(
        correlation_id=shared_pb2.UUID(string=str(batch.correlation_id)),
        options=options,
        proposed_messages=proposed_messages,
        is_final=True,  # This specifies the end of an atomic transaction.
    )
    return batch_append_req


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
                # Todo: What is this actually for?
                self.subscription_id = UUID(read_resp.confirmation.subscription_id)
            else:  # pragma: no cover
                raise ESDBClientException(
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
                raise StreamNotFound(f"Stream {self.stream_name!r} not found")
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
            raise handle_rpc_error(e) from e
        assert isinstance(read_resp, streams_pb2.ReadResp)
        return read_resp


class SubscriptionReadRequest:
    def __init__(self, group_name: str, stream_name: Optional[str] = None) -> None:
        self.group_name = group_name
        self.stream_name = stream_name
        self.queue: queue.Queue[shared_pb2.UUID] = queue.Queue()
        self._has_requested_options = False

    def __next__(self) -> persistent_pb2.ReadReq:
        if not self._has_requested_options:
            self._has_requested_options = True
            options = persistent_pb2.ReadReq.Options(
                group_name=self.group_name,
                buffer_size=100,
                uuid_option=persistent_pb2.ReadReq.Options.UUIDOption(
                    # structured=shared_pb2.Empty(),
                    string=shared_pb2.Empty(),
                ),
            )
            # Decide 'stream_option'.
            if isinstance(self.stream_name, str):
                options.stream_identifier.CopyFrom(
                    shared_pb2.StreamIdentifier(
                        stream_name=self.stream_name.encode("utf8")
                    )
                )
            else:
                options.all.CopyFrom(shared_pb2.Empty())
            return persistent_pb2.ReadReq(options=options)
        else:
            ids = []
            # Send an Ack whenever there are 100 acks, or at least
            # every 100ms when there are some acks, otherwise don't.
            while True:
                try:
                    event_id = self.queue.get(timeout=0.1)
                    ids.append(event_id)
                    if len(ids) >= 100:
                        return persistent_pb2.ReadReq(
                            ack=persistent_pb2.ReadReq.Ack(ids=ids)
                        )
                except queue.Empty:
                    if len(ids) >= 1:
                        return persistent_pb2.ReadReq(
                            ack=persistent_pb2.ReadReq.Ack(ids=ids)
                        )
                    else:  # pragma: no cover
                        pass

    def ack(self, event_id: UUID) -> None:
        self.queue.put(shared_pb2.UUID(string=str(event_id)))

    # Todo: Implement nack().


class SubscriptionReadResponse:
    def __init__(self, resp: Iterable[persistent_pb2.ReadResp]):
        self.resp = iter(resp)

    def __iter__(self) -> "SubscriptionReadResponse":
        return self

    def __next__(self) -> RecordedEvent:
        while True:
            try:
                response = next(self.resp)
            except RpcError as e:
                if e.code() == StatusCode.NOT_FOUND:
                    raise SubscriptionNotFound() from e
                else:
                    raise handle_rpc_error(e) from e
            assert isinstance(response, persistent_pb2.ReadResp)
            content_oneof = response.WhichOneof("content")
            if content_oneof == "event":
                event = response.event.event
                position_oneof = response.event.WhichOneof("position")
                if position_oneof == "commit_position":
                    commit_position = response.event.commit_position
                else:  # pragma: no cover
                    # We only get here with EventStoreDB < 22.10.
                    assert position_oneof == "no_position", position_oneof
                    commit_position = None

                return RecordedEvent(
                    id=UUID(event.id.string),
                    type=event.metadata.get("type", ""),
                    data=event.data,
                    metadata=event.custom_metadata,
                    content_type=event.metadata.get("content-type", ""),
                    stream_name=event.stream_identifier.stream_name.decode("utf8"),
                    stream_position=event.stream_revision,
                    commit_position=commit_position,
                )


@dataclass
class SubscriptionInfo:
    event_source: str
    group_name: str
    status: str
    average_per_second: int
    total_items: int
    count_since_last_measurement: int
    last_checkpointed_event_position: str
    last_known_event_position: str
    resolve_link_tos: bool
    start_from: str
    message_timeout_milliseconds: int
    extra_statistics: bool
    max_retry_count: int
    live_buffer_size: int
    buffer_size: int
    read_batch_size: int
    check_point_after_milliseconds: int
    min_check_point_count: int
    max_check_point_count: int
    read_buffer_count: int
    live_buffer_count: int
    retry_buffer_count: int
    total_in_flight_messages: int
    outstanding_messages_count: int
    named_consumer_strategy: str
    max_subscriber_count: int
    parked_message_count: int


class PersistentSubscriptionsService(ESDBService):
    """
    Encapsulates the 'persistent.PersistentSubscriptions' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = persistent_pb2_grpc.PersistentSubscriptionsStub(channel)

    @overload
    def create(
        self,
        group_name: str,
        *,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        # Todo: Expose alternative consumer strategies.
        consumer_strategy: str = "DispatchToSingle",
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent "all streams" subscription.
        """

    @overload
    def create(
        self,
        group_name: str,
        *,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        consumer_strategy: str = "DispatchToSingle",
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent stream subscription.
        """

    def create(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        # Todo: Expose alternative consumer strategies.
        consumer_strategy: str = "DispatchToSingle",
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        # Construct 'settings'.
        settings = persistent_pb2.CreateReq.Settings(
            resolve_links=False,
            extra_statistics=False,
            max_retry_count=5,
            min_checkpoint_count=10,  # server recorded position
            max_checkpoint_count=10,  # server recorded position
            max_subscriber_count=5,
            live_buffer_size=1000,  # how many new events to hold in memory?
            read_batch_size=8,  # how many events to read from DB records?
            history_buffer_size=200,  # how many recorded events to hold in memory?
            message_timeout_ms=1000,
            checkpoint_after_ms=100,
            consumer_strategy=consumer_strategy,
        )

        # Construct CreateReq.Options.
        options = persistent_pb2.CreateReq.Options(
            group_name=group_name,
            settings=settings,
        )

        # Decide 'stream_option'.
        if isinstance(stream_name, str):
            # Todo: Support persistent subscription to a stream.
            stream_options = persistent_pb2.CreateReq.StreamOptions(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
            )
            # Decide 'revision_option'.
            if isinstance(stream_position, int):
                stream_options.revision = stream_position
            elif from_end is False:
                stream_options.start.CopyFrom(shared_pb2.Empty())
            else:
                stream_options.end.CopyFrom(shared_pb2.Empty())
            options.stream.CopyFrom(stream_options)
        else:
            if commit_position is not None:
                all_options = persistent_pb2.CreateReq.AllOptions(
                    position=persistent_pb2.CreateReq.Position(
                        commit_position=commit_position,
                        prepare_position=commit_position,
                    ),
                )
            elif from_end:
                all_options = persistent_pb2.CreateReq.AllOptions(
                    end=shared_pb2.Empty(),
                )
            else:
                all_options = persistent_pb2.CreateReq.AllOptions(
                    start=shared_pb2.Empty(),
                )

            # Decide 'filter_option'.
            if filter_exclude or filter_include:
                if filter_include:
                    filter_regex = "^" + "|".join(filter_include) + "$"
                else:
                    filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

                filter = persistent_pb2.CreateReq.AllOptions.FilterOptions(
                    event_type=persistent_pb2.CreateReq.AllOptions.FilterOptions.Expression(
                        regex=filter_regex
                    ),
                    # Todo: What does 'window' mean?
                    # max=shared_pb2.Empty(),
                    count=shared_pb2.Empty(),
                    # Todo: What does 'checkpointIntervalMultiplier' mean?
                    checkpointIntervalMultiplier=5,
                )
                all_options.filter.CopyFrom(filter)
            else:
                no_filter = shared_pb2.Empty()
                all_options.no_filter.CopyFrom(no_filter)

            options.all.CopyFrom(all_options)

        # Construct RPC request.
        request = persistent_pb2.CreateReq(options=options)

        # Call 'Create' RPC.
        try:
            response = self._stub.Create(
                request,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(response, persistent_pb2.CreateResp)

    def read(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        read_req = SubscriptionReadRequest(
            group_name=group_name,
            stream_name=stream_name,
        )
        read_resp = self._stub.Read(
            read_req,
            timeout=timeout,
            metadata=self._metadata(metadata),
            credentials=credentials,
        )
        return (read_req, SubscriptionReadResponse(read_resp))

    def get_info(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> SubscriptionInfo:
        if stream_name is None:
            options = persistent_pb2.GetInfoReq.Options(
                all=shared_pb2.Empty(),
                group_name=group_name,
            )
        else:
            options = persistent_pb2.GetInfoReq.Options(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                group_name=group_name,
            )

        req = persistent_pb2.GetInfoReq(options=options)
        try:
            resp = self._stub.GetInfo(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.NOT_FOUND:
                raise SubscriptionNotFound() from e
            else:
                raise handle_rpc_error(e) from e

        else:
            assert isinstance(resp, persistent_pb2.GetInfoResp)
            s = resp.subscription_info
            return SubscriptionInfo(
                event_source=s.event_source,
                group_name=s.group_name,
                status=s.status,
                average_per_second=s.average_per_second,
                total_items=s.total_items,
                count_since_last_measurement=s.count_since_last_measurement,
                last_checkpointed_event_position=s.last_checkpointed_event_position,
                last_known_event_position=s.last_known_event_position,
                resolve_link_tos=s.resolve_link_tos,
                start_from=s.start_from,
                message_timeout_milliseconds=s.message_timeout_milliseconds,
                extra_statistics=s.extra_statistics,
                max_retry_count=s.max_retry_count,
                live_buffer_size=s.live_buffer_size,
                buffer_size=s.buffer_size,
                read_batch_size=s.read_batch_size,
                check_point_after_milliseconds=s.check_point_after_milliseconds,
                min_check_point_count=s.min_check_point_count,
                max_check_point_count=s.max_check_point_count,
                read_buffer_count=s.read_buffer_count,
                live_buffer_count=s.live_buffer_count,
                retry_buffer_count=s.retry_buffer_count,
                total_in_flight_messages=s.total_in_flight_messages,
                outstanding_messages_count=s.outstanding_messages_count,
                named_consumer_strategy=s.named_consumer_strategy,
                max_subscriber_count=s.max_subscriber_count,
                parked_message_count=s.parked_message_count,
            )

    def list(
        self,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> List[SubscriptionInfo]:
        if stream_name is None:
            options = persistent_pb2.ListReq.Options(
                list_all_subscriptions=shared_pb2.Empty()
            )
        else:
            options = persistent_pb2.ListReq.Options(
                list_for_stream=persistent_pb2.ListReq.StreamOption(
                    stream=shared_pb2.StreamIdentifier(
                        stream_name=stream_name.encode("utf8")
                    )
                )
            )

        req = persistent_pb2.ListReq(options=options)
        try:
            resp = self._stub.List(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.NOT_FOUND:
                return []
            else:
                raise handle_rpc_error(e) from e

        assert isinstance(resp, persistent_pb2.ListResp)
        return [
            SubscriptionInfo(
                event_source=s.event_source,
                group_name=s.group_name,
                status=s.status,
                average_per_second=s.average_per_second,
                total_items=s.total_items,
                count_since_last_measurement=s.count_since_last_measurement,
                last_checkpointed_event_position=s.last_checkpointed_event_position,
                last_known_event_position=s.last_known_event_position,
                resolve_link_tos=s.resolve_link_tos,
                start_from=s.start_from,
                message_timeout_milliseconds=s.message_timeout_milliseconds,
                extra_statistics=s.extra_statistics,
                max_retry_count=s.max_retry_count,
                live_buffer_size=s.live_buffer_size,
                buffer_size=s.buffer_size,
                read_batch_size=s.read_batch_size,
                check_point_after_milliseconds=s.check_point_after_milliseconds,
                min_check_point_count=s.min_check_point_count,
                max_check_point_count=s.max_check_point_count,
                read_buffer_count=s.read_buffer_count,
                live_buffer_count=s.live_buffer_count,
                retry_buffer_count=s.retry_buffer_count,
                total_in_flight_messages=s.total_in_flight_messages,
                outstanding_messages_count=s.outstanding_messages_count,
                named_consumer_strategy=s.named_consumer_strategy,
                max_subscriber_count=s.max_subscriber_count,
                parked_message_count=s.parked_message_count,
            )
            for s in resp.subscriptions
        ]

    def delete(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        if stream_name is None:
            options = persistent_pb2.DeleteReq.Options(
                all=shared_pb2.Empty(),
                group_name=group_name,
            )
        else:
            options = persistent_pb2.DeleteReq.Options(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                group_name=group_name,
            )

        req = persistent_pb2.DeleteReq(options=options)
        try:
            resp = self._stub.Delete(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.NOT_FOUND:
                raise SubscriptionNotFound() from e
            else:
                raise handle_rpc_error(e) from e
        assert isinstance(resp, persistent_pb2.DeleteResp)


@dataclass
class ClusterMember:
    state: str
    address: str
    port: int


NODE_STATE_LEADER = "NODE_STATE_LEADER"
NODE_STATE_FOLLOWER = "NODE_STATE_FOLLOWER"
NODE_STATE_REPLICA = "NODE_STATE_REPLICA"
NODE_STATE_OTHER = "NODE_STATE_OTHER"

GOSSIP_API_NODE_STATES_MAPPING = {
    gossip_pb2.MemberInfo.VNodeState.Follower: NODE_STATE_FOLLOWER,
    gossip_pb2.MemberInfo.VNodeState.Leader: NODE_STATE_LEADER,
    gossip_pb2.MemberInfo.VNodeState.ReadOnlyReplica: NODE_STATE_REPLICA,
}


class GossipService(ESDBService):
    """
    Encapsulates the 'gossip.Gossip' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = gossip_pb2_grpc.GossipStub(channel)

    def read(
        self,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Sequence[ClusterMember]:
        try:
            read_resp = self._stub.Read(
                shared_pb2.Empty(),
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except RpcError as e:
            raise handle_rpc_error(e) from e

        assert isinstance(read_resp, gossip_pb2.ClusterInfo)

        members = []
        for member_info in read_resp.members:
            member = ClusterMember(
                GOSSIP_API_NODE_STATES_MAPPING.get(member_info.state, NODE_STATE_OTHER),
                member_info.http_end_point.address,
                member_info.http_end_point.port,
            )
            members.append(member)
        return tuple(members)


CLUSTER_GOSSIP_NODE_STATES_MAPPING = {
    cluster_pb2.MemberInfo.VNodeState.Follower: NODE_STATE_FOLLOWER,
    cluster_pb2.MemberInfo.VNodeState.Leader: NODE_STATE_LEADER,
    cluster_pb2.MemberInfo.VNodeState.ReadOnlyReplica: NODE_STATE_REPLICA,
}


class ClusterGossipService(ESDBService):
    """
    Encapsulates the 'cluster.Gossip' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = cluster_pb2_grpc.GossipStub(channel)

    def read(
        self,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Sequence[ClusterMember]:
        """
        Returns a sequence of ClusterMember
        """

        try:
            read_resp = self._stub.Read(
                shared_pb2.Empty(),
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except RpcError as e:
            raise handle_rpc_error(e) from e

        assert isinstance(read_resp, cluster_pb2.ClusterInfo)

        members = []
        for member_info in read_resp.members:
            assert isinstance(member_info, cluster_pb2.MemberInfo)
            # Todo: Here we might want to use member_info.advertise_host_to_client_as
            #   and member_info.advertise_http_port_to_client_as, but I don't know
            #   what the difference is. Are these different from
            #   member_info.http_end_point.address and member_info.http_end_point.port?
            member = ClusterMember(
                state=CLUSTER_GOSSIP_NODE_STATES_MAPPING.get(
                    member_info.state, NODE_STATE_OTHER
                ),
                address=member_info.http_end_point.address,
                port=member_info.http_end_point.port,
            )
            members.append(member)
        return tuple(members)
