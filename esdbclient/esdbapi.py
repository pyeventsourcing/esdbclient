# -*- coding: utf-8 -*-
import queue
import sys
from base64 import b64encode
from dataclasses import dataclass, field
from typing import Dict, Iterable, Iterator, Optional, Sequence, Tuple, overload
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

from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    DeadlineExceeded,
    ExpectedPositionError,
    GrpcError,
    ServiceUnavailable,
    StreamNotFound,
)
from esdbclient.protos.Grpc import persistent_pb2, shared_pb2, status_pb2, streams_pb2
from esdbclient.protos.Grpc.persistent_pb2_grpc import PersistentSubscriptionsStub
from esdbclient.protos.Grpc.streams_pb2_grpc import StreamsStub


def handle_rpc_error(e: RpcError) -> GrpcError:
    """
    Converts gRPC errors to client exceptions.
    """
    if isinstance(e, Call):
        if e.code() == StatusCode.UNAVAILABLE:
            return ServiceUnavailable(e)
        elif e.code() == StatusCode.DEADLINE_EXCEEDED:
            return DeadlineExceeded(e)
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
    commit_position: Optional[int]
    error: Optional[Exception]


class Streams:
    """
    Encapsulates the 'Streams' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = StreamsStub(channel)

    @overload
    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
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
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Constructs and sends a gRPC 'ReadReq' to the 'Read' rpc.

        Returns a generator which yields RecordedEvent objects.
        """

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

        # Construct read request.
        read_req = streams_pb2.ReadReq(options=options)

        # Send the read request, and iterate over the response.
        try:
            for response in self._stub.Read(
                read_req, timeout=timeout, credentials=credentials
            ):
                assert isinstance(response, streams_pb2.ReadResp)
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

                    yield RecordedEvent(
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
                    raise StreamNotFound(f"Stream {stream_name!r} not found")
        except RpcError as e:
            raise handle_rpc_error(e) from e

    def append(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        """
        Constructs and sends a stream of gRPC 'AppendReq' to the 'Append' rpc.

        Returns the commit position of the last appended event.
        """
        # Generate append requests.
        requests = self._generate_append_requests(
            stream_name=stream_name, expected_position=expected_position, events=events
        )

        # Call the gRPC method.
        try:
            response = self._stub.Append(requests, timeout=timeout)
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

    def batch_append(
        self,
        batches: Iterable[BatchAppendRequest],
        timeout: Optional[float] = None,
    ) -> Iterable[BatchAppendResponse]:
        # Generate batch append requests.
        requests = BatchAppendRequestIterator(batches)

        # Call the gRPC method.
        try:
            responses = self._stub.BatchAppend(requests, timeout=timeout)
            for response in responses:
                assert isinstance(response, streams_pb2.BatchAppendResp)
                correlation_id = UUID(response.correlation_id.string)
                stream_name = requests.pop_stream_name(correlation_id)
                # Response 'result' is either 'success' or 'wrong_expected_version'.
                result_oneof = response.WhichOneof("result")
                if result_oneof == "success":
                    yield BatchAppendResponse(
                        commit_position=response.success.position.commit_position,
                        error=None,
                    )
                else:
                    assert result_oneof == "error"
                    assert isinstance(response.error, status_pb2.Status)
                    error = response.error

                    # Todo: Maybe somehow distinguish between
                    yield BatchAppendResponse(
                        commit_position=None,
                        error=ExpectedPositionError(
                            f"Stream name: {stream_name}",
                            error,
                        ),
                    )
            else:
                pass  # pragma: no cover

        except RpcError as e:
            raise handle_rpc_error(e) from e


class BatchAppendRequestIterator(Iterator[streams_pb2.BatchAppendReq]):
    def __init__(self, batches: Iterable[BatchAppendRequest]):
        self.batches = iter(batches)
        self.stream_names_by_correlation_id: Dict[UUID, str] = {}

    def __next__(self) -> streams_pb2.BatchAppendReq:
        batch = next(self.batches)
        self.stream_names_by_correlation_id[batch.correlation_id] = batch.stream_name
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
        proposed_messages = []
        for event in batch.events:
            proposed_message = streams_pb2.BatchAppendReq.ProposedMessage(
                id=shared_pb2.UUID(string=str(event.id)),
                metadata={"type": event.type, "content-type": event.content_type},
                custom_metadata=event.metadata,
                data=event.data,
            )
            proposed_messages.append(proposed_message)
        return streams_pb2.BatchAppendReq(
            correlation_id=shared_pb2.UUID(string=str(batch.correlation_id)),
            options=options,
            proposed_messages=proposed_messages,
            is_final=True,
        )

    def pop_stream_name(self, correlation_id: UUID) -> str:
        return self.stream_names_by_correlation_id.pop(correlation_id)


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


class Subscriptions:
    """
    Encapsulates the 'Subscriptions' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = PersistentSubscriptionsStub(channel)

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
        credentials: Optional[CallCredentials] = None,
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        read_req = SubscriptionReadRequest(
            group_name=group_name,
            stream_name=stream_name,
        )
        read_resp = self._stub.Read(read_req, timeout=timeout, credentials=credentials)
        return (read_req, SubscriptionReadResponse(read_resp))


class BasicAuthCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        credentials = b64encode(f"{username}:{password}".encode())
        self._metadata = (("authorization", (b"Basic " + credentials)),)

    def __call__(
        self, context: AuthMetadataContext, callback: AuthMetadataPluginCallback
    ) -> None:
        callback(self._metadata, None)
