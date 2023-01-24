# -*- coding: utf-8 -*-
import sys
from queue import Queue
from typing import Iterable, Optional, Sequence, Tuple, overload
from uuid import UUID, uuid4

from grpc import Call, Channel, RpcError, StatusCode
from typing_extensions import Literal

from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    DeadlineExceeded,
    ExpectedPositionError,
    GrpcError,
    ServiceUnavailable,
    StreamNotFound,
)
from esdbclient.protos.Grpc import (
    persistent_pb2 as grpc_persistent,
    shared_pb2 as grpc_shared,
    streams_pb2 as grpc_streams,
)
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


class Streams:
    """
    Encapsulates the 'Streams' gRPC service.
    """

    def __init__(self, channel: Channel):
        self.stub = StreamsStub(channel)

    @overload
    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading events from an individual stream.
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
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading events from "all streams".
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
    ) -> Iterable[RecordedEvent]:
        """
        Signature for reading events with a catch-up subscription.
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
    ) -> Iterable[RecordedEvent]:
        """
        Constructs and sends a gRPC 'ReadReq' to the 'Read' rpc.

        Returns a generator which yields RecordedEvent objects.
        """
        # Decide read request options.
        if stream_name is not None:
            assert isinstance(stream_name, str)
            assert commit_position is None
            stream_options = grpc_streams.ReadReq.Options.StreamOptions(
                stream_identifier=grpc_shared.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                revision=stream_position or 0,
                start=grpc_shared.Empty()
                if stream_position is None and not backwards
                else None,
                end=grpc_shared.Empty()
                if stream_position is None and backwards
                else None,
            )
            all_options = None
        else:
            assert stream_position is None
            if isinstance(commit_position, int):
                position = grpc_streams.ReadReq.Options.Position(
                    commit_position=commit_position,
                    prepare_position=commit_position,
                )
            else:
                position = None
            stream_options = None
            all_options = grpc_streams.ReadReq.Options.AllOptions(
                position=position,
                start=grpc_shared.Empty()
                if position is None and not backwards
                else None,
                end=grpc_shared.Empty() if position is None and backwards else None,
            )

        # Decide read direction.
        if backwards is False:
            read_direction = grpc_streams.ReadReq.Options.Forwards
        else:
            read_direction = grpc_streams.ReadReq.Options.Backwards

        # Decide filter options.
        if all_options is not None and (filter_exclude or filter_include):
            if filter_include:
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter_options = grpc_streams.ReadReq.Options.FilterOptions(
                stream_identifier=None,
                event_type=grpc_streams.ReadReq.Options.FilterOptions.Expression(
                    regex=filter_regex
                ),
                count=grpc_shared.Empty(),  # Todo: Figure out what 'window' should be.
            )
        else:
            filter_options = None
        if subscribe:
            subscription_options = grpc_streams.ReadReq.Options.SubscriptionOptions()
        else:
            subscription_options = None

        # Construct a read request.
        request = grpc_streams.ReadReq(
            options=grpc_streams.ReadReq.Options(
                stream=stream_options,
                all=all_options,
                read_direction=read_direction,
                resolve_links=False,
                count=limit,
                subscription=subscription_options,
                filter=filter_options,
                no_filter=grpc_shared.Empty() if filter_options is None else None,
                uuid_option=grpc_streams.ReadReq.Options.UUIDOption(
                    structured=grpc_shared.Empty(), string=grpc_shared.Empty()
                ),
            )
        )

        # Send the read request, and iterate over the response.
        try:
            for response in self.stub.Read(request, timeout=timeout):
                assert isinstance(response, grpc_streams.ReadResp)
                content_attribute_name = response.WhichOneof("content")
                if content_attribute_name == "event":
                    event = response.event.event
                    yield RecordedEvent(
                        id=UUID(event.id.string),
                        type=event.metadata["type"],
                        data=event.data,
                        metadata=event.custom_metadata,
                        stream_name=event.stream_identifier.stream_name.decode("utf8"),
                        stream_position=event.stream_revision,
                        commit_position=response.event.commit_position,
                    )
                elif content_attribute_name == "stream_not_found":
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
        Constructs and sends a gRPC 'AppendReq' to the 'Append' rpc.

        Returns an integer representing the current commit position.
        """
        # Consider the expected_position argument.
        if expected_position is None:
            # Stream is expected not to exist.
            no_stream = grpc_shared.Empty()
            any = None
        else:
            assert isinstance(expected_position, int)
            no_stream = None
            if expected_position >= 0:
                # Stream is expected to exist.
                any = None
            else:
                # Disable optimistic concurrency control.
                expected_position = None
                any = grpc_shared.Empty()

        # Build the list of append requests.
        requests = [
            grpc_streams.AppendReq(
                options=grpc_streams.AppendReq.Options(
                    stream_identifier=grpc_shared.StreamIdentifier(
                        stream_name=stream_name.encode("utf8")
                    ),
                    revision=expected_position or 0,
                    no_stream=no_stream,
                    any=any,
                ),
            )
        ]
        for event in events:
            requests.append(
                grpc_streams.AppendReq(
                    proposed_message=grpc_streams.AppendReq.ProposedMessage(
                        id=grpc_shared.UUID(string=str(uuid4())),
                        metadata={
                            "type": event.type,
                            "content-type": "application/octet-stream",
                        },
                        custom_metadata=event.metadata,
                        data=event.data,
                    )
                )
            )

        # Call the gRPC method.
        try:
            response = self.stub.Append(iter(requests), timeout=timeout)
        except RpcError as e:
            raise handle_rpc_error(e) from e
        else:
            assert isinstance(response, grpc_streams.AppendResp)
            # Check for success.
            if response.WhichOneof("result") == "success":
                return response.success.position.commit_position

            # Figure out what went wrong.
            if (
                response.wrong_expected_version.WhichOneof("current_revision_option")
                == "current_revision"
            ):
                current_position = response.wrong_expected_version.current_revision
                raise ExpectedPositionError(f"Current position is {current_position}")
            else:
                raise ExpectedPositionError(f"Stream {stream_name!r} does not exist")


class SubscriptionReadRequest:
    def __init__(self, group_name: str) -> None:
        self.group_name = group_name
        self.queue: Queue[grpc_shared.UUID] = Queue()
        self._has_requested_options = False

    def __next__(self) -> grpc_persistent.ReadReq:
        if not self._has_requested_options:
            self._has_requested_options = True
            return grpc_persistent.ReadReq(
                options=grpc_persistent.ReadReq.Options(
                    all=grpc_shared.Empty(),
                    group_name=self.group_name,
                    buffer_size=100,
                    uuid_option=grpc_persistent.ReadReq.Options.UUIDOption(
                        structured=grpc_shared.Empty(),
                        string=grpc_shared.Empty(),
                    ),
                )
            )
        else:
            ids = []
            while True:
                event_id = self.queue.get()
                ids.append(event_id)
                if len(ids) >= 100:
                    return grpc_persistent.ReadReq(
                        ack=grpc_persistent.ReadReq.Ack(ids=ids)
                    )

    def ack(self, event_id: UUID) -> None:
        self.queue.put(grpc_shared.UUID(string=str(event_id)))


class SubscriptionReadResponse:
    def __init__(self, resp: Iterable[grpc_persistent.ReadResp]):
        self.resp = iter(resp)

    def __iter__(self) -> "SubscriptionReadResponse":
        return self

    def __next__(self) -> RecordedEvent:
        while True:
            response = next(self.resp)
            assert isinstance(response, grpc_persistent.ReadResp)
            if response.WhichOneof("content") == "event":
                return RecordedEvent(
                    id=UUID(response.event.event.id.string),
                    type=response.event.event.metadata["type"],
                    data=response.event.event.data,
                    metadata=response.event.event.custom_metadata,
                    stream_name=response.event.event.stream_identifier.stream_name.decode(
                        "utf8"
                    ),
                    stream_position=response.event.event.stream_revision,
                    commit_position=response.event.commit_position,
                )


class Subscriptions:
    """
    Encapsulates the 'Subscriptions' gRPC service.
    """

    def __init__(self, channel: Channel):
        self.stub = PersistentSubscriptionsStub(channel)

    def create(
        self,
        group_name: str,
        position: int,
        consumer_strategy: str = "DispatchToSingle",
    ) -> None:
        settings = grpc_persistent.CreateReq.Settings(
            resolve_links=False,
            extra_statistics=False,
            max_retry_count=5,
            min_checkpoint_count=10,  # server recorded position
            max_checkpoint_count=10,  # server recorded position
            max_subscriber_count=5,
            live_buffer_size=1000,  # how many new events to hold in memory?
            read_batch_size=20,  # how many events to read from DB records?
            history_buffer_size=200,  # how many recorded events to hold in memory?
            message_timeout_ms=1000,
            checkpoint_after_ms=100,
            consumer_strategy=consumer_strategy,
        )
        options = grpc_persistent.CreateReq.Options(
            all=grpc_persistent.CreateReq.AllOptions(
                position=grpc_persistent.CreateReq.Position(
                    commit_position=position,
                    prepare_position=position,
                ),
                start=grpc_shared.Empty(),
            ),
            group_name=group_name,
            settings=settings,
        )
        create_req = grpc_persistent.CreateReq(
            options=options,
        )
        self.stub.Create(create_req)

    def read(
        self, group_name: str
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        read_req = SubscriptionReadRequest(group_name=group_name)
        read_resp = self.stub.Read(read_req)
        return (read_req, SubscriptionReadResponse(read_resp))
