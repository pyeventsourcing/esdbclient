# -*- coding: utf-8 -*-
import sys
from queue import Queue
from typing import Iterable, Optional, Sequence, Tuple, overload
from uuid import uuid4

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
from esdbclient.protos.Grpc.persistent_pb2 import (
    CreateReq,
    ReadReq as SubscriptionReadReq,
    ReadResp as SubscriptionReadResp,
)
from esdbclient.protos.Grpc.persistent_pb2_grpc import PersistentSubscriptionsStub
from esdbclient.protos.Grpc.shared_pb2 import UUID, Empty, StreamIdentifier
from esdbclient.protos.Grpc.streams_pb2 import (
    AppendReq,
    AppendResp,
    ReadReq as StreamsReadReq,
    ReadResp as StreamsReadResp,
)
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
            stream_options = StreamsReadReq.Options.StreamOptions(
                stream_identifier=StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                revision=stream_position or 0,
                start=Empty() if stream_position is None and not backwards else None,
                end=Empty() if stream_position is None and backwards else None,
            )
            all_options = None
        else:
            assert stream_position is None
            if isinstance(commit_position, int):
                position = StreamsReadReq.Options.Position(
                    commit_position=commit_position,
                    prepare_position=commit_position,
                )
            else:
                position = None
            stream_options = None
            all_options = StreamsReadReq.Options.AllOptions(
                position=position,
                start=Empty() if position is None and not backwards else None,
                end=Empty() if position is None and backwards else None,
            )

        # Decide read direction.
        if backwards is False:
            read_direction = StreamsReadReq.Options.Forwards
        else:
            read_direction = StreamsReadReq.Options.Backwards

        # Decide filter options.
        if all_options is not None and (filter_exclude or filter_include):
            if filter_include:
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter_options = StreamsReadReq.Options.FilterOptions(
                stream_identifier=None,
                event_type=StreamsReadReq.Options.FilterOptions.Expression(
                    regex=filter_regex
                ),
                count=Empty(),  # Todo: Figure out what 'window' should be.
            )
        else:
            filter_options = None
        if subscribe:
            subscription_options = StreamsReadReq.Options.SubscriptionOptions()
        else:
            subscription_options = None

        # Construct a read request.
        request = StreamsReadReq(
            options=StreamsReadReq.Options(
                stream=stream_options,
                all=all_options,
                read_direction=read_direction,
                resolve_links=False,
                count=limit,
                subscription=subscription_options,
                filter=filter_options,
                no_filter=Empty() if filter_options is None else None,
                uuid_option=StreamsReadReq.Options.UUIDOption(
                    structured=Empty(), string=Empty()
                ),
            )
        )

        # Send the read request, and iterate over the response.
        try:
            for response in self.stub.Read(request, timeout=timeout):
                assert isinstance(response, StreamsReadResp)
                if response.WhichOneof("content") == "stream_not_found":
                    raise StreamNotFound(f"Stream '{stream_name}' not found")
                yield RecordedEvent(
                    id=response.event.event.id,
                    type=response.event.event.metadata["type"],
                    data=response.event.event.data,
                    metadata=response.event.event.custom_metadata,
                    stream_name=response.event.event.stream_identifier.stream_name.decode(
                        "utf8"
                    ),
                    stream_position=response.event.event.stream_revision,
                    commit_position=response.event.commit_position,
                )
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
            no_stream = Empty()
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
                any = Empty()

        # Build the list of append requests.
        requests = [
            AppendReq(
                options=AppendReq.Options(
                    stream_identifier=StreamIdentifier(
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
                AppendReq(
                    proposed_message=AppendReq.ProposedMessage(
                        id=UUID(string=str(uuid4())),
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
            assert isinstance(response, AppendResp)
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
                raise ExpectedPositionError(f"Stream '{stream_name}' does not exist")


class SubscriptionReadRequest:
    def __init__(self, group_name):
        self.group_name = group_name
        self.queue = Queue()

    def __iter__(self):
        yield SubscriptionReadReq(
            options=SubscriptionReadReq.Options(
                all=Empty(),
                group_name=self.group_name,
                buffer_size=100,
                uuid_option=SubscriptionReadReq.Options.UUIDOption(
                    structured=Empty(),
                    string=Empty(),
                ),
            )
        )
        ids = []
        while True:
            event_id = self.queue.get()
            ids.append(event_id)
            if len(ids) >= 100:
                yield SubscriptionReadReq(
                    ack=SubscriptionReadReq.Ack(
                        ids=ids
                    )
                )
                ids = []

    def ack(self, event_id: UUID):
        self.queue.put(event_id)


class SubscriptionReadResponse:
    def __init__(self, resp):
        self.resp = resp

    def __iter__(self):
        for response in self.resp:
            assert isinstance(response, SubscriptionReadResp)
            if response.WhichOneof("content") == "event":
                yield RecordedEvent(
                    id=response.event.event.id,
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
        self, group_name: str, position: int, consumer_strategy="DispatchToSingle"
    ):
        settings = CreateReq.Settings(
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
        options = CreateReq.Options(
            all=CreateReq.AllOptions(
                position=CreateReq.Position(
                    commit_position=position,
                    prepare_position=position,
                ),
                start=Empty(),
            ),
            group_name=group_name,
            settings=settings,
        )
        create_req = CreateReq(
            options=options,
        )
        self.stub.Create(create_req)

    def read(
        self, group_name: str
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        read_req = SubscriptionReadRequest(group_name=group_name)
        read_resp = self.stub.Read(iter(read_req))
        return (read_req, SubscriptionReadResponse(read_resp))
