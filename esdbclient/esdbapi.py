# -*- coding: utf-8 -*-
import sys
from base64 import b64encode
from queue import Empty, Queue
from typing import Iterable, Optional, Sequence, Tuple, overload
from uuid import UUID

import grpc
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
        credentials: Optional[CallCredentials] = None,
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
        credentials: Optional[CallCredentials] = None,
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
        credentials: Optional[CallCredentials] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Constructs and sends a gRPC 'ReadReq' to the 'Read' rpc.

        Returns a generator which yields RecordedEvent objects.
        """

        # Construct ReadReq.Options.
        options = grpc_streams.ReadReq.Options()

        # Decide 'stream_option'.
        if stream_name is not None:
            assert isinstance(stream_name, str)
            assert commit_position is None
            stream_options = grpc_streams.ReadReq.Options.StreamOptions(
                stream_identifier=grpc_shared.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                revision=stream_position or 0,
            )
            # Decide 'revision_option'.
            if stream_position is not None:
                stream_options.revision = stream_position
            elif backwards is False:
                stream_options.start.CopyFrom(grpc_shared.Empty())
            else:
                stream_options.end.CopyFrom(grpc_shared.Empty())
            options.stream.CopyFrom(stream_options)
        else:
            assert stream_position is None
            if commit_position is not None:
                all_options = grpc_streams.ReadReq.Options.AllOptions(
                    position=grpc_streams.ReadReq.Options.Position(
                        commit_position=commit_position,
                        prepare_position=commit_position,
                    )
                )
            elif backwards:
                all_options = grpc_streams.ReadReq.Options.AllOptions(
                    end=grpc_shared.Empty()
                )
            else:
                all_options = grpc_streams.ReadReq.Options.AllOptions(
                    start=grpc_shared.Empty()
                )
            options.all.CopyFrom(all_options)

        # Decide 'read_direction'.
        if backwards is False:
            options.read_direction = grpc_streams.ReadReq.Options.Forwards
        else:
            options.read_direction = grpc_streams.ReadReq.Options.Backwards

        # Decide 'resolve_links'.
        options.resolve_links = False

        # Decide 'count_option'.
        if subscribe:
            subscription = grpc_streams.ReadReq.Options.SubscriptionOptions()
            options.subscription.CopyFrom(subscription)

        else:
            options.count = limit

        # Decide 'filter_option'.
        if filter_exclude or filter_include:
            if filter_include:
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter = grpc_streams.ReadReq.Options.FilterOptions(
                event_type=grpc_streams.ReadReq.Options.FilterOptions.Expression(
                    regex=filter_regex
                ),
                # Todo: What does 'window' mean?
                # max=grpc_shared.Empty(),
                count=grpc_shared.Empty(),
                # Todo: What does 'checkpointIntervalMultiplier' mean?
                checkpointIntervalMultiplier=5,
            )
            options.filter.CopyFrom(filter)
        else:
            no_filter = grpc_shared.Empty()
            options.no_filter.CopyFrom(no_filter)

        # Decide 'uuid_option'.
        options.uuid_option.CopyFrom(
            grpc_streams.ReadReq.Options.UUIDOption(string=grpc_shared.Empty())
        )

        # Construct read request.
        read_req = grpc_streams.ReadReq(options=options)

        # Send the read request, and iterate over the response.
        try:
            for response in self._stub.Read(
                read_req, timeout=timeout, credentials=credentials
            ):
                assert isinstance(response, grpc_streams.ReadResp)
                content_attribute_name = response.WhichOneof("content")
                if content_attribute_name == "event":
                    event = response.event.event
                    yield RecordedEvent(
                        id=UUID(event.id.string),
                        type=event.metadata["type"],
                        data=event.data,
                        content_type=event.metadata["content-type"],
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
                        id=grpc_shared.UUID(string=str(event.id)),
                        metadata={
                            "type": event.type,
                            "content-type": event.content_type,
                        },
                        custom_metadata=event.metadata,
                        data=event.data,
                    )
                )
            )

        # Call the gRPC method.
        try:
            response = self._stub.Append(iter(requests), timeout=timeout)
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
                try:
                    event_id = self.queue.get(timeout=0.1)
                    ids.append(event_id)
                    if len(ids) >= 100:
                        return grpc_persistent.ReadReq(
                            ack=grpc_persistent.ReadReq.Ack(ids=ids)
                        )
                except Empty:
                    if len(ids) >= 1:
                        return grpc_persistent.ReadReq(
                            ack=grpc_persistent.ReadReq.Ack(ids=ids)
                        )

    def ack(self, event_id: UUID) -> None:
        self.queue.put(grpc_shared.UUID(string=str(event_id)))

    # Todo: Implement nack().


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
                event = response.event.event
                stream_name = event.stream_identifier.stream_name.decode("utf8")
                if stream_name.startswith("$"):
                    continue
                return RecordedEvent(
                    id=UUID(event.id.string),
                    type=event.metadata.get("type", ""),
                    data=event.data,
                    metadata=event.custom_metadata,
                    content_type=event.metadata.get("content-type", ""),
                    stream_name=stream_name,
                    stream_position=event.stream_revision,
                    commit_position=response.event.commit_position,
                )


class Subscriptions:
    """
    Encapsulates the 'Subscriptions' gRPC service.
    """

    def __init__(self, channel: Channel):
        self._stub = PersistentSubscriptionsStub(channel)

    def create(
        self,
        group_name: str,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        consumer_strategy: str = "DispatchToSingle",
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        # Construct 'settings'.
        settings = grpc_persistent.CreateReq.Settings(
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
        options = grpc_persistent.CreateReq.Options(
            group_name=group_name,
            settings=settings,
        )

        # Decide 'stream_option'.
        if False:
            pass  # pragma: no cover
            # Todo: Support persistent subscription to a stream.
            # options.stream.CopyFrom(stream_options)
        else:
            if commit_position is not None:
                all_options = grpc_persistent.CreateReq.AllOptions(
                    position=grpc_persistent.CreateReq.Position(
                        commit_position=commit_position,
                        prepare_position=commit_position,
                    ),
                )
            elif from_end:
                all_options = grpc_persistent.CreateReq.AllOptions(
                    end=grpc_shared.Empty(),
                )
            else:
                all_options = grpc_persistent.CreateReq.AllOptions(
                    start=grpc_shared.Empty(),
                )

            # Decide 'filter_option'.
            if filter_exclude or filter_include:
                if filter_include:
                    filter_regex = "^" + "|".join(filter_include) + "$"
                else:
                    filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

                filter = grpc_persistent.CreateReq.AllOptions.FilterOptions(
                    event_type=grpc_persistent.CreateReq.AllOptions.FilterOptions.Expression(
                        regex=filter_regex
                    ),
                    # Todo: What does 'window' mean?
                    # max=grpc_shared.Empty(),
                    count=grpc_shared.Empty(),
                    # Todo: What does 'checkpointIntervalMultiplier' mean?
                    checkpointIntervalMultiplier=5,
                )
                all_options.filter.CopyFrom(filter)
            else:
                no_filter = grpc_shared.Empty()
                all_options.no_filter.CopyFrom(no_filter)

            options.all.CopyFrom(all_options)

        # Construct RPC request.
        request = grpc_persistent.CreateReq(options=options)

        # Call 'Create' RPC.
        try:
            response = self._stub.Create(
                request,
                timeout=timeout,
                credentials=credentials,
            )
        except RpcError as e:  # pragma: no cover
            raise handle_rpc_error(e) from e
        assert isinstance(response, grpc_persistent.CreateResp)

    def read(
        self,
        group_name: str,
        timeout: Optional[float] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        read_req = SubscriptionReadRequest(group_name=group_name)
        try:
            read_resp = self._stub.Read(
                read_req, timeout=timeout, credentials=credentials
            )
        except RpcError as e:  # pragma: no cover
            raise handle_rpc_error(e) from e
        return (read_req, SubscriptionReadResponse(read_resp))


class BasicAuthCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        credentials = b64encode(f"{username}:{password}".encode())
        self._metadata = (("authorization", (b"Basic " + credentials)),)

    def __call__(
        self, context: AuthMetadataContext, callback: AuthMetadataPluginCallback
    ) -> None:
        callback(self._metadata, None)
