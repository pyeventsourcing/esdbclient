# -*- coding: utf-8 -*-
import sys
from typing import Iterable, Optional, Sequence, overload
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
from esdbclient.protos.Grpc.shared_pb2 import UUID, Empty, StreamIdentifier
from esdbclient.protos.Grpc.streams_pb2 import AppendReq, AppendResp, ReadReq, ReadResp
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
            stream_options = ReadReq.Options.StreamOptions(
                stream_identifier=StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                revision=stream_position,
                start=Empty() if stream_position is None and not backwards else None,
                end=Empty() if stream_position is None and backwards else None,
            )
            all_options = None
        else:
            assert stream_position is None
            if isinstance(commit_position, int):
                position = ReadReq.Options.Position(
                    commit_position=commit_position,
                    prepare_position=commit_position,
                )
            else:
                position = None
            stream_options = None
            all_options = ReadReq.Options.AllOptions(
                position=position,
                start=Empty() if position is None and not backwards else None,
                end=Empty() if position is None and backwards else None,
            )

        # Decide read direction.
        if backwards is False:
            read_direction = ReadReq.Options.Forwards
        else:
            read_direction = ReadReq.Options.Backwards

        # Decide filter options.
        if all_options is not None and (filter_exclude or filter_include):
            if filter_include:
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter_options = ReadReq.Options.FilterOptions(
                stream_identifier=None,
                event_type=ReadReq.Options.FilterOptions.Expression(regex=filter_regex),
                count=Empty(),  # Todo: Figure out what 'window' should be.
            )
        else:
            filter_options = None
        if subscribe:
            subscription_options = ReadReq.Options.SubscriptionOptions()
        else:
            subscription_options = None

        # Construct a read request.
        request = ReadReq(
            options=ReadReq.Options(
                stream=stream_options,
                all=all_options,
                read_direction=read_direction,
                resolve_links=False,
                count=limit,
                subscription=subscription_options,
                filter=filter_options,
                no_filter=Empty() if filter_options is None else None,
                uuid_option=ReadReq.Options.UUIDOption(
                    structured=Empty(), string=Empty()
                ),
            )
        )

        # Send the read request, and iterate over the response.
        try:
            for response in self.stub.Read(request, timeout=timeout):
                assert isinstance(response, ReadResp)
                if response.WhichOneof("content") == "stream_not_found":
                    raise StreamNotFound(f"Stream '{stream_name}' not found")
                yield RecordedEvent(
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
                    revision=expected_position,
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
