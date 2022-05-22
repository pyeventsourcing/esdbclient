# -*- coding: utf-8 -*-
import sys
from dataclasses import dataclass
from typing import Iterable, List, Optional
from uuid import uuid4

import grpc
from grpc import Channel, ChannelConnectivity, RpcError, StatusCode
from grpc._channel import _MultiThreadedRendezvous

from esdbclient.protos.Grpc.shared_pb2 import UUID, Empty, StreamIdentifier
from esdbclient.protos.Grpc.streams_pb2 import AppendReq, AppendResp, ReadReq, ReadResp
from esdbclient.protos.Grpc.streams_pb2_grpc import StreamsStub


class EsdbClientException(Exception):
    pass


class GrpcError(EsdbClientException):
    pass


class ServiceUnavailable(GrpcError):
    pass


class StreamNotFound(EsdbClientException):
    pass


class AppendPositionError(EsdbClientException):
    pass


def handle_rpc_error(e: RpcError) -> None:
    if isinstance(e, _MultiThreadedRendezvous):
        if e.code() == StatusCode.UNAVAILABLE:
            raise ServiceUnavailable(e)
    raise GrpcError(e) from None


@dataclass(frozen=True)
class NewEvent:
    type: str
    data: bytes


@dataclass(frozen=True)
class RecordedEvent:
    type: str
    data: bytes
    commit_position: int


class EsdbClient:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.channel = grpc.insecure_channel(self.uri)
        self.channel.subscribe(self.handle_channel_state_change)

        self.streams = Streams(self.channel)

    def handle_channel_state_change(
        self, channel_connectivity: ChannelConnectivity
    ) -> None:
        print("Channel state change:", channel_connectivity)

    def read_stream(
        self,
        stream_name: str,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
    ) -> Iterable[RecordedEvent]:
        return self.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            limit=limit,
        )

    def append_events(
        self, stream_name: str, position: Optional[int], events: List[NewEvent]
    ) -> int:
        return self.streams.append(
            stream_name=stream_name, position=position, events=events
        )

    def read_all(
        self,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
    ) -> Iterable[RecordedEvent]:
        return self.streams.read(
            commit_position=commit_position,
            backwards=backwards,
            limit=limit,
        )


class Streams:
    def __init__(self, channel: Channel):
        self.stub = StreamsStub(channel)

    def read(
        self,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
    ) -> Iterable[RecordedEvent]:
        """
        Returns iterable of recorded events from the server.

        This is a unary request and stream response call.

        :param stream_name: Name of the stream.
        :param stream_position: Position in the stream to start reading.
        :param backwards: Direction in which to read.
        :param limit: Maximum number of events in response.
        :return: Iterable of committed events.
        """
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

        if backwards is False:
            read_direction = ReadReq.Options.Forwards
        else:
            read_direction = ReadReq.Options.Backwards

        request = ReadReq(
            options=ReadReq.Options(
                stream=stream_options,
                all=all_options,
                read_direction=read_direction,
                resolve_links=False,
                count=limit,
                # subscription=ReadReq.Options.SubscriptionOptions(),
                # filter=ReadReq.Options.FilterOptions(),
                no_filter=Empty(),
                uuid_option=ReadReq.Options.UUIDOption(
                    structured=Empty(), string=Empty()
                ),
            )
        )
        try:
            for response in self.stub.Read(request):
                assert isinstance(response, ReadResp)
                if response.WhichOneof("content") == "stream_not_found":
                    raise StreamNotFound(f"Stream '{stream_name}' not found")
                yield RecordedEvent(
                    type=response.event.event.metadata["type"],
                    data=response.event.event.data,
                    commit_position=response.event.commit_position,
                )
        except RpcError as e:
            handle_rpc_error(e)

    def append(
        self, stream_name: str, position: Optional[int], events: List[NewEvent]
    ) -> int:
        requests = [
            AppendReq(
                options=AppendReq.Options(
                    stream_identifier=StreamIdentifier(
                        stream_name=stream_name.encode("utf8")
                    ),
                    revision=position,
                    no_stream=Empty() if position is None else None,
                    # any=None,
                    # stream_exists=None,
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
                        custom_metadata=b"",
                        data=event.data,
                    )
                )
            )
        try:
            response = self.stub.Append(iter(requests))
        except RpcError as e:
            handle_rpc_error(e)

        assert isinstance(response, AppendResp)
        if response.WhichOneof("result") == "success":
            return response.success.position.commit_position

        if (
            response.wrong_expected_version.WhichOneof("current_revision_option")
            == "current_revision"
        ):
            current_position = response.wrong_expected_version.current_revision
            raise AppendPositionError(f"Current position is {current_position}")
        else:
            raise AppendPositionError(f"Stream '{stream_name}' does not exist")
