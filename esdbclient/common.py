# -*- coding: utf-8 -*-
from base64 import b64encode
from typing import TYPE_CHECKING, Optional, Sequence, Tuple, Union
from uuid import UUID

import grpc
import grpc.aio

from esdbclient.connection_spec import ConnectionSpec
from esdbclient.events import RecordedEvent
from esdbclient.exceptions import (
    AbortedByServer,
    CancelledByClient,
    ConsumerTooSlow,
    DeadlineExceeded,
    EventStoreDBClientException,
    ExceptionThrownByHandler,
    GrpcError,
    NodeIsNotLeader,
    NotFound,
    ServiceUnavailable,
)
from esdbclient.protos.Grpc import persistent_pb2, streams_pb2

if TYPE_CHECKING:  # pragma: no cover
    from grpc import Metadata

else:
    Metadata = Tuple[Tuple[str, str], ...]

__all__ = ["handle_rpc_error", "BasicAuthCallCredentials", "ESDBService", "Metadata"]


DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER = 5
DEFAULT_BATCH_APPEND_REQUEST_DEADLINE = 315576000000
DEFAULT_WINDOW_SIZE = 30


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


def handle_rpc_error(e: grpc.RpcError) -> EventStoreDBClientException:
    """
    Converts gRPC errors to client exceptions.
    """
    if isinstance(e, (grpc.Call, grpc.aio.AioRpcError)):
        if (
            e.code() == grpc.StatusCode.UNKNOWN
            and "Exception was thrown by handler" in str(e.details())
        ):
            return ExceptionThrownByHandler(e)
        elif e.code() == grpc.StatusCode.ABORTED:
            details = e.details()
            if isinstance(details, str) and "Consumer too slow" in details:
                return ConsumerTooSlow()
            else:
                return AbortedByServer()
        elif (
            e.code() == grpc.StatusCode.CANCELLED
            and e.details() == "Locally cancelled by application!"
        ):
            return CancelledByClient(e)
        elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return DeadlineExceeded(e)
        elif e.code() == grpc.StatusCode.UNAVAILABLE:
            return ServiceUnavailable(e)
        elif (
            e.code() == grpc.StatusCode.NOT_FOUND
            and e.details() == "Leader info available"
        ):
            return NodeIsNotLeader(e)
        elif e.code() == grpc.StatusCode.NOT_FOUND:
            return NotFound()
    return GrpcError(e)


class ESDBService:
    def __init__(self, connection_spec: ConnectionSpec):
        self.connection_spec = connection_spec

    def _metadata(
        self, metadata: Optional[Metadata], requires_leader: bool = False
    ) -> Metadata:
        default = (
            "true"
            if self.connection_spec.options.NodePreference == "leader"
            else "false"
        )
        requires_leader_metadata: Metadata = (
            ("requires-leader", "true" if requires_leader else default),
        )
        metadata = tuple() if metadata is None else metadata
        return metadata + requires_leader_metadata


def construct_filter_include_regex(patterns: Sequence[str]) -> str:
    patterns = [patterns] if isinstance(patterns, str) else patterns
    return "^" + "|".join(patterns) + "$"


def construct_filter_exclude_regex(patterns: Sequence[str]) -> str:
    patterns = [patterns] if isinstance(patterns, str) else patterns
    return "^(?!(" + "|".join([s + "$" for s in patterns]) + "))"


def construct_recorded_event(
    read_event: Union[
        streams_pb2.ReadResp.ReadEvent, persistent_pb2.ReadResp.ReadEvent
    ],
) -> Optional[RecordedEvent]:
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

    position_oneof = read_event.WhichOneof("position")
    if position_oneof == "commit_position":
        ignore_commit_position = False
        # # Is this equality always true? Ans: Not when resolve_links=True.
        # assert read_event.commit_position == event.commit_position
    else:  # pragma: no cover
        # We get here with EventStoreDB < 22.10 when reading a stream.
        assert position_oneof == "no_position", position_oneof
        ignore_commit_position = True

    if isinstance(read_event, persistent_pb2.ReadResp.ReadEvent):
        retry_count: Optional[int] = read_event.retry_count
    else:
        retry_count = None

    if link.id.string == "":
        recorded_event_link: Optional[RecordedEvent] = None
    else:
        recorded_event_link = RecordedEvent(
            id=UUID(link.id.string),
            type=link.metadata.get("type", ""),
            data=link.data,
            metadata=link.custom_metadata,
            content_type=link.metadata.get("content-type", ""),
            stream_name=link.stream_identifier.stream_name.decode("utf8"),
            stream_position=link.stream_revision,
            commit_position=None if ignore_commit_position else link.commit_position,
            retry_count=retry_count,
        )

    recorded_event = RecordedEvent(
        id=UUID(event.id.string),
        type=event.metadata.get("type", ""),
        data=event.data,
        metadata=event.custom_metadata,
        content_type=event.metadata.get("content-type", ""),
        stream_name=event.stream_identifier.stream_name.decode("utf8"),
        stream_position=event.stream_revision,
        commit_position=None if ignore_commit_position else event.commit_position,
        retry_count=retry_count,
        link=recorded_event_link,
    )
    return recorded_event
