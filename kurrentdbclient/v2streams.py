from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING

import grpc
import grpc.aio
from google.protobuf import struct_pb2

from kurrentdbclient.common import (
    AsyncGrpcStreamers,
    GrpcStreamers,
    KurrentDBService,
    Metadata,
    TGrpcStreamers,
    handle_rpc_error,
)
from kurrentdbclient.events import (
    ConsistencyCheck,
    ContentType,
    NewEvent,
    NewEvents,
    NewRecord,
    StreamState,
    StreamStateCheck,
)
from kurrentdbclient.exceptions import (
    CancelledByClientError,
    ProgrammingError,
)
from kurrentdbclient.protos.v2.streams import (
    streams_pb2,
    streams_pb2_grpc,
)

if TYPE_CHECKING:

    from kurrentdbclient.connection_spec import ConnectionSpec


# no cover <25.1: start


class BaseStreamsService(KurrentDBService[TGrpcStreamers]):
    def __init__(
        self,
        grpc_channel: grpc.Channel | grpc.aio.Channel,
        connection_spec: ConnectionSpec,
        grpc_streamers: TGrpcStreamers,
    ):
        super().__init__(connection_spec=connection_spec, grpc_streamers=grpc_streamers)
        self._stub = streams_pb2_grpc.StreamsServiceStub(grpc_channel)  # type: ignore[no-untyped-call]


class AppendRequestIterator(Iterator[streams_pb2.AppendRequest]):
    def __init__(self, new_events: Iterable[NewEvents]):
        self.new_events = iter(new_events)
        self.errored: Exception | None = None

    def __next__(self) -> streams_pb2.AppendRequest:
        try:
            new_events = next(self.new_events)
            return streams_pb2.AppendRequest(
                stream=new_events.stream_name,
                records=[
                    streams_pb2.AppendRecord(
                        record_id=str(event.id),
                        properties=_metadata_to_properties(event),
                        schema=streams_pb2.SchemaInfo(
                            format=_content_type_to_schema_format(event.content_type),
                            name=event.type,
                        ),
                        data=event.data,
                    )
                    for event in new_events.events
                ],
                expected_revision=_current_version_to_expected_revision(
                    new_events.current_version
                ),
            )
        except Exception as e:
            self.errored = e
            raise


def _current_version_to_expected_revision(current_version: int | StreamState) -> int:
    if isinstance(current_version, StreamState):
        if current_version == StreamState.NO_STREAM:
            return -1
        if current_version == StreamState.ANY:
            return -2
        assert current_version == StreamState.EXISTS
        return -4
    if isinstance(current_version, int) and current_version >= 0:
        return current_version
    msg = f"Unsupported current_version value: {current_version}"
    raise ProgrammingError(msg)


def _content_type_to_schema_format(
    content_type: ContentType,
) -> streams_pb2.SchemaFormat.ValueType:
    return (
        streams_pb2.SCHEMA_FORMAT_JSON
        if content_type == "application/json"
        else streams_pb2.SCHEMA_FORMAT_BYTES
    )


def _metadata_to_properties(event: NewEvent | NewRecord) -> dict[str, struct_pb2.Value]:
    properties = {}
    if event.metadata:
        try:
            json_obj = json.loads(event.metadata.decode())
            if not isinstance(json_obj, dict):
                msg = f"Not a JSON object: {json_obj}"
                raise ValueError(msg)
            for key, value in json_obj.items():
                if not isinstance(value, str):
                    msg = f"Not a JSON string: {value}"
                    raise ValueError(msg)
                properties[key] = struct_pb2.Value(string_value=value)
        except ValueError as e:
            msg = (
                f"Invalid metadata: {e}. NewEvent metadata in"
                f" multi-append call must either be empty or"
                f" a JSON document with string values: {event}"
            )
            raise ProgrammingError(msg) from e
    return properties


# class V2AsyncStreamsService(BaseStreamsService[AsyncGrpcStreamers]):
#     async def multi_append(
#         self,
#         events: Iterable[NewEvents],
#         timeout: float | None = None,
#         metadata: Metadata | None = None,
#         credentials: grpc.CallCredentials | None = None,
#     ) -> int:
#         try:
#             req = self._construct_append_requests(events)
#             append_session_response = await self._stub.AppendSession(
#                 req,
#                 timeout=timeout,
#                 metadata=self._metadata(metadata, requires_leader=True),
#                 credentials=credentials,
#             )
#             assert isinstance(
#                 append_session_response, streams_pb2.AppendSessionResponse
#             )
#             return append_session_response.position
#
#         except grpc.RpcError as e:
#             raise handle_rpc_error(e) from None


@dataclass(frozen=True)
class AppendRecordsResult:
    commit_position: int
    stream_positions: tuple[StreamPositionResult, ...]


@dataclass(frozen=True)
class StreamPositionResult:
    stream_name: str
    stream_position: int


class V2StreamsService(BaseStreamsService[GrpcStreamers]):
    """
    Encapsulates the 'streams.Streams' gRPC service.
    """

    def multi_append(
        self,
        events: Iterable[NewEvents],
        timeout: float | None = None,
        metadata: Metadata | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:
        append_requests = AppendRequestIterator(events)
        try:
            append_session_response = self._stub.AppendSession(
                append_requests,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
            assert isinstance(
                append_session_response, streams_pb2.AppendSessionResponse
            )
            return append_session_response.position

        except grpc.RpcError as e:
            # Check if there was an error iterating the request.
            details = e.details()
            if (
                details is not None
                and "Exception iterating requests!" in details
                and append_requests.errored
            ):
                raise append_requests.errored from None

            # Otherwise handle a genuine RPC error.
            raise handle_rpc_error(e) from e

    def append_records(  # pragma: <26.1 no cover
        self,
        records: Iterable[NewRecord],
        checks: Iterable[ConsistencyCheck] | None,
        timeout: float | None = None,
        metadata: Metadata | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AppendRecordsResult:
        append_requests = _construct_append_records_request(records, checks)
        try:
            append_records_response = self._stub.AppendRecords(
                append_requests,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
            assert isinstance(
                append_records_response, streams_pb2.AppendRecordsResponse
            )
            return _convert_append_records_response(append_records_response)
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e


def _convert_append_records_response(
    append_records_response: streams_pb2.AppendRecordsResponse,
) -> AppendRecordsResult:  # pragma: <26.1 no cover
    return AppendRecordsResult(
        commit_position=append_records_response.position,
        stream_positions=tuple(
            StreamPositionResult(
                stream_name=s.stream,
                stream_position=s.revision,
            )
            for s in append_records_response.revisions
        ),
    )


def _construct_append_records_request(
    records: Iterable[NewRecord],
    checks: Iterable[ConsistencyCheck] | None,
) -> streams_pb2.AppendRecordsRequest:  # pragma: <26.1 no cover
    return streams_pb2.AppendRecordsRequest(
        records=[
            streams_pb2.AppendRecord(
                record_id=str(r.id),
                properties=_metadata_to_properties(r),
                schema=streams_pb2.SchemaInfo(
                    format=_content_type_to_schema_format(r.content_type),
                    name=r.type,
                ),
                data=r.data,
                stream=r.stream_name,
            )
            for r in records
        ],
        checks=[
            streams_pb2.ConsistencyCheck(
                stream_state=streams_pb2.ConsistencyCheck.StreamStateCheck(
                    stream=c.stream_name,
                    expected_state=_current_version_to_expected_revision(
                        c.expected_state
                    ),
                )
            )
            for c in checks or []
        ],
    )


class AsyncV2StreamsService(BaseStreamsService[AsyncGrpcStreamers]):
    async def multi_append(
        self,
        events: Iterable[NewEvents],
        timeout: float | None = None,
        metadata: Metadata | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:
        append_requests = AppendRequestIterator(events)
        try:
            append_session_response = await self._stub.AppendSession(
                append_requests,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
            assert isinstance(
                append_session_response, streams_pb2.AppendSessionResponse
            )
            return append_session_response.position

        except grpc.RpcError as e:

            # Otherwise handle a genuine RPC error.
            raise handle_rpc_error(e) from e
        except asyncio.CancelledError:
            # Check if there was an error iterating the request.
            if append_requests.errored:
                raise append_requests.errored from None
            raise CancelledByClientError from None  # pragma: no cover

    async def append_records(  # pragma: <26.1 no cover
        self,
        records: Iterable[NewRecord],
        checks: Iterable[StreamStateCheck] | None,
        timeout: float | None = None,
        metadata: Metadata | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AppendRecordsResult:
        append_requests = _construct_append_records_request(records, checks)
        try:
            append_records_response = await self._stub.AppendRecords(
                append_requests,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
            assert isinstance(
                append_records_response, streams_pb2.AppendRecordsResponse
            )
            return _convert_append_records_response(append_records_response)
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e


# no cover <25.1: stop
