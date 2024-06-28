# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Optional, Union

import grpc
import grpc.aio
from google.protobuf import struct_pb2

from esdbclient.common import (
    AsyncGrpcStreamers,
    ESDBService,
    GrpcStreamers,
    Metadata,
    TGrpcStreamers,
    handle_rpc_error,
)
from esdbclient.connection_spec import ConnectionSpec
from esdbclient.exceptions import EventStoreDBClientException
from esdbclient.protos.Grpc import projections_pb2, projections_pb2_grpc, shared_pb2


@dataclass(frozen=True)
class ProjectionStatistics:
    core_processing_time: int
    version: int
    epoch: int
    effective_name: str
    writes_in_progress: int
    reads_in_progress: int
    partitions_cached: int
    status: str
    state_reason: str
    name: str
    mode: str
    position: str
    progress: float
    last_checkpoint: str
    events_processed_after_restart: int
    checkpoint_status: str
    buffered_events: int
    write_pending_events_before_checkpoint: int
    write_pending_events_after_checkpoint: int


@dataclass(frozen=True)
class ProjectionState:
    value: Any


@dataclass(frozen=True)
class ProjectionResult:
    value: Any


class BaseProjectionsService(ESDBService[TGrpcStreamers]):
    def __init__(
        self,
        channel: Union[grpc.Channel, grpc.aio.Channel],
        connection_spec: ConnectionSpec,
        grpc_streamers: TGrpcStreamers,
    ):
        super().__init__(connection_spec=connection_spec, grpc_streamers=grpc_streamers)
        self._stub = projections_pb2_grpc.ProjectionsStub(channel)  # type: ignore[no-untyped-call]

    @staticmethod
    def _construct_create_req(
        query: str,
        name: str,
        emit_enabled: bool,
        track_emitted_streams: bool,
    ) -> projections_pb2.CreateReq:
        options = projections_pb2.CreateReq.Options(
            continuous=projections_pb2.CreateReq.Options.Continuous(
                name=name,
                emit_enabled=emit_enabled,
                track_emitted_streams=track_emitted_streams,
            ),
            query=query,
        )
        return projections_pb2.CreateReq(options=options)

    @staticmethod
    def _construct_update_req(
        name: str,
        query: str,
        emit_enabled: bool,
    ) -> projections_pb2.UpdateReq:
        return projections_pb2.UpdateReq(
            options=projections_pb2.UpdateReq.Options(
                name=name,
                query=query,
                emit_enabled=emit_enabled,
            )
        )

    @staticmethod
    def _construct_delete_req(
        name: str,
        delete_emitted_streams: bool,
        delete_state_stream: bool,
        delete_checkpoint_stream: bool,
    ) -> projections_pb2.DeleteReq:
        return projections_pb2.DeleteReq(
            options=projections_pb2.DeleteReq.Options(
                name=name,
                delete_emitted_streams=delete_emitted_streams,
                delete_state_stream=delete_state_stream,
                delete_checkpoint_stream=delete_checkpoint_stream,
            ),
        )

    @staticmethod
    def _construct_statistics_req(
        name: str,
    ) -> projections_pb2.StatisticsReq:
        return projections_pb2.StatisticsReq(
            options=projections_pb2.StatisticsReq.Options(name=name),
        )

    @staticmethod
    def _construct_disable_req(
        name: str,
        write_checkpoint: bool,
    ) -> projections_pb2.DisableReq:
        return projections_pb2.DisableReq(
            options=projections_pb2.DisableReq.Options(
                name=name,
                write_checkpoint=write_checkpoint,
            ),
        )

    @staticmethod
    def _construct_enable_req(
        name: str,
    ) -> projections_pb2.EnableReq:
        return projections_pb2.EnableReq(
            options=projections_pb2.EnableReq.Options(
                name=name,
            ),
        )

    @staticmethod
    def _construct_reset_req(
        name: str,
        write_checkpoint: bool,
    ) -> projections_pb2.ResetReq:
        return projections_pb2.ResetReq(
            options=projections_pb2.ResetReq.Options(
                name=name,
                write_checkpoint=write_checkpoint,
            ),
        )

    @staticmethod
    def _construct_state_req(
        name: str,
        partition: str,
    ) -> projections_pb2.StateReq:
        return projections_pb2.StateReq(
            options=projections_pb2.StateReq.Options(
                name=name,
                partition=partition,
            ),
        )

    # @staticmethod
    # def _construct_result_req(
    #     name: str,
    #     partition: str,
    # ) -> projections_pb2.ResultReq:
    #     return projections_pb2.ResultReq(
    #         options=projections_pb2.ResultReq.Options(
    #             name=name,
    #             partition=partition,
    #         ),
    #     )

    def _extract_value(self, value: struct_pb2.Value) -> Any:
        # message Value {
        #   // The kind of value.
        #   oneof kind {
        #     // Represents a null value.
        #     NullValue null_value = 1;
        #     // Represents a double value.
        #     double number_value = 2;
        #     // Represents a string value.
        #     string string_value = 3;
        #     // Represents a boolean value.
        #     bool bool_value = 4;
        #     // Represents a structured value.
        #     Struct struct_value = 5;
        #     // Represents a repeated `Value`.
        #     ListValue list_value = 6;
        #   }
        # }
        kind_oneof = value.WhichOneof("kind")
        if kind_oneof == "null_value":
            return None
        elif kind_oneof == "number_value":
            return value.number_value
        elif kind_oneof == "string_value":
            return value.string_value
        elif kind_oneof == "bool_value":
            return value.bool_value
        elif kind_oneof == "struct_value":
            fields = value.struct_value.fields
            return {f: self._extract_value(fields[f]) for f in fields}
        elif kind_oneof == "list_value":
            return [self._extract_value(v) for v in value.list_value.values]
        else:  # pragma: no cover
            raise ValueError(f"Unsupported kind of value '{kind_oneof}': {value}")

    @staticmethod
    def _construct_projection_statistics(
        statistics_resp: projections_pb2.StatisticsResp,
    ) -> ProjectionStatistics:
        return ProjectionStatistics(
            core_processing_time=statistics_resp.details.coreProcessingTime,
            version=statistics_resp.details.version,
            epoch=statistics_resp.details.epoch,
            effective_name=statistics_resp.details.effectiveName,
            writes_in_progress=statistics_resp.details.writesInProgress,
            reads_in_progress=statistics_resp.details.readsInProgress,
            partitions_cached=statistics_resp.details.partitionsCached,
            status=statistics_resp.details.status,
            state_reason=statistics_resp.details.stateReason,
            name=statistics_resp.details.name,
            mode=statistics_resp.details.mode,
            position=statistics_resp.details.position,
            progress=statistics_resp.details.progress,
            last_checkpoint=statistics_resp.details.lastCheckpoint,
            events_processed_after_restart=statistics_resp.details.eventsProcessedAfterRestart,
            checkpoint_status=statistics_resp.details.checkpointStatus,
            buffered_events=statistics_resp.details.bufferedEvents,
            write_pending_events_before_checkpoint=statistics_resp.details.writePendingEventsBeforeCheckpoint,
            write_pending_events_after_checkpoint=statistics_resp.details.writePendingEventsAfterCheckpoint,
        )


class AsyncProjectionsService(BaseProjectionsService[AsyncGrpcStreamers]):
    async def create(
        self,
        query: str,
        name: str,
        emit_enabled: bool,
        track_emitted_streams: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        create_req = self._construct_create_req(
            query=query,
            name=name,
            emit_enabled=emit_enabled,
            track_emitted_streams=track_emitted_streams,
        )
        try:
            create_resp = await self._stub.Create(
                create_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(create_resp, projections_pb2.CreateResp)

    async def update(
        self,
        name: str,
        query: str,
        emit_enabled: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        update_req = self._construct_update_req(
            name=name,
            query=query,
            emit_enabled=emit_enabled,
        )
        try:
            update_resp = await self._stub.Update(
                update_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(update_resp, projections_pb2.UpdateResp)

    async def delete(
        self,
        name: str,
        delete_emitted_streams: bool,
        delete_state_stream: bool,
        delete_checkpoint_stream: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        delete_req = self._construct_delete_req(
            name=name,
            delete_emitted_streams=delete_emitted_streams,
            delete_state_stream=delete_state_stream,
            delete_checkpoint_stream=delete_checkpoint_stream,
        )
        try:
            delete_resp = await self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(delete_resp, projections_pb2.DeleteResp)

    async def get_statistics(
        self,
        name: str,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ProjectionStatistics:
        statistics_req = self._construct_statistics_req(name=name)
        try:
            statistics_resps = self._stub.Statistics(
                statistics_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
            async for statistics_resp in statistics_resps:
                assert isinstance(
                    statistics_resp, projections_pb2.StatisticsResp
                ), statistics_resp
                projection_statistics = self._construct_projection_statistics(
                    statistics_resp
                )
                return projection_statistics
            else:  # pragma: no cover
                raise EventStoreDBClientException(
                    "Statistics request didn't return any statistics"
                )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None

    async def disable(
        self,
        name: str,
        write_checkpoint: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        disable_req = self._construct_disable_req(
            name=name,
            write_checkpoint=write_checkpoint,
        )
        try:
            disable_resp = await self._stub.Disable(
                disable_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(disable_resp, projections_pb2.DisableResp)

    async def enable(
        self,
        name: str,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        enable_req = self._construct_enable_req(
            name=name,
        )
        try:
            enable_resp = await self._stub.Enable(
                enable_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(enable_resp, projections_pb2.EnableResp)

    async def reset(
        self,
        name: str,
        write_checkpoint: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        reset_req = self._construct_reset_req(
            name=name,
            write_checkpoint=write_checkpoint,
        )
        try:
            reset_resp = await self._stub.Reset(
                reset_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(reset_resp, projections_pb2.ResetResp)

    async def get_state(
        self,
        name: str,
        partition: str,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ProjectionState:
        state_req = self._construct_state_req(
            name=name,
            partition=partition,
        )
        try:
            state_resp = await self._stub.State(
                state_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(state_resp, projections_pb2.StateResp)
        value = self._extract_value(state_resp.state)
        return ProjectionState(value=value)

    # async def get_result(
    #     self,
    #     name: str,
    #     partition: str,
    #     timeout: Optional[float] = None,
    #     metadata: Optional[Metadata] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> ProjectionResult:
    #     result_req = self._construct_result_req(
    #         name=name,
    #         partition=partition,
    #     )
    #     try:
    #         result_resp = await self._stub.Result(
    #             result_req,
    #             timeout=timeout,
    #             metadata=self._metadata(metadata, requires_leader=True),
    #             credentials=credentials,
    #         )
    #     except grpc.RpcError as e:
    #         raise handle_rpc_error(e) from None
    #     assert isinstance(result_resp, projections_pb2.ResultResp)
    #     value = self._extract_value(result_resp.result)
    #     return ProjectionResult(value=value)

    async def restart_subsystem(
        self,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        try:
            empty_resp = await self._stub.RestartSubsystem(
                shared_pb2.Empty(),
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:  # pragma: no cover
            raise handle_rpc_error(e) from None
        assert isinstance(empty_resp, shared_pb2.Empty)


class ProjectionsService(BaseProjectionsService[GrpcStreamers]):
    """
    Encapsulates the 'gossip.Projections' gRPC service.
    """

    def create(
        self,
        query: str,
        name: str,
        emit_enabled: bool,
        track_emitted_streams: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        create_req = self._construct_create_req(
            query=query,
            name=name,
            emit_enabled=emit_enabled,
            track_emitted_streams=track_emitted_streams,
        )
        try:
            create_resp = self._stub.Create(
                create_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(create_resp, projections_pb2.CreateResp)

    def update(
        self,
        name: str,
        query: str,
        emit_enabled: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        update_req = self._construct_update_req(
            name=name,
            query=query,
            emit_enabled=emit_enabled,
        )
        try:
            update_resp = self._stub.Update(
                update_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(update_resp, projections_pb2.UpdateResp)

    def delete(
        self,
        name: str,
        delete_emitted_streams: bool,
        delete_state_stream: bool,
        delete_checkpoint_stream: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        delete_req = self._construct_delete_req(
            name=name,
            delete_emitted_streams=delete_emitted_streams,
            delete_state_stream=delete_state_stream,
            delete_checkpoint_stream=delete_checkpoint_stream,
        )
        try:
            delete_resp = self._stub.Delete(
                delete_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(delete_resp, projections_pb2.DeleteResp)

    def get_statistics(
        self,
        name: str,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ProjectionStatistics:
        statistics_req = self._construct_statistics_req(name=name)
        try:
            statistics_resps = self._stub.Statistics(
                statistics_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
            for statistics_resp in statistics_resps:
                assert isinstance(
                    statistics_resp, projections_pb2.StatisticsResp
                ), statistics_resp
                projection_statistics = self._construct_projection_statistics(
                    statistics_resp
                )
                return projection_statistics
            else:  # pragma: no cover
                raise EventStoreDBClientException(
                    "Statistics request didn't return any statistics"
                )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None

    def disable(
        self,
        name: str,
        write_checkpoint: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        disable_req = self._construct_disable_req(
            name=name,
            write_checkpoint=write_checkpoint,
        )
        try:
            disable_resp = self._stub.Disable(
                disable_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(disable_resp, projections_pb2.DisableResp)

    def enable(
        self,
        name: str,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        enable_req = self._construct_enable_req(
            name=name,
        )
        try:
            enable_resp = self._stub.Enable(
                enable_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(enable_resp, projections_pb2.EnableResp)

    def reset(
        self,
        name: str,
        write_checkpoint: bool,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        reset_req = self._construct_reset_req(
            name=name,
            write_checkpoint=write_checkpoint,
        )
        try:
            reset_resp = self._stub.Reset(
                reset_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(reset_resp, projections_pb2.ResetResp)

    def get_state(
        self,
        name: str,
        partition: str,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ProjectionState:
        state_req = self._construct_state_req(
            name=name,
            partition=partition,
        )
        try:
            state_resp = self._stub.State(
                state_req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(state_resp, projections_pb2.StateResp)
        value = self._extract_value(state_resp.state)
        return ProjectionState(value=value)

    # def get_result(
    #     self,
    #     name: str,
    #     partition: str,
    #     timeout: Optional[float] = None,
    #     metadata: Optional[Metadata] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> ProjectionResult:
    #     result_req = self._construct_result_req(
    #         name=name,
    #         partition=partition,
    #     )
    #     try:
    #         result_resp = self._stub.Result(
    #             result_req,
    #             timeout=timeout,
    #             metadata=self._metadata(metadata, requires_leader=True),
    #             credentials=credentials,
    #         )
    #     except grpc.RpcError as e:
    #         raise handle_rpc_error(e) from None
    #     assert isinstance(result_resp, projections_pb2.ResultResp)
    #     value = self._extract_value(result_resp.result)
    #     return ProjectionResult(value=value)

    def restart_subsystem(
        self,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        try:
            empty_resp = self._stub.RestartSubsystem(
                shared_pb2.Empty(),
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:  # pragma: no cover
            raise handle_rpc_error(e) from None
        assert isinstance(empty_resp, shared_pb2.Empty)
