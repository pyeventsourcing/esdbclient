# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional, Sequence, Union

import grpc
import grpc.aio

from esdbclient.common import (
    AsyncGrpcStreamers,
    ESDBService,
    GrpcStreamers,
    Metadata,
    TGrpcStreamers,
    handle_rpc_error,
)
from esdbclient.connection_spec import ConnectionSpec
from esdbclient.protos.Grpc import gossip_pb2, gossip_pb2_grpc, shared_pb2


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


class BaseGossipService(ESDBService[TGrpcStreamers]):
    def __init__(
        self,
        channel: Union[grpc.Channel, grpc.aio.Channel],
        connection_spec: ConnectionSpec,
        grpc_streamers: TGrpcStreamers,
    ):
        super().__init__(connection_spec=connection_spec, grpc_streamers=grpc_streamers)
        self._stub = gossip_pb2_grpc.GossipStub(channel)  # type: ignore[no-untyped-call]

    @staticmethod
    def _construct_cluster_members(
        cluster_info: gossip_pb2.ClusterInfo,
    ) -> Sequence[ClusterMember]:
        members = []
        for member_info in cluster_info.members:
            member = ClusterMember(
                GOSSIP_API_NODE_STATES_MAPPING.get(member_info.state, NODE_STATE_OTHER),
                member_info.http_end_point.address,
                member_info.http_end_point.port,
            )
            members.append(member)
        return tuple(members)


class AsyncGossipService(BaseGossipService[AsyncGrpcStreamers]):
    async def read(
        self,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[ClusterMember]:
        try:
            read_resp = await self._stub.Read(
                shared_pb2.Empty(),
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None

        return self._construct_cluster_members(read_resp)


class GossipService(BaseGossipService[GrpcStreamers]):
    """
    Encapsulates the 'gossip.Gossip' gRPC service.
    """

    def read(
        self,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[ClusterMember]:
        try:
            read_resp = self._stub.Read(
                shared_pb2.Empty(),
                timeout=timeout,
                metadata=self._metadata(metadata),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None

        return self._construct_cluster_members(read_resp)
