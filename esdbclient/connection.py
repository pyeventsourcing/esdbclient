# -*- coding: utf-8 -*-

import grpc.aio

from esdbclient.common import AsyncGrpcStreamers, GrpcStreamers
from esdbclient.connection_spec import ConnectionSpec
from esdbclient.gossip import AsyncGossipService, GossipService
from esdbclient.persistent import (
    AsyncPersistentSubscriptionsService,
    PersistentSubscriptionsService,
)
from esdbclient.projections import AsyncProjectionsService, ProjectionsService
from esdbclient.streams import AsyncStreamsService, StreamsService


class BaseESDBConnection:
    def __init__(self, grpc_target: str):
        self._grpc_target = grpc_target

    @property
    def grpc_target(self) -> str:
        return self._grpc_target


class ESDBConnection(BaseESDBConnection):
    def __init__(
        self,
        grpc_channel: grpc.Channel,
        grpc_target: str,
        connection_spec: ConnectionSpec,
    ) -> None:
        super().__init__(grpc_target)
        self._grpc_channel = grpc_channel
        self._grpc_streamers = GrpcStreamers()
        self.gossip = GossipService(
            channel=grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        self.streams = StreamsService(
            grpc_channel=grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        self.persistent_subscriptions = PersistentSubscriptionsService(
            channel=grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        self.projections = ProjectionsService(
            channel=grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        # self._channel_connectivity_state: Optional[ChannelConnectivity] = None
        # self.grpc_channel.subscribe(self._receive_channel_connectivity_state)

    # def _receive_channel_connectivity_state(
    #     self, connectivity: ChannelConnectivity
    # ) -> None:
    #     self._channel_connectivity_state = connectivity
    #     # print("Channel connectivity state:", connectivity)

    def close(self) -> None:
        self._grpc_streamers.close()
        # self.grpc_channel.unsubscribe(self._receive_channel_connectivity_state)
        # sleep(0.1)  # Allow connectivity polling to stop.
        # print("closing channel")
        self._grpc_channel.close()
        # print("closed channel")


class AsyncESDBConnection(BaseESDBConnection):
    def __init__(
        self,
        grpc_channel: grpc.aio.Channel,
        grpc_target: str,
        connection_spec: ConnectionSpec,
    ) -> None:
        super().__init__(grpc_target)
        self._grpc_channel = grpc_channel
        self._grpc_target = grpc_target
        self._grpc_streamers = AsyncGrpcStreamers()
        self.gossip = AsyncGossipService(
            grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        self.streams = AsyncStreamsService(
            grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        self.persistent_subscriptions = AsyncPersistentSubscriptionsService(
            grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )
        self.projections = AsyncProjectionsService(
            grpc_channel,
            connection_spec=connection_spec,
            grpc_streamers=self._grpc_streamers,
        )

    async def close(self) -> None:
        await self._grpc_streamers.close()
        await self._grpc_channel.close(grace=5)
