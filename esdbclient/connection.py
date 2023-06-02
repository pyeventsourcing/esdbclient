# -*- coding: utf-8 -*-

import grpc.aio

from esdbclient.connection_spec import ConnectionSpec
from esdbclient.gossip import (
    AsyncioClusterGossipService,
    AsyncioGossipService,
    ClusterGossipService,
    GossipService,
)
from esdbclient.persistent import (
    AsyncioPersistentSubscriptionsService,
    PersistentSubscriptionsService,
)
from esdbclient.streams import AsyncioStreamsService, StreamsService


class ESDBConnection:
    def __init__(
        self,
        grpc_channel: grpc.Channel,
        grpc_target: str,
        connection_spec: ConnectionSpec,
    ) -> None:
        self.grpc_channel = grpc_channel
        self.grpc_target = grpc_target
        self.streams = StreamsService(grpc_channel, connection_spec=connection_spec)
        self.persistent_subscriptions = PersistentSubscriptionsService(
            grpc_channel, connection_spec=connection_spec
        )
        self.gossip = GossipService(grpc_channel, connection_spec=connection_spec)
        self.cluster_gossip = ClusterGossipService(
            grpc_channel, connection_spec=connection_spec
        )
        # self._channel_connectivity_state: Optional[ChannelConnectivity] = None
        # self.grpc_channel.subscribe(self._receive_channel_connectivity_state)

    # def _receive_channel_connectivity_state(
    #     self, connectivity: ChannelConnectivity
    # ) -> None:
    #     self._channel_connectivity_state = connectivity
    #     # print("Channel connectivity state:", connectivity)

    def close(self) -> None:
        # self.grpc_channel.unsubscribe(self._receive_channel_connectivity_state)
        # sleep(0.1)  # Allow connectivity polling to stop.
        self.grpc_channel.close()


class AsyncioESDBConnection:
    def __init__(
        self,
        grpc_channel: grpc.aio.Channel,
        grpc_target: str,
        connection_spec: ConnectionSpec,
    ) -> None:
        self.grpc_channel = grpc_channel
        self.grpc_target = grpc_target
        self.connection_spec = connection_spec
        self.streams = AsyncioStreamsService(
            grpc_channel, connection_spec=connection_spec
        )
        self.persistent_subscriptions = AsyncioPersistentSubscriptionsService(
            grpc_channel, connection_spec=connection_spec
        )
        self.gossip = AsyncioGossipService(
            grpc_channel, connection_spec=connection_spec
        )
        self.cluster_gossip = AsyncioClusterGossipService(
            grpc_channel, connection_spec=connection_spec
        )

    async def close(self) -> None:
        await self.grpc_channel.close(grace=5)
