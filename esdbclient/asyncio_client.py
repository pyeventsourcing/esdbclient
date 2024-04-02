# -*- coding: utf-8 -*-
import asyncio
import sys
from asyncio import Event, Lock
from functools import wraps
from typing import Any, Callable, Iterable, Optional, Sequence, TypeVar, Union, cast

import grpc.aio

from esdbclient.client import DEFAULT_EXCLUDE_FILTER, BaseEventStoreDBClient
from esdbclient.common import (
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_WINDOW_SIZE,
    GrpcOptions,
)
from esdbclient.connection import AsyncioESDBConnection
from esdbclient.connection_spec import NODE_PREFERENCE_LEADER, URI_SCHEME_ESDB
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    DeadlineExceeded,
    DiscoveryFailed,
    GrpcError,
    NodeIsNotLeader,
    ServiceUnavailable,
)
from esdbclient.streams import (
    AsyncioCatchupSubscription,
    AsyncioReadResponse,
    StreamState,
)

_TCallable = TypeVar("_TCallable", bound=Callable[..., Any])


def autoreconnect(f: _TCallable) -> _TCallable:
    @wraps(f)
    async def autoreconnect_decorator(
        client: "_AsyncioEventStoreDBClient", *args: Any, **kwargs: Any
    ) -> Any:
        try:
            return await f(client, *args, **kwargs)

        except NodeIsNotLeader as e:
            if (
                client.connection_spec.options.NodePreference == NODE_PREFERENCE_LEADER
                and not (
                    client.connection_spec.scheme == URI_SCHEME_ESDB
                    and len(client.connection_spec.targets) == 1
                )
            ):
                await client.reconnect(e.leader_grpc_target)
                await asyncio.sleep(0.1)
                return await f(client, *args, **kwargs)
            else:
                raise

        except grpc.aio.UsageError as e:
            if "Channel is closed" in str(e):
                await client.reconnect()
                await asyncio.sleep(0.1)
                return await f(client, *args, **kwargs)
            else:  # pragma: no cover
                raise

        except ServiceUnavailable:
            await client.reconnect()
            await asyncio.sleep(0.1)
            return await f(client, *args, **kwargs)

    return cast(_TCallable, autoreconnect_decorator)


def retrygrpc(f: _TCallable) -> _TCallable:
    @wraps(f)
    async def retrygrpc_decorator(*args: Any, **kwargs: Any) -> Any:
        try:
            return await f(*args, **kwargs)
        except GrpcError:
            await asyncio.sleep(0.1)
            return await f(*args, **kwargs)

    return cast(_TCallable, retrygrpc_decorator)


async def AsyncioEventStoreDBClient(
    uri: str, root_certificates: Optional[str] = None
) -> "_AsyncioEventStoreDBClient":
    client = _AsyncioEventStoreDBClient(uri=uri, root_certificates=root_certificates)
    await client.connect()
    return client


class _AsyncioEventStoreDBClient(BaseEventStoreDBClient):
    def __init__(self, uri: str, root_certificates: Optional[str] = None):
        super().__init__(uri=uri, root_certificates=root_certificates)
        self._is_reconnection_required = Event()
        self._reconnection_lock = Lock()

    async def connect(self) -> None:
        self._connection = await self._connect()

    async def reconnect(self, grpc_target: Optional[str] = None) -> None:
        self._is_reconnection_required.set()
        async with self._reconnection_lock:
            if self._is_reconnection_required.is_set():
                new = await self._connect(grpc_target)
                old, self._connection = self._connection, new
                await old.close()
                self._is_reconnection_required.clear()
            else:  # pragma: no cover
                # Todo: Test with concurrent writes to wrong node state.
                pass

    async def _connect(
        self, grpc_target: Optional[str] = None
    ) -> AsyncioESDBConnection:
        if grpc_target:
            # Just connect to the given target.
            return self._construct_esdb_connection(grpc_target)
        if (
            self.connection_spec.scheme == URI_SCHEME_ESDB
            and len(self.connection_spec.targets) == 1
        ):
            # Just connect to the specified target.
            return self._construct_esdb_connection(
                grpc_target=self.connection_spec.targets[0],
            )
        # Discover preferred node in cluster.
        return await self._discover_preferred_node()

    async def _discover_preferred_node(self) -> AsyncioESDBConnection:
        attempts = self.connection_spec.options.MaxDiscoverAttempts
        assert attempts > 0
        if self.connection_spec.scheme == URI_SCHEME_ESDB:
            grpc_options: GrpcOptions = ()
        else:
            grpc_options = (("grpc.lb_policy_name", "round_robin"),)
        while True:
            # Attempt to discover preferred node.
            try:
                last_exception: Optional[Exception] = None
                for grpc_target in self.connection_spec.targets:
                    connection = self._construct_esdb_connection(
                        grpc_target=grpc_target,
                        grpc_options=grpc_options,
                    )
                    try:
                        cluster_members = await connection.gossip.read(
                            timeout=self.connection_spec.options.GossipTimeout,
                            metadata=self._call_metadata,
                            credentials=self._call_credentials,
                        )
                    except (GrpcError, DeadlineExceeded) as e:
                        last_exception = e
                        await connection.close()
                    else:
                        break
                else:
                    msg = (
                        "Failed to obtain cluster info from"
                        f" '{','.join(self.connection_spec.targets)}':"
                        f" {str(last_exception)}"
                    )
                    raise DiscoveryFailed(msg) from last_exception

                preferred_member = self._select_preferred_member(cluster_members)

            except DiscoveryFailed:
                attempts -= 1
                if attempts == 0:
                    raise
                else:
                    await asyncio.sleep(
                        self.connection_spec.options.DiscoveryInterval / 1000
                    )
            else:
                break

        # Maybe close connection and connect to preferred node.
        if len(cluster_members) > 1:  # forgive not "advertising" single node
            preferred_target = f"{preferred_member.address}:{preferred_member.port}"
            if preferred_target != connection.grpc_target:
                await connection.close()
                connection = self._construct_esdb_connection(preferred_target)

        return connection

    def _construct_esdb_connection(
        self, grpc_target: str, grpc_options: GrpcOptions = ()
    ) -> AsyncioESDBConnection:
        grpc_options = self.grpc_options + grpc_options
        if self.connection_spec.options.Tls is True:
            assert self.root_certificates is not None
            root_certificates = self.root_certificates.encode()
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certificates
            )
            grpc_channel = grpc.aio.secure_channel(
                target=grpc_target,
                credentials=channel_credentials,
                options=grpc_options,
            )
        else:
            grpc_channel = grpc.aio.insecure_channel(
                target=grpc_target, options=grpc_options
            )

        return AsyncioESDBConnection(
            grpc_channel=grpc_channel,
            grpc_target=grpc_target,
            connection_spec=self.connection_spec,
        )

    @retrygrpc
    @autoreconnect
    async def append_events(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        timeout = timeout if timeout is not None else self._default_deadline
        return await self._connection.streams.batch_append(
            stream_name=stream_name,
            current_version=current_version,
            events=events,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    async def append_to_stream(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        events: Union[NewEvent, Iterable[NewEvent]],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        if isinstance(events, NewEvent):
            events = [events]
        return await self.append_events(
            stream_name=stream_name,
            current_version=current_version,
            events=events,
            timeout=timeout,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def read_all(
        self,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioReadResponse:
        """
        Reads recorded events in "all streams" in the database.
        """
        return await self._connection.streams.read(
            commit_position=commit_position,
            backwards=backwards,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=limit,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def get_stream(
        self,
        stream_name: str,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[RecordedEvent]:
        """
        Lists recorded events from the named stream.
        """
        events = await self.read_stream(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            limit=limit,
            timeout=timeout,
            credentials=credentials or self._call_credentials,
        )
        return tuple([e async for e in events])

    async def read_stream(
        self,
        stream_name: str,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioReadResponse:
        """
        Reads recorded events from the named stream.
        """
        return await self._connection.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            resolve_links=resolve_links,
            limit=limit,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def subscribe_to_all(
        self,
        commit_position: Optional[int] = None,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        include_checkpoints: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Starts a catch-up subscription, from which all
        recorded events in the database can be received.
        """
        return await self._connection.streams.read(
            commit_position=commit_position,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            subscribe=True,
            include_checkpoints=include_checkpoints,
            window_size=window_size,
            checkpoint_interval_multiplier=checkpoint_interval_multiplier,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def delete_stream(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        # Todo: Reconsider using current_version=None to indicate "stream exists"?
        timeout = timeout if timeout is not None else self._default_deadline
        await self._connection.streams.delete(
            stream_name=stream_name,
            current_version=current_version,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def tombstone_stream(
        self,
        stream_name: str,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline
        await self._connection.streams.tombstone(
            stream_name=stream_name,
            current_version=current_version,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    async def close(self) -> None:
        await self._connection.close()
