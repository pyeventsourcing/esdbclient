# -*- coding: utf-8 -*-
import asyncio
import sys
from asyncio import Event, Lock
from functools import wraps
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
    overload,
)

import grpc.aio
from typing_extensions import Literal

from esdbclient.client import DEFAULT_EXCLUDE_FILTER, BaseEventStoreDBClient
from esdbclient.common import (
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
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
from esdbclient.persistent import (
    AsyncioPersistentSubscription,
    ConsumerStrategy,
    SubscriptionInfo,
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
        *,
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
        *,
        current_version: Union[int, StreamState],
        events: Union[NewEvent, Sequence[NewEvent]],
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
        *,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
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
            resolve_links=resolve_links,
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
        *,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[RecordedEvent]:
        """
        Lists recorded events from the named stream.
        """
        async with await self.read_stream(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            resolve_links=resolve_links,
            limit=limit,
            timeout=timeout,
            credentials=credentials or self._call_credentials,
        ) as events:
            return tuple([e async for e in events])

    async def read_stream(
        self,
        stream_name: str,
        *,
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
        *,
        commit_position: Optional[int] = None,
        from_end: bool = False,
        resolve_links: bool = False,
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
            from_end=from_end,
            resolve_links=resolve_links,
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

    @overload
    async def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Signature to start catch-up subscription from the start of the stream.
        """

    @overload
    async def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Signature to start catch-up subscription from a particular stream position.
        """

    @overload
    async def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        from_end: Literal[True] = True,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Signature to start catch-up subscription from the end of the stream.
        """

    @retrygrpc
    @autoreconnect
    async def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        stream_position: Optional[int] = None,
        from_end: bool = False,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioCatchupSubscription:
        """
        Starts a catch-up subscription from which
        recorded events in a stream can be received.
        """
        return await self._connection.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            from_end=from_end,
            resolve_links=resolve_links,
            subscribe=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def delete_stream(
        self,
        stream_name: str,
        *,
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
        *,
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

    @overload
    async def create_subscription_to_all(
        self,
        group_name: str,
        *,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from start of database.
        """

    @overload
    async def create_subscription_to_all(
        self,
        group_name: str,
        *,
        commit_position: int,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from a commit position.
        """

    @overload
    async def create_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from end of database.
        """

    @retrygrpc
    @autoreconnect
    async def create_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Creates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.persistent_subscriptions.create(
            group_name=group_name,
            from_end=from_end,
            commit_position=commit_position,
            resolve_links=resolve_links,
            consumer_strategy=consumer_strategy,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            window_size=window_size,
            checkpoint_interval_multiplier=checkpoint_interval_multiplier,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    async def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating stream subscription from start of stream.
        """

    @overload
    async def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating stream subscription from stream position.
        """

    @overload
    async def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating stream subscription from end of stream.
        """

    @retrygrpc
    @autoreconnect
    async def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Creates a persistent subscription on one stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.persistent_subscriptions.create(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            stream_position=stream_position,
            resolve_links=resolve_links,
            consumer_strategy=consumer_strategy,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def read_subscription_to_all(
        self,
        group_name: str,
        *,
        event_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioPersistentSubscription:
        """
        Reads a persistent subscription on all streams.
        """
        return await self._connection.persistent_subscriptions.read(
            group_name=group_name,
            event_buffer_size=event_buffer_size,
            max_ack_batch_size=max_ack_batch_size,
            max_ack_delay=max_ack_delay,
            stopping_grace=stopping_grace,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def read_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        event_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> AsyncioPersistentSubscription:
        """
        Reads a persistent subscription on one stream.
        """
        return await self._connection.persistent_subscriptions.read(
            group_name=group_name,
            stream_name=stream_name,
            event_buffer_size=event_buffer_size,
            max_ack_batch_size=max_ack_batch_size,
            max_ack_delay=max_ack_delay,
            stopping_grace=stopping_grace,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def get_subscription_info(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> SubscriptionInfo:
        """
        Gets info for a persistent subscription.
        """
        return await self._connection.persistent_subscriptions.get_info(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def list_subscriptions(
        self,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[SubscriptionInfo]:
        """
        Lists all persistent subscriptions.
        """
        return await self._connection.persistent_subscriptions.list(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def list_subscriptions_to_stream(
        self,
        stream_name: str,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[SubscriptionInfo]:
        """
        Lists persistent stream subscriptions.
        """
        return await self._connection.persistent_subscriptions.list(
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating persistent subscription from start of database.
        """

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        commit_position: int,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating persistent subscription from a commit position.
        """

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating persistent subscription from end of database.
        """

    @retrygrpc
    @autoreconnect
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Updates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.persistent_subscriptions.update(
            group_name=group_name,
            from_end=from_end,
            commit_position=commit_position,
            resolve_links=resolve_links,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating stream subscription from start of stream.
        """

    @overload
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating stream subscription from stream position.
        """

    @overload
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating stream subscription from end of stream.
        """

    @retrygrpc
    @autoreconnect
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Updates a persistent subscription on one stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.persistent_subscriptions.update(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            stream_position=stream_position,
            resolve_links=resolve_links,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def replay_parked_events(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.persistent_subscriptions.replay_parked(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def delete_subscription(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Deletes a persistent subscription.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.persistent_subscriptions.delete(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    async def close(self) -> None:
        if not self._is_closed:
            try:
                esdb_connection = self._connection
                del self._connection
            except AttributeError:  # pragma: no cover
                pass
            else:
                await esdb_connection.close()
                self._is_closed = True

    async def __aenter__(self) -> "_AsyncioEventStoreDBClient":
        return self

    async def __aexit__(self, *args: Any, **kwargs: Any) -> None:
        await self.close()
