from __future__ import annotations

import asyncio
import json
import sys
from asyncio import Event, Lock
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    TypeVar,
    cast,
    overload,
)

import grpc.aio
from typing_extensions import Literal, Self

from kurrentdbclient.client import DEFAULT_EXCLUDE_FILTER, BaseKurrentDBClient
from kurrentdbclient.common import (
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
    DEFAULT_PERSISTENT_SUB_EVENT_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUB_MAX_ACK_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUB_MAX_ACK_DELAY,
    DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUB_STOPPING_GRACE,
    DEFAULT_WINDOW_SIZE,
    AbstractAsyncCatchupSubscription,
    AbstractAsyncPersistentSubscription,
    GrpcOptions,
    grpc_target,
)
from kurrentdbclient.connection import AsyncKurrentDBConnection
from kurrentdbclient.connection_spec import (
    NODE_PREFERENCE_LEADER,
    URI_SCHEMES_NON_DISCOVER,
)
from kurrentdbclient.events import NewEvent, NewEvents, RecordedEvent, StreamState
from kurrentdbclient.exceptions import (
    DeadlineExceededError,
    DiscoveryFailedError,
    GrpcError,
    NodeIsNotLeaderError,
    NotFoundError,
    ServiceUnavailableError,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from kurrentdbclient.persistent import ConsumerStrategy, SubscriptionInfo
    from kurrentdbclient.projections import ProjectionState, ProjectionStatistics
    from kurrentdbclient.streams import AsyncReadResponse

_TCallable = TypeVar("_TCallable", bound=Callable[..., Any])


def autoreconnect(f: _TCallable) -> _TCallable:
    @wraps(f)
    async def autoreconnect_decorator(
        client: AsyncKurrentDBClient, *args: Any, **kwargs: Any
    ) -> Any:
        try:
            return await f(client, *args, **kwargs)

        except NodeIsNotLeaderError as e:
            if (
                client.connection_spec.options.node_preference == NODE_PREFERENCE_LEADER
                and not (
                    client.connection_spec.scheme in URI_SCHEMES_NON_DISCOVER
                    and len(client.connection_spec.targets) == 1
                )
            ):
                await client.reconnect(
                    grpc_target(e.host, e.port) if e.host and e.port else None
                )
                await asyncio.sleep(0.1)
                return await f(client, *args, **kwargs)
            raise

        except grpc.aio.UsageError as e:
            if "Channel is closed" in str(e):
                await client.reconnect()
                await asyncio.sleep(0.1)
                return await f(client, *args, **kwargs)
            raise  # pragma: no cover

        except ServiceUnavailableError:
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


class AsyncKurrentDBClient(BaseKurrentDBClient):
    def __init__(
        self,
        uri: str,
        root_certificates: str | bytes | None = None,
        private_key: str | bytes | None = None,
        certificate_chain: str | bytes | None = None,
    ):
        super().__init__(
            uri=uri,
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        )
        self._is_reconnection_required = Event()
        self._reconnection_lock = Lock()

    @property
    def connection_target(self) -> str:
        return self._connection.grpc_target

    async def connect(self) -> None:
        self._connection = await self._connect()

    async def reconnect(self, grpc_target: str | None = None) -> None:
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
        self, grpc_target: str | None = None
    ) -> AsyncKurrentDBConnection:
        if grpc_target:
            # Just connect to the given target.
            return self._construct_esdb_connection(grpc_target)
        if (
            self.connection_spec.scheme in URI_SCHEMES_NON_DISCOVER
            and len(self.connection_spec.targets) == 1
        ):
            # Just connect to the specified target.
            return self._construct_esdb_connection(
                grpc_target=self.connection_spec.targets[0],
            )
        # Discover preferred node in cluster.
        return await self._discover_preferred_node()

    async def _discover_preferred_node(self) -> AsyncKurrentDBConnection:
        attempts = self.connection_spec.options.max_discover_attempts
        assert attempts > 0
        if self.connection_spec.scheme in URI_SCHEMES_NON_DISCOVER:
            grpc_options: GrpcOptions = ()
        else:
            grpc_options = (("grpc.lb_policy_name", "round_robin"),)
        while True:
            # Attempt to discover preferred node.
            try:
                last_exception: Exception | None = None
                for grpc_target in self.connection_spec.targets:
                    connection = self._construct_esdb_connection(
                        grpc_target=grpc_target,
                        grpc_options=grpc_options,
                    )
                    try:
                        cluster_members = await connection.gossip.read(
                            timeout=self.connection_spec.options.gossip_timeout,
                            metadata=self._call_metadata,
                            credentials=self._call_credentials,
                        )
                    except (GrpcError, DeadlineExceededError) as e:
                        last_exception = e
                        await connection.close()
                    else:
                        break
                else:
                    msg = (
                        "Failed to obtain cluster info from"
                        f" '{','.join(self.connection_spec.targets)}':"
                        f" {last_exception!s}"
                    )
                    raise DiscoveryFailedError(msg) from last_exception

                preferred_member = self._select_preferred_member(cluster_members)

            except DiscoveryFailedError:
                attempts -= 1
                if attempts == 0:
                    raise
                await asyncio.sleep(
                    self.connection_spec.options.discovery_interval / 1000
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
    ) -> AsyncKurrentDBConnection:
        grpc_options = self.grpc_options + grpc_options
        if self.connection_spec.options.tls is True:
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=self.root_certificates,
                private_key=self.private_key,
                certificate_chain=self.certificate_chain,
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

        return AsyncKurrentDBConnection(
            grpc_channel=grpc_channel,
            grpc_target=grpc_target,
            connection_spec=self.connection_spec,
        )

    async def append_events(
        self,
        stream_name: str,
        *,
        current_version: int | StreamState,
        events: Iterable[NewEvent],
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:
        return await self.append_to_stream(
            stream_name=stream_name,
            current_version=current_version,
            events=events,
            timeout=timeout,
            credentials=credentials,
        )

    @retrygrpc
    @autoreconnect
    async def append_to_stream(
        self,
        stream_name: str,
        *,
        current_version: int | StreamState,
        events: NewEvent | Iterable[NewEvent],
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:
        timeout = timeout if timeout is not None else self._default_deadline

        if isinstance(events, NewEvent):
            events = [events]

        return await self._connection.streams.batch_append(
            stream_name=stream_name,
            current_version=current_version,
            events=events,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def multi_append_to_stream(
        self,
        /,
        events: NewEvents | Iterable[NewEvents],
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:  # pragma: no v2cover
        """
        Appends new events to one or many streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline
        if isinstance(events, NewEvents):
            events = [events]
        return await self._connection.v2streams.multi_append(
            events=events,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def read_all(
        self,
        *,
        commit_position: int | None = None,
        backwards: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        limit: int = sys.maxsize,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AsyncReadResponse:
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
    async def get_commit_position(
        self,
        *,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int:
        """
        Returns the current commit position of the database.
        """
        read_response = await self.read_all(
            backwards=True,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=1,
            timeout=timeout,
            credentials=credentials,
        )
        recorded_events = tuple([ev async for ev in read_response])
        if recorded_events:
            return recorded_events[0].commit_position
        return 0

    @retrygrpc
    @autoreconnect
    async def get_stream(
        self,
        stream_name: str,
        *,
        stream_position: int | None = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[RecordedEvent, ...]:
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

    @retrygrpc
    @autoreconnect
    async def read_stream(
        self,
        stream_name: str,
        *,
        stream_position: int | None = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AsyncReadResponse:
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
    async def get_stream_metadata(
        self,
        stream_name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> tuple[dict[str, Any], int | Literal[StreamState.NO_STREAM]]:
        """
        Gets the stream metadata.
        """
        metadata_stream_name = f"$${stream_name}"
        try:
            metadata_events = await self.get_stream(
                stream_name=metadata_stream_name,
                backwards=True,
                limit=1,
                timeout=timeout,
                credentials=credentials or self._call_credentials,
            )
        except NotFoundError:
            return {}, StreamState.NO_STREAM
        else:
            metadata_event = metadata_events[0]
            return json.loads(metadata_event.data), metadata_event.stream_position

    async def set_stream_metadata(
        self,
        stream_name: str,
        *,
        metadata: dict[str, Any],
        current_version: int | StreamState = StreamState.ANY,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Sets the stream metadata.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        metadata_stream_name = f"$${stream_name}"
        metadata_event = NewEvent(
            type="$metadata",
            data=json.dumps(metadata).encode("utf8"),
        )
        await self.append_events(
            stream_name=metadata_stream_name,
            current_version=current_version,
            events=[metadata_event],
            timeout=timeout,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def subscribe_to_all(
        self,
        *,
        commit_position: int | None = None,
        from_end: bool = False,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        include_checkpoints: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        include_caught_up: bool = False,
        include_fell_behind: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AbstractAsyncCatchupSubscription:
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
            include_caught_up=include_caught_up,
            include_fell_behind=include_fell_behind,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    #
    # @overload
    # async def subscribe_to_stream(
    #     self,
    #     stream_name: str,
    #     *,
    #     resolve_links: bool = False,
    #     include_caught_up: bool = False,
    #     include_fell_behind: bool = False,
    #     timeout: Optional[float] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> AsyncCatchupSubscription:
    #     """
    #     Signature to start catch-up subscription from the start of the stream.
    #     """
    #
    # @overload
    # async def subscribe_to_stream(
    #     self,
    #     stream_name: str,
    #     *,
    #     stream_position: int,
    #     resolve_links: bool = False,
    #     include_caught_up: bool = False,
    #     include_fell_behind: bool = False,
    #     timeout: Optional[float] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> AsyncCatchupSubscription:
    #     """
    #     Signature to start catch-up subscription from a particular stream position.
    #     """
    #
    # @overload
    # async def subscribe_to_stream(
    #     self,
    #     stream_name: str,
    #     *,
    #     from_end: Literal[True] = True,
    #     resolve_links: bool = False,
    #     include_caught_up: bool = False,
    #     include_fell_behind: bool = False,
    #     timeout: Optional[float] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> AsyncCatchupSubscription:
    #     """
    #     Signature to start catch-up subscription from the end of the stream.
    #     """

    @retrygrpc
    @autoreconnect
    async def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        stream_position: int | None = None,
        from_end: bool = False,
        resolve_links: bool = False,
        include_caught_up: bool = False,
        include_fell_behind: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AbstractAsyncCatchupSubscription:
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
            include_caught_up=include_caught_up,
            include_fell_behind=include_fell_behind,
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
        current_version: int | StreamState,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        current_version: int | StreamState,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline
        await self._connection.streams.tombstone(
            stream_name=stream_name,
            current_version=current_version,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def get_current_version(
        self,
        stream_name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> int | Literal[StreamState.NO_STREAM]:
        """
        Returns the current position of the end of a stream.
        """
        try:
            events = [
                e
                async for e in await self._connection.streams.read(
                    stream_name=stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                    metadata=self._call_metadata,
                    credentials=credentials or self._call_credentials,
                )
            ]
        except NotFoundError:
            # StreamState.NO_STREAM is the correct "current version" both when appending
            # to a stream that never existed and when appending to a stream that has
            # been deleted (in this case of a deleted stream, the "current version"
            # before deletion is also correct).
            return StreamState.NO_STREAM
        else:
            last_event = events[0]
            return last_event.stream_position

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
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        commit_position: int | None = None,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
            checkpoint_after=checkpoint_after,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            extra_statistics=extra_statistics,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        stream_position: int | None = None,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUB_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUB_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUB_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUB_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUB_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUB_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUB_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUB_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
            checkpoint_after=checkpoint_after,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            extra_statistics=extra_statistics,
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
        event_buffer_size: int = DEFAULT_PERSISTENT_SUB_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUB_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUB_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUB_STOPPING_GRACE,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AbstractAsyncPersistentSubscription:
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
        event_buffer_size: int = DEFAULT_PERSISTENT_SUB_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUB_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUB_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUB_STOPPING_GRACE,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> AbstractAsyncPersistentSubscription:
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
        stream_name: str | None = None,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from same stream position.
        """

    @overload
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: Literal[False],
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from start of stream.
        """

    @overload
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: Literal[True],
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from end of stream.
        """

    @overload
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from stream position.
        """

    @retrygrpc
    @autoreconnect
    async def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool | None = None,
        stream_position: int | None = None,
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Updates a persistent subscription on one stream.
        """

        info = await self.get_subscription_info(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            credentials=credentials,
        )
        kwargs = info.update_stream_kwargs(
            from_end=from_end,
            stream_position=stream_position,
            resolve_links=resolve_links,
            consumer_strategy=consumer_strategy,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            checkpoint_after=checkpoint_after,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            extra_statistics=extra_statistics,
        )

        await self._connection.persistent_subscriptions.update(
            group_name=group_name,
            stream_name=stream_name,
            **kwargs,
            timeout=timeout if timeout is not None else self._default_deadline,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from same database position.
        """

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: Literal[False],
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from start of database.
        """

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: Literal[True],
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating subscription to run from end of database.
        """

    @overload
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        commit_position: int,
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Signature for updating persistent subscription to run from a commit position.
        """

    @retrygrpc
    @autoreconnect
    async def update_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: bool | None = None,
        commit_position: int | None = None,
        resolve_links: bool | None = None,
        consumer_strategy: ConsumerStrategy | None = None,
        message_timeout: float | None = None,
        max_retry_count: int | None = None,
        min_checkpoint_count: int | None = None,
        max_checkpoint_count: int | None = None,
        checkpoint_after: float | None = None,
        max_subscriber_count: int | None = None,
        live_buffer_size: int | None = None,
        read_batch_size: int | None = None,
        history_buffer_size: int | None = None,
        extra_statistics: bool | None = None,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Updates a persistent subscription on all streams.
        """

        info = await self.get_subscription_info(
            group_name=group_name, timeout=timeout, credentials=credentials
        )
        kwargs = info.update_all_kwargs(
            from_end=from_end,
            commit_position=commit_position,
            resolve_links=resolve_links,
            consumer_strategy=consumer_strategy,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            checkpoint_after=checkpoint_after,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            extra_statistics=extra_statistics,
        )

        await self._connection.persistent_subscriptions.update(
            group_name=group_name,
            **kwargs,
            timeout=timeout if timeout is not None else self._default_deadline,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def replay_parked_events(
        self,
        group_name: str,
        stream_name: str | None = None,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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
        stream_name: str | None = None,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
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

    @retrygrpc
    @autoreconnect
    async def create_projection(
        self,
        *,
        name: str,
        query: str,
        emit_enabled: bool = False,
        track_emitted_streams: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Creates a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.create(
            query=query,
            name=name,
            emit_enabled=emit_enabled,
            track_emitted_streams=track_emitted_streams,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def update_projection(
        self,
        name: str,
        *,
        query: str,
        emit_enabled: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Updates a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.update(
            name=name,
            query=query,
            emit_enabled=emit_enabled,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def delete_projection(
        self,
        name: str,
        *,
        delete_emitted_streams: bool = False,
        delete_state_stream: bool = False,
        delete_checkpoint_stream: bool = False,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Deletes a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.delete(
            name=name,
            delete_emitted_streams=delete_emitted_streams,
            delete_state_stream=delete_state_stream,
            delete_checkpoint_stream=delete_checkpoint_stream,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def get_projection_statistics(
        self,
        name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> ProjectionStatistics:
        """
        Gets projection statistics.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return await self._connection.projections.get_statistics(
            name=name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def list_continuous_projection_statistics(
        self,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> list[ProjectionStatistics]:
        """
        Lists statistics for continuous projections.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return await self._connection.projections.list_statistics(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def list_all_projection_statistics(
        self,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> list[ProjectionStatistics]:
        """
        Lists statistics for all projections.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return await self._connection.projections.list_statistics(
            all=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def disable_projection(
        self,
        name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Disables a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.disable(
            name=name,
            write_checkpoint=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def abort_projection(
        self,
        name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Aborts a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.disable(
            name=name,
            write_checkpoint=False,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def enable_projection(
        self,
        name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Disables a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.enable(
            name=name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def reset_projection(
        self,
        name: str,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Resets a projection.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        await self._connection.projections.reset(
            name=name,
            write_checkpoint=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    async def get_projection_state(
        self,
        name: str,
        partition: str = "",
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> ProjectionState:
        """
        Gets projection state.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return await self._connection.projections.get_state(
            name=name,
            partition=partition,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    # @retrygrpc
    # @autoreconnect
    # async def get_projection_result(
    #     self,
    #     name: str,
    #     *,
    #     timeout: Optional[float] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> ProjectionResult:
    #     """
    #     Gets projection result.
    #     """
    #     timeout = timeout if timeout is not None else self._default_deadline
    #
    #     return await self._connection.projections.get_result(
    #         name=name,
    #         partition="",
    #         timeout=timeout,
    #         metadata=self._call_metadata,
    #         credentials=credentials or self._call_credentials,
    #     )

    @retrygrpc
    @autoreconnect
    async def restart_projections_subsystem(
        self,
        *,
        timeout: float | None = None,
        credentials: grpc.CallCredentials | None = None,
    ) -> None:
        """
        Restarts projections subsystem.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return await self._connection.projections.restart_subsystem(
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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object, **kwargs: Any) -> None:
        await self.close()


class _AsyncioKurrentDBClient(AsyncKurrentDBClient):
    pass
