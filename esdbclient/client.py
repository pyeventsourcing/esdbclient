# -*- coding: utf-8 -*-
import json
import random
import sys
from functools import wraps
from threading import Event, Lock
from time import sleep
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    cast,
    overload,
)

import dns.resolver
import grpc
from grpc import CallCredentials, RpcError

from esdbclient.connection import (
    NODE_PREFERENCE_FOLLOWER,
    NODE_PREFERENCE_LEADER,
    NODE_PREFERENCE_RANDOM,
    NODE_PREFERENCE_REPLICA,
    URI_SCHEME_ESDB,
    URI_SCHEME_ESDB_DISCOVER,
    ConnectionSpec,
    ESDBConnection,
)
from esdbclient.esdbapi import (
    NODE_STATE_FOLLOWER,
    NODE_STATE_LEADER,
    NODE_STATE_REPLICA,
    BasicAuthCallCredentials,
    BatchAppendRequest,
    ClusterMember,
    SubscriptionInfo,
    SubscriptionReadRequest,
    SubscriptionReadResponse,
    handle_rpc_error,
)
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    DiscoveryFailed,
    FollowerNotFound,
    GossipSeedError,
    GrpcError,
    LeaderNotFound,
    NodeIsNotLeader,
    ReadOnlyReplicaNotFound,
    StreamNotFound,
)

# Matches the 'type' of "system" events.
ESDB_SYSTEM_EVENTS_REGEX = r"\$.+"
# Matches the 'type' of "PersistentConfig" events.
ESDB_PERSISTENT_CONFIG_EVENTS_REGEX = r"PersistentConfig\d+"

DEFAULT_EXCLUDE_FILTER = (ESDB_SYSTEM_EVENTS_REGEX, ESDB_PERSISTENT_CONFIG_EVENTS_REGEX)

_TCallable = TypeVar("_TCallable", bound=Callable[..., Any])


def autoreconnect(f: _TCallable) -> _TCallable:
    @wraps(f)
    def wrapper(self, *args, **kwargs):  # type: ignore
        assert isinstance(self, ESDBClient)
        try:
            return f(self, *args, **kwargs)

        except NodeIsNotLeader:
            if self.connection_spec.options.NodePreference == NODE_PREFERENCE_LEADER:
                self.reconnect()
                return f(self, *args, **kwargs)
            else:
                raise

        except ValueError as e:
            if "Channel closed!" in str(e):
                self.reconnect()
                return f(self, *args, **kwargs)
            else:  # pragma: no cover
                raise

    return cast(_TCallable, wrapper)


def retrygrpc(f: _TCallable) -> _TCallable:
    @wraps(f)
    def wrapper(self, *args, **kwargs):  # type: ignore
        assert isinstance(self, ESDBClient)
        try:
            return f(self, *args, **kwargs)
        except GrpcError:
            sleep(0.1)
            return f(self, *args, **kwargs)

    return cast(_TCallable, wrapper)


class ESDBClient:
    """
    Encapsulates the EventStoreDB gRPC API.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        *,
        root_certificates: Optional[str] = None,
    ) -> None:
        self._is_reconnection_required = Event()
        self._reconnection_lock = Lock()
        self.root_certificates = root_certificates
        self.connection_spec = ConnectionSpec(uri)

        self._default_deadline = self.connection_spec.options.DefaultDeadline

        self.grpc_options: Dict[str, Any] = {
            "grpc.max_receive_message_length": 17 * 1024 * 1024,
        }
        if self.connection_spec.options.KeepAliveInterval is not None:
            self.grpc_options["grpc.keepalive_ms"] = (
                self.connection_spec.options.KeepAliveInterval
            )
        if self.connection_spec.options.KeepAliveTimeout is not None:
            self.grpc_options["grpc.keepalive_timeout_ms"] = (
                self.connection_spec.options.KeepAliveTimeout
            )

        self._call_metadata = (
            ("connection-name", self.connection_spec.options.ConnectionName),
        )

        if self.connection_spec.options.Tls:
            self._call_credentials = self._construct_call_credentials(
                self.connection_spec.username, self.connection_spec.password
            )
        else:
            self._call_credentials = None

        self._connection = self._connect_to_preferred_node()

        # self._batch_append_futures_lock = Lock()
        # self._batch_append_futures_queue = BatchAppendFutureQueue()
        # self._batch_append_thread = Thread(
        #     target=self._batch_append_future_result_loop, daemon=True
        # )
        # self._batch_append_thread.start()

    def _construct_call_credentials(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[CallCredentials]:
        if username and password:
            return grpc.metadata_call_credentials(
                BasicAuthCallCredentials(username, password)
            )
        else:
            return None

    def _connect_to_preferred_node(self) -> ESDBConnection:
        # Obtain the gossip seed (a list of gRPC targets).
        if self.connection_spec.scheme == URI_SCHEME_ESDB_DISCOVER:
            assert len(self.connection_spec.targets) == 1
            cluster_fqdn = self.connection_spec.targets[0]
            answers = dns.resolver.resolve(cluster_fqdn, "A")
            gossip_seed: Sequence[str] = [f"{s.address}:2113" for s in answers]
        else:
            assert self.connection_spec.scheme == URI_SCHEME_ESDB
            gossip_seed = self.connection_spec.targets

        # Check the gossip seed isn't empty.
        if len(gossip_seed) == 0:
            raise GossipSeedError(self.connection_spec.uri)

        # Discover preferred node.
        attempts = self.connection_spec.options.MaxDiscoverAttempts
        assert attempts > 0
        while True:
            try:
                preferred, cluster_members, connection = self._discover_preferred_node(
                    gossip_seed=gossip_seed
                )
            except DiscoveryFailed as e:
                attempts -= 1
                if attempts == 0:
                    raise e
                else:
                    sleep(self.connection_spec.options.DiscoveryInterval / 1000)
            else:
                break  # coverage issue with Python 3.8 and 3.9 only, pragma: no cover

        # Maybe reconnect to preferred node.
        if len(cluster_members) > 1:  # forgive not "advertising" single node
            # Check gossip seed target matches advertised member address and port.
            grpc_target = f"{preferred.address}:{preferred.port}"
            if connection.grpc_target != grpc_target:
                # Need to connect to a different node.
                connection.close()
                connection = self._construct_connection(grpc_target)

        return connection

    def _discover_preferred_node(
        self, gossip_seed: Sequence[str]
    ) -> Tuple[ClusterMember, Sequence[ClusterMember], ESDBConnection]:
        # Iterate through the gossip seed...
        last_exception: Optional[Exception] = None
        for grpc_target in gossip_seed:
            # Construct a connection.
            connection = self._construct_connection(grpc_target)

            # Read the gossip (get cluster members).
            try:
                cluster_members = connection.gossip.read(
                    timeout=self.connection_spec.options.GossipTimeout,
                    metadata=self._call_metadata,
                    credentials=self._call_credentials,
                )
            except GrpcError as e:
                last_exception = e
                connection.close()
            else:
                break
        else:
            msg = f"Failed to read from gossip seed: {gossip_seed}"
            raise DiscoveryFailed(msg) from last_exception

        # Select a node according to node preference.
        node_preference = self.connection_spec.options.NodePreference
        if node_preference == NODE_PREFERENCE_LEADER:
            leaders = [c for c in cluster_members if c.state == NODE_STATE_LEADER]
            if len(leaders) != 1:  # pragma: no cover
                # Todo: Somehow cover this with a test.
                raise LeaderNotFound(f"Expected one leader, discovered {len(leaders)}")
            cluster_member = leaders[0]
        elif node_preference == NODE_PREFERENCE_FOLLOWER:
            followers = [c for c in cluster_members if c.state == NODE_STATE_FOLLOWER]
            if len(followers) == 0:
                raise FollowerNotFound()
            cluster_member = random.choice(followers)
        elif node_preference == NODE_PREFERENCE_REPLICA:
            replicas = [c for c in cluster_members if c.state == NODE_STATE_REPLICA]
            if len(replicas) == 0:
                raise ReadOnlyReplicaNotFound()
            # Todo: Somehow cover this with a test (how to setup a read-only replica?)
            cluster_member = random.choice(replicas)  # pragma: no cover
        else:
            assert node_preference == NODE_PREFERENCE_RANDOM
            assert len(cluster_members) > 0
            cluster_member = random.choice(cluster_members)
        return cluster_member, cluster_members, connection

    def reconnect(self) -> None:
        self._is_reconnection_required.set()
        with self._reconnection_lock:
            if self._is_reconnection_required.is_set():
                new = self._connect_to_preferred_node()
                old, self._connection = self._connection, new
                old.close()
            else:  # pragma: no cover
                # Todo: Test with concurrent writes to wrong node state.
                pass

    def _construct_connection(self, grpc_target: str) -> ESDBConnection:
        grpc_options: Tuple[Tuple[str, str], ...] = tuple(self.grpc_options.items())
        if self.connection_spec.options.Tls is True:
            if self.root_certificates is None:
                raise ValueError("root_certificates is required for secure connection")

            assert self.connection_spec.username
            assert self.connection_spec.password
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=self.root_certificates.encode()
            )
            grpc_channel = grpc.secure_channel(
                target=grpc_target,
                credentials=channel_credentials,
                options=grpc_options,
            )
        else:
            grpc_channel = grpc.insecure_channel(
                target=grpc_target, options=grpc_options
            )

        return ESDBConnection(grpc_channel=grpc_channel, grpc_target=grpc_target)

    # def _batch_append_future_result_loop(self) -> None:
    #     # while self._channel_connectivity_state is not ChannelConnectivity.SHUTDOWN:
    #     try:
    #         self._connection.streams.batch_append_multiplexed(
    #             futures_queue=self._batch_append_futures_queue,
    #             timeout=None,
    #             metadata=self._call_metadata,
    #             credentials=self._call_credentials,
    #         )
    #     except ESDBClientException as e:
    #         self._clear_batch_append_futures_queue(e)
    #     else:
    #         self._clear_batch_append_futures_queue(  # pragma: no cover
    #             ESDBClientException("Request not sent")
    #         )
    #     # print("Looping on call to batch_append_multiplexed()....")
    #
    # def _clear_batch_append_futures_queue(self, error: ESDBClientException) -> None:
    #     with self._batch_append_futures_lock:
    #         try:
    #             while True:
    #                 future = self._batch_append_futures_queue.get(block=False)
    #                 future.set_exception(error)  # pragma: no cover
    #         except Empty:
    #             pass
    #
    # def append_events_multiplexed(
    #     self,
    #     stream_name: str,
    #     expected_position: Optional[int],
    #     events: Iterable[NewEvent],
    #     timeout: Optional[float] = None,
    # ) -> int:
    #     timeout = timeout if timeout is not None else self._default_deadline
    #     batch_append_request = BatchAppendRequest(
    #         stream_name=stream_name, expected_position=expected_position, events=events
    #     )
    #     future = BatchAppendFuture(batch_append_request=batch_append_request)
    #     with self._batch_append_futures_lock:
    #         self._batch_append_futures_queue.put(future)
    #     response = future.result(timeout=timeout)
    #     assert isinstance(response.commit_position, int)
    #     return response.commit_position

    @retrygrpc
    @autoreconnect
    def append_events(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        timeout = timeout if timeout is not None else self._default_deadline
        return self._connection.streams.batch_append(
            BatchAppendRequest(
                stream_name=stream_name,
                expected_position=expected_position,
                events=events,
            ),
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        ).commit_position

    @retrygrpc
    @autoreconnect
    def append_event(
        self,
        stream_name: str,
        expected_position: Optional[int],
        event: NewEvent,
        timeout: Optional[float] = None,
    ) -> int:
        """
        Appends a new event to the named stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline
        return self._connection.streams.append(
            stream_name=stream_name,
            expected_position=expected_position,
            events=[event],
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def delete_stream(
        self,
        stream_name: str,
        expected_position: Optional[int],
        timeout: Optional[float] = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline
        self._connection.streams.delete(
            stream_name=stream_name,
            expected_position=expected_position,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def tombstone_stream(
        self,
        stream_name: str,
        expected_position: Optional[int],
        timeout: Optional[float] = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline
        self._connection.streams.tombstone(
            stream_name=stream_name,
            expected_position=expected_position,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_stream_events(
        self,
        stream_name: str,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Reads recorded events from the named stream.
        """
        return self._connection.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            limit=limit,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_all_events(
        self,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Reads recorded events in "all streams" in the database.
        """
        return self._connection.streams.read(
            commit_position=commit_position,
            backwards=backwards,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            limit=limit,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def get_stream_position(
        self,
        stream_name: str,
        timeout: Optional[float] = None,
    ) -> Optional[int]:
        """
        Returns the current position of the end of a stream.
        """
        try:
            last_event = list(
                self._connection.streams.read(
                    stream_name=stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                    metadata=self._call_metadata,
                    credentials=self._call_credentials,
                )
            )[0]
        except StreamNotFound:
            return None
        else:
            return last_event.stream_position

    @retrygrpc
    @autoreconnect
    def get_commit_position(
        self,
        timeout: Optional[float] = None,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
    ) -> int:
        """
        Returns the current commit position of the database.
        """
        recorded_events = self.read_all_events(
            backwards=True,
            filter_exclude=filter_exclude,
            limit=1,
            timeout=timeout,
        )
        commit_position = 0
        for ev in recorded_events:
            assert ev.commit_position is not None
            commit_position = ev.commit_position
        return commit_position

    @retrygrpc
    @autoreconnect
    def get_stream_metadata(
        self, stream_name: str, timeout: Optional[float] = None
    ) -> Tuple[Dict[str, Any], Optional[int]]:
        """
        Gets the stream metadata.
        """
        metadata_stream_name = f"$${stream_name}"
        try:
            metadata_events = list(
                self.read_stream_events(
                    stream_name=metadata_stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                )
            )
        except StreamNotFound:
            return {}, None
        else:
            metadata_event = metadata_events[0]
            return json.loads(metadata_event.data), metadata_event.stream_position

    def set_stream_metadata(
        self,
        stream_name: str,
        metadata: Dict[str, Any],
        expected_position: Optional[int] = -1,
        timeout: Optional[float] = None,
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
        self.append_event(
            stream_name=metadata_stream_name,
            expected_position=expected_position,
            event=metadata_event,
            timeout=timeout,
        )

    @retrygrpc
    @autoreconnect
    def subscribe_all_events(
        self,
        commit_position: Optional[int] = None,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> Iterator[RecordedEvent]:
        """
        Starts a catch-up subscription, from which all
        recorded events in the database can be received.
        """
        read_resp = self._connection.streams.read(
            commit_position=commit_position,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            subscribe=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )
        return CatchupSubscription(read_resp=read_resp)

    @retrygrpc
    @autoreconnect
    def subscribe_stream_events(
        self,
        stream_name: str,
        stream_position: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> Iterator[RecordedEvent]:
        """
        Starts a catch-up subscription from which
        recorded events in a stream can be received.
        """
        read_resp = self._connection.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            subscribe=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )
        return CatchupSubscription(read_resp=read_resp)

    @overload
    def create_subscription(
        self,
        group_name: str,
        *,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from start of database.
        """

    @overload
    def create_subscription(
        self,
        group_name: str,
        *,
        commit_position: int,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from a commit position.
        """

    @overload
    def create_subscription(
        self,
        group_name: str,
        *,
        from_end: bool = True,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from end of database.
        """

    @retrygrpc
    @autoreconnect
    def create_subscription(
        self,
        group_name: str,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> None:
        """
        Creates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._connection.persistent_subscriptions.create(
            group_name=group_name,
            from_end=from_end,
            commit_position=commit_position,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @overload
    def create_stream_subscription(
        self,
        group_name: str,
        stream_name: str,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signature for creating stream subscription from start of stream.
        """

    @overload
    def create_stream_subscription(
        self,
        group_name: str,
        stream_name: str,
        *,
        stream_position: int,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signature for creating stream subscription from stream position.
        """

    @overload
    def create_stream_subscription(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = True,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Signature for creating stream subscription from end of stream.
        """

    @retrygrpc
    @autoreconnect
    def create_stream_subscription(
        self,
        group_name: str,
        stream_name: str,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Creates a persistent subscription on one stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._connection.persistent_subscriptions.create(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            stream_position=stream_position,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_subscription(
        self, group_name: str, timeout: Optional[float] = None
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        """
        Reads a persistent subscription on all streams.
        """
        return self._connection.persistent_subscriptions.read(
            group_name=group_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_stream_subscription(
        self, group_name: str, stream_name: str, timeout: Optional[float] = None
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        """
        Reads a persistent subscription on one stream.
        """
        return self._connection.persistent_subscriptions.read(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def get_subscription_info(
        self, group_name: str, timeout: Optional[float] = None
    ) -> SubscriptionInfo:
        """
        Gets info for a persistent subscription.
        """
        return self._connection.persistent_subscriptions.get_info(
            group_name=group_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def get_stream_subscription_info(
        self, group_name: str, stream_name: str, timeout: Optional[float] = None
    ) -> SubscriptionInfo:
        """
        Gets info for a persistent subscription.
        """
        return self._connection.persistent_subscriptions.get_info(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def list_subscriptions(
        self, timeout: Optional[float] = None
    ) -> Sequence[SubscriptionInfo]:
        """
        Lists all persistent subscriptions.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return self._connection.persistent_subscriptions.list(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def list_stream_subscriptions(
        self, stream_name: str, timeout: Optional[float] = None
    ) -> Sequence[SubscriptionInfo]:
        """
        Lists persistent stream subscriptions.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        return self._connection.persistent_subscriptions.list(
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def delete_subscription(
        self, group_name: str, timeout: Optional[float] = None
    ) -> None:
        """
        Creates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._connection.persistent_subscriptions.delete(
            group_name=group_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def delete_stream_subscription(
        self, group_name: str, stream_name: str, timeout: Optional[float] = None
    ) -> None:
        """
        Creates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._connection.persistent_subscriptions.delete(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_gossip(self, timeout: Optional[float] = None) -> Sequence[ClusterMember]:
        timeout = (
            timeout
            if timeout is not None
            else self.connection_spec.options.GossipTimeout
        )
        return self._connection.gossip.read(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_cluster_gossip(
        self, timeout: Optional[float] = None
    ) -> Sequence[ClusterMember]:
        timeout = (
            timeout
            if timeout is not None
            else self.connection_spec.options.GossipTimeout
        )
        return self._connection.cluster_gossip.read(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=self._call_credentials,
        )

    def close(self) -> None:
        """
        Closes the gRPC channel.
        """
        try:
            c = self._connection
        except AttributeError:
            pass
        else:
            c.close()

    def __del__(self) -> None:
        self.close()


class CatchupSubscription(Iterator[RecordedEvent]):
    """
    Encapsulates read response for a catch-up subscription.
    """

    def __init__(
        self,
        read_resp: Iterable[RecordedEvent],
    ):
        self.read_resp = iter(read_resp)

    def __iter__(self) -> Iterator[RecordedEvent]:
        return self

    def __next__(self) -> RecordedEvent:
        try:
            return next(self.read_resp)
        except RpcError as e:
            raise handle_rpc_error(e) from e  # pragma: no cover
