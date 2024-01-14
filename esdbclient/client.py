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
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

import dns.exception
import dns.resolver
import grpc
from typing_extensions import Literal

from esdbclient.common import (
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
    DEFAULT_WINDOW_SIZE,
    BasicAuthCallCredentials,
)
from esdbclient.connection import ESDBConnection
from esdbclient.connection_spec import (
    NODE_PREFERENCE_FOLLOWER,
    NODE_PREFERENCE_LEADER,
    NODE_PREFERENCE_RANDOM,
    NODE_PREFERENCE_REPLICA,
    URI_SCHEME_ESDB,
    URI_SCHEME_ESDB_DISCOVER,
    ConnectionSpec,
)
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    DiscoveryFailed,
    DNSError,
    FollowerNotFound,
    GossipSeedError,
    GrpcError,
    LeaderNotFound,
    NodeIsNotLeader,
    NotFound,
    ReadOnlyReplicaNotFound,
    ServiceUnavailable,
)
from esdbclient.gossip import (
    NODE_STATE_FOLLOWER,
    NODE_STATE_LEADER,
    NODE_STATE_REPLICA,
    ClusterMember,
)
from esdbclient.persistent import (
    ConsumerStrategy,
    PersistentSubscription,
    SubscriptionInfo,
)
from esdbclient.streams import CatchupSubscription, ReadResponse, StreamState

# Matches the 'type' of "system" events.
ESDB_SYSTEM_EVENTS_REGEX = r"\$.+"
# Matches the 'type' of "PersistentConfig" events.
ESDB_PERSISTENT_CONFIG_EVENTS_REGEX = r"PersistentConfig\d+"

DEFAULT_EXCLUDE_FILTER = (ESDB_SYSTEM_EVENTS_REGEX, ESDB_PERSISTENT_CONFIG_EVENTS_REGEX)

_TCallable = TypeVar("_TCallable", bound=Callable[..., Any])


def autoreconnect(f: _TCallable) -> _TCallable:
    @wraps(f)
    def autoreconnect_decorator(
        client: "EventStoreDBClient", *args: Any, **kwargs: Any
    ) -> Any:
        try:
            return f(client, *args, **kwargs)

        except NodeIsNotLeader:
            if client.connection_spec.options.NodePreference == NODE_PREFERENCE_LEADER:
                client.reconnect()
                sleep(0.1)
                return f(client, *args, **kwargs)
            else:
                raise

        except ValueError as e:
            if "Channel closed!" in str(e):
                client.reconnect()
                sleep(0.1)
                return f(client, *args, **kwargs)
            else:  # pragma: no cover
                raise

        except ServiceUnavailable:
            client.reconnect()
            sleep(0.1)
            return f(client, *args, **kwargs)

    return cast(_TCallable, autoreconnect_decorator)


def retrygrpc(f: _TCallable) -> _TCallable:
    @wraps(f)
    def retrygrpc_decorator(*args: Any, **kwargs: Any) -> Any:
        try:
            return f(*args, **kwargs)
        except GrpcError:
            sleep(0.1)
            return f(*args, **kwargs)

    return cast(_TCallable, retrygrpc_decorator)


class BaseEventStoreDBClient:
    def __init__(
        self,
        uri: Optional[str] = None,
        *,
        root_certificates: Optional[str] = None,
    ) -> None:
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
            self._call_credentials = self.construct_call_credentials(
                self.connection_spec.username, self.connection_spec.password
            )
        else:
            self._call_credentials = None

    def construct_call_credentials(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[grpc.CallCredentials]:
        if username and password:
            return grpc.metadata_call_credentials(
                BasicAuthCallCredentials(username, password)
            )
        else:
            return None


class EventStoreDBClient(BaseEventStoreDBClient):
    """
    Encapsulates the EventStoreDB gRPC API.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        *,
        root_certificates: Optional[str] = None,
    ) -> None:
        self._is_closed = False
        super().__init__(uri, root_certificates=root_certificates)
        self._is_reconnection_required = Event()
        self._reconnection_lock = Lock()
        self._esdb = self._connect_to_preferred_node()

        # self._batch_append_futures_lock = Lock()
        # self._batch_append_futures_queue = BatchAppendFutureQueue()
        # self._batch_append_thread = Thread(
        #     target=self._batch_append_future_result_loop, daemon=True
        # )
        # self._batch_append_thread.start()

    def _connect_to_preferred_node(self) -> ESDBConnection:
        # Obtain the gossip seed (a list of gRPC targets).
        if self.connection_spec.scheme == URI_SCHEME_ESDB_DISCOVER:
            assert len(self.connection_spec.targets) == 1
            cluster_fqdn, _, port = self.connection_spec.targets[0].partition(":")
            if port == "":
                port = "2113"
            try:
                answers = dns.resolver.resolve(cluster_fqdn, "A")
            except dns.exception.DNSException as e:
                raise DNSError() from e
            gossip_seed: Sequence[str] = [f"{s.address}:{port}" for s in answers]
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
                connection = self._construct_esdb_connection(grpc_target)

        return connection

    def _discover_preferred_node(
        self, gossip_seed: Sequence[str]
    ) -> Tuple[ClusterMember, Sequence[ClusterMember], ESDBConnection]:
        # Iterate through the gossip seed...
        last_exception: Optional[Exception] = None
        for grpc_target in gossip_seed:
            # Construct a connection.
            connection = self._construct_esdb_connection(grpc_target)

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
                new_conn = self._connect_to_preferred_node()
                old_conn, self._esdb = self._esdb, new_conn
                old_conn.close()
                self._is_reconnection_required.clear()
            else:  # pragma: no cover
                # Todo: Test with concurrent writes to wrong node state.
                pass

    def _construct_esdb_connection(self, grpc_target: str) -> ESDBConnection:
        return ESDBConnection(
            grpc_channel=self._construct_grpc_channel(grpc_target),
            grpc_target=grpc_target,
            connection_spec=self.connection_spec,
        )

    def _construct_grpc_channel(self, grpc_target: str) -> grpc.Channel:
        grpc_options: Tuple[Tuple[str, str], ...] = tuple(self.grpc_options.items())
        if self.connection_spec.options.Tls is True:
            if not self.connection_spec.username or not self.connection_spec.password:
                raise ValueError("username and password are required")
            if self.root_certificates is None:
                root_certificates = None
            else:
                root_certificates = self.root_certificates.encode()
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certificates
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
        return grpc_channel

    # def _batch_append_future_result_loop(self) -> None:
    #     # while self._channel_connectivity_state is not ChannelConnectivity.SHUTDOWN:
    #     try:
    #         self._connection.streams.batch_append_multiplexed(
    #             futures_queue=self._batch_append_futures_queue,
    #             timeout=None,
    #             metadata=self._call_metadata,
    #             credentials=credentials or self._call_credentials,
    #         )
    #     except EventStoreDBClientException as e:
    #         self._clear_batch_append_futures_queue(e)
    #     else:
    #         self._clear_batch_append_futures_queue(  # pragma: no cover
    #             EventStoreDBClientException("Request not sent")
    #         )
    #     # print("Looping on call to batch_append_multiplexed()....")
    #
    # def _clear_batch_append_futures_queue(self, error: EventStoreDBClientException) -> None:
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
    #     current_version: Optional[int],
    #     events: Iterable[NewEvent],
    #     timeout: Optional[float] = None,
    # ) -> int:
    #     timeout = timeout if timeout is not None else self._default_deadline
    #     batch_append_request = BatchAppendRequest(
    #         stream_name=stream_name, current_version=current_version, events=events
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
        *,
        current_version: Union[int, StreamState],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        timeout = timeout if timeout is not None else self._default_deadline
        return self._esdb.streams.batch_append(
            stream_name=stream_name,
            current_version=current_version,
            events=events,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def append_event(
        self,
        stream_name: str,
        *,
        current_version: Union[int, StreamState],
        event: NewEvent,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        """
        Appends a new event to the named stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline
        return self._esdb.streams.append(
            stream_name=stream_name,
            current_version=current_version,
            events=[event],
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    def append_to_stream(
        self,
        stream_name: str,
        *,
        current_version: Union[int, StreamState],
        events: Union[NewEvent, Sequence[NewEvent]],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        """
        Appends a new event or a sequence of new events to the named stream.
        """
        if isinstance(events, NewEvent):
            return self.append_event(
                stream_name=stream_name,
                current_version=current_version,
                event=events,
                timeout=timeout,
                credentials=credentials,
            )
        else:
            return self.append_events(
                stream_name=stream_name,
                current_version=current_version,
                events=events,
                timeout=timeout,
                credentials=credentials,
            )

    @retrygrpc
    @autoreconnect
    def delete_stream(
        self,
        stream_name: str,
        *,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        # Todo: Reconsider using current_version=None to indicate "stream exists"?
        timeout = timeout if timeout is not None else self._default_deadline
        self._esdb.streams.delete(
            stream_name=stream_name,
            current_version=current_version,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def tombstone_stream(
        self,
        stream_name: str,
        *,
        current_version: Union[int, StreamState],
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline
        self._esdb.streams.tombstone(
            stream_name=stream_name,
            current_version=current_version,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def get_stream(
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
        Returns a sequence of recorded events from the named stream.
        """
        return tuple(
            self.read_stream(
                stream_name=stream_name,
                stream_position=stream_position,
                backwards=backwards,
                resolve_links=resolve_links,
                limit=limit,
                timeout=timeout,
                credentials=credentials or self._call_credentials,
            )
        )

    def read_stream(
        self,
        stream_name: str,
        *,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        resolve_links: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> ReadResponse:
        """
        Reads recorded events from the named stream.
        """
        return self._esdb.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            resolve_links=resolve_links,
            limit=limit,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    def read_all(
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
    ) -> ReadResponse:
        """
        Reads recorded events in "all streams" in the database.
        """
        return self._esdb.streams.read(
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
    def get_current_version(
        self,
        stream_name: str,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Union[int, Literal[StreamState.NO_STREAM]]:
        """
        Returns the current position of the end of a stream.
        """
        try:
            last_event = list(
                self._esdb.streams.read(
                    stream_name=stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                    metadata=self._call_metadata,
                    credentials=credentials or self._call_credentials,
                )
            )[0]
        except NotFound:
            # StreamState.NO_STREAM is the correct "current version" both when appending
            # to a stream that never existed and when appending to a stream that has
            # been deleted (in this case of a deleted stream, the "current version"
            # before deletion is also correct).
            return StreamState.NO_STREAM
        else:
            return last_event.stream_position

    @retrygrpc
    @autoreconnect
    def get_commit_position(
        self,
        *,
        filter_exclude: Sequence[str] = DEFAULT_EXCLUDE_FILTER,
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> int:
        """
        Returns the current commit position of the database.
        """
        recorded_events = self.read_all(
            backwards=True,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            limit=1,
            timeout=timeout,
            credentials=credentials,
        )
        commit_position = 0
        for ev in recorded_events:
            assert ev.commit_position is not None
            commit_position = ev.commit_position
        return commit_position

    @retrygrpc
    @autoreconnect
    def get_stream_metadata(
        self,
        stream_name: str,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Tuple[Dict[str, Any], Union[int, Literal[StreamState.NO_STREAM]]]:
        """
        Gets the stream metadata.
        """
        metadata_stream_name = f"$${stream_name}"
        try:
            metadata_events = list(
                self.get_stream(
                    stream_name=metadata_stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                    credentials=credentials or self._call_credentials,
                )
            )
        except NotFound:
            return {}, StreamState.NO_STREAM
        else:
            metadata_event = metadata_events[0]
            return json.loads(metadata_event.data), metadata_event.stream_position

    def set_stream_metadata(
        self,
        stream_name: str,
        *,
        metadata: Dict[str, Any],
        current_version: Union[int, StreamState] = StreamState.ANY,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
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
            current_version=current_version,
            event=metadata_event,
            timeout=timeout,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def subscribe_to_all(
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
    ) -> CatchupSubscription:
        """
        Starts a catch-up subscription, from which all
        recorded events in the database can be received.
        """
        return self._esdb.streams.read(
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
    def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> CatchupSubscription:
        """
        Signature to start catch-up subscription from the start of the stream.
        """

    @overload
    def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> CatchupSubscription:
        """
        Signature to start catch-up subscription from a particular stream position.
        """

    @overload
    def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        from_end: Literal[True] = True,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> CatchupSubscription:
        """
        Signature to start catch-up subscription from the end of the stream.
        """

    @retrygrpc
    @autoreconnect
    def subscribe_to_stream(
        self,
        stream_name: str,
        *,
        stream_position: Optional[int] = None,
        from_end: bool = False,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> CatchupSubscription:
        """
        Starts a catch-up subscription from which
        recorded events in a stream can be received.
        """
        return self._esdb.streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            from_end=from_end,
            resolve_links=resolve_links,
            subscribe=True,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    def create_subscription_to_all(
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
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from start of database.
        """

    @overload
    def create_subscription_to_all(
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
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from a commit position.
        """

    @overload
    def create_subscription_to_all(
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
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating persistent subscription from end of database.
        """

    @retrygrpc
    @autoreconnect
    def create_subscription_to_all(
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
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Creates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._esdb.persistent_subscriptions.create(
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
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating stream subscription from start of stream.
        """

    @overload
    def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating stream subscription from stream position.
        """

    @overload
    def create_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating stream subscription from end of stream.
        """

    @retrygrpc
    @autoreconnect
    def create_subscription_to_stream(
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
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Creates a persistent subscription on one stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._esdb.persistent_subscriptions.create(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            stream_position=stream_position,
            resolve_links=resolve_links,
            consumer_strategy=consumer_strategy,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_subscription_to_all(
        self,
        group_name: str,
        *,
        event_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> PersistentSubscription:
        """
        Reads a persistent subscription on all streams.
        """
        return self._esdb.persistent_subscriptions.read(
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
    def read_subscription_to_stream(
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
    ) -> PersistentSubscription:
        """
        Reads a persistent subscription on one stream.
        """
        return self._esdb.persistent_subscriptions.read(
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
    def get_subscription_info(
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
        return self._esdb.persistent_subscriptions.get_info(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def list_subscriptions(
        self,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[SubscriptionInfo]:
        """
        Lists all persistent subscriptions.
        """
        return self._esdb.persistent_subscriptions.list(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def list_subscriptions_to_stream(
        self,
        stream_name: str,
        *,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[SubscriptionInfo]:
        """
        Lists persistent stream subscriptions.
        """
        return self._esdb.persistent_subscriptions.list(
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    def update_subscription_to_all(
        self,
        group_name: str,
        *,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating persistent subscription from start of database.
        """

    @overload
    def update_subscription_to_all(
        self,
        group_name: str,
        *,
        commit_position: int,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating persistent subscription from a commit position.
        """

    @overload
    def update_subscription_to_all(
        self,
        group_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating persistent subscription from end of database.
        """

    @retrygrpc
    @autoreconnect
    def update_subscription_to_all(
        self,
        group_name: str,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Updates a persistent subscription on all streams.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._esdb.persistent_subscriptions.update(
            group_name=group_name,
            from_end=from_end,
            commit_position=commit_position,
            resolve_links=resolve_links,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @overload
    def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating stream subscription from start of stream.
        """

    @overload
    def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        stream_position: int,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating stream subscription from stream position.
        """

    @overload
    def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        *,
        from_end: bool = True,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating stream subscription from end of stream.
        """

    @retrygrpc
    @autoreconnect
    def update_subscription_to_stream(
        self,
        group_name: str,
        stream_name: str,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Updates a persistent subscription on one stream.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._esdb.persistent_subscriptions.update(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            stream_position=stream_position,
            resolve_links=resolve_links,
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            max_subscriber_count=max_subscriber_count,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def replay_parked_events(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        timeout = timeout if timeout is not None else self._default_deadline

        self._esdb.persistent_subscriptions.replay_parked(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def delete_subscription(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Deletes a persistent subscription.
        """
        timeout = timeout if timeout is not None else self._default_deadline

        self._esdb.persistent_subscriptions.delete(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    @retrygrpc
    @autoreconnect
    def read_gossip(
        self,
        timeout: Optional[float] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> Sequence[ClusterMember]:
        timeout = (
            timeout
            if timeout is not None
            else self.connection_spec.options.GossipTimeout
        )
        return self._esdb.gossip.read(
            timeout=timeout,
            metadata=self._call_metadata,
            credentials=credentials or self._call_credentials,
        )

    # Getting 'AccessDenied' with ESDB v23.10.
    # @retrygrpc
    # @autoreconnect
    # def read_cluster_gossip(
    #     self,
    #     timeout: Optional[float] = None,
    #     credentials: Optional[grpc.CallCredentials] = None,
    # ) -> Sequence[ClusterMember]:
    #     timeout = (
    #         timeout
    #         if timeout is not None
    #         else self.connection_spec.options.GossipTimeout
    #     )
    #     return self._connection.cluster_gossip.read(
    #         timeout=timeout,
    #         metadata=self._call_metadata,
    #         credentials=credentials or self._call_credentials,
    #     )

    def close(self) -> None:
        """
        Closes the gRPC channel.
        """
        if not self._is_closed:
            try:
                esdb_connection = self._esdb
            except AttributeError:
                pass
            else:
                esdb_connection.close()
                self._is_closed = True

    def __del__(self) -> None:
        self.close()
