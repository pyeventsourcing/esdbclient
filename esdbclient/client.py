# -*- coding: utf-8 -*-
import json
import re
import sys
from queue import Empty
from threading import Lock, Thread
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Optional,
    Sequence,
    Tuple,
    Union,
    overload,
)

import grpc
from grpc import ChannelConnectivity, RpcError

from esdbclient.esdbapi import (
    BasicAuthCallCredentials,
    BatchAppendFuture,
    BatchAppendFutureQueue,
    BatchAppendRequest,
    Streams,
    SubscriptionReadRequest,
    SubscriptionReadResponse,
    Subscriptions,
    handle_rpc_error,
)
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import ESDBClientException, StreamNotFound

# Matches the 'type' of "system" events.
ESDB_SYSTEM_EVENTS_REGEX = r"\$.+"
# Matches the 'type' of "PersistentConfig" events.
ESDB_PERSISTENT_CONFIG_EVENTS_REGEX = r"PersistentConfig\d+"

DEFAULT_EXCLUDE_FILTER = (ESDB_SYSTEM_EVENTS_REGEX, ESDB_PERSISTENT_CONFIG_EVENTS_REGEX)


class ESDBClient:
    """
    Encapsulates the EventStoreDB gRPC API.
    """

    def __init__(
        self,
        uri: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[int, str]] = None,
        server_cert: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        # Todo: look at connection string for "tls=true" or "tls=false"

        if isinstance(uri, (str, bytes)) and re.match(pattern=r"^\w+:\d+$", string=uri):
            self.grpc_target = uri
        elif host is not None:
            assert port is not None
            self.grpc_target = f"{host}:{port}"
        else:
            raise ValueError("Invalid constructor args")
        if server_cert is None:
            self._channel = grpc.insecure_channel(target=self.grpc_target)
            self._call_credentials = None
        else:
            assert username is not None
            assert password is not None
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=server_cert.encode()
            )
            self._channel = grpc.secure_channel(
                target=self.grpc_target, credentials=channel_credentials
            )
            self._call_credentials = grpc.metadata_call_credentials(
                BasicAuthCallCredentials(username, password)
            )
        self._channel.subscribe(self._receive_channel_connectivity_state)
        self._channel_connectivity_state: Optional[ChannelConnectivity] = None
        self._streams = Streams(self._channel)
        self._subscriptions = Subscriptions(self._channel)

        self._batch_append_futures_lock = Lock()
        self._batch_append_futures_queue = BatchAppendFutureQueue()
        self._batch_append_thread = Thread(
            target=self._batch_append_future_result_loop, daemon=True
        )
        self._batch_append_thread.start()

    def _receive_channel_connectivity_state(
        self, connectivity: ChannelConnectivity
    ) -> None:
        self._channel_connectivity_state = connectivity
        # print("Channel connectivity state:", connectivity)

    def _batch_append_future_result_loop(self) -> None:
        # while self._channel_connectivity_state is not ChannelConnectivity.SHUTDOWN:
        try:
            self._streams.batch_append_multiplexed(
                futures_queue=self._batch_append_futures_queue,
                timeout=None,
                credentials=self._call_credentials,
            )
        except ESDBClientException as e:
            self._clear_batch_append_futures_queue(e)
        else:
            self._clear_batch_append_futures_queue(  # pragma: no cover
                ESDBClientException("Request not sent")
            )
        # print("Looping on call to batch_append_multiplexed()....")

    def _clear_batch_append_futures_queue(self, error: ESDBClientException) -> None:
        with self._batch_append_futures_lock:
            try:
                while True:
                    future = self._batch_append_futures_queue.get(block=False)
                    future.set_exception(error)  # pragma: no cover
            except Empty:
                pass

    def append_events_multiplexed(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        batch_append_request = BatchAppendRequest(
            stream_name=stream_name, expected_position=expected_position, events=events
        )
        future = BatchAppendFuture(batch_append_request=batch_append_request)
        with self._batch_append_futures_lock:
            self._batch_append_futures_queue.put(future)
        response = future.result(timeout=timeout)
        assert isinstance(response.commit_position, int)
        return response.commit_position

    def append_events(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        return self._streams.batch_append(
            BatchAppendRequest(
                stream_name=stream_name,
                expected_position=expected_position,
                events=events,
            ),
            timeout=timeout,
            credentials=self._call_credentials,
        ).commit_position

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
        return self._streams.append(
            stream_name=stream_name,
            expected_position=expected_position,
            events=[event],
            timeout=timeout,
            credentials=self._call_credentials,
        )

    def delete_stream(self, stream_name: str, expected_position: Optional[int]) -> None:
        self._streams.delete(
            stream_name=stream_name, expected_position=expected_position
        )

    def tombstone_stream(
        self, stream_name: str, expected_position: Optional[int]
    ) -> None:
        self._streams.tombstone(
            stream_name=stream_name, expected_position=expected_position
        )

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
        return self._streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            backwards=backwards,
            limit=limit,
            timeout=timeout,
            credentials=self._call_credentials,
        )

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
        return self._streams.read(
            commit_position=commit_position,
            backwards=backwards,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            limit=limit,
            timeout=timeout,
            credentials=self._call_credentials,
        )

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
                self._streams.read(
                    stream_name=stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                    credentials=self._call_credentials,
                )
            )[0]
        except StreamNotFound:
            return None
        else:
            return last_event.stream_position

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
        read_resp = self._streams.read(
            commit_position=commit_position,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            subscribe=True,
            timeout=timeout,
            credentials=self._call_credentials,
        )
        return CatchupSubscription(read_resp=read_resp)

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
        read_resp = self._streams.read(
            stream_name=stream_name,
            stream_position=stream_position,
            subscribe=True,
            timeout=timeout,
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
        self._subscriptions.create(
            group_name=group_name,
            from_end=from_end,
            commit_position=commit_position,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            timeout=timeout,
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
        self._subscriptions.create(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            stream_position=stream_position,
            timeout=timeout,
            credentials=self._call_credentials,
        )

    def read_subscription(
        self, group_name: str, timeout: Optional[float] = None
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        """
        Reads a persistent subscription on all streams.
        """
        return self._subscriptions.read(
            group_name=group_name,
            timeout=timeout,
            credentials=self._call_credentials,
        )

    def read_stream_subscription(
        self, group_name: str, stream_name: str, timeout: Optional[float] = None
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        """
        Reads a persistent subscription on one stream.
        """
        return self._subscriptions.read(
            group_name=group_name,
            stream_name=stream_name,
            timeout=timeout,
            credentials=self._call_credentials,
        )

    def close(self) -> None:
        """
        Closes the gRPC channel.
        """
        self._channel.unsubscribe(self._receive_channel_connectivity_state)
        self._channel.close()

    def __del__(self) -> None:
        # print("Client deleted")
        if hasattr(self, "_channel"):
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
