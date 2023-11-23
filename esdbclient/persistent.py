# -*- coding: utf-8 -*-
import queue
from abc import abstractmethod
from dataclasses import dataclass
from threading import Event
from time import sleep
from typing import Any, Iterator, List, Optional, Sequence, Tuple, Union, overload
from uuid import UUID

import grpc
from typing_extensions import Literal, Protocol, runtime_checkable

from esdbclient.common import (
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_WINDOW_SIZE,
    ESDBService,
    Metadata,
    construct_filter_exclude_regex,
    construct_filter_include_regex,
    construct_recorded_event,
    handle_rpc_error,
)
from esdbclient.connection_spec import ConnectionSpec
from esdbclient.events import RecordedEvent
from esdbclient.exceptions import (
    CancelledByClient,
    EventStoreDBClientException,
    NodeIsNotLeader,
    SubscriptionConfirmationError,
)
from esdbclient.protos.Grpc import persistent_pb2, persistent_pb2_grpc, shared_pb2


@runtime_checkable
class _ReadResps(Iterator[persistent_pb2.ReadResp], Protocol):
    @abstractmethod
    def cancel(self) -> None:
        ...  # pragma: no cover


ConsumerStrategy = Literal[
    "DispatchToSingle", "RoundRobin", "Pinned", "PinnedByCorrelation"
]


class BaseSubscriptionReadReqs:
    def __init__(
        self, group_name: str, stream_name: Optional[str] = None, buffer_size: int = 100
    ) -> None:
        self.group_name = group_name
        self.stream_name = stream_name
        assert isinstance(buffer_size, int) and buffer_size > 0, buffer_size
        self._server_buffer_size = buffer_size
        self._ack_buffer_size = min(buffer_size, 100)
        self._has_requested_options = False

    def _construct_initial_read_req(self) -> persistent_pb2.ReadReq:
        options = persistent_pb2.ReadReq.Options(
            group_name=self.group_name,
            buffer_size=self._server_buffer_size,
            uuid_option=persistent_pb2.ReadReq.Options.UUIDOption(
                # structured=shared_pb2.Empty(),
                string=shared_pb2.Empty(),
            ),
        )
        # Decide 'stream_option'.
        if isinstance(self.stream_name, str):
            options.stream_identifier.CopyFrom(
                shared_pb2.StreamIdentifier(stream_name=self.stream_name.encode("utf8"))
            )
        else:
            options.all.CopyFrom(shared_pb2.Empty())
        return persistent_pb2.ReadReq(options=options)

    @staticmethod
    def _construct_ack_or_nack_read_req(
        subscription_id: bytes, event_ids: List[UUID], action: str
    ) -> persistent_pb2.ReadReq:
        ids = [shared_pb2.UUID(string=str(event_id)) for event_id in event_ids]
        if action == "ack":
            return persistent_pb2.ReadReq(
                ack=persistent_pb2.ReadReq.Ack(
                    id=subscription_id,
                    ids=ids,
                )
            )
        else:
            if action == "unknown":
                grpc_action = persistent_pb2.ReadReq.Nack.Unknown
            elif action == "park":
                grpc_action = persistent_pb2.ReadReq.Nack.Park
            elif action == "retry":
                grpc_action = persistent_pb2.ReadReq.Nack.Retry
            elif action == "skip":
                grpc_action = persistent_pb2.ReadReq.Nack.Skip
            else:
                assert action == "stop"
                grpc_action = persistent_pb2.ReadReq.Nack.Stop
            return persistent_pb2.ReadReq(
                nack=persistent_pb2.ReadReq.Nack(
                    id=subscription_id,
                    ids=ids,
                    action=grpc_action,
                )
            )


@dataclass
class SubscriptionInfo:
    event_source: str
    group_name: str
    status: str
    average_per_second: int
    total_items: int
    count_since_last_measurement: int
    last_checkpointed_event_position: str
    last_known_event_position: str
    resolve_link_tos: bool
    start_from: str
    message_timeout_milliseconds: int
    extra_statistics: bool
    max_retry_count: int
    live_buffer_size: int
    buffer_size: int
    read_batch_size: int
    check_point_after_milliseconds: int
    min_check_point_count: int
    max_check_point_count: int
    read_buffer_count: int
    live_buffer_count: int
    retry_buffer_count: int
    total_in_flight_messages: int
    outstanding_messages_count: int
    named_consumer_strategy: str
    max_subscriber_count: int
    parked_message_count: int


class BasePersistentSubscriptionsService(ESDBService):
    def __init__(
        self,
        channel: Union[grpc.Channel, grpc.aio.Channel],
        connection_spec: ConnectionSpec,
    ):
        super().__init__(connection_spec=connection_spec)
        self._stub = persistent_pb2_grpc.PersistentSubscriptionsStub(channel)

    def _construct_create_req(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
    ) -> persistent_pb2.CreateReq:
        # Construct 'settings'.
        settings = persistent_pb2.CreateReq.Settings(
            resolve_links=resolve_links,
            extra_statistics=False,
            max_retry_count=5,
            min_checkpoint_count=10,  # server recorded position
            max_checkpoint_count=10,  # server recorded position
            max_subscriber_count=5,
            live_buffer_size=1000,  # how many new events to hold in memory?
            read_batch_size=8,  # how many events to read from DB records?
            history_buffer_size=200,  # how many recorded events to hold in memory?
            message_timeout_ms=1000,
            checkpoint_after_ms=100,
            consumer_strategy=consumer_strategy,
        )
        # Construct CreateReq.Options.
        options = persistent_pb2.CreateReq.Options(
            group_name=group_name,
            settings=settings,
        )
        # Decide 'stream_option'.
        if isinstance(stream_name, str):
            stream_options = persistent_pb2.CreateReq.StreamOptions(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
            )
            # Decide 'revision_option'.
            if isinstance(stream_position, int):
                stream_options.revision = stream_position
            elif from_end is False:
                stream_options.start.CopyFrom(shared_pb2.Empty())
            else:
                stream_options.end.CopyFrom(shared_pb2.Empty())
            options.stream.CopyFrom(stream_options)
        else:
            if commit_position is not None:
                all_options = persistent_pb2.CreateReq.AllOptions(
                    position=persistent_pb2.CreateReq.Position(
                        commit_position=commit_position,
                        prepare_position=commit_position,
                    ),
                )
            elif from_end:
                all_options = persistent_pb2.CreateReq.AllOptions(
                    end=shared_pb2.Empty(),
                )
            else:
                all_options = persistent_pb2.CreateReq.AllOptions(
                    start=shared_pb2.Empty(),
                )

            # Decide 'filter_option'.
            if filter_exclude or filter_include:
                if filter_include:
                    regex = construct_filter_include_regex(filter_include)
                else:
                    regex = construct_filter_exclude_regex(filter_exclude)

                expression = (
                    persistent_pb2.CreateReq.AllOptions.FilterOptions.Expression(
                        regex=regex
                    )
                )

                if filter_by_stream_name:
                    stream_identifier_filter = expression
                    event_type_filter = None
                else:
                    stream_identifier_filter = None
                    event_type_filter = expression

                filter_options = persistent_pb2.CreateReq.AllOptions.FilterOptions(
                    stream_identifier=stream_identifier_filter,
                    event_type=event_type_filter,
                    max=window_size,
                    checkpointIntervalMultiplier=checkpoint_interval_multiplier,
                )
                all_options.filter.CopyFrom(filter_options)
            else:
                no_filter = shared_pb2.Empty()
                all_options.no_filter.CopyFrom(no_filter)

            options.all.CopyFrom(all_options)
        # Construct RPC request.
        return persistent_pb2.CreateReq(options=options)

    @staticmethod
    def _construct_get_info_req(
        group_name: str,
        stream_name: Optional[str] = None,
    ) -> persistent_pb2.GetInfoReq:
        if stream_name is None:
            options = persistent_pb2.GetInfoReq.Options(
                all=shared_pb2.Empty(),
                group_name=group_name,
            )
        else:
            options = persistent_pb2.GetInfoReq.Options(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                group_name=group_name,
            )
        return persistent_pb2.GetInfoReq(options=options)

    @staticmethod
    def _construct_list_req(stream_name: Optional[str]) -> persistent_pb2.ListReq:
        if stream_name is None:
            options = persistent_pb2.ListReq.Options(
                list_all_subscriptions=shared_pb2.Empty()
            )
        else:
            options = persistent_pb2.ListReq.Options(
                list_for_stream=persistent_pb2.ListReq.StreamOption(
                    stream=shared_pb2.StreamIdentifier(
                        stream_name=stream_name.encode("utf8")
                    )
                )
            )
        return persistent_pb2.ListReq(options=options)

    def _construct_update_req(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
    ) -> persistent_pb2.UpdateReq:
        # Construct 'settings'.
        settings = persistent_pb2.UpdateReq.Settings(
            resolve_links=resolve_links,
            extra_statistics=False,
            max_retry_count=5,
            min_checkpoint_count=10,  # server recorded position
            max_checkpoint_count=10,  # server recorded position
            max_subscriber_count=5,
            live_buffer_size=1000,  # how many new events to hold in memory?
            read_batch_size=8,  # how many events to read from DB records?
            history_buffer_size=200,  # how many recorded events to hold in memory?
            message_timeout_ms=1000,
            checkpoint_after_ms=100,
        )
        # Construct UpdateReq.Options.
        options = persistent_pb2.UpdateReq.Options(
            group_name=group_name,
            settings=settings,
        )
        # Decide 'stream_option'.
        if isinstance(stream_name, str):
            stream_options = persistent_pb2.UpdateReq.StreamOptions(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
            )
            # Decide 'revision_option'.
            if isinstance(stream_position, int):
                stream_options.revision = stream_position
            elif from_end is False:
                stream_options.start.CopyFrom(shared_pb2.Empty())
            else:
                stream_options.end.CopyFrom(shared_pb2.Empty())
            options.stream.CopyFrom(stream_options)
        else:
            if commit_position is not None:
                all_options = persistent_pb2.UpdateReq.AllOptions(
                    position=persistent_pb2.UpdateReq.Position(
                        commit_position=commit_position,
                        prepare_position=commit_position,
                    ),
                )
            elif from_end:
                all_options = persistent_pb2.UpdateReq.AllOptions(
                    end=shared_pb2.Empty(),
                )
            else:
                all_options = persistent_pb2.UpdateReq.AllOptions(
                    start=shared_pb2.Empty(),
                )

            options.all.CopyFrom(all_options)
        # Construct RPC request.
        return persistent_pb2.UpdateReq(options=options)

    @staticmethod
    def _construct_delete_req(
        group_name: str, stream_name: Optional[str]
    ) -> persistent_pb2.DeleteReq:
        if stream_name is None:
            options = persistent_pb2.DeleteReq.Options(
                all=shared_pb2.Empty(),
                group_name=group_name,
            )
        else:
            options = persistent_pb2.DeleteReq.Options(
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                group_name=group_name,
            )
        return persistent_pb2.DeleteReq(options=options)

    @staticmethod
    def _construct_replay_parked_req(
        group_name: str, stream_name: Optional[str]
    ) -> persistent_pb2.ReplayParkedReq:
        if stream_name is None:
            options = persistent_pb2.ReplayParkedReq.Options(
                group_name=group_name,
                all=shared_pb2.Empty(),
                no_limit=shared_pb2.Empty(),
            )
        else:
            options = persistent_pb2.ReplayParkedReq.Options(
                group_name=group_name,
                stream_identifier=shared_pb2.StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                no_limit=shared_pb2.Empty(),
            )
        return persistent_pb2.ReplayParkedReq(options=options)

    def _construct_subscription_infos(
        self, resp: persistent_pb2.ListResp
    ) -> List[SubscriptionInfo]:
        return [self._construct_subscription_info(s) for s in resp.subscriptions]

    @staticmethod
    def _construct_subscription_info(
        s: persistent_pb2.SubscriptionInfo,
    ) -> SubscriptionInfo:
        return SubscriptionInfo(
            event_source=s.event_source,
            group_name=s.group_name,
            status=s.status,
            average_per_second=s.average_per_second,
            total_items=s.total_items,
            count_since_last_measurement=s.count_since_last_measurement,
            last_checkpointed_event_position=s.last_checkpointed_event_position,
            last_known_event_position=s.last_known_event_position,
            resolve_link_tos=s.resolve_link_tos,
            start_from=s.start_from,
            message_timeout_milliseconds=s.message_timeout_milliseconds,
            extra_statistics=s.extra_statistics,
            max_retry_count=s.max_retry_count,
            live_buffer_size=s.live_buffer_size,
            buffer_size=s.buffer_size,
            read_batch_size=s.read_batch_size,
            check_point_after_milliseconds=s.check_point_after_milliseconds,
            min_check_point_count=s.min_check_point_count,
            max_check_point_count=s.max_check_point_count,
            read_buffer_count=s.read_buffer_count,
            live_buffer_count=s.live_buffer_count,
            retry_buffer_count=s.retry_buffer_count,
            total_in_flight_messages=s.total_in_flight_messages,
            outstanding_messages_count=s.outstanding_messages_count,
            named_consumer_strategy=s.named_consumer_strategy,
            max_subscriber_count=s.max_subscriber_count,
            parked_message_count=s.parked_message_count,
        )


class SubscriptionReadReqs(BaseSubscriptionReadReqs):
    def __init__(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        buffer_size: int = 100,
        grace: float = 0.2,
    ) -> None:
        super().__init__(
            group_name=group_name, stream_name=stream_name, buffer_size=buffer_size
        )
        self.subscription_id = b""
        self._grace = grace
        self._queue: queue.Queue[Tuple[Optional[UUID], str]] = queue.Queue()
        self._held: Optional[Tuple[UUID, str]] = (
            None  # Used when changing n/ack action.
        )
        self._queue_was_poisoned = False  # Indicates queue was poisoned.
        self._is_stopped = Event()  # Indicates req loop has exited.

    def __next__(self) -> persistent_pb2.ReadReq:
        # First send read request options, then send a batch of n/acks
        # whenever the buffer is full, or when the n/ack actions changes,
        # or periodically, or when stopping.

        if not self._has_requested_options:
            # Send initial read request options.
            self._has_requested_options = True
            return self._construct_initial_read_req()
        else:
            # Initialise buffer of n/acks.
            buffer = []  # Buffer of n/acks.

            # Initialise the "current action" - we need to detect when this changes.
            current_action: Optional[str] = None

            # Move any "held" n/ack from previous call to the buffer now.
            if self._held is not None:
                # Move held n/ack to buffer.
                held_id, current_action = self._held
                self._held = None
                buffer.append(held_id)

            # Collect n/acks from the queue, until the queue is poisoned.
            while True:
                try:
                    # If queue was poisoned, stop the iteration.
                    if self._queue_was_poisoned:
                        # Allow time for server to process last n/acks.
                        sleep(self._grace)
                        self._is_stopped.set()
                        raise StopIteration() from None

                    # Wait for next n/ack from the queue (with timeout).
                    event_id, action = self._queue.get(timeout=0.1)

                    # If queue was poisoned....
                    if action == "poison":
                        self._queue_was_poisoned = True
                        if len(buffer):
                            # ...send everything in the buffer.
                            assert current_action is not None
                            return self._construct_ack_or_nack_read_req(
                                subscription_id=self.subscription_id,
                                event_ids=buffer,
                                action=current_action,
                            )
                    else:
                        assert isinstance(event_id, UUID)
                        if current_action is None:
                            # Set the "current action" if there isn't one already.
                            current_action = action
                        elif action != current_action:
                            # The n/ack action changed, so send everything we have now.
                            self._held = (event_id, action)
                            return self._construct_ack_or_nack_read_req(
                                subscription_id=self.subscription_id,
                                event_ids=buffer,
                                action=current_action,
                            )

                        # Append event ID of queued n/ack to the buffer.
                        buffer.append(event_id)

                        if len(buffer) >= self._ack_buffer_size:
                            # Buffer is full so send everything we have now.
                            return self._construct_ack_or_nack_read_req(
                                subscription_id=self.subscription_id,
                                event_ids=buffer,
                                action=current_action,
                            )
                except queue.Empty:
                    # Queue timed out, so send everything we have now.
                    if len(buffer) > 0:
                        assert current_action is not None
                        return self._construct_ack_or_nack_read_req(
                            subscription_id=self.subscription_id,
                            event_ids=buffer,
                            action=current_action,
                        )
                    else:  # pragma: no cover
                        pass

    def ack(self, event_id: UUID) -> None:
        self._queue.put((event_id, "ack"))

    def nack(
        self,
        event_id: UUID,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        assert action in ["unknown", "park", "retry", "skip", "stop"]
        self._queue.put((event_id, action))

    def stop(self) -> None:
        self._queue.put((None, "poison"))
        self._is_stopped.wait(timeout=5)


class AsyncioPersistentSubscriptionsService(BasePersistentSubscriptionsService):
    pass


class PersistentSubscription(Iterator[RecordedEvent]):
    def __init__(
        self,
        read_reqs: SubscriptionReadReqs,
        read_resps: _ReadResps,
        expected_group_name: str,
        stream_name: Optional[str],
    ):
        self.read_reqs = read_reqs
        self.read_resps = read_resps

        first_read_resp = self._get_next_read_resp()
        if first_read_resp.WhichOneof("content") == "subscription_confirmation":
            expected_stream_name = stream_name if stream_name is not None else "$all"
            subscription_id = first_read_resp.subscription_confirmation.subscription_id
            confirmed_stream_name, _, confirmed_group_name = subscription_id.partition(
                "::"
            )
            if (
                confirmed_group_name != expected_group_name
                or confirmed_stream_name != expected_stream_name
            ):  # pragma: no cover
                raise SubscriptionConfirmationError()
            self.read_reqs.subscription_id = subscription_id.encode()
        else:  # pragma: no cover
            raise EventStoreDBClientException(
                f"Expected subscription confirmation, got: {first_read_resp}"
            )

    def __iter__(self) -> Iterator[RecordedEvent]:
        return self

    def __next__(self) -> RecordedEvent:
        while True:
            try:
                read_resp = self._get_next_read_resp()
            except CancelledByClient as e:
                raise StopIteration() from e
            content_oneof = read_resp.WhichOneof("content")
            if content_oneof == "event":
                recorded_event = construct_recorded_event(read_resp.event)
                if recorded_event is not None:
                    return recorded_event
                else:  # pragma: no cover
                    # Sometimes get here when resolving links after deleting a stream.
                    # Sometimes never, e.g. when the test suite runs, don't know why.
                    pass
            else:  # pragma: no cover
                pass

    def _get_next_read_resp(self) -> persistent_pb2.ReadResp:
        try:
            read_resp = next(self.read_resps)
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(read_resp, persistent_pb2.ReadResp)
        return read_resp

    def stop(self) -> None:
        self.read_reqs.stop()
        self.read_resps.cancel()

    def __enter__(self, *args: Any, **kwargs: Any) -> "PersistentSubscription":
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self.stop()

    def ack(self, event_id: UUID) -> None:
        self.read_reqs.ack(event_id)

    def nack(
        self,
        event_id: UUID,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        self.read_reqs.nack(event_id, action=action)

    def __del__(self) -> None:
        self.stop()


class PersistentSubscriptionsService(BasePersistentSubscriptionsService):
    """
    Encapsulates the 'persistent.PersistentSubscriptions' gRPC service.
    """

    @overload
    def create(
        self,
        group_name: str,
        *,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent subscription to all.
        """

    @overload
    def create(
        self,
        group_name: str,
        *,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent stream subscription.
        """

    def create(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        window_size: int = DEFAULT_WINDOW_SIZE,
        checkpoint_interval_multiplier: int = DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        request = self._construct_create_req(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            commit_position=commit_position,
            stream_position=stream_position,
            resolve_links=resolve_links,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
            window_size=window_size,
            checkpoint_interval_multiplier=checkpoint_interval_multiplier,
            consumer_strategy=consumer_strategy,
        )
        # Call 'Create' RPC.
        try:
            response = self._stub.Create(
                request,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(response, persistent_pb2.CreateResp)

    def read(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        buffer_size: int = 100,
        grace: float = 0.2,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> PersistentSubscription:
        read_reqs = SubscriptionReadReqs(
            group_name=group_name,
            stream_name=stream_name,
            buffer_size=buffer_size,
            grace=grace,
        )
        read_resps = self._stub.Read(
            read_reqs,
            timeout=timeout,
            metadata=self._metadata(metadata, requires_leader=True),
            credentials=credentials,
        )
        assert isinstance(read_resps, _ReadResps)

        return PersistentSubscription(
            read_reqs=read_reqs,
            read_resps=read_resps,
            expected_group_name=group_name,
            stream_name=stream_name,
        )

    def get_info(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> SubscriptionInfo:
        req = self._construct_get_info_req(group_name, stream_name)
        try:
            resp = self._stub.GetInfo(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            if (
                e.code() == grpc.StatusCode.UNAVAILABLE
                and e.details() == "Server Is Not Ready"
            ):
                raise NodeIsNotLeader() from e
            raise handle_rpc_error(e) from e

        else:
            assert isinstance(resp, persistent_pb2.GetInfoResp)
            return self._construct_subscription_info(resp.subscription_info)

    def list(
        self,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> List[SubscriptionInfo]:
        req = self._construct_list_req(stream_name)
        try:
            resp = self._stub.List(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return []
            elif (
                e.code() == grpc.StatusCode.UNAVAILABLE
                and e.details() == "Server Is Not Ready"
            ):
                raise NodeIsNotLeader() from e
            else:
                raise handle_rpc_error(e) from e

        assert isinstance(resp, persistent_pb2.ListResp)
        return self._construct_subscription_infos(resp)

    @overload
    def update(
        self,
        group_name: str,
        *,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating a persistent subscription to all.
        """

    @overload
    def update(
        self,
        group_name: str,
        *,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating a persistent stream subscription.
        """

    def update(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        request = self._construct_update_req(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            commit_position=commit_position,
            stream_position=stream_position,
            resolve_links=resolve_links,
        )
        # Call 'Update' RPC.
        try:
            response = self._stub.Update(
                request,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(response, persistent_pb2.UpdateResp)

    def delete(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        req = self._construct_delete_req(group_name, stream_name)
        try:
            resp = self._stub.Delete(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(resp, persistent_pb2.DeleteResp)

    def replay_parked(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        req = self._construct_replay_parked_req(group_name, stream_name)
        try:
            resp = self._stub.ReplayParked(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(resp, persistent_pb2.ReplayParkedResp)
