# -*- coding: utf-8 -*-
import queue
import time
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
    DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
    DEFAULT_WINDOW_SIZE,
    ESDBService,
    GrpcStreamer,
    GrpcStreamers,
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
    ExceptionIteratingRequests,
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
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        event_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
    ) -> None:
        self.group_name = group_name
        self.stream_name = stream_name
        assert isinstance(event_buffer_size, int) and event_buffer_size > 0
        self._event_buffer_size = event_buffer_size
        self._max_ack_batch_size = max_ack_batch_size
        self._max_ack_delay = max_ack_delay
        self._stopping_grace = stopping_grace
        self._has_requested_options = False
        self.subscription_id = b""

    def _construct_initial_read_req(self) -> persistent_pb2.ReadReq:
        options = persistent_pb2.ReadReq.Options(
            group_name=self.group_name,
            buffer_size=self._event_buffer_size,
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
        grpc_streamers: GrpcStreamers,
    ):
        super().__init__(connection_spec=connection_spec, grpc_streamers=grpc_streamers)
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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    ) -> persistent_pb2.CreateReq:
        # Construct 'settings'.
        settings = persistent_pb2.CreateReq.Settings(
            resolve_links=resolve_links,
            extra_statistics=False,
            max_retry_count=max_retry_count,
            min_checkpoint_count=10,  # server recorded position
            max_checkpoint_count=10,  # server recorded position
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=1000,  # how many new events to hold in memory?
            read_batch_size=8,  # how many events to read from DB records?
            history_buffer_size=200,  # how many recorded events to hold in memory?
            message_timeout_ms=int(round(1000 * message_timeout)),  # time before retry
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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    ) -> persistent_pb2.UpdateReq:
        # Construct 'settings'.
        settings = persistent_pb2.UpdateReq.Settings(
            resolve_links=resolve_links,
            extra_statistics=False,
            max_retry_count=max_retry_count,
            min_checkpoint_count=10,  # server recorded position
            max_checkpoint_count=10,  # server recorded position
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=1000,  # how many new events to hold in memory?
            read_batch_size=8,  # how many events to read from DB records?
            history_buffer_size=200,  # how many recorded events to hold in memory?
            message_timeout_ms=int(round(1000 * message_timeout)),  # time before retry
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
        event_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
    ) -> None:
        super().__init__(
            group_name=group_name,
            stream_name=stream_name,
            event_buffer_size=event_buffer_size,
            max_ack_batch_size=max_ack_batch_size,
            max_ack_delay=max_ack_delay,
            stopping_grace=stopping_grace,
        )
        self._ack_queue: queue.Queue[Tuple[Optional[UUID], str]] = queue.Queue()
        self._ack_held: Optional[Tuple[UUID, str]] = (
            None  # Used for changing n/ack action.
        )
        self._queue_poison_sent = False
        self._is_stopping = False
        self._is_stopped = Event()  # Indicates req loop has exited.
        self._is_aborted = Event()  # Indicates req loop has exited.
        self._update_last_ack_batch_time()
        self.errored: Optional[Exception] = None

    def __next__(self) -> persistent_pb2.ReadReq:
        try:
            # First send read request options, then send a batch of n/acks
            # whenever the buffer is full, or when the n/ack actions changes,
            # or periodically, or when stopping.

            if not self._has_requested_options:
                # Send initial read request options.
                self._has_requested_options = True
                return self._construct_initial_read_req()
            else:
                # Send a batch of n/acks.

                # Initialise batch, maybe from held n/ack.
                batch_ids: List[UUID] = []
                batch_action: Optional[str] = None
                if self._ack_held is not None:
                    held_event_id, batch_action = self._ack_held
                    batch_ids.append(held_event_id)
                    self._ack_held = None

                # Get n/acks from the queue, until the queue is poisoned.
                while True:
                    try:
                        # Maybe stop the iteration.
                        if self._is_stopping:
                            # Allow time for server to process last n/acks.
                            sleep(self._stopping_grace)
                            self._is_stopped.set()
                            raise StopIteration() from None

                        # Wait for next n/ack, timeout with "max ack delay".
                        get_timeout = max(0.0, self._calc_time_until_next_ack_batch())
                        event_id, action = self._ack_queue.get(timeout=get_timeout)

                        # If queue was poisoned, send non-empty batch now.
                        if action == "poison":
                            self._is_stopping = True
                            if len(batch_ids):
                                assert batch_action is not None
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=batch_ids,
                                    action=batch_action,
                                )
                        else:
                            assert isinstance(event_id, UUID)
                            if batch_action is None:
                                # Set the "current action" if there isn't one already.
                                batch_action = action
                            elif action != batch_action:
                                # Action changed, hold this ack and send the batch.
                                self._ack_held = (event_id, action)
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=batch_ids,
                                    action=batch_action,
                                )

                            # Add event ID to the batch.
                            batch_ids.append(event_id)

                            # Send the batch if full or late.
                            is_batch_late = self._calc_time_until_next_ack_batch() < 0.0
                            is_batch_full = len(batch_ids) >= self._max_ack_batch_size
                            if is_batch_full or is_batch_late:
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=batch_ids,
                                    action=batch_action,
                                )
                    except queue.Empty:
                        # Send a non-empty batch at least every "max ack delay".
                        self._update_last_ack_batch_time()
                        if len(batch_ids) > 0:
                            assert batch_action is not None
                            return self._construct_ack_or_nack_read_req(
                                subscription_id=self.subscription_id,
                                event_ids=batch_ids,
                                action=batch_action,
                            )
                        else:  # pragma: no cover
                            pass
        except Exception as e:
            self.errored = e
            raise

    def _update_last_ack_batch_time(self) -> None:
        self._last_ack_batch_time = time.monotonic()

    def _calc_time_until_next_ack_batch(self) -> float:
        return self._last_ack_batch_time + self._max_ack_delay - time.monotonic()

    def ack(self, event_id: UUID) -> None:
        self._ack_queue.put((event_id, "ack"))

    def nack(
        self,
        event_id: UUID,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        assert action in ["unknown", "park", "retry", "skip", "stop"]
        self._ack_queue.put((event_id, action))

    def abort(self) -> None:
        self._is_stopping = True
        self._ack_queue.put((None, "poison"))  # in case blocked on __next__()
        self._is_stopped.set()

    def stop(self) -> None:
        if not self._is_stopped.is_set():
            self._ack_queue.put((None, "poison"))
            self._queue_poison_sent = True
            self._is_stopped.wait(timeout=5)
        else:  # pragma: no cover
            pass


class AsyncioPersistentSubscriptionsService(BasePersistentSubscriptionsService):
    pass


class PersistentSubscription(GrpcStreamer, Iterator[RecordedEvent]):
    def __init__(
        self,
        read_reqs: SubscriptionReadReqs,
        read_resps: _ReadResps,
        expected_group_name: str,
        stream_name: Optional[str],
        grpc_streamers: GrpcStreamers,
    ):
        self._read_reqs = read_reqs
        self._read_resps = read_resps
        self._grpc_streamers = grpc_streamers
        self._grpc_streamers[id(self)] = self
        self._is_stopped = False

        try:
            first_read_resp = self._get_next_read_resp()

            if first_read_resp.WhichOneof("content") == "subscription_confirmation":
                expected_stream_name = (
                    stream_name if stream_name is not None else "$all"
                )
                subscription_id = (
                    first_read_resp.subscription_confirmation.subscription_id
                )
                confirmed_stream_name, _, confirmed_group_name = (
                    subscription_id.partition("::")
                )
                if (
                    confirmed_group_name != expected_group_name
                    or confirmed_stream_name != expected_stream_name
                ):  # pragma: no cover
                    raise SubscriptionConfirmationError()
                self._read_reqs.subscription_id = subscription_id.encode()
            else:  # pragma: no cover
                raise EventStoreDBClientException(
                    f"Expected subscription confirmation, got: {first_read_resp}"
                )
        except Exception:
            self._abort()
            raise

    def __iter__(self) -> Iterator[RecordedEvent]:
        return self

    def __next__(self) -> RecordedEvent:
        while True:
            try:
                read_resp = self._get_next_read_resp()
            except CancelledByClient:
                raise StopIteration() from None
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
            read_resp = next(self._read_resps)
        except grpc.RpcError as e:
            details = e.details()
            if (
                details is not None
                and "Exception iterating requests!" in details
                and self._read_reqs.errored
            ):
                raise ExceptionIteratingRequests() from self._read_reqs.errored
            raise handle_rpc_error(e) from None
        assert isinstance(read_resp, persistent_pb2.ReadResp)
        return read_resp

    def _abort(self) -> None:
        self._read_reqs.abort()
        self._read_resps.cancel()
        self._grpc_streamers.pop(id(self))
        self._is_stopped = True

    def stop(self) -> None:
        if not self._is_stopped:
            self._read_reqs.stop()
            self._read_resps.cancel()
            self._grpc_streamers.pop(id(self))
            self._is_stopped = True

    def __enter__(self, *args: Any, **kwargs: Any) -> "PersistentSubscription":
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self.stop()

    def ack(self, item: Union[UUID, RecordedEvent]) -> None:
        self._read_reqs.ack(event_id=self._get_event_id(item))

    def nack(
        self,
        item: Union[UUID, RecordedEvent],
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        self._read_reqs.nack(event_id=self._get_event_id(item), action=action)

    @staticmethod
    def _get_event_id(item: Union[UUID, RecordedEvent]) -> UUID:
        if isinstance(item, RecordedEvent):
            return item.ack_id
        else:
            return item

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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
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
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            max_subscriber_count=max_subscriber_count,
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
            raise handle_rpc_error(e) from None
        assert isinstance(response, persistent_pb2.CreateResp)

    def read(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        event_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
        max_ack_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
        max_ack_delay: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
        stopping_grace: float = DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> PersistentSubscription:
        read_reqs = SubscriptionReadReqs(
            group_name=group_name,
            stream_name=stream_name,
            event_buffer_size=event_buffer_size,
            max_ack_batch_size=max_ack_batch_size,
            max_ack_delay=max_ack_delay,
            stopping_grace=stopping_grace,
        )
        read_resps = self._stub.Read(
            read_reqs,
            timeout=timeout,
            metadata=self._metadata(metadata, requires_leader=True),
            credentials=credentials,
        )
        assert isinstance(read_resps, _ReadResps)

        subscription = PersistentSubscription(
            grpc_streamers=self._grpc_streamers,
            read_reqs=read_reqs,
            read_resps=read_resps,
            expected_group_name=group_name,
            stream_name=stream_name,
        )
        return subscription

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
            raise handle_rpc_error(e) from None

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
                raise handle_rpc_error(e) from None

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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
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
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
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
            message_timeout=message_timeout,
            max_retry_count=max_retry_count,
            max_subscriber_count=max_subscriber_count,
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
            raise handle_rpc_error(e) from None
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
            raise handle_rpc_error(e) from None
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
            raise handle_rpc_error(e) from None
        assert isinstance(resp, persistent_pb2.ReplayParkedResp)
