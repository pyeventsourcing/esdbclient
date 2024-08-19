# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import queue
import time
from abc import abstractmethod
from dataclasses import dataclass
from threading import Event
from time import sleep
from typing import (
    AsyncIterator,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    overload,
)
from uuid import UUID

import grpc
from grpc.aio import AioRpcError, StreamStreamCall
from typing_extensions import Literal, Protocol, TypedDict, runtime_checkable

from esdbclient.common import (
    DEFAULT_CHECKPOINT_INTERVAL_MULTIPLIER,
    DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
    DEFAULT_PERSISTENT_SUBSCRIPTION_EVENT_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_ACK_DELAY,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
    DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
    DEFAULT_PERSISTENT_SUBSCRIPTION_STOPPING_GRACE,
    DEFAULT_WINDOW_SIZE,
    AbstractAsyncPersistentSubscription,
    AbstractPersistentSubscription,
    AsyncGrpcStreamer,
    AsyncGrpcStreamers,
    ESDBService,
    GrpcStreamer,
    GrpcStreamers,
    Metadata,
    TGrpcStreamers,
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
    ProgrammingError,
    SubscriptionConfirmationError,
)
from esdbclient.protos.Grpc import persistent_pb2, persistent_pb2_grpc, shared_pb2


@runtime_checkable
class _ReadResps(Iterator[persistent_pb2.ReadResp], Protocol):
    @abstractmethod
    def cancel(self) -> None:  # pragma: no cover
        ...


@runtime_checkable
class _AsyncioReadResps(Iterator[persistent_pb2.ReadResp], Protocol):
    @abstractmethod
    async def cancel(self) -> None:  # pragma: no cover
        ...


ConsumerStrategy = Literal[
    "DispatchToSingle", "RoundRobin", "Pinned", "PinnedByCorrelation"
]

CONSUMER_STRATEGIES: Dict[
    ConsumerStrategy, persistent_pb2.UpdateReq.ConsumerStrategy.ValueType
] = {
    "DispatchToSingle": persistent_pb2.UpdateReq.ConsumerStrategy.DispatchToSingle,
    "RoundRobin": persistent_pb2.UpdateReq.ConsumerStrategy.RoundRobin,
    "Pinned": persistent_pb2.UpdateReq.ConsumerStrategy.Pinned,
}


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
            read_req = persistent_pb2.ReadReq(
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
            read_req = persistent_pb2.ReadReq(
                nack=persistent_pb2.ReadReq.Nack(
                    id=subscription_id,
                    ids=ids,
                    action=grpc_action,
                )
            )
        return read_req

    def _update_last_ack_batch_time(self) -> None:
        self._last_ack_batch_time = time.monotonic()

    def _calc_time_until_next_ack_batch(self) -> float:
        return self._last_ack_batch_time + self._max_ack_delay - time.monotonic()


class AsyncSubscriptionReadReqs(
    BaseSubscriptionReadReqs, AsyncIterator[persistent_pb2.ReadReq]
):
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
        self._ack_queue: asyncio.Queue[Tuple[Optional[UUID], str]] = asyncio.Queue()
        self._ack_held: Optional[Tuple[UUID, str]] = (
            None  # Used for changing n/ack action.
        )
        self._is_poisoned = False
        self._is_stopping = False
        self._is_stopped = asyncio.Event()
        self._update_last_ack_batch_time()
        self.errored: Optional[BaseException] = None
        self._batch_ids: List[UUID] = []

    def __aiter__(self) -> AsyncSubscriptionReadReqs:
        return self

    async def __anext__(self) -> persistent_pb2.ReadReq:
        try:
            # First return read request with options, then return read request
            # with batch of n/acks whenever the batch is full, or when the n/ack
            # actions changes, or periodically, or when stopping.

            if not self._has_requested_options:
                # Return initial read request with options.
                self._has_requested_options = True
                return self._construct_initial_read_req()
            else:
                # Return read request with a batch of n/acks.

                # Initialise batch, maybe from held n/ack.
                for _ in self._batch_ids:
                    self._ack_queue.task_done()
                self._batch_ids = []
                batch_action: Optional[str] = None
                if self._ack_held is not None:
                    held_event_id, batch_action = self._ack_held
                    self._batch_ids.append(held_event_id)
                    self._ack_held = None

                # Get n/acks from the queue, until the queue is poisoned.
                while True:
                    # Maybe stop the iteration.
                    if self._is_stopping:
                        # Allow time for server to process last n/acks.
                        await asyncio.sleep(self._stopping_grace)
                        self._ack_queue.task_done()
                        raise StopAsyncIteration() from None

                    try:
                        # Wait for next n/ack, timeout with "max ack delay".
                        get_timeout = max(0.0, self._calc_time_until_next_ack_batch())
                        event_id, action = await asyncio.wait_for(
                            self._ack_queue.get(), timeout=get_timeout
                        )

                        # If queue was poisoned, send non-empty batch now.
                        if action == "poison":
                            self._is_stopping = True
                            if len(self._batch_ids):
                                assert batch_action is not None
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=self._batch_ids,
                                    action=batch_action,
                                )
                        else:
                            if not isinstance(event_id, UUID):
                                msg = f"event_id {repr(event_id)} is not a UUID"
                                raise ValueError(msg)
                            if batch_action is None:
                                # Set the "current action" if there isn't one already.
                                batch_action = action
                            elif action != batch_action:
                                # Action changed, hold this ack and send the batch.
                                self._ack_held = (event_id, action)
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=self._batch_ids,
                                    action=batch_action,
                                )

                            # Add event ID to the batch.
                            self._batch_ids.append(event_id)

                            # Send the batch if full.
                            if len(self._batch_ids) >= self._max_ack_batch_size:
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=self._batch_ids,
                                    action=batch_action,
                                )
                    except asyncio.TimeoutError:
                        self._update_last_ack_batch_time()  # positive next get_timeout
                        # Send a non-empty batch at least every "max ack delay".
                        if len(self._batch_ids) > 0:
                            assert batch_action is not None
                            return self._construct_ack_or_nack_read_req(
                                subscription_id=self.subscription_id,
                                event_ids=self._batch_ids,
                                action=batch_action,
                            )
                        else:
                            pass
        except BaseException as e:
            self._is_stopped.set()
            if not isinstance(e, StopAsyncIteration):
                self.errored = e
            raise e

    async def ack(self, event_id: UUID) -> None:
        if self._is_poisoned:
            raise ProgrammingError("Subscription has already been stopped")
        await self._ack_queue.put((event_id, "ack"))

    async def nack(
        self,
        event_id: UUID,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        if self._is_poisoned:
            raise ProgrammingError("Subscription has already been stopped")
        assert action in ["unknown", "park", "retry", "skip", "stop"]
        await self._ack_queue.put((event_id, action))

    async def stop(self, wait_until_stopped: bool = True) -> None:
        if not self._is_poisoned:
            self._is_poisoned = True
            await self._ack_queue.put((None, "poison"))
            if wait_until_stopped:
                await self._is_stopped.wait()


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
                    # Maybe stop the iteration.
                    if self._is_stopping:
                        # Allow time for server to process last n/acks.
                        sleep(self._stopping_grace)
                        self._is_stopped.set()
                        raise StopIteration() from None

                    try:
                        # Wait for next n/ack, timeout with "max ack delay".
                        get_timeout = max(0.0, self._calc_time_until_next_ack_batch())
                        event_id, action = self._ack_queue.get(timeout=get_timeout)
                        self._ack_queue.task_done()

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
                            if not isinstance(event_id, UUID):
                                msg = f"event_id {repr(event_id)} is not a UUID"
                                raise ValueError(msg)
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

                            # Send the batch if full.
                            if len(batch_ids) >= self._max_ack_batch_size:
                                self._update_last_ack_batch_time()
                                return self._construct_ack_or_nack_read_req(
                                    subscription_id=self.subscription_id,
                                    event_ids=batch_ids,
                                    action=batch_action,
                                )
                    except queue.Empty:
                        self._update_last_ack_batch_time()  # positive next get_timeout
                        # Send a non-empty batch at least every "max ack delay".
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


class BasePersistentSubscription:
    pass


class AsyncPersistentSubscription(
    BasePersistentSubscription, AsyncGrpcStreamer, AbstractAsyncPersistentSubscription
):
    def __init__(
        self,
        read_reqs: AsyncSubscriptionReadReqs,
        stream_stream_call: grpc.aio.StreamStreamCall[
            persistent_pb2.ReadReq, persistent_pb2.ReadResp
        ],
        expected_group_name: str,
        stream_name: Optional[str],
        grpc_streamers: AsyncGrpcStreamers,
    ) -> None:
        super().__init__(grpc_streamers=grpc_streamers)
        self._read_reqs = read_reqs
        self._stream_stream_call = stream_stream_call
        self._stream_stream_call_iter = stream_stream_call.__aiter__()
        self._expected_group_name = expected_group_name
        self._stream_name = stream_name

    async def init(self) -> None:
        try:
            first_read_resp = await self._get_next_read_resp()
            if first_read_resp.WhichOneof("content") == "subscription_confirmation":
                expected_stream_name = (
                    self._stream_name if self._stream_name is not None else "$all"
                )
                subscription_id = (
                    first_read_resp.subscription_confirmation.subscription_id
                )
                confirmed_stream_name, _, confirmed_group_name = (
                    subscription_id.partition("::")
                )
                if (
                    confirmed_group_name != self._expected_group_name
                    or confirmed_stream_name != expected_stream_name
                ):  # pragma: no cover
                    raise SubscriptionConfirmationError()
                self._subscription_id = subscription_id
            else:  # pragma: no cover
                raise EventStoreDBClientException(
                    f"Expected subscription confirmation, got: {first_read_resp}"
                )
        except BaseException:
            await self.stop(wait_until_stopped=False)
            raise

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

    async def __anext__(self) -> RecordedEvent:
        try:
            while True:
                read_resp = await self._get_next_read_resp()
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
        except BaseException:
            await self.stop(wait_until_stopped=False)
            raise

    async def _get_next_read_resp(self) -> persistent_pb2.ReadResp:
        try:
            response = await self._stream_stream_call_iter.__anext__()
            if self._has_iter_error_for_testing():
                raise AioRpcError(
                    grpc.StatusCode.INTERNAL,
                    grpc.aio.Metadata(),
                    grpc.aio.Metadata(),
                    "",
                    "",
                )

        except asyncio.CancelledError as e:
            await self.stop(wait_until_stopped=False)
            if self._read_reqs.errored:
                raise ExceptionIteratingRequests() from self._read_reqs.errored
            else:
                raise StopAsyncIteration() from e
        except grpc.aio.AioRpcError as e:
            await self.stop(wait_until_stopped=False)
            raise handle_rpc_error(e) from None
        else:
            return response

    async def stop(self, wait_until_stopped: bool = True) -> None:
        if not await self._set_is_stopped():
            await self._read_reqs.stop(wait_until_stopped=wait_until_stopped)
            self._stream_stream_call.cancel()
            await asyncio.sleep(0.05)
            self._grpc_streamers.remove(self)

    async def ack(self, item: Union[UUID, RecordedEvent]) -> None:
        await self._read_reqs.ack(event_id=self._get_event_id(item))

    async def nack(
        self,
        item: Union[UUID, RecordedEvent],
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        await self._read_reqs.nack(event_id=self._get_event_id(item), action=action)

    @staticmethod
    def _get_event_id(item: Union[UUID, RecordedEvent]) -> UUID:
        if isinstance(item, RecordedEvent):
            return item.ack_id
        else:
            return item


class PersistentSubscription(
    GrpcStreamer, AbstractPersistentSubscription, BasePersistentSubscription
):
    def __init__(
        self,
        read_reqs: SubscriptionReadReqs,
        read_resps: _ReadResps,
        expected_group_name: str,
        stream_name: Optional[str],
        grpc_streamers: GrpcStreamers,
    ):
        super().__init__(grpc_streamers=grpc_streamers)
        self._read_reqs = read_reqs
        self._read_resps = read_resps

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
                self._subscription_id = subscription_id
                self._read_reqs.subscription_id = subscription_id.encode()
            else:  # pragma: no cover
                raise EventStoreDBClientException(
                    f"Expected subscription confirmation, got: {first_read_resp}"
                )
        except Exception:
            self._abort()
            raise

    @property
    def subscription_id(self) -> str:
        return self._subscription_id

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
        self._grpc_streamers.remove(self)
        self._is_stopped = True

    def stop(self) -> None:
        if not self._set_is_stopped():
            self._read_reqs.stop()
            self._read_resps.cancel()
            self._grpc_streamers.remove(self)

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


class SubscriptionUpdateKwargs(TypedDict):
    from_end: bool
    resolve_links: bool
    consumer_strategy: ConsumerStrategy
    message_timeout: float
    max_retry_count: int
    min_checkpoint_count: int
    max_checkpoint_count: int
    checkpoint_after: float
    max_subscriber_count: int
    live_buffer_size: int
    read_batch_size: int
    history_buffer_size: int
    extra_statistics: bool


class SubscriptionUpdateAllKwargs(SubscriptionUpdateKwargs):
    commit_position: Optional[int]


class SubscriptionUpdateStreamKwargs(SubscriptionUpdateKwargs):
    stream_position: Optional[int]


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
    resolve_links: bool
    start_from: str
    message_timeout: float
    extra_statistics: bool
    max_retry_count: int
    live_buffer_size: int
    history_buffer_size: int
    read_batch_size: int
    checkpoint_after: float
    min_checkpoint_count: int
    max_checkpoint_count: int
    read_buffer_count: int
    live_buffer_count: int
    retry_buffer_count: int
    total_in_flight_messages: int
    outstanding_messages_count: int
    consumer_strategy: ConsumerStrategy
    max_subscriber_count: int
    parked_message_count: int

    def update_all_kwargs(
        self,
        from_end: Optional[bool] = None,
        commit_position: Optional[int] = None,
        resolve_links: Optional[bool] = None,
        consumer_strategy: Optional[ConsumerStrategy] = None,
        message_timeout: Optional[float] = None,
        max_retry_count: Optional[int] = None,
        min_checkpoint_count: Optional[int] = None,
        max_checkpoint_count: Optional[int] = None,
        checkpoint_after: Optional[float] = None,
        max_subscriber_count: Optional[int] = None,
        live_buffer_size: Optional[int] = None,
        read_batch_size: Optional[int] = None,
        history_buffer_size: Optional[int] = None,
        extra_statistics: Optional[bool] = None,
    ) -> SubscriptionUpdateAllKwargs:
        # Deal with 'start_from' conventions...
        assert self.event_source == "$all"
        if from_end is None and commit_position is None:
            commit_position = int(self.start_from.split("/")[0].split(":")[1])
            if commit_position == 0:
                from_end = False
                commit_position = None
            elif commit_position == -1:
                from_end = True
                commit_position = None
            else:
                from_end = False
                commit_position = commit_position

        kwargs: SubscriptionUpdateAllKwargs = {
            "from_end": from_end or False,
            "commit_position": commit_position,
            "resolve_links": (
                self.resolve_links if resolve_links is None else resolve_links
            ),
            "consumer_strategy": (
                self.consumer_strategy
                if consumer_strategy is None
                else consumer_strategy
            ),
            "message_timeout": (
                self.message_timeout if message_timeout is None else message_timeout
            ),
            "max_retry_count": (
                self.max_retry_count if max_retry_count is None else max_retry_count
            ),
            "min_checkpoint_count": (
                self.min_checkpoint_count
                if min_checkpoint_count is None
                else min_checkpoint_count
            ),
            "max_checkpoint_count": (
                self.max_checkpoint_count
                if max_checkpoint_count is None
                else max_checkpoint_count
            ),
            "checkpoint_after": (
                self.checkpoint_after if checkpoint_after is None else checkpoint_after
            ),
            "max_subscriber_count": (
                self.max_subscriber_count
                if max_subscriber_count is None
                else max_subscriber_count
            ),
            "live_buffer_size": (
                self.live_buffer_size if live_buffer_size is None else live_buffer_size
            ),
            "read_batch_size": (
                self.read_batch_size if read_batch_size is None else read_batch_size
            ),
            "history_buffer_size": (
                self.history_buffer_size
                if history_buffer_size is None
                else history_buffer_size
            ),
            "extra_statistics": (
                self.extra_statistics if extra_statistics is None else extra_statistics
            ),
        }

        return kwargs

    def update_stream_kwargs(
        self,
        from_end: Optional[bool] = None,
        stream_position: Optional[int] = None,
        resolve_links: Optional[bool] = None,
        consumer_strategy: Optional[ConsumerStrategy] = None,
        message_timeout: Optional[float] = None,
        max_retry_count: Optional[int] = None,
        min_checkpoint_count: Optional[int] = None,
        max_checkpoint_count: Optional[int] = None,
        checkpoint_after: Optional[float] = None,
        max_subscriber_count: Optional[int] = None,
        live_buffer_size: Optional[int] = None,
        read_batch_size: Optional[int] = None,
        history_buffer_size: Optional[int] = None,
        extra_statistics: Optional[bool] = None,
    ) -> SubscriptionUpdateStreamKwargs:
        if from_end is None and stream_position is None:
            stream_position = int(self.start_from)
            if stream_position == 0:
                from_end = False
                stream_position = None
            elif stream_position == -1:
                from_end = True
                stream_position = None
            else:
                from_end = False
                stream_position = stream_position

        kwargs: SubscriptionUpdateStreamKwargs = {
            "from_end": from_end or False,
            "stream_position": stream_position,
            "resolve_links": (
                self.resolve_links if resolve_links is None else resolve_links
            ),
            "consumer_strategy": (
                self.consumer_strategy
                if consumer_strategy is None
                else consumer_strategy
            ),
            "message_timeout": (
                self.message_timeout if message_timeout is None else message_timeout
            ),
            "max_retry_count": (
                self.max_retry_count if max_retry_count is None else max_retry_count
            ),
            "min_checkpoint_count": (
                self.min_checkpoint_count
                if min_checkpoint_count is None
                else min_checkpoint_count
            ),
            "max_checkpoint_count": (
                self.max_checkpoint_count
                if max_checkpoint_count is None
                else max_checkpoint_count
            ),
            "checkpoint_after": (
                self.checkpoint_after if checkpoint_after is None else checkpoint_after
            ),
            "max_subscriber_count": (
                self.max_subscriber_count
                if max_subscriber_count is None
                else max_subscriber_count
            ),
            "live_buffer_size": (
                self.live_buffer_size if live_buffer_size is None else live_buffer_size
            ),
            "read_batch_size": (
                self.read_batch_size if read_batch_size is None else read_batch_size
            ),
            "history_buffer_size": (
                self.history_buffer_size
                if history_buffer_size is None
                else history_buffer_size
            ),
            "extra_statistics": (
                self.extra_statistics if extra_statistics is None else extra_statistics
            ),
        }
        return kwargs


class BasePersistentSubscriptionsService(ESDBService[TGrpcStreamers]):
    def __init__(
        self,
        channel: Union[grpc.Channel, grpc.aio.Channel],
        connection_spec: ConnectionSpec,
        grpc_streamers: TGrpcStreamers,
    ):
        super().__init__(connection_spec=connection_spec, grpc_streamers=grpc_streamers)
        self._stub = persistent_pb2_grpc.PersistentSubscriptionsStub(channel)  # type: ignore[no-untyped-call]

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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
    ) -> persistent_pb2.CreateReq:
        # Construct 'settings'.
        settings = persistent_pb2.CreateReq.Settings(
            resolve_links=resolve_links,
            extra_statistics=extra_statistics,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            message_timeout_ms=int(round(1000 * message_timeout)),
            checkpoint_after_ms=int(round(1000 * checkpoint_after)),
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
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
    ) -> persistent_pb2.UpdateReq:
        # Construct UpdateReq.Settings.
        named_consumer_strategy = CONSUMER_STRATEGIES[consumer_strategy]
        settings = persistent_pb2.UpdateReq.Settings(
            resolve_links=resolve_links,
            named_consumer_strategy=named_consumer_strategy,
            extra_statistics=extra_statistics,
            max_retry_count=max_retry_count,
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            message_timeout_ms=int(round(1000 * message_timeout)),
            checkpoint_after_ms=int(round(1000 * checkpoint_after)),
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
            resolve_links=s.resolve_link_tos,
            start_from=s.start_from,
            message_timeout=s.message_timeout_milliseconds / 1000.0,
            extra_statistics=s.extra_statistics,
            max_retry_count=s.max_retry_count,
            live_buffer_size=s.live_buffer_size,
            history_buffer_size=s.buffer_size,
            read_batch_size=s.read_batch_size,
            checkpoint_after=s.check_point_after_milliseconds / 1000.0,
            min_checkpoint_count=s.min_check_point_count,
            max_checkpoint_count=s.max_check_point_count,
            read_buffer_count=s.read_buffer_count,
            live_buffer_count=s.live_buffer_count,
            retry_buffer_count=s.retry_buffer_count,
            total_in_flight_messages=s.total_in_flight_messages,
            outstanding_messages_count=s.outstanding_messages_count,
            consumer_strategy=cast(ConsumerStrategy, s.named_consumer_strategy),
            max_subscriber_count=s.max_subscriber_count,
            parked_message_count=s.parked_message_count,
        )


class AsyncPersistentSubscriptionsService(
    BasePersistentSubscriptionsService[AsyncGrpcStreamers]
):
    @overload
    async def create(
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent subscription to all.
        """

    @overload
    async def create(
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent stream subscription.
        """

    async def create(
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            checkpoint_after=checkpoint_after,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            extra_statistics=extra_statistics,
        )
        # Call 'Create' RPC.
        try:
            response = await self._stub.Create(
                request,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(response, persistent_pb2.CreateResp)

    async def read(
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
    ) -> AsyncPersistentSubscription:
        read_reqs = AsyncSubscriptionReadReqs(
            group_name=group_name,
            stream_name=stream_name,
            event_buffer_size=event_buffer_size,
            max_ack_batch_size=max_ack_batch_size,
            max_ack_delay=max_ack_delay,
            stopping_grace=stopping_grace,
        )
        stream_stream_call = self._stub.Read(
            read_reqs,
            timeout=timeout,
            metadata=self._metadata(metadata, requires_leader=True),
            credentials=credentials,
        )
        assert isinstance(stream_stream_call, StreamStreamCall)

        subscription = AsyncPersistentSubscription(
            read_reqs=read_reqs,
            stream_stream_call=stream_stream_call,
            grpc_streamers=self._grpc_streamers,
            expected_group_name=group_name,
            stream_name=stream_name,
        )
        await subscription.init()
        return subscription

    async def get_info(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> SubscriptionInfo:
        req = self._construct_get_info_req(group_name, stream_name)
        try:
            resp = await self._stub.GetInfo(
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
                raise NodeIsNotLeader(e) from None
            raise handle_rpc_error(e) from None

        else:
            assert isinstance(resp, persistent_pb2.GetInfoResp)
            return self._construct_subscription_info(resp.subscription_info)

    async def list(
        self,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> List[SubscriptionInfo]:
        req = self._construct_list_req(stream_name)
        try:
            resp = await self._stub.List(
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
                raise NodeIsNotLeader(e) from None
            else:
                raise handle_rpc_error(e) from None

        assert isinstance(resp, persistent_pb2.ListResp)
        return self._construct_subscription_infos(resp)

    @overload
    async def update(
        self,
        group_name: str,
        *,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating a persistent subscription to all.
        """

    @overload
    async def update(
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        """
        Signature for updating a persistent stream subscription.
        """

    async def update(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        resolve_links: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
        # Call 'Update' RPC.
        try:
            response = await self._stub.Update(
                request,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(response, persistent_pb2.UpdateResp)

    async def delete(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        req = self._construct_delete_req(group_name, stream_name)
        try:
            resp = await self._stub.Delete(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(resp, persistent_pb2.DeleteResp)

    async def replay_parked(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[grpc.CallCredentials] = None,
    ) -> None:
        req = self._construct_replay_parked_req(group_name, stream_name)
        try:
            resp = await self._stub.ReplayParked(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except grpc.RpcError as e:
            raise handle_rpc_error(e) from None
        assert isinstance(resp, persistent_pb2.ReplayParkedResp)


class PersistentSubscriptionsService(BasePersistentSubscriptionsService[GrpcStreamers]):
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
            min_checkpoint_count=min_checkpoint_count,
            max_checkpoint_count=max_checkpoint_count,
            checkpoint_after=checkpoint_after,
            max_subscriber_count=max_subscriber_count,
            live_buffer_size=live_buffer_size,
            read_batch_size=read_batch_size,
            history_buffer_size=history_buffer_size,
            extra_statistics=extra_statistics,
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
    ) -> AbstractPersistentSubscription:
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
        # assert isinstance(read_resps, _ReadResps), type(_ReadResps)

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
                raise NodeIsNotLeader(e) from None
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
                raise NodeIsNotLeader(e) from None
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
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        message_timeout: float = DEFAULT_PERSISTENT_SUBSCRIPTION_MESSAGE_TIMEOUT,
        max_retry_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_RETRY_COUNT,
        min_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MIN_CHECKPOINT_COUNT,
        max_checkpoint_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_CHECKPOINT_COUNT,
        checkpoint_after: float = DEFAULT_PERSISTENT_SUBSCRIPTION_CHECKPOINT_AFTER,
        max_subscriber_count: int = DEFAULT_PERSISTENT_SUBSCRIPTION_MAX_SUBSCRIBER_COUNT,
        live_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_LIVE_BUFFER_SIZE,
        read_batch_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_READ_BATCH_SIZE,
        history_buffer_size: int = DEFAULT_PERSISTENT_SUBSCRIPTION_HISTORY_BUFFER_SIZE,
        extra_statistics: bool = False,
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
