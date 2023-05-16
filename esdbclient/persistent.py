# -*- coding: utf-8 -*-
import queue
from dataclasses import dataclass
from typing import Iterator, List, Optional, Sequence, Tuple, Union, overload
from uuid import UUID

import grpc
from grpc import CallCredentials, RpcError, StatusCode
from grpc._channel import _MultiThreadedRendezvous
from typing_extensions import Literal

from esdbclient.esdbapibase import ESDBService, Metadata, handle_rpc_error
from esdbclient.events import RecordedEvent
from esdbclient.exceptions import (
    CancelledByClient,
    ESDBClientException,
    NodeIsNotLeader,
    SubscriptionConfirmationError,
)
from esdbclient.protos.Grpc import persistent_pb2, persistent_pb2_grpc, shared_pb2

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
        self._buffer_size = buffer_size
        self._has_requested_options = False

    def _construct_initial_read_req(self) -> persistent_pb2.ReadReq:
        options = persistent_pb2.ReadReq.Options(
            group_name=self.group_name,
            buffer_size=self._buffer_size,
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
        ids: List[shared_pb2.UUID], action: str
    ) -> persistent_pb2.ReadReq:
        if action == "ack":
            return persistent_pb2.ReadReq(ack=persistent_pb2.ReadReq.Ack(ids=ids))
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
                nack=persistent_pb2.ReadReq.Nack(ids=ids, action=grpc_action)
            )


class BasePersistentSubscription:
    @staticmethod
    def _construct_recorded_event(read_resp: persistent_pb2.ReadResp) -> RecordedEvent:
        event = read_resp.event.event
        position_oneof = read_resp.event.WhichOneof("position")
        if position_oneof == "commit_position":
            commit_position = read_resp.event.commit_position
        else:  # pragma: no cover
            # We only get here with EventStoreDB < 22.10.
            assert position_oneof == "no_position", position_oneof
            commit_position = None
        recorded_event = RecordedEvent(
            id=UUID(event.id.string),
            type=event.metadata.get("type", ""),
            data=event.data,
            metadata=event.custom_metadata,
            content_type=event.metadata.get("content-type", ""),
            stream_name=event.stream_identifier.stream_name.decode("utf8"),
            stream_position=event.stream_revision,
            commit_position=commit_position,
            retry_count=read_resp.event.retry_count,
        )
        return recorded_event


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
    def __init__(self, channel: Union[grpc.Channel, grpc.aio.Channel]):
        self._stub = persistent_pb2_grpc.PersistentSubscriptionsStub(channel)

    def _construct_create_req(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        commit_position: Optional[int] = None,
        stream_position: Optional[int] = None,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
    ) -> persistent_pb2.CreateReq:
        # Construct 'settings'.
        settings = persistent_pb2.CreateReq.Settings(
            resolve_links=False,
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
                    filter_include = (
                        [filter_include]
                        if isinstance(filter_include, str)
                        else filter_include
                    )
                    filter_regex = "^" + "|".join(filter_include) + "$"
                else:
                    filter_exclude = (
                        [filter_exclude]
                        if isinstance(filter_exclude, str)
                        else filter_exclude
                    )
                    filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

                filter_expression = (
                    persistent_pb2.CreateReq.AllOptions.FilterOptions.Expression(
                        regex=filter_regex
                    )
                )

                if filter_by_stream_name:
                    stream_identifier_filter = filter_expression
                    event_type_filter = None
                else:
                    stream_identifier_filter = None
                    event_type_filter = filter_expression

                filter = persistent_pb2.CreateReq.AllOptions.FilterOptions(
                    stream_identifier=stream_identifier_filter,
                    event_type=event_type_filter,
                    # Todo: What does 'window' mean?
                    # max=shared_pb2.Empty(),
                    count=shared_pb2.Empty(),
                    # Todo: What does 'checkpointIntervalMultiplier' mean?
                    checkpointIntervalMultiplier=5,
                )
                all_options.filter.CopyFrom(filter)
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
    ) -> persistent_pb2.UpdateReq:
        # Construct 'settings'.
        settings = persistent_pb2.UpdateReq.Settings(
            resolve_links=False,
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
        self, group_name: str, stream_name: Optional[str] = None, buffer_size: int = 100
    ) -> None:
        super().__init__(
            group_name=group_name, stream_name=stream_name, buffer_size=buffer_size
        )
        self.queue: queue.Queue[Tuple[UUID, str]] = queue.Queue()
        self.held: Optional[Tuple[UUID, str]] = None
        self._is_stopped = True

    def __next__(self) -> persistent_pb2.ReadReq:
        if not self._has_requested_options:
            self._has_requested_options = True
            return self._construct_initial_read_req()
        else:
            # Send a response whenever there are 100 acks or nacks of
            # the same kind, or when the kind of ack or nack changes,
            # or at least every 100ms when there is something to send.
            ids = []
            current_action: Optional[str] = None
            if self.held is not None:
                held_id, current_action = self.held
                self.held = None
                ids.append(shared_pb2.UUID(string=str(held_id)))
            while True:
                try:
                    event_id, action = self.queue.get(timeout=0.1)
                    if current_action is None:
                        current_action = action
                    elif current_action != action:
                        self.held = (event_id, action)
                        return self._construct_ack_or_nack_read_req(ids, current_action)
                    ids.append(shared_pb2.UUID(string=str(event_id)))
                    if len(ids) >= self._buffer_size:
                        return self._construct_ack_or_nack_read_req(ids, current_action)
                except queue.Empty as e:
                    if len(ids) >= 1:
                        assert current_action is not None
                        return self._construct_ack_or_nack_read_req(ids, current_action)
                    elif self._is_stopped:
                        raise StopIteration() from e
                    else:  # pragma: no cover
                        pass

    def ack(self, event_id: UUID) -> None:
        self.queue.put((event_id, "ack"))

    def nack(
        self,
        event_id: UUID,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        assert action in ["unknown", "park", "retry", "skip", "stop"]
        self.queue.put((event_id, action))

    def stop(self) -> None:
        self._is_stopped = True


class AsyncioPersistentSubscriptionsService(BasePersistentSubscriptionsService):
    pass


class PersistentSubscription(Iterator[RecordedEvent], BasePersistentSubscription):
    def __init__(
        self,
        reqs: SubscriptionReadReqs,
        resps: _MultiThreadedRendezvous,
        group_name: str,
        stream_name: Optional[str],
    ):
        self.reqs = reqs
        self.resps = resps
        read_resp = self._get_next_read_resp()
        content_oneof = read_resp.WhichOneof("content")

        if content_oneof == "subscription_confirmation":
            confirmed_stream_name, _, confirmed_group_name = (
                read_resp.subscription_confirmation.subscription_id.partition("::")
            )
            if confirmed_group_name != group_name or confirmed_stream_name != (
                stream_name if stream_name is not None else "$all"
            ):  # pragma: no cover
                raise SubscriptionConfirmationError()
        else:  # pragma: no cover
            raise ESDBClientException(
                f"Expected subscription confirmation, got: {read_resp}"
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
                return self._construct_recorded_event(read_resp)
            else:  # pragma: no cover
                pass

    def _get_next_read_resp(self) -> persistent_pb2.ReadResp:
        try:
            read_resp = next(self.resps)
        except RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(read_resp, persistent_pb2.ReadResp)
        return read_resp

    def stop(self) -> None:
        self.reqs.stop()
        self.resps.cancel()

    def ack(self, event_id: UUID) -> None:
        self.reqs.ack(event_id)

    def nack(
        self,
        event_id: UUID,
        action: Literal["unknown", "park", "retry", "skip", "stop"],
    ) -> None:
        self.reqs.nack(event_id, action=action)

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
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        """
        Signature for creating a persistent "all streams" subscription.
        """

    @overload
    def create(
        self,
        group_name: str,
        *,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
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
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        filter_by_stream_name: bool = False,
        consumer_strategy: ConsumerStrategy = "DispatchToSingle",
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        request = self._construct_create_req(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            commit_position=commit_position,
            stream_position=stream_position,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            filter_by_stream_name=filter_by_stream_name,
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
        except RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(response, persistent_pb2.CreateResp)

    def read(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        buffer_size: int = 100,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> PersistentSubscription:
        read_reqs = SubscriptionReadReqs(
            group_name=group_name,
            stream_name=stream_name,
            buffer_size=buffer_size,
        )
        read_resps: _MultiThreadedRendezvous = self._stub.Read(
            read_reqs,
            timeout=timeout,
            metadata=self._metadata(metadata, requires_leader=True),
            credentials=credentials,
        )
        return PersistentSubscription(
            reqs=read_reqs,
            resps=read_resps,
            group_name=group_name,
            stream_name=stream_name,
        )

    def get_info(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> SubscriptionInfo:
        req = self._construct_get_info_req(group_name, stream_name)
        try:
            resp = self._stub.GetInfo(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            if (
                e.code() == StatusCode.UNAVAILABLE
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
        credentials: Optional[CallCredentials] = None,
    ) -> List[SubscriptionInfo]:
        req = self._construct_list_req(stream_name)
        try:
            resp = self._stub.List(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            if e.code() == StatusCode.NOT_FOUND:
                return []
            elif (
                e.code() == StatusCode.UNAVAILABLE
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
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        """
        Signature for updating a persistent "all streams" subscription.
        """

    @overload
    def update(
        self,
        group_name: str,
        *,
        stream_name: Optional[str] = None,
        from_end: bool = False,
        stream_position: Optional[int] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
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
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        request = self._construct_update_req(
            group_name=group_name,
            stream_name=stream_name,
            from_end=from_end,
            commit_position=commit_position,
            stream_position=stream_position,
        )
        # Call 'Update' RPC.
        try:
            response = self._stub.Update(
                request,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(response, persistent_pb2.UpdateResp)

    def delete(
        self,
        group_name: str,
        stream_name: Optional[str] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Metadata] = None,
        credentials: Optional[CallCredentials] = None,
    ) -> None:
        req = self._construct_delete_req(group_name, stream_name)
        try:
            resp = self._stub.Delete(
                req,
                timeout=timeout,
                metadata=self._metadata(metadata, requires_leader=True),
                credentials=credentials,
            )
        except RpcError as e:
            raise handle_rpc_error(e) from e
        assert isinstance(resp, persistent_pb2.DeleteResp)
