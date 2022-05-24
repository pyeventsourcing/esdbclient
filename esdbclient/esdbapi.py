# -*- coding: utf-8 -*-
import sys
from typing import Iterable, List, Optional, Sequence, overload
from uuid import uuid4

from grpc import Call, Channel, RpcError, StatusCode
from typing_extensions import Literal

from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import (
    DeadlineExceeded,
    ExpectedPositionError,
    GrpcError,
    ServiceUnavailable,
    StreamNotFound,
)
from esdbclient.protos.Grpc.shared_pb2 import UUID, Empty, StreamIdentifier
from esdbclient.protos.Grpc.streams_pb2 import AppendReq, AppendResp, ReadReq, ReadResp
from esdbclient.protos.Grpc.streams_pb2_grpc import StreamsStub


def handle_rpc_error(e: RpcError) -> None:
    if isinstance(e, Call):
        if e.code() == StatusCode.UNAVAILABLE:
            raise ServiceUnavailable(e)
        elif e.code() == StatusCode.DEADLINE_EXCEEDED:
            raise DeadlineExceeded(e)
    raise GrpcError(e) from None


class Streams:
    def __init__(self, channel: Channel):
        self.stub = StreamsStub(channel)

    @overload
    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        ...  # pragma: no cover

    @overload
    def read(
        self,
        *,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        ...  # pragma: no cover

    @overload
    def read(
        self,
        *,
        commit_position: Optional[int] = None,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        subscribe: Literal[True],
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        ...  # pragma: no cover

    def read(
        self,
        *,
        stream_name: Optional[str] = None,
        stream_position: Optional[int] = None,
        commit_position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
        limit: int = sys.maxsize,
        subscribe: bool = False,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Returns iterable of recorded events from the server.

        Either call with `stream_name` and optional `stream_position`
        to read from a given stream, or call without `stream_name`
        and with optional `commit_position` to read from "all streams".

        Calling with `backwards=True` will read events in reverse
        order. Otherwise, events will be read "forwards" in the
        order they were appended (the default is `backwards=False`).

        Reading forwards without specifying a given position will read from
        the start of either the named stream or all streams, and the first
        event will be included. Reading backwards without specifying a given
        position will read from the end of either the named stream or all
        streams, and the last event will be included.

        When reading forwards from either a stream position or a given commit
        position, the event at that position will be included. When reading
        backwards from a given stream position, the event at that position
        will also be included. However, please note, when reading backwards
        from a given commit position, the event at that position will NOT
        be included.

        Calling with `limit` (an integer) will limit the number of
        returned events.

        :param stream_name: Name of the stream.
        :param stream_position: Position in the stream to start reading.
        :param commit_position: Position in the stream to start reading.
        :param backwards: Direction in which to read.
        :param filter_exclude: Sequence of expressions to exclude.
        :param filter_include: Sequence of expressions to include.
        :param limit: Maximum number of events in response.
        :param subscribe: Set True to read future events (default False).
        :param timeout: Optional integer, specifying timeout for operation.
        :return: Iterable of committed events.
        """
        if stream_name is not None:
            assert isinstance(stream_name, str)
            assert commit_position is None
            stream_options = ReadReq.Options.StreamOptions(
                stream_identifier=StreamIdentifier(
                    stream_name=stream_name.encode("utf8")
                ),
                revision=stream_position,
                start=Empty() if stream_position is None and not backwards else None,
                end=Empty() if stream_position is None and backwards else None,
            )
            all_options = None
        else:
            assert stream_position is None
            if isinstance(commit_position, int):
                position = ReadReq.Options.Position(
                    commit_position=commit_position,
                    prepare_position=commit_position,
                )
            else:
                position = None
            stream_options = None
            all_options = ReadReq.Options.AllOptions(
                position=position,
                start=Empty() if position is None and not backwards else None,
                end=Empty() if position is None and backwards else None,
            )

        if backwards is False:
            read_direction = ReadReq.Options.Forwards
        else:
            read_direction = ReadReq.Options.Backwards

        if all_options is not None and (filter_exclude or filter_include):
            if filter_include:
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"

            filter_options = ReadReq.Options.FilterOptions(
                stream_identifier=None,
                event_type=ReadReq.Options.FilterOptions.Expression(regex=filter_regex),
                count=Empty(),  # Todo: Figure out what 'window' should be.
            )
        else:
            filter_options = None
        if subscribe:
            subscription_options = ReadReq.Options.SubscriptionOptions()
        else:
            subscription_options = None
        request = ReadReq(
            options=ReadReq.Options(
                stream=stream_options,
                all=all_options,
                read_direction=read_direction,
                resolve_links=False,
                count=limit,
                subscription=subscription_options,
                filter=filter_options,
                no_filter=Empty() if filter_options is None else None,
                uuid_option=ReadReq.Options.UUIDOption(
                    structured=Empty(), string=Empty()
                ),
            )
        )
        try:
            for response in self.stub.Read(request, timeout=timeout):
                assert isinstance(response, ReadResp)
                if response.WhichOneof("content") == "stream_not_found":
                    raise StreamNotFound(f"Stream '{stream_name}' not found")
                yield RecordedEvent(
                    type=response.event.event.metadata["type"],
                    data=response.event.event.data,
                    metadata=response.event.event.custom_metadata,
                    stream_name=response.event.event.stream_identifier.stream_name.decode(
                        "utf8"
                    ),
                    stream_position=response.event.event.stream_revision,
                    commit_position=response.event.commit_position,
                )
        except RpcError as e:
            handle_rpc_error(e)

    def append(
        self,
        stream_name: str,
        expected_position: Optional[int],
        new_events: List[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        """
        Appends events to named stream.

        :param stream_name: Name of the stream.
        :param expected_position: Expected stream position. This should be
          `None` for a new stream, otherwise it should be the position of
          the last recorded event in the stream (an non-negative integer).
          If a negative integer is given, optimistic concurrency control
          will be disabled.
        :param new_events: New events to be appended.
        :return: Commit position after events have been appended. This is the
           commit position of the last event to be appended either in this call,
           or in the previous call if zero events were given to be appended.
        """
        if expected_position is None:
            no_stream = Empty()
            any = None
        else:
            assert isinstance(expected_position, int)
            no_stream = None
            if expected_position >= 0:
                any = None
            else:
                expected_position = None
                any = Empty()

        requests = [
            AppendReq(
                options=AppendReq.Options(
                    stream_identifier=StreamIdentifier(
                        stream_name=stream_name.encode("utf8")
                    ),
                    revision=expected_position,
                    no_stream=no_stream,
                    any=any,
                    # stream_exists=None,
                ),
            )
        ]
        for event in new_events:
            requests.append(
                AppendReq(
                    proposed_message=AppendReq.ProposedMessage(
                        id=UUID(string=str(uuid4())),
                        metadata={
                            "type": event.type,
                            "content-type": "application/octet-stream",
                        },
                        custom_metadata=event.metadata,
                        data=event.data,
                    )
                )
            )
        try:
            response = self.stub.Append(iter(requests), timeout=timeout)
        except RpcError as e:
            handle_rpc_error(e)

        assert isinstance(response, AppendResp)
        if response.WhichOneof("result") == "success":
            return response.success.position.commit_position

        if (
            response.wrong_expected_version.WhichOneof("current_revision_option")
            == "current_revision"
        ):
            current_position = response.wrong_expected_version.current_revision
            raise ExpectedPositionError(f"Current position is {current_position}")
        else:
            raise ExpectedPositionError(f"Stream '{stream_name}' does not exist")
