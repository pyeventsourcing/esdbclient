# -*- coding: utf-8 -*-
import re
import sys
from typing import Iterable, Iterator, Optional, Pattern, Sequence, Tuple

import grpc

from esdbclient.esdbapi import (
    Streams,
    SubscriptionReadRequest,
    SubscriptionReadResponse,
    Subscriptions,
)
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import StreamNotFound

ESDB_EVENTS_REGEX = "\\$.*"  # Matches the 'type' of system events.


class EsdbClient:
    """
    Encapsulates the EventStoreDB gRPC API.
    """

    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.channel = grpc.insecure_channel(self.uri)
        self.streams = Streams(self.channel)
        self.subscriptions = Subscriptions(self.channel)

    def append_events(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: Iterable[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        """
        Appends new events to the named stream.
        """
        return self.streams.append(
            stream_name=stream_name,
            expected_position=expected_position,
            events=events,
            timeout=timeout,
        )

    def read_stream_events(
        self,
        stream_name: str,
        position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Reads recorded events from the named stream.
        """
        return self.streams.read(
            stream_name=stream_name,
            stream_position=position,
            backwards=backwards,
            limit=limit,
            timeout=timeout,
        )

    def read_all_events(
        self,
        position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = (ESDB_EVENTS_REGEX,),
        filter_include: Sequence[str] = (),
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Reads recorded events in "all streams" in the database.
        """
        return self.streams.read(
            commit_position=position,
            backwards=backwards,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            limit=limit,
            timeout=timeout,
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
                self.streams.read(
                    stream_name=stream_name,
                    backwards=True,
                    limit=1,
                    timeout=timeout,
                )
            )[0]
        except StreamNotFound:
            return None
        else:
            return last_event.stream_position

    def get_commit_position(
        self,
        timeout: Optional[float] = None,
        filter_exclude: Sequence[str] = (ESDB_EVENTS_REGEX,),
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
            commit_position = ev.commit_position
        return commit_position

    def subscribe_all_events(
        self,
        position: Optional[int] = None,
        filter_exclude: Sequence[str] = (ESDB_EVENTS_REGEX,),
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        """
        Returns a catch-up subscription, from which recorded
        events in "all streams" in the database can be received.
        """
        event_generator = self.streams.read(
            commit_position=position,
            subscribe=True,
            timeout=timeout,
        )
        return CatchupSubscription(
            event_generator=event_generator,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
        )

    def create_subscription(self, name: str, position: int) -> None:
        self.subscriptions.create(name, position=position)

    def read_subscription(
        self, group_name: str
    ) -> Tuple[SubscriptionReadRequest, SubscriptionReadResponse]:
        return self.subscriptions.read(group_name=group_name)


class CatchupSubscription:
    """
    Encapsulates generator of recorded events when
    reading events in a catch-up subscription.

    Filters recorded events, because the server doesn't.
    """

    def __init__(
        self,
        event_generator: Iterable[RecordedEvent],
        filter_exclude: Sequence[str] = (),
        filter_include: Sequence[str] = (),
    ):
        self.event_generator = event_generator
        if filter_exclude or filter_include:
            if filter_include:
                filter_regex = "^" + "|".join(filter_include) + "$"
            else:
                filter_regex = "^(?!(" + "|".join(filter_exclude) + ")).*$"
            self.filter_regex: Optional[Pattern[str]] = re.compile(filter_regex)
        else:
            self.filter_regex = None

    def __iter__(self) -> Iterator[RecordedEvent]:
        for event in self.event_generator:
            if self.filter_regex is None:
                yield event
            elif re.match(self.filter_regex, event.type):
                yield event
