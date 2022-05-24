# -*- coding: utf-8 -*-
import re
import sys
from typing import Iterator, List, Optional, Pattern, Sequence

import grpc

from esdbclient.esdbapi import Streams
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import StreamNotFound


class CatchupSubscription:
    def __init__(
        self,
        event_generator: Iterator[RecordedEvent],
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
        while True:
            recorded_event = next(self.event_generator)
            if recorded_event.type == "" and recorded_event.stream_name == "":
                continue  # Todo: What is this? occurs several times (has commit_position=0)
            if self.filter_regex is None:
                yield recorded_event
            elif re.match(self.filter_regex, recorded_event.type):
                yield recorded_event


class EsdbClient:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.channel = grpc.insecure_channel(self.uri)
        self.streams = Streams(self.channel)

    def append_events(
        self, stream_name: str, expected_position: Optional[int], events: List[NewEvent]
    ) -> int:
        return self.streams.append(
            stream_name=stream_name,
            expected_position=expected_position,
            new_events=events,
        )

    def read_stream_events(
        self,
        stream_name: str,
        position: Optional[int] = None,
        backwards: bool = False,
        limit: int = sys.maxsize,
    ) -> Iterator[RecordedEvent]:
        return self.streams.read(
            stream_name=stream_name,
            stream_position=position,
            backwards=backwards,
            limit=limit,
        )

    def read_all_events(
        self,
        position: Optional[int] = None,
        backwards: bool = False,
        filter_exclude: Sequence[str] = ("\\$.*",),  # Exclude "system events".
        filter_include: Sequence[str] = (),
        limit: int = sys.maxsize,
    ) -> Iterator[RecordedEvent]:
        return self.streams.read(
            commit_position=position,
            backwards=backwards,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
            limit=limit,
        )

    def get_stream_position(self, stream_name: str) -> Optional[int]:
        try:
            last_event = list(
                self.streams.read(
                    stream_name=stream_name,
                    backwards=True,
                    limit=1,
                )
            )[0]
        except StreamNotFound:
            return None
        else:
            return last_event.stream_position

    def get_commit_position(self) -> int:
        recorded_events = self.read_all_events(
            backwards=True,
            filter_exclude=("\\$.*", ".*Snapshot"),
            limit=1,
        )
        commit_position = 0
        for ev in recorded_events:
            commit_position = ev.commit_position
        return commit_position

    def subscribe_all_events(
        self,
        position: Optional[int] = None,
        filter_exclude: Sequence[str] = ("\\$.*",),  # Exclude "system events".
        filter_include: Sequence[str] = (),
    ) -> CatchupSubscription:
        response = self.streams.read(commit_position=position, subscribe=True)
        return CatchupSubscription(
            event_generator=response,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
        )
