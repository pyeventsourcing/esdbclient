# -*- coding: utf-8 -*-
import re
import sys
from typing import Iterable, Iterator, List, Optional, Pattern, Sequence

import grpc

from esdbclient.esdbapi import Streams
from esdbclient.events import NewEvent, RecordedEvent
from esdbclient.exceptions import StreamNotFound

SYSTEM_EVENTS_REGEX = "\\$.*"


class CatchupSubscription:
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
            if event.type == "" and event.stream_name == "":
                # Todo: What is this? occurs several times (has commit_position=0)
                continue
            if self.filter_regex is None:
                yield event
            elif re.match(self.filter_regex, event.type):
                yield event


class EsdbClient:
    def __init__(self, uri: str) -> None:
        self.uri = uri
        self.channel = grpc.insecure_channel(self.uri)
        self.streams = Streams(self.channel)

    def append_events(
        self,
        stream_name: str,
        expected_position: Optional[int],
        events: List[NewEvent],
        timeout: Optional[float] = None,
    ) -> int:
        return self.streams.append(
            stream_name=stream_name,
            expected_position=expected_position,
            new_events=events,
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
        filter_exclude: Sequence[str] = (SYSTEM_EVENTS_REGEX,),
        filter_include: Sequence[str] = (),
        limit: int = sys.maxsize,
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
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
        filter_exclude: Sequence[str] = (SYSTEM_EVENTS_REGEX,),
    ) -> int:
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
        filter_exclude: Sequence[str] = (SYSTEM_EVENTS_REGEX,),
        filter_include: Sequence[str] = (),
        timeout: Optional[float] = None,
    ) -> Iterable[RecordedEvent]:
        response = self.streams.read(
            commit_position=position,
            subscribe=True,
            timeout=timeout,
        )
        return CatchupSubscription(
            event_generator=response,
            filter_exclude=filter_exclude,
            filter_include=filter_include,
        )
