# -*- coding: utf-8 -*-
from dataclasses import dataclass


@dataclass(frozen=True)
class NewEvent:
    type: str
    data: bytes
    metadata: bytes


@dataclass(frozen=True)
class RecordedEvent(NewEvent):
    stream_name: str
    stream_position: int
    commit_position: int
