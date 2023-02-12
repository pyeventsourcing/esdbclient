# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from typing import Optional
from uuid import UUID, uuid4

from typing_extensions import Literal

ContentType = Literal["application/json", "application/octet-stream"]


@dataclass(frozen=True)
class NewEvent:
    """
    Encapsulates event data to be recorded in EventStoreDB.
    """

    type: str
    data: bytes
    metadata: bytes = b""
    content_type: ContentType = "application/json"
    id: UUID = field(default_factory=uuid4)


@dataclass(frozen=True)
class RecordedEvent:
    """
    Encapsulates event data that has been recorded in EventStoreDB.
    """

    type: str
    data: bytes
    metadata: bytes
    content_type: str
    id: UUID
    stream_name: str
    stream_position: int
    commit_position: Optional[int]
