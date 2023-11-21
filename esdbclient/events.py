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
    retry_count: Optional[int] = None
    link: Optional["RecordedEvent"] = None


class Checkpoint(RecordedEvent):
    CHECKPOINT_ID = UUID("00000000-0000-0000-0000-000000000000")

    def __init__(self, commit_position: int):
        super().__init__(
            id=self.CHECKPOINT_ID,
            type="",
            data=b"",
            content_type="",
            metadata=b"",
            stream_name="",
            stream_position=0,
            commit_position=commit_position,
        )
