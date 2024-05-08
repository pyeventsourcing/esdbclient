# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from datetime import datetime
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

    def __eq__(self, other: object) -> bool:
        return isinstance(other, (NewEvent, RecordedEvent)) and self.id == other.id


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
    prepare_position: Optional[int]
    recorded_at: Optional[datetime] = None
    link: Optional["RecordedEvent"] = None
    retry_count: Optional[int] = None

    @property
    def ack_id(self) -> UUID:
        if self.link is not None:
            return self.link.id
        else:
            return self.id

    @property
    def is_system_event(self) -> bool:
        return self.type.startswith("$")

    @property
    def is_link_event(self) -> bool:
        return self.type == "$>"

    @property
    def is_resolved_event(self) -> bool:
        return self.link is not None

    @property
    def is_checkpoint(self) -> bool:
        return False

    @property
    def is_caught_up(self) -> bool:
        return False

    # @property
    # def is_fell_behind(self) -> bool:
    #     return False


@dataclass(frozen=True)
class Checkpoint(RecordedEvent):
    CHECKPOINT_ID = UUID("00000000-0000-0000-0000-000000000000")

    def __init__(self, commit_position: int, prepare_position: int) -> None:
        super().__init__(
            id=Checkpoint.CHECKPOINT_ID,
            type="",
            data=b"",
            content_type="",
            metadata=b"",
            stream_name="",
            stream_position=0,
            commit_position=commit_position,
            prepare_position=prepare_position,
        )

    @property
    def is_checkpoint(self) -> bool:
        return True


@dataclass(frozen=True)
class CaughtUp(RecordedEvent):
    CAUGHT_UP_ID = UUID("00000000-0000-0000-0000-000000000000")

    def __init__(self) -> None:
        super().__init__(
            id=CaughtUp.CAUGHT_UP_ID,
            type="",
            data=b"",
            content_type="",
            metadata=b"",
            stream_name="",
            stream_position=0,
            commit_position=0,
            prepare_position=0,
        )

    @property
    def is_caught_up(self) -> bool:
        return True


# @dataclass(frozen=True)
# class FellBehind(RecordedEvent):
#     FELL_BEHIND_ID = UUID("00000000-0000-0000-0000-000000000000")
#
#     def __init__(self) -> None:
#         super().__init__(
#             id=FellBehind.FELL_BEHIND_ID,
#             type="",
#             data=b"",
#             content_type="",
#             metadata=b"",
#             stream_name="",
#             stream_position=0,
#             commit_position=0,
#         )
#
#     @property
#     def is_fell_behind(self) -> bool:
#         return True
