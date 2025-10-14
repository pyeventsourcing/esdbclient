from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from typing_extensions import Literal

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

ContentType = Literal["application/json", "application/octet-stream"]


@dataclass(frozen=True)
class NewEvent:
    """
    Encapsulates event data to be recorded in KurrentDB.
    """

    type: str
    data: bytes
    metadata: bytes = b""
    content_type: ContentType = "application/json"
    id: UUID = field(default_factory=uuid4)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, (NewEvent, RecordedEvent)) and self.id == other.id


@dataclass(frozen=True)
class NewEvents:
    """
    Encapsulates multiple event data to be recorded in a KurrentDB stream.
    """

    stream_name: str
    events: Iterable[NewEvent]
    current_version: int | StreamState


@dataclass(frozen=True)
class RecordedEvent:
    """
    Encapsulates event data that has been recorded in KurrentDB.
    """

    type: str
    data: bytes
    metadata: bytes
    content_type: str
    id: UUID
    stream_name: str
    stream_position: int
    commit_position: int
    prepare_position: int
    recorded_at: datetime | None = None
    link: RecordedEvent | None = None
    retry_count: int | None = None

    @property
    def ack_id(self) -> UUID:
        if self.link is not None:
            return self.link.id
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

    @property
    def is_fell_behind(self) -> bool:
        return False


@dataclass(frozen=True)
class Checkpoint(RecordedEvent):
    CHECKPOINT_ID = UUID("00000000-0000-0000-0000-000000000000")

    def __init__(
        self,
        commit_position: int,
        prepare_position: int,
        recorded_at: datetime | None,
    ) -> None:
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
            recorded_at=recorded_at,
        )

    @property
    def is_checkpoint(self) -> bool:
        return True


@dataclass(frozen=True)
class CaughtUp(RecordedEvent):
    CAUGHT_UP_ID = UUID("00000000-0000-0000-0000-000000000000")

    def __init__(
        self,
        stream_position: int,
        commit_position: int,
        prepare_position: int,
        recorded_at: datetime | None,
    ) -> None:
        super().__init__(
            id=CaughtUp.CAUGHT_UP_ID,
            type="",
            data=b"",
            content_type="",
            metadata=b"",
            stream_name="",
            stream_position=stream_position,
            commit_position=commit_position,
            prepare_position=prepare_position,
            recorded_at=recorded_at,
        )

    @property
    def is_caught_up(self) -> bool:
        return True


@dataclass(frozen=True)
class FellBehind(RecordedEvent):
    FELL_BEHIND_ID = UUID("00000000-0000-0000-0000-000000000000")

    def __init__(
        self,
        stream_position: int,
        commit_position: int,
        prepare_position: int,
        recorded_at: datetime | None,
    ) -> None:
        super().__init__(
            id=FellBehind.FELL_BEHIND_ID,
            type="",
            data=b"",
            content_type="",
            metadata=b"",
            stream_name="",
            stream_position=stream_position,
            commit_position=commit_position,
            prepare_position=prepare_position,
            recorded_at=recorded_at,
        )

    @property
    def is_fell_behind(self) -> bool:
        return True


class StreamState(Enum):
    ANY = "ANY"
    NO_STREAM = "NO_STREAM"
    EXISTS = "EXISTS"
