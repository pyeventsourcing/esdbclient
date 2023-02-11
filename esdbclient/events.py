# -*- coding: utf-8 -*-
from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class NewEvent:
    """
    Encapsulates event data to be recorded in EventStoreDB.
    """

    type: str
    data: bytes
    metadata: bytes
    # Todo: Idempotent write needs to use the same event ID.
    # event_id: Optional[UUID] = None


@dataclass(frozen=True)
class RecordedEvent(NewEvent):
    """
    Encapsulates event data that has been recorded in EventStoreDB.
    """

    id: UUID
    stream_name: str
    stream_position: int
    commit_position: int
