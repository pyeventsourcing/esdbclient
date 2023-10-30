# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    ESDB_SYSTEM_EVENTS_REGEX,
    EventStoreDBClient,
    NewEvent,
    StreamState,
)

client = EventStoreDBClient(uri="esdb://localhost:2113?tls=false")

# region exclude-system
subscription = client.subscribe_to_all(
    filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX]
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    break

# endregion exclude-system

stream_name = str(uuid4())

event_data = NewEvent(
    type="customer-one",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data,
)

# region event-type-prefix
subscription = client.subscribe_to_all(
    filter_by_stream_name=False,
    filter_include=r"customer-\w+",
    # commit_position=0,  # ! How to start from end
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    # endregion event-type-prefix
    break

event_data_one = NewEvent(
    type="user-one",
    data=b'{"id": "1", "important_data": "some value"}',
)
event_data_two = NewEvent(
    type="company-one",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data_one,
)
client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data_two,
)

# region event-type-regex
subscription = client.subscribe_to_all(
    filter_by_stream_name=False,
    filter_include=["^user|^company"],
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    # endregion event-type-regex
    break


event_data = NewEvent(
    type="test-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name="test-stream",
    current_version=StreamState.ANY,
    events=event_data,
)

# region stream-prefix
subscription = client.subscribe_to_all(
    filter_by_stream_name=True,
    filter_include=[r"test-\w+"],
)
for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    # endregion stream-prefix
    break

client.append_to_stream(
    stream_name="user-stream",
    current_version=StreamState.ANY,
    events=event_data,
)

# region stream-regex
subscription = client.subscribe_to_all(
    filter_by_stream_name=True,
    filter_include=["^user|^company"],
)
for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    # endregion stream-regex
    break

# region checkpoint-with-interval
subscription = client.subscribe_to_all(
    filter_by_stream_name=False,
    filter_include="/^[^\\$].*/",
    include_checkpoints=True,  # ! How to handle checkpoint reached event
)
# endregion checkpoint-with-interval

# region checkpoint
for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    break
# endregion checkpoint

client.close()
