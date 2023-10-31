# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    ESDB_SYSTEM_EVENTS_REGEX,
    Checkpoint,
    EventStoreDBClient,
    NewEvent,
    StreamState,
)
from esdbclient.streams import CatchupSubscription, RecordedEvent
from tests.test_client import get_server_certificate

ESDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = EventStoreDBClient(
    uri=f"esdb://admin:changeit@{ESDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(ESDB_TARGET),
)


def handle_event(ev: RecordedEvent, sub: CatchupSubscription):
    print(f"handling event: {ev.stream_position} {ev.type}")
    sub.stop()


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
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")

    # do something with the event
    handle_event(event, subscription)
# endregion event-type-prefix

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
    filter_include=["user.*", "company.*"],
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")

    # do something with the event
    handle_event(event, subscription)
# endregion event-type-regex

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

    # do something with the event
    handle_event(event, subscription)
# endregion stream-prefix

client.append_to_stream(
    stream_name="user-stream",
    current_version=StreamState.ANY,
    events=event_data,
)

# region stream-regex
subscription = client.subscribe_to_all(
    filter_by_stream_name=True,
    filter_include=["user.*", "company.*"],
)
for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")

    # do something with the event
    handle_event(event, subscription)
# endregion stream-regex

# region checkpoint-with-interval
subscription = client.subscribe_to_all(
    filter_by_stream_name=False,
    filter_exclude=[ESDB_SYSTEM_EVENTS_REGEX],
    include_checkpoints=True,
    checkpoint_interval_multiplier=1000,
)
# endregion checkpoint-with-interval

# region checkpoint
for event in subscription:
    if isinstance(event, Checkpoint):
        print(f"checkpoint taken at {event.commit_position}")
        break
# endregion checkpoint

client.close()
