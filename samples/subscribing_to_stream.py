# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    EventStoreDBClient,
    NewEvent,
    StreamState,
    exceptions,
)
from esdbclient.streams import CatchupSubscription, RecordedEvent
from tests.test_client import get_server_certificate

ESDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = EventStoreDBClient(
    uri=f"esdb://admin:changeit@{ESDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(ESDB_TARGET),
)

stream_name = str(uuid4())

for _ in range(5):
    event_data = NewEvent(type="test-event", data=b"")

    event = client.append_to_stream(
        stream_name,
        events=event_data,
        current_version=StreamState.ANY,
    )


subscription: CatchupSubscription


def handle_event(ev: RecordedEvent):
    print(f"handling event: {ev.stream_position} {ev.type}")
    global subscription
    subscription.stop()


# region subscribe-to-stream
subscription = client.subscribe_to_stream(stream_name)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")

    # do something with the event
    handle_event(event)
# endregion subscribe-to-stream

# region subscribe-to-stream-from-position
client.subscribe_to_stream(stream_name, stream_position=20)
# endregion subscribe-to-stream-from-position

# region subscribe-to-stream-live
client.subscribe_to_stream(stream_name)
# endregion subscribe-to-stream-live

# region subscribe-to-stream-resolving-linktos
client.subscribe_to_stream(
    stream_name="$et-myEventType",
    stream_position=0,
    resolve_links=True,
)
# endregion subscribe-to-stream-resolving-linktos

# region subscribe-to-stream-subscription-dropped
subscription = client.subscribe_to_stream(
    stream_name,
    stream_position=0,
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    # endregion subscribe-to-stream-subscription-dropped
    subscription.stop()

# region subscribe-to-all
subscription = client.subscribe_to_all(from_end=True)

# Append some events
event = client.append_to_stream(
    stream_name,
    events=NewEvent(type="test-event", data=b""),
    current_version=StreamState.ANY,
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")

    # do something with the event
    handle_event(event)
# endregion subscribe-to-all

try:
    # region subscribe-to-all-from-position
    client.subscribe_to_all(commit_position=1_056)
    # endregion subscribe-to-all-from-position
except exceptions.GrpcError:
    # Commit position does not exist in this sample database, so we are skipping
    pass

# region subscribe-to-all-live
subscription = client.subscribe_to_all(from_end=True)

# Append some events
commit_position = client.append_to_stream(
    stream_name,
    events=NewEvent(type="test-event", data=b""),
    current_version=StreamState.ANY,
)

# Retrieve the event we just appended from our subscription.
next_event = next(subscription)
# endregion subscribe-to-all-live
assert next_event.commit_position == commit_position


# region subscribe-to-all-subscription-dropped
subscription = client.subscribe_to_all(commit_position=0)

try:
    for event in subscription:
        print(f"received event: {event.stream_position} {event.type}")

        # do something with the event
        handle_event(event)
except exceptions.AbortedByServer:
    print("Subscription aborted by server")
except Exception as e:
    print("Something else went wrong", e)
# endregion subscribe-to-all-subscription-dropped


# region stream-prefix-filtered-subscription
client.subscribe_to_all(
    filter_include=[r"test-\w+"],
    filter_by_stream_name=True,
)
# endregion stream-prefix-filtered-subscription

# region overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

client.subscribe_to_all(credentials=credentials)
# endregion overriding-user-credentials


client.close()
