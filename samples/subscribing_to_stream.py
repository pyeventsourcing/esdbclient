# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    EventStoreDBClient,
    NewEvent,
    StreamState,
    exceptions,
)
from esdbclient.exceptions import ConsumerTooSlow
from esdbclient.streams import CatchupSubscription, RecordedEvent
from tests.test_client import get_server_certificate

DEBUG = False
_print = print


def print(*args):
    if DEBUG:
        _print(*args)


ESDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = EventStoreDBClient(
    uri=f"esdb://admin:changeit@{ESDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(ESDB_TARGET),
)

stream_name = str(uuid4())

client.append_to_stream(
    stream_name,
    events=[NewEvent(type="test-event", data=b"") for _ in range(5)],
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


# region subscribe-to-all
subscription = client.subscribe_to_all(from_end=True)

# Append some events
event = client.append_to_stream(
    stream_name=stream_name,
    events=[NewEvent(type="MyEventType", data=b"")],
    current_version=StreamState.ANY,
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")

    # do something with the event
    handle_event(event)
# endregion subscribe-to-all


# region subscribe-to-stream-from-position
subscription = client.subscribe_to_stream(
    stream_name=stream_name,
    stream_position=20,
)
# endregion subscribe-to-stream-from-position
subscription.stop()


try:
    # region subscribe-to-all-from-position
    subscription = client.subscribe_to_all(
        commit_position=1_056,
    )
    # endregion subscribe-to-all-from-position
except exceptions.GrpcError:
    # Commit position does not exist in this sample database, so we are skipping
    pass


# region subscribe-to-stream-live
subscription = client.subscribe_to_stream(
    stream_name=stream_name,
    from_end=True,
)
# endregion subscribe-to-stream-live
subscription.stop()

# region subscribe-to-all-live
subscription = client.subscribe_to_all(from_end=True)

# Append some events
commit_position = client.append_to_stream(
    stream_name,
    events=NewEvent(type="MyEventType", data=b""),
    current_version=StreamState.ANY,
)

# Retrieve the event we just appended from our subscription.
next_event = next(subscription)
# endregion subscribe-to-all-live
subscription.stop()


# region subscribe-to-stream-resolving-linktos
subscription = client.subscribe_to_stream(
    stream_name="$et-MyEventType",
    stream_position=0,
    resolve_links=True,
)
# endregion subscribe-to-stream-resolving-linktos
subscription.stop()


# region subscribe-to-stream-subscription-dropped
# get last recorded stream position
last_stream_position = 0

while True:
    subscription = client.subscribe_to_stream(
        stream_name=stream_name,
        stream_position=last_stream_position,
    )
    try:
        for event in subscription:
            # remember stream position
            last_stream_position = event.stream_position

            # record stream position
            handle_event(event)

    except ConsumerTooSlow:
        # subscription was dropped
        continue
    # endregion subscribe-to-stream-subscription-dropped
    break


# region subscribe-to-all-subscription-dropped
# get last recorded commit position
last_commit_position = 0

while True:
    subscription = client.subscribe_to_all(
        commit_position=last_commit_position,
    )
    try:
        for event in subscription:
            # remember commit position
            last_commit_position = event.commit_position

            # record commit position
            handle_event(event)

    except ConsumerTooSlow:
        # subscription was dropped
        continue
    # endregion subscribe-to-all-subscription-dropped
    break


# region overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

subscription = client.subscribe_to_all(
    credentials=credentials,
)
# endregion overriding-user-credentials


# region stream-prefix-filtered-subscription
client.subscribe_to_all(
    filter_include=[r"test-\w+"],
    filter_by_stream_name=True,
)
# endregion stream-prefix-filtered-subscription


client.close()
