# ruff: noqa: S106
from time import sleep
from typing import Optional
from uuid import uuid4

from kurrentdbclient import (
    KurrentDBClient,
    NewEvent,
    StreamState,
)
from kurrentdbclient.exceptions import ConsumerTooSlowError
from kurrentdbclient.streams import CatchupSubscription, RecordedEvent
from tests.test_client import get_server_certificate

DEBUG = False
_print = print


def print(*args):  # noqa: A001
    if DEBUG:
        _print(*args)


KDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = KurrentDBClient(
    uri=f"kurrentdb://admin:changeit@{KDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(KDB_TARGET),
)

stream_name = "MyStreamType-" + str(uuid4())

client.append_to_stream(
    stream_name,
    events=[
        NewEvent(type="test-event", data=b"{}") for _ in range(5)
    ],
    current_version=StreamState.ANY,
)


subscription: CatchupSubscription


def handle_event(ev: RecordedEvent):
    print(f"handling event: {ev.stream_position} {ev.type}")
    subscription.stop()


# region subscribe-to-stream
with client.subscribe_to_stream(stream_name) as subscription:
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-stream


# region subscribe-to-all
with client.subscribe_to_all() as subscription:
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-all


# region subscribe-to-stream-from-position
with client.subscribe_to_stream(
    stream_name=stream_name,
    stream_position=3,
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-stream-from-position

# region subscribe-to-all-from-position
with client.subscribe_to_all(
    commit_position=0,
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-all-from-position


# region subscribe-to-stream-live
with client.subscribe_to_stream(
    stream_name=stream_name,
    from_end=True,
) as subscription:

    # Elsewhere, append new events...
    commit_position = client.append_to_stream(
        stream_name=stream_name,
        events=NewEvent(type="MyEventType", data=b"{}"),
        current_version=StreamState.ANY,
    )

    # Receive new events...
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-stream-live


# region subscribe-to-all-live
with client.subscribe_to_all(from_end=True) as subscription:

    # Elsewhere, append new events...
    commit_position = client.append_to_stream(
        stream_name=stream_name,
        events=NewEvent(type="MyEventType", data=b"{}"),
        current_version=StreamState.ANY,
    )

    # Receive new events...
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-all-live


# Allow time for system projection to process event.
sleep(1)
# Check there is a "$et-MyEventType" stream by now.
events = client.get_stream("$et-MyEventType")


# region subscribe-to-stream-resolving-linktos
with client.subscribe_to_stream(
    stream_name="$et-MyEventType",
    resolve_links=True,
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion subscribe-to-stream-resolving-linktos


def get_recorded_stream_position() -> Optional[int]:  # noqa: FA100
    return None


# region subscribe-to-stream-subscription-dropped
while True:
    with client.subscribe_to_stream(
        stream_name=stream_name,
        stream_position=get_recorded_stream_position(),
    ) as subscription:
        try:
            for event in subscription:
                # record stream position
                handle_event(event)

        except ConsumerTooSlowError:
            # subscription was dropped
            continue
    # endregion subscribe-to-stream-subscription-dropped
    break


def get_recorded_commit_position() -> Optional[int]:  # noqa: FA100
    return 0


# region subscribe-to-all-subscription-dropped
while True:
    with client.subscribe_to_all(
        commit_position=get_recorded_commit_position(),
    ) as subscription:
        try:
            for event in subscription:
                # record commit position
                handle_event(event)

        except ConsumerTooSlowError:
            # subscription was dropped
            continue
    # endregion subscribe-to-all-subscription-dropped
    break


# region overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

with client.subscribe_to_all(
    credentials=credentials,
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion overriding-user-credentials


# region stream-prefix-filtered-subscription
with client.subscribe_to_all(
    filter_include=[r"MyStreamType-.*"],
    filter_by_stream_name=True,
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion stream-prefix-filtered-subscription

client.close()
