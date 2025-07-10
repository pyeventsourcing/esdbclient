from uuid import uuid4

from kurrentdbclient import (
    Checkpoint,
    KurrentDBClient,
    NewEvent,
    StreamState,
)
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

subscription: CatchupSubscription


def handle_event(ev: RecordedEvent):
    print(f"handling event: {ev.stream_position} {ev.type}")
    subscription.stop()


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

# region exclude-system
with client.subscribe_to_all(filter_exclude=r"\$.+") as subscription:
    for event in subscription:
        handle_event(event)
# endregion exclude-system


# region event-type-prefix
with client.subscribe_to_all(
    filter_include=[r"customer-.*"],
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion event-type-prefix


event_data = NewEvent(
    type="test-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name="user-" + str(uuid4()),
    current_version=StreamState.ANY,
    events=event_data,
)

# region stream-prefix
with client.subscribe_to_all(
    filter_by_stream_name=True,
    filter_include=[r"user-.*"],
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion stream-prefix

client.append_to_stream(
    stream_name="account-stream",
    current_version=StreamState.ANY,
    events=event_data,
)


# region stream-regex
with client.subscribe_to_all(
    filter_by_stream_name=True,
    filter_include=["account.*", "savings.*"],
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion stream-regex

# region checkpoint
# get last recorded commit position
recorded_position = 0

with client.subscribe_to_all(
    commit_position=recorded_position,
    filter_by_stream_name=True,
    filter_include=["account.*", "savings.*"],
    include_checkpoints=True,
) as subscription:
    for event in subscription:

        # checkpoints have a commit positions
        if isinstance(event, Checkpoint):
            print("We got a checkpoint!", event.commit_position)
        else:
            print("We got an event!", event.commit_position)

        # record commit position
        handle_event(event)
# endregion checkpoint


print("region checkpoint-with-interval")
# region checkpoint-with-interval
with client.subscribe_to_all(
    commit_position=recorded_position,
    include_checkpoints=True,
    checkpoint_interval_multiplier=5,
) as subscription:
    for event in subscription:
        handle_event(event)
# endregion checkpoint-with-interval

client.close()
