# ruff: noqa: PERF203
from uuid import uuid4

from kurrentdbclient import KurrentDBClient, NewEvent, StreamState
from kurrentdbclient.persistent import (
    PersistentSubscription,
    RecordedEvent,
)
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

subscription: PersistentSubscription


def handle_event(ev: RecordedEvent):
    print(f"handling event: {ev.stream_position} {ev.type}")
    subscription.stop()


stream_name = "user-" + str(uuid4())

event_data = NewEvent(
    type="some-event",
    data=b"{}",
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data,
)


group_name = str(uuid4())

# region create-persistent-subscription-to-stream
client.create_subscription_to_stream(
    group_name=group_name,
    stream_name=stream_name,
)
# endregion create-persistent-subscription-to-stream


# region subscribe-to-persistent-subscription-to-stream
with client.read_subscription_to_stream(
    group_name=group_name,
    stream_name=stream_name,
) as subscription:
    for event in subscription:
        try:
            handle_event(event)
        except Exception:
            subscription.nack(event, action="park")
        else:
            subscription.ack(event)
# endregion subscribe-to-persistent-subscription-to-stream

# Delete the subscription and make a new one.
client.delete_subscription(
    group_name=group_name,
    stream_name=stream_name,
)

group_name = str(uuid4())

client.create_subscription_to_stream(
    group_name=group_name,
    stream_name=stream_name,
)

# region subscribe-to-persistent-subscription-with-manual-acks
with client.read_subscription_to_stream(
    group_name=group_name,
    stream_name=stream_name,
) as subscription:
    for event in subscription:
        try:
            handle_event(event)
        except Exception:
            if event.retry_count < 5:
                subscription.nack(event, action="retry")
            else:
                subscription.nack(event, action="park")
        else:
            subscription.ack(event)
# endregion subscribe-to-persistent-subscription-with-manual-acks


# region update-persistent-subscription
client.update_subscription_to_stream(
    group_name=group_name,
    stream_name=stream_name,
    resolve_links=True,
)
# endregion update-persistent-subscription


# region delete-persistent-subscription
client.delete_subscription(
    group_name=group_name,
    stream_name=stream_name,
)
# endregion delete-persistent-subscription


# region create-persistent-subscription-to-all
client.create_subscription_to_all(
    group_name=group_name,
    filter_by_stream_name=True,
    filter_include=[r"user-.*"],
)
# endregion create-persistent-subscription-to-all


# region subscribe-to-persistent-subscription-to-all
with client.read_subscription_to_all(
    group_name=group_name,
) as subscription:
    for event in subscription:
        try:
            handle_event(event)
        except Exception:
            subscription.nack(event, action="park")
        else:
            subscription.ack(event)
# endregion subscribe-to-persistent-subscription-to-all

client.close()
