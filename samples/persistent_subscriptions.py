# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import EventStoreDBClient, NewEvent, StreamState
from esdbclient.exceptions import ConsumerTooSlow
from esdbclient.persistent import (
    PersistentSubscription,
    RecordedEvent,
)
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

subscription: PersistentSubscription


def handle_event(ev: RecordedEvent):
    print(f"handling event: {ev.stream_position} {ev.type}")
    global subscription
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


# region create-persistent-subscription-to-stream
client.create_subscription_to_stream(
    group_name="subscription-group",
    stream_name=stream_name,
)
# endregion create-persistent-subscription-to-stream


group_name = str(uuid4())

# region create-persistent-subscription-to-all
client.create_subscription_to_all(
    group_name=group_name,
    filter_by_stream_name=True,
    filter_include=[r"user-.*"],
)
# endregion create-persistent-subscription-to-all


# region subscribe-to-persistent-subscription-to-stream
while True:
    subscription = client.read_subscription_to_stream(
        group_name="subscription-group",
        stream_name=stream_name,
    )
    try:
        for event in subscription:
            try:
                handle_event(event)
            except Exception:
                subscription.nack(event, action="park")
            else:
                subscription.ack(event)

    except ConsumerTooSlow:
        # subscription was dropped
        continue

    # endregion subscribe-to-persistent-subscription-to-stream
    break

group_name = str(uuid4())

client.create_subscription_to_all(
    group_name=group_name,
)


# region subscribe-to-persistent-subscription-to-all
while True:
    subscription = client.read_subscription_to_all(
        group_name=group_name,
    )
    try:
        for event in subscription:
            try:
                handle_event(event)
            except Exception:
                subscription.nack(event, action="park")
            else:
                subscription.ack(event)

    except ConsumerTooSlow:
        # subscription was dropped
        continue
    # endregion subscribe-to-persistent-subscription-to-all
    break

group_name = str(uuid4())

client.create_subscription_to_stream(
    group_name=group_name,
    stream_name=stream_name,
)


# region subscribe-to-persistent-subscription-with-manual-acks
while True:
    subscription = client.read_subscription_to_stream(
        group_name=group_name,
        stream_name=stream_name,
    )
    try:
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

    except ConsumerTooSlow:
        # subscription was dropped
        continue
    # endregion subscribe-to-persistent-subscription-with-manual-acks
    break


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

client.close()
