# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import EventStoreDBClient, NewEvent, StreamState
from esdbclient.persistent import (
    PersistentSubscription,
    RecordedEvent,
)
from tests.test_client import get_server_certificate

stream_name = str(uuid4())

ESDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = EventStoreDBClient(
    uri=f"esdb://admin:changeit@{ESDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(ESDB_TARGET),
)


def handle_event(ev: RecordedEvent, sub: PersistentSubscription):
    print(f"handling event: {ev.stream_position} {ev.type}")
    sub.stop()


# region create-persistent-subscription-to-stream
client.create_subscription_to_stream(
    stream_name=stream_name,
    group_name="subscription-group",
)
# endregion create-persistent-subscription-to-stream

event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data,
)

# region subscribe-to-persistent-subscription-to-stream
subscription = client.read_subscription_to_stream(
    stream_name=stream_name,
    group_name="subscription-group",
)

try:
    for event in subscription:
        try:
            print(
                f"handling event {event.type} with retryCount",
                f"{event.retry_count}",
            )
            subscription.ack(event_id=event.id)

            # do something with the event
            handle_event(event, subscription)
        except Exception as ex:
            print(f"handling failed with exception {ex}")
            subscription.nack(event_id=event.id, action="park")
except Exception as e:
    print(f"Subscription was dropped. {e}")
# endregion subscribe-to-persistent-subscription-to-stream


event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data,
)

group_name = str(uuid4())

client.create_subscription_to_all(
    group_name=group_name,
)

# region subscribe-to-persistent-subscription-to-all
subscription = client.read_subscription_to_all(
    group_name=group_name,
)

try:
    for event in subscription:
        print(
            f"handling event {event.type} with retryCount",
            f"{event.retry_count}",
        )
        subscription.ack(event_id=event.id)

        # do something with the event
        handle_event(event, subscription)
except Exception as e:
    print(f"Subscription was dropped. {e}")
# endregion subscribe-to-persistent-subscription-to-all

group_name = str(uuid4())

# region create-persistent-subscription-to-all
client.create_subscription_to_all(
    group_name=group_name,
    filter_include="test" + ".*",
    filter_by_stream_name=True,
)
# endregion create-persistent-subscription-to-all

stream_name = str(uuid4())
group_name = str(uuid4())

client.create_subscription_to_stream(
    stream_name=stream_name,
    group_name=group_name,
)

event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data,
)

# region subscribe-to-persistent-subscription-with-manual-acks
subscription = client.read_subscription_to_stream(
    stream_name=stream_name,
    group_name=group_name,
)

try:
    for event in subscription:
        try:
            print(
                f"handling event {event.type} with retryCount",
                f"{event.retry_count}",
            )
            subscription.ack(event_id=event.id)

            # do something with the event
            handle_event(event, subscription)
        except Exception as ex:
            print(f"handling failed with exception {ex}")
            subscription.nack(event_id=event.id, action="park")
except Exception as e:
    print(f"Subscription was dropped. {e}")
# endregion subscribe-to-persistent-subscription-with-manual-acks


# region update-persistent-subscription
client.update_subscription_to_stream(
    stream_name=stream_name,
    group_name=group_name,
)
# endregion update-persistent-subscription


# region delete-persistent-subscription
client.delete_subscription(
    stream_name=stream_name,
    group_name=group_name,
)
# endregion delete-persistent-subscription

client.close()
