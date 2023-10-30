from esdbclient import NewEvent, StreamState, EventStoreDBClient
from uuid import uuid4

stream_name = str(uuid4())

client = EventStoreDBClient(uri="esdb://localhost:2113?tls=false")

# region create-persistent-subscription-to-stream
client.create_subscription_to_stream(
    stream_name=stream_name,
    group_name="subscription-group"
)
# endregion create-persistent-subscription-to-stream

event_data = NewEvent(
    type='some-event',
    data=b'{"id": "1", "important_data": "some value"}'
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data
)

# region subscribe-to-persistent-subscription-to-stream
subscription = client.read_subscription_to_stream(
    stream_name=stream_name,
    group_name="subscription-group",
)

try:
    for event in subscription:
        try:
            print(f'handling event {event.type} with retryCount {event.retry_count}')
            subscription.ack(event_id=event.id)
            break
        except Exception as ex:
            print(f'handling failed with exception {ex}')
            subscription.nack(event_id=event.id, action="park")
except BaseException as e:
    print(f'Subscription was dropped. {e}')
# endregion subscribe-to-persistent-subscription-to-stream


event_data = NewEvent(
    type='some-event',
    data=b'{"id": "1", "important_data": "some value"}'
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data
)

GROUP_NAME = str(uuid4())

client.create_subscription_to_all(
    group_name=GROUP_NAME
)

# region subscribe-to-persistent-subscription-to-all
subscription = client.read_subscription_to_all(
    group_name=GROUP_NAME
)

try:
    for event in subscription:
        print('handling event {event.type} with retryCount {event.retry_count}')
        subscription.ack(event_id=event.id)
        break
except BaseException as e:
    print(f'Subscription was dropped. {e}')
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
    group_name=group_name
)

event_data = NewEvent(
    type='some-event',
    data=b'{"id": "1", "important_data": "some value"}'
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event_data,
)

# region subscribe-to-persistent-subscription-with-manual-acks
subscription = client.read_subscription_to_stream(
    stream_name=stream_name,
    group_name=group_name
)

try:
    for event in subscription:
        try:
            print(f'handling event {event.type} with retryCount {event.retry_count}')
            subscription.ack(event_id=event.id)
            break
        except Exception as ex:
            print(f'handling failed with exception {ex}')
            subscription.nack(event_id=event.id, action="park")
            break
except BaseException as e:
    print(f'Subscription was dropped. {e}')
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
