from esdbclient import EventStoreDBClient, StreamState, NewEvent, exceptions
from typing import List

from uuid import uuid4

client = EventStoreDBClient(uri="esdb://localhost:2113?tls=false")

stream_name = str(uuid4())

event_data_one = NewEvent(
    type="test-event",
    data=b''
)

client.append_to_stream(
    stream_name=stream_name,
    events=event_data_one,
    current_version=StreamState.ANY
)

events: List[NewEvent] = []

for _ in range(50):
    event_data = NewEvent(
        type="test-event",
        data=b''
    )

    event = client.append_to_stream(
        stream_name=stream_name,
        events=event_data,
        current_version=StreamState.ANY
    )
    events.append(event_data)


# region subscribe-to-stream
subscription = client.subscribe_to_stream(
    stream_name=stream_name
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
# endregion subscribe-to-stream
    break

# region subscribe-to-stream-from-position
client.subscribe_to_stream(
    stream_name=stream_name,
    stream_position=20
)
# endregion subscribe-to-stream-from-position

# region subscribe-to-stream-live
client.subscribe_to_stream(
    stream_name=stream_name,
)
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
    stream_name=stream_name,
    stream_position=0
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
# endregion subscribe-to-stream-subscription-dropped
    break


# region subscribe-to-all
subscription = client.subscribe_to_all(
    commit_position=0,  # ! How do I read from end of stream,
)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
# endregion subscribe-to-all
    break

try:
    # region subscribe-to-all-from-position
    client.subscribe_to_all(commit_position=1_056)  # ! How do I read prepare position
    # endregion subscribe-to-all-from-position
except Exception as e:
    pass

# region subscribe-to-all-live
client.subscribe_to_all(
    commit_position=0,  # ! How do I read from end of stream,
)
# endregion subscribe-to-all-live

# region subscribe-to-all-subscription-dropped
subscription = client.subscribe_to_all(commit_position=0)

for event in subscription:
    print(f"received event: {event.stream_position} {event.type}")
    break
    # ! How to handle subscription dropped?
# endregion subscribe-to-all-subscription-dropped


# region stream-prefix-filtered-subscription
client.subscribe_to_all(
    filter_include=[r"test-\w+"],
    filter_by_stream_name=True,
)
# endregion stream-prefix-filtered-subscription
# region stream-regex-filtered-subscription
client.subscribe_to_all(
    filter_include=["/invoice-\\d\\d\\d/g"],
    filter_by_stream_name=True,
)
# endregion stream-regex-filtered-subscription


try:
    # region overriding-user-credentials
    credentials = client.construct_call_credentials(
        username='admin',
        password='changeit'
    )

    client.subscribe_to_all(
        credentials=credentials
    )
    # endregion overriding-user-credentials
except exceptions.GrpcError:
    pass


client.close()
