# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    EventStoreDBClient,
    NewEvent,
    StreamState,
    exceptions,
)

client = EventStoreDBClient(uri="esdb://localhost:2113?tls=false")

stream_name = str(uuid4())
stream_name_one = str(uuid4())
concurrency_stream = str(uuid4())

# region append-to-stream
event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

commit_position = client.append_to_stream(
    stream_name=stream_name_one,
    current_version=StreamState.NO_STREAM,
    events=event_data,
)
# endregion append-to-stream


# region append-duplicate-event
event = NewEvent(
    id=uuid4(),
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

try:
    client.append_to_stream(
        stream_name="some-stream",
        current_version=StreamState.ANY,
        events=event,
    )

    client.append_to_stream(
        stream_name="some-stream",
        current_version=StreamState.ANY,
        events=event,
    )
except Exception as e:
    print("Error appending second event:", e)
# endregion append-duplicate-event


try:
    # region append-with-no-stream
    event_one = NewEvent(
        type="some-event",
        data=b'{"id": "1", "important_data": "some value"}',
    )

    client.append_to_stream(
        stream_name="no-stream-stream",
        current_version=StreamState.NO_STREAM,
        events=event_one,
    )

    event_two = NewEvent(
        type="some-event",
        data=b'{"id": "2", "important_data": "some other value"}',
    )

    # attempt to append the same event again
    client.append_to_stream(
        stream_name="no-stream-stream",
        current_version=StreamState.NO_STREAM,
        events=event_two,
    )
    # endregion append-with-no-stream
except exceptions.WrongCurrentVersion:
    pass

client.append_to_stream(
    stream_name=concurrency_stream,
    current_version=StreamState.ANY,
    events=NewEvent(
        type="some-event",
        data=b'{"id": "1", "value": "some value"}',
    ),
)

try:
    # region append-with-concurrency-check
    events = client.get_stream(
        stream_name=concurrency_stream,
        backwards=True,
        limit=1,
    )

    revision = StreamState.NO_STREAM
    for event in events:
        revision = event.stream_position

    last_event = events[-1]

    client_one_event = NewEvent(
        type="some-event",
        data=b'{"id": "1", "important_data": "client one"}',
    )

    client.append_to_stream(
        stream_name=concurrency_stream,
        current_version=revision,
        events=client_one_event,
    )

    client_two_event = NewEvent(
        type="some-event",
        data=b'{"id": "1", "important_data": "client two"}',
    )

    client.append_to_stream(
        stream_name=concurrency_stream,
        current_version=revision,
        events=client_two_event,
    )
    # endregion append-with-concurrency-check
except exceptions.WrongCurrentVersion:
    pass

event = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

try:
    # region overriding-user-credentials
    credentials = client.construct_call_credentials(
        username="admin",
        password="changeit",
    )

    commit_position = client.append_to_stream(
        stream_name=stream_name,
        current_version=StreamState.ANY,
        events=event,
        credentials=credentials,
    )
    # endregion overriding-user-credentials

    print(f"Commit position: {commit_position}")
except exceptions.GrpcError:
    pass

client.close()
