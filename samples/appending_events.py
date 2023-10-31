# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    EventStoreDBClient,
    NewEvent,
    StreamState,
    exceptions,
)
from tests.test_client import get_server_certificate

ESDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = EventStoreDBClient(
    uri=f"esdb://admin:changeit@{ESDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(ESDB_TARGET),
)

STREAM_NAME = str(uuid4())

# region append-to-stream
event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

commit_position = client.append_to_stream(
    stream_name=STREAM_NAME,
    current_version=StreamState.NO_STREAM,
    events=event_data,
)
# endregion append-to-stream

STREAM_NAME = str(uuid4())

# region append-duplicate-event
event = NewEvent(
    id=uuid4(),
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=STREAM_NAME,
    current_version=StreamState.ANY,
    events=event,
)

try:
    client.append_to_stream(
        stream_name=STREAM_NAME,
        current_version=StreamState.ANY,
        events=event,
    )
except Exception as e:
    print("Error appending second event:", e)
    # endregion append-duplicate-event
    pass

STREAM_NAME = str(uuid4())

# region append-with-no-stream
event_one = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

client.append_to_stream(
    stream_name=STREAM_NAME,
    current_version=StreamState.NO_STREAM,
    events=event_one,
)

event_two = NewEvent(
    type="some-event",
    data=b'{"id": "2", "important_data": "some other value"}',
)

try:
    # attempt to append the same event again
    client.append_to_stream(
        stream_name=STREAM_NAME,
        current_version=StreamState.NO_STREAM,
        events=event_two,
    )
except exceptions.WrongCurrentVersion:
    print("Error appending second event")
    # endregion append-with-no-stream
    pass

STREAM_NAME = str(uuid4())

client.append_to_stream(
    stream_name=STREAM_NAME,
    current_version=StreamState.ANY,
    events=NewEvent(
        type="some-event",
        data=b'{"id": "1", "value": "some value"}',
    ),
)

# region append-with-concurrency-check
events = client.get_stream(
    stream_name=STREAM_NAME,
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
    stream_name=STREAM_NAME,
    current_version=revision,
    events=client_one_event,
)

client_two_event = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "client two"}',
)

try:
    client.append_to_stream(
        stream_name=STREAM_NAME,
        current_version=revision,
        events=client_two_event,
    )
except exceptions.WrongCurrentVersion:
    print("Error appending second event")
    # endregion append-with-concurrency-check
    pass

STREAM_NAME = str(uuid4())

event = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

# region overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

commit_position = client.append_to_stream(
    stream_name=STREAM_NAME,
    current_version=StreamState.ANY,
    events=event,
    credentials=credentials,
)
# endregion overriding-user-credentials
print(f"Commit position: {commit_position}")

client.close()
