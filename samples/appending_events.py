# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import (
    EventStoreDBClient,
    NewEvent,
    StreamState,
    exceptions,
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

stream_name = str(uuid4())

# region append-to-stream
event1 = NewEvent(
    type="some-event",
    data=b'{"important_data": "some value"}',
)

commit_position = client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.NO_STREAM,
    events=[event1],
)
# endregion append-to-stream

stream_name = str(uuid4())

# region append-duplicate-event
event = NewEvent(
    id=uuid4(),
    type="some-event",
    data=b'{"important_data": "some value"}',
    metadata=b"{}",
    content_type="application/json",
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event,
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event,
)
# endregion append-duplicate-event
assert len(client.get_stream(stream_name)) == 1

# region append-with-no-stream
event1 = NewEvent(
    type="some-event",
    data=b'{"important_data": "some value"}',
)

stream_name = str(uuid4())

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.NO_STREAM,
    events=event1,
)

event2 = NewEvent(
    type="some-event",
    data=b'{"important_data": "some other value"}',
)

try:
    # attempt to append the same event again
    client.append_to_stream(
        stream_name=stream_name,
        current_version=StreamState.NO_STREAM,
        events=event2,
    )
except exceptions.WrongCurrentVersion:
    print("Error appending second event")
    # endregion append-with-no-stream
else:
    raise Exception("Exception not raised")

stream_name = str(uuid4())

client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=NewEvent(
        type="some-event",
        data=b'{"id": "1", "value": "some value"}',
    ),
)

# region append-with-concurrency-check

original_version = StreamState.NO_STREAM

for event in client.read_stream(stream_name):
    original_version = event.stream_position


event1 = NewEvent(
    type="some-event",
    data=b'{"important_data": "some value"}',
)

client.append_to_stream(
    stream_name=stream_name,
    current_version=original_version,
    events=event1,
)

event2 = NewEvent(
    type="some-event",
    data=b'{"important_data": "some other value"}',
)

try:
    client.append_to_stream(
        stream_name=stream_name,
        current_version=original_version,
        events=event2,
    )
except exceptions.WrongCurrentVersion:
    print("Error appending event2")
    # endregion append-with-concurrency-check
else:
    raise Exception("Exception not raised")

stream_name = str(uuid4())

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
    stream_name=stream_name,
    current_version=StreamState.ANY,
    events=event,
    credentials=credentials,
)
# endregion overriding-user-credentials

client.close()
