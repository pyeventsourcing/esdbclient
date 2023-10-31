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

event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)

# append 20 events
for _ in range(20):
    client.append_to_stream(
        stream_name=STREAM_NAME,
        current_version=StreamState.ANY,
        events=event_data,
    )

# region read-from-stream
events = client.get_stream(
    stream_name=STREAM_NAME,
    stream_position=0,
    limit=100,
)

# endregion read-from-stream

# region iterate-stream
for event in events:
    # Doing something productive with the event
    print(f"Event: {event}")
# endregion iterate-stream


# region read-from-stream-position
events = client.get_stream(
    stream_name=STREAM_NAME,
    stream_position=10,
    limit=20,
)
# endregion read-from-stream-position

# region iterate-stream
for event in events:
    print(f"Event: {event}")
# endregion iterate-stream


try:
    # region overriding-user-credentials
    credentials = client.construct_call_credentials(
        username="admin",
        password="changeit",
    )

    stream = client.read_stream(
        stream_name=STREAM_NAME,
        stream_position=0,
        credentials=credentials,
    )
    # endregion overriding-user-credentials
except exceptions.GrpcError:
    pass

unknown_stream_name = str(uuid4())

# region checking-for-stream-presence
try:
    events = client.get_stream(
        stream_name=unknown_stream_name,
        backwards=False,
        stream_position=10,
        limit=100,
    )

    for event in events:
        print(event.data)
except exceptions.NotFound:
    pass
# endregion checking-for-stream-presence

# region reading-backwards
events = client.get_stream(
    stream_name=STREAM_NAME,
    backwards=True,
    limit=10,
)

for event in events:
    print(f"Event: {event}")
# endregion reading-backwards


# region read-from-all-stream
stream = client.read_all(
    commit_position=0,
    backwards=False,
    limit=100,
)
# endregion read-from-all-stream
# region read-from-all-stream-iterate
events = tuple(stream)

for event in events:
    print(f"Event: {event}")
# endregion read-from-all-stream-iterate


# region ignore-system-events
stream = client.read_all(limit=100)

events = tuple(stream)

for event in events:
    if event.type.startswith("$"):
        continue

    print(f"Event: {event.type}")
# endregion ignore-system-events


# region read-from-all-stream-backwards
stream = client.read_all(
    backwards=True,
    limit=100,
)
# endregion read-from-all-stream-backwards
# region read-from-all-stream-backwards-iterate
events = tuple(stream)

for event in events:
    print(f"Event: {event.type}")
# endregion read-from-all-stream-backwards-iterate


# region read-from-all-stream-resolving-link-Tos
client.read_all(limit=100)
# endregion read-from-all-stream-resolving-link-Tos


# region read-all-overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

client.read_all(
    commit_position=0,
    credentials=credentials,
)
# endregion read-all-overriding-user-credentials

client.close()
