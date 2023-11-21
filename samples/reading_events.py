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

DEBUG = False
_print = print


def print(*args):
    if DEBUG:
        _print(*args)


stream_name = str(uuid4())

event_data = NewEvent(
    type="some-event",
    data=b'{"id": "1", "important_data": "some value"}',
)


for _ in range(1):
    client.append_to_stream(
        stream_name=stream_name,
        current_version=StreamState.ANY,
        events=event_data,
    )

# region read-from-stream
events = client.get_stream(
    stream_name=stream_name,
    stream_position=0,
    limit=100,
)
# endregion read-from-stream

# region iterate-stream
for event in events:
    # Doing something productive with the event
    print(f"Event: {event}")
# endregion iterate-stream


# region overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

events = client.get_stream(
    stream_name=stream_name,
    credentials=credentials,
)
# endregion overriding-user-credentials


# region read-from-stream-position
events = client.get_stream(
    stream_name=stream_name,
    stream_position=10,
)
# endregion read-from-stream-position


# region reading-backwards
events = client.get_stream(
    stream_name=stream_name,
    backwards=True,
)
# endregion reading-backwards


unknown_stream_name = str(uuid4())

# region checking-for-stream-presence
try:
    events = client.get_stream(
        stream_name=unknown_stream_name, limit=1
    )
except exceptions.NotFound:
    print("The stream was not found")
# endregion checking-for-stream-presence
else:
    raise Exception("Exception not raised")


# region read-from-all-stream
events = client.read_all(
    commit_position=0,
    limit=100,
)
# endregion read-from-all-stream


# region read-from-all-stream-iterate
for event in events:
    print(f"Event: {event}")
# endregion read-from-all-stream-iterate


# region read-from-all-stream-resolving-link-Tos
events = client.read_all(
    commit_position=0,
    limit=100,
    resolve_links=True,
)
# endregion read-from-all-stream-resolving-link-Tos


# region read-all-overriding-user-credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

events = client.read_all(
    commit_position=0,
    limit=100,
    credentials=credentials,
)
# endregion read-all-overriding-user-credentials


# region read-from-all-stream-backwards
events = client.read_all(
    backwards=True,
    limit=100,
)
# endregion read-from-all-stream-backwards


# region ignore-system-events
events = client.read_all(
    filter_exclude=[],  # system events are excluded by default
)

for event in events:
    if event.type.startswith("$"):
        continue

    print(f"Event: {event.type}")
# endregion ignore-system-events


# Is this actually used?
# region read-from-all-stream-backwards-iterate
for event in events:
    print(f"Event: {event.type}")
# endregion read-from-all-stream-backwards-iterate


client.close()
