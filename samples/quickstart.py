# -*- coding: utf-8 -*-
from uuid import uuid4

from esdbclient import EventStoreDBClient, NewEvent, StreamState

"""
# region createClient
client = EventStoreDBClient(
    uri="{connectionString}"
)
# endregion createClient
"""


client = EventStoreDBClient(uri="esdb://localhost:2113?tls=false")

# region createEvent
event_data = NewEvent(
    id=uuid4(),
    type="TestEvent",
    data=b"I wrote my first event",
)

# endregion createEvent

# region appendEvents
client.append_to_stream(
    stream_name="some-stream",
    events=event_data,
    current_version=StreamState.ANY,
)
# endregion appendEvents

# region readStream
events = client.get_stream(
    stream_name="some-stream",
    limit=10,
)

for event in events:
    # Doing something productive with the event
    print(event)
# endregion readStream

client.close()
