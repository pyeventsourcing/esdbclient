from uuid import uuid4

from kurrentdbclient import KurrentDBClient, NewEvent, StreamState
from tests.test_client import get_server_certificate

DEBUG = False
_print = print


def print(*args):  # noqa: A001
    if DEBUG:
        _print(*args)


KDB_TARGET = "localhost:2114"
qs = "MaxDiscoverAttempts=2&DiscoveryInterval=100&GossipTimeout=1"

client = KurrentDBClient(
    uri=f"kurrentdb://admin:changeit@{KDB_TARGET}?{qs}",
    root_certificates=get_server_certificate(KDB_TARGET),
)

stream_name = str(uuid4())


"""
# region createClient
client = KurrentDBClient(
    uri=connection_string
)
# endregion createClient
"""

# region createEvent
new_event = NewEvent(
    id=uuid4(),
    type="TestEvent",
    data=b"I wrote my first event",
)
# endregion createEvent

# region appendEvents
client.append_to_stream(
    stream_name=stream_name,
    events=[new_event],
    current_version=StreamState.ANY,
)
# endregion appendEvents

# region readStream
events = client.read_stream(stream_name)
# endregion readStream

client.close()
