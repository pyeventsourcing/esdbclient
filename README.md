# Python gRPC Client for EventStoreDB

This package provides a Python gRPC client for
[EventStoreDB](https://www.eventstore.com/). It has been
developed and tested to work with version 21.10 of EventStoreDB,
and with Python versions 3.7, 3.8, 3.9, and 3.10.

Methods have typing annotations, the static typing is checked
with mypy, and the test coverage is 100%.

Not all the features of the EventStoreDB API are presented
by this client in its current form, however most of the useful
aspects are presented in an easy-to-use interface (see below).

## Installation

Use pip to install this package from
[the Python Package Index](https://pypi.org/project/esdbclient/).

    $ pip install esdbclient

It is recommended to install Python packages into a Python virtual environment.

## Getting started

### Start EventStoreDB

Use Docker to run EventStoreDB from the official container image on DockerHub.

    $ docker run -d --name my-eventstoredb -it -p 2113:2113 -p 1113:1113 eventstore/eventstore:21.10.2-buster-slim --insecure

Please note, this will start the server without SSL/TLS enabled, allowing
only "insecure" connections. This version of this Python client does not
support SSL/TLS connections. A later version of this library will support
"secure" connections.

### Construct client

The class `EsdbClient` can be constructed with a `uri` that indicates the hostname
and port number of the EventStoreDB server.

```python
from esdbclient.client import EsdbClient

client = EsdbClient(uri='localhost:2113')
```

### Append events

The method `append_events()` can be used to append events to
a stream. Three required arguments are required, `stream_name`,
`expected_position` and `new_events`.

The `stream_name` argument is a string that uniquely identifies
the stream in the database.

The `expected_position` argument is an optional integer that specifies
the expected position of the end of the stream in the database: either
an integer representing the expected current position of the stream,
or `None` if the stream is expected not to exist.

The `events` argument is a list of new event objects to be appended to the
stream. The class `NewEvent` can be used to construct new event objects.

The method `append_events()` returns the "commit position", which is a
monotonically increasing integer representing the position of the recorded
event in a "total order" of all recorded events in the database.

In the example below, a stream is created by appending a new event with
`expected_position=None`.

```python
from uuid import uuid4

from esdbclient.events import NewEvent

# Construct new event object.
event1 = NewEvent(type='OrderCreated', data=b'{}', metadata=b'{}')

# Define stream name.
stream_name1 = str(uuid4())

# Append list of events to new stream.
commit_position1 = client.append_events(
    stream_name=stream_name1, expected_position=None, events=[event1]
)
```

In the example below, two subsequent events are appended to an existing
stream. Since the stream only has one recorded event, and the stream
positions are zero-based, the expected position of the end of the stream
is `0`.

```python
event2 = NewEvent(type='OrderUpdated', data=b'{}', metadata=b'{}')
event3 = NewEvent(type='OrderDeleted', data=b'{}', metadata=b'{}')

commit_position2 = client.append_events(
    stream_name1, expected_position=0, events=[event2, event3]
)
```

Please note, whilst the append operation is atomic, so that either all
or none of a given list of events will be recorded, by design it is only
possible with EventStoreDB to atomically record events in one stream.

### Read stream events

The method `read_stream_events()` can be used to read the recorded
events in a stream. This method requires one argument, `stream_name`,
which is the name of the stream to be read. By default, all recorded
events in the stream are returned in the order they were recorded.
An iterable object of recorded events is returned.

The example below shows how to read all the recorded events in a stream
forwards from the start to the end.

```python
events = list(client.read_stream_events(stream_name=stream_name1))

assert len(events) == 3

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data

assert events[1].stream_name == stream_name1
assert events[1].stream_position == 1
assert events[1].type == event2.type
assert events[1].data == event2.data

assert events[2].stream_name == stream_name1
assert events[2].stream_position == 2
assert events[2].type == event3.type
assert events[2].data == event3.data
```

The method `read_stream_events()` also supports four optional arguments,
`position`, `backwards`, `limit`, and `timeout`.

The argument `position` is an optional integer that can be used to indicate
the position in the stream from which to start reading. This argument is `None`
by default, which means the stream will be read either from the start of the
stream (the default behaviour), or from the end of the stream if `backwards` is
`True`. When reading a stream from a specified position in the stream, the
recorded event at that position WILL be included, both when reading forwards
from that position, and when reading backwards from that position.

The argument `backwards` is a boolean, by default `False`, which means the
stream will be read forwards by default, so that events are returned in the
order they were appended, If `backwards` is `True`, the stream will be read
backwards, so that events are returned in reverse order.

The argument `limit` is an integer which limits the number of events that will
be returned.

The argument `timeout` is a float which sets a deadline for the completion of
the gRPC operation.

The example below shows how to read recorded events in a stream forwards from
a specific stream position.

```python
events = list(client.read_stream_events(stream_name1, position=1))

assert len(events) == 2

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 1
assert events[0].type == event2.type
assert events[0].data == event2.data

assert events[1].stream_name == stream_name1
assert events[1].stream_position == 2
assert events[1].type == event3.type
assert events[1].data == event3.data
```

The example below shows how to read all the recorded events in a stream backwards from
the end of the stream to the start of the stream.

```python
events = list(client.read_stream_events(stream_name1, backwards=True))

assert len(events) == 3

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 2
assert events[0].type == event3.type
assert events[0].data == event3.data

assert events[1].stream_name == stream_name1
assert events[1].stream_position == 1
assert events[1].type == event2.type
assert events[1].data == event2.data
```

The example below shows how to read a limited number (two) of the recorded events
in stream forwards from the start of the stream.

```python
events = list(client.read_stream_events(stream_name1, limit=2))

assert len(events) == 2

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data

assert events[1].stream_name == stream_name1
assert events[1].stream_position == 1
assert events[1].type == event2.type
assert events[1].data == event2.data
```

The example below shows how to read a limited number of the recorded events
in a stream backwards from a given stream position.

```python
events = list(client.read_stream_events(stream_name1, position=2, backwards=True, limit=1))

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 2
assert events[0].type == event3.type
assert events[0].data == event3.data
```

### Get current stream position

The method `get_stream_position()` can be used to get the current
stream position of the last event in the stream.

```python
stream_position = client.get_stream_position(stream_name1)

assert stream_position == 2
```

The sequence of stream positions is gapless. It is
also usually zero-based, so that the position of the end of the stream
when one event has been appended is `0`. The position is `1` after two
events have been appended, `2` after three events have been appended,
and so on.

The position of a stream that does not exist is reported by this method to
be `None`.

```python
stream_position = client.get_stream_position(stream_name='stream-unknown')

assert stream_position == None
```

This method takes an optional argument `timeout` which is a float that sets
a deadline for the completion of the gRPC operation.


### Read all recorded events

The method `read_all_events()` can be used to read all recorded events
in the database in the order they were committed. An iterable object of
recorded events is returned.

The example below shows how to read all events in the database in the
order they were recorded.

```python
events = list(client.read_all_events())

assert len(events) >= 3
```

The method `read_stream_events()` supports six optional arguments,
`position`, `backwards`, `filter_exclude`, `filter_include`, `limit`,
and `timeout`.

The argument `position` is an optional integer that can be used to specify
the commit position from which to start reading. This argument is `None` by
default, meaning that all the events will be read either from the start, or
from the end if `backwards` is `True` (see below). Please note, if specified,
the specified position must be an actually existing commit position, because
any other number will (at least in EventStoreDB v21.10) result in a server
error. Please also note, when reading forwards the event at the given position
WILL be included. However, when reading backwards the event at the given position
will NOT be included.

The argument `backwards` is a boolean which is by default `False` meaning all the
events will be read forwards by default, so that events are returned in the
order they were committed, If `backwards` is `True`, all the events will be read
backwards, so that events are returned in reverse order.

The argument `filter_exclude` is a sequence of regular expressions that
match the type strings of recorded events that should not be included.
By default, this argument will exclude EventStoreDB "system events",
which by convention all have type strings that start with the `$` sign.
But it can be used to also exclude snapshots, if all snapshots are recorded
with the same type string. This argument is ignored if `filter_include` is set.

Please note, characters that have a special meaning in regular expressions
will need to be escaped with double-backslash when using these characters
to match strings. For example, to match EventStoreDB "system events" use use
`\\$.*`.

The argument `filter_include` is a sequence of regular expressions (strings)
that match the type strings of recorded events that should be included. By
default, this argument is an empty tuple. If this argument is set to a
non-empty sequence, the `filter_include` argument is ignored.

Please note, the filtering happens on the EventStoreDB server, and the `limit` argument
is applied after filtering.

The argument `limit` is an integer which limits the number of events that will
be returned.

The argument `timeout` is a float which sets a deadline for the completion of
the gRPC operation.

The example below shows how to read all recorded events from a particular commit position.

```python
events = list(client.read_all_events(position=commit_position1))

assert len(events) == 3

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data

assert events[1].stream_name == stream_name1
assert events[1].stream_position == 1
assert events[1].type == event2.type
assert events[1].data == event2.data

assert events[2].stream_name == stream_name1
assert events[2].stream_position == 2
assert events[2].type == event3.type
assert events[2].data == event3.data
```

The example below shows how to read all recorded events in reverse order.

```python
events = list(client.read_all_events(backwards=True))

assert len(events) >= 3

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 2
assert events[0].type == event3.type
assert events[0].data == event3.data

assert events[1].stream_name == stream_name1
assert events[1].stream_position == 1
assert events[1].type == event2.type
assert events[1].data == event2.data

assert events[2].stream_name == stream_name1
assert events[2].stream_position == 0
assert events[2].type == event1.type
assert events[2].data == event1.data
```

The example below shows how to read a limited number (one) of the recorded events
in the database forwards from a specific commit position.

```python
events = list(client.read_all_events(position=commit_position1, limit=1))

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data
```

The example below shows how to read a limited number (one) of the recorded events
in the database backwards from the end. This gives the last recorded event.

```python
events = list(client.read_all_events(backwards=True, limit=1))

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 2
assert events[0].type == event3.type
assert events[0].data == event3.data
```

### Get current commit position

The method `get_commit_position()` can be used to get the current
commit position of the database.

```python
commit_position = client.get_commit_position()
```

The sequence of commit positions is not gapless. It represents the position
on disk, so there are usually differences between successive commits.

This method is provided as a convenience when testing, and otherwise isn't
very useful. In particular, when reading all events (see above) or subscribing
to events (see below), the commit position would normally be read from the
downstream database, so that you are reading from the last position that was
successfully processed.

This method takes an optional argument `timeout` which is a float that sets
a deadline for the completion of the gRPC operation.

### Catch-up subscriptions

The method `subscribe_all_events()` can be used to create a
"catch-up subscription" to EventStoreDB. The optional argument
`position` can be used to specify a commit position from which
to receive recorded events. Please note, returned events are those
after the given commit position.

This method returns a subscription object, which is an iterable object,
from which recorded events can be obtained by iteration.

The example below shows how to subscribe to receive all recorded
events from a specific commit position. Three already-existing
events are received, and then three new events are recorded, which
are then received via the subscription.

```python

# Get the commit position (usually from database of materialised views).
commit_position = client.get_commit_position()

# Append three events.
stream_name1 = str(uuid4())
event1 = NewEvent(type='OrderCreated', data=b'{}', metadata=b'{}')
event2 = NewEvent(type='OrderUpdated', data=b'{}', metadata=b'{}')
event3 = NewEvent(type='OrderDeleted', data=b'{}', metadata=b'{}')
client.append_events(
    stream_name1, expected_position=None, events=[event1, event2, event3]
)

# Subscribe from the commit position.
subscription = client.subscribe_all_events(position=commit_position)

# Catch up by receiving the three events from the subscription.
events = []
for event in subscription:
    # Check the stream name is 'stream_name1'.
    assert event.stream_name == stream_name1
    events.append(event)
    if len(events) == 3:
        break

# Append three more events.
stream_name = str(uuid4())
event1 = NewEvent(type='OrderCreated', data=b'{}', metadata=b'{}')
event2 = NewEvent(type='OrderUpdated', data=b'{}', metadata=b'{}')
event3 = NewEvent(type='OrderDeleted', data=b'{}', metadata=b'{}')
client.append_events(
    stream_name, expected_position=None, events=[event1, event2, event3]
)

# Receive the three new events from the same subscription.
events = []
for event in subscription:
    # Check the stream name is 'stream_name2'.
    assert event.stream_name == stream_name
    events.append(event)
    if len(events) == 3:
        break
```

This kind of subscription is not recorded in EventStoreDB. It is simply
a streaming gRPC call which is kept open by the server, with newly recorded
events sent to the client. This kind of subscription is closed as soon as
the subscription object goes out of memory.

```python
# End the subscription.
del subscription
```

To accomplish "exactly once" processing of the events, the commit position
should be recorded atomically and uniquely along with the result of processing
received events, for example in the same database as materialised views when
implementing eventually-consistent CQRS, or in the same database as a downstream
analytics or reporting or archiving application.

The subscription object might be used within a thread dedicated to receiving
events, with recorded events put on a queue for processing in a different
thread. This package doesn't provide such a thing, you need to do that yourself.
Just make sure to resume after an error by reconstructing both the subscription
and the queue, using your last recorded commit position to resume the subscription.

Many such subscriptions can be created, and all will receive the events they
are subscribed to receive. Received events do not need to (and cannot) be
acknowledged back to EventStoreDB.

This method also support three other optional arguments, `filter_exclude`,
`filter_include`, and `timeout`.

The argument `filter_exclude` is a sequence of regular expressions that
match the type strings of recorded events that should not be included.
By default, this argument will exclude EventStoreDB "system events",
which by convention all have type strings that start with the `$` sign.
But it can be used to also exclude snapshots. This argument is ignored
if `filter_include` is set.

Please note, characters that have a special meaning in regular expressions
will need to be escaped with double-backslash when using these characters
to match strings. For example, to match EventStoreDB "system events" use use
`\\$.*`.

The argument `filter_include` is a sequence of regular expressions (strings)
that match the type strings of recorded events that should be included. By
default, this argument is an empty tuple. If this argument is set to a
non-empty sequence, the `filter_include` argument is ignored.

Please note, in this version of this Python client, the filtering happens
within the client, rather than on the server, like when reading all events, because
for some passing these filter options in the read request seems to cause an error
in EventStoreDB v21.10.

The argument `timeout` is a float which sets a deadline for the completion of
the gRPC operation. This probably isn't very useful, but is included for
completeness and consistency with the other methods.

### The NewEvent class

The `NewEvent` class can be used to define new events.

The attribute `type` is a unicode string, used to specify the type of the event
to be recorded.

The attribute `data` is a byte string, used to specify the data of the event
to be recorded. Please note, in this version of this Python client,
writing JSON event data to EventStoreDB isn't supported, but it might be in
a future version.

The attribute `metadata` is a byte string, used to specify metadata for the event
to be recorded.

```python
new_event = NewEvent(
    type='OrderCreated',
    data=b'{}',
    metadata=b'{}',
)
```

### The RecordedEvent class

The `RecordedEvent` class is used when reading recorded events.

The attribute `type` is a unicode string, used to indicate the type of the event
that was recorded.

The attribute `data` is a byte string, used to indicate the data of the event
that was recorded.

The attribute `metadata` is a byte string, used to indicate metadata for the event
that was recorded.

The attribute `stream_name` is a unicode string, used to indicate the type of
the name of the stream in which the event was recorded.

The attribute `stream_position` is an integer, used to indicate
the position in the stream at which the event was recorded.

The attribute `commit_position` is an integer, used to indicate
the position in total order of all recorded events at which the
event was recorded.

```python

from esdbclient.events import RecordedEvent

new_event = RecordedEvent(
    type='OrderCreated',
    data=b'{}',
    metadata=b'{}',
    stream_name='stream1',
    stream_position=0,
    commit_position=512,
)
```

### Stop EventStoreDB

Use Docker to stop and remove the EventStoreDB container.

    $ docker stop my-eventstoredb
	$ docker rm my-eventstoredb


## Developers

Clone the project repository, set up a virtual environment, and install
dependencies.

Use your IDE (e.g. PyCharm) to open the project repository. Create a
Poetry virtual environment, and then update packages.

    $ make update-packages

Alternatively, use the ``make install`` command to create a dedicated
Python virtual environment for this project.

    $ make install

The ``make install`` command uses the build tool Poetry to create a dedicated
Python virtual environment for this project, and installs popular development
dependencies such as Black, isort and pytest.

Add tests in `./tests`. Add code in `./esdbclient`.

Start EventStoreDB.

    $ make start-eventstoredb

Run tests.

    $ make test

Stop EventStoreDB.

    $ make stop-eventstoredb

Check the formatting of the code.

    $ make lint

Reformat the code.

    $ make fmt

Add dependencies in `pyproject.toml` and then update installed packages.

    $ make update-packages
