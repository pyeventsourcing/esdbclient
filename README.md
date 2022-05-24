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

Use pip to install this package from the
[the Python Package Index](https://pypi.org/project/esdbclient/).

    $ pip install esdbclient

It is recommended to install Python packages into a Python virtual environment.

## Getting started

### Start EventStoreDB

Use Docker to run EventStoreDB from the official container image on DockerHub.

    $ docker run -d --name my-eventstoredb -it -p 2113:2113 -p 1113:1113 eventstore/eventstore:21.10.2-buster-slim --insecure

### Construct client

The class `EsdbClient` can be constructed with a `uri` that indicates the hostname
and port number of the EventStoreDB server.

```python
from esdbclient.client import EsdbClient

client = EsdbClient(uri='localhost:2113')
```

### Append events

The method `append_events()` can be used to append events to
a stream.

The arguments `stream_name`, `expected_position` and `new_events`
are required.

The `stream_name` argument is a string that uniquely identifies
the stream in the database.

The `expected_position` argument is an optional integer that specifies
the expected position of the end of the stream in the database: either
an integer representing the expected current position of the stream;
or `None` if the stream is expected not to exist.

The `events` argument is a list of new event objects to be appended to the
stream. The class `NewEvent` can be used to construct new event objects.

The method `append_events()` returns the "commit position", which is a
monotonically increasing integer representing the position of the recorded
event in a "total order" of all recorded events in all streams.

In the example below, a stream is created by appending a new event with
`expected_position=None`.

```python
from uuid import uuid4

from esdbclient.events import NewEvent

# Construct new event object.
event1 = NewEvent(type="OrderCreated", data=b"{}", metadata=b"{}")

# Define stream name.
stream_name1 = str(uuid4())

# Append list of events to new stream.
commit_position1 = client.append_events(
    stream_name=stream_name1, expected_position=None, events=[event1]
)
```

In the example below, two subsequent events are appended to an existing
stream. Since the stream only has one recorded event, the current position
of the end of the stream is `0`.

```python
event2 = NewEvent(type="OrderUpdated", data=b"{}", metadata=b"{}")
event3 = NewEvent(type="OrderDeleted", data=b"{}", metadata=b"{}")

commit_position2 = client.append_events(
    stream_name1, expected_position=0, events=[event2, event3]
)
```

Please note, whilst the appending in one operation of a list of events
is atomic (either all or none will be recorded), by design it is only
possible with EventStoreDB to atomically record events in one stream.

### Read stream events

The method `read_stream_events()` can be used to read the recorded
events in a stream. The argument `stream_name` is required. By default,
all recorded events in the stream are returned in the order they were
appended. An iterable object of recorded events is returned.

Read all the recorded events in a stream.

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

The method `read_stream_events()` also supports three optional arguments,
`position`, `backwards`, and `limit`.

The argument `position` is an optional integer that can be used to indicate
the stream position from which to start reading. This argument is `None` by default,
meaning that the stream will be read from the start, or from the end if `backwards`
is `True`.

The argument `backwards` is a boolean which is by default `False` meaning the
stream will be read forwards by default, so that events are returned in the
order they were appended, If `backwards` is `True`, the stream will be read
backwards, so that events are returned in reverse order.

The argument `limit` is an integer which limits the number of events that will
be returned.

Read recorded events in a stream from a specific stream position.

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

Read the recorded events in a stream backwards from the end.

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

Read a limited number of recorded events in stream.

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

Read a limited number of recorded events backwards from given position.

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
current_position = client.get_stream_position(stream_name1)

assert current_position == 2
```

The sequence of stream positions is intended to be gapless. It is
also usually zero-based, so that the position of the end of the stream
when one event has been appended is `0`. The position is `1` after two
events have been appended, `2` after three events have been appended,
and so on.

The position of a stream that does not exist is reported by this method to
be `None`.

```python
current_position = client.get_stream_position(stream_name="stream-unknown")

assert current_position == None
```

### Read all recorded events

The method `read_all_events()` can be used to read all recorded events
in all streams in the order they were committed. An iterable object of
recorded events is returned.

Read events from all streams in the order they were committed.

```python
events = list(client.read_all_events())

assert len(events) >= 3
```

The method `read_stream_events()` supports three optional arguments,
`position`, `backwards`, and `limit`.

The argument `position` is an optional integer that can be used to indicate
the commit position from which to start reading. This argument is `None` by default,
meaning that all the events will be read from the start, or from the end if `backwards`
is `True`. Please note, if given the commit position must be an actually existing
commit position, and any other numbers will result in an exception being raised.

The argument `backwards` is a boolean which is by default `False` meaning all the
events will be read forwards by default, so that events are returned in the
order they were committed, If `backwards` is `True`, all the events will be read
backwards, so that events are returned in reverse order.

The argument `limit` is an integer which limits the number of events that will
be returned.

Please note, if `backwards` is used in combination with `position`, the recorded
event at the given commit position will NOT be included. This differs from reading
events from a stream backwards from a stream position, when the recorded event at
the given stream position WILL be included.

Read recorded events in a stream from a particular position.

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

Read all the recorded events backwards from the end.

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

Read a limited number of recorded events from a specific commit position.

```python
events = list(client.read_all_events(position=commit_position1, limit=1))

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data
```

Read a limited number of recorded events backwards from the end.

```python
events = list(client.read_all_events(backwards=True, limit=1))

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 2
assert events[0].type == event3.type
assert events[0].data == event3.data
```

### Catch-up subscriptions

The method `subscribe_all_events()` can be used to create a
"catch-up subscription" to EventStoreDB. The optional argument
`position` can be used to specify a commit position from which
to receive recorded events. Please note, returned events are those
after the given commit position.

This method returns an iterable object, a `CatchupSubscription`,
from which recorded events can be obtained by iterating over the
subscription object.

The recorded events can be processed. The commit position of
the recorded event should be stored atomically with the results
of processing the event. The value of the `position` argument can
then be determined by reading the recorded commit position. This
will accomplish "exactly once" processing of the events, from the
point of view of the recorded results of processing the events, so
long as there is a uniqueness constraint on the recorded commit
position.


```python

# Get the current commit position.
commit_position = client.get_commit_position()

# Append three more events.
stream_name = str(uuid4())
event1 = NewEvent(type="OrderCreated", data=b"", metadata=b"{}")
event2 = NewEvent(type="OrderUpdated", data=b"", metadata=b"{}")
event3 = NewEvent(type="OrderDeleted", data=b"", metadata=b"{}")
client.append_events(
    stream_name, expected_position=None, events=[event1, event2, event3]
)

# Subscribe from the last commit position.
subscription = client.subscribe_all_events(position=commit_position)

# Check the stream name of the newly received events.
events = []
for event in subscription:
    assert event.stream_name == stream_name
    events.append(event)
    if len(events) == 3:
        break

del subscription
```

The `CatchupSubscription` subscription object might be used within a thread,
with recorded events put on a queue, for processing in a different thread.
However, this client doesn't provide such a thing.

### The NewEvent class

The `NewEvent` class can be used to define new events.

The attribute `type` is a unicode string, used to specify the type of the event
to be recorded.

The attribute `data` is a byte string, used to specify the data of the event
to be recorded.

The attribute `metadata` is a byte string, used to specify metadata for the event
to be recorded.

```python
new_event = NewEvent(
    type="OrderCreated",
    data=b"{}",
    metadata=b"{}",
)
```

### The RecordedEvent class

The `RecordedEvent` class is used when reading recorded events.

The attribute `type` is a unicode string, used to indicate the type of the event
that was recorded.

The attribute `data` is a byte string, used to specify the data of the event
that was recorded.

The attribute `metadata` is a byte string, used to specify metadata for the event
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
    type="OrderCreated",
    data=b"{}",
    metadata=b"{}",
    stream_name="stream1",
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
