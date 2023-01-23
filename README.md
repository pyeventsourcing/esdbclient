# Python gRPC Client for EventStoreDB

This package provides a Python gRPC client for
[EventStoreDB](https://www.eventstore.com/). It has been
developed and tested to work with EventStoreDB LTS version 21.10,
and with Python versions 3.7, 3.8, 3.9, 3.10, and 3.11.

Methods have typing annotations, the static typing is checked
with mypy, and the test coverage is 100%.

Not all the features of the EventStoreDB API are presented
by this client in its current form, however many of the most
useful aspects are presented in an easy-to-use interface (see below).
For an example of usage, see the [eventsourcing-eventstoredb](
https://github.com/pyeventsourcing/eventsourcing-eventstoredb) package.

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
support SSL/TLS connections. A future version of this library will support
"secure" connections.


### Construct client

The class `EsdbClient` can be constructed with a `uri` that indicates the
hostname and port number of the EventStoreDB server.

```python
from esdbclient import EsdbClient

client = EsdbClient(uri='localhost:2113')
```

### Append events

The method `append_events()` can be used to append events to
a stream.

Three arguments are required, `stream_name`, `expected_position`
and `events`.

The `stream_name` argument is a string that uniquely identifies
the stream in the database.

The `expected_position` argument is an optional integer that specifies
the expected position of the end of the stream in the database. That is,
either a positive integer representing the expected current position of
the stream, or `None` if the stream is expected not to exist. If there
is a mismatch with the actual position of the end of the stream, an exception
`ExpectedPositionError` will be raised by the client. This accomplishes
optimistic concurrency control when appending new events. If you need to
get the current position of the end of a steam, use the `get_stream_position()`
method (see below). If you wish to disable optimistic concurrency, set the
`expected_position` to a negative integer.

The `events` argument is a sequence of new event objects to be appended to the
named stream. The class `NewEvent` can be used to construct new event objects.

In the example below, a stream is created by appending a new event with
`expected_position=None`.

```python
from uuid import uuid4

from esdbclient import NewEvent

# Construct new event object.
event1 = NewEvent(
    type='OrderCreated',
    data=b'{}',
    metadata=b'{}'
)

# Define stream name.
stream_name1 = str(uuid4())

# Append list of events to new stream.
commit_position1 = client.append_events(
    stream_name=stream_name1,
    expected_position=None,
    events=[event1],
)
```

If the append operation is successful, this method
will return the database "commit position" as it was when the
operation was completed. Otherwise, an exception will be raised.

The commit position value can be used to wait for downstream
processing to have processed the appended events. For example,
after making a command, a user interface can wait before
making a query for an eventually consistent materialised
view that would be stale if those events have not yet been
processed.

A "commit position" is a monotonically increasing integer representing
the position of the recorded event in a "total order" of all recorded
events in the database. The sequence of commit positions is not gapless.
It represents the position of the event record on disk, and there are
usually large differences between successive commits.

In the example below, two subsequent events are appended to an existing
stream. The sequences of stream positions are zero-based, and so when a
stream only has one recorded event, the expected position of the end of
the stream is `0`.

```python
event2 = NewEvent(
    type='OrderUpdated',
    data=b'{}',
    metadata=b'{}',
)
event3 = NewEvent(
    type='OrderDeleted',
    data=b'{}',
    metadata=b'{}',
)

commit_position2 = client.append_events(
    stream_name=stream_name1,
    expected_position=0,
    events=[event2, event3],
)
```

Please note, the append operation is atomic, so that either all
or none of a given list of events will be recorded. By design it is only
possible with EventStoreDB to atomically record events in one stream,
which means each operation must only include events of one stream.

This method takes an optional argument `timeout` which is a float that sets
a deadline for the completion of the gRPC operation.


### Get current stream position

The method `get_stream_position()` can be used to get the
position of the end of a stream (the position of the last
recorded event in the stream).

```python
stream_position = client.get_stream_position(
    stream_name=stream_name1
)

assert stream_position == 2
```

The sequence of stream positions is gapless. It is zero-based, so that
the position of the end of the stream is `0` after one event has been
appended. The position is `1` after two events have been appended, and
`2` after three events have been appended, and so on.

If a stream does not exist, the returned stream position is `None`,
which corresponds to the required expected position when appending
events to a stream that does not exist (see above).

```python
stream_position = client.get_stream_position(
    stream_name='stream-unknown'
)

assert stream_position == None
```

This method takes an optional argument `timeout` which is a float that sets
a deadline for the completion of the gRPC operation.


### Read stream events

The method `read_stream_events()` can be used to read the recorded
events in a stream. An iterable object of recorded events is returned.

One argument is required, `stream_name`, which is the name of the
stream to be read. By default, the recorded events in the stream
are returned in the order they were recorded.

The example below shows how to read the recorded events of a stream
forwards from the start of the stream to the end of the stream.

```python
response = client.read_stream_events(
    stream_name=stream_name1
)
```

The iterable object is actually a Python generator, and we need to
iterate over it to get the recorded events from gRPC.

Let's construct a list from the generator, so that we can
check we have the events that were recorded above.

```python
events = list(response)
```

Now that we have a list of events, we can check we got the
three events that were appended to the stream.

```python
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

The optional argument `position` is an optional integer that can be used to indicate
the position in the stream from which to start reading. This argument is `None`
by default, which means the stream will be read either from the start of the
stream (the default behaviour), or from the end of the stream if `backwards` is
`True` (see below). When reading a stream from a specific position in the stream, the
recorded event at that position WILL be included, both when reading forwards
from that position, and when reading backwards from that position.

The optional argument `backwards` is a boolean, by default `False`, which means the
stream will be read forwards by default, so that events are returned in the
order they were appended, If `backwards` is `True`, the stream will be read
backwards, so that events are returned in reverse order.

The optional argument `limit` is an integer which limits the number of events that will
be returned. The default value is `sys.maxint`.

The optional argument `timeout` is a float which sets a deadline for the completion of
the gRPC operation.

The example below shows how to read recorded events in a stream forwards from
a specific stream position to the end of the stream.

```python
events = list(
    client.read_stream_events(
        stream_name=stream_name1,
        position=1,
    )
)

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

The example below shows how to read the recorded events in a stream backwards from
the end of the stream to the start of the stream.

```python
events = list(
    client.read_stream_events(
        stream_name=stream_name1,
        backwards=True,
    )
)

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
events = list(
    client.read_stream_events(
        stream_name=stream_name1,
        limit=2,
    )
)

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
events = list(
    client.read_stream_events(
        stream_name=stream_name1,
        position=2,
        backwards=True,
        limit=1,
    )
)

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 2
assert events[0].type == event3.type
assert events[0].data == event3.data
```

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

The optional argument `position` is an optional integer that can be used to specify
the commit position from which to start reading. This argument is `None` by
default, meaning that all the events will be read either from the start, or
from the end if `backwards` is `True` (see below). Please note, if specified,
the specified position must be an actually existing commit position, because
any other number will result in a server error (at least in EventStoreDB v21.10).
Please also note, when reading forwards the event at the specified position
WILL be included. However when reading backwards, the event at the specified position
will NOT be included. (This backwards behaviour of excluding the specified
position differs from the behaviour when reading a stream, I'm not sure why.)

The optional argument `backwards` is a boolean which is by default `False` meaning the
events will be read forwards by default, so that events are returned in the
order they were committed, If `backwards` is `True`, the events will be read
backwards, so that events are returned in reverse order.

The optional argument `filter_exclude` is a sequence of regular expressions that
match the type strings of recorded events that should not be included. By default,
this argument will match "system events", so that they will not be included.
This argument is ignored if `filter_include` is set.

The optional argument `filter_include` is a sequence of regular expressions
that match the type strings of recorded events that should be included. By
default, this argument is an empty tuple. If this argument is set to a
non-empty sequence, the `filter_exclude` argument is ignored.

Please note, the filtering happens on the EventStoreDB server, and the
`limit` argument is applied after filtering. See below for more information
about filter regular expressions.

The optional argument `limit` is an integer which limits the number of events that will
be returned. The default value is `sys.maxint`.

The optional argument `timeout` is a float which sets a deadline for the completion of
the gRPC operation.

The example below shows how to read all recorded events from a particular commit position.

```python
events = list(
    client.read_all_events(
        position=commit_position1
    )
)

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
events = list(
    client.read_all_events(
        backwards=True
    )
)

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
events = list(
    client.read_all_events(
        position=commit_position1,
        limit=1,
    )
)

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data
```

The example below shows how to read a limited number (one) of the recorded events
in the database backwards from the end. This gives the last recorded event.

```python
events = list(
    client.read_all_events(
        backwards=True,
        limit=1,
    )
)

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

This method is provided as a convenience when testing, and otherwise isn't
very useful. In particular, when reading all events (see above) or subscribing
to all events with a catch-up subscription (see below), the commit position
would normally be read from the downstream database, so that you are reading
from the last position that was successfully processed.

This method takes an optional argument `timeout` which is a float that sets
a deadline for the completion of the gRPC operation.


### Catch-up subscriptions

The method `subscribe_all_events()` can be used to create a
"catch-up subscription" to EventStoreDB.

This method takes an optional `position` argument, which can be
used to specify a commit position from which to subscribe for
recorded events. The default value is `None`, which means
the subscription will operate from the first recorded event
in the database.

This method returns an iterable object, from which recorded events
can be obtained by iteration, including events that are recorded
after the subscription was created.

Many catch-up subscriptions can be created, concurrently or
successively, and all will receive all the events they are
subscribed to receive.

The value of the `commit_position` attribute of recorded events can be
recorded along with the results of processing recorded events,
to track progress and to allow event processing to be resumed at
the correct position.

The example below shows how to subscribe to receive all recorded
events from a specific commit position. Three already-existing
events are received, and then three new events are recorded, which
are then received via the subscription.

```python

# Get the commit position (usually from database of materialised views).
commit_position = client.get_commit_position()

# Append three events.
stream_name1 = str(uuid4())
event1 = NewEvent(
    type='OrderCreated',
    data=b'{}',
    metadata=b'{}',
)
event2 = NewEvent(
    type='OrderUpdated',
    data=b'{}',
    metadata=b'{}',
)
event3 = NewEvent(
    type='OrderDeleted',
    data=b'{}',
    metadata=b'{}',
)
client.append_events(
    stream_name=stream_name1,
    expected_position=None,
    events=[event1, event2, event3],
)

# Subscribe from the commit position.
subscription = client.subscribe_all_events(
    position=commit_position
)

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
event4 = NewEvent(
    type='OrderCreated',
    data=b'{}',
    metadata=b'{}',
)
event5 = NewEvent(
    type='OrderUpdated',
    data=b'{}',
    metadata=b'{}',
)
event6 = NewEvent(
    type='OrderDeleted',
    data=b'{}',
    metadata=b'{}',
)
client.append_events(
    stream_name=stream_name,
    expected_position=None,
    events=[event4, event5, event6],
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

This method also support three other optional arguments, `filter_exclude`,
`filter_include`, and `timeout`.

The argument `filter_exclude` is a sequence of regular expressions that match
the type strings of recorded events that should not be included. By default,
this argument will match "system events", so that they will not be included.
This argument is ignored if `filter_include` is set.

The argument `filter_include` is a sequence of regular expressions
that match the type strings of recorded events that should be included. By
default, this argument is an empty tuple. If this argument is set to a
non-empty sequence, the `filter_exclude` argument is ignored.

Please note, in this version of this Python client, the filtering happens
within the client (rather than on the server as when reading all events) because
passing these filter options in the read request for subscriptions seems to cause
an error in EventStoreDB v21.10. See below for more information about filter
regular expressions.

The argument `timeout` is a float which sets a deadline for the completion of
the gRPC operation. This probably isn't very useful, but is included for
completeness and consistency with the other methods.

Catch-up subscriptions are not registered in EventStoreDB (they are not
"persistent subscriptions). It is simply a streaming gRPC call which is
kept open by the server, with newly recorded events sent to the client
as the client iterates over the subscription. This kind of subscription
is closed as soon as the subscription object goes out of memory.

```python
# End the subscription.
del subscription
```

Received events do not need to be (and indeed cannot be) acknowledged back
to the EventStoreDB server. Acknowledging events is an aspect of "persistent
subscriptions", which is a feature of EventStoreDB that is not (currently)
supported by this client. Whilst there are some advantages of persistent
subscriptions, by tracking in the upstream server the position in the commit
sequence of events that have been processed, there is a danger of "dual writing"
in the consumption of events. The danger is that if the event is successfully
processed but then the acknowledgment fails, the event may be processed more
than once. On the other hand, if the acknowledgment is successful but then the
processing fails, the event may not be been processed. By either processing
an events more than once, or failing to process and event, the resulting state
of the processing of the recorded events might be inaccurate, or possibly
inconsistent, and perhaps catastrophically so. Of course, such inaccuracies may
or may not matter in your situation. But catastrophic inconsistencies may halt
processing until the issue is resolved. The only protection against this danger
is to avoid "dual writing" by atomically recording the commit position of an
event that has been processed along with the results of process the event, that
is with both things being recorded in the same transaction.

To accomplish "exactly once" processing of the events, the commit position
of a recorded event should be recorded atomically and uniquely along with
the result of processing recorded events, for example in the same database
as materialised views when implementing eventually-consistent CQRS, or in
the same database as a downstream analytics or reporting or archiving
application. This avoids "dual writing" in the processing of events.

The subscription object might be used directly when processing events. It might
also be used within a thread dedicated to receiving events, with recorded events
put on a queue for processing in a different thread. This package doesn't provide
such thread or queue objects, you would need to do that yourself. Just make sure
to reconstruct the subscription (and the queue) using your last recorded commit
position when resuming the subscription after an error, to be sure all events
are processed once.

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

recorded_event = RecordedEvent(
    id=uuid4(),
    type='OrderCreated',
    data=b'{}',
    metadata=b'{}',
    stream_name='stream1',
    stream_position=0,
    commit_position=512,
)
```

### Filter regular expressions

The `filter_exclude` and `filter_include` arguments in `read_all_events()` and
`subscribe_all_events()` are applied to the `type` attribute of recorded events.

The default value of the `filter_exclude` arguments is designed to exclude
EventStoreDB "system events", which otherwise would be included. System
events, by convention in EventStoreDB, all have `type` strings that
start with the `$` sign.

Please note, characters that have a special meaning in regular expressions
will need to be escaped (with double-backslash) when matching these characters
in type strings.

For example, to match EventStoreDB system events, use the sequence `['\\$.*']`.
Please note, the constant `ESDB_EVENTS_REGEX` is set to `'\\$.*'`. You
can import this value (`from esdbclient import ESDB_EVENTS_REGEX`) and use
it when building longer sequences of regular expressions. For example,
to exclude system events and snapshots, you might use the sequence
`[ESDB_EVENTS_REGEX, '.*Snapshot']` as the value of the `filter_exclude`
argument.


### Stop EventStoreDB

Use Docker to stop and remove the EventStoreDB container.

    $ docker stop my-eventstoredb
	$ docker rm my-eventstoredb


## Developers

### Install Poetry

The first thing is to check you have Poetry installed.

    $ poetry --version

If you don't, then please [install Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer).

    $ curl -sSL https://install.python-poetry.org | python3 -

It will help to make sure Poetry's bin directory is in your `PATH` environment variable.

But in any case, make sure you know the path to the `poetry` executable. The Poetry
installer tells you where it has been installed, and how to configure your shell.

Please refer to the [Poetry docs](https://python-poetry.org/docs/) for guidance on
using Poetry.

### Setup for PyCharm users

You can easily obtain the project files using PyCharm (menu "Git > Clone...").
PyCharm will then usually prompt you to open the project.

Open the project in a new window. PyCharm will then usually prompt you to create
a new virtual environment.

Create a new Poetry virtual environment for the project. If PyCharm doesn't already
know where your `poetry` executable is, then set the path to your `poetry` executable
in the "New Poetry Environment" form input field labelled "Poetry executable". In the
"New Poetry Environment" form, you will also have the opportunity to select which
Python executable will be used by the virtual environment.

PyCharm will then create a new Poetry virtual environment for your project, using
a particular version of Python, and also install into this virtual environment the
project's package dependencies according to the `pyproject.toml` file, or the
`poetry.lock` file if that exists in the project files.

You can add different Poetry environments for different Python versions, and switch
between them using the "Python Interpreter" settings of PyCharm. If you want to use
a version of Python that isn't installed, either use your favourite package manager,
or install Python by downloading an installer for recent versions of Python directly
from the [Python website](https://www.python.org/downloads/).

Once project dependencies have been installed, you should be able to run tests
from within PyCharm (right-click on the `tests` folder and select the 'Run' option).

Because of a conflict between pytest and PyCharm's debugger and the coverage tool,
you may need to add ``--no-cov`` as an option to the test runner template. Alternatively,
just use the Python Standard Library's ``unittest`` module.

You should also be able to open a terminal window in PyCharm, and run the project's
Makefile commands from the command line (see below).

### Setup from command line

Obtain the project files, using Git or suitable alternative.

In a terminal application, change your current working directory
to the root folder of the project files. There should be a Makefile
in this folder.

Use the Makefile to create a new Poetry virtual environment for the
project and install the project's package dependencies into it,
using the following command.

    $ make install-packages

It's also possible to also install the project in 'editable mode'.

    $ make install

Please note, if you create the virtual environment in this way, and then try to
open the project in PyCharm and configure the project to use this virtual
environment as an "Existing Poetry Environment", PyCharm sometimes has some
issues (don't know why) which might be problematic. If you encounter such
issues, you can resolve these issues by deleting the virtual environment
and creating the Poetry virtual environment using PyCharm (see above).

### Project Makefile commands

You can start EventStoreDB using the following command.

    $ make start-eventstoredb

You can run tests using the following command (needs EventStoreDB to be running).

    $ make test

You can stop EventStoreDB using the following command.

    $ make stop-eventstoredb

You can check the formatting of the code using the following command.

    $ make lint

You can reformat the code using the following command.

    $ make fmt

Tests belong in `./tests`. Code-under-test belongs in `./esdbclient`.

Edit package dependencies in `pyproject.toml`. Update installed packages (and the
`poetry.lock` file) using the following command.

    $ make update-packages
