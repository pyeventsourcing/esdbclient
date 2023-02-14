# Python gRPC Client for EventStoreDB

This package provides a Python gRPC client for
[EventStoreDB](https://www.eventstore.com/).

This client is implemented as the Python class `ESDBClient`.

This client has been developed and tested to work with EventStoreDB LTS
versions 21.10 and 21.10, without and without SSL/TLS enabled on both
these versions, and with Python versions 3.7, 3.8, 3.9, 3.10, and 3.11
across all of the above. The test coverage is 100% including branch coverage.

All the Python code in this package has typing annotations. The static typing
annotations are checked relatively strictly with mypy. The code is formatted
with black and isort, and also checked with flake8. Poetry is used for package
management during development, and for building and publishing distributions to
[PyPI](https://pypi.org/project/esdbclient/).

Not all the features of the EventStoreDB API are presented
by this client in its current form, however many of the most
useful aspects are presented in an easy-to-use interface (see below).

Probably the three most useful methods are `append_events()`, `read_stream_events()`
and `subscribe_all_events()`.

* The `append_events()` method can be used to record atomically all the new
events that are generated when executing a command in an application.

* The `read_stream_events()` method can be used to retrieve all the recorded
events for an aggregate before executing a command in an application.

* The `subscribe_all_events()` method can be used by a downstream component
to process recorded events with exactly-once semantics.

For an example of usage, see the [eventsourcing-eventstoredb](
https://github.com/pyeventsourcing/eventsourcing-eventstoredb) package.

## Table of contents

<!-- TOC -->
* [Install package](#install-package)
  * [From PyPI](#from-pypi)
  * [With Poetry](#with-poetry)
* [Server container](#server-container)
  * [Run container](#run-container)
  * [Stop container](#stop-container)
* [Client class](#client-class)
  * [Import class from package](#import-class-from-package)
  * [Contruct client class](#contruct-client-class)
* [Streams](#streams)
  * [Append events](#append-events)
  * [Append event](#append-event)
  * [Idempotent append operations](#idempotent-append-operations)
  * [Read stream events](#read-stream-events)
  * [Read all events](#read-all-events)
  * [Get current stream position](#get-current-stream-position)
  * [Get current commit position](#get-current-commit-position)
* [Catch-up subscriptions](#catch-up-subscriptions)
  * [How to implement exactly-once event processing](#how-to-implement-exactly-once-event-processing)
  * [Subscribe all events](#subscribe-all-events)
  * [Subscribe stream events](#subscribe-stream-events)
* [Persistent subscriptions](#persistent-subscriptions)
  * [Create subscription](#create-subscription)
  * [Read subscription](#read-subscription)
  * [Create stream subscription](#create-stream-subscription)
  * [Read stream subscription](#read-stream-subscription)
* [Notes](#notes)
  * [Regular expression filters](#regular-expression-filters)
  * [New event objects](#new-event-objects)
  * [Recorded event objects](#recorded-event-objects)
* [Contributors](#contributors)
  * [Install Poetry](#install-poetry)
  * [Setup for PyCharm users](#setup-for-pycharm-users)
  * [Setup from command line](#setup-from-command-line)
  * [Project Makefile commands](#project-makefile-commands)
<!-- TOC -->

## Install package

It is recommended to install Python packages into a Python virtual environment.

### From PyPI

You can use pip to install this package directly from
[the Python Package Index](https://pypi.org/project/esdbclient/).

    $ pip install esdbclient

### With Poetry

You can use Poetry to add this package to your pyproject.toml and install it.

    $ poetry add esdbclient

## Server container

The EventStoreDB server can be run locally using the official Docker container image.

### Run container

Use Docker to run EventStoreDB using the official Docker container image on DockerHub.

For development, you can start a "secure" server locally on port 2113.

    $ docker run -d --name my-eventstoredb -it -p 2113:2113 --env "HOME=/tmp" eventstore/eventstore:22.10.0-buster-slim --dev

Alternatively, you can start an "insecure" server locally on port 2113.

    $ docker run -d --name my-eventstoredb -it -p 2113:2113 eventstore/eventstore:22.10.0-buster-slim --insecure

To connect to the "insecure" local server using the client in this package, you just need
to know the local hostname and the port number. To connect to the "secure" local
development server, you will also need to know that the username is "admin" and
the password is "changeit". You will also need to get the SSL/TLS certificate from
the server. You can get the server certificate with the following command.

    $ python -c "import ssl; print(get_server_certificate(addr=('localhost', 2113)))"


### Stop container

To stop and remove the `my-eventstoredb` container created above, use the following Docker commands.

    $ docker stop my-eventstoredb
	$ docker rm my-eventstoredb


## Client class

This client is implemented as the Python class `ESDBClient`.

### Import class from package

The `ESDBClient` class can be imported from the `esdbclient` package.

```python
from esdbclient import ESDBClient
```

### Contruct client class

The `ESDBClient` class can be constructed with `host` and `port` arguments.
The `host` and `port` arguments indicate the hostname and port number of the
EventStoreDB server.

If the EventStoreDB server is "secure", then also use the `server_cert`,
`username` and `password` arguments.

The `host` argument is expected to be a Python `str`. The `port` argument is expected
to be a Python `int`. The `server_cert` is expected to be a Python `str` containing
the PEM encoded SSL/TLS server certificate. Both `username` and `password` are expected
to be a Python `str`.

In the example below, the constructor argument values are taken from the operating
system environment, because the examples in this document are tested with both
a "secure" and an "insecure" server.

```python
import os

client = ESDBClient(
    host=os.getenv("ESDB_HOST"),
    port=int(os.getenv("ESDB_PORT")),
    server_cert=os.getenv("ESDB_SERVER_CERT"),
    username=os.getenv("ESDB_USERNAME"),
    password=os.getenv("ESDB_PASSWORD"),
)
```

## Streams

In EventStoreDB, a "stream" is a sequence of recorded events that all have
the same "stream name". Each recorded event has a "stream position" in its stream,
and a "commit position" in the database. The stream positions of the recorded events
in a stream is a gapless sequence starting from zero. The commit positions of the
recorded events in the database form a sequence that is not gapless.

The methods `append_events()`, `read_stream_events()` and `read_all_events()` can
be used to record and read events in the database.

### Append events

The `append_events()` method can be used to write a sequence of new events atomically
to a "stream". Writing new events either creates a stream, or appends events to the end
of a stream. This method is idempotent (see below).

This method can be used to record atomically all the new
events that are generated when executing a command in an application.

Three arguments are required, `stream_name`, `expected_position`
and `events`.

The `stream_name` argument is required, and is expected to be a Python
`str` object that uniquely identifies the stream in the database.

The `expected_position` argument is required, is expected to be: `None`
if events are being written to a new stream, and otherwise an Python `int`
equal to the position in the stream of the last recorded event in the stream.

The `events` argument is required, and is expected to be a sequence of new
event objects to be appended to the named stream. The `NewEvent` class should
be used to construct new event objects (see below).

This method takes an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

Streams are created by writing events. The correct value of the `expected_position`
argument when writing the first event of a new stream is `None`. Please note, it is
not possible to somehow create an "empty" stream in EventStoreDB.

The stream positions of recorded events in a stream start from zero, and form a gapless
sequence of integers. The stream position of the first recorded event in a stream is
`0`. And so when appending the second new event to a stream that has one recorded event,
the correct value of the `expected_position` argument is `0`. Similarly, the stream
position of the second recorded event in a stream is `1`, and so when appending the
third new event to a stream that has two recorded events, the correct value of the
`expected_position` argument is `1`. And so on... (There is a theoretical maximum
number of recorded events that any stream can have, but I'm not sure what it is;
maybe 9,223,372,036,854,775,807 because it is implemented as a `long` in C#?)

If there is a mismatch between the given value of the `expected_position` argument
and the position of the last recorded event in a stream, then an `ExpectedPositionError`
exception will be raised. This effectively accomplishes optimistic concurrency control.

If you wish to disable optimistic concurrency control when appending new events, you
can set the `expected_position` to a negative integer.

If you need to discover the current position of the last recorded event in a stream,
you can use the `get_stream_position()` method (see below).

Please note, the append events operation is atomic, so that either all
or none of the given new events will be recorded. By design, it is only
possible with EventStoreDB to atomically record new events in one stream.

In the example below, a new event is appended to a new stream.

```python
from uuid import UUID, uuid4

from esdbclient import NewEvent

# Construct new event object.
event1 = NewEvent(type='OrderCreated', data=b'data1')

# Define stream name.
stream_name1 = str(uuid4())

# Append list of events to new stream.
commit_position1 = client.append_events(
    stream_name=stream_name1,
    expected_position=None,
    events=[event1],
)
```

In the example below, two subsequent events are appended to an existing
stream.

```python
event2 = NewEvent(type='OrderUpdated', data=b'data2')
event3 = NewEvent(type='OrderDeleted', data=b'data3')

commit_position2 = client.append_events(
    stream_name=stream_name1,
    expected_position=0,
    events=[event2, event3],
)
```

If the operation is successful, this method returns an integer
representing the overall "commit position" as it was when the operation
was completed. Otherwise, an exception will be raised.

A "commit position" is a monotonically increasing integer representing
the position of the recorded event in a "total order" of all recorded
events in the database across all streams. It is the actual position
of the event record on disk, and there are usually large differences
between successive commits. In consequence, the sequence of commit
positions is not gapless. Indeed, there are usually large differences
between the commit positions of successive recorded events.

The "commit position" returned by `append_events()` is that of the last
recorded event in the given batch of new events.

The "commit position" returned in this way can therefore be used to wait
for a downstream component to have processed all the events that were recorded.

For example, consider a user interface command that results in the recording
of new events, and a query into an eventually consistent materialized
view in a downstream component that is updated from these events. If the new
events have not yet been processed, the view would be stale. The "commit position"
can be used by the user interface to poll the downstream component until it has
processed those new events, after which time the view will not be stale.


### Append event

The `append_event()` method can be used to write a single new event to a stream.

Three arguments are required, `stream_name`, `expected_position` and `event`.

This method works in the same way as `append_events()`, however `event` is expected
to be a single `NewEvent`.

This method takes an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

Since the handling of a command in your application may result in one or many
new events, and the results of handling a command should be recorded atomically,
and the writing of new events generated by a command handler is usually a concern
that is factored out and used everywhere in a project, it is quite usual in a project
to only use `append_events()` to record new events. For this reason, an example is
not provided here.


### Idempotent append operations

Sometimes it may happen that a new event is successfully recorded and then somehow
the connection to the database gets interrupted before the successful call can return
successfully to the client. In case of an error when appending an event, it may be
desirable to retry appending the same event at the same position. If the event was
in fact successfully recorded, it is convenient for the retry to return successfully
without raising an error due to optimistic concurrency control (as described above).

The example below shows the `append_events()` method being called again with
`event3` and `expected_position=2`. We can see that repeating the call to
`append_events()` returns successfully.

```python
# Retry appending event3.
commit_position_retry = client.append_events(
    stream_name=stream_name1,
    expected_position=0,
    events=[event2, event3],
)
```

We can see that the same commit position is returned as above.

```python
assert commit_position_retry == commit_position2
```

We can also see the stream has been unchanged despite calling the append_events()
twice with the same arguments, by calling `read_stream_events()`. That is, there
are still only three events in the stream.

```python
response = client.read_stream_events(
    stream_name=stream_name1
)

events = list(response)
assert len(events) == 3
```

This idempotent behaviour is activated because the `NewEvent` class has an `id`
attribute that, by default, is assigned a new and unique version-4 UUID when an
instance of `NewEvent` is constructed. If events with the same `id` are appended
at the same `expected_position`, the stream will be unchanged, the operation will
complete successfully, and the same commit position will be returned to the caller.

```python
assert isinstance(event1.id, UUID)
assert isinstance(event2.id, UUID)
assert isinstance(event3.id, UUID)

assert event1.id != event2.id
assert event2.id != event3.id

assert events[0].id == event1.id
assert events[1].id == event2.id
assert events[2].id == event3.id
```

It is possible to set the `id` constructor argument of `NewEvent` when instantiating
the `NewEvent` class, but in the examples above we have been using the default
behaviour, which is that the `id` value is generated when the `NewEvent` class is
instantiated.


### Read stream events

The `read_stream_events()` method can be used to read the recorded events of a stream.

This method can be used to retrieve all the recorded events for an aggregate before
executing a command in an application.

This method has one required argument, `stream_name`, which is the name of
the stream from which to read events. By default, the recorded events in the
stream are returned in the order they were recorded.

The method `read_stream_events()` also supports four optional arguments,
`stream_position`, `backwards`, `limit`, and `timeout`.

The optional `stream_position` argument is an optional integer that can be used to
indicate the position in the stream from which to start reading. This argument is
`None` by default, which means the stream will be read either from the start of the
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

The optional argument `timeout` is a Python `float` which sets a deadline for the completion of
the gRPC operation.

This method returns a Python iterable object that yields `RecordedEvent` objects.
These recorded event objects are instances of the `RecordedEvent` class (see below)

The example below shows how to read the recorded events of a stream
forwards from the start of the stream to the end of the stream. The
name of a stream is given when calling the method. In this example,
the iterable response object is converted into a Python `list`, which
contains all the recorded event objects that were read from the stream.

```python
response = client.read_stream_events(
    stream_name=stream_name1
)

events = list(response)
```

Now that we have a list of event objects, we can check we got the
three events that were appended to the stream, and that they are
ordered exactly as they were appended.

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

The example below shows how to read recorded events in a stream forwards from
a specific stream position to the end of the stream.

```python
events = list(
    client.read_stream_events(
        stream_name=stream_name1,
        stream_position=1,
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
in a stream forwards from the start of the stream.

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

The example below shows how to read a limited number (one) of the recorded
events in a stream backwards from a given stream position.

```python
events = list(
    client.read_stream_events(
        stream_name=stream_name1,
        stream_position=2,
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

### Read all events

The method `read_all_events()` can be used to read all recorded events
in the database in the order they were recorded. An iterable object of
recorded events is returned. This iterable object will stop when it has
yielded the last recorded event.

This method supports six optional arguments, `commit_position`, `backwards`,
`filter_exclude`, `filter_include`, `limit`, and `timeout`.

The optional argument `commit_position` is an optional integer that can be used to
specify the commit position from which to start reading. This argument is `None` by
default, meaning that all the events will be read either from the start, or
from the end if `backwards` is `True` (see below). Please note, if specified,
the specified position must be an actually existing commit position, because
any other number will result in a server error (at least in EventStoreDB v21.10).

The optional argument `backwards` is a boolean which is by default `False` meaning the
events will be read forwards by default, so that events are returned in the
order they were committed, If `backwards` is `True`, the events will be read
backwards, so that events are returned in reverse order.

The optional argument `filter_exclude` is a sequence of regular expressions that
match the type strings of recorded events that should not be included. By default,
this argument will match "system events", so that they will not be included.
This argument is ignored if `filter_include` is set to a non-empty sequence.

The optional argument `filter_include` is a sequence of regular expressions
that match the type strings of recorded events that should be included. By
default, this argument is an empty tuple. If this argument is set to a
non-empty sequence, the `filter_exclude` argument is ignored.

The optional argument `limit` is an integer which limits the number of events that will
be returned. The default value is `sys.maxint`.

The optional argument `timeout` is a Python `float` which sets a deadline for the completion of
the gRPC operation.

The filtering of events is done on the EventStoreDB server. The
`limit` argument is applied on the server after filtering. See below for
more information about filter regular expressions.

When reading forwards from a specific commit position, the event at the specified
position WILL be included. However, when reading backwards, the event at the
specified position will NOT be included. (This non-inclusive behaviour, of excluding
the specified commit position when reading all streams backwards, differs from the
behaviour when reading a stream backwards from a specific stream position, I'm
not sure why.)

The example below shows how to read all events in the database in the
order they were recorded.

```python
events = list(client.read_all_events())

assert len(events) >= 3
```

The example below shows how to read all recorded events from a specific commit position.

```python
events = list(
    client.read_all_events(
        commit_position=commit_position1
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
in the database forwards from a specific commit position. Please note, when reading
all events forwards from a specific commit position, the event at the specified
position WILL be included.


```python
events = list(
    client.read_all_events(
        commit_position=commit_position1,
        limit=1,
    )
)

assert len(events) == 1

assert events[0].stream_name == stream_name1
assert events[0].stream_position == 0
assert events[0].type == event1.type
assert events[0].data == event1.data

assert events[0].commit_position == commit_position1
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

The example below shows how to read a limited number (one) of the recorded events
in the database backwards from a specific commit position. Please note, when reading
all events backwards from a specific commit position, the event at the specified
position WILL NOT be included.

```python
events = list(
    client.read_all_events(
        commit_position=commit_position2,
        backwards=True,
        limit=1,
    )
)

assert len(events) == 1

assert events[0].commit_position < commit_position2
```


### Get current stream position

The `get_stream_position()` method can be used to
get the "stream position" of the last recorded event in a stream.

This method has a `stream_name` argument, which is required.

This method also takes an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

The sequence of positions in a stream is gapless. It is zero-based,
so that a stream with one recorded event has a current stream
position of `0`. The current stream position is `1` when a stream has
two events, and it is `2` when there are events, and so on.

In the example below, the current stream position is obtained of the
stream to which events were appended in the examples above.
Because the sequence of stream positions is zero-based, and because
three events were appended, so the current stream position is `2`.

```python
stream_position = client.get_stream_position(
    stream_name=stream_name1
)

assert stream_position == 2
```

If a stream does not exist, the returned stream position value is `None`,
which matches the required expected position when appending the first event
of a new stream (see above).

```python
stream_position = client.get_stream_position(
    stream_name=str(uuid4())
)

assert stream_position == None
```

This method takes an optional argument `timeout` which is a Python `float` that sets
a deadline for the completion of the gRPC operation.


### Get current commit position

The method `get_commit_position()` can be used to get the current
commit position of the database.

```python
commit_position = client.get_commit_position()
```

This method takes an optional argument `timeout` which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

This method can be useful to measure progress of a downstream component
that is processing all recorded events, by comparing the current commit
position with the recorded commit position of the last successfully processed
event in a downstream component.

The value of the `commit_position` argument when reading events either by using
the `read_all_events()` method or by using a catch-up subscription would usually
be determined by the recorded commit position of the last successfully processed
event in a downstream component.


## Catch-up subscriptions

A "catch-up subscription" can be used to receive already recorded events, but
it will also return events that are recorded after the subscription was started.

The method `subscribe_stream_events()` starts a catch-up subscription to receive
events from a specific stream. The method `subscribe_all_events()` starts a catch-up
subscription to receive all events in the database.

Catch-up subscriptions are simply a streaming gRPC call which is
kept open by the server, with newly recorded events sent to the client
as the client iterates over the subscription.

Many catch-up subscriptions can be created, concurrently or successively, and all
will receive all the recorded events they have been requested to receive.

Received recorded events are instances of the `RecordedEvent` class (see below).
Recorded event objects have a commit position, amonst other attributes.

### How to implement exactly-once event processing

The commit positions of recorded events that are received and processed by a
downstream component are usefully recorded by the downstream component so that
the commit position of last processed event can be determined.

The last recorded commit position can be used to specify the commit position from which
to subscribe when processing is resumed. Since this commit position will represent the
position of the last successfully processed event in a downstream component, so it
will be usual to want the next event after this position, because that is the next
event that has not yet been processed. For this reason, when subscribing for events
from a specific commit position using a catch-up subscription in EventStoreDB, the
recorded event at the specified commit position will NOT be included in the sequence
of recorded events that are received.

To accomplish "exactly-once" processing of recorded events in a downstream
component when using a catch-up subscription, the commit position of a recorded
event should be recorded atomically and uniquely along with the result of processing
recorded events, for example in the same database as materialised views when
implementing eventually-consistent CQRS, or in the same database as a downstream
analytics or reporting or archiving application. By recording the commit position
of recorded events atomically with the new state that results from processing
recorded events, "dual writing" in the consumption of recorded events can be
avoided. By also recording the commit position uniquely, the new state cannot be
recorded twice, and hence the recorded state of the downstream component will be
updated only once for any recorded event. By using the greatest recorded commit
position to resume a catch-up subscription, all recorded events will eventually
be processed. The combination of the "at-most-once" condition and the "at-least-once"
condition gives the "exactly-once" condition.

The danger with "dual writing" in the consumption of recorded events is that if a
recorded event is successfully processed and new state recorded atomically in one
transaction with the commit position recorded in a separate transaction, one may
happen and not the other. If the new state is recorded but the position is lost,
and then the processing is stopped and resumed, the recorded event may be processed
twice. On the other hand, if the commit position is recorded but the new state is
lost, the recorded event may effectively not be processed at all. By either
processing an event more than once, or by failing to process an event, the recorded
state of the downstream component might be inaccurate, or possibly inconsistent, and
perhaps catastrophically so. Such consequences may or may not matter in your situation.
But sometimes inconsistencies may halt processing until the issue is resolved. You can
avoid "dual writing" in the consumption of events by atomically recording the commit
position of a recorded event along with the new state that results from processing that
event in the same atomic transaction. By making the recording of the commit positions
unique, so that transactions will be rolled back when there is a conflict, you will
prevent the results of any duplicate processing of a recorded event being committed.

Recorded events received from a catch-up subscription cannot be acknowledged back
to the EventStoreDB server. Acknowledging events, however, is an aspect of "persistent
subscriptions" (see below). Hoping to rely on acknowledging events to an upstream
component is an example of dual writing.

### Subscribe all events

The`subscribe_all_events()` method can be used to start a "catch-up" subscription
that can return all events in the database.

This method can be used by a downstream component
to process recorded events with exactly-once semantics.

This method takes an optional `commit_position` argument, which can be
used to specify a commit position from which to subscribe for
recorded events. The default value is `None`, which means
the subscription will operate from the first recorded event
in the database. If a commit position is given, it must match
an actually existing commit position in the database. The events
that are obtained will not include the event recorded at that commit
position.

This method also takes three other optional arguments, `filter_exclude`,
`filter_include`, and `timeout`.

The argument `filter_exclude` is a sequence of regular expressions matching
the type strings of recorded events that should be excluded. By default,
this argument will match "system events", so that they will not be included.
This argument is ignored if `filter_include` is set to a non-empty sequence.

The argument `filter_include` is a sequence of regular expressions
matching the type strings of recorded events that should be included. By
default, this argument is an empty tuple. If this argument is set to a
non-empty sequence, the `filter_exclude` argument is ignored.

Please note, the filtering happens on the EventStoreDB server, and the
`limit` argument is applied on the server after filtering. See below for
more information about filter regular expressions.

The argument `timeout` is a Python `float` which sets a deadline for the completion of
the gRPC operation. This probably isn't very useful, but is included for
completeness and consistency with the other methods.

This method returns a Python iterator that yields recorded events, including events
that are recorded after the subscription was created. Iterating over this object will
therefore not stop, unless the connection to the database is lost. The connection will
be closed when the iterator object is deleted from memory, which will happen when the
iterator object goes out of scope or is explicitly deleted (see below). The connection
may also be closed by the server.

The subscription object can be used directly, but it might be used within a threaded
loop dedicated to receiving events that can be stopped in a controlled way, with
recorded events put on a queue for processing in a different thread. This package
doesn't provide such a threaded or queuing object class. Just make sure to reconstruct
the subscription (and the queue) using the last recorded commit position when resuming
the subscription after an error, to be sure all events are processed once.

The example below shows how to subscribe to receive all recorded
events from the start, and then resuming from a specific commit position.
Three already-recorded events are received, and then three new events are
recorded, which are then received via the subscription.

```python

# Append an event to a new stream.
stream_name2 = str(uuid4())
event4 = NewEvent(type='OrderCreated', data=b'data4')
client.append_events(
    stream_name=stream_name2,
    expected_position=None,
    events=[event4],
)

# Subscribe from the first recorded event in the database.
subscription = client.subscribe_all_events()
received_events = []

# Process events received from the catch-up subscription.
for event in subscription:
    last_commit_position = event.commit_position
    received_events.append(event)
    if event.id == event4.id:
        break

assert received_events[-4].id == event1.id
assert received_events[-3].id == event2.id
assert received_events[-2].id == event3.id
assert received_events[-1].id == event4.id

# Append subsequent events to the stream.
event5 = NewEvent(type='OrderUpdated', data=b'data5')
client.append_events(
    stream_name=stream_name2,
    expected_position=0,
    events=[event5],
)

# Receive subsequent events from the subscription.
for event in subscription:
    last_commit_position = event.commit_position
    received_events.append(event)
    if event.id == event5.id:
        break


assert received_events[-5].id == event1.id
assert received_events[-4].id == event2.id
assert received_events[-3].id == event3.id
assert received_events[-2].id == event4.id
assert received_events[-1].id == event5.id


# Append more events to the stream.
event6 = NewEvent(type='OrderDeleted', data=b'data6')
client.append_events(
    stream_name=stream_name2,
    expected_position=1,
    events=[event6],
)


# Resume subscribing from the last commit position.
subscription = client.subscribe_all_events(
    commit_position=last_commit_position
)


# Catch up by receiving the new event from the subscription.
for event in subscription:
    received_events.append(event)
    if event.id == event6.id:
        break

assert received_events[-6].id == event1.id
assert received_events[-5].id == event2.id
assert received_events[-4].id == event3.id
assert received_events[-3].id == event4.id
assert received_events[-2].id == event5.id
assert received_events[-1].id == event6.id


# Append three more events to a new stream.
stream_name3 = str(uuid4())
event7 = NewEvent(type='OrderCreated', data=b'data7')
event8 = NewEvent(type='OrderUpdated', data=b'data8')
event9 = NewEvent(type='OrderDeleted', data=b'data9')

client.append_events(
    stream_name=stream_name3,
    expected_position=None,
    events=[event7, event8, event9],
)

# Receive the three new events from the resumed subscription.
for event in subscription:
    received_events.append(event)
    if event.id == event9.id:
        break

assert received_events[-9].id == event1.id
assert received_events[-8].id == event2.id
assert received_events[-7].id == event3.id
assert received_events[-6].id == event4.id
assert received_events[-5].id == event5.id
assert received_events[-4].id == event6.id
assert received_events[-3].id == event7.id
assert received_events[-2].id == event8.id
assert received_events[-1].id == event9.id
```

The catch-up subscription gRPC operation is ended as soon as the subscription object
goes out of scope or is explicitly deleted from memory.

```python
# End the subscription.
del subscription
```

### Subscribe stream events

The`subscribe_stream_events()` method can be used to start a "catch-up" subscription
that can return all events in a stream.

This method takes a `stream_name` argument, which specifies the name of the stream
from which recorded events will be received.

This method takes an optional `stream_position` argument, which specifies a
stream position in the stream from which recorded events will be received. The
event at the specified stream position will not be included.

This method takes an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

The example below shows how to start a catch-up subscription to a stream.

```python

# Subscribe to events from stream2, from the start.
subscription = client.subscribe_stream_events(stream_name=stream_name2)

# Read from the subscription.
events = []
for event in subscription:
    events.append(event)
    if event.id == event6.id:
        break

# Check we got events only from stream2.
assert len(events) == 3
events[0].stream_name == stream_name2
events[1].stream_name == stream_name2
events[2].stream_name == stream_name2

# Append another event to stream1.
event10 = NewEvent(type="OrderUndeleted", data=b'data10')
client.append_events(
    stream_name=stream_name1,
    expected_position=2,
    events=[event10],
)

# Append another event to stream2.
event11 = NewEvent(type="OrderUndeleted", data=b'data11')
client.append_events(
    stream_name=stream_name2,
    expected_position=2,
    events=[event11]
)

# Continue reading from the subscription.
for event in subscription:
    events.append(event)
    if event.id == event11.id:
        break

# Check we got events only from stream2.
assert len(events) == 4
events[0].stream_name == stream_name2
events[1].stream_name == stream_name2
events[2].stream_name == stream_name2
events[3].stream_name == stream_name2
```

The example below shows how to start a catch-up subscription to a stream from a
specific stream position.

```python

# Subscribe to events from stream2, from the start.
subscription = client.subscribe_stream_events(
    stream_name=stream_name2,
    stream_position=1,
)

# Read event from the subscription.
events = []
for event in subscription:
    events.append(event)
    if event.id == event11.id:
        break

# Check we got events only after position 1.
assert len(events) == 2
events[0].id == event6.id
events[0].stream_position == 2
events[0].stream_name == stream_name2
events[1].id == event11.id
events[1].stream_position == 3
events[1].stream_name == stream_name2
```

## Persistent subscriptions

### Create subscription

The method `create_subscription()` can be used to create a
"persistent subscription" to EventStoreDB.

This method takes a required `group_name` argument, which is the
name of a "group" of consumers of the subscription.

This method takes an optional `from_end` argument, which can be
used to specify that the group of consumers of the subscription should
only receive events that were recorded after the subscription was created.

This method takes an optional `commit_position` argument, which can be
used to specify a commit position from which the group of consumers of
the subscription should receive events. Please note, the recorded event
at the specified commit position MAY be included in the recorded events
received by the group of consumers.

If neither `from_end` or `commit_position` are specified, the group of consumers
of the subscription will receive all recorded events.

This method also takes option `filter_exclude`, `filter_include`
arguments, which work in the same way as they do in the `subscribe_all_events()`
method.

This method also takes an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

The method `create_subscription()` does not return a value, because
recorded events are obtained by the group of consumers of the subscription
using the `read_subscription()` method.

*Please note, in this version of this client the "consumer strategy" is
set to "DispatchToSingle". Support for choosing other consumer strategies
supported by EventStoreDB will in future be supported in this client.*

In the example below, a persistent subscription is created.

```python
# Create a persistent subscription.
group_name = f"group-{uuid4()}"
client.create_subscription(group_name=group_name)
```

### Read subscription

The method `read_subscription()` can be used by a group of consumers to receive
recorded events from a persistent subscription created using `create_subscription`.

This method takes a required `group_name` argument, which is
the name of a "group" of consumers of the subscription specified
when `create_subscription()` was called.

This method also takes an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method returns a 2-tuple: a "read request" object and a "read response" object.

```python
read_req, read_resp = client.read_subscription(group_name=group_name)
```

The "read response" object is an iterator that yields recorded events from
the specified commit position.

The "read request" object has an `ack()` method that can be used by a consumer
in a group to acknowledge to the server that it has received and successfully
processed a recorded event. This will prevent that recorded event being received
by another consumer in the same group. The `ack()` method takes an `event_id`
argument, which is the ID of the recorded event that has been received.

The example below iterates over the "read response" object, and calls `ack()`
on the "read response" object. The for loop breaks when we have received
the last event, so that we can continue with the examples below.

```python
events = []
for event in read_resp:
    events.append(event)

    # Acknowledge the received event.
    read_req.ack(event_id=event.id)

    # Break when the last event has been received.
    if event.id == event11.id:
        break
```

The received events are the events we appended above.

```python
assert events[-11].id == event1.id
assert events[-10].id == event2.id
assert events[-9].id == event3.id
assert events[-8].id == event4.id
assert events[-7].id == event5.id
assert events[-6].id == event6.id
assert events[-5].id == event7.id
assert events[-4].id == event8.id
assert events[-3].id == event9.id
assert events[-2].id == event10.id
assert events[-1].id == event11.id
```

The "read request" object also has an `nack()` method that can be used by a consumer
in a group to acknowledge to the server that it has failed successfully to
process a recorded event. This will allow that recorded event to be received
by this or another consumer in the same group.

It might be more useful to encapsulate the request and response objects and to iterate
over the "read response" in a separate thread, to call back to a handler function when
a recorded event is received, and call `ack()` if the handler does not raise an
exception, and to call `nack()` if an exception is raised. The example below shows how
this might be done.

```python
from threading import Thread


class SubscriptionReader:
    def __init__(self, client, group_name, callback):
        self.client = client
        self.group_name = group_name
        self.callback = callback
        self.thread = Thread(target=self.read_subscription, daemon=True)
        self.error = None

    def start(self):
        self.thread.start()

    def join(self):
        self.thread.join()

    def read_subscription(self):
        req, resp = self.client.read_subscription(group_name=self.group_name)
        for event in resp:
            try:
                self.callback(event)
            except Exception as e:
                # req.nack(event.id)  # not yet implemented....
                self.error = e
                break
            else:
                req.ack(event.id)


# Create another persistent subscription.
group_name = f"group-{uuid4()}"
client.create_subscription(group_name=group_name)

events = []

def handle_event(event):
    events.append(event)
    if event.id == event11.id:
        raise Exception()


reader = SubscriptionReader(
    client=client,
    group_name=group_name,
    callback=handle_event
)

reader.start()
reader.join()

assert events[-1].id == event11.id
```

Please note, when processing events in a downstream component, the commit position of
the last successfully processed event is usefully recorded by the downstream component
so that the commit position can be determined by the downstream component from its own
recorded when it is restarted. This commit position can be used to specify the commit
position from which to subscribe. Since this commit position represents the position of
the last successfully processed event in a downstream component, so it will be usual to
want to read from the next event after this position, because that is the next event
that needs to be processed. However, when subscribing for events using a persistent
subscription in EventStoreDB, the event at the specified commit position MAY be returned
as the first event in the received sequence of recorded events, and so it may
be necessary to check the commit position of the received events and to discard
any  recorded event object that has a commit position equal to the commit position
specified in the request.

Whilst there are some advantages of persistent subscriptions, in particular the
processing of recorded events by a group of consumers, by tracking in the
upstream server the position in the commit sequence of events that have been processed,
there is a danger of "dual writing" in the consumption of events. Reliability
in processing of recorded events by a group of consumers will rely instead on
idempotent handling of duplicate messages, and resilience to out-of-order delivery.

### Create stream subscription

The `create_stream_subscription()` method can be used to create a persistent
subscription for a stream.

This method has two required arguments, `group_name` and `stream_name`. The
`group_name` argument names the group of consumers that will receive events
from this subscription. The `stream_name` argument specifies which stream
the subscription will follow. The values of both these arguments are expected
to be Python `str` objects.

This method has an optional `stream_position` argument, which specifies a
stream position from which to subscribe. The recorded event at this stream
position will be received when reading the subscription.

This method has an optional `from_end` argument, which is a Python `bool`.
By default, the value of this argument is False. If this argument is set
to a True value, reading from the subscription will receive only events
recorded after the subscription was created. That is, it is not inclusive
of the current stream position.

This method also takes an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method does not return a value. Events can be received by iterating
over the value returned by calling `read_stream_subscription()` (see below).

The example below creates a persistent stream subscription from the start of the stream.

```python
# Create a persistent stream subscription from start of the stream.
group_name1 = f"group-{uuid4()}"
client.create_stream_subscription(
    group_name=group_name1,
    stream_name=stream_name1,
)
```

The example below creates a persistent stream subscription from a stream position.

```python
# Create a persistent stream subscription from a stream position.
group_name2 = f"group-{uuid4()}"
client.create_stream_subscription(
    group_name=group_name2,
    stream_name=stream_name2,
    stream_position=2
)
```

The example below creates a persistent stream subscription from the end of the stream.

```python
# Create a persistent stream subscription from end of the stream.
group_name3 = f"group-{uuid4()}"
client.create_stream_subscription(
    group_name=group_name3,
    stream_name=stream_name3,
    from_end=True
)
```

### Read stream subscription

The `read_stream_subscription()` method can be used to create a persistent
subscription for a stream.

This method has two required arguments, `group_name` and `stream_name`, which
should match the values of arguments used when calling `create_stream_subscription()`.

This method also takes an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

Just like `read_subscription`, this method returns a 2-tuple: a "read request" object
and a "read response" object.

```python
read_req, read_resp = client.read_stream_subscription(
    group_name=group_name1,
    stream_name=stream_name1,
)
```

The example below iterates over the "read response" object, and calls `ack()`
on the "read response" object. The for loop breaks when we have received
the last event in the stream, so that we can finish the examples in this
documentation.

```python
events = []
for event in read_resp:
    events.append(event)

    # Acknowledge the received event.
    read_req.ack(event_id=event.id)

    # Break when the last event has been received.
    if event.id == event10.id:
        break
```

We can check we received all the events that were appended to `stream_name1`
in the examples above.

```python
assert len(events) == 4
assert events[0].stream_name == stream_name1
assert events[0].id == event1.id
assert events[1].stream_name == stream_name1
assert events[1].id == event2.id
assert events[2].stream_name == stream_name1
assert events[2].id == event3.id
assert events[3].stream_name == stream_name1
assert events[3].id == event10.id
```


## Notes

### Regular expression filters

The filter arguments in `read_all_events()`, `subscribe_all_events()`,
`create_subscription()` and `commit_position()` are applied to the `type`
attribute of recorded events.

The default value of the `filter_exclude` arguments is designed to exclude
EventStoreDB "system" and "persistence subscription config" events, which
otherwise would be included. System events generated by EventStoreDB all
have `type` strings that start with the `$` sign. Persistence subscription
events generated when manipulating persistence subscriptions all have `type`
strings that start with `PersistentConfig`.

For example, to match the type of EventStoreDB system events, use the regular
expression `r'\$.+'`. Please note, the constant `ESDB_SYSTEM_EVENTS_REGEX` is
set to `r'\$.+'`. You can import this value
(`from esdbclient import ESDB_SYSTEM_EVENTS_REGEX`) and use
it when building longer sequences of regular expressions.

Similarly, to match the type of EventStoreDB persistence subscription events, use the
regular expression `r'PersistentConfig\d+'`. The constant `ESDB_PERSISTENT_CONFIG_EVENTS_REGEX`
is set to `r'PersistentConfig\d+'`. You can also import this value
(`from esdbclient import ESDB_PERSISTENT_CONFIG_EVENTS_REGEX`) and use it when building
longer sequences of regular expressions.

The constant `DEFAULT_EXCLUDE_FILTER` is a sequence of regular expressions that match
the events that EventStoreDB generates internally, events that are extraneous to those
which you append using the `append_events()` method.

For example, to exclude system events and persistence subscription configuration events,
and snapshots, you might use the sequence `DEFAULT_EXCLUDE_FILTER + ['.*Snapshot']` as
the value of the `filter_exclude` argument when calling `read_all_events()`,
`subscribe_all_events()`, `create_subscription()` or `get_commit_position()`.

### New event objects

The `NewEvent` class is used when appending events.

The required argument `type` is a Python `str` object, used to indicate the type of
the event that will be recorded.

The required argument `data` is a Python `bytes` object, used to indicate the data of
the event that will be recorded.

The optional argument `metadata` is a Python `bytes` object, used to indicate any
metadata of the event that will be recorded. The default value is an empty `bytes`
object.

The optional argument `content_type` is a Python `str` object, used to indicate the
type of the data that will be recorded for this event. The default value is
`application/json`, which indicates that the `data` was serialised using JSON.
An alternative value for this argument is `application/octet-stream`.

The optional argument `id` is a Python `UUID` object, used to specify the unique ID
of the event that will be recorded. This value will default to a new version-4 UUID.

```python
new_event1 = NewEvent(
    type='OrderCreated',
    data=b'{"name": "Greg"}',
)
assert new_event1.type == 'OrderCreated'
assert new_event1.data == b'{"name": "Greg"}'
assert new_event1.metadata == b''
assert new_event1.content_type == 'application/json'
assert isinstance(new_event1.id, UUID)

event_id = uuid4()
new_event2 = NewEvent(
    type='ImageCreated',
    data=b'01010101010101',
    metadata=b'{"a": 1}',
    content_type='application/octet-stream',
    id=event_id,
)
assert new_event2.type == 'ImageCreated'
assert new_event2.data == b'01010101010101'
assert new_event2.metadata == b'{"a": 1}'
assert new_event2.content_type == 'application/octet-stream'
assert new_event2.id == event_id
```

### Recorded event objects

The `RecordedEvent` class is used when reading events.

The attribute `type` is a Python `str` object, used to indicate the type of event
that was recorded.

The attribute `data` is a Python `bytes` object, used to indicate the data of the
event that was recorded.

The attribute `metadata` is a Python `bytes` object, used to indicate the metadata of
the event that was recorded.

The attribute `content_type` is a Python `str` object, used to indicate the type of
data that was recorded for this event (usually `application/json` to indicate that
this data can be parsed as JSON, but alternatively `application/octet-stream` to
indicate that it is something else).

The attribute `id` is a Python `UUID` object, used to indicate the unique ID of the
event that was recorded. Please note, when recorded events are returned from a call
to `read_stream_events()` in EventStoreDB v21.10, the commit position is not actually
set in the response. This attribute is typed as an optional value (`Optional[UUID]`),
and in the case of using EventStoreDB v21.10 the value of this attribute will be `None`
when reading recorded events from a stream. Recorded events will however have this
values set when reading recorded events from `read_all_events()` and from both
catch-up and persistent subscriptions.

The attribute `stream_name` is a Python `str` object, used to indicate the name of the
stream in which the event was recorded.

The attribute `stream_position` is a Python `int`, used to indicate the position in the
stream at which the event was recorded.

The attribute `commit_position` is a Python `int`, used to indicate the commit position
at which the event was recorded.

```python
from esdbclient.events import RecordedEvent

recorded_event = RecordedEvent(
    type='OrderCreated',
    data=b'{}',
    metadata=b'',
    content_type='application/json',
    id=uuid4(),
    stream_name='stream1',
    stream_position=0,
    commit_position=512,
)
```

## Contributors

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
project's package dependencies according to the project's `poetry.lock` file.

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
