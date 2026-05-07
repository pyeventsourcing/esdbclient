---
order: 4
---

# Reading Events

This guide describes the Python client methods for reading events from KurrentDB.

## Introduction

Recorded events can be read from a [named stream](#read-stream), from the
[global transaction log](#read-all), and from [secondary indexes](#read-index).

The Python clients for KurrentDB have four methods for reading events:

* [`get_stream()`](#get-stream) – returns a Python `tuple` of events from a named stream
* [`read_stream()`](#read-stream) – returns a streaming iterable of events from a named stream
* [`read_all()`](#read-all) – returns a streaming iterable of events from global transaction log
* [`read_index()`](#read-index) – returns a streaming iterable of events from a secondary index


## Recorded Events

The Python client for KurrentDB uses the `RecordedEvent` class when presenting recorded events.

A `RecordedEvent` object specifies the type string, binary data, metadata, content type, and ID of a [new event](./appending-events.md#new-events) that has been recorded.

Additionally, it specifies the event's stream name and stream position, the commit and prepare position, the recorded time, and possibly a link event and a persistent subscription consumer group retry count.

| Field              | Type                  | Description                                                                           |
|--------------------|-----------------------|---------------------------------------------------------------------------------------|
| `type`             | `str`                 | The type of the event                                                                 |
| `data`             | `bytes`               | The content of the event                                                              |
| `metadata`         | `bytes`               | Event metadata                                                                        |
| `content_type`     | `str`                 | The format of the content                                                             |
| `id`               | `UUID`                | A unique ID for the event                                                             |
| `stream_name`      | `str`                 | A unique ID for the event                                                             |
| `stream_position`  | `int`                 | Position of the event in the stream                                                   |
| `commit_position`  | `int`                 | Position of the event in the global transaction log                                   |
| `recorded_at`      | `datetime\|None`      | Timestamp added by KurrentDB                                                          |
| `link`             | `RecordedEvent\|None` | Resolved link event                                                                   |
| `retry_count`      | `int\|None`           | Number of times this event has been sent to a persistence subscription consumer group |

You will never need to construct a `RecordedEvent` object. However, all events returned from KurrentDB by the Python clients are
presented as `RecordedEvent` objects, and so it is important to understand these fields.

## Get Stream

Use the `get_stream()` method to get a `tuple` of events from a stream in KurrentDB.

You can get all the events or a sample of the events from an individual stream,
starting from any position in the stream, either forwards or backwards.

This is a convenient alternative to [`read_stream()`](#read-stream) that returns a Python `tuple` collection.

| Parameter         | Description                                                                                                                          | Default  |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------|----------|
| `stream_name`     | Stream from which events will be read.                                                                                               |          |
| `stream_position` | Position from which to start reading events.                                                                                         | `None`   |
| `backwards`       | Activate reading of events in reverse order.                                                                                         | `False`  |
| `resolve_links`   | Activate resolution of "link events".                                                                                                | `False`  |
| `limit`           | Maximum number of events to return.                                                                                                  | `None`   |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                         | `None`   |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

On success, `get_stream()` returns a `tuple` of [`RecordedEvent`](#recorded-events) objects.

### Examples

Let's set up the examples by [connecting to KurrentDB](./getting-started.md#connecting-to-kurrentdb) and [appending new events](./appending-events.md).

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import KurrentDBClient, NewEvent, StreamState

# Connect to KurrentDB
uri = "kurrentdb://127.0.0.1:2113?tls=false"
client = KurrentDBClient(uri)

# Construct new event objects
event1 = NewEvent(
    type="OrderCreated",
    data=b'{"order_id": "order-123"}',
)
event2 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "processing"}',
)
event3 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "shipped"}',
)

# Append the events to a new stream
commit_position = client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event1, event2, event3],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import AsyncKurrentDBClient, NewEvent, StreamState

# Connect to KurrentDB
uri = "kurrentdb://127.0.0.1:2113?tls=false"
client = AsyncKurrentDBClient(uri)

# Construct new event objects
event1 = NewEvent(
    type="OrderCreated",
    data=b'{"order_id": "order-123"}',
)
event2 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "processing"}',
)
event3 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "shipped"}',
)

# Append the events to a new stream
commit_position = await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event1, event2, event3],
)
```
:::

### Reading Forwards

The simplest way to get stream events is to supply a `stream_name` argument.
This is a typical operation when retrieving events to construct a decision model
in a command handler.

Here's an example of getting all events from stream `"order-123"`.

::: tabs
@tab sync
```python:no-line-numbers
for event in client.get_stream(stream_name="order-123"):
    print(f"Event: {event.type} at position {event.stream_position}")
```
@tab async
```python:no-line-numbers
for event in await client.get_stream(stream_name="order-123"):
    print(f"Event: {event.type} at position {event.stream_position}")
```
:::

### Reading Backwards

Set the `backwards` parameter to `True` to get stream events in reverse order.

::: tabs
@tab sync
```python:no-line-numbers
# Get all events backwards from the end
for event in client.get_stream(
    stream_name="order-123",
    backwards=True,
):
    assert event.stream_position == 2
    break
```
@tab async
```python:no-line-numbers
# Get all events backwards from the end
for event in await client.get_stream(
    stream_name="order-123",
    backwards=True,
):
    assert event.stream_position == 2
    break
```
:::

:::tip
Get stream event backwards with a limit of `1` to find the last position in the stream.

Alternatively, call the more convenient method `get_current_version()`.
:::


### Limited Number

Passing in a `limit` argument allows you to restrict the number of events that are returned.

In the example below, we read a maximum of two events from the stream:

::: tabs
@tab sync
```python:no-line-numbers
events = client.get_stream(
    stream_name="order-123",
    limit=2
)

assert len(events) == 2
```
@tab async
```python:no-line-numbers
events = await client.get_stream(
    stream_name="order-123",
    limit=2
)

assert len(events) == 2
```
:::


### From Stream Position

Specifying a `stream_position` argument will get events from a specific position. This is
useful, for example, when advancing a snapshot of an aggregate to the latest current state.

Getting stream events from a specific position is inclusive, which means
the event at that position will be returned by the response.

::: tabs
@tab sync
```python:no-line-numbers
# Get events from a specific stream position
for event in client.get_stream(
    stream_name="order-123",
    stream_position=1,
):
    assert event.stream_position == 1
    break
```
@tab async
```python:no-line-numbers
# Get events from a specific stream position
for event in await client.get_stream(
    stream_name="order-123",
    stream_position=1,
):
    assert event.stream_position == 1
    break
```
:::

### Resolving Link Events

KurrentDB projections can create "link events" that are pointers to events you have appended to a stream.

Set `resolve_links=True` so that KurrentDB will resolve the "link events" and return the linked events.

::: tabs
@tab sync
```python:no-line-numbers
for event in client.get_stream(
    stream_name="$et-OrderCreated",
    resolve_links=True
):
    assert event.type == "OrderCreated"
```
@tab async
```python:no-line-numbers
for event in await client.get_stream(
    stream_name="$et-OrderCreated",
    resolve_links=True
):
    assert event.type == "OrderCreated"
```
:::


### Not Found Error

Reading a stream that doesn't exist will raise a `NotFoundError` exception.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    client.get_stream(stream_name="not-a-stream")
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    await client.get_stream(stream_name="not-a-stream")
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
:::


## Read Stream

Use the `read_stream()` method to read events from a stream in KurrentDB.

You can read all the events or a sample of the events from an individual stream,
starting from any position in the stream, and can read either forwards or
backwards.

Alternatively, use [`get_stream()`](#get-stream) to get a Python `tuple` collection
of events, rather then a streaming iterable response.

| Parameter         | Description                                                                              | Default |
|-------------------|------------------------------------------------------------------------------------------|---------|
| `stream_name`     | Stream from which events will be read.                                                   |         |
| `stream_position` | Position from which to start reading events.                                             | `None`  |
| `backwards`       | Activate reading of events in reverse order.                                             | `False` |
| `resolve_links`   | Activate resolution of "link events".                                                    | `False` |
| `limit`           | Maximum number of events to return.                                                      | `None`  |
| `timeout`         | Maximum duration of operation (in seconds).                                             | `None`  |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, `read_stream()` returns an iterable of `RecordedEvent` objects.

Please note, a `NotFoundError` exception will be raised if the stream does not exist.

### Reading Forwards

The simplest way to read a stream is to supply a `stream_name` argument and read
every event already recorded in that stream. This is a typical operation when retrieving
events to construct a decision model in a command handler.

::: tabs
@tab sync
```python:no-line-numbers
with client.read_stream(stream_name="order-123") as events:
    for event in events:
        print(f"Event: {event.type} at position {event.stream_position}")
```
@tab async
```python:no-line-numbers
async with await client.read_stream(stream_name="order-123") as events:
    async for event in events:
        print(f"Event: {event.type} at position {event.stream_position}")
```
:::

### Reading Backwards

Set `backwards=True` to read stream events in reverse order.

::: tabs
@tab sync
```python:no-line-numbers
# Read all events backwards from the end
with client.read_stream(
    stream_name="order-123",
    backwards=True,
) as events:
    for event in events:
        assert event.stream_position == 2
        break
```
@tab async
```python:no-line-numbers
# Read all events backwards from the end
async with await client.read_stream(
    stream_name="order-123",
    backwards=True,
) as events:
    async for event in events:
        assert event.stream_position == 2
        break
```
:::

:::tip
Read backwards with a limit of `1` to find the last position in the stream.

Alternatively, call the convenience Python client method `get_current_version()`.
:::

### Limited Number

Passing in a `limit` argument allows you to restrict the number of events that are returned.

::: tabs
@tab sync
```python:no-line-numbers
with client.read_stream(
    stream_name="order-123",
    limit=2
) as events:
    assert len(tuple(events)) == 2
```
@tab async
```python:no-line-numbers
async with await client.read_stream(
    stream_name="order-123",
    limit=2
) as events:
    assert len([e async for e in events]) == 2
```
:::


### From Stream Position

Specifying a `stream_position` argument will start reading from a specific position in the stream. This is
useful, for example, when advancing a snapshot of an aggregate to the latest current state.

::: tabs
@tab sync
```python:no-line-numbers
# Read from a specific stream position
with client.read_stream(
    stream_name="order-123",
    stream_position=1,
) as events:
    for event in events:
        assert event.stream_position == 1
        break
```
@tab async
```python:no-line-numbers
# Read from a specific stream position
async with await client.read_stream(
    stream_name="order-123",
    stream_position=1,
) as events:
    async for event in events:
        assert event.stream_position == 1
        break
```
:::

Please note, reading a stream from a specific position is inclusive, which means
the event at that position will be returned by the response.

### Resolving Link Events

KurrentDB projections can create "link events" that are pointers to events you have appended to a stream.

Set `resolve_links=True` so that KurrentDB will resolve the "link events" and return the linked events.

::: tabs
@tab sync
```python:no-line-numbers
with client.read_stream(
    stream_name="$et-OrderCreated",
    resolve_links=True
) as events:
    for event in events:
        assert event.type == "OrderCreated"
```
@tab async
```python:no-line-numbers
async with await client.read_stream(
    stream_name="$et-OrderCreated",
    resolve_links=True
) as events:
    async for event in events:
        assert event.type == "OrderCreated"
```
:::


### Not Found Error

Reading a stream that doesn't exist will raise a `NotFoundError` exception.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    with client.read_stream(
        stream_name="not-a-stream"
    ) as events:
        ...
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    async with await client.read_stream(
        stream_name="not-a-stream"
    ) as events:
        ...
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
:::


## Read All

Use the `read_all()` method to read events from the global transaction log.

No arguments are required when reading from the global transaction log.
You can start from a particular commit position, read events backwards, and read
a limited number of events. You can also filter events by type string or stream name.

| Parameter               | Description                                                                                                                                              | Default       |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `commit_position`       | Position from which to start reading events.                                                                                                             | `None`        |
| `backwards`             | Activate reading of events in reverse order.                                                                                                             | `False`       |
| `resolve_links`         | Activate resolution of "link events".                                                                                                                    | `False`       |
| `filter_exclude`        | [Patterns](./reading-events.md#server-side-filtering) for excluding events.                                                                                                                           | System events |
| `filter_include`        | [Patterns](./reading-events.md#server-side-filtering) for including events (if set, only matching events will be returned).                                                                           | `()`          |
| `filter_by_stream_name` | Filter by stream name rather than event type.                                                                                                            | `False`       |
| `limit`                 | Maximum number of events to return.                                                                                                                      | `None`        |
| `timeout`               | Maximum duration of operation (in seconds).                                                                                                              | `None`        |
| `credentials`           | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`        |

On success, `read_all()` returns an iterable of `RecordedEvent` objects.

### Server-Side Filtering

KurrentDB supports server-side filtering of events while reading from, or subscribing
to, the global transaction log, so that you can receive only the events you care about.
You can filter by event type or stream name using regular expressions.

The `filter_include` and `filter_exclude` parameters are designed to have exactly
the opposite effect from each other, so that a sequence of strings given to
`filter_include` will return exactly those events which would be excluded if
the same argument value were used with `filter_exclude`. And vice versa, so that
a sequence of strings given to `filter_exclude` will return exactly those events
that would not be included if the same argument value were used with `filter_include`.

The `filter_include` parameter takes precedence over `filter_exclude`. That is to say,
if you pass arguments for both, the `filter_exclude` argument will be ignored.

The `filter_include` and `filter_exclude` parameters are typed as `Sequence[str]`
which means that you can either pass a single `str`, or a collection of `str`.
The `str` value or values should be unanchored regular expression patterns. If you supply
a collection of `str`, they will be concatenated together by the Python client as bracketed
alternatives in a larger regular expression that is anchored to the start and end
of the strings being matched. So there is no need to include the `'^'` and `'$'`
anchor assertions.

KurrentDB generates "system events" that all have a `type` that begins with `"$"`.
By default, system events are excluded, along with `PersistentConfig` and `Result` events.
If you want to also exclude other types of events, then use an argument for `filter_exclude`
that adds to the default argument value `DEFAULT_EXCLUDE_FILTER`. If you especially want
to include system events, then you can override the default filter by passing an empty
sequence as the `filter_exclude` argument. If you want to select only for system events,
then specify a suitable `filter_include` argument.

You should use wildcards if you want to match substrings. For example, `"Order.*"` matches
all strings that start with `"Order"`. Alternatively,`".*Snapshot"` matches all strings
that end with `"Snapshot"`.

Characters that are metacharacters with special meaning in regular expressions,
such as `.` `*` `+` `?` `^` `$` `|` `(` `)` `[` `]` `{` `}` `\` must be escaped to be used
literally when matching event types and stream names. Python's raw string literals can help
to avoid doubling of escape backslashes. For example `r"\$.*"` can be used to match system
event types that all start with the `$` character.

### Reading Forwards

The simplest way to read events from the global transaction log is to call `read_all()` without arguments.

::: tabs
@tab sync
```python:no-line-numbers
# Read all events from the beginning
with client.read_all() as events:
    # Iterate through the sync streaming response with a 'for' loop
    for event in events:
        print(f"Event: {event.type} from stream {event.stream_name}")
```
@tab async
```python:no-line-numbers
# Read all events from the beginning
async with await client.read_all() as events:
    # Iterate through the async streaming response with an 'async for' loop
    async for event in events:
        print(f"Event: {event.type} from stream {event.stream_name}")
```
:::

### Reading Backwards

Set `backwards=True` to read the global transaction log backwards from the end.

::: tabs
@tab sync
```python:no-line-numbers
# Read all events backwards from the end
with client.read_all(backwards=True) as events:
    ...

# Read backwards from a specific commit position
with client.read_all(
    commit_position=commit_position,
    backwards=True,
) as events:
    ...
```
@tab async
```python:no-line-numbers
# Read all events backwards from the end
async with await client.read_all(backwards=True) as events:
    ...

# Read backwards from a specific commit position
async with await client.read_all(
    commit_position=commit_position,
    backwards=True,
) as events:
    ...
```
:::

:::tip
Read one event backwards to find the last position in the global transaction log.

Alternatively, call the more convenient Python client method `get_commit_position()`.
:::

### Limited Number

Passing in a `limit` allows you to restrict the number of events that are returned.

In the example below, we read a maximum of 100 events:

::: tabs
@tab sync
```python:no-line-numbers
with client.read_all(
    limit=100
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_all(
    limit=100
) as events:
    ...
```
:::

### From Commit Position

You can also start reading from a specific position in the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
# Read from a specific commit position
with client.read_all(
    commit_position=commit_position
) as events:
    ...
```
@tab async
```python:no-line-numbers
# Read from a specific commit position
async with await client.read_all(
    commit_position=commit_position
) as events:
    ...
```
:::

Please note, an `InvalidCommitPositionError` exception will be raised
if the commit position does not exist.


### Resolving Link Events

KurrentDB projections can create "link events" that are pointers to events you have appended to a stream.

Set `resolve_links=True` so that KurrentDB will resolve the "link events" and return the linked events.

::: tabs
@tab sync
```python:no-line-numbers
with client.read_all(
    resolve_links=True
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_all(
    resolve_links=True
) as events:
    ...
```
:::


### Filtering Examples

You can read more selectively from the global transaction log with [server-side filtering](#server-side-filtering) by supplying an argument for either the `filter_include` or the `filter_exclude` parameters.

By default, events will be filtered by `type`. Alternatively, you can filter events by `stream_name`
name by setting the `filter_by_stream_name` parameter to `True`.

Here's an example that reads all events that have a `type` starting with `"Order"`:

::: tabs
@tab sync
```python:no-line-numbers
with client.read_all(
    filter_include=["Order.*"]
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_all(
    filter_include=["Order.*"]
) as events:
    ...
```
:::

Here's an example that selects all events that do not have a `type` starting with `"Order"`:

::: tabs
@tab sync
```python:no-line-numbers
with client.read_all(
    filter_exclude=["Order.*"]
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_all(
    filter_exclude=["Order.*"]
) as events:
    ...
```
:::

Here's an example that selects all events that have a `stream_name` starting with `"order"`:

::: tabs
@tab sync
```python:no-line-numbers
with client.read_all(
    filter_include=["order.*"],
    filter_by_stream_name=True,
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_all(
    filter_include=["order.*"],
    filter_by_stream_name=True,
) as events:
    ...
```
:::

Here's an example that selects all events that do not have a `stream_name` starting with `"order"`:

::: tabs
@tab sync
```python:no-line-numbers
with client.read_all(
    filter_exclude=["order.*"],
    filter_by_stream_name=True,
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_all(
    filter_exclude=["order.*"],
    filter_by_stream_name=True,
) as events:
    ...
```
:::


## Read Index

::: info
Supported by KurrentDB 25.1 and later.
:::

Use the `read_index()` method to read events from a secondary index in KurrentDB.

You can read events from a secondary index starting from any commit position.

| Parameter         | Description                                                                                                                                              | Default |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `index_name`      | Name of secondary index (`"$idx-"` prefix is optional).                                                                                                  |         |
| `commit_position` | Position from which to start reading events.                                                                                                             | `None`  |
| `limit`           | Maximum number of events to return.                                                                                                                      | `None`  |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, `read_index()` returns an iterable of `RecordedEvent` objects.

### Reading Forwards

The simplest way to read from a secondary index is to call `read_index()` with the name of an index.

Here's an example of reading all events with type string `"OrderCreated"`.

::: tabs
@tab sync
```python:no-line-numbers
# Read all OrderCreated events
with client.read_index(
    index_name="et-OrderCreated"
) as events:

    # Iterate through the sync streaming response with a 'for' loop
    for event in events:
        print(f"Event: {event.type} from stream {event.stream_name}")
```
@tab async
```python:no-line-numbers
# Read all OrderCreated events
async with await client.read_index(
    index_name="et-OrderCreated"
) as events:

    # Iterate through the async streaming response with an 'async for' loop
    async for event in events:
        print(f"Event: {event.type} from stream {event.stream_name}")
```
:::


### From Commit Position

You can also start reading a secondary index from a specific position in the global transaction log.

Here's an example of reading a secondary index from a specific commit position.

::: tabs
@tab sync
```python:no-line-numbers
# Read from a specific commit position
with client.read_index(
    index_name="et-OrderCreated",
    commit_position=commit_position,
) as events:
    for event in events:
        break
```
@tab async
```python:no-line-numbers
# Read from a specific commit position
async with await client.read_index(
    index_name="et-OrderCreated",
    commit_position=commit_position,
) as events:
    async for event in events:
        break
```
:::

Please note, an `InvalidCommitPositionError` exception will be raised
if the commit position does not exist.


### Limited Number

Passing in a `limit` allows you to restrict the number of events that are returned.

Here's an example of reading a maximum of 100 events from a secondary index.

::: tabs
@tab sync
```python:no-line-numbers
with client.read_index(
    index_name="et-OrderCreated",
    limit=100,
) as events:
    ...
```
@tab async
```python:no-line-numbers
async with await client.read_index(
    index_name="et-OrderCreated",
    limit=100,
) as events:
    ...
```
:::
