---
order: 3
---

# Reading Events

KurrentDB provides two primary methods for reading events: reading from a
named stream, and reading from all events across the entire event store.

Events in KurrentDB are organized within individual streams and use two
distinct positioning systems to track their location. The **stream position** is
an integer that represents the sequential position of an
event within its specific stream. Events are numbered starting from 0, with each
new event receiving the next sequential position (0, 1, 2, 3...). The
**commit position** represents the event's location in KurrentDB's global
transaction log and is a single integer value that indicates where the event
was committed in the log.

These positioning identifiers are essential for reading operations, as they
allow you to specify exactly where to start reading from within a stream or
across the entire event store.

There are two client methods for reading events from KurrentDB.

* [`read_stream()`](#read-stream)
* [`read_all()`](#read-all)


## Read stream

You can use the `read_stream()` method to read events from a stream in KurrentDB.

### Description

You can read all the events or a sample of the events from an individual stream,
starting from any position in the stream, and can read either forward or
backward.

### Parameters

| Name              | Type                   | Required | Default | Description                                                    |
|-------------------|------------------------|----------|---------|----------------------------------------------------------------|
| `stream_name`     | `str`                  | Yes      |         | Stream from which events will be read.                         |
| `stream_position` | `int \| None`          | No       | `None`  | Position from which to start reading events.                   |
| `backwards`       | `bool`                 | No       | `False` | Activate reading of events in reverse order.                   |                                                                               
| `resolve_links`   | `bool`                 | No       | `False` | Activate resolution of "link events".                          |                                                                               
| `limit`           | `int \| None`          | No       | `None`  | Maximum number of events to return.                            |                                                                         
| `timeout`         | `float`                | No       | `None`  | Maximum duration, in seconds, for completion of the operation. |                                                                         
| `credentials`     | `CallCredentials` | No       | `None`  | Override credentials derived from the connection string.       |                                                                         

### Return value

On success, `read_stream()` returns an iterable of `RecordedEvent` objects.

::: tip
The Python client method `get_stream()` was added as a convenient method that
evaluates the iterable response from `read_stream()` as a Python `tuple` of `RecordedEvent` objects.

You can use the `get_stream()` method for simple cases or `read_stream()` for streaming large
result sets.
:::

### Examples

The examples below read events from the stream `"order:123"`.

Let's begin by connecting to KurrentDB and appending some events.

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
    data=b'{"order_id": "order:123"}',
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
commit_position3 = client.append_to_stream(
    stream_name="order:123",
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
await client.connect()

# Construct new event objects
event1 = NewEvent(
    type="OrderCreated",
    data=b'{"order_id": "order:123"}',
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
commit_position3 = await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.NO_STREAM,
    events=[event1, event2, event3],
)
```
:::

That gives three events recorded in the stream `"order:123"` that we can read.

#### Reading forwards

The simplest way to read a stream is use supply a `stream_name` argument, for example `"order:123"`,
and read every event already recorded in that stream. This is a typical operation when retrieving
events to construct a decision model, such as an event-sourced aggregate.

Remember `get_stream()` returns a `tuple` whereas `read_stream()` returns a more basic iterable.

::: tabs
@tab sync
```python:no-line-numbers
# Get all events from a stream
events = client.get_stream(stream_name="order:123")

# Iterate through a tuple of events with a 'for' loop
for event in events:
    print(f"Event: {event.type} at position {event.stream_position}")
    
# Or use read_stream() for streaming
events = client.read_stream(stream_name="order:123")

# Iterate through the sync streaming response with a 'for' loop
for event in events:
    print(f"Event: {event.type} at position {event.stream_position}")

```
@tab async
```python:no-line-numbers
# Get all events from a stream
events = await client.get_stream(stream_name="order:123")

# Iterate through a tuple of events with a 'for' loop
for event in events:
    print(f"Event: {event.type} at position {event.stream_position}")
    
# Or use read_stream() for streaming
events = await client.read_stream(stream_name="order:123")

# Iterate through the async streaming response with an 'async for' loop
async for event in events:
    print(f"Event: {event.type} at position {event.stream_position}")
```
:::

::: tip
The `read_stream()` method of the sync client returns a Python `Iterable`.
However, the async client returns an `AsyncIterable`
so you must use an `async for` loop instead.
:::

#### Checking if the stream exists

Reading a stream that doesn't exist will raise a `NotFoundError` exception.
It is important to handle this error when attempting to read a stream that
may not exist.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    events = client.get_stream(stream_name="not-a-stream")
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    events = await client.get_stream(stream_name="not-a-stream")        
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
:::

#### Reading from a stream position

Use `stream_position` to start reading from a specific position in the stream. This is
useful for example when advancing a snapshot of an aggregate to the latest current state.

The example below reads from stream position `2`.

::: tabs
@tab sync
```python:no-line-numbers
# Read from a specific stream position
events = client.read_stream(
    stream_name="order:123", 
    stream_position=2,
)
```
@tab async
```python:no-line-numbers
# Read from a specific stream position
events = await client.read_stream(
    stream_name="order:123", 
    stream_position=1,
)
```
:::

::: tip
Without setting `stream_position`, by default the Python clients will from the first stream position
when reading forwards, and from the last stream position when reading backwards.
:::


#### Reading backwards

Set the `backwards` parameter to `True` to read events backwards from a stream:

::: tabs
@tab sync
```python:no-line-numbers
# Read all events backwards from the end
events = client.read_stream(
    stream_name="order:123",
    backwards=True,
)

# Read backwards from a specific position
events = client.read_stream(
    stream_name="order:123",
    stream_position=2,
    backwards=True,
)
```
@tab async
```python:no-line-numbers
# Read all events backwards from the end
events = await client.read_stream(
    stream_name="order:123",
    backwards=True,
)

# Read backwards from a specific position
events = await client.read_stream(
    stream_name="order:123",
    stream_position=2,
    backwards=True,
)
```
:::

:::tip
Read backwards with a limit of `1` to find the last position in the stream.

Alternatively, call the convenience method `get_stream_position()`.
:::

#### Resolving link events

KurrentDB projections can create "link events" that are pointers to events you have appended to a stream.

Set `resolve_links` to `True` so that KurrentDB will resolve the "link event" and return the linked event.

::: tabs
@tab sync
```python:no-line-numbers
events = client.get_stream(
    stream_name="order:123",
    resolve_links=True
)
```
@tab async
```python:no-line-numbers
events = await client.get_stream(
    stream_name="order:123",
    resolve_links=True
)
```
:::

::: tip
You can tell if you are receiving "link events" because the `type` attribute start with `"$>"`.
:::

#### Reading a limited number of events

Passing in a `limit` argument allows you to restrict the number of events that are returned.

In the example below, we read a maximum of two events from the stream:

::: tabs
@tab sync
```python:no-line-numbers
events = client.get_stream(
    stream_name="order:123",
    limit=2
)
```
@tab async
```python:no-line-numbers
events = await client.get_stream(
    stream_name="order:123",
    limit=2
)
```
:::

#### User credentials

You can use the `credentials` parameter of `read_stream()` and `get_stream()` to override the credentials given with the [user
info](./getting-started.md#user-info-string) part of a client connection string. The helper method
`construct_call_credentials()` constructs a `grpc.CallCredentials` object from a username and password.

::: tabs
@tab sync
```python:no-line-numbers
# Construct call credentials
credentials = client.construct_call_credentials(
    username="user101", 
    password="my-middle-name", # :-)
)

# Use credentials for reading
events = client.get_stream(
    stream_name="order:123",
    credentials=credentials
)
```
@tab async
```python:no-line-numbers
# Construct call credentials
credentials = client.construct_call_credentials(
    username="user101", 
    password="my-middle-name", # :-)
)

# Use credentials for reading
events = await client.get_stream(
    stream_name="order:123",
    credentials=credentials
)
```
:::


## Read all

You can use the `read_all()` method to read from all events across the entire event store.

### Description

Reading from all events is similar to reading from an individual stream, but
please note there are differences.

You need to provide a commit position instead of a stream position when reading
from all events.

When connecting to a "secure" server, you need to use user account credentials
that have sufficient permissions to read from all events.

You can also provide arguments for filtering events, either to include or exclude
events, by either event type (the default) or by stream name.

### Parameters

| Name                    | Type               | Required | Default | Description                                                                |
|-------------------------|--------------------|----------|---------|----------------------------------------------------------------------------|
| `commit_position`       | `int \| None`      | No       | `None`  | Position from which to start reading events.                               |
| `backwards`             | `bool`             | No       | `False` | Activate reading of events in reverse order.                               |
| `resolve_links`         | `bool`             | No       | `False` | Activate resolution of "link events".                                      |
| `filter_exclude`        | `Sequence[str]`    | No       |         | Exclude matching events (system generated events are excluded by default). |
| `filter_include`        | `Sequence[str]`    | No       | `()`    | Include matching events (if set, only matching events will be returned).   |
| `filter_by_stream_name` | `bool`             | No       | `False` | Filter by stream name (default is to filter by event type).                |
| `limit`                 | `int \| None`      | No       | `None`  | Maximum number of events to return.                                        |
| `timeout`               | `float`            | No       | `None`  | Maximum duration, in seconds, for completion of the operation.             |
| `credentials`           | `CallCredentials`  | No       | `None`  | Override credentials derived from the connection string.                   |

### Return value

On success, `read_all()` returns an iterable of `RecordedEvent` objects.

### Examples

#### Reading forwards

The simplest way to read all events forwards is to use the `read_all()` method.

::: tabs
@tab sync
```python:no-line-numbers
# Read all events from the beginning
events = client.read_all()

# Iterate through the sync streaming response with a 'for' loop
for event in events:
    print(f"Event: {event.type} from stream {event.stream_name}")
```
@tab async
```python:no-line-numbers
# Read all events from the beginning
events = await client.read_all()

# Iterate through the async streaming response with an 'async for' loop
async for event in events:
    print(f"Event: {event.type} from stream {event.stream_name}")
```
:::

::: tip
The `read_all()` method of the sync client returns a Python `Iterable`.
However, the async client returns an `AsyncIterable`
so you must use an `async for` loop instead.
:::



#### Reading from a commit position

You can also start reading from a specific position in the global log:

::: tabs
@tab sync
```python:no-line-numbers
# Read from a specific commit position
events = client.read_all(
    commit_position=commit_position3,
)
```
@tab async
```python:no-line-numbers
# Read from a specific commit position
events = await client.read_all(
    commit_position=commit_position3,
)
```
:::

### Reading backwards

In addition to reading all events forwards, you can read them backwards. To
read all events backwards, set the `backwards` parameter to `True`:

::: tabs
@tab sync
```python:no-line-numbers
# Read all events backwards from the end
events = client.read_all(backwards=True)

# Read backwards from a specific commit position
events = client.read_all(
    commit_position=commit_position3,
    backwards=True,
)
```
@tab async
```python:no-line-numbers
# Read all events backwards from the end
events = await client.read_all(backwards=True)

# Read backwards from a specific commit position
events = await client.read_all(
    commit_position=commit_position3,
    backwards=True,
)
```
:::

:::tip
Read one event backwards to find the last position in the global log.

Alternatively, call the convenience method `get_commit_position()`.
:::

#### Resolving link events

KurrentDB projections can create "link events" that are pointers to events you have appended to a stream.

Set `resolve_links` to `True` so that KurrentDB will resolve the "link event" and return the linked event.

::: tabs
@tab sync
```python:no-line-numbers
events = client.read_all(resolve_links=True)
```
@tab async
```python:no-line-numbers
events = await client.read_all(resolve_links=True)
```
:::

#### Reading a limited number of events

Passing in a `limit` allows you to restrict the number of events that are returned.

In the example below, we read a maximum of 100 events:

::: tabs
@tab sync
```python:no-line-numbers
events = client.read_all(
    limit=100,
)
```
@tab async
```python:no-line-numbers
events = await client.read_all(
    limit=100,
)
```
:::

#### User credentials

You can use the `credentials` parameter of `read_all()` to override the credentials given with the [user
info](./getting-started.md#user-info-string) part of a client connection string. The helper method
`construct_call_credentials()` constructs a `grpc.CallCredentials` object from a username and password.

::: tabs
@tab sync
```python:no-line-numbers
# Construct call credentials  
credentials = client.construct_call_credentials(
    username="user101", 
    password="my-middle-name", # :-)
)

# Use credentials for reading
events = client.read_all(credentials=credentials)
```
@tab async
```python:no-line-numbers
# Construct call credentials  
credentials = client.construct_call_credentials(
    username="user101", 
    password="my-middle-name", # :-)
)

# Use credentials for reading
events = await client.read_all(credentials=credentials)
```
:::

### Filtering events by type and stream name

You can read more selectively by supplying an argument for either the `filter_include` or the `filter_exclude` parameters.

By default, events will be filtered by `type`. Alternatively, you can filter events by `stream_name`
name by setting the `filter_by_stream_name` parameter to `True`.

The example below selects all events that have a `type` starting with `"Order"`:

::: tabs
@tab sync
```python:no-line-numbers
events = client.read_all(
    filter_include="Order.*",
)
```
@tab async
```python:no-line-numbers
events = await client.read_all(
    filter_include="Order.*",
)
```
:::

The example below selects all events that do not have a `type` starting with `"Order"`:

::: tabs
@tab sync
```python:no-line-numbers
events = client.read_all(
    filter_exclude="Order.*",
)
```
@tab async
```python:no-line-numbers
events = await client.read_all(
    filter_exclude="Order.*",
)
```
:::

The example below selects all events that have a `stream_name` starting with `"order"`:

::: tabs
@tab sync
```python:no-line-numbers
events = client.read_all(
    filter_include="order.*",
    filter_by_stream_name=True,
)
```
@tab async
```python:no-line-numbers
events = await client.read_all(
    filter_include="order.*",
    filter_by_stream_name=True,
)
```
:::

The example below selects all events that do not have a `stream_name` starting with `"order"`:

::: tabs
@tab sync
```python:no-line-numbers
events = client.read_all(
    filter_exclude="order.*",
    filter_by_stream_name=True,
)
```
@tab async
```python:no-line-numbers
events = await client.read_all(
    filter_exclude="order.*",
    filter_by_stream_name=True,
)
```
:::

::: tip
The `filter_include` and `filter_exclude` parameters are designed to have exactly
the opposite effect from each other, so that a sequence of strings given to
`filter_include` will return exactly those events which would be excluded if
the same argument value were used with `filter_exclude`. And vice versa, so that
a sequence of strings given to `filter_exclude` will return exactly those events
that would not be included if the same argument value were used with `filter_include`.
:::

::: tip
The `filter_include` parameter takes precedence over `filter_exclude`. That is to say,
if you pass arguments for both, the `filter_exclude` argument will be ignored.
:::

::: tip
The `filter_include` and `filter_exclude` arguments are typed as `Sequence[str]`
which means that you can either pass a single `str`, or a collection of `str`.
:::

::: tip
The `str` values should be unanchored regular expression patterns. If you supply
a collection of `str`, they will be concatenated together by the Python client as bracketed
alternatives in a larger regular expression that is anchored to the start and end
of the strings being matched. So there is no need to include the `'^'` and `'$'`
anchor assertions.
:::

::: tip
Characters that are metacharacters with special meaning in regular expressions,
such as `.` `*` `+` `?` `^` `$` `|` `(` `)` `[` `]` `{` `}` `\` must be escaped to be used
literally when matching event types and stream names. Python's raw string literals can help
to avoid doubling of escape backslashes. For example `r"\$.*"` can be used to match system
event types.
:::

::: tip
You should use wildcards if you want to match substrings. For example, `"Order.*"` matches
all strings that start with `"Order"`. Alternatively,`".*Snapshot"` matches all strings
that end with `"Snapshot"`.
:::

::: tip
KurrentDB generates "system events" that all have a `type` that begins with `"$"`.
They are filtered out by default, along with `PersistentConfig` and `Result` events.
If you want to read all events excluding custom events whilst also excluding the default
events, then use an argument for `filter_exclude` that adds to the default value
DEFAULT_EXCLUDE_FILTER. If you especially want to read system events, then you can
override the default filter by passing an empty sequence as the `filter_exclude`
argument, or by selecting for them with a value of `filter_include`.
:::



