---
order: 2
---

# Appending events

KurrentDB is an append-only event store database. The [Getting started](./getting-started.md#writing-to-kurrentdb) page 
introduced the topic of writing to KurrentDB.

There are two client methods for writing events to KurrentDB.

* [`append_to_stream()`](#append-to-stream)
* [`multi_append_to_stream()`](#multi-append-to-stream)

Let's explore what KurrentDB can do in more detail.

::: info Requires leader
If you are using a KurrentDB cluster, please note, events can be appended only to leader nodes.
:::


## Append to stream

Events in KurrentDB are organized in "streams". You can use the `append_to_stream()` method to append
new events to a stream in KurrentDB. 

::: info What is a stream?
A stream in KurrentDB is a sequence of recorded events, each with a unique integer position. Each stream has
a unique name. The positions of events in a stream are gapless.

Stream positions in KurrentDB are "zero-based". The first event in a stream has position `0`, the
second event has position `1`, the third has position `2`, and so on.
:::


### Description

When appending events with `append_to_stream()`, you must provide a stream name and an iterable of `NewEvent`
objects. You must also specify what optimistic concurrency control you want KurrentDB to activate before recording
the new events.

Optionally, you can also specify a timeout for the completion of the operation, and override
credentials given in the "user info" part of the connection string.

The `append_to_stream()` method is atomic, which means that either all or none of the new events will be recorded.

Use the `current_version` parameter to active or deactivate optimistic concurrency control.

::: tip
Please study the [Optimistic concurrency control](#optimistic-concurrency-control) section below for more information about optimistic concurrent controls in KurrentDB.
:::

The `append_to_stream()` method is idempotent. If you call `append_to_stream()` twice with the same event IDs and
the same `current_version` then the second call will succeed idempotently without creating duplicate events.

### Parameters

| Name              | Type                   | Required | Default | Description                                                    |
|-------------------|------------------------|----------|---------|----------------------------------------------------------------|
| `stream_name`     | `str`                  | Yes      |         | Stream to which new events will be appended.                   |
| `events`          | `Iterable[NewEvent]`   | Yes      |         | Events to append to the stream.                                |
| `current_version` | `int \| StreamState`   | Yes      |         | Activate or deactivate optimistic concurrent control.          |                                                                               
| `timeout`         | `float`                | No       | None    | Maximum duration, in seconds, for completion of the operation. |                                                                         
| `credentials`     | `grpc.CallCredentials` | No       | None    | Override credentials derived from the connection string.       |                                                                         

### Return value

On success, `append_to_stream()` returns a commit position (`int`). The "commit position" is the
position in the database of the last event recorded event. The value returned from `append_to_stream()`
can be used when redirecting a user to an eventually consistent view.


### Example

The example below connects to KurrentDB and appends a new event to a new stream.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import KurrentDBClient, NewEvent, StreamState

# Connect to KurrentDB
uri = "kurrentdb://127.0.0.1:2113?tls=false"
client = KurrentDBClient(uri)


# Construct a new event object
event1 = NewEvent(
    type="OrderCreated",
    data=b'{"order_id": "order:123"}',
)

# Append the event to a stream
commit_position = client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.NO_STREAM,
    events=[event1],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import AsyncKurrentDBClient, NewEvent, StreamState

# Connect to KurrentDB
uri = "kurrentdb://127.0.0.1:2113?tls=false"
client = AsyncKurrentDBClient(uri)
await client.connect()

# Construct a new event object
event1 = NewEvent(
    type="OrderCreated",
    data=b'{"order_id": "order:123"}',
)

# Append the event to a stream
commit_position = await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.NO_STREAM,
    events=[event1],
)
```
:::


## The NewEvent class

Use the `NewEvent` dataclass when appending new events to KurrentDB.

The `NewEvent` dataclass allows you to specify a event's type and content. Optionally, you can
also specify the content type, metadata, and a unique ID.

### Fields

| Name           | Type    | Required | Default              | Description               |
|----------------|---------|----------|----------------------|---------------------------|
| `type`         | `str`   | Yes      |                      | The type of the event     |
| `data`         | `bytes` | Yes      |                      | The content of the event  |
| `metadata`     | `bytes` | No       | `b""`                | Event metadata            |
| `content_type` | `str`   | No       | `"application/json"` | The format of the content |
| `id`           | `UUID`  | No       | `uuid.uuid4()`       | A unique ID for the event |


### Event type

Each new event should be supplied with an event `type`. Usually `NewEvent` objects are serialised representations
of different types of domain events. It is common to for the `type` string of a `NewEvent` object to represent a
domain event class, as it makes serialising and de-serialising of the event easy.

### Event data

Usually the serialized state of a domain event object. If you serialize your domain events as JSON objects,
you can take advantage of of KurrentDB's other functionality, such as projections. But you can serialize events
using whatever format suits your requirements. The data will be stored as encoded bytes.

### Event metadata

Storing additional information alongside your event that is not part of the event itself is supported by KurrentDB.
This can be correlation IDs, timestamps, access information, etc. KurrentDB allows you to store a separate byte array
containing this information to keep it separate.

### Event content type

The content type indicates whether the event is stored as JSON or binary format. You can choose between
`'application/json'` (default) and `'application/octet-stream'` when creating your `NewEvent` object.
For example, if you are using Message Pack or Protobuf to serialise your domain events, or you are
serialising with JSON but also using application-level compression or encryption, then you can use
`'application/octet-stream'` as the content type.

### Event ID

Events can be uniquely identified using the `id` field of `NewEvent`. If two events with the same `UUID`
are appended to the same stream with the same optimistic concurrency control, KurrentDB will only append
one of the events to the stream.

## Optimistic concurrency control

When appending events to a stream, you must supply a `current_version` argument. This informs KurrentDB of
the state you expect the stream to be in when appending an event. If the stream isn't in that state, a
`WrongCurrentVersionError` exception will be raised.

There are several available options for the `current_version` argument: 
- Integer value - The stream position of the last recorded event
- `StreamState.NO_STREAM` - Stream should not exist
- `StreamState.EXISTS` - Stream should exist
- `StreamState.ANY` - No concurrency check

Usually, you will use: either `StreamState.NO_STREAM` when writing new events to a new stream; or the stream position
of the last recorded event in the stream when writing subsequent events to an existing stream. This will protect the
stream from becoming inconsistent due to conflicting concurrent writers.

Alternatively, you can specify `StreamState.EXISTS`, which requires only that the stream already has at least one event.

Or, you can fully deactivate concurrency control by specifying `StreamState.ANY`.

### Examples

Let's recall that the stream `"order:123"` was created in the [example](#example) above.

The example below shows that a second event can be successfully appended with `current_version`
as the stream position of the first appended event, which is `0`.

::: tabs
@tab sync
```python:no-line-numbers
event2 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "processing"}',
)

client.append_to_stream(
    stream_name="order:123",
    current_version=0,  # <-- correct value
    events=[event2],
)
```
@tab async
```python:no-line-numbers
event2 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "processing"}',
)

await client.append_to_stream(
    stream_name="order:123",
    current_version=0,  # <-- correct value
    events=[event2],
)
```
:::

The example below shows that a third event can be successfully appended with `current_version`
as the stream position of the second appended event, which is `1`.

::: tabs
@tab sync
```python:no-line-numbers
event3 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "shipped"}',
)

client.append_to_stream(
    stream_name="order:123",
    current_version=1,  # <-- correct value
    events=[event3],
)
```
@tab async
```python:no-line-numbers
event3 = NewEvent(
    type="OrderUpdated",
    data=b'{"status": "shipped"}',
)

await client.append_to_stream(
    stream_name="order:123",
    current_version=1,  # <-- correct value
    events=[event3],
)
```
:::

The operation in the example below fails because we use `StreamState.NO_STREAM` as the value of
`current_version`, however the stream already exists.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import WrongCurrentVersionError 

try:
    client.append_to_stream(
        stream_name="order:123",
        current_version=StreamState.NO_STREAM,  # <-- wrong value
        events=[event3],
    )
    
except WrongCurrentVersionError:
    print("Stream already exists!")
    
else:
    raise Exception("Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import WrongCurrentVersionError 

try:
    await client.append_to_stream(
        stream_name="order:123",
        current_version=StreamState.NO_STREAM,  # <-- wrong value
        events=[event3],
    )
    
except WrongCurrentVersionError:
    print("Stream already exists!")
    
else:
    raise Exception("Shouldn't get here")
```
:::

The operation in the example below fails because we use `0` as the value of
`current_version`, however the stream position of the last recorded event
is now `2`.

::: tabs
@tab sync
```python:no-line-numbers
try:
    client.append_to_stream(
        stream_name="order:123",
        current_version=0,  # <-- incorrect value
        events=[event3],
    )

except WrongCurrentVersionError:
    print("Wrong current version!")

else:
    raise Exception("Shouldn't get here")
```
@tab async
```python:no-line-numbers
try:
    await client.append_to_stream(
        stream_name="order:123",
        current_version=0,  # <-- incorrect value
        events=[event3],
    )

except WrongCurrentVersionError:
    print("Wrong current version!")

else:
    raise Exception("Shouldn't get here")
```
:::

## Idempotent append

Under certain conditions, KurrentDB allows append requests to succeed idempotently. Sometimes an
append operation can succeed in KurrentDB, but the response can fail to reach the client, perhaps
due to a network failure.

If you call `append_to_stream()` twice with the same event IDs and the same `current_version` then
the second call will succeed idempotently.

The examples below show the operations in the previous examples succeeding idempotently.

::: tabs
@tab sync
```python:no-line-numbers
assert 2 == client.get_current_version("order:123")

client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.NO_STREAM,
    events=[event1],
)

client.append_to_stream(
    stream_name="order:123",
    current_version=0,
    events=[event2],
)

client.append_to_stream(
    stream_name="order:123",
    current_version=1,
    events=[event3],
)

assert 2 == client.get_current_version("order:123")
```
@tab async
```python:no-line-numbers
assert 2 == await client.get_current_version("order:123")

await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.NO_STREAM,
    events=[event1],
)

await client.append_to_stream(
    stream_name="order:123",
    current_version=0,
    events=[event2],
)

await client.append_to_stream(
    stream_name="order:123",
    current_version=1,
    events=[event3],
)

assert 2 == await client.get_current_version("order:123")
```
:::

The idempotent append behavior means that retries of apparently failed operations, that were actually successful,
will be apparently successful without actually having any further effect.

The idempotent append behavior can be understood as a kind of "forgiveness" for optimistic concurrency control failures,
without which clients would need to probe the database to discover if an apparently failed request actually succeeded.
But it also avoids recording duplicate events when optimistic concurrency controls are partially or fully disabled.

The examples below show `append_to_stream()` being called with `event1`, `event2`, and `event3` whilst optimistic
concurrency controls have been either fully or partially disabled, and that the stream has not changed.

::: tabs
@tab sync
```python:no-line-numbers
client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.ANY,
    events=[event1],
)

client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.EXISTS,
    events=[event2],
)

client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.EXISTS,
    events=[event3],
)

assert 2 == client.get_current_version("order:123")
```
@tab async
```python:no-line-numbers
await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.ANY,
    events=[event1],
)

await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.EXISTS,
    events=[event2],
)

await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.EXISTS,
    events=[event3],
)

assert 2 == await client.get_current_version("order:123")
```
:::

Please note, whilst in many cases this will avoid recording duplicates, this does not protect against recording
more than one event with the same ID, for example by appending an event with the same ID, either at a much later
time when disabling concurrency controls, or by specifying correctly the position of the last recorded event.

## User credentials

You can use the `credentials` parameter of `append_to_stream()` to override the credentials given with the [user
info](./getting-started.md#user-info-string) part of a client connection string. The helper method
`construct_call_credentials()` constructs a `grpc.CallCredentials` object from a username and password.

::: tabs
@tab sync
```python:no-line-numbers
# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin", 
    password="changeit",
)

# Use credentials for this specific operation
commit_position = client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.ANY,
    events=[event3],
    credentials=credentials,
)
```
@tab async
```python:no-line-numbers
# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin", 
    password="changeit",
)

# Use credentials for this specific operation
commit_position = await client.append_to_stream(
    stream_name="order:123",
    current_version=StreamState.ANY,
    events=[event3],
    credentials=credentials,
)
```
:::

## Multi-append to stream

::: info
Supported by KurrentDB 25.1 and later.
:::

You can use the`multi_append_to_stream()` to append new events atomically to multiple
streams.



### Description

Use the multi-stream append operation when you want to atomically append new
events to multiple streams in one call.

When appending events with `multi_append_to_stream()` you must provide an iterable of `NewEvents`
objects.

Optionally, you can also specify a timeout for the completion of the operation, and override
credentials given in the "user info" part of the connection string.

The `multi_append_to_stream()` method is atomic, which means that either all or none of the new events will be recorded.

The `multi_append_to_stream()` method is also idempotent.

### Parameters

| Name              | Type                   | Required | Default | Description                                                    |
|-------------------|------------------------|----------|---------|----------------------------------------------------------------|
| `events`          | `Iterable[NewEvents]`  | Yes      |         | Events to append to the stream.                                |
| `timeout`         | `float`                | No       | None    | Maximum duration, in seconds, for completion of the operation. |                                                                         
| `credentials`     | `grpc.CallCredentials` | No       | None    | Override credentials derived from the connection string.       |                                                                         


### Return value

On success, `append_to_stream()` returns a commit position (`int`). The "commit position" is the
position in the database of the last event recorded event. The value returned from `multi_append_to_stream()`
can be used when redirecting a user to an eventually consistent view.

### Example

The example below appends new events to two streams.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import NewEvents

new_events1 = NewEvents(
    stream_name="order:123",
    events=[
        NewEvent(type='EventType1', data=b'{}'),
        NewEvent(type='EventType2', data=b'{}'),
    ],
    current_version=2,
)

new_events2 = NewEvents(
    stream_name="order:456",
    events=[
        NewEvent(type='EventType3', data=b'{}'),
        NewEvent(type='EventType4', data=b'{}'),
    ],
    current_version=StreamState.NO_STREAM,
)

client.multi_append_to_stream(
    events=[new_events1, new_events2],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import NewEvents

new_events1 = NewEvents(
    stream_name="order:123",
    events=[
        NewEvent(type='EventType1', data=b'{}'),
        NewEvent(type='EventType2', data=b'{}'),
    ],
    current_version=2,
)

new_events2 = NewEvents(
    stream_name="order:456",
    events=[
        NewEvent(type='EventType3', data=b'{}'),
        NewEvent(type='EventType4', data=b'{}'),
    ],
    current_version=StreamState.NO_STREAM,
)

await client.multi_append_to_stream(
    events=[new_events1, new_events2],
)
```
:::


## The NewEvents class

Use the `NewEvents` dataclass when appending event to multiple streams.

The `NewEvents` dataclass allows you to specify the stream name, events to be
appended to that stream, and optimistic concurrency controls for that stream.

### Fields

The `NewEvents` dataclass has three fields, which are effectively the parameters of
the `append_to_stream()` method that are missing from the `multi_append_to_stream()`
method.

| Name              | Type                   | Required | Default | Description                                           |
|-------------------|------------------------|----------|---------|-------------------------------------------------------|
| `stream_name`     | `str`                  | Yes      |         | Stream to which new events will be appended.          |
| `events`          | `Iterable[NewEvent]`   | Yes      |         | The `NewEvent` objects to append to the stream.       |
| `current_version` | `int \| StreamState`   | Yes      |         | Activate or deactivate optimistic concurrent control. |                                                                               

The sections above describe the fields of the `NewEvent` class, and also the behavior of the optimistic concurrency
controls, and apply consistently to the operation of `multi_append_to_stream()`.

There is one main difference: restrictions on the use of the `metadata` field of `NewEvent`.

### Metadata restrictions for multi-append

When appending events with `multi_append_to_stream()`, the `metadata` field of
each `NewEvent` must be either an empty `bytes` string or a `bytes` string
containing a JSON object whose values are strings.

The following metadata values are OK.

|   | Description                    | Examples        |
|---|--------------------------------|-----------------|
| ✅ | Empty bytes                    | `b""`           |
| ✅ | JSON object with string values | `b'{"a": "1"}'` |


The following metadata values are NOT okay and will cause a
`ProgrammingError` exception.

|   | Description                        | Examples                                      |
|---|------------------------------------|-----------------------------------------------|
| ❌ | Random bytes                       | `b'\xf5d\xc5W3^b\xb0(\xf9\x01D\x81\xa7Y\x98'` |
| ❌ | JSON string                      | `b'"abcdef"'`                                 |
| ❌ | JSON object with non-string values | `b'{"a": 1}'` or `b'{"a": false}'`            |
| ❌ | Nested JSON objects                | `b'{"a": {}}'`                                |
