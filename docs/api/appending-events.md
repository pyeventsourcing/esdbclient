---
order: 2
---

# Appending events

::: info Requirements
Requires leader node.
:::

When you start working with KurrentDB, your application streams are empty. The first meaningful operation is to add one or more events to the database using this API.

::: tip
Check the [Getting Started](getting-started.md) guide to learn how to configure and use the client SDK.
:::


## Append your first event

The simplest way to append an event to KurrentDB is to create a `NewEvent` object and call the `append_to_stream()` method.

The `append_to_stream()` method takes a sequence of new event objects that can contain JSON or binary data, which allows you to save more than one event in a single batch.

### Parameters

- `stream_name` (`str`): The target stream to which the contained iterable of
  events will be appended.
- `events` (`Iterable[NewEvent]`): The events to append to the indicated stream.
- `current_version` (`int | StreamState`): Expected version for optimistic
  concurrency. Use an integer for the expected last recorded event position, or
  a `StreamState` value:
  - `StreamState.NO_STREAM` — stream must not exist or must have been deleted
  - `StreamState.ANY` — disables concurrency checks
  - `StreamState.EXISTS` — requires the stream to already have at least one event
- `timeout` (optional): Python `float` that sets a maximum duration, in seconds,
  for the completion of the gRPC operation.
- `credentials` (optional): Call credentials that override credentials derived
  from the connection string URI.

### Return value

On success, `append_to_stream()` returns the commit position (`int`) of the
last event.

### Example

```python
import uuid
from kurrentdbclient import KurrentDBClient, NewEvent, StreamState

# Construct a new event object
event = NewEvent(
    type='OrderCreated',
    data=b'{"order_id": "' + str(uuid.uuid4()).encode() + b'"}',
    id=uuid.uuid4()
)

# Append the event to a stream
commit_position = client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event]
)
```

::: tip
If you are new to Event Sourcing, please study the [Handling concurrency](#handling-concurrency) section below.
:::

## Working with NewEvent

Events appended to KurrentDB via the Python client must be instances of `NewEvent`.

The `NewEvent` dataclass allows you to specify the event's content, the type of event, and whether it's in JSON format. In its simplest form, you need two required arguments: **type** and **data**. There are also three optional arguments: **metadata**, **content_type**, and **id**.

### Event ID

This takes the format of a `UUID` and is used to uniquely identify the event you are trying to append. If two events with the same `UUID` are appended to the same stream in quick succession, KurrentDB will only append one of the events to the stream. 

For example, the following code will only append a single event:

```python
import uuid
from kurrentdbclient import NewEvent, StreamState

# Create an event with a specific ID
event_id = uuid.uuid4()
event = NewEvent(
    type='OrderCreated',
    data=b'{"order_id": "1"}',
    id=event_id
)

# Append the event
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event]
)

# Attempt to append the same event again - this will be idempotent
client.append_to_stream(
    stream_name="order-123",
    current_version=0,
    events=[event]
)
```

### Event type

Each event should be supplied with an event type. This unique string is used to identify the type of event you are saving. 

It is common to see the explicit event code type name used as the type as it makes serialising and de-serialising of the event easy. However, we recommend against this as it couples the storage to the type and will make it more difficult if you need to version the event at a later date.

### Event data

Representation of your event data. It is recommended that you store your events as JSON objects. This allows you to take advantage of all of KurrentDB's functionality, such as projections. That said, you can save events using whatever format suits your workflow. Eventually, the data will be stored as encoded bytes.

### Event metadata

Storing additional information alongside your event that is not part of the event itself is standard practice. This can be correlation IDs, timestamps, access information, etc. KurrentDB allows you to store a separate byte array containing this information to keep it separate.

### Event content type

The content type indicates whether the event is stored as JSON or binary format. You can choose between `'application/json'` (default) and `'application/octet-stream'` when creating your `NewEvent` object. 

## Handling concurrency

When appending events to a stream, you can supply a *current version*. Your client uses this to inform KurrentDB of the state or version you expect the stream to be in when appending an event. If the stream isn't in that state, a `WrongCurrentVersionError` exception will be raised. 

For example, if you try to append the same record twice, expecting both times that the stream doesn't exist, you will get an exception on the second:

```python
# First append - stream doesn't exist yet
event1 = NewEvent(
    type='OrderCreated',
    data=b'{"order_id": "1"}',
    id=uuid.uuid4()
)

client.append_to_stream(
    stream_name="order-456",
    current_version=StreamState.NO_STREAM,
    events=[event1]
)

# Second append - this will raise WrongCurrentVersionError
# because the stream now exists
event2 = NewEvent(
    type='OrderCreated',
    data=b'{"order_id": "2"}',
    id=uuid.uuid4()
)

try:
    client.append_to_stream(
        stream_name="order-456",
        current_version=StreamState.NO_STREAM,  # Stream exists now!
        events=[event2]
    )
except WrongCurrentVersionError:
    print("Stream already exists!")
```

There are several available expected version options: 
- `StreamState.ANY` - No concurrency check
- `StreamState.NO_STREAM` - Stream should not exist
- `StreamState.EXISTS` - Stream should exist
- Integer value - Stream should be at specific version

This check can be used to implement optimistic concurrency. When retrieving a
stream from KurrentDB, note the current version number. When you save it back,
you can determine if somebody else has modified the record in the meantime.

```python
# Get the current state of the stream
recorded_events = client.get_stream("order-789")
current_version = len(recorded_events) - 1 if recorded_events else StreamState.NO_STREAM

# Update the order
event = NewEvent(
    type='OrderUpdated',
    data=b'{"order_id": "1", "status": "processing"}',
    id=uuid.uuid4()
)

# Append with concurrency control
client.append_to_stream(
    stream_name="order-789",
    current_version=current_version,
    events=[event]
)

# Try to append another event with the same current_version
# This will fail if someone else has written to the stream
event2 = NewEvent(
    type='OrderUpdated',
    data=b'{"order_id": "2", "status": "shipped"}',
    id=uuid.uuid4()
)

try:
    client.append_to_stream(
        stream_name="order-789",
        current_version=current_version,  # This might be stale now
        events=[event2]
    )
except WrongCurrentVersionError:
    print("Someone else modified the stream, need to retry")
```

## User credentials

You can provide user credentials to append the data as follows. This will override the default credentials set on the connection.

```python
from kurrentdbclient import KurrentDBClient

# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin", 
    password="changeit"
)

# Use credentials for this specific operation
commit_position = client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event],
    credentials=credentials
)
```

## Multi-stream append

::: info Requirements
Supported by KurrentDB 25.1 and later.
:::

The `KurrentDBClient` method `multi_append_to_stream()` records many sequences
of new events, each sequence being appended to a different stream. The operation
is atomic across all sequences provided.

Use the multi-stream append operation when you want to atomically append new
events to multiple streams in one call. Either all the provided sequences of
events are written, or none of them are.

### Parameters

- `events` (required): A single `NewEvents` instance or an iterable of
  `NewEvents` instances.
- `timeout` (optional): Python `float` that sets a maximum duration, in seconds,
  for the completion of the gRPC operation.
- `credentials` (optional): Call credentials that override credentials derived
  from the connection string URI.

If the operation succeeds, the method returns the commit position of the last
event in the last sequence.

### Return value

On success, `multi_append_to_stream()` returns the commit position (`int`) of the
last event in the last provided sequence.

### NewEvents

Import the `NewEvents` dataclass from `kurrentdbclient`. It has three fields
that mirror the arguments of `append_to_stream()`:

- `stream_name` (`str`): The target stream to which the contained iterable of
  events will be appended.
- `events` (`Iterable[NewEvent]`): The events to append to the indicated stream.
- `current_version` (`int | StreamState`): Expected version for optimistic
  concurrency. Use an integer for the expected last recorded event position, or
  a `StreamState` value:
  - `StreamState.NO_STREAM` — stream must not exist or must have been deleted
  - `StreamState.ANY` — disables concurrency checks
  - `StreamState.EXISTS` — requires the stream to already have at least one event

### Example

```python
import uuid
from kurrentdbclient import (
    KurrentDBClient,
    NewEvent,
    NewEvents,
    StreamState,
)

# Assuming you have an existing client
# client = KurrentDBClient(uri="kurrentdb://admin:changeit@localhost:2113?tls=false")

new_events1 = NewEvents(
    stream_name=str(uuid.uuid4()),
    events=[
        NewEvent(type='EventType1', data=b'{}'),
        NewEvent(type='EventType2', data=b'{}'),
    ],
    current_version=StreamState.NO_STREAM,
)

new_events2 = NewEvents(
    stream_name=str(uuid.uuid4()),
    events=[
        NewEvent(type='EventType3', data=b'{}'),
        NewEvent(type='EventType4', data=b'{}'),
    ],
    current_version=StreamState.NO_STREAM,
)

commit_position = client.multi_append_to_stream(
    events=[new_events1, new_events2],
    # timeout=5.0,              # optional
    # credentials=credentials,  # optional
)

print("Committed at:", commit_position)
```

### Metadata restrictions for multi-append

When appending events with `multi_append_to_stream()`, the `metadata` field of
each `NewEvent` must be either an empty `bytes` string or a `bytes` string
containing a JSON object whose values are strings.

The following metadata values are OK:
- `b""` (empty bytes)
- `b'{"a": "1"}'` (JSON object with string values)

The following metadata values are NOT OK and will result in a
`kurrentdbclient.exceptions.ProgrammingError`:
- Random bytes like `b'\xf5d\xc5W3^b\xb0(\xf9\x01D\x81\xa7Y\x98'`
- A JSON string like `b'"abcdef"'`
- JSON object with non-string values, e.g. `b'{"a": 1}'`, `b'{"a": false}'`,
  or nested objects like `b'{"a": {}}'`

