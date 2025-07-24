---
order: 2
---

# Appending events

When you start working with KurrentDB, your application streams are empty. The first meaningful operation is to add one or more events to the database using this API.

::: tip
Check the [Getting Started](getting-started.md) guide to learn how to configure and use the client SDK.
:::

## Append your first event

The simplest way to append an event to KurrentDB is to create a `NewEvent` object and call the `append_to_stream()` method.

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

The `append_to_stream()` method takes a sequence of new event objects that can contain JSON or binary data, which allows you to save more than one event in a single batch.
 
Outside the example above, other options exist for dealing with different scenarios. 

::: tip
If you are new to Event Sourcing, please study the [Handling concurrency](#handling-concurrency) section below.
:::

## Working with NewEvent

Events appended to KurrentDB must be wrapped in a `NewEvent` object. This allows you to specify the event's content, the type of event, and whether it's in JSON format. In its simplest form, you need two required arguments: **type** and **data**, and three optional arguments: **metadata**, **content_type**, and **id**.

### EventID

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

### EventType

Each event should be supplied with an event type. This unique string is used to identify the type of event you are saving. 

It is common to see the explicit event code type name used as the type as it makes serialising and de-serialising of the event easy. However, we recommend against this as it couples the storage to the type and will make it more difficult if you need to version the event at a later date.

### Data

Representation of your event data. It is recommended that you store your events as JSON objects. This allows you to take advantage of all of KurrentDB's functionality, such as projections. That said, you can save events using whatever format suits your workflow. Eventually, the data will be stored as encoded bytes.

### Metadata

Storing additional information alongside your event that is not part of the event itself is standard practice. This can be correlation IDs, timestamps, access information, etc. KurrentDB allows you to store a separate byte array containing this information to keep it separate.

### ContentType

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