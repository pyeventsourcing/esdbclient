---
order: 2
---

# Appending events

This guide describes the Python client methods for recording new events in KurrentDB.

## Introduction

In KurrentDB, events are appended to [streams](#what-is-a-stream).

The Python client for KurrentDB has two methods for writing new events:

* [`append_to_stream()`](#append-to-stream) – write a collection of events to a named stream
* [`multi_append_to_stream()`](#multi-append-to-stream) – write many collections of events, each to a different stream

::: info Requires leader
When connecting to a KurrentDB cluster, events can be written only to the leader node.
:::

These methods are atomic and [idempotent](#idempotent-append-behavior). All or none of the new events will be recorded once.

The Python client for KurrentDB also has methods for getting and setting [stream metadata](@server/features/streams.md#metadata-and-reserved-names):

* [`get_stream_metadata()`](#get-stream-metadata)
* [`set_stream_metadata()`](#set-stream-metadata)

## What is a stream?

A stream in KurrentDB is a sequence of recorded events, each with a unique integer
position. Each stream has a unique name. The positions of events in a stream are
zero-based and gapless. The first event in a stream has position `0`, the
second event has position `1`, the third has position `2`, and so on.

## Events in KurrentDB

KurrentDB organises events in streams within a global transaction log.

Two sequence numbers are assigned to each recorded event:

* **stream position** – the position of a recorded event within
 its stream
* **commit position** – the position of a recorded event in the global
transaction log

These numbers are assigned when new events are recorded, and used when recorded events are read.

## New events

The `NewEvent` class is provided for specifying new events before calling an append method.

| Field          | Type    | Description               | Default              |
|----------------|---------|---------------------------|----------------------|
| `type`         | `str`   | The type of the event     |                      |
| `data`         | `bytes` | The content of the event  |                      |
| `metadata`     | `bytes` | Event metadata            | `b""`                |
| `content_type` | `str`   | The format of the content | `"application/json"` |
| `id`           | `UUID`  | A unique ID for the event | `uuid.uuid4()`       |


### Event type

Each new event must be supplied with an event `type` string.

### Event data

The `data` field is a Python bytes object that carries the event payload. Usually the serialized state of a domain event object. If you serialize your
domain events as JSON objects, you can take advantage of KurrentDB's other functionality, such as projections. But you
can serialize events using whatever format suits your requirements. The data will be stored as encoded bytes.

### Event metadata

The `metadata` field is a Python bytes object that carries salient information about the event. It can be used for storing additional information alongside your event
payload, such as correlation IDs, timestamps, access information, etc. KurrentDB allows you to store a separate byte array containing this information to keep it separate.

### Event content type

The `content_type` field indicates whether the event is stored as JSON or binary format. You can choose between
`'application/json'` (default) and `'application/octet-stream'`. For example, if you are using Message Pack or
Protobuf to serialise your domain events, or you are serialising with JSON but also using application-level
compression or encryption, then you can use `'application/octet-stream'` as the content type. The default
value is `'application/json'`.

### Event ID

The `id` field is a `UUID` object that can uniquely identify the event. KurrentDB does not enforce unique event IDs,
however they are used to activate [idempotent append behavior](#idempotent-append-behavior). If two events with the
same `UUID` are appended to the same stream with the same optimistic concurrency control, KurrentDB will only append
one of the events to the stream. The default value is a new version 4 UUID.

### Examples

Here's an example where only the `type` string and binary `data` are provided.

```python:no-line-numbers
from kurrentdbclient import NewEvent

order_created = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)
```

You may also specify `metadata`, `content_type` and an `id`.

```python:no-line-numbers
from uuid import uuid4

order_created = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
    metadata=b'{"correlation_id": "56"}',
    content_type="application/json",
    id=uuid4(),
)
```



## Append to stream

The Python client's `append_to_stream()` method appends new events to a named stream.

This method is atomic and [idempotent](#idempotent-append-behavior).

Provide a `stream_name` argument, an `events` argument, and a `current_version` argument.

The `events` argument must be an iterable of [`NewEvent`](#new-events) objects. The `current_version` parameter specifies
what [optimistic concurrency control](#optimistic-concurrency-control) you want KurrentDB to apply.

| Parameter         | Description                                                                                                                                              | Default  |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `stream_name`     | Stream to which the `events` will be appended.                                                                                                           |          |
| `events`          | The [NewEvent](#new-events) objects to be appended to the stream.                                                                                        |          |
| `current_version` | The [optimistic concurrency control](#optimistic-concurrency-control) for appending `events`.                                                            |          |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                                              | `None`   |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

If successful, `append_to_stream()` returns the commit position (`int`) of the last event
it appended. This value represents that event’s position in the global transaction log,
and can be used by applications to wait until eventually consistent views reflect newly
recorded events.

### Optimistic concurrency control

The `current_version` argument can be used to inform KurrentDB of the state you expect
a stream to be in when appending events.

There are several available options for the `current_version` argument:
- `StreamState.ANY` - No concurrency check
- `StreamState.EXISTS` - Stream should exist
- `StreamState.NO_STREAM` - Stream should not exist
- `int` value - Stream position of the last recorded event

If the optimistic concurrency control fails, a `WrongCurrentVersionError` exception will be raised.

Usually you will use `StreamState.NO_STREAM` when writing new events to a new stream, and then
the correct stream position of the last recorded event in the stream when writing subsequent events.
This will protect the stream from becoming inconsistent due to conflicting concurrent writers.

Alternatively, you can specify `StreamState.EXISTS`, which requires only that the stream already
has at least one event.

Or, you can fully deactivate concurrency control by specifying `StreamState.ANY`.

Let's see how to activate and deactivate optimistic concurrency control.

### Append to new stream

Here's an example appending the first event to stream `'order-123'`.
The `current_version` argument `StreamState.NO_STREAM` requires that no events
have been appended for this stream name.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import KurrentDBClient, StreamState

# Connect to KurrentDB
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
client = KurrentDBClient(connection_string)

# Create a new stream with a new event
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,  # <-- correct value
    events=[order_created],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import AsyncKurrentDBClient, StreamState

# Connect to KurrentDB
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
client = AsyncKurrentDBClient(connection_string)

# Create a new stream with a new event
await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,  # <-- correct value
    events=[order_created],
)
```
:::

### Append to existing stream

Here's an example appending a second event to stream `'order-123'`. The
`current_version` argument `0` is the position of the first event in the stream.

::: tabs
@tab sync
```python:no-line-numbers
payment_received = NewEvent(
    type="PaymentCompleted",
    data=b'{}',
)

client.append_to_stream(
    stream_name="order-123",
    current_version=0,  # <-- correct value
    events=[payment_received],
)
```
@tab async
```python:no-line-numbers
payment_received = NewEvent(
    type="PaymentCompleted",
    data=b'{}',
)

await client.append_to_stream(
    stream_name="order-123",
    current_version=0,  # <-- correct value
    events=[payment_received],
)
```
:::

Here's an example that shows a third event can be successfully appended with `current_version`
as the stream position of the second appended event, which is `1`.

::: tabs
@tab sync
```python:no-line-numbers
product_shipped = NewEvent(
    type="ProductShipped",
    data=b'{}',
)

client.append_to_stream(
    stream_name="order-123",
    current_version=1,  # <-- correct value
    events=[product_shipped],
)
```
@tab async
```python:no-line-numbers
product_shipped = NewEvent(
    type="ProductShipped",
    data=b'{}',
)

await client.append_to_stream(
    stream_name="order-123",
    current_version=1,  # <-- correct value
    events=[product_shipped],
)
```
:::

### Wrong current version error

Here's an example that shows optimistic concurrent control rejecting an append options.
In this example,`StreamState.NO_STREAM` is specified as the value of `current_version`,
however the stream already exists, and so the append operation fails.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import WrongCurrentVersionError

product_received = NewEvent(
    type="ProductReceived",
    data=b'{}',
)

try:
    client.append_to_stream(
        stream_name="order-123",
        current_version=StreamState.NO_STREAM,  # <-- wrong value
        events=[product_received],
    )

except WrongCurrentVersionError:
    print("Stream already exists!")

else:
    raise Exception("Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import WrongCurrentVersionError

product_received = NewEvent(
    type="ProductReceived",
    data=b'{}',
)

try:
    await client.append_to_stream(
        stream_name="order-123",
        current_version=StreamState.NO_STREAM,  # <-- wrong value
        events=[product_received],
    )

except WrongCurrentVersionError:
    print("Stream already exists!")

else:
    raise Exception("Shouldn't get here")
```
:::

Similarly, the append operation in the example below fails because the value of
`current_version` is `0`, however the stream position of the last recorded event
in stream `order-123` is `2`.

::: tabs
@tab sync
```python:no-line-numbers
try:
    client.append_to_stream(
        stream_name="order-123",
        current_version=0,  # <-- incorrect value
        events=[product_shipped],
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
        stream_name="order-123",
        current_version=0,  # <-- incorrect value
        events=[product_shipped],
    )

except WrongCurrentVersionError:
    print("Wrong current version!")

else:
    raise Exception("Shouldn't get here")
```
:::

### Idempotent append behavior

When [optimistic concurrency control](#optimistic-concurrency-control) is activated,
retrying a successful append operation will return without failing due to the previous success.

When optimistic concurrent control is [fully or partially disabled](#optimistic-concurrency-control),
a successful append operation will return without appending duplicate events.

Without KurrentDB's idempotent append behavior, a client would need to probe
the database to determine whether an apparently failed request had actually succeeded.
This behavior depends on events having unique event IDs, which is the default when constructing [`NewEvent`](#new-events) objects.

Please note, KurrentDB does not enforce unique event IDs. The idempotent append behaviour does not protect against
recording more than one event with the same ID, for example by appending an event with
the same ID in a different stream, or in the same stream when specifying correctly
the position of the last recorded event, or in the same stream at a much later
time when disabling concurrency controls.

Here are some examples showing previous operations succeeding idempotently.

::: tabs
@tab sync
```python:no-line-numbers
# Check the stream has exactly three events.
assert len(client.get_stream("order-123")) == 3

# Retry order created - succeeds idempotently.
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[order_created],
)

# Retry payment received - succeeds idempotently.
client.append_to_stream(
    stream_name="order-123",
    current_version=0,
    events=[payment_received],
)

# Retry product shipped - succeeds idempotently.
client.append_to_stream(
    stream_name="order-123",
    current_version=1,
    events=[product_shipped],
)

# Check the stream has exactly three events.
assert len(client.get_stream("order-123")) == 3
```
@tab async
```python:no-line-numbers
# Check the stream has exactly two events.
assert len(await client.get_stream("order-123")) == 3

# Retry appending first event - succeeds idempotently.
await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[order_created],
)

# Retry appending second event - succeeds idempotently.
await client.append_to_stream(
    stream_name="order-123",
    current_version=0,
    events=[payment_received],
)

# Retry appending third event - succeeds idempotently.
await client.append_to_stream(
    stream_name="order-123",
    current_version=1,
    events=[product_shipped],
)

# Check the stream has exactly three events.
assert len(await client.get_stream("order-123")) == 3
```
:::

Here are some examples showing idempotent append behavior when optimistic
concurrency controls have been either fully or partially disabled. Duplicate events are
not recorded: the steam still has exactly two events.

::: tabs
@tab sync
```python:no-line-numbers
# Fully disabled concurrency control - succeeds idempotently.
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.ANY,
    events=[order_created],
)

# Partially disabled concurrency control - succeeds idempotently.
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.EXISTS,
    events=[payment_received],
)

# Partially disabled concurrency control - succeeds idempotently.
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.EXISTS,
    events=[product_shipped],
)

# Check the stream has exactly three events.
assert len(client.get_stream("order-123")) == 3
```
@tab async
```python:no-line-numbers
# Fully disabled concurrency control - succeeds idempotently.
await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.ANY,
    events=[order_created],
)

# Partially disabled concurrency control - succeeds idempotently.
await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.EXISTS,
    events=[payment_received],
)

# Partially disabled concurrency control - succeeds idempotently.
await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.EXISTS,
    events=[product_shipped],
)

# Check the stream has exactly three events.
assert len(await client.get_stream("order-123")) == 3
```
:::

## Multi-append to stream

::: info
Supported by KurrentDB 25.1 and later.
:::

You can use the `multi_append_to_stream()` method to append new events to multiple
streams.

This method is atomic and [idempotent](#idempotent-append-behavior).

Provide an `events` argument, an iterable of [`NewEvents`](#the-newevents-class) objects.
Each specifies a stream name, a collection of
[`NewEvent`](#new-events) objects to be appended to that stream, and an
[optimistic concurrency control](#optimistic-concurrency-control) to be used when
appending those events to that stream.

| Parameter     | Description                                                                                                  | Default |
|---------------|--------------------------------------------------------------------------------------------------------------|---------|
| `events`      | An iterable of [NewEvents](#the-newevents-class) objects.                                                    |         |
| `timeout`     | Maximum duration of operation (in seconds).                                                                 | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

If successful, `multi_append_to_stream()` returns the commit position (`int`) of the last event
it appended. This value represents the event’s position in the global transaction log
and can be used to ensure that eventually consistent views reflect the new events.

### The NewEvents class

Use the `NewEvents` dataclass when [appending events to multiple streams](#multi-append-to-stream).

The fields of a `NewEvents` object specify a `stream_name`, the `events` to be
appended to that stream, and a `current_version` value for [optimistic concurrency control](#optimistic-concurrency-control)
of that stream.
These fields have the same meaning as the corresponding parameters of [`append_to_stream()`](#append-to-stream).

| Field             | Type                 | Description                                                            |
|-------------------|----------------------|------------------------------------------------------------------------|
| `stream_name`     | `str`                | Stream to which new events will be appended.                           |
| `events`          | `Iterable[NewEvent]` | The [`NewEvent`](#new-events) objects to append to the stream. |
| `current_version` | `int\|StreamState`   | The [optimistic concurrency](#optimistic-concurrency-control) control  |

The fields of a `NewEvents` object are like the arguments of [`append_to_stream()`](#append-to-stream).
Because [`multi_append_to_stream()`](#multi-append-to-stream) allows many such things in one call, many
streams can be written to in one atomic operation.

### Metadata restrictions

When appending events with `multi_append_to_stream()`, the `metadata` field of
each `NewEvent` must be either an empty `bytes` string or a `bytes` string
containing a JSON object whose values are strings.

The following metadata values are acceptable.

|   | Description                    | Examples        |
|---|--------------------------------|-----------------|
| ✅ | Empty bytes                    | `b""`           |
| ✅ | JSON object with string values | `b'{"a": "1"}'` |


The following metadata values are NOT acceptable and will cause a
`ProgrammingError` exception.

|   | Description                        | Examples                                      |
|---|------------------------------------|-----------------------------------------------|
| ❌ | Random bytes                       | `b'\xf5d\xc5W3^b\xb0(\xf9\x01D\x81\xa7Y\x98'` |
| ❌ | JSON string                      | `b'"abcdef"'`                                 |
| ❌ | JSON object with non-string values | `b'{"a": 1}'` or `b'{"a": false}'`            |
| ❌ | Nested JSON objects                | `b'{"a": {}}'`                                |

### Example

The example below appends new events to two streams.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import NewEvents

student_events = NewEvents(
    stream_name="student-123",
    events=[
        NewEvent(
            type='StudentRegistered',
            data=b'{"name": "Joe"}'
        ),
        NewEvent(
            type='StudentJoinedCourse',
            data=b'{"course_id": "course-456"}'
        ),
    ],
    current_version=StreamState.NO_STREAM,
)

course_events = NewEvents(
    stream_name="course-456",
    events=[
        NewEvent(
            type='CourseCreated',
            data=b'{"name": "French"}'
        ),
        NewEvent(
            type='StudentJoinedCourse',
            data=b'{"student_id": "student-123"}'
        ),
    ],
    current_version=StreamState.NO_STREAM,
)

client.multi_append_to_stream(
    events=[student_events, course_events],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import NewEvents

student_events = NewEvents(
    stream_name="student-123",
    events=[
        NewEvent(
            type='StudentRegistered',
            data=b'{"name": "Joe"}'
        ),
        NewEvent(
            type='StudentJoinedCourse',
            data=b'{"course_id": "course-456"}'
        ),
    ],
    current_version=StreamState.NO_STREAM,
)

course_events = NewEvents(
    stream_name="course-456",
    events=[
        NewEvent(
            type='CourseCreated',
            data=b'{"name": "French"}'
        ),
        NewEvent(
            type='StudentJoinedCourse',
            data=b'{"student_id": "student-123"}'
        ),
    ],
    current_version=StreamState.NO_STREAM,
)

await client.multi_append_to_stream(
    events=[student_events, course_events],
)
```
:::

## Get stream metadata

You can use the `get_stream_metadata()` method to get [stream metadata](@server/features/streams.md#metadata-and-reserved-names).

Provide a `stream_name` argument.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `stream_name` | Metadata for this stream will be returned.                                                                                                               |         |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

If successful, `get_stream_metadata()` returns a Python `dict` of metadata keys and values for the named stream, along with the current version of the stream's metadata stream.
If the named stream does not exist, the `dict` will be empty and the current version value will be `StreamState.NO_STREAM`. These two values can
be used as arguments of `metadata` and `current_version` when calling [`set_stream_metadata()`](#set-stream-metadata).

### Example

The example below gets metadata for stream `"order-123"`.

::: tabs
@tab sync
```python:no-line-numbers
metadata, current_version = client.get_stream_metadata(
    stream_name="order-123",
)
```
@tab async
```python:no-line-numbers
metadata, current_version = await client.get_stream_metadata(
    stream_name="order-123",
)
```
:::

## Set stream metadata

You can use the `set_stream_metadata()` method to set [stream metadata](@server/features/streams.md#metadata-and-reserved-names).

Provide a `stream_name` argument, a Python `dict` of stream metadata keys and values, and optionally the current version of the stream's metadata stream.

The named stream's metadata will be overwritten with the given `dict`.

| Parameter         | Description                                                                                                                                              | Default           |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| `stream_name`     | Metadata for this stream will be updated.                                                                                                                |                   |
| `metadata`        | A Python `dict` of stream metadata keys and values.                                                                                                      |                   |
| `current_version` | The [optimistic concurrency control](#optimistic-concurrency-control) for setting stream metadata.                                                       | `StreamState.ANY` |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                                              | `None`            |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`            |

If successful, `set_stream_metadata()` returns `None`.

If the named stream does not exist, the metadata will be set anyway. This allows streams to be configured before they are used.

### Example

The example below sets metadata for stream `"order-123"`.

::: tabs
@tab sync
```python:no-line-numbers
metadata["foo"] = "bar"

client.set_stream_metadata(
    stream_name="order-123",
    metadata=metadata,
    current_version=current_version,
)

metadata, _ = client.get_stream_metadata("order-123")
assert metadata["foo"] == "bar"
```
@tab async
```python:no-line-numbers
metadata["foo"] = "bar"

await client.set_stream_metadata(
    stream_name="order-123",
    metadata=metadata,
    current_version=current_version,
)

metadata, _ = await client.get_stream_metadata("order-123")
assert metadata["foo"] == "bar"
```
:::
