---
order: 3
---

# Appending Events

This guide describes methods for recording new events in KurrentDB.

## Overview

The [Python clients for KurrentDB](getting-started.md#python-clients-for-kurrentdb) have three methods for writing new events:

* [`append_to_stream()`](#append-to-stream) – write a collection of events to a single stream
* [`multi_append_to_stream()`](#multi-append-to-stream) – write collections of events to different streams
* [`append_records()`](#append-records) – write events to one or many streams in any order

These methods are atomic and [idempotent](#idempotent-append-behavior).

There are also methods for getting and setting [stream metadata](@server/features/streams.md#metadata-and-reserved-names):

* [`get_stream_metadata()`](#get-stream-metadata)
* [`set_stream_metadata()`](#set-stream-metadata)

## New Event Records

KurrentDB organises event records in streams within a global transaction log.

KurrentDB assigns two sequence numbers to each new event record:

* **Commit position** – The position in the global transaction log.
* **Stream position** – The position of an event in its stream.

Each stream has a unique name. Stream positions are zero-based and gapless.
The position in a stream is position `0`, the second is position `1`, the
third is position `2`, and so on. Positions in KurrentDB's global transaction log
are not gapless.

The Python clients use two different dataclasses for
specifying new event records:

* Use [`NewEvent`](#the-newevent-class) with [`append_to_stream()`](#append-to-stream) and [`multi_append_to_stream()`](#multi-append-to-stream).
* Use [`NewRecord`](#the-newrecord-class) with the [`append_records()`](#append-records) method.

The `data` field of [`NewEvent`](#the-newevent-class) and [`NewRecord`](#the-newrecord-class) is a
Python bytes object that carries the event payload. If you serialize your event state as JSON,
you can take advantage of KurrentDB's broader functionality such as projections. But you may
serialize using whatever format suits your requirements.

The `content_type` field of [`NewEvent`](#the-newevent-class) and [`NewRecord`](#the-newrecord-class) indicates whether the `data`
is serialised as JSON or another binary format. You can choose between `"application/json"` and `"application/octet-stream"`. For
example, if you are using Message Pack or Protobuf to serialise your domain events, or you are serialising with JSON but also
using application-level compression or encryption, then you can use `"application/octet-stream"` as the content type. The default
value is `"application/json"`.

The `metadata` field of [`NewEvent`](#the-newevent-class) and [`NewRecord`](#the-newrecord-class) is
a Python bytes object that carries salient information about the event. It can be used for storing
additional information alongside your event payload, such as correlation IDs, timestamps, access information,
etc. KurrentDB allows you to store a separate byte array containing this information to keep it separate.
See [metadata restrictions](#metadata-restrictions) when using [`multi_append_to_stream()`](#multi-append-to-stream) and [`append_records()`](#append-records).

The `id` field of [`NewEvent`](#the-newevent-class) and [`NewRecord`](#the-newrecord-class) is a `UUID` object that can uniquely identify the event. KurrentDB does not enforce unique event IDs,
however they are used to activate [idempotent append behavior](#idempotent-append-behavior). If two events with the
same `UUID` are appended to the same stream with the same optimistic concurrency control, KurrentDB will only append
one of the events to the stream. The default value is a new version 4 UUID.

## Consistency Checks

When writing to a stream, you can activate consistency checks using:
* the `current_version` argument of the [`append_to_stream()`](#the-newevent-class) method,
* the `current_version` field of the [`NewEvents`](#the-newevents-class) dataclass in [`multi_append_to_stream()`](#multi-append-to-stream),
* the `expected_state` field of [`StreamStateCheck`](#the-streamstatecheck-class) in [`append_records()`](#append-records).

There are several available options for this value:
- `int` value - Stream position of the last recorded event
- `StreamState.NO_STREAM` - Stream should not exist
- `StreamState.EXISTS` - Stream should exist
- `StreamState.ANY` - No concurrency check

To protect the stream from becoming inconsistent due to conflicting concurrent writers,
use `StreamState.NO_STREAM` when writing to a new stream, and the last stream position
when writing subsequent events. Alternatively, you may require only that the stream has at least one event by using `StreamState.EXISTS`
or fully deactivate concurrency control by using `StreamState.ANY`.

If any of your consistency checks fail, [`append_to_stream()`](#append-to-stream) and
[`multi_append_to_stream()`](#multi-append-to-stream) will raise a `WrongCurrentVersionError`
exception. The [`append_records()`](#append-records) method will raise a `ConsistencyChecksFailedError`
exception.

## Idempotent Append Behavior

KurrentDB's append operations are idempotent, with respect to the event IDs.
So long as the event IDs are unchanged, retrying a successful append operation
will return successfully, without failing due to any [consistency checks](#consistency-checks),
and without appending duplicate events.

Without KurrentDB's idempotent append behavior, when an append request apparently
fails, a client would have to probe the database to determine whether it succeeded.

Please note, KurrentDB does not enforce unique event IDs.

## Append to Stream

The `append_to_stream()` method appends new event records to a named stream.

| Parameter         | Type                                                    | Description                                                                                                                                              |
|-------------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stream_name`     | `str`                                                   | Stream to which the `events` will be appended.                                                                                                           |
| `events`          | `Iterable[NewEvent]`                                    | The [NewEvent](#the-newevent-class) objects to be appended to the stream.                                                                                |
| `current_version` | <nobr><code>int \| StreamState</code></nobr>            | [Consistency check](#consistency-checks) for appending the given `events`.                                                                               |
| `timeout`         | <nobr><code>float \| None</code></nobr>                 | Maximum duration of operation (in seconds).                                                                                                              |
| `credentials`     | <nobr><code>grpc.CallCredentials \| None</code></nobr>  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

The `append_to_stream()` method returns the commit position (`int`) of the last event.
This value can be used by applications to wait until eventually consistent views reflect
newly recorded events.

If the consistency check fails, the `append_to_stream()` method will raise will raise
a `WrongCurrentVersionError` exception.

This method is atomic and [idempotent](#idempotent-append-behavior).

::: info Requires leader
Events can only be written to the "leader" node of a KurrentDB cluster.
:::

### The NewEvent Class

Use the `NewEvent` dataclass with the
[`append_to_stream()`](#append-to-stream) and [`multi_append_to_stream()`](#multi-append-to-stream) methods.

| Field          | Type    | Description                | Default              |
|----------------|---------|----------------------------|----------------------|
| `type`         | `str`   | The type of the event.     |                      |
| `data`         | `bytes` | The content of the event.  |                      |
| `metadata`     | `bytes` | Event metadata.            | `b""`                |
| `content_type` | `str`   | The format of the content. | `"application/json"` |
| `id`           | `UUID`  | A unique ID for the event. | `uuid.uuid4()`       |

### Examples

#### Append to New Stream

The example below appends an event to a new stream `"student-1"`.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import (
    AsyncKurrentDBClient,
    NewEvent,
    StreamState,
)

# Connect to KurrentDB
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
client = KurrentDBClient(connection_string)

# Create a new stream with a new event
client.append_to_stream(
    stream_name="student-1",
    events=[
        NewEvent(
            type="StudentRegistered",
            data=b'{"name": "Greg"}',
        ),
    ],
    current_version=StreamState.NO_STREAM,
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import (
    AsyncKurrentDBClient,
    NewEvent,
    StreamState,
)

# Connect to KurrentDB
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
client = AsyncKurrentDBClient(connection_string)

# Create a new stream with a new event
await client.append_to_stream(
    stream_name="student-1",
    events=[
        NewEvent(
            type="StudentRegistered",
            data=b'{"name": "Greg"}',
        ),
    ],
    current_version=StreamState.NO_STREAM,
)
```
:::

The argument `current_version=StreamState.NO_STREAM` checks that no previous events
have been appended.

#### Append to Existing Stream

The example below appends a second event to stream `"student-1"`.

::: tabs
@tab sync
```python:no-line-numbers
client.append_to_stream(
    stream_name="student-1",
    events=[
        NewEvent(
            type="StudentNameChanged",
            data=b'{"name": "Gregory"}',
        ),
    ],
    current_version=0,
)
```
@tab async
```python:no-line-numbers
await client.append_to_stream(
    stream_name="student-1",
    events=[
        NewEvent(
            type="StudentNameChanged",
            data=b'{"name": "Gregory"}',
        ),
    ],
    current_version=0,
)
```
:::

The argument `current_version=0` checks that exactly one event has been appended to the stream.


#### Wrong Current Version Error

The example below shows consistency checks failing an append operation.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import WrongCurrentVersionError

try:
    client.append_to_stream(
        stream_name="student-1",
        events=[
            NewEvent(
                type="StudentRegistered",
                data=b'{"name": "Greg"}',
            ),
        ],
        current_version=StreamState.NO_STREAM,
    )

except WrongCurrentVersionError as e:
    assert e.stream_name == "student-1"
    assert e.expected_version == StreamState.NO_STREAM
    assert e.actual_version == 1

else:
    raise Exception("Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import WrongCurrentVersionError

try:
    await client.append_to_stream(
        stream_name="student-1",
        events=[
            NewEvent(
                type="StudentRegistered",
                data=b'{"name": "Greg"}',
            ),
        ],
        current_version=StreamState.NO_STREAM,
    )

except WrongCurrentVersionError as e:
    assert e.stream_name == "student-1"
    assert e.expected_version == StreamState.NO_STREAM
    assert e.actual_version == 1

else:
    raise Exception("Shouldn't get here")
```
:::

The `StreamState.NO_STREAM` value is wrong because the stream already
has two events. The append operation fails by raising a `WrongCurrentVersionError`
exception.

## Multi-Append to Stream

The `multi_append_to_stream()` method appends groups of new events to different streams.

| Parameter     | Type                                                    | Description                                                                                                                                              |
|---------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `events`      | `Iterable[NewEvents]`                                   | An iterable of [NewEvents](#the-newevents-class) objects.                                                                                                |
| `timeout`     | <nobr><code>float \| None</code></nobr>                 | Maximum duration of operation (in seconds).                                                                                                              |
| `credentials` | <nobr><code>grpc.CallCredentials \| None</code></nobr>  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). |

The `multi_append_to_stream()` method returns the commit position (`int`) of the last event.
This value can be used by applications to wait until eventually consistent views reflect
newly recorded events.

If more than one `NewEvents` object mentions the same `stream_name`, the `multi_append_to_stream()`
method raises a `MultiAppendToSameStreamError` exception.

If any of the consistency checks fail, the `multi_append_to_stream()` method raises a
`WrongCurrentVersionError` exception.

This method is atomic and [idempotent](#idempotent-append-behavior).

::: info Requires leader
Events can only be written to the "leader" node of a KurrentDB cluster.
:::

::: info KurrentDB 25.1+
The `multi_append_to_stream()` method is supported by KurrentDB 25.1 and later.
:::


### The NewEvents Class

Use the `NewEvents` dataclass with the
[`multi_append_to_stream()`](#multi-append-to-stream) method.

| Field             | Type                                         | Description                                                            |
|-------------------|----------------------------------------------|------------------------------------------------------------------------|
| `stream_name`     | `str`                                        | Stream to which new events will be appended.                           |
| `events`          | `Iterable[NewEvent]`                         | The [`NewEvent`](#the-newevent-class) objects to append to the stream. |
| `current_version` | <nobr><code>int \| StreamState</code></nobr> | [Consistency check](#consistency-checks).     |


### Examples

The example below appends events to two streams.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import NewEvents

client.multi_append_to_stream(
    events=[
        NewEvents(
            stream_name="course-1",
            events=[
                NewEvent(
                    type='CourseCreated',
                    data=b'{"name": "French"}'
                ),
            ],
            current_version=StreamState.NO_STREAM,
        ),
        NewEvents(
            stream_name="course-2",
            events=[
                NewEvent(
                    type='CourseCreated',
                    data=b'{"name": "German"}'
                ),
            ],
            current_version=StreamState.NO_STREAM,
        ),
    ],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import NewEvents

await client.multi_append_to_stream(
    events=[
        NewEvents(
            stream_name="course-1",
            events=[
                NewEvent(
                    type='CourseCreated',
                    data=b'{"name": "French"}'
                ),
            ],
            current_version=StreamState.NO_STREAM,
        ),
        NewEvents(
            stream_name="course-2",
            events=[
                NewEvent(
                    type='CourseCreated',
                    data=b'{"name": "German"}'
                ),
            ],
            current_version=StreamState.NO_STREAM,
        ),
    ],
)
```
:::

The `StreamState.NO_STREAM` values check that no previous events
have been appended.

#### Append to Existing Streams

The example below appends events to two existing streams.

::: tabs
@tab sync
```python:no-line-numbers
client.multi_append_to_stream(
    events=[
        NewEvents(
            stream_name="student-1",
            events=[
                NewEvent(
                    type='StudentJoinedCourse',
                    data=b'{"course_id": "course-1"}'
                ),
            ],
            current_version=1,
        ),
        NewEvents(
            stream_name="course-1",
            events=[
                NewEvent(
                    type='StudentJoinedCourse',
                    data=b'{"student_id": "student-1"}'
                ),
            ],
            current_version=0,
        ),
    ],
)
```
@tab async
```python:no-line-numbers
await client.multi_append_to_stream(
    events=[
        NewEvents(
            stream_name="student-1",
            events=[
                NewEvent(
                    type='StudentJoinedCourse',
                    data=b'{"course_id": "course-1"}'
                ),
            ],
            current_version=1,
        ),
        NewEvents(
            stream_name="course-1",
            events=[
                NewEvent(
                    type='StudentJoinedCourse',
                    data=b'{"student_id": "student-1"}'
                ),
            ],
            current_version=0,
        ),
    ],
)
```
:::

The `current_version=1` value checks exactly two events have been appended to `"student-1"`.

The `current_version=0` value checks exactly one event has been appended to `"course-1"`

## Append Records

The `append_records()` method appends new event records to multiple streams in any order.

| Parameter     | Type                                                          | Description                                                                                                                                              |
|---------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `records`     | `Iterable[NewRecord]`                                         | An iterable of [NewRecord](#the-newrecord-class) objects.                                                                                                |
| `checks`      | <nobr><code>Iterable[StreamStateCheck] \| None</code></nobr>  | An iterable of [StreamStateCheck](#the-streamstatecheck-class) objects.                                                                                  |
| `timeout`     | <nobr><code>float \| None</code></nobr>                       | Maximum duration of operation (in seconds).                                                                                                              |
| `credentials` | <nobr><code>grpc.CallCredentials \| None</code></nobr>        | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). |

The `append_records()` method returns the commit position (`int`) of the last event.
This value can be used by applications to wait until eventually consistent views reflect
newly recorded events.

If one or more of the consistency checks fail, the `append_records()` method raises a `ConsistencyChecksFailedError` that details all
of the consistency check failures.

This method is atomic and [idempotent](#idempotent-append-behavior).

::: info Requires leader
Events can only be written to the "leader" node of a KurrentDB cluster.
:::

::: info KurrentDB 26.1+
The `append_records()` method is supported by KurrentDB 26.1 and later.
:::

### The NewRecord Class

Use the `NewRecord` dataclass with the [`append_records()`](#append-records) method.

| Field          | Type    | Description                                                  | Default              |
|----------------|---------|--------------------------------------------------------------|----------------------|
| `stream_name`  | `str`   | Stream to which this record will be appended.                |                      |
| `type`         | `str`   | The type of the event.                                       |                      |
| `data`         | `bytes` | The content of the event.                                    |                      |
| `metadata`     | `bytes` | Event metadata (has [restrictions](#metadata-restrictions)). | `b""`                |
| `content_type` | `str`   | The format of the content.                                   | `"application/json"` |
| `id`           | `UUID`  | A unique ID for the event.                                   | `uuid.uuid4()`       |


### The StreamStateCheck Class

Use the `StreamStateCheck` dataclass with the [`append_records()`](#append-records) method.

| Field            | Type                                         | Description                                                                                    |
|------------------|----------------------------------------------|------------------------------------------------------------------------------------------------|
| `stream_name`    | `str`                                        | Stream to which this record will be appended.                                                  |
| `expected_state` | <nobr><code>StreamState \| int</code></nobr> | [Consistency check](#consistency-checks) for setting stream metadata. |


### Example

The example below appends events to streams `"student-2"` and `"course-2"`.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import NewRecord, StreamStateCheck

client.append_records(
    records=[
        NewRecord(
            stream_name="student-2",
            type='StudentRegistered',
            data=b'{"name": "Joe"}'
        ),
        NewRecord(
            stream_name="course-2",
            type='StudentJoinedCourse',
            data=b'{"student_id": "student-2"}'
        ),
        NewRecord(
            stream_name="student-2",
            type='StudentJoinedCourse',
            data=b'{"course_id": "course-2"}'
        ),
    ],
    checks=[
        StreamStateCheck(
            stream_name="student-2",
            expected_state=StreamState.NO_STREAM,
        ),
        StreamStateCheck(
            stream_name="course-2",
            expected_state=0,
        ),
    ],
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import NewRecord, StreamStateCheck

await client.append_records(
    records=[
        NewRecord(
            stream_name="student-2",
            type='StudentRegistered',
            data=b'{"name": "Joe"}'
        ),
        NewRecord(
            stream_name="course-2",
            type='StudentJoinedCourse',
            data=b'{"student_id": "student-2"}'
        ),
        NewRecord(
            stream_name="student-2",
            type='StudentJoinedCourse',
            data=b'{"course_id": "course-2"}'
        ),
    ],
    checks=[
        StreamStateCheck(
            stream_name="student-2",
            expected_state=StreamState.NO_STREAM,
        ),
        StreamStateCheck(
            stream_name="course-2",
            expected_state=0,
        ),
    ],
)
```
:::

The `expected_state=StreamState.NO_STREAM` value checks `"student-2"` has no events.

The `expected_state=0` value checks exactly one event has been appended to `"course-2"`

#### Consistency Checks Failed Error

The example below shows consistency checks failing an append operation.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import (
    ConsistencyChecksFailedError,
    ConsistencyCheckFailure,
    StreamStateCheckFailure,
)

try:
    client.append_records(
        records=[
            NewRecord(
                stream_name="student-2",
                type='StudentRegistered',
                data=b'{"name": "Joe"}'
            ),
        ],
        checks=[
            StreamStateCheck(
                stream_name="student-2",
                expected_state=StreamState.NO_STREAM,
            ),
        ],
    )
except ConsistencyChecksFailedError as e:
    assert len(e.failures) == 1
    assert isinstance(e.failures[0], ConsistencyCheckFailure)
    consistency_check_failure = e.failures[0]
    assert consistency_check_failure.check_index == 0
    assert consistency_check_failure.stream_state_failure is not None
    assert consistency_check_failure.stream_state_failure.stream_name == "student-2"
    assert consistency_check_failure.stream_state_failure.expected_state == -1
    assert consistency_check_failure.stream_state_failure.actual_state == 1

else:
    raise Exception("Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import (
    ConsistencyChecksFailedError,
    ConsistencyCheckFailure,
    StreamStateCheckFailure,
)

try:
    await client.append_records(
        records=[
            NewRecord(
                stream_name="student-2",
                type='StudentRegistered',
                data=b'{"name": "Joe"}'
            ),
        ],
        checks=[
            StreamStateCheck(
                stream_name="student-2",
                expected_state=StreamState.NO_STREAM,
            ),
        ],
    )

except ConsistencyChecksFailedError as e:
    assert len(e.failures) == 1
    assert isinstance(e.failures[0], ConsistencyCheckFailure)
    consistency_check_failure = e.failures[0]
    assert consistency_check_failure.check_index == 0
    assert consistency_check_failure.stream_state_failure is not None
    assert consistency_check_failure.stream_state_failure.stream_name == "student-2"
    assert consistency_check_failure.stream_state_failure.expected_state == -1
    assert consistency_check_failure.stream_state_failure.actual_state == 1

else:
    raise Exception("Shouldn't get here")
```
:::

The value `current_version=StreamState.NO_STREAM` is wrong because the stream already
exists. The append operation fails by raising a `ConsistencyChecksFailedError`
exception which details the failure.


## Metadata Restrictions

When appending events with [`multi_append_to_stream()`](#multi-append-to-stream) and
[`append_records()`](#append-records), the `metadata` field of `NewEvent` or
`NewRecord` must be either an empty `bytes` string or a `bytes` string containing
a JSON object whose values are strings.

### Examples

The following metadata values are acceptable.

|   | Description                    | Example         |
|---|--------------------------------|-----------------|
| ✅ | Empty bytes                    | `b""`           |
| ✅ | JSON object with string values | `b'{"a": "1"}'` |


The following metadata values are NOT acceptable and will cause a
`ProgrammingError` exception.

|   | Description                         | Example                                       |
|---|-------------------------------------|-----------------------------------------------|
| ❌ | Random bytes                        | `b'\xf5d\xc5W3^b\xb0(\xf9\x01D\x81\xa7Y\x98'` |
| ❌ | JSON string                         | `b'"abcdef"'`                                 |
| ❌ | JSON object with non-string values  | `b'{"a": 1}'` or `b'{"a": false}'`            |
| ❌ | Nested JSON objects                 | `b'{"a": {}}'`                                |


## Get Stream Metadata

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

## Set Stream Metadata

You can use the `set_stream_metadata()` method to set [stream metadata](@server/features/streams.md#metadata-and-reserved-names).

Provide a `stream_name` argument, a Python `dict` of stream metadata keys and values, and optionally the current version of the stream's metadata stream.

The named stream's metadata will be overwritten with the given `dict`.

| Parameter         | Description                                                                                                                                              | Default           |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------|
| `stream_name`     | Metadata for this stream will be updated.                                                                                                                |                   |
| `metadata`        | A Python `dict` of stream metadata keys and values.                                                                                                      |                   |
| `current_version` | [Consistency check](#consistency-checks) for setting stream metadata.                                                       | `StreamState.ANY` |
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
