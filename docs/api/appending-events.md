---
order: 3
---

# Appending events

This guide describes Python client methods for writing new events and records to KurrentDB.

## Overview

The [Python clients for KurrentDB](getting-started.md#python-clients-for-kurrentdb) have three methods for writing new events:

* [`append_to_stream()`](#append-to-stream) – write a collection of events to a single stream
* [`multi_append_to_stream()`](#multi-append-to-stream) – write collections of events to multiple streams
* [`append_records()`](#append-records) – write events to one or more streams in any order

These methods are atomic and support [idempotent retries](#idempotent-behavior).

The clients also provide methods for managing [stream metadata](@server/features/streams.md#metadata-and-reserved-names):

* [`get_stream_metadata()`](#get-stream-metadata)
* [`set_stream_metadata()`](#set-stream-metadata)

## New event records

KurrentDB organises records into streams within a global transaction log.

The Python clients provide two dataclasses for creating new records:

* Use [`NewEvent`](#the-newevent-class) with [`append_to_stream()`](#append-to-stream) and [`multi_append_to_stream()`](#multi-append-to-stream).
* Use [`NewRecord`](#the-newrecord-class) with the [`append_records()`](#append-records) method.

::: info
[`NewRecord`](#the-newrecord-class) reflects the evolution of the KurrentDB API. Unlike [`NewEvent`](#the-newevent-class),
it has a `stream_name` field. The name "record" reflects the fact that users may store more than
just events in KurrentDB.
:::

The `data` field of [`NewEvent`](#the-newevent-class) and [`NewRecord`](#the-newrecord-class) is a
Python `bytes` object that contains the record payload. If you serialize your events as JSON,
you can take advantage of KurrentDB features such as projections. However, you are free
to use any serialization format that suits your requirements.

The `content_type` field indicates whether `data` contains JSON or another binary format.
You can choose between `"application/json"` and `"application/octet-stream"`. For
example, if you use Message Pack or Protobuf, or if you compress or encrypt JSON data
before writing it, use `"application/octet-stream"`. The default value is `"application/json"`.

The `metadata` field of is a Python `bytes` object that contains additional information
about the record, such as correlation IDs, timestamps, access information, or other
application-specific values. Metadata is stored separately from the payload.
See [metadata restrictions](#metadata-restrictions) when using [`multi_append_to_stream()`](#multi-append-to-stream) and [`append_records()`](#append-records).

The `id` field is a `UUID` that can uniquely identify the record. KurrentDB does not
enforce uniqueness of record IDs, but they are used to support [idempotent append behavior](#idempotent-behavior).
By default, a new version 4 UUID is generated.

When a record is written, KurrentDB assigns two sequence numbers:

* **Commit position** – The position of the record in the global transaction log.
* **Stream position** – The position of the record within its stream.

Each stream has a unique name. Stream positions are zero-based and gapless.
The position in a stream is position `0`, the second is position `1`, the
third is position `2`, and so on. Positions in KurrentDB's global transaction log
are not gapless.


## Consistency checks

When writing to a stream, you can use consistency checks to ensure that the stream is
in the expected state before events are appended.

Consistency checks are specified using:
* the `current_version` argument of [`append_to_stream()`](#the-newevent-class),
* the `current_version` field of the [`NewEvents`](#the-newevents-class) dataclass in [`multi_append_to_stream()`](#multi-append-to-stream), or
* the `expected_state` field of [`StreamStateCheck`](#the-streamstatecheck-class) in [`append_records()`](#append-records).

The following consistency checks are available:
- `int` - the stream's current version must match the specified value,
- `StreamState.NO_STREAM` - stream must not exist,
- `StreamState.EXISTS` - stream must exist, and
- `StreamState.ANY` - no consistency check is performed.

To prevent concurrent writers from producing unexpected results, use `StreamState.NO_STREAM` when
creating a new stream and the stream's current version when writing subsequent events.
Alternatively, use `StreamState.EXISTS` to require that the stream already exists.
Consistency checks can be diabled by using `StreamState.ANY`.

If a consistency check fails, [`append_to_stream()`](#append-to-stream) and
[`multi_append_to_stream()`](#multi-append-to-stream) will raise a `WrongCurrentVersionError`. The
[`append_records()`](#append-records) method raises a `ConsistencyChecksFailedError`.

## Idempotent behavior

KurrentDB append operations are idempotent. If a successful append operation is retried
with the same [consistency checks](#consistency-checks) and the same event IDs, the
operation will succeed without appending duplicate events.

This allows clients to safely retry append operations when the outcome is uncertain,
for example when a network failure occurs after the operation has been committed but
before the response has been received.

KurrentDB does not enforce globally unique event IDs. Event IDs are used only to detect
retries of the same append operation.

## Append to stream

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

This method is atomic and [idempotent](#idempotent-behavior).

::: info Requires leader
Events can only be written to the "leader" node of a KurrentDB cluster.
:::

### The NewEvent class

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

#### Append to new stream

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

#### Append to existing stream

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


#### Wrong current version error

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

## Multi-append to stream

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

This method is atomic and [idempotent](#idempotent-behavior).

::: info Requires leader
Events can only be written to the "leader" node of a KurrentDB cluster.
:::

::: info KurrentDB 25.1+
The `multi_append_to_stream()` method is supported by KurrentDB 25.1 and later.
:::


### The NewEvents class

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

#### Append to existing streams

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

## Append records

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

This method is atomic and [idempotent](#idempotent-behavior).

::: info Requires leader
Events can only be written to the "leader" node of a KurrentDB cluster.
:::

::: info KurrentDB 26.1+
The `append_records()` method is supported by KurrentDB 26.1 and later.
:::

### The NewRecord class

Use the `NewRecord` dataclass with the [`append_records()`](#append-records) method.

| Field          | Type    | Description                                                  | Default              |
|----------------|---------|--------------------------------------------------------------|----------------------|
| `stream_name`  | `str`   | Stream to which this record will be appended.                |                      |
| `type`         | `str`   | The type of the event.                                       |                      |
| `data`         | `bytes` | The content of the event.                                    |                      |
| `metadata`     | `bytes` | Event metadata (has [restrictions](#metadata-restrictions)). | `b""`                |
| `content_type` | `str`   | The format of the content.                                   | `"application/json"` |
| `id`           | `UUID`  | A unique ID for the event.                                   | `uuid.uuid4()`       |


### The StreamStateCheck class

Use the `StreamStateCheck` dataclass with the [`append_records()`](#append-records) method.

| Field            | Type                                         | Description                                                                                    | Default              |
|------------------|----------------------------------------------|------------------------------------------------------------------------------------------------|----------------------|
| `stream_name`    | `str`                                        | Stream to which this record will be appended.                                                  |                      |
| `expected_state` | <nobr><code>StreamState \| int</code></nobr> | [Consistency check](#consistency-checks) for setting stream metadata. |                      |


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

#### Consistency checks failed error

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
    failure = e.failures[0]
    assert isinstance(failure, ConsistencyCheckFailure)
    assert failure.check_index == 0
    assert failure.stream_state_failure is not None
    assert failure.stream_state_failure.stream_name == "student-2"
    assert failure.stream_state_failure.expected_state == -1
    assert failure.stream_state_failure.actual_state == 1
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
    failure = e.failures[0]
    assert isinstance(failure, ConsistencyCheckFailure)
    assert failure.check_index == 0
    assert failure.stream_state_failure is not None
    assert failure.stream_state_failure.stream_name == "student-2"
    assert failure.stream_state_failure.expected_state == -1
    assert failure.stream_state_failure.actual_state == 1
else:
    raise Exception("Shouldn't get here")
```
:::

The value `current_version=StreamState.NO_STREAM` is wrong because the stream already
exists. The append operation fails by raising a `ConsistencyChecksFailedError`
exception which details the failure.


## Metadata restrictions

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


## Get stream metadata

The `get_stream_metadata()` method gets [stream metadata](@server/features/streams.md#metadata-and-reserved-names) for a particular stream.

| Parameter     | Type                                                    | Description                                                                                                                                              |
|---------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stream_name` | `str`                                                   | The name of a stream the metadata applies to (don't use `$$` prefix here).                                                                               |
| `timeout`     | <nobr><code>float \| None</code></nobr>                 | Maximum duration of operation (in seconds).                                                                                                              |
| `credentials` | <nobr><code>grpc.CallCredentials \| None</code></nobr>  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). |

The `get_stream_metadata()` method returns a Python `dict` of metadata keys and values for the named stream, along with the current version of the stream's metadata stream.
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

The `set_stream_metadata()` method sets [stream metadata](@server/features/streams.md#metadata-and-reserved-names) for a particular stream.
If the named stream does not exist, the metadata will be set anyway. This allows streams
to be configured before they are used.

| Parameter         | Type                                                    | Description                                                                                                                                              |
|-------------------|---------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| `stream_name`     | `str`                                                   | The name of a stream the metadata applies to (don't use `$$` prefix here).                                                                               |
| `metadata`        | `dict[str, Any]`                                        | A Python `dict` of stream metadata keys and values. Needs to be serializable by `json.dumps()`.                                                          |
| `current_version` | <nobr><code>int \| StreamState</code></nobr>            | [Consistency check](#consistency-checks) for setting stream metadata.                                                                                    |
| `timeout`         | <nobr><code>float \| None</code></nobr>                 | Maximum duration of operation (in seconds).                                                                                                              |
| `credentials`     | <nobr><code>grpc.CallCredentials \| None</code></nobr>  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). |

The `set_stream_metadata()` method returns `None`.

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
