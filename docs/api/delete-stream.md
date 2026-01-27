---
order: 6
---

# Deleting events

This guide describes the Python client methods for deleting streams.

## Introduction


In KurrentDB, you can delete events and streams either partially or
completely. Stream [metadata settings](./appending-events.md#set-stream-metadata)
like `$maxAge` and `$maxCount` help control how long events are
kept or how many events are stored in a stream, but they won't delete the entire
stream.  When you need to fully remove a stream, KurrentDB offers two
options: Soft Delete and Hard Delete.

The Python clients have two methods for deleting streams:

* `delete_stream()` – soft delete
* `tombstone_stream()` – hard delete

## Delete stream

The `delete_stream()` method "soft deletes" a stream in KurrentDB.

Soft delete in KurrentDB allows you to mark a stream for deletion without
completely removing it, so you can still add new events later. While you can do
this through the UI, using code is often better for automating the process,
handling many streams at once, or including custom rules. Code is especially
helpful for large-scale deletions or when you need to integrate soft deletes
into other workflows.

While "soft delete" marks the events for deletion, actual removal occurs during
the next scavenging process. The stream can still be reopened by appending new events.

| Parameter         | Description                                                                                                                                              | Default  |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `stream_name`     | Stream to be "soft deleted".                                                                                                                             |          |
| `current_version` | The [optimistic concurrency control](./appending-events.md#optimistic-concurrency-control) for deleting a stream.                                        |          |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                                              | `None`   |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

If successful, `delete_stream()` returns `None`.

### Example

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

Now let's "soft delete" the stream.

::: tabs
@tab sync
```python:no-line-numbers
# Get the current version of the stream
current_version = client.get_current_version(stream_name="order-123")

# Soft delete the stream
client.delete_stream(
    stream_name="order-123",
    current_version=current_version
)
```
@tab async
```python:no-line-numbers
# Get the current version of the stream
current_version = await client.get_current_version(stream_name="order-123")

# Soft delete the stream
await client.delete_stream(
    stream_name="order-123",
    current_version=current_version
)
```
:::


## Tombstone stream

The `tombstone_stream()` method "hard deletes" a stream in KurrentDB.

Hard delete in KurrentDB permanently removes a stream and its events. While
you can use the HTTP API, code is often better for automating the process,
managing multiple streams, and ensuring precise control. Code is especially
useful when you need to integrate hard delete into larger workflows or apply
specific conditions. Note that when a stream is hard deleted, you cannot reuse
the stream name, it will raise an exception if you try to append to it again.


| Parameter         | Description                                                                                                                                              | Default  |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `stream_name`     | Stream to be "hard deleted".                                                                                                                             |          |
| `current_version` | The [optimistic concurrency control](./appending-events.md#optimistic-concurrency-control) for deleting a stream.                                        |          |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                                              | `None`   |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

If successful, `tombstone_stream()` returns `None`.

### Example

::: tabs
@tab sync
```python:no-line-numbers
# Get the current version of the stream
current_version = client.get_current_version(stream_name="order-123")

# Hard delete (tombstone) the stream
client.tombstone_stream(
    stream_name="order-123",
    current_version=current_version
)

print("Tombstoned stream: order-123")
```
@tab async
```python:no-line-numbers
# Get the current version of the stream
current_version = await client.get_current_version(stream_name="order-123")

# Hard delete (tombstone) the stream
await client.tombstone_stream(
    stream_name="order-123",
    current_version=current_version
)
print("Tombstoned stream: order-123")
```
:::
