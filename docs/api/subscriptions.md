---
order: 6
---

# Catch-up subscriptions

This guide describes the Python client methods for catch-up subscriptions.

## Introduction

Catchup-subscriptions are like the responses from the read methods, with one
useful difference: they return [already-recorded events](./reading-events.md#recorded-events), and then
continue as new events are subsequently recorded.

You can subscribe to individual streams, to the global transaction log, and to a secondary indexes.

The Python clients for KurrentDB have three methods for catch-up subscriptions.

* [`subscribe_to_stream()`](#subscribe-to-stream) – returns a catch-up subscription to a stream
* [`subscribe_to_all()`](#subscribe-to-all) - returns a catch-up subscription to global transaction log
* [`subscribe_to_index()`](#subscribe-to-index) – returns a catch-up subscription to a secondary index

## Subscribe to stream

The `subscribe_to_stream()` method returns a catch-up subscription to a stream.

The only required argument is the name of a stream.

You can subscribe to all the events in a stream, or a sample of the events from the named stream,
optionally starting after a specific stream position or the end of the stream.

| Parameter             | Description                                                                                                                                              | Default |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `stream_name`         | Stream from which events will be read.                                                                                                                   |         |
| `stream_position`     | Position after which to start reading events.                                                                                                            | `None`  |
| `from_end`            | Read from the end of the stream (new events only).                                                                                                       | `False` |
| `resolve_links`       | Activate resolution of "link events".                                                                                                                    | `False` |
| `include_caught_up`   | Receive "caught up" messages when iterating the response.                                                                                                | `False` |
| `include_fell_behind` | Receive "fell behind" messages when iterating the response.                                                                                              | `False` |
| `timeout`             | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`         | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, `subscribe_to_stream()` returns an iterable of `RecordedEvent` objects.

Please note, a `NotFoundError` exception will be raised if the stream does not exist.


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

# Append the first event to a new stream
commit_position = client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event1],
)

# Append second and third event to the same stream.
client.append_to_stream(
    stream_name="order-123",
    current_version=0,
    events=[event2, event3],
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

# Append the first event to a new stream
commit_position = await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[event1],
)

# Append second and third event to the same stream.
await client.append_to_stream(
    stream_name="order-123",
    current_version=0,
    events=[event2, event3],
)
```
:::

### Basic subscription

The simplest way to subscribe to a stream is to supply a `stream_name` argument.

::: tabs
@tab sync
```python:no-line-numbers
# Subscribe to all events in a stream
subscription = client.subscribe_to_stream(stream_name="order-123")

# Iterate through the subscription with a 'for' loop
for event in subscription:
    assert event.stream_position == 0
    assert event.id == event1.id
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Subscribe to all events in a stream
subscription = await client.subscribe_to_stream(stream_name="order-123")

# Iterate through the subscription with an async 'for' loop
async for event in subscription:
    assert event.stream_position == 0
    assert event.id == event1.id
    break  # <-- so we can continue with the examples
```
:::

### After stream position

Specifying a `stream_position` argument will get events after that position.

::: tabs
@tab sync
```python:no-line-numbers
# Get events after a specific stream position
for event in client.subscribe_to_stream(
    stream_name="order-123",
    stream_position=1,
):
    assert event.stream_position == 2
    assert event.id == event3.id
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Get events after a specific stream position
async for event in await client.subscribe_to_stream(
    stream_name="order-123",
    stream_position=1,
):
    assert event.stream_position == 2
    assert event.id == event3.id
    break  # <-- so we can continue with the examples
```
:::

### From end of stream

Here's an example of subscribing from the end of a stream for "live events" only.

::: tabs
@tab sync
```python:no-line-numbers
subscription = client.subscribe_to_stream(
    stream_name="order-123",
    from_end=True,
)
```
@tab async
```python:no-line-numbers
subscription = await client.subscribe_to_stream(
    stream_name="order-123",
    from_end=True,
)
```
:::

### Resolving link events

When you subscribe to a stream with link events (e.g., category streams), set `resolve_links` to `True`.

::: tabs
@tab sync
```python:no-line-numbers
for event in client.subscribe_to_stream(
    stream_name="$et-OrderCreated",
    resolve_links=True
):
    assert event.type == "OrderCreated"
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
async for event in await client.subscribe_to_stream(
    stream_name="$et-OrderCreated",
    resolve_links=True
):
    assert event.type == "OrderCreated"
    break  # <-- so we can continue with the examples
```
:::

Link events point to events in other streams in KurrentDB. These are
generally created by projections such as the by-event-type projection which
links events of the same event type into the same stream. This makes it easy
to look up all events of a specific type. However, it may be faster to use
a [filtered subscription](#subscribe-to-all) for a specific type or stream name prefix
than subscribing to the corresponding system projection.


### Stream not found error

Subscribing to a stream that doesn't exist will raise a `NotFoundError` exception.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    client.subscribe_to_stream(stream_name="not-a-stream")
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
@tab async
```python:no-line-numbers
from kurrentdbclient.exceptions import NotFoundError

try:
    await client.subscribe_to_stream(stream_name="not-a-stream")
except NotFoundError:
    print("Success: Stream does not exist")
except Exception as e:
    print(f"Shouldn't get here")
```
:::


## Subscribe to all

The `subscribe_to_all()` method returns a catch-up subscription to the global transaction log.

A catch-up subscription to the global transaction log will return all events in
chronological order. You can start after a particular commit position, or from
the end of the log so that only new events are received. You can also filter
events by type string or by stream name.

See notes on [filtering the global transaction log](./reading-events.md#server-side-filtering).

| Parameter               | Description                                                                                                                                              | Default       |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `commit_position`       | Position after which to start reading events.                                                                                                            | `None`        |
| `from_end`              | Read from the end of the database (new events only).                                                                                                     | `False`       |
| `resolve_links`         | Activate resolution of "link events".                                                                                                                    | `False`       |
| `filter_exclude`        | [Patterns](./reading-events.md#server-side-filtering) for excluding events.                                                                                                                           | System events |
| `filter_include`        | [Patterns](./reading-events.md#server-side-filtering) for including events (if set, only matching events will be returned).                                                                           | `()`          |
| `filter_by_stream_name` | Filter by stream name (default is to filter by event type).                                                                                              | `False`       |
| `include_caught_up`     | Receive "caught up" messages when iterating the response.                                                                                                | `False`       |
| `include_fell_behind`   | Receive "fell behind" messages when iterating the response.                                                                                              | `False`       |
| `timeout`               | Maximum duration of operation (in seconds).                                                                                                              | `None`        |
| `credentials`           | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`        |

On success, `subscribe_to_all()` returns an iterable of `RecordedEvent` objects.

### Examples

Let's see how to use `subscribe_to_all()` by looking at some examples.

### Basic subscription

::: tabs
@tab sync
```python:no-line-numbers
# Subscribe to all events in global transaction log
subscription = client.subscribe_to_all()

# Iterate through the subscription with a 'for' loop
for event in subscription:
    print(f"Event: {event.type} at position {event.commit_position}")
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Subscribe to all events in global transaction log
subscription = await client.subscribe_to_all()

# Iterate through the subscription with an async 'for' loop
async for event in subscription:
    print(f"Event: {event.type} at position {event.commit_position}")
    break  # <-- so we can continue with the examples
```
:::

### After commit position

Specifying a `commit_position` argument will get events after that position in the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
# Get events after a specific commit position
for event in client.subscribe_to_all(
    commit_position=commit_position,
):
    assert event.id == event2.id
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Get events after a specific commit position
async for event in await client.subscribe_to_all(
    commit_position=commit_position,
):
    assert event.id == event2.id
    break  # <-- so we can continue with the examples
```
:::

### Live events only

Here's an example of subscribing from the end of the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
# Get events after a specific stream position
subscription = client.subscribe_to_all(from_end=True)
```
@tab async
```python:no-line-numbers
# Get events after a specific stream position
subscription = await client.subscribe_to_all(from_end=True)
```
:::

### Resolving link events

KurrentDB projections can create "link events" that are pointers to events you have appended to a stream.

Set `resolve_links=True` so that KurrentDB will resolve the "link events" and return the linked events.

::: tabs
@tab sync
```python:no-line-numbers
subscription = client.subscribe_to_all(resolve_links=True)
```
@tab async
```python:no-line-numbers
subscription = await client.subscribe_to_all(resolve_links=True)
```
:::


### Filtering by event type

Here's an example of filtering for certain event types.

::: tabs
@tab sync
```python:no-line-numbers
for event in client.subscribe_to_all(
    filter_include=["OrderCreated", "OrderUpdated"],
):
    assert event.type in ["OrderCreated", "OrderUpdated"]
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
async for event in await client.subscribe_to_all(
    filter_include=["OrderCreated", "OrderUpdated"],
):
    assert event.type in ["OrderCreated", "OrderUpdated"]
    break  # <-- so we can continue with the examples
```
:::

### Filtering by stream name

Here's an example of filtering for a stream category.

::: tabs
@tab sync
```python:no-line-numbers
# Filter by stream name prefix
for event in client.subscribe_to_all(
    filter_include=["order-.*"],
    filter_by_stream_name=True
):
    assert event.stream_name.startswith("order")
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Filter by stream name prefix
async for event in await client.subscribe_to_all(
    filter_include=['order-.*'],
    filter_by_stream_name=True
):
    assert event.stream_name.startswith("order")
    break  # <-- so we can continue with the examples
```
:::

### Checkpointing

When a catch-up subscription to the global transaction log is used to process events,
you can checkpoint progress by recording the commit position of the last processed event.

If you record commit positions in the same atomic database transaction as the results
of processing an event, and with a uniqueness constraint, and you resume using the last
recorded position, the processing of events from the global transaction log will
immediately have "exactly once" semantics.

If you are filtering the subscription, you can set `include_checkpoints=True` to cause
KurrentDB occasionally to send the commit positions of events that have been excluded
from the catch-up subscription, so that progress across large gaps can also be checkpointed.
These emerge from the catch-up subscription as `Checkpoint` objects. Please note, occasionally
KurrentDB will send a checkpoint with the same commit position as a recorded event, which means
you must check first to see if an item is a `RecordedEvent` before recording the commit position
of a `Checkpoint` object.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import Checkpoint, RecordedEvent

def process_events_with_checkpointing(client, projection):
    # Get the last checkpoint
    last_commit_position = projection.get_last_checkpoint()

    # Subscribe using the last checkpoint
    subscription = client.subscribe_to_all(
        commit_position=last_commit_position,
        include_checkpoints=True
    )

    for item in subscription:
        if type(item) is RecordedEvent:
            # Regular event processing
            new_state = {"key": "value"}
            # Record commit position with new state
            projection.update_state(new_state, item.commit_position)

        elif type(item) is Checkpoint:
            # Record commit position
            projection.save_checkpoint(item.commit_position)

        break  # <-- so we can continue with the examples



class Projection:
    def __init__(self):
        self._last_checkpoint = None
        self._current_state = {}

    def get_last_checkpoint(self):
        return self._last_checkpoint

    def save_checkpoint(self, checkpoint):
        # Only update if checkpoint is greater than last recorded.
        if (
            self._last_checkpoint is None
            or checkpoint > self._last_checkpoint
        ):
            self._last_checkpoint = checkpoint

    def update_state(self, state, checkpoint):
        # Only update if checkpoint is greater than last recorded.
        if (
            self._last_checkpoint is not None
            and checkpoint <= self._last_checkpoint
        ):
            msg = f"Checkpoint conflict: {checkpoint} <= {self._last_checkpoint}"
            raise ValueError(msg)
        self._last_checkpoint = checkpoint
        self._current_state = state


process_events_with_checkpointing(client, Projection())


```
@tab async
```python:no-line-numbers
from kurrentdbclient import Checkpoint, RecordedEvent

async def process_events_with_checkpointing(client, projection):
    # Get the last checkpoint
    last_commit_position = await projection.get_last_checkpoint()

    # Subscribe using the last checkpoint
    subscription = await client.subscribe_to_all(
        commit_position=last_commit_position,
        include_checkpoints=True
    )

    async for item in subscription:
        if type(item) is RecordedEvent:
            # Regular event processing
            new_state = {"key": "value"}
            # Record commit position with new state
            await projection.update_state(new_state, item.commit_position)

        elif type(item) is Checkpoint:
            # Record commit position
            await projection.save_checkpoint(item.commit_position)

        break  # <-- so we can continue with the examples


class Projection:
    def __init__(self):
        self._last_checkpoint = None
        self._current_state = {}

    async def get_last_checkpoint(self):
        return self._last_checkpoint

    async def save_checkpoint(self, checkpoint):
        # Only update if checkpoint is greater than last recorded.
        if (
            self._last_checkpoint is None
            or checkpoint > self._last_checkpoint
        ):
            self._last_checkpoint = checkpoint

    async def update_state(self, state, checkpoint):
        # Only update if checkpoint is greater than last recorded.
        if (
            self._last_checkpoint is not None
            and checkpoint <= self._last_checkpoint
        ):
            msg = f"Checkpoint conflict: {checkpoint} <= {self._last_checkpoint}"
            raise ValueError(msg)
        self._last_checkpoint = checkpoint
        self._current_state = state


await process_events_with_checkpointing(client, Projection())
```
:::

The same principles can be applied when processing events from a stream or a secondary index.

## Subscribe to index

::: info
Supported by KurrentDB 25.1 and later.
:::

The `subscribe_to_index()` method returns a catch-up subscription to a secondary index.

You can subscribe to all the events in a secondary index, optionally starting after a commit position.

| Parameter         | Description                                                                                                                                              | Default |
|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `index_name`      | Name of secondary index (`"$idx-"` prefix is optional).                                                                                                  |         |
| `commit_position` | Position after which to start reading events.                                                                                                            | `None`  |
| `timeout`         | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`     | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, `subscribe_to_index()` returns an iterable of `RecordedEvent` objects.

### Examples

Let's see how to use `subscribe_to_index()` by looking at some examples.

### Basic subscription

::: tabs
@tab sync
```python:no-line-numbers
# Subscribe to all events in a secondary index
subscription = client.subscribe_to_index(index_name="et-OrderCreated")

# Iterate through the subscription with a 'for' loop
for event in subscription:
    assert event.type == "OrderCreated"
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Subscribe to all events in a secondary index
subscription = await client.subscribe_to_index(index_name="et-OrderCreated")

# Iterate through the subscription with a 'for' loop
async for event in subscription:
    assert event.type == "OrderCreated"
    break  # <-- so we can continue with the examples
```
:::


### After commit position

::: tabs
@tab sync
```python:no-line-numbers
# Subscribe to all events in a secondary index
subscription = client.subscribe_to_index(
    index_name="et-OrderUpdated",
    commit_position=commit_position,
)

# Iterate through the subscription with a 'for' loop
for event in subscription:
    assert event.type == "OrderUpdated"
    break  # <-- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Subscribe to all events in a secondary index
subscription = await client.subscribe_to_index(
    index_name="et-OrderUpdated",
    commit_position=commit_position,
)

# Iterate through the subscription with a 'for' loop
async for event in subscription:
    assert event.type == "OrderUpdated"
    break  # <-- so we can continue with the examples
```
:::


## Handling dropped subscriptions

An application which hosts the subscription can go offline for some time for
different reasons. It could be a crash, infrastructure failure, or a new version
deployment. You should implement retry logic to handle such cases.

::: tabs
@tab sync
```python:no-line-numbers
import time
from kurrentdbclient.exceptions import ConsumerTooSlowError, GrpcError

projection = Projection()
retries = 5

while True:
    try:
        process_events_with_checkpointing(client, projection)
        break  # <-- so we can continue with the examples
    except (ConsumerTooSlowError, GrpcError):
        if retries <= 0:
            raise
        retries -= 1
        time.sleep(5)
        continue
```
@tab async
```python:no-line-numbers
import time
from kurrentdbclient.exceptions import ConsumerTooSlowError, GrpcError

projection = Projection()
retries = 5

while True:
    try:
        await process_events_with_checkpointing(client, projection)
        break  # <-- so we can continue with the examples
    except (ConsumerTooSlowError, GrpcError):
        if retries <= 0:
            raise
        retries -= 1
        time.sleep(5)
        continue
```
:::

## Handling subscription state changes

::: info EventStoreDB 23.10.0+
This feature requires EventStoreDB version 23.10.0 or later.
:::

When a subscription processes historical events and reaches the end of the
stream, it transitions from "catching up". You can detect this
transition by using the `include_caught_up` parameter
of the catch-up subscription methods.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import CaughtUp

# Subscribe with caught-up notifications
subscription = client.subscribe_to_stream(
    stream_name="order-123",
    include_caught_up=True,
)

for item in subscription:
    if type(item) is CaughtUp:
        print("Subscription has caught up to live events")
        break  # <-- so we can continue with the examples
    else:
        # Regular event processing
        print(f"Processing event: {item.type}")

```
@tab async
```python:no-line-numbers
from kurrentdbclient import CaughtUp

# Subscribe with caught-up notifications
subscription = await client.subscribe_to_stream(
    stream_name="order-123",
    include_caught_up=True,
)

async for item in subscription:
    if type(item) is CaughtUp:
        print("Subscription has caught up to live events")
        break  # <-- so we can continue with the examples
    else:
        # Regular event processing
        print(f"Processing event: {item.type}")

```
:::

::: tip
The caught-up notification is only emitted when transitioning from catching up to live mode. If you subscribe from the end of a stream, you'll immediately be in live mode and this notification will be sent right away.
:::
