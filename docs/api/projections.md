---
order: 8
---

# Projections

This guide describes the Python client methods for working with
[projections](@server/features/projections.md) in KurrentDB.

::: tip
Projections require [event data](./appending-events.md#new-events) to be JSON.
:::

## Introduction

KurrentDB has a [projections subsystem](@server/features/projections.md) that lets
you append new events or link existing events to streams in a reactive manner.

The Python client has twelve methods for working with projections:

* [`create_projection()`](#create-projection)
* [`get_projection_state()`](#get-projection-state)
* [`disable_projection()`](#disable-projection)
* [`update_projection()`](#update-projection)
* [`reset_projection()`](#reset-projection)
* [`enable_projection()`](#enable-projection)
* [`get_projection_statistics()`](#get-projection-statistics)
* [`list_continuous_projection_statistics()`](#list-continuous-projection-statistics)
* [`list_all_projection_statistics()`](#list-all-projection-statistics)
* [`abort_projection()`](#abort-projection)
* [`delete_projection()`](#delete-projection)
* [`restart_projections_subsystem()`](#restart-projections-subsystem)

Let's get started by [connecting to KurrentDB](./getting-started.md#connecting-to-kurrentdb) and [appending new events](./appending-events.md).

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


## Create Projection

Use the `create_projection()` method to create a "continuous" projection with a Javascript "query".

The `emit_enabled` argument must be `True` if the `query` code includes a call to `.emit()` otherwise the projection will not run.

If `track_emitted_streams` is `True` then any emitted emitted streams can be optionally
deleted when a projection is deleted. See [`delete_projection()`](#delete-projection)
for more details.

| Parameter               | Description                                                                                                                                              | Default |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`                  | Name of the projection.                                                                                                                                  |         |
| `query`                 | Javascript projection code, defines what the projection will do.                                                                                         |         |
| `emit_enabled`          | Whether a projection will be able to emit events.                                                                                                        | `False` |
| `track_emitted_streams` | Whether emitted streams are tracked.                                                                                                                     | `False` |
| `timeout`               | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`           | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method returns `None`.

### Example

The Javascript code below will project the stream named `"order-123"`. It will:
* initialise the projection's current state to a Javascript object with "count" and "list" values; and
* for each appended `OrderCreated` event in the projected stream:
  * increment "count" value; and
  * and emit an `Emitted` event to stream `"emitted-order-123"`.

```python:no-line-numbers
projection_query = """
fromStream("order-123")
    .when({
      $init: function(){
        return {
          count: 0,
          list: [null, "2.10", true]
        };
      },
      OrderCreated: function(s,e){
        s.count += 1;
        emit("emitted-order-123", "Emitted", {}, {});
      }
    })
    .outputState()
"""
```

Now let's create a projection that uses this query.

::: tabs
@tab sync
```python:no-line-numbers
client.create_projection(
    name="projection-order-123",
    query=projection_query,
    emit_enabled=True,
    track_emitted_streams=True,
)
```
@tab async
```python:no-line-numbers
await client.create_projection(
    name="projection-order-123",
    query=projection_query,
    emit_enabled=True,
    track_emitted_streams=True,
)
```
:::

## Get Projection State

Use the `get_projection_state()` method to get a projection's current state.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`        | Name of the projection.                                                                                                                                  |         |
| `partition`   | Projection partition (optional).                                                                                                                         | `""`    |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success this method returns a `ProjectionState` object with a `value` attribute
that corresponds to the current state of the projection.

In the example below, the current state of the projection is a Python `dict` that
has a `"count"` value and a `"list"` value. These values correspond to the projection
query and events from the [previous example](#create-projection).

::: tabs
@tab sync
```python:no-line-numbers
from time import sleep

sleep(1)

state = client.get_projection_state("projection-order-123")
assert 1 == state.value["count"]
assert [None, "2.10", True] == state.value["list"]
```
@tab async
```python:no-line-numbers
from time import sleep

sleep(1)

state = await client.get_projection_state("projection-order-123")
assert 1 == state.value["count"]
assert [None, "2.10", True] == state.value["list"]
```
:::


## Disable Projection

Use the `disable_projection()` method to stop a projection processing new events.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`        | Name of the projection.                                                                                                                                  |         |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

The example below stops the `"order-123""` projection.

::: tabs
@tab sync
```python:no-line-numbers
client.disable_projection(name="projection-order-123")
```
@tab async
```python:no-line-numbers
await client.disable_projection(name="projection-order-123")
```
:::

## Update Projection

Use the `update_projection()` method to adjust the projection query.

If `query` includes a call to `.emit()`, the `emit_enabled` argument must be `True`, otherwise the projection will not run.

A projection must be disabled before it can be updated.

| Parameter      | Description                                                                                                                                              | Default |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`         | Name of the projection.                                                                                                                                  |         |
| `query`        | Javascript projection code, defines what the projection will do.                                                                                         |         |
| `emit_enabled` | Whether a projection will be able to emit events.                                                                                                        | `False` |
| `timeout`      | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

::: tabs
@tab sync
```python:no-line-numbers
client.update_projection(
    name="projection-order-123",
    query=projection_query,
    emit_enabled=True,
)
```
@tab async
```python:no-line-numbers
await client.update_projection(
    name="projection-order-123",
    query=projection_query,
    emit_enabled=True,
)
```
:::

## Reset Projection

Use the `reset_projection()` method to reset the current state of a projection.

A projection must be disabled before it can be reset.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`        | Name of the projection.                                                                                                                                  |         |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

::: tabs
@tab sync
```python:no-line-numbers
client.reset_projection(
    name="projection-order-123",
)
```
@tab async
```python:no-line-numbers
await client.reset_projection(
    name="projection-order-123",
)
```
:::

## Enable Projection

Use the `enable_projection()` method to start a projection that has been disabled.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`        | Name of the projection.                                                                                                                                  |         |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

::: tabs
@tab sync
```python:no-line-numbers
client.enable_projection(
    name="projection-order-123",
)
```
@tab async
```python:no-line-numbers
await client.enable_projection(
    name="projection-order-123",
)
```
:::

## Get Projection Statistics

Use the `get_projection_statistics()` method to get statistics for a projection.

| Parameter     | Description                                                                                                                                              | Default  |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name`        | Name of the projection.                                                                                                                                  |          |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`   |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

On success, this method returns a [`ProjectionStatistics`](#the-projectionstatistics-class) object.

::: tabs
@tab sync
```python:no-line-numbers
statistics = client.get_projection_statistics(
    name="projection-order-123",
)
assert "Running" == statistics.status
```
@tab async
```python:no-line-numbers
statistics = await client.get_projection_statistics(
    name="projection-order-123",
)
assert "Running" == statistics.status
```
:::

## List Continuous Projection Statistics

Use the `list_continuous_projection_statistics()` method to get a list of statistics for all continuous projections.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method returns a list of [`ProjectionStatistics`](#the-projectionstatistics-class) objects.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.projections import ProjectionStatistics

statistics = client.list_continuous_projection_statistics()

assert isinstance(statistics, list)
assert 0 < len(statistics)
assert isinstance(statistics[0], ProjectionStatistics)
```
@tab async
```python:no-line-numbers
from kurrentdbclient.projections import ProjectionStatistics

statistics = await client.list_continuous_projection_statistics()

assert isinstance(statistics, list)
assert 0 < len(statistics)
assert isinstance(statistics[0], ProjectionStatistics)
```
:::

## List All Projection Statistics

Use the `list_all_projection_statistics()` method to get a list of statistics for all projections.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method returns a list of [`ProjectionStatistics`](#the-projectionstatistics-class) objects.

::: tabs
@tab sync
```python:no-line-numbers
statistics = client.list_all_projection_statistics()

assert isinstance(statistics, list)
assert 0 < len(statistics)
assert isinstance(statistics[0], ProjectionStatistics)
```
@tab async
```python:no-line-numbers
statistics = await client.list_all_projection_statistics()

assert isinstance(statistics, list)
assert 0 < len(statistics)
assert isinstance(statistics[0], ProjectionStatistics)
```
:::

## Abort Projection

Use the `abort_projection()` method to abort a projection.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`        | Name of the projection.                                                                                                                                  |         |
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

::: tabs
@tab sync
```python:no-line-numbers
client.abort_projection(
    name="projection-order-123",
)
```
@tab async
```python:no-line-numbers
await client.abort_projection(
    name="projection-order-123",
)
```
:::

## Delete Projection

Use the `delete_projection()` method to delete a projection.

A projection must be disabled before it can be deleted.

Attempting to delete a projection that is running will raise an `OperationFailedError` exception.


| Parameter      | Description                                                                                                                                              | Default |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `name`         | Name of the projection.                                                                                                                                  |         |
| `timeout`      | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

::: tabs
@tab sync
```python:no-line-numbers
client.disable_projection(name="projection-order-123")

client.delete_projection(
    "projection-order-123",
    delete_emitted_streams=True,
    delete_state_stream=True,
    delete_checkpoint_stream=True,
)
```
@tab async
```python:no-line-numbers
await client.disable_projection(name="projection-order-123")

await client.delete_projection(
    "projection-order-123",
    delete_emitted_streams=True,
    delete_state_stream=True,
    delete_checkpoint_stream=True,
)
```
:::

On success, this method will return `None`.

## Restart Projections Subsystem

Use the `restart_projections_subsystem()` method to restart the projections subsystem.

| Parameter          | Description                                                                                                                                              | Default |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `timeout`          | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`      | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

On success, this method will return `None`.

::: tabs
@tab sync
```python:no-line-numbers
client.restart_projections_subsystem()
```
@tab async
```python:no-line-numbers
await client.restart_projections_subsystem()
```
:::

## The ProjectionStatistics Class

The `ProjectionStatistics` dataclass is defined with the following fields:

| Field                                      | Type     | Description                                                                                                                                                                                            |
|--------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `core_processing_time`                     | `int`    | The total time, in ms, the projection took to handle events since the last restart.                                                                                                                    |
| `version`                                  | `int`    | This is used internally, the version is increased when the projection is edited or reset.                                                                                                              |
| `epoch`                                    | `int`    | This is used internally, the epoch is increased when the projection is reset.                                                                                                                          |
| `effective_name`                           | `str`    | The name of the projection.                                                                                                                                                                            |
| `writes_in_progress`                       | `int`    | The number of write requests to emitted streams currently in progress, these writes can be batches of events.                                                                                          |
| `reads_in_progress`                        | `int`    | The number of read requests currently in progress.                                                                                                                                                     |
| `partitions_cached`                        | `int`    | The number of cached projection partitions.                                                                                                                                                            |
| `status`                                   | `str`    | A human readable string of the current statuses of the projection (see below).                                                                                                                         |
| `state_reason`                             | `str`    | A human readable string explaining the reason of the current projection state.                                                                                                                         |
| `name`                                     | `str`    | The name of the projection.                                                                                                                                                                            |
| `mode`                                     | `str`    | `Continuous`, `OneTime` , `Transient`                                                                                                                                                                  |
| `position`                                 | `str`    | The position of the last processed event.                                                                                                                                                              |
| `progress`                                 | `float`  | The progress, in %, indicates how far this projection has processed event, in case of a restart this could be -1% or some number. It will be updated as soon as a new event is appended and processed. |
| `last_checkpoint`                          | `str`    | The position of the last checkpoint of this projection.                                                                                                                                                |
| `events_processed_after_restart`           | `int`    | The number of events processed since the last restart of this projection.                                                                                                                              |
| `checkpoint_status`                        | `str`    | A human readable string explaining the current operation performed on the checkpoint: `requested`, `writing`.                                                                                          |
| `buffered_events`                          | `int`    | The number of events in the projection read buffer.                                                                                                                                                    |
| `write_pending_events_before_checkpoint`   | `int`    | The number of events waiting to be appended to emitted streams before the pending checkpoint can be written.                                                                                           |
| `write_pending_events_after_checkpoint`    | `int`    | The number of events to be appended to emitted streams since the last checkpoint.                                                                                                                      |


The `status` string is a combination of the following values.

The first three are the most common one, as the other one are transient values while the projection is initialised or stopped.

| Value               | Description                                                                                                            |
|---------------------|------------------------------------------------------------------------------------------------------------------------|
| Running             | The projection is running and processing events.                                                                       |
| Stopped             | The projection is stopped and is no longer processing new events.                                                      |
| Faulted             | An error occurred in the projection, StateReason will give the fault details, the projection is not processing events. |
| Initial             | This is the initial state, before the projection is fully initialised.                                                 |
| Suspended           | The projection is suspended and will not process events, this happens while stopping the projection.                   |
| LoadStateRequested  | The state of the projection is being retrieved, this happens while the projection is starting.                         |
| StateLoaded         | The state of the projection is loaded, this happens while the projection is starting.                                  |
| Subscribed          | The projection has successfully subscribed to its readers, this happens while the projection is starting.              |
| FaultedStopping     | This happens before the projection is stopped due to an error in the projection.                                       |
| Stopping            | The projection is being stopped.                                                                                       |
| CompletingPhase     | This happens while the projection is stopping.                                                                         |
| PhaseCompleted      | This happens while the projection is stopping.                                                                         |
