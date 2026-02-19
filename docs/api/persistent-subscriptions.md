---
order: 7
---

# Persistent Subscriptions

This guide describes the Python client methods for persistent subscriptions.

## Introduction

Persistent subscriptions are similar to [catch-up subscriptions](./subscriptions.md) with two key differences:

* Persistent subscriptions are defined on the server and checkpoints are maintained
  by the server. This means that clients can reconnect to a persistent subscription
  and automatically receive unprocessed events.
* It's possible to connect more than one consumer to the same persistent subscription.
  The server will send events to all connected consumers, according
  the choice of consumer strategy.

You can read more about persistent subscriptions in the [server documentation](@server/features/persistent-subscriptions.md).

### Creating Subscription Groups

The first step is to create a new persistent subscription group. Admin permissions are required.

The Python clients have two methods for creating a persistent subscription group:

* [`create_subscription_to_stream()`](#create-subscription-to-stream) – create persistent subscription to a stream
* [`create_subscription_to_all()`](#create-subscription-to-all) – create persistent subscription to global transaction log

### Consumer Strategies

When creating a persistent subscription group, you can choose between a number of consumer strategies.

#### DispatchToSingle (default)

Distributes events to a single consumer until the buffer size is reached. After
that, the next consumer is selected in a round-robin style, and the process
repeats.

This option can be seen as a fall-back scenario for high availability, when a
single consumer processes all the events until it reaches its maximum capacity.
When that happens, another consumer takes the load to free up the main consumer
resources.

#### RoundRobin

Distributes events to all consumers evenly. If the buffer size is reached,
the consumer won't receive more events until it acknowledges or not acknowledges
events in its buffer.

This strategy provides equal load balancing between all consumers in the group.

#### Pinned

For use with an indexing projection such as the system by-category projection.

KurrentDB inspects the event for its source stream id, hashing the id to one
of 1024 buckets assigned to individual consumers. When a consumer connects,
it is assigned some existing buckets. When a consumer disconnects, its
buckets are assigned to other consumers. This naively attempts to maintain
a balanced workload.

The main aim of this strategy is to decrease the likelihood of concurrency and
ordering issues while maintaining load balancing. This is **not a guarantee**,
and you should handle the usual ordering and concurrency issues.

### Consuming Events

Consumers read from existing subscription groups. The server distributes events to
consumers according to the subscription group's consumer strategy setting.

The Python clients have two methods for reading from a subscription group:

* [`read_subscription_to_stream()`](#read-subscription-to-stream) – start consuming events from a stream
* [`read_subscription_to_all()`](#read-subscription-to-all) – start consuming events from the global transaction log

These methods return a `PersistentSubscription` object.

The `PersistenceSubscription` class is a Python iterable that returns [RecordedEvent](./reading-events.md#recorded-events) objects,
and which has two methods, `ack()` and `nack()`, for acknowledging and negatively acknowledging received events.

Consumers must use `ack()` or `nack()` to acknowledge or negatively acknowledge received events.

### Acknowledgements

If processing is successful, a consumer should call `ack()` on its `PersistentSubscription` object,
passing in the `RecordedEvent` object that was successfully consumed. This will pick the correct
event ID to send to the server, letting the server know the message has been handled.

| Parameter | Type              | Description                  |
|-----------|-------------------|------------------------------|
| `item`    | `RecordedEvent`   | Successfully consumed events |

#### Negative Acknowledgements

If processing fails for some reason, the consumer should call `nack()` on its `PersistentSubscription` object,
passing in both the `RecordedEvent` and a negative acknowledgement action.

| Parameter | Type            | Description                   |
|-----------|-----------------|-------------------------------|
| `item`    | `RecordedEvent` | Unsuccessfully consumed event |
| `action`  | `str`           | Name of action                |

The negative acknowledgement `action` describes what the server should do with the event.

| Action    | Description                                                           |
|-----------|:----------------------------------------------------------------------|
| `"park"`  | Park the message and do not resend. Put it on poison queue.           |
| `"retry"` | Explicitly retry the message.                                         |
| `"skip"`  | Skip this message do not resend and do not put in poison queue.       |
| `"stop"`  | Stop the subscription.                                                |

### Adjusting Group Settings

You can edit the settings of an existing subscription group while it is running,
you don't need to delete and recreate it to change settings. When you update the
subscription group, it resets itself internally, dropping the connections and
having them reconnect. You must have admin permissions to update a persistent
subscription group.

The Python clients have two methods for adjusting the settings of a persistent subscription group.

* [`update_subscription_to_stream()`](#update-subscription-to-stream) – update settings for subscription to a stream
* [`update_subscription_to_all()`](#update-subscription-to-all) – update settings for subscription to global transaction log

### Getting Subscription Info

The Python clients have three methods for getting information about existing persistent subscriptions.

* [`get_subscription_info()`](#get-subscription-info) – get information about a persistent subscription
* [`list_subscriptions_to_stream()`](#list-subscriptions-to-stream) – get information about all persistent subscriptions to a stream
* [`list_subscriptions()`](#list-subscriptions) – get information about all existing persistent subscriptions


### Deleting Subscription Groups

Remove a subscription group with the delete operation. Like the creating and updating,
you must have admin permissions to delete a persistent subscription group.

The Python clients have one method for deleting a persistent subscription group.

* [`delete_subscription()`](#delete-subscription) – delete subscription group


## Create Subscription to Stream

Use `create_subscription_to_stream()` to create a group for consuming a stream.

The persistent subscription can be created before the steam.

| Parameter              | Description                                                                                                                                              | Default              |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| `group_name`           | Name of persistent subscription group.                                                                                                                   |                      |
| `stream_name`          | Name of stream from which to consume events.                                                                                                             |                      |
| `from_end`             | Whether to start the subscription from the end of the stream.                                                                                            | `False`              |
| `stream_position`      | Position in stream from which to consume events (inclusive).                                                                                             | `None`               |
| `resolve_links`        | Whether the subscription should resolve link events to their linked events.                                                                              | `False`              |
| `consumer_strategy`    | The [consumer strategy](#consumer-strategies) to use for distributing events to client consumers.                                                        | `"DispatchToSingle"` |
| `message_timeout`      | The amount of time (in seconds) after which to consider a message as timed out and retried.                                                              | `30.0`               |
| `max_retry_count`      | The maximum number of retries before a message will be parked.                                                                                           | `10`                 |
| `min_checkpoint_count` | The minimum number of messages to process before a checkpoint may be written.                                                                            | `10`                 |
| `max_checkpoint_count` | The maximum number of messages not checkpoint before forcing a checkpoint.                                                                               | `1000`               |
| `checkpoint_after`     | The maximum duration of time (in seconds) before forcing a checkpoint.                                                                                   | `2.0`                |
| `max_subscriber_count` | The maximum number of subscribers allowed (`0` is unbounded).                                                                                            | `5`                  |
| `live_buffer_size`     | The size of the buffer (in-memory) listening to live messages as they happen before paging occurs.                                                       | `500`                |
| `read_batch_size`      | The number of events read at a time when paging through history.                                                                                         | `200`                |
| `history_buffer_size`  | The number of events to cache when paging through history.                                                                                               | `500`                |
| `extra_statistics`     | Whether to track latency statistics on this subscription.                                                                                                | `False`              |
| `timeout`              | Maximum duration of operation (in seconds).                                                                                                              | `None`               |
| `credentials`          | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`               |

#### Example

Here's an example showing how to create a persistent subscription to a stream.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import KurrentDBClient

# Connect to KurrentDB
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
client = KurrentDBClient(connection_string)

# Create a persistent subscription to a specific stream
client.create_subscription_to_stream(
    group_name="stream-subscription",
    stream_name="order-123"
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import AsyncKurrentDBClient

# Connect to KurrentDB
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
client = AsyncKurrentDBClient(connection_string)

# Create a persistent subscription to a specific stream
await client.create_subscription_to_stream(
    group_name="stream-subscription",
    stream_name="order-123"
)
```
:::


## Create Subscription to All

Use `create_subscription_to_all()` to create a group for consuming the global transaction log.

| Parameter               | Description                                                                                                                                              | Default              |
|-------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|
| `group_name`            | Name of persistent subscription group.                                                                                                                   |                      |
| `from_end`              | Whether to start the subscription from the end of the stream.                                                                                            | `False`              |
| `commit_position`       | Position in global transaction log from which to consume events (inclusive).                                                                             | `None`               |
| `resolve_links`         | Whether the subscription should resolve link events to their linked events.                                                                              | `False`              |
| `filter_exclude`        | [Patterns](./reading-events.md#server-side-filtering) for excluding events.                                                                              | System events        |
| `filter_include`        | [Patterns](./reading-events.md#server-side-filtering) for including events (if set, only matching events will be returned).                              | `()`                 |
| `filter_by_stream_name` | Filter by stream name rather than event type.                                                                                                            | `False`              |
| `consumer_strategy`     | The [consumer strategy](#consumer-strategies) to use for distributing events to client consumers.                                                        | `"DispatchToSingle"` |
| `message_timeout`       | The duration of time (in seconds) after which to consider a message as timed out and retried.                                                            | `30.0`               |
| `max_retry_count`       | The maximum number of retries before a message will be parked.                                                                                           | `10`                 |
| `min_checkpoint_count`  | The minimum number of messages to process before a checkpoint may be written.                                                                            | `10`                 |
| `max_checkpoint_count`  | The maximum number of messages not checkpoint before forcing a checkpoint.                                                                               | `1000`               |
| `checkpoint_after`      | The maximum duration of time (in seconds) before forcing a checkpoint.                                                                                   | `2.0`                |
| `max_subscriber_count`  | The maximum number of subscribers allowed (`0` is unbounded).                                                                                            | `5`                  |
| `live_buffer_size`      | The size of the buffer (in-memory) listening to live messages as they happen before paging occurs.                                                       | `500`                |
| `read_batch_size`       | The number of events read at a time when paging through history.                                                                                         | `200`                |
| `history_buffer_size`   | The number of events to cache when paging through history.                                                                                               | `500`                |
| `extra_statistics`      | Whether to track latency statistics on this subscription.                                                                                                | `False`              |
| `timeout`               | Maximum duration of operation (in seconds).                                                                                                              | `None`               |
| `credentials`           | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`               |


#### Example

Here's an example showing how to create a persistent subscription to the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
client.create_subscription_to_all(
    group_name="transaction-log-subscription",
    filter_include=["OrderCreated"],
)
```
@tab async
```python:no-line-numbers
await client.create_subscription_to_all(
    group_name="transaction-log-subscription",
    filter_include=["OrderCreated"],
)
```
:::

## Read Subscription to Stream

Use `read_subscription_to_stream()` to start consuming events from a stream.

| Parameter            | Description                                                                                                                                              | Default |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `group_name`         | Name of persistent subscription group.                                                                                                                   |         |
| `stream_name`        | Name of stream from which to consume events.                                                                                                             |         |
| `event_buffer_size`  | Number of events in consumer buffer.                                                                                                                     | `150`   |
| `max_ack_batch_size` | Number of acknowledgements before sending all to server.                                                                                                 | `50`    |
| `max_ack_delay`      | Amount of time (in seconds) before sending acknowledgements to server.                                                                                   | `0.2`   |
| `stopping_grace`     | Amount of time (in seconds) to allow server to receive acknowledgements when consumer is stopping.                                                       | `0.2`   |
| `timeout`            | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`        | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

Returns a [`PersistentSubscription`](#consuming-events) object.

#### Example

Here's an example showing how to consume events from a subscription to a stream.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import NewEvent, StreamState

# Create a new stream with a new event
order_created = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)
client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[order_created],
)

# Connect to a persistent subscription for a specific stream
subscription = client.read_subscription_to_stream(
    group_name="stream-subscription",
    stream_name="order-123"
)

# Process events and acknowledge them
for event in subscription:
    try:
        # Process the event
        print(f"Processing event: {event.type}")

        # Acknowledge successful processing
        subscription.ack(event)

    except Exception as e:
        # Handle processing errors
        print(f"Error processing event: {e}")
        subscription.nack(event, action="retry")

    break  # <- so we can continue with the examples
```
@tab async
```python:no-line-numbers
from kurrentdbclient import NewEvent, StreamState

# Create a new stream with a new event
order_created = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)
await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.NO_STREAM,
    events=[order_created],
)

# Connect to a persistent subscription for a specific stream
subscription = await client.read_subscription_to_stream(
    group_name="stream-subscription",
    stream_name="order-123"
)

# Process events and acknowledge them
async for event in subscription:
    try:
        # Process the event
        print(f"Processing event: {event.type}")

        # Acknowledge successful processing
        await subscription.ack(event)

    except Exception as e:
        # Handle processing errors
        print(f"Error processing event: {e}")
        await subscription.nack(event, action="retry")

    break  # <- so we can continue with the examples
```
:::

## Read Subscription to All

Use `read_subscription_to_all()` to start consuming events from the global transaction log.

| Parameter            | Description                                                                                                                                              | Default |
|----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `group_name`         | Name of persistent subscription group.                                                                                                                   |         |
| `event_buffer_size`  | Number of events in consumer buffer.                                                                                                                     | `150`   |
| `max_ack_batch_size` | Number of acknowledgements before sending all to server.                                                                                                 | `50`    |
| `max_ack_delay`      | Amount of time (in seconds) before sending acknowledgements to server.                                                                                   | `0.2`   |
| `stopping_grace`     | Amount of time (in seconds) to allow server to receive acknowledgements when consumer is stopping.                                                       | `0.2`   |
| `timeout`            | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`        | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

Returns a [`PersistentSubscription`](#consuming-events) object.

#### Example

Here's an example showing how to consume events from a subscription to the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
# Connect to a persistent subscription for all events
subscription = client.read_subscription_to_all(
    group_name="transaction-log-subscription"
)

# Process events and acknowledge them
for event in subscription:
    try:
        # Process the event
        print(f"Processing event: {event.type} from stream {event.stream_name}")

        # Acknowledge successful processing
        subscription.ack(event)

    except Exception as e:
        # Handle processing errors
        print(f"Error processing event: {e}")
        subscription.nack(event, action="retry")

    break  # <- so we can continue with the examples
```
@tab async
```python:no-line-numbers
# Connect to a persistent subscription for all events
subscription = await client.read_subscription_to_all(
    group_name="transaction-log-subscription"
)

# Process events and acknowledge them
async for event in subscription:
    try:
        # Process the event
        print(f"Processing event: {event.type} from stream {event.stream_name}")

        # Acknowledge successful processing
        await subscription.ack(event)

    except Exception as e:
        # Handle processing errors
        print(f"Error processing event: {e}")
        await subscription.nack(event, action="retry")

    break  # <- so we can continue with the examples
```
:::

## Update Subscription to Stream

Use `update_subscription_to_stream()` to adjust a group consuming from a stream.

| Parameter              | Description                                                                                                                                              | Default  |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `group_name`           | Name of persistent subscription group.                                                                                                                   |          |
| `stream_name`          | Name of stream from which to consume events.                                                                                                             |          |
| `from_end`             | Whether to start the subscription from the end of the stream.                                                                                            | `None`   |
| `stream_position`      | Position in stream from which to consume events (inclusive).                                                                                             | `None`   |
| `resolve_links`        | Whether the subscription should resolve link events to their linked events.                                                                              | `None`   |
| `consumer_strategy`    | The [consumer strategy](#consumer-strategies) to use for distributing events to client consumers.                                                        | `None`   |
| `message_timeout`      | The amount of time (in seconds) after which to consider a message as timed out and retried.                                                              | `None`   |
| `max_retry_count`      | The maximum number of retries (due to timeout) before a message is considered to be parked.                                                              | `None`   |
| `min_checkpoint_count` | The minimum number of messages to process before a checkpoint may be written.                                                                            | `None`   |
| `max_checkpoint_count` | The maximum number of messages not checkpoint before forcing a checkpoint.                                                                               | `None`   |
| `checkpoint_after`     | The maximum duration of time (in seconds) before forcing a checkpoint.                                                                                   | `None`   |
| `max_subscriber_count` | The maximum number of subscribers allowed (`0` is unbounded).                                                                                            | `None`   |
| `live_buffer_size`     | The size of the buffer (in-memory) listening to live messages as they happen before paging occurs.                                                       | `None`   |
| `read_batch_size`      | The number of events read at a time when paging through history.                                                                                         | `None`   |
| `history_buffer_size`  | The number of events to cache when paging through history.                                                                                               | `None`   |
| `extra_statistics`     | Whether to track latency statistics on this subscription.                                                                                                | `None`   |
| `timeout`              | Maximum duration of operation (in seconds).                                                                                                              | `None`   |
| `credentials`          | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`   |

#### Example

Here's an example showing how to update a persistent subscription to a stream.

::: tabs
@tab sync
```python:no-line-numbers
client.update_subscription_to_stream(
    group_name="stream-subscription",
    stream_name="order-123",
    resolve_links=True,
    min_checkpoint_count=20
)
```
@tab async
```python:no-line-numbers
await client.update_subscription_to_stream(
    group_name="stream-subscription",
    stream_name="order-123",
    resolve_links=True,
    min_checkpoint_count=20
)
```
:::

## Update Subscription to All

Use `update_subscription_to_all()` to adjust a group consuming from the global transaction log.

| Parameter              | Description                                                                                                                                              | Default |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `group_name`           | Name of persistent subscription group.                                                                                                                   |         |
| `from_end`             | Whether to start the subscription from the end of the stream.                                                                                            | `None`  |
| `commit_position`      | Position in global transaction log from which to consume events (inclusive).                                                                             | `None`  |
| `resolve_links`        | Whether the subscription should resolve link events to their linked events.                                                                              | `None`  |
| `consumer_strategy`    | The [consumer strategy](#consumer-strategies) to use for distributing events to client consumers.                                                        | `None`  |
| `message_timeout`      | The amount of time (in seconds) after which to consider a message as timed out and retried.                                                              | `None`  |
| `max_retry_count`      | The maximum number of retries (due to timeout) before a message is considered to be parked.                                                              | `None`  |
| `min_checkpoint_count` | The minimum number of messages to process before a checkpoint may be written.                                                                            | `None`  |
| `max_checkpoint_count` | The maximum number of messages not checkpoint before forcing a checkpoint.                                                                               | `None`  |
| `checkpoint_after`     | The maximum duration of time (in seconds) before forcing a checkpoint.                                                                                   | `None`  |
| `max_subscriber_count` | The maximum number of subscribers allowed (`0` is unbounded).                                                                                            | `None`  |
| `live_buffer_size`     | The size of the buffer (in-memory) listening to live messages as they happen before paging occurs.                                                       | `None`  |
| `read_batch_size`      | The number of events read at a time when paging through history.                                                                                         | `None`  |
| `history_buffer_size`  | The number of events to cache when paging through history.                                                                                               | `None`  |
| `extra_statistics`     | Whether to track latency statistics on this subscription.                                                                                                | `None`  |
| `timeout`              | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`          | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |


Please note, filter settings cannot be updated.

#### Example

Here's an example showing how to update a persistent subscription to the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
client.update_subscription_to_all(
    group_name="transaction-log-subscription",
    resolve_links=True,
    min_checkpoint_count=20
)
```
@tab async
```python:no-line-numbers
await client.update_subscription_to_all(
    group_name="transaction-log-subscription",
    resolve_links=True,
    min_checkpoint_count=20
)
```
:::

## Get Subscription Info

Use `get_subscription_info()` to get a [`SubscriptionInfo`](#subscription-info) object for a persistent subscription group.

| Parameter      | Description                                                                                                                                              | Default |
|----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `group_name`   | Name of persistent subscription group.                                                                                                                   |         |
| `stream_name`  | Name of stream (optional).                                                                                                                               | `None`  |
| `timeout`      | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials`  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |


#### Examples

Here's an example showing how to get subscription information for a subscription to a stream.

::: tabs
@tab sync
```python:no-line-numbers
client.get_subscription_info(
    group_name="stream-subscription",
    stream_name="order-123",
)
```
@tab async
```python:no-line-numbers
await client.get_subscription_info(
    group_name="stream-subscription",
    stream_name="order-123",
)
```
:::

Here's an example showing how to get subscription information for a subscription to the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
client.get_subscription_info(
    group_name="transaction-log-subscription",
)
```
@tab async
```python:no-line-numbers
await client.get_subscription_info(
    group_name="transaction-log-subscription",
)
```
:::


## List Subscriptions to Stream

Use `list_subscriptions_to_stream()` to return a list of [`SubscriptionInfo`](#subscription-info) objects describing persistent subscriptions to a named stream.

| Parameter        | Description                                                                                                                                               | Default |
|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `stream_name`    | Name of stream.                                                                                                                                           |         |
| `timeout`        | Maximum duration of operation (in seconds).                                                                                                               | `None`  |
| `credentials`    | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration).  | `None`  |

#### Example

Here's an example showing how to get information for all persistent subscriptions to a stream.

::: tabs
@tab sync
```python:no-line-numbers
client.list_subscriptions_to_stream(stream_name="order-123")
```
@tab async
```python:no-line-numbers
await client.list_subscriptions_to_stream(stream_name="order-123")
```
:::


## List Subscriptions

Use `list_subscriptions()` to return a list of [`SubscriptionInfo`](#subscription-info) objects describing all existing persistent subscriptions.

| Parameter     | Description                                                                                                                                              | Default |
|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `timeout`     | Maximum duration of operation (in seconds).                                                                                                              | `None`  |
| `credentials` | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration). | `None`  |

#### Example

Here's an example showing how to get information for all existing persistent subscriptions.

::: tabs
@tab sync
```python:no-line-numbers
client.list_subscriptions()
```
@tab async
```python:no-line-numbers
await client.list_subscriptions()
```
:::

## Delete Subscription

Use `delete_subscription()` to permanently delete a persistent subscription group.

| Parameter      | Description                                                                                                                                               | Default |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `group_name`   | Name of persistent subscription group.                                                                                                                    |         |
| `stream_name`  | Name of stream (optional).                                                                                                                                | `None`  |
| `timeout`      | Maximum duration of operation (in seconds).                                                                                                               | `None`  |
| `credentials`  | [Override credentials](./getting-started.md#overriding-user-credentials) derived from [client configuration](./getting-started.md#client-configuration).  | `None`  |

#### Examples

Here's an example showing how to delete a persistent subscription to a stream.

::: tabs
@tab sync
```python:no-line-numbers
client.delete_subscription(
    group_name="stream-subscription",
    stream_name="order-123"
)
```
@tab async
```python:no-line-numbers
await client.delete_subscription(
    group_name="stream-subscription",
    stream_name="order-123"
)
```
:::

Here's an example showing how to delete a persistent subscription to the global transaction log.

::: tabs
@tab sync
```python:no-line-numbers
client.delete_subscription(
    group_name="transaction-log-subscription"
)
```
@tab async
```python:no-line-numbers
await client.delete_subscription(
    group_name="transaction-log-subscription"
)
```
:::

## Subscription Info

The `SubscriptionInfo` objects returned by [`get_subscription_info()`](#get-subscription-info),
[`list_subscriptions_to_stream()`](#list-subscriptions-to-stream), and [`list_subscriptions()`](#list-subscriptions)
have the following fields.

| Field                              | Type                                                                         |
|------------------------------------|------------------------------------------------------------------------------|
| `event_source`                     | `str`                                                                        |
| `group_name`                       | `str`                                                                        |
| `status`                           | `str`                                                                        |
| `average_per_second`               | `int`                                                                        |
| `total_items`                      | `int`                                                                        |
| `count_since_last_measurement`     | `int`                                                                        |
| `last_checkpointed_event_position` | `str`                                                                        |
| `last_known_event_position`        | `str`                                                                        |
| `resolve_links`                    | `bool`                                                                       |
| `start_from`                       | `str`                                                                        |
| `message_timeout`                  | `float`                                                                      |
| `extra_statistics`                 | `bool`                                                                       |
| `max_retry_count`                  | `int`                                                                        |
| `live_buffer_size`                 | `int`                                                                        |
| `history_buffer_size`              | `int`                                                                        |
| `read_batch_size`                  | `int`                                                                        |
| `checkpoint_after`                 | `float`                                                                      |
| `min_checkpoint_count`             | `int`                                                                        |
| `max_checkpoint_count`             | `int`                                                                        |
| `read_buffer_count`                | `int`                                                                        |
| `live_buffer_count`                | `int`                                                                        |
| `retry_buffer_count`               | `int`                                                                        |
| `total_in_flight_messages`         | `int`                                                                        |
| `outstanding_messages_count`       | `int`                                                                        |
| `consumer_strategy`                | `Literal["DispatchToSingle", "RoundRobin", "Pinned", "PinnedByCorrelation"]` |
| `max_subscriber_count`             | `int`                                                                        |
| `parked_message_count`             | `int`                                                                        |
| `connections`                      | `list[ConnectionInfo]`                                                       |

The `ConnectionInfo` objects included in the `connections` field of `SubscriptionInfo` have the following fields.

| Field                          | Type                |
|--------------------------------|---------------------|
| `from_`                        | `str`               |
| `username`                     | `str`               |
| `average_items_per_second`     | `int`               |
| `total_items`                  | `int`               |
| `count_since_last_measurement` | `int`               |
| `observed_measurements`        | `list[Measurement]` |
| `available_slots`              | `int`               |
| `in_flight_messages`           | `int`               |
| `connection_name`              | `str`               |

The `Measurement` objects included in the `observed_measurements` field of `ConnectionInfo` have the following fields.

| Field     | Type    |
|-----------|---------|
| `key`     | `str`   |
| `value`   | `int`   |
