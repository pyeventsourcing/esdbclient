---
order: 4
---

# Catch-up Subscriptions

Subscriptions allow you to subscribe to a stream and receive notifications about new events added to the stream. You provide an event handler and an optional starting point to the subscription. The handler is called for each event from the starting point onward.

If events already exist, the handler will be called for each event one by one until it reaches the end of the stream. The server will then notify the handler whenever a new event appears.

:::tip
Check the [Getting Started](getting-started.md) guide to learn how to configure and use the client SDK.
:::

## Basic Subscriptions

You can subscribe to a single stream or to all events in the database using catch-up subscriptions.

**Stream subscription:**

```python
from kurrentdbclient import KurrentDBClient

# Subscribe to a specific stream
subscription = client.subscribe_to_stream(stream_name="order-123")

# Iterate over events as they arrive
for event in subscription:
    stream_name = event.stream_name
    stream_position = event.stream_position
    
    # Handle the event
    print(f"Received event: {event.type} at position {stream_position}")
    
    # Stop condition (for example purposes)
    if some_condition:
        subscription.stop()
        break
```

**Subscribe to all events:**

```python
# Subscribe to all events in the database
subscription = client.subscribe_to_all()

# Iterate over events as they arrive
for event in subscription:
    stream_name = event.stream_name
    commit_position = event.commit_position
    
    # Handle the event
    print(f"Received event: {event.type} from stream {stream_name}")
    
    # Stop condition (for example purposes)
    if some_condition:
        subscription.stop()
        break
```

When you subscribe to a stream with link events (e.g., category streams), set `resolve_links` to `True`.

```python
# Subscribe with link resolution
subscription = client.subscribe_to_stream(
    stream_name="$ce-order", 
    resolve_links=True
)
```

## Subscribing from a Position

Both stream and all-events subscriptions accept a starting position if you want to read from a specific point onward. If events already exist at the position you subscribe to, they will be read on the server side and sent to the subscription.

Once caught up, the server will push any new events received on the streams to the client. There is no difference between catching up and live on the client side.

::: warning
The positions provided to the subscriptions are exclusive. You will only receive the next event after the subscribed position.
:::

**Stream from specific position:**

To subscribe to a stream from a specific position, provide a stream position (integer representing the stream position):

```python
# Subscribe from a specific stream position
subscription = client.subscribe_to_stream(
    stream_name="order-123",
    stream_position=20
)
```

**All events from specific position:**

For all events, provide a commit position:

```python
# Subscribe to all events from a specific commit position
subscription = client.subscribe_to_all(commit_position=1056)
```

**Live updates only:**

Subscribe to the end of a stream to get only new events:

```python
# Stream - subscribe from the end
subscription = client.subscribe_to_stream(
    stream_name="order-123",
    from_end=True
)

# All events - subscribe from the end
subscription = client.subscribe_to_all(from_end=True)
```

## Resolving link events

Link events point to events in other streams in KurrentDB. These are
generally created by projections such as the by-event-type projection which
links events of the same event type into the same stream. This makes it easier
to look up all events of a specific type.

::: tip
[Filtered subscriptions](subscriptions.md#server-side-filtering) make it easier
and faster to subscribe to all events of a specific type or matching a prefix.
:::

When reading a stream you can specify whether to resolve links. By default,
link events are not resolved. You can change this behaviour by setting the
`resolve_links` parameter to `True`:

```python
# Subscribe with link resolution enabled
subscription = client.subscribe_to_stream(
    stream_name="$ce-order",
    resolve_links=True
)
```

## Subscription Drops and Recovery

When a subscription stops or experiences an error, it will be dropped. You can handle
this by catching exceptions and implementing retry logic in your application.

The subscription can drop for various reasons, including network issues, server
problems, or if the subscription is too slow to process events.

### Handling Dropped Subscriptions

An application which hosts the subscription can go offline for some time for
different reasons. It could be a crash, infrastructure failure, or a new version
deployment. You should implement retry logic to handle such cases.

```python
import time
from kurrentdbclient.exceptions import StreamNotFoundError

def create_subscription_with_retry(client, stream_name, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            subscription = client.subscribe_to_stream(stream_name)
            return subscription
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                raise e
            print(f"Subscription failed, retrying in 5 seconds... ({retries}/{max_retries})")
            time.sleep(5)

# Usage
subscription = create_subscription_with_retry(client, "order-123")
```

## Handling Subscription State Changes

::: info EventStoreDB 23.10.0+
This feature requires EventStoreDB version 23.10.0 or later.
:::

When a subscription processes historical events and reaches the end of the
stream, it transitions from "catching up" to "live" mode. You can detect this
transition by using the `include_caught_up` parameter when creating the subscription.

```python
# Subscribe with caught-up notifications
subscription = client.subscribe_to_stream(
    stream_name="order-123",
    include_caught_up=True
)

for item in subscription:
    if hasattr(item, 'is_caught_up') and item.is_caught_up:
        print("Subscription has caught up to live events")
    else:
        # Regular event processing
        print(f"Processing event: {item.type}")
```

::: tip
The caught-up notification is only emitted when transitioning from catching up to live mode. If you subscribe from the end of a stream, you'll immediately be in live mode and this notification will be sent right away.
:::


## User credentials

The user creating a subscription must have read access to the stream it's
subscribing to, and only admin users may subscribe to all events or create filtered
subscriptions.

The code below shows how you can provide user credentials for a subscription.
When you specify subscription credentials explicitly, it will override the
default credentials set for the client. If you don't specify any credentials,
the client will use the credentials specified for the client.

```python
# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin", 
    password="changeit"
)

# Use credentials for subscription
subscription = client.subscribe_to_all(credentials=credentials)
```

## Server-side Filtering

KurrentDB allows you to filter events while subscribing to all events to only receive the events you care about. You can filter by event type or stream name using regular expressions. Server-side filtering is currently only available when subscribing to all events.

::: tip
Server-side filtering was introduced as a simpler alternative to projections. You should consider filtering before creating a projection to include the events you care about.
:::

**Basic filtering:**

```python
# Filter by stream name prefix
subscription = client.subscribe_to_all(
    filter_include=('test-.*', 'other-.*'),
    filter_by_stream_name=True
)
```

### Filtering out system events

System events are prefixed with `$` and are filtered out by default when subscribing to all events. If you want to include them, you can override the default filter:

```python
# Include system events by using an empty exclude filter
subscription = client.subscribe_to_all(filter_exclude=())
```

### Filtering by event type

**By prefix:**

```python
# Filter by event type prefix
subscription = client.subscribe_to_all(
    filter_include=('customer-.*',),
    filter_by_stream_name=False  # This is the default
)
```

**By regular expression:**

```python
# Filter by event type using regex
subscription = client.subscribe_to_all(
    filter_include=(r'^user.*|^company.*',)
)
```

### Filtering by stream name

**By prefix:**

```python
# Filter by stream name prefix
subscription = client.subscribe_to_all(
    filter_include=('user-.*',),
    filter_by_stream_name=True
)
```

**By regular expression:**

```python
# Filter by stream name using regex (exclude system streams)
subscription = client.subscribe_to_all(
    filter_include=(r'^[^$].*',),
    filter_by_stream_name=True
)
```

## Checkpointing

When a catch-up subscription is used to process all events containing many events, the last thing you want is for your application to crash midway, forcing you to restart from the beginning.

### What is a checkpoint?

A checkpoint is the position of an event in the all-events stream that your application has processed. By saving this position to a persistent store (e.g., a database), it allows your catch-up subscription to:

- Recover from crashes by reading the checkpoint and resuming from that position
- Avoid reprocessing all events from the start

To create a checkpoint, store the event's commit position.

### Updating checkpoints at regular intervals

You can implement checkpointing by periodically saving the commit position of processed events to a persistent store.

```python
import time

def process_events_with_checkpointing(client, checkpoint_store):
    # Get the last checkpoint
    last_commit_position = checkpoint_store.get_last_checkpoint()
    
    # Subscribe from the last checkpoint
    subscription = client.subscribe_to_all(
        commit_position=last_commit_position,
        include_checkpoints=True
    )
    
    events_processed = 0
    checkpoint_interval = 100  # Save checkpoint every 100 events
    
    for item in subscription:
        if hasattr(item, 'is_checkpoint') and item.is_checkpoint:
            # This is a checkpoint message from the server
            checkpoint_store.save_checkpoint(item.commit_position)
            print(f"Checkpoint saved at position {item.commit_position}")
        else:
            # Regular event processing
            print(f"Processing event: {item.type} at position {item.commit_position}")
            
            events_processed += 1
            
            # Save checkpoint at regular intervals
            if events_processed % checkpoint_interval == 0:
                checkpoint_store.save_checkpoint(item.commit_position)
                print(f"Checkpoint saved at position {item.commit_position}")
```

### Configuring the checkpoint interval

You can adjust how often the server sends checkpoint notifications by configuring the subscription:

```python
# Subscribe with custom checkpoint configuration
subscription = client.subscribe_to_all(
    include_checkpoints=True,
    # Checkpoints will be sent by the server periodically
)
```

By implementing checkpointing, you can balance between reducing checkpoint overhead and ensuring quick recovery in case of a failure.

::: info
The server automatically sends checkpoint notifications at regular intervals to help you implement exactly-once processing patterns.
:::
