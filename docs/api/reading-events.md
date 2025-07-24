---
order: 3
---

# Reading Events

KurrentDB provides two primary methods for reading events: reading from an
individual stream to retrieve events from a specific named stream, or reading
from all events to access all events across the entire event store.

Events in KurrentDB are organized within individual streams and use two
distinct positioning systems to track their location. The **stream position** is
an integer that represents the sequential position of an
event within its specific stream. Events are numbered starting from 0, with each
new event receiving the next sequential position (0, 1, 2, 3...). The
**commit position** represents the event's location in KurrentDB's global
transaction log and is a single integer value that indicates where the event
was committed in the log.

These positioning identifiers are essential for reading operations, as they
allow you to specify exactly where to start reading from within a stream or
across the entire event store.

## Reading from a stream

You can read all the events or a sample of the events from individual streams,
starting from any position in the stream, and can read either forward or
backward. It is only possible to read events from a single stream at a time. You
can read events from the global event log, which spans across streams. Learn
more about this process in the [Read from all events](#reading-from-all-events)
section below.

### Reading forwards

The simplest way to read a stream forwards is to supply a stream name and 
optionally specify a stream position from which to start. You can use the
`get_stream()` method for simple cases or `read_stream()` for streaming large
result sets.

```python
from kurrentdbclient import KurrentDBClient

# Read all events from a stream
events = client.get_stream(stream_name="some-stream")

# Iterate through the events
for event in events:
    print(f"Event: {event.type} at position {event.stream_position}")
```

You can also start reading from a specific position in the stream:

```python
# Read from a specific stream position
events = client.get_stream(
    stream_name="some-stream", 
    stream_position=10,
    limit=20
)

# Or use read_stream for streaming
read_response = client.read_stream(
    stream_name="some-stream",
    stream_position=10,
    limit=20
)

# Iterate through the streaming response
for event in read_response:
    print(f"Event: {event.type}")
```

There are a number of additional arguments you can provide when reading a stream.

#### limit

Passing in the limit allows you to restrict the number of events that are returned.

In the example below, we read a maximum of 20 events from the stream:

```python
events = client.get_stream(
    stream_name="some-stream",
    stream_position=10,
    limit=20
)
```

#### resolve_links

When using projections to create new events you can set whether the generated events are pointers to existing events. Setting this value to `True` will tell KurrentDB to return the event as well as the event linking to it.

```python
events = client.get_stream(
    stream_name="some-stream",
    resolve_links=True
)
```

#### credentials

The credentials used to read the data can override the default credentials set on the connection.

```python
# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin", 
    password="changeit"
)

# Use credentials for reading
events = client.get_stream(
    stream_name="some-stream",
    credentials=credentials
)
```

### Reading backwards

In addition to reading a stream forwards, streams can be read backwards. To read events backwards, set the `backwards` parameter to `True`:

```python
# Read all events backwards from the end
events = client.get_stream(
    stream_name="some-stream",
    backwards=True
)

# Read backwards from a specific position
events = client.get_stream(
    stream_name="some-stream",
    stream_position=100,
    backwards=True,
    limit=10
)
```

:::tip
Read one event backwards to find the last position in the stream.
:::

### Checking if the stream exists

Reading a stream that doesn't exist will raise a `NotFoundError` exception. It is important to handle this error when attempting to read a stream that may not exist.

For example:

```python
from kurrentdbclient.exceptions import NotFoundError

try:
    events = client.get_stream(stream_name="order-0")
    
    for event in events:
        print(f"Event: {event.type}")
        
except NotFoundError:
    print("Stream does not exist")
except Exception as e:
    print(f"Error reading stream: {e}")
```

## Reading from all events

Reading from all events is similar to reading from an individual stream, but please note there are differences. One significant difference is the need to provide admin user account credentials to read from all events. Additionally, you need to provide a commit position instead of a stream position when reading from all events.

### Reading forwards

The simplest way to read all events forwards is to use the `read_all()` method. You can specify a commit position from which you want to start:

```python
# Read all events from the beginning
read_response = client.read_all()

# Iterate through all events
for event in read_response:
    print(f"Event: {event.type} from stream {event.stream_name}")
```

You can also start reading from a specific position in the global log:

```python
# Read from a specific commit position
read_response = client.read_all(
    commit_position=1110,
    limit=100
)

# Iterate through the events
for event in read_response:
    print(f"Event: {event.type} at commit position {event.commit_position}")
```

There are a number of additional arguments you can provide when reading all events.

#### limit

Passing in the limit allows you to restrict the number of events that are returned.

In the example below, we read a maximum of 100 events:

```python
read_response = client.read_all(
    commit_position=1110,
    limit=100
)
```

#### resolve_links

When using projections to create new events you can set whether the generated events are pointers to existing events. Setting this value to `True` will tell KurrentDB to return the event as well as the event linking to it.

```python
read_response = client.read_all(resolve_links=True)
```

#### credentials

The credentials used to read the data can override the default credentials set on the connection.

```python
# Construct call credentials  
credentials = client.construct_call_credentials(
    username="admin", 
    password="changeit"
)

# Use credentials for reading
read_response = client.read_all(credentials=credentials)
```

### Reading backwards

In addition to reading all events forwards, you can read them backwards. To
read all events backwards, set the `backwards` parameter to `True`:

```python
# Read all events backwards from the end
read_response = client.read_all(backwards=True)

# Read backwards from a specific commit position
read_response = client.read_all(
    commit_position=5000,
    backwards=True,
    limit=50
)
```

:::tip
Read one event backwards to find the last position in the global log.
:::

### Handling system events

KurrentDB will also return system events when reading from all events. In most cases you can ignore these events, and they are filtered out by default.

All system events begin with `$` and can be easily ignored. If you want to include system events, you can override the default filter:

```python
# Include system events by overriding the default filter
read_response = client.read_all(filter_exclude=())

# Process events and check for system events
for event in read_response:
    if event.type.startswith("$"):
        print(f"System event: {event.type}")
        continue
    
    print(f"User event: {event.type}")
```