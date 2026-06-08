---
order: 1
---

# Getting Started

This guide will help you get started with the Python clients for KurrentDB:
* [Start KurrentDB locally](#running-kurrentdb-locally)
* [Install the Python package](#installation)
* [Client configuration](#client-configuration)
* [Connect to KurrentDB](#connecting-to-kurrentdb)
* [Create new events](#creating-new-events)
* [Append events to streams](#appending-to-a-stream)
* [Read streams](#reading-a-stream)

## Running KurrentDB Locally

You can start KurrentDB with "insecure" mode in Docker by using the `--insecure` flag:

```bash:no-line-numbers
docker run --name kurrentdb-node -it -p 2113:2113 \
    docker.kurrent.io/kurrent-lts/kurrentdb:latest \
    --insecure \
    --run-projections=All \
    --enable-atom-pub-over-http
```

Please read the server docs for more details about [KurrentDB installation](@server/quick-start/installation.html).

## Installation

The [`kurrentdbclient`](https://pypi.org/project/kurrentdbclient/) package provides the official sync and async Python clients for KurrentDB.

### Install or Update Python

For information about how to get the latest version of Python, see the official [Python documentation](https://www.python.org/downloads/).

Before installing the Python clients for KurrentDB, ensure you’re using Python 3.10 or later.

### Install the Python Clients

If you use `uv`:

```bash:no-line-numbers
uv add "kurrentdbclient~=1.2"
```

If you use `poetry`:

```bash:no-line-numbers
poetry add "kurrentdbclient~=1.2"
```

If you prefer a manual setup with `pip`:

```bash:no-line-numbers
python -m venv .venv
source .venv/bin/activate
pip install "kurrentdbclient~=1.2"
```

## Python Clients for KurrentDB

The `kurrentdbclient` Python package provides both sync and async clients for KurrentDB.

The sync and async clients have exactly the same methods and behaviors as each other.

* Sync client – **blocking** interface suitable for sequential code and multi-threaded apps

* Async client – **asynchronous** interface suitable for high-concurrency applications

This documentation provides examples for both sync and async clients in tabbed boxes (see below).

The official sync and async Python clients have been tested with KurrentDB versions 25.0, 25.1, 26.0,
and 26.1, and EventStoreDB versions 23.10 and 24.10, with and without SSL/TLS, in both
single-server and cluster modes, across Python versions 3.10, 3.11, 3.12, 3.13, and 3.14.

## Client Configuration

All KurrentDB clients use a standardized [connection string](./connection-strings.md) to configure their connection to KurrentDB.

When KurrentDB is [running locally](#running-kurrentdb-locally) with "insecure" mode, use a connection string with `tls=false`:

```python:no-line-numbers
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
```
For production services, ask your service provider for a valid [connection string](./connection-strings.md).


## Connecting to KurrentDB

To connect to KurrentDB from Python, instantiate a [client](#python-clients-for-kurrentdb) with a [connection string](#client-configuration).

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import KurrentDBClient

client = KurrentDBClient(connection_string)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import AsyncKurrentDBClient

client = AsyncKurrentDBClient(connection_string)
```
:::

## Creating New Events

Use the [`NewEvent`](./appending-events.md#the-newevent-class) class to define new events with a `type` string and binary `data`.

```python:no-line-numbers
from kurrentdbclient import NewEvent

new_event = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)
```

See the [`NewEvent`](./appending-events.md#the-newevent-class) documentation for more details.

## Appending to a Stream

The client [`append_to_stream()`](./appending-events.md#append-to-stream) method records new events in KurrentDB.

When appending to a stream, specify a `stream_name`, the new [`events`](./appending-events.md#the-newevent-class) and a [`current_version`](./appending-events.md#consistency-checks).

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import NewEvent, StreamState

new_event = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)

client.append_to_stream(
    stream_name="order-123",
    events=[new_event],
    current_version=StreamState.NO_STREAM,
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import NewEvent, StreamState

new_event = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)

await client.append_to_stream(
    stream_name="order-123",
    events=[new_event],
    current_version=StreamState.NO_STREAM,
)
```
:::

See [Appending Events](./appending-events.md) for more information about writing to KurrentDB.


## Reading a Stream

The client [`get_stream()`](./reading-events.md#get-stream) method reads events from a named stream.

::: tabs
@tab sync
```python:no-line-numbers
for recorded_event in client.get_stream(
    stream_name="order-123"
):
    print("Stream name:", recorded_event.stream_name)
    print("Stream position:", recorded_event.stream_position)
    print("Commit position:", recorded_event.commit_position)
    print("Event type:", recorded_event.type)
    print("Event data:", recorded_event.data)
    print("Event ID:", recorded_event.id)
```
@tab async
```python:no-line-numbers
for recorded_event in await client.get_stream(
    stream_name="order-123"
):
    print("Stream name:", recorded_event.stream_name)
    print("Stream position:", recorded_event.stream_position)
    print("Commit position:", recorded_event.commit_position)
    print("Event type:", recorded_event.type)
    print("Event data:", recorded_event.data)
    print("Event ID:", recorded_event.id)
```
:::

See [Reading Events](./reading-events.md) for more information about reading from KurrentDB.

## Overriding User Credentials

You can use the `credentials` parameter of the Python client methods to override the [user info](./connection-strings.md#user-info) given in a client connection string.

Use the `construct_call_credentials()` method to construct a `CallCredentials` object from a username and password.

::: tabs
@tab sync
```python:no-line-numbers
# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

# Use credentials for this specific operation
commit_position = client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.ANY,
    events=[new_event],
    credentials=credentials,
)
```
@tab async
```python:no-line-numbers
# Construct call credentials
credentials = client.construct_call_credentials(
    username="admin",
    password="changeit",
)

# Use credentials for this specific operation
commit_position = await client.append_to_stream(
    stream_name="order-123",
    current_version=StreamState.ANY,
    events=[new_event],
    credentials=credentials,
)
```
:::
