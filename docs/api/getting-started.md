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

The `kurrentdbclient` Python package provides the official Python clients for KurrentDB.

### Install or Update Python

Before installing the Python client for KurrentDB, ensure you’re using Python 3.10 or later.

For information about how to get the latest version of Python, see the official [Python documentation](https://www.python.org/downloads/).

### Setup a Virtual Environment

Once you have a supported version of Python installed, create a virtual environment and activate it:

Create a virtual environment:

```bash:no-line-numbers
python -m venv .venv
```

Activate the virtual environment:

```bash:no-line-numbers
source .venv/bin/activate
```

### Install the Package

Install the [`kurrentdbclient`](https://pypi.org/project/kurrentdbclient/) Python package via pip:

```bash:no-line-numbers
pip install "kurrentdbclient"
```

If your project requires a specific version, or has compatibility concerns with certain versions, you may provide constraints when installing:

```bash:no-line-numbers
pip install "kurrentdbclient~=1.2"
```

## Python Clients for KurrentDB

The `kurrentdbclient` Python package provides sync and async clients for KurrentDB:

* Sync client – **blocking** interface suitable for sequential code and multi-threaded apps

* Async client – **asynchronous** interface suitable for high-concurrency applications

## Client Configuration

KurrentDB clients use a standardized [connection string](./connection-strings.md) to configure their connection to KurrentDB.

When KurrentDB is [running locally](#running-kurrentdb-locally) with "insecure" mode, use a connection string with `tls=false`:

```python:no-line-numbers
connection_string = "kurrentdb://127.0.0.1:2113?tls=false"
```
For production services, ask your service provider for a valid [connection string](./connection-strings.md).


## Connecting to KurrentDB

To connect to KurrentDB, instantiate a [sync or async client](#python-clients-for-kurrentdb) with a [suitable connection string](#client-configuration).

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

Use the [`NewEvent`](./appending-events.md#new-events) class to define new events with a `type` string and binary `data`.

```python:no-line-numbers
from kurrentdbclient import NewEvent

new_event = NewEvent(
    type="OrderCreated",
    data=b'{"name": "Greg"}',
)
```

See the [`NewEvent`](./appending-events.md#new-events) documentation for more details.

## Appending to a Stream

The Python client's [`append_to_stream()`](./appending-events.md#append-to-stream) method records new events in KurrentDB.

When appending to a stream, specify a `stream_name`, the new [`events`](./appending-events.md#new-events) and a [`current_version`](./appending-events.md#optimistic-concurrency-control).

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import StreamState

client.append_to_stream(
    stream_name="order-123",
    events=[new_event],
    current_version=StreamState.NO_STREAM,
)
```
@tab async
```python:no-line-numbers
from kurrentdbclient import StreamState

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

The Python client's [`get_stream()`](./reading-events.md#get-stream) method reads events from a named stream.

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
