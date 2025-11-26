---
order: 1
---

# Getting started

This guide will help you get started with the Python client for KurrentDB.
It covers the basic steps to connect to KurrentDB, create events, append them
to streams, and read them back.

## Install package

Add the `kurrentdbclient` package to your Python project:


::: tabs
@tab uv
```bash:no-line-numbers
uv add "kurrentdbclient~=1.1"
```
@tab poetry
```bash:no-line-numbers
poetry add "kurrentdbclient~=1.1"
```
@tab pipenv
```bash:no-line-numbers
pipenv install "kurrentdbclient~=1.1"
```
@tab pip
```bash:no-line-numbers
pip install "kurrentdbclient~=1.1" && pip freeze > requirements.txt
```
:::

Once the Python package has been installed, you can import one of the Python client classes for KurrentDB.

## Import class

The Python package `kurrentdbclient` provides two client classes for KurrentDB.

* `KurrentDBClient` provides a **blocking** interface, and is suitable for standard sequential code and multi-threaded applications.

* `AsyncKurrentDBClient` provides an **asynchronous** interface, and is suitable for high-concurrency applications using Python’s native `asyncio` framework.

Both can be imported from the `kurrentdbclient` package.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient import KurrentDBClient
```
@tab async
```python:no-line-numbers
from kurrentdbclient import AsyncKurrentDBClient
```
:::

The client classes can be constructed with a KurrentDB connection string.

## Connection strings

KurrentDB clients use **connection strings** to configure their connection to KurrentDB.
KurrentDB connection strings are standardized across all the official KurrentDB clients.

:::info
When connecting to a production database, ask your server administrator for a valid connection string.
:::

### Two protocols

KurrentDB connection strings support two protocols.

* **`kurrentdb://`** for **connecting directly** to specific KurrentDB server endpoints.

* **`kurrentdb+discover://`** for connecting using cluster discovery **via DNS A records**.

With the `kurrentdb://` protocol you can specify one or many endpoints, separated by commas. If you specify only one
endpoint, the client will **connect directly and remain with it**. If you specify
many endpoints, the client will use them to query for cluster information and **pick an endpoint from the
obtained cluster information** for continuing operations, according to the node preference specified by the connection
string - see options below. This process will be repeated if the client detects that it needs
to reconnect to the cluster. An "endpoint" can be a specified either as a host name or an
IP address, with a port number.

With the `kurrentdb+discover://` protocol you should specify a fully-qualified domain name of a KurrentDB cluster,
with an optional port number. Using the cluster's **DNS A records**, the client will query for cluster information
and pick an endpoint from the cluster information for continuing operations, according to the node preference
specified by the connection string - see options below. This process will be repeated if the client detects that
it needs to reconnect to the cluster.

### User info string

Both the `kurrentdb://` and `kurrentdb+discover://` protocols support an optional user info string.
If it exists, the user info string must be separated from the rest of the URI
with the `"@"` character. The user info string must include a username and a password,
separated with the `":"` character.

The user info is sent by the client in a "basic auth" authorization header in each gRPC
call to a "secure" server. This authorization header is used by the server to authenticate
the client. The Python client does not allow call credentials to be transferred to
"insecure" servers (option `tls=false`).

### Examples

In the examples below, `user` is a username and `pass` is a password.

For connecting directly to a single node, use the following format:

```:no-line-numbers
kurrentdb://user:pass@node1.example.com:2113
```

For connecting to a cluster, using specific endpoints to obtain cluster information, whilst observing your node
preference for continuing operations, use the following format:

```:no-line-numbers
kurrentdb://user:pass@node1.example.com:2113,node2.example.com:2113,node3.example.com:2113
```

For connecting to a cluster, where `cluster1.example.com` is configured with DNS A records for the cluster endpoints,
whilst observing your node preference for continuing operations, use the following format:

```:no-line-numbers
kurrentdb+discover://user:pass@cluster1.example.com:2113
```

### Options

The table below describes optional query parameters that can be used in the connection string to configure the client.

| Parameter             | Accepted values                                   | Default     | Description                                                                                                                                         |
|-----------------------|---------------------------------------------------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `tls`                 | `true`, `false`                                   | `true`      | Set to `false` when connecting to KurrentDB running with "insecure" mode.                                                                           |
| `connectionName`      | Any string                                        | Random UUID | Connection name                                                                                                                                     |
| `maxDiscoverAttempts` | Integer                                           | `10`        | Number of attempts to discover the cluster.                                                                                                         |
| `discoveryInterval`   | Integer                                           | `100`       | Cluster discovery polling interval in milliseconds.                                                                                                 |
| `gossipTimeout`       | Integer                                           | `5`         | Gossip timeout in seconds, when the gossip call times out, it will be retried.                                                                      |
| `nodePreference`      | `leader`, `follower`, `random`, `readOnlyReplica` | `leader`    | Preferred node role. When creating a client for write operations, always use `leader`.                                                              |
| `tlsCaFile`           | File system path                                  | None        | Path to the CA file when connecting to a secure cluster with a certificate that's not signed by a trusted CA.                                       |
| `defaultDeadline`     | Integer                                           | None        | Maximum duration, in seconds, for completion of client operations. Can be overridden per operation using the `timeout` parameter of client methods. |
| `keepAliveInterval`   | Integer                                           | None        | Interval between keep-alive ping calls, in milliseconds.                                                                                            |
| `keepAliveTimeout`    | Integer                                           | None        | Keep-alive ping call timeout, in milliseconds.                                                                                                      |
| `userCertFile`        | File system path                                  | None        | User certificate file for X.509 authentication.                                                                                                     |
| `userKeyFile`         | File system path                                  | None        | Key file for the user certificate used for X.509 authentication.                                                                                    |

:::tip
Please note, all option field names and values are case-insensitive.
:::

## Start KurrentDB

For local development, you can run KurrentDB in "insecure" mode with Docker.

```bash
export KURRENTDB_IMAGE=docker.kurrent.io/kurrent-latest/kurrentdb:latest
docker run --rm -p 2113:2113 $KURRENTDB_IMAGE --insecure
```

Then use connection string `kurrentdb://127.0.0.1:2113?tls=false` when connecting to KurrentDB.


## Connect to KurrentDB

Construct a client with a connection string.

::: warning
If you are using the async client, you will need also to call the async `connect()` method.
:::

The example below connects to a KurrentDB server running locally in "insecure" mode.

::: tabs
@tab sync
```python
uri = "kurrentdb://127.0.0.1:2113?tls=false"
client = KurrentDBClient(uri)
# immediately connected to KurrentDB
```
@tab async
```python
uri = "kurrentdb://127.0.0.1:2113?tls=false"
client = AsyncKurrentDBClient(uri)
await client.connect()  # connect to KurrentDB
```
:::

:::tip
The sync and async client classes have identical methods, except the methods of the async client are
defined with `async def` and so must be `await`-ed when called. 
:::

## Test the connection

You can test the connection by getting the database "commit position", which is the
position in the database of the last recorded event.

You can get the "commit position" by calling the client method `get_commit_position()`.

::: tabs
@tab sync
```python
client.get_commit_position()
```
@tab async
```python
await client.get_commit_position()
```
:::

If you have just started KurrentDB for the first time, the returned value will be zero, `0`, which
indicates that no events have been recorded.

:::caution
If the connection fails, check KurrentDB is running locally in "insecure" mode (see above).
:::



## Writing to KurrentDB

KurrentDB is an event store database. So let's get started by appending an event!

The client method `append_to_stream()` writes new events in KurrentDB.

The example below appends the first new event of stream `"order:123"`.

::: tabs
@tab sync
```python
from kurrentdbclient import NewEvent, StreamState

client.append_to_stream(
    stream_name="order:123",
    events=[
        NewEvent(type="OrderCreated", data=b'{"name": "Greg"}'),
    ],
    current_version=StreamState.NO_STREAM,
)
```
@tab async
```python
from kurrentdbclient import NewEvent, StreamState

await client.append_to_stream(
    stream_name="order:123",
    events=[
        NewEvent(type="OrderCreated", data=b'{"name": "Greg"}'),
    ],
    current_version=StreamState.NO_STREAM,
)
```
:::

The `stream_name` parameter identifies the "stream" to which events will be appended.

The `events` parameter is a list of `NewEvent` objects. The `NewEvent` class is a Python `dataclass`. 

The `current_version` parameter activates optimistic concurrent control. The `StreamState.NO_STREAM`
argument indicates we require the stream has no previously recorded events.

See [Appending events](./appending-events.md) for more information about writing to KurrentDB.

## Reading from KurrentDB

The client method `read_stream()` returns an iterator of events that have been recorded in KurrentDB.

The example below reads and prints the events of stream `"order:123"`.

::: tabs
@tab sync
```python
for event in client.read_stream("order:123"):
    print(event)
```
@tab async
```python
async for event in await client.read_stream("order:123"):
    print(event)
```
:::

The first parameter is used to identify the stream. In the example above, the given argument is `"order:123"`.

See [Reading events](./appending-events.md) for more information about reading from KurrentDB.
