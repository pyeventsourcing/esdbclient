---
order: 1
---

# Getting started

This guide will help you get started with KurrentDB in your Java application.
It covers the basic steps to connect to KurrentDB, create events, append them
to streams, and read them back.

## Required packages

Add the following package to your virtual environment:

```bash
pip install kurrentdbclient
```

## Connecting to KurrentDB

To connect your application to KurrentDB, you need to configure and create a client instance.

::: tip Insecure clusters
The recommended way to connect to KurrentDB is using secure mode (which is
the default). However, if your KurrentDB instance is running in insecure
mode, you must explicitly set `tls=false` in your connection string or client configuration.
:::

KurrentDB uses connection strings to configure the client connection. The connection string supports two protocols:

- **`kurrentdb://`** - for connecting directly to specific node endpoints (single node or multi-node cluster with explicit endpoints)
- **`kurrentdb+discover://`** - for connecting using cluster discovery via DNS or gossip endpoints

When using `kurrentdb://`, you specify the exact endpoints to connect to. The client will connect directly to these endpoints. For multi-node clusters, you can specify multiple endpoints separated by commas, and the client will query each node's Gossip API to get cluster information, then picks a node based on the URI's node preference.

With `kurrentdb+discover://`, the client uses cluster discovery to find available nodes. This is particularly useful when you have a DNS A record pointing to cluster nodes or when you want the client to automatically discover the cluster topology.

::: info Gossip support
Since EventStoreDB 22.10, the database supports gossip on single-node deployments, so
`kurrentdb+discover://` can be used for any topology, including single-node setups.
:::

For cluster connections using discovery, use the following format:

```
kurrentdb+discover://admin:changeit@cluster.dns.name:2113
```

Where `cluster.dns.name` is a DNS `A` record that points to all cluster nodes.

For direct connections to specific endpoints, you can specify individual nodes:

```
kurrentdb://admin:changeit@node1.dns.name:2113,node2.dns.name:2113,node3.dns.name:2113
```

Or for a single node:

```
kurrentdb://admin:changeit@localhost:2113
```

There are a number of query parameters that can be used in the connection string to instruct the cluster how and where the connection should be established. All query parameters are optional.

| Parameter             | Accepted values                                   | Default  | Description                                                                                                                                    |
|-----------------------|---------------------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------|
| `tls`                 | `true`, `false`                                   | `true`   | Use secure connection, set to `false` when connecting to a non-secure server or cluster.                                                       |
| `connectionName`      | Any string                                        | None     | Connection name                                                                                                                                |
| `maxDiscoverAttempts` | Number                                            | `10`     | Number of attempts to discover the cluster.                                                                                                    |
| `discoveryInterval`   | Number                                            | `100`    | Cluster discovery polling interval in milliseconds.                                                                                            |
| `gossipTimeout`       | Number                                            | `5`      | Gossip timeout in seconds, when the gossip call times out, it will be retried.                                                                 |
| `nodePreference`      | `leader`, `follower`, `random`, `readOnlyReplica` | `leader` | Preferred node role. When creating a client for write operations, always use `leader`.                                                         |
| `tlsVerifyCert`       | `true`, `false`                                   | `true`   | In secure mode, set to `true` when using an untrusted connection to the node if you don't have the CA file available. Don't use in production. |
| `tlsCaFile`           | String, file path                                 | None     | Path to the CA file when connecting to a secure cluster with a certificate that's not signed by a trusted CA.                                  |
| `defaultDeadline`     | Number                                            | None     | Default timeout for client operations, in milliseconds. Most clients allow overriding the deadline per operation.                              |
| `keepAliveInterval`   | Number                                            | `10`     | Interval between keep-alive ping calls, in seconds.                                                                                            |
| `keepAliveTimeout`    | Number                                            | `10`     | Keep-alive ping call timeout, in seconds.                                                                                                      |
| `userCertFile`        | String, file path                                 | None     | User certificate file for X.509 authentication.                                                                                                |
| `userKeyFile`         | String, file path                                 | None     | Key file for the user certificate used for X.509 authentication.                                                                               |

When connecting to an insecure instance, specify `tls=false` parameter. For example, for a node running locally use `kurrentdb://localhost:2113?tls=false`. Note that usernames and passwords aren't provided there because insecure deployments don't support authentication and authorisation.

## Creating a client

First, create a client and get it connected to the database.

```py
client = KurrentDBClient(uri="kurrentdb://admin:changeit@localhost:2113?tls=false")
```

The client instance can be used as a singleton across the whole application. It doesn't need to open or close the connection.

## Creating an event

This package defines a `NewEvent` class and a `RecordedEvent` class. The `NewEvent`
class should be used when writing events to the database. The `RecordedEvent`
class is used when reading events from the database.

```py
new_event = NewEvent(
    type='OrderCreated',
    data=b'{"name": "Greg"}',
)
```

## Appending events

Each event in the database has its own unique identifier (UUID). The database uses it to ensure idempotent writes, but it only works if you specify the stream revision when appending events to the stream.

In the snippet below, we append the event to the stream `orders`.

```py
import uuid
from kurrentdbclient import NewEvent, StreamState

# Define a new stream name.
stream_name = str(uuid.uuid4())

# Append the new events to the new stream.
commit_position = client.append_to_stream(
    stream_name=stream_name,
    current_version=StreamState.NO_STREAM,
    events=[event1],
)

```

Here we are appending events without checking if the stream exists or if the stream version matches the expected event version. See more advanced scenarios in [appending events documentation](./appending-events.md).

## Reading events

Finally, we can read events back from the stream.

```py
events = client.get_stream(
    stream_name=stream_name
)
```