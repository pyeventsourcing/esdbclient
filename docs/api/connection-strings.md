---
order: 2
---

# Connection strings

This guide explains the standardized connection string format used by all official KurrentDB clients.

:::info
For production services, ask your service provider for a valid connection string.
:::

KurrentDB clients use a connection string to configure their connection to KurrentDB.

## Two protocols

KurrentDB connection strings support two protocols.

* **`kurrentdb://`** for **connecting directly** to specific KurrentDB server endpoints.

* **`kurrentdb+discover://`** for connecting using cluster discovery **via DNS records**.

With the `kurrentdb://` protocol, you can specify one or many endpoints, separated by
commas. If a single endpoint is specified, the client **connects directly** to that endpoint
and continues to use it for subsequent operations. If multiple endpoints are specified,
the client uses them to query cluster information and selects an endpoint **from the
discovered cluster members** according to the node preference specified in the connection
string (see options below). If the client needs to reconnect, this discovery process is
repeated. An endpoint may be specified as either a host name or an IP address, together
with a port number.

With the `kurrentdb+discover://` protocol, you should specify the fully-qualified domain
name of a KurrentDB cluster, with an optional port number. The client **resolves the
cluster domain name** to discover cluster nodes, queries the cluster for membership information,
and then selects an endpoint according to the node preference specified in the
connection string (see options below). If the client needs to reconnect, the discovery process is repeated.

## User info

Both the `kurrentdb://` and `kurrentdb+discover://` protocols support an optional user info string.
If it exists, the user info string must be separated from the rest of the URI
with the `"@"` character. The user info string must include a username and a password,
separated with the `":"` character.

The user info is sent by the client in a "basic auth" authorization header in each gRPC
call to a "secure" server. This authorization header is used by the server to authenticate
the client. The Python clients do not allow call credentials to be transferred to
"insecure" servers (option `tls=false`).

## Examples

In the examples below, `user` is a username and `pass` is a password.

For connecting directly to a single node:

```:no-line-numbers
kurrentdb://user:pass@node1:2113
```

For connecting to a cluster using specific endpoints to obtain cluster information:

```:no-line-numbers
kurrentdb://user:pass@node1:2113,node2:2113,node3:2113
```

For connecting to a cluster configured with DNS A records for the cluster endpoints:

```:no-line-numbers
kurrentdb+discover://user:pass@cluster1:2113
```

## User certificates

To authenticate a client with an X.509 certificate, you need:

* KurrentDB version 25.0+ [configured for user certificates](@server/security/user-authentication.html#user-x-509-certificates); and
* A valid client certificate and private key.

Then use the `userCertFile` and `userKeyFile` connection string options.

Here's an example for connecting to KurrentDB with a client certificate.

```:no-line-numbers
kurrentdb://node1:2113?userCertFile=user_cert.pem&userKeyFile=user_key.pem
```

::: info
A licence is required to authenticate clients with user certificates.
:::

## Connection options

The table below describes optional query parameters that can be used in the connection string to configure the client.
All option field names and values are case-insensitive.
| Field name            | Accepted values                                   | Default     | Description                                                                                                                                         |
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

