<a href="https://kurrent.io">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/pyeventsourcing/kurrentdbclient/raw/1.0/KurrentLogo-White.png.png">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/pyeventsourcing/kurrentdbclient/raw/1.0/KurrentLogo-Black.png">
    <img alt="Kurrent" src="https://github.com/pyeventsourcing/kurrentdbclient/raw/1.0/KurrentLogo-Plum.png" height="50%" width="50%">
  </picture>
</a>

Please note: following the rebranding of EventStoreDB to KurrentDB, this package
is a rebranding of the [`esdbclient`](https://pypi.org/project/esdbclient) package.

# Python gRPC Client for KurrentDB

This [Python package](https://pypi.org/project/kurrentdbclient/) provides multithreaded and asyncio Python
clients for the [KurrentDB](https://kurrent.io/) database.

The multithreaded `KurrentDBClient` is described in detail below. Please scroll
down for <a href="#asyncio-client">information</a> about `AsyncKurrentDBClient`.

These clients have been developed and are being maintained in a collaboration
with the KurrentDB team, and are officially supported by Kurrent Inc.
Although not all aspects of the KurrentDB gRPC API are implemented, most
features are presented in an easy-to-use interface.

These clients have been tested to work with KurrentDB 25.0.0, without and without
SSL/TLS, with both single-server and cluster modes, and with Python versions
3.9, 3.10, 3.11, 3.12, and 3.13.

The test suite has 100% line and branch coverage. The code has typing annotations
checked strictly with mypy. The code is formatted with black and isort, and checked
with flake8. Poetry is used for package management during development, and for
building and publishing distributions to [PyPI](https://pypi.org/project/kurrentdbclient/).

For an example of usage, see the [eventsourcing-kurrentdb](
https://github.com/pyeventsourcing/eventsourcing-kurrentdb) package.


<!-- TOC -->
* [Synopsis](#synopsis)
* [Install package](#install-package)
  * [From PyPI](#from-pypi)
  * [With Poetry](#with-poetry)
* [KurrentDB server](#kurrentdb-server)
  * [Run container](#run-container)
  * [Stop container](#stop-container)
* [KurrentDB client](#kurrentdb-client)
  * [Import class](#import-class)
  * [Construct client](#construct-client)
* [Connection strings](#connection-strings)
  * [Two schemes](#two-schemes)
  * [User info string](#user-info-string)
  * [Query string](#query-string)
  * [Examples](#examples)
* [Event objects](#event-objects)
  * [New events](#new-events)
  * [Recorded events](#recorded-events)
* [Streams](#streams)
  * [Append events](#append-events)
  * [Idempotent append operations](#idempotent-append-operations)
  * [Read stream events](#read-stream-events)
  * [Get current version](#get-current-version)
  * [How to implement snapshotting with KurrentDB](#how-to-implement-snapshotting-with-kurrentdb)
  * [Read all events](#read-all-events)
  * [Get commit position](#get-commit-position)
  * [Get stream metadata](#get-stream-metadata)
  * [Set stream metadata](#set-stream-metadata)
  * [Delete stream](#delete-stream)
  * [Tombstone stream](#tombstone-stream)
* [Catch-up subscriptions](#catch-up-subscriptions)
  * [Subscribe to all events](#subscribe-to-all-events)
  * [Subscribe to stream events](#subscribe-to-stream-events)
  * [How to implement exactly-once event processing](#how-to-implement-exactly-once-event-processing)
* [Persistent subscriptions](#persistent-subscriptions)
  * [Create subscription to all](#create-subscription-to-all)
  * [Read subscription to all](#read-subscription-to-all)
  * [How to write a persistent subscription consumer](#how-to-write-a-persistent-subscription-consumer)
  * [Update subscription to all](#update-subscription-to-all)
  * [Create subscription to stream](#create-subscription-to-stream)
  * [Read subscription to stream](#read-subscription-to-stream)
  * [Update subscription to stream](#update-subscription-to-stream)
  * [Replay parked events](#replay-parked-events)
  * [Get subscription info](#get-subscription-info)
  * [List subscriptions](#list-subscriptions)
  * [List subscriptions to stream](#list-subscriptions-to-stream)
  * [Delete subscription](#delete-subscription)
* [Projections](#projections)
  * [Create projection](#create-projection)
  * [Get projection state](#get-projection-state)
  * [Update projection](#update-projection)
  * [Get projection statistics](#get-projection-statistics)
  * [List all projection statistics](#list-all-projection-statistics)
  * [List continuous projection statistics](#list-continuous-projection-statistics)
  * [Disable projection](#disable-projection)
  * [Enable projection](#enable-projection)
  * [Abort projection](#abort-projection)
  * [Reset projection](#reset-projection)
  * [Delete projection](#delete-projection)
  * [Restart projections subsystem](#restart-projections-subsystem)
* [Call credentials](#call-credentials)
  * [Construct call credentials](#construct-call-credentials)
* [Connection](#connection)
  * [Reconnect](#reconnect)
  * [Close](#close)
* [Asyncio client](#asyncio-client)
  * [Synopsis](#synopsis-1)
  * [FastAPI](#fastapi)
* [Notes](#notes)
  * [Regular expression filters](#regular-expression-filters)
  * [Reconnect and retry method decorators](#reconnect-and-retry-method-decorators)
* [Instrumentation](#instrumentation)
  * [OpenTelemetry](#open-telemetry)
* [Communities](#communities)
* [Contributors](#contributors)
  * [Install Poetry](#install-poetry)
  * [Setup for PyCharm users](#setup-for-pycharm-users)
  * [Setup from command line](#setup-from-command-line)
  * [Project Makefile commands](#project-makefile-commands)
<!-- TOC -->

## Synopsis<a id="synopsis"></a>

The `KurrentDBClient` class can be imported from the `kurrentdbclient` package.

Probably the three most useful methods of `KurrentDBClient` are:

* `append_to_stream()` This method can be used to record new events in a particular
"stream". This is useful, for example, when executing a command in an application
that mutates an aggregate. This method is "atomic" in that either all or none of
the events will be recorded.

* `get_stream()` This method can be used to retrieve all the recorded
events in a "stream". This is useful, for example, when reconstructing
an aggregate from recorded events before executing a command in an
application that creates new events.

* `subscribe_to_all()` This method can be used to receive all recorded events in
the database. This is useful, for example, in event-processing components because
it supports processing events with "exactly-once" semantics.

The example below uses an "insecure" KurrentDB server running locally on port 2113.

```python
import uuid

from kurrentdbclient import KurrentDBClient, NewEvent, StreamState

# Construct KurrentDBClient with a KurrentDB URI. The
# connection string URI specifies that the client should
# connect to an "insecure" server running on port 2113.

client = KurrentDBClient(
    uri="kurrentdb://localhost:2113?Tls=false"
)

# Generate new events. Typically, domain events of different
# types are generated in a domain model, and then serialized
# into NewEvent objects. An aggregate ID may be used as the
# name of a stream in KurrentDB.

stream_name1 = str(uuid.uuid4())
event1 = NewEvent(
    type='OrderCreated',
    data=b'{"order_number": "123456"}'
)
event2 = NewEvent(
    type='OrderSubmitted',
    data=b'{}'
)
event3 = NewEvent(
    type='OrderCancelled',
    data=b'{}'
)

# Append new events to a new stream. The value returned
# from the append_to_stream() method is the overall
# "commit position" in the database of the last new event
# recorded by this operation. The returned "commit position"
# may be used in a user interface to poll an eventually
# consistent event-processing component until it can
# present an up-to-date materialized view. New events are
# each allocated a "stream position", which is the next
# available position in the stream, starting from 0.

commit_position1 = client.append_to_stream(
    stream_name=stream_name1,
    current_version=StreamState.NO_STREAM,
    events=[event1, event2],
)

# Append events to an existing stream. The "current version"
# is the "stream position" of the last recorded event in a
# stream. We have recorded two new events, so the "current
# version" is 1. The exception 'WrongCurrentVersionError' will be
# raised if an incorrect value is given.

commit_position2 = client.append_to_stream(
    stream_name=stream_name1,
    current_version=1,
    events=[event3],
)

# - allocated commit positions increase monotonically
assert commit_position2 > commit_position1

# Get events recorded in a stream. This method returns
# a sequence of recorded event objects. The recorded
# event objects may be deserialized to domain event
# objects of different types and used to reconstruct
# an aggregate in a domain model.

recorded_events = client.get_stream(
    stream_name=stream_name1
)

# - stream 'stream_name1' now has three events
assert len(recorded_events) == 3

# - allocated stream positions are zero-based and gapless
assert recorded_events[0].stream_position == 0
assert recorded_events[1].stream_position == 1
assert recorded_events[2].stream_position == 2

# - event attribute values are recorded faithfully
assert recorded_events[0].type == "OrderCreated"
assert recorded_events[0].data == b'{"order_number": "123456"}'
assert recorded_events[0].id == event1.id

assert recorded_events[1].type == "OrderSubmitted"
assert recorded_events[1].data == b'{}'
assert recorded_events[1].id == event2.id

assert recorded_events[2].type == "OrderCancelled"
assert recorded_events[2].data == b'{}'
assert recorded_events[2].id == event3.id

# Start a catch-up subscription from last recorded position.
# This method returns a "catch-up subscription" object,
# which can be iterated over to obtain recorded events.
# The iterator will not stop when there are no more recorded
# events to be returned, but instead will block, and then continue
# when further events are recorded. It can be used as a context
# manager so that the underlying streaming gRPC call to the database
# can be cancelled cleanly in case of any error.

received_events = []
with client.subscribe_to_all(commit_position=0) as subscription:
    # Iterate over the catch-up subscription. Process each recorded
    # event in turn. Within an atomic database transaction, record
    # the event's "commit position" along with any new state generated
    # by processing the event. Use the component's last recorded commit
    # position when restarting the catch-up subscription.

    for event in subscription:
        received_events.append(event)

        if event.commit_position == commit_position2:
            # Break so we can continue with the example.
            break

# - events are received in the order they were recorded
assert received_events[-3].type == "OrderCreated"
assert received_events[-3].data == b'{"order_number": "123456"}'
assert received_events[-3].id == event1.id

assert received_events[-2].type == "OrderSubmitted"
assert received_events[-2].data == b'{}'
assert received_events[-2].id == event2.id

assert received_events[-1].type == "OrderCancelled"
assert received_events[-1].data == b'{}'
assert received_events[-1].id == event3.id

# Close the client's gRPC connection.

client.close()
```


## Install package<a id="install-package"></a>

It is recommended to install Python packages into a Python virtual environment.

### From PyPI<a id="from-pypi"></a>

You can use pip to install this package directly from
[the Python Package Index](https://pypi.org/project/kurrentdbclient/).

    $ pip install kurrentdbclient

### With Poetry<a id="with-poetry"></a>

You can use Poetry to add this package to your pyproject.toml and install it.

    $ poetry add kurrentdbclient

## KurrentDB server<a id="kurrentdb-server"></a>

The KurrentDB server can be run locally using the official Docker container image.

### Run container<a id="run-container"></a>

For development, you can run a "secure" KurrentDB server using the following command.

    $ docker run -d --name kurrentdb-secure -it -p 2113:2113 --env "HOME=/tmp" docker.eventstore.com/kurrent-latest/kurrentdb:25.0.0-x64-8.0-bookworm-slim --dev

As we will see, your client will need a KurrentDB connection string URI as the value
of its `uri` constructor argument. The connection string for this "secure" KurrentDB
server would be:

    kurrentdb://admin:changeit@localhost:2113

To connect to a "secure" server, you will usually need to include a "username"
and a "password" in the connection string, so that the server can authenticate the
client. With KurrentDB, the default username is "admin" and the default password
is "changeit".

When connecting to a "secure" server, you may also need to provide an SSL/TLS certificate
as the value of the `root_certificates` constructor argument. If the server certificate
is publicly signed, the root certificates of the certificate authority may be installed
locally and picked up by the grpc package from a default location. The client uses the
root SSL/TLS certificate to authenticate the server. For development, you can either
use the SSL/TLS certificate of a self-signing certificate authority used to create the
server's certificate. Or, when using a single-node cluster, you can just use the server
certificate itself, getting the server certificate with the following Python code.

```python
import ssl

server_certificate = ssl.get_server_certificate(addr=('localhost', 2113))
```

Alternatively, you can start an "insecure" server using the following command.

    $ docker run -d --name kurrentdb-insecure -it -p 2113:2113 docker.eventstore.com/kurrent-latest/kurrentdb:25.0.0-x64-8.0-bookworm-slim --insecure

The connection string URI for this "insecure" server would be:

    kurrentdb://localhost:2113?Tls=false

As we will see, when connecting to an "insecure" server, there is no need to include
a "username" and a "password" in the connection string. If you do, these values will
be ignored by the client, so that they are not sent over an insecure channel.

Please note, the "insecure" connection string uses a query string with the field-value
`Tls=false`. The value of this field is by default `true`.

### Stop container<a id="stop-container"></a>

To stop and remove the "secure" container, use the following Docker commands.

    $ docker stop kurrentdb-secure
	$ docker rm kurrentdb-secure

To stop and remove the "insecure" container, use the following Docker commands.

    $ docker stop kurrentdb-insecure
	$ docker rm kurrentdb-insecure


## KurrentDB client<a id="kurrentdb-client"></a>

This KurrentDB client is implemented in the `kurrentdbclient` package with
the `KurrentDBClient` class.

### Import class<a id="import-class"></a>

The `KurrentDBClient` class can be imported from the `kurrentdbclient` package.

```python
from kurrentdbclient import KurrentDBClient
```

### Construct client<a id="construct-client"></a>

The `KurrentDBClient` class has one required constructor argument, `uri`, and three
optional constructor argument, `root_certificates`, `private_key`, and `certificate_chain`.

The `uri` argument is expected to be a KurrentDB connection string URI that
conforms with the standard KurrentDB "kurrentdb" or "kurrentdb+discover" URI schemes.

The client must be configured to create a "secure" connection to a "secure" server,
or alternatively an "insecure" connection to an "insecure" server. By default, the
client will attempt to create a "secure" connection. And so, when connecting to an
"insecure" server, the connection string must specify that the client should attempt
to make an "insecure" connection by using the URI query string field-value `Tls=false`.

The optional `root_certificates` argument can be either a Python `str` or a Python `bytes`
object containing PEM encoded SSL/TLS certificate(s), and is used to authenticate the
server to the client. When connecting to an "insecure" service, the value of this
argument will be ignored. When connecting to a "secure" server, it may be necessary to
set this argument. Typically, the value of this argument would be the public certificate
of the certificate authority that was responsible for generating the certificate used by
the KurrentDB server. It is unnecessary to set this value in this case if certificate
authority certificates are installed locally, such that the Python grpc library can pick
them up from a default location. Alternatively, for development, you can use the server's
certificate itself. The value of this argument is passed directly to `grpc.ssl_channel_credentials()`.

An alternative way to supply the `root_certificates` argument is through the `tlsCaFile` field-value of the connection string URI query string (see below). If the `tlsCaFile` field-value is specified, the `root_certificates` argument will be ignored.

The optional `private_key` and `certificate_chain` arguments are both either a Python
`str` or a Python `bytes` object. These arguments may be used to authenticate the client
to the server. It is necessary to provide correct values for these arguments when connecting
to a "secure" server that is running the commercial edition of KurrentDB with the
User Certificates plugin enabled. The value of `private_key` should be the X.509 user
certificate's private key in PEM format. The value of `certificate_chain` should be the
X.509 user certificate itself in PEM format. The values of these arguments are passed
directly to `grpc.ssl_channel_credentials()`. When connecting to an "insecure" service,
the values of these arguments will be ignored. Please note, an alternative way of
supplying the client with a user certificate and private key is to use the `UserCertFile`
and `UserKeyFile` field-values of the connection string URI query string (see below).
If the `UserCertFile` field-value is specified, the `certificate_chain` argument will be
ignored. If the `UserKeyFile` field-value is specified, the `public_key` argument will be
ignored.

In the example below, constructor argument values for `uri` and `root_certificates` are
taken from the operating system environment.

```python
import os

client = KurrentDBClient(
    uri=os.getenv("KDB_URI"),
    root_certificates=os.getenv("KDB_ROOT_CERTIFICATES"),
)
```

## Connection strings<a id="connection-strings"></a>

A KurrentDB connection string is a URI that conforms with one of two possible
schemes: either the "kurrentdb" scheme, or the "kurrentdb+discover" scheme.

The syntax and semantics of the KurrentDB URI schemes are described below. The
syntax is defined using [EBNF](https://en.wikipedia.org/wiki/Extended_Backus–Naur_form).

Please note, also supported are "kdb" and "esdb" which are synonyms for the "kurrentdb"
scheme, and "kdb+discover" and "esdb+disover" which are synonyms for the "kdb+discover"
scheme.

### Two schemes<a id="two-schemes"></a>

The "kurrentdb" URI scheme can be defined in the following way.

    kurrentdb-uri = "kurrentdb://" , [ user-info , "@" ] , grpc-target, { "," , grpc-target } , [ "?" , query-string ] ;

In the "kurrentdb" URI scheme, after the optional user info string, there must be at least
one gRPC target. If there are several gRPC targets, they must be separated from each
other with the "," character.

Each gRPC target should indicate a KurrentDB gRPC server socket, all in the same
KurrentDB cluster, by specifying a host and a port number separated with the ":"
character. The host may be a hostname that can be resolved to an IP address, or an IP
address.

    grpc-target = ( hostname | ip-address ) , ":" , port-number ;

If there is one gRPC target, the client will simply attempt to connect to this
server, and it will use this connection when recording and retrieving events.

If there are two or more gRPC targets, the client will attempt to connect to the
Gossip API of each in turn, attempting to obtain information about the cluster from
it, until information about the cluster is obtained. A member of the cluster is then
selected by the client according to the "node preference" specified by the connection
string URI. The client will then close its connection and connect to the selected node
without the 'round robin' load balancing strategy. If the "node preference" is "leader",
and after connecting to a leader, if the leader becomes a follower, the client will
reconnect to the new leader.


The "kurrentdb+discover" URI scheme can be defined in the following way.

    kurrentdb-discover-uri = "kurrentdb+discover://" , [ user-info, "@" ] , cluster-domainname, [ ":" , port-number ] , [ "?" , query-string ] ;

In the "kurrentdb+discover" URI scheme, after the optional user info string, there should be
a domain name which identifies a cluster of KurrentDB servers. Individual nodes in
the cluster should be declared with DNS 'A' records.

The client will use the cluster domain name with the gRPC library's 'round robin' load
balancing strategy to call the Gossip APIs of addresses discovered from DNS 'A' records.
Information about the KurrentDB cluster is obtained from the Gossip API. A member of
the cluster is then selected by the client according to the "node preference" option.
The client will then close its connection and connect to the selected node without the
'round robin' load balancing strategy. If the "node preference" is "leader",
and after connecting to a leader, if the leader becomes a follower, the client will
reconnect to the new leader.

### User info string<a id="user-info-string"></a>

In both the "kurrentdb" and "kurrentdb+discover" schemes, the URI may include a user info string.
If it exists in the URI, the user info string must be separated from the rest of the URI
with the "@" character. The user info string must include a username and a password,
separated with the ":" character.

    user-info = username , ":" , password ;

The user info is sent by the client in a "basic auth" authorization header in each gRPC
call to a "secure" server. This authorization header is used by the server to authenticate
the client. The Python gRPC library does not allow call credentials to be transferred to
"insecure" servers.

### Query string<a id="query-string"></a>

In both the "kurrentdb" and "kurrentdb+discover" schemes, the optional query string must be one
or many field-value arguments, separated from each other with the "&" character.

    query-string = field-value, { "&", field-value } ;

Each field-value argument must be one of the supported fields, and an
appropriate value, separated with the "=" character.

    field-value = ( "Tls", "=" , "true" | "false" )
                | ( "TlsVerifyCert", "=" , "true" | "false" )
                | ( "ConnectionName", "=" , string )
                | ( "NodePreference", "=" , "leader" | "follower" | "readonlyreplica" | "random" )
                | ( "DefaultDeadline", "=" , integer )
                | ( "GossipTimeout", "=" , integer )
                | ( "MaxDiscoverAttempts", "=" , integer )
                | ( "DiscoveryInterval", "=" , integer )
                | ( "KeepAliveInterval", "=" , integer )
                | ( "KeepAliveTimeout", "=" , integer ) ;
                | ( "TlsCaFile", "=" , string ) ;
                | ( "UserCertFile", "=" , string ) ;
                | ( "UserKeyFile", "=" , string ) ;

The table below describes the query string field-values supported by this client.

| Field               | Value                                                                 | Description                                                                                                                                                       |
|---------------------|-----------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Tls                 | "true", "false" (default: "true")                                     | If "true" the client will create a "secure" gRPC channel. If "false" the client will create an "insecure" gRPC channel. This must match the server configuration. |
| TlsVerifyCert       | "true", "false" (default: "true")                                     | This value is currently ignored.                                                                                                                                  |
| ConnectionName      | string (default: auto-generated version-4 UUID)                       | Sent in call metadata for every call, to identify the client to the cluster.                                                                                      |
| NodePreference      | "leader", "follower", "readonlyreplica", "random" (default: "leader") | The node state preferred by the client. The client will select a node from the cluster info received from the Gossip API according to this preference.            |
| DefaultDeadline     | integer (default: `None`)                                             | The default value (in seconds) of the `timeout` argument of client "write" methods such as `append_to_stream()`.                                                  |
| GossipTimeout       | integer (default: 5)                                                  | The default value (in seconds) of the `timeout` argument of gossip read methods, such as `read_gossip()`.                                                         |
| MaxDiscoverAttempts | integer (default: 10)                                                 | The number of attempts to read gossip when connecting or reconnecting to a cluster member.                                                                        |
| DiscoveryInterval   | integer (default: 100)                                                | How long to wait (in milliseconds) between gossip retries.                                                                                                        |
| KeepAliveInterval   | integer (default: `None`)                                             | The value (in milliseconds) of the "grpc.keepalive_ms" gRPC channel option.                                                                                       |
| KeepAliveTimeout    | integer (default: `None`)                                             | The value (in milliseconds) of the "grpc.keepalive_timeout_ms" gRPC channel option.                                                                               |
| TlsCaFile           | string (default: `None`)                                              | Absolute filesystem path to file containing the CA certicate in PEM format. This will be used to verify the server's certificate.                                 |
| UserCertFile        | string (default: `None`)                                              | Absolute filesystem path to file containing the X.509 user certificate in PEM format.                                                                             |
| UserKeyFile         | string (default: `None`)                                              | Absolute filesystem path to file containing the X.509 user certificate's private key in PEM format.                                                               |


Please note, the client is insensitive to the case of fields and values. If fields are
repeated in the query string, the query string will be parsed without error. However,
the connection options used by the client will use the value of the first field. All
the other field-values in the query string with the same field name will be ignored.
Fields without values will also be ignored.

If the client's node preference is "follower" and there are no follower
nodes in the cluster, then the client will raise an exception. Similarly, if the
client's node preference is "readonlyreplica" and there are no read-only replica
nodes in the cluster, then the client will also raise an exception.

The gRPC channel option "grpc.max_receive_message_length" is automatically
configured to the value `17 * 1024 * 1024`. This value cannot be configured.


### Examples<a id="examples"></a>

Here are some examples of KurrentDB connection string URIs.

The following URI will cause the client to make an "insecure" connection to
gRPC target `'localhost:2113'`. Because the client's node preference is "follower",
methods that can be called on a follower should complete successfully, methods that
require a leader will raise a `NodeIsNotLeaderError` exception.

    kurrentdb://127.0.0.1:2113?Tls=false&NodePreference=follower

The following URI will cause the client to make an "insecure" connection to
gRPC target `'localhost:2113'`. Because the client's node preference is "leader",
if this node is not a leader, then a `NodeIsNotLeaderError` exception will be raised by
all methods.

    kurrentdb://127.0.0.1:2113?Tls=false&NodePreference=leader

The following URI will cause the client to make a "secure" connection to
gRPC target `'localhost:2113'` with username `'admin'` and password `'changeit'`
as the default call credentials when making calls to the KurrentDB gRPC API.
Because the client's node preference is "leader", by default, if this node is not
a leader, then a `NodeIsNotLeaderError` exception will be raised by all methods.

    kurrentdb://admin:changeit@localhost:2113

The following URI will cause the client to make "secure" connections, firstly to
get cluster info from either `'localhost:2111'`, or `'localhost:2112'`, or `'localhost:2113'`.
Because the client's node preference is "leader", the client will select the leader
node from the cluster info and reconnect to the leader. If the "leader" node becomes
a "follower" and another node becomes "leader", then the client will reconnect to the
new leader.

    kurrentdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?NodePreference=leader


The following URI will cause the client to make "secure" connections, firstly to
get cluster info from either `'localhost:2111'`, or `'localhost:2112'`, or `'localhost:2113'`.
Because the client's node preference is "follower", the client will select a follower
node from the cluster info and reconnect to this follower. Please note, if the "follower"
node becomes the "leader", the client will not reconnect to a follower -- such behavior
may be implemented in a future version of the client and server.

    kurrentdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?NodePreference=follower


The following URI will cause the client to make "secure" connections, firstly to get
cluster info from addresses in DNS 'A' records for `'cluster1.example.com'`, and then
to connect to a "leader" node. The client will use a default timeout
of 5 seconds when making calls to KurrentDB API "write" methods.

    kurrentdb+discover://admin:changeit@cluster1.example.com?DefaultDeadline=5


The following URI will cause the client to make "secure" connections, firstly to get
cluster info from addresses in DNS 'A' records for `'cluster1.example.com'`, and then
to connect to a "leader" node. It will configure gRPC connections with a "keep alive
interval" and a "keep alive timeout".

    kurrentdb+discover://admin:changeit@cluster1.example.com?KeepAliveInterval=10000&KeepAliveTimeout=10000


## Event objects<a id="event-objects"></a>

This package defines a `NewEvent` class and a `RecordedEvent` class. The
`NewEvent` class should be used when writing events to the database. The
`RecordedEvent` class is used when reading events from the database.

### New events<a id="new-events"></a>

The `NewEvent` class should be used when writing events to a KurrentDB database.
You will need to construct new event objects before calling `append_to_stream()`.

The `NewEvent` class is a frozen Python dataclass. It has two required constructor
arguments (`type` and `data`) and three optional constructor arguments (`metadata`,
`content_type` and `id`).

The required `type` argument is a Python `str`, used to describe the type of
domain event that is being recorded.

The required `data` argument is a Python `bytes` object, used to state the
serialized data of the domain event that is being recorded.

The optional `metadata` argument is a Python `bytes` object, used to indicate any
metadata of the event that will be recorded. The default value is an empty `bytes`
object.

The optional `content_type` argument is a Python `str`, used to indicate the
kind of data that is being recorded. The default value is `'application/json'`,
which indicates that the `data` was serialised using JSON. An alternative value
for this argument is the more general indication `'application/octet-stream'`.

The optional `id` argument is a Python `UUID` object, used to specify the unique ID
of the event that will be recorded. If no value is provided, a new version-4 UUID
will be generated.

```python
new_event1 = NewEvent(
    type='OrderCreated',
    data=b'{"name": "Greg"}',
)
assert new_event1.type == 'OrderCreated'
assert new_event1.data == b'{"name": "Greg"}'
assert new_event1.metadata == b''
assert new_event1.content_type == 'application/json'
assert isinstance(new_event1.id, uuid.UUID)

event_id = uuid.uuid4()
new_event2 = NewEvent(
    type='ImageCreated',
    data=b'01010101010101',
    metadata=b'{"a": 1}',
    content_type='application/octet-stream',
    id=event_id,
)
assert new_event2.type == 'ImageCreated'
assert new_event2.data == b'01010101010101'
assert new_event2.metadata == b'{"a": 1}'
assert new_event2.content_type == 'application/octet-stream'
assert new_event2.id == event_id
```

### Recorded events<a id="recorded-events"></a>

The `RecordedEvent` class is used when reading events from a KurrentDB
database. The client will return event objects of this type from all methods
that return recorded events, such as `get_stream()`, `subscribe_to_all()`,
and `read_subscription_to_all()`. You do not need to construct recorded event objects.

Like `NewEvent`, the `RecordedEvent` class is a frozen Python dataclass. It has
all the attributes that `NewEvent` has (`type`, `data`, `metadata`, `content_type`, `id`)
that follow from an event having been recorded, and some additional attributes that follow
from the recording of an event (`stream_name`, `stream_position`, `commit_position`,
`recorded_at`). It also has a `link` attribute, which is `None` unless the recorded
event is a "link event" that has been "resolved" to the linked event. And it has a
`retry_count` which has an integer value when receiving recorded events from persistence
subscriptions, otherwise the value of `retry_count` is `None`.

The `type` attribute is a Python `str`, used to indicate the type of an event
that was recorded.

The `data` attribute is a Python `bytes` object, used to indicate the data of an
event that was recorded.

The `metadata` attribute is a Python `bytes` object, used to indicate the metadata of
an event that was recorded.

The `content_type` attribute is a Python `str`, used to indicate the type of
data that was recorded for an event. It is usually `'application/json'`, indicating
that the data can be parsed as JSON. Alternatively, it is `'application/octet-stream'`.

The `id` attribute is a Python `UUID` object, used to indicate the unique ID of an
event that was recorded.

The `stream_name` attribute is a Python `str`, used to indicate the name of a
stream in which an event was recorded.

The `stream_position` attribute is a Python `int`, used to indicate the position in a
stream at which an event was recorded.

In KurrentDB, a "stream position" is an integer representing the position of a
recorded event in a stream. Each recorded event is recorded at a position in a stream.
Each stream position is occupied by only one recorded event. New events are recorded at the
next unoccupied position. All sequences of stream positions are zero-based and gapless.

The `commit_position` attribute is a Python `int`, used to indicate the position in the
database at which an event was recorded.

In KurrentDB, a "commit position" is an integer representing the position of a
recorded event in the database. Each recorded event is recorded at a position in the
database. Each commit position is occupied by only one recorded event. Commit positions
are zero-based and increase monotonically as new events are recorded. But, unlike stream
positions, the sequence of successive commit positions is not gapless. Indeed, there are
usually large differences between the commit positions of successively recorded events.

The `recorded_at` attribute is a Python `datetime`, used to indicate when an event was
recorded by the database.

The `link` attribute is an optional `RecordedEvent` that carries information about
a "link event" that has been "resolved" to the linked event. This allows access to
the link event attributes when link events have been resolved, for example access
to the correct event ID to be used when acknowledging or negatively acknowledging
link events. Link events are "resolved" when the `resolve_links` argument is `True`
and when replaying parked events (negatively acknowledging an event received from
a persistent subscription with the `'park'` action will create a link event, and
when parked events are replayed they are received as resolved events). The
`ack_id` property helps with obtaining the correct event ID to use when acknowledging
or negatively acknowledging events received from persistent subscriptions.

The `retry_count` is a Python `int`, used to indicate the number of times a persistent
subscription has retried sending the event to a consumer.

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class RecordedEvent:
    """
    Encapsulates event data that has been recorded in KurrentDB.
    """

    type: str
    data: bytes
    metadata: bytes
    content_type: str
    id: UUID
    stream_name: str
    stream_position: int
    commit_position: Optional[int]
    recorded_at: Optional[datetime] = None
    link: Optional["RecordedEvent"] = None
    retry_count: Optional[int] = None

    @property
    def ack_id(self) -> UUID:
        if self.link is not None:
            return self.link.id
        else:
            return self.id

    @property
    def is_system_event(self) -> bool:
        return self.type.startswith("$")

    @property
    def is_link_event(self) -> bool:
        return self.type == "$>"

    @property
    def is_resolved_event(self) -> bool:
        return self.link is not None

    @property
    def is_checkpoint(self) -> bool:
        return False
```

The property `ack_id` can be used to obtain the correct event ID to `ack()` or `nack()`
events received when reading persistent subscriptions. The returned value is either the
value of the `id` attribute of the `link` attribute, if `link` is not `None`, otherwise
it is the value of the `id` attribute.

The property `is_system_event` indicates whether the event is a "system event". System
events have a `type` value that starts with `'$'`.

The property `is_link_event` indicates whether the event is a "link event". Link
events have a `type` value of `'$>'`.

The property `is_resolve_event` indicates whether the event has been resolved from a
"link event". The returned value is `True` if `link` is not `None`.

The property `is_checkpoint` is `False`. This can be used to identify `Checkpoint`
instances returned when receiving events from `include_checkpoints=True`.



## Streams<a id="streams"></a>

In KurrentDB, a "stream" is a sequence of recorded events that all have
the same "stream name". There will normally be many streams in a database,
each with many recorded events. Each recorded event has a position in its stream
(the "stream position"), and a position in the database (the "commit position").
Stream positions are zero-based and gapless. Commit positions are also zero-based,
but are not gapless.

The methods `append_to_stream()`, `get_stream()` and `read_all()` can
be used to read and record in the database.

### Append events<a id="append-events"></a>

*requires leader*

The `append_to_stream()` method can be used atomically to record a sequence of new events.
If the operation is successful, it returns the commit position of the last event in the
sequence that has been recorded.

This method has three required arguments, `stream_name`, `current_version`
and `events`.

The required `stream_name` argument is a Python `str` that uniquely identifies a
stream to which a sequence of events will be appended.

The required `current_version` argument is expected to be either a Python `int`
that indicates the stream position of the last recorded event in the stream, or
`StreamState.NO_STREAM` if the stream does not yet exist or has been deleted. The
stream positions are zero-based and gapless, so that if a stream has two events, the
`current_version` should be 1. If an incorrect value is given, this method will raise a
`WrongCurrentVersionError` exception. This behavior is designed to provide concurrency
control when recording new events. The correct value of `current_version` for any stream
can be obtained by calling `get_current_version()`. However, the typical approach is to
reconstruct an aggregate from the recorded events, so that the version of the aggregate
is the stream position of the last recorded event, then have the aggregate generate new
events, and then use the current version of the aggregate as the value of the
`current_version` argument when appending the new aggregate events. This ensures
the consistency of the recorded aggregate events, because operations that generate
new aggregate events can be retried with a freshly reconstructed aggregate if
a `WrongCurrentVersionError` exception is encountered when recording new events. This
controlling behavior can be entirely disabled by setting the value of the `current_version`
argument to the constant `StreamState.ANY`. More selectively, this behaviour can be
disabled for existing streams by setting the value of the `current_version`
argument to the constant `StreamState.EXISTS`.

The required `events` argument is expected to be a sequence of new event objects. The
`NewEvent` class should be used to construct new event objects. The `append_to_stream()`
operation is atomic, so that either all or none of the new events will be recorded. It
is not possible with KurrentDB atomically to record new events in more than one stream.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, a new event, `event1`, is appended to a new stream. The stream
does not yet exist, so `current_version` is `StreamState.NO_STREAM`.

```python
# Construct a new event object.
event1 = NewEvent(type='OrderCreated', data=b'{}')

# Define a new stream name.
stream_name1 = str(uuid.uuid4())

# Append the new events to the new stream.
commit_position1 = client.append_to_stream(
    stream_name=stream_name1,
    current_version=StreamState.NO_STREAM,
    events=[event1],
)
```

In the example below, two subsequent events are appended to an existing
stream. The stream has one recorded event, so `current_version` is `0`.

```python
event2 = NewEvent(type='OrderUpdated', data=b'{}')
event3 = NewEvent(type='OrderDeleted', data=b'{}')

commit_position2 = client.append_to_stream(
    stream_name=stream_name1,
    current_version=0,
    events=[event2, event3],
)
```

The returned values, `commit_position1` and `commit_position2`, are the
commit positions in the database of the last events in the recorded sequences.
That is, `commit_position1` is the commit position of `event1` and
`commit_position2` is the commit position of `event3`.

Commit positions that are returned in this way can be used by a user interface to poll
a downstream component until it has processed all the newly recorded events. For example,
consider a user interface command that results in the recording of new events, and an
eventually consistent materialized view in a downstream component that is updated from
these events. If the new events have not yet been processed, the view might be stale,
or out-of-date. Instead of displaying a stale view, the user interface can poll the
downstream component until it has processed the newly recorded events, and then display
an up-to-date view to the user.


### Idempotent append operations<a id="idempotent-append-operations"></a>

The `append_to_stream()` method is "idempotent" with respect to the `id` value of a
`NewEvent` object. That is to say, if `append_to_stream()` is called with events
whose `id` values are equal to those already recorded in the stream, then the
method call will successfully return, with the commit position of the last new event,
without making any changes to the database.

This is because sometimes it may happen, when calling `append_to_stream()`, that the new
events are successfully recorded, but somehow something bad happens before the method call
can return successfully to the caller. In this case, we cannot be sure that the events have
in fact been recorded, and so we may wish to retry.

If the events were in fact successfully recorded, it is convenient for the retried method call
to return successfully, and without either raising an exception (when `current_version`
is either `StreamState.NO_STREAM` an integer value) or creating further event records
(when `current_version` is `StreamState.ANY` or `StreamState.EXISTS`), as it would
if the `append_to_stream()` method were not idempotent.

If the method call initially failed and the new events were not in fact recorded, it
makes good sense, when the method call is retried, that the new events are recorded
and that the method call returns successfully. If the concurrency controls have not been disabled,
that is if the `current version` is either `StreamState.NO_STREAM` or an integer value, and
if a `WrongCurrentVersionError` exception is raised when retrying the method call, then we can assume
both that the initial method call did not in fact successfully record the events, and also
that subsequent events have in the meantime been recorded by somebody else. In this case,
an application command which generated the new events may need to be executed again. And
the user of the application may need to be given an opportunity to decide if they still wish to
proceed with their original intention, by displaying a suitable error with an up-to-date view of
the recorded state. In the case where concurrency controls have been disabled, by using `StreamState.ANY` or
`StreamState.EXISTS` as the value of `current_version`, retrying a method call that failed to
return successfully will, more simply, just attempt to ensure the new events are recorded, regardless
of their resulting stream positions. In either case, when the method call does return successfully, we
can be sure the events have been recorded.

The example below shows the `append_to_stream()` method being called again with events
`event2` and `event3`, and with `current_version=0`. We can see that repeating the call
to `append_to_stream()` returns successfully without raising a `WrongCurrentVersionError`
exception, as it would if the `append_to_stream()` operation were not idempotent.

```python
# Retry appending event3.
commit_position_retry = client.append_to_stream(
    stream_name=stream_name1,
    current_version=0,
    events=[event2, event3],
)
```

We can see that the same commit position is returned as above.

```python
assert commit_position_retry == commit_position2
```

The example below shows the `append_to_stream()` method being called again with events
`event2` and `event3`, with and `current_version=StreamState.ANY`.

```python
# Retry appending event3.
commit_position_retry = client.append_to_stream(
    stream_name=stream_name1,
    current_version=0,
    events=[event2, event3],
)
```

We can see that the same commit position is returned as above.

```python
assert commit_position_retry == commit_position2
```

By calling `get_stream()`, we can also see the stream has been unchanged.
That is, there are still only three events in the stream.

```python
events = client.get_stream(
    stream_name=stream_name1
)

assert len(events) == 3
```

This idempotent behaviour depends on the `id` attribute of the `NewEvent` class.
This attribute is, by default, assigned a new and unique version-4 UUID when an
instance of `NewEvent` is constructed. To set the `id` value of a `NewEvent`,
the optional `id` constructor argument can be used when constructing `NewEvent` objects.


### Read stream events<a id="read-stream-events"></a>

The `read_stream()` method can be used to get events that have been appended
to a stream. This method returns a "read response" object.

A "read response" object is a Python iterator. Recorded events can be
obtained by iterating over the "read response" object. Recorded events are
streamed from the server to the client as the iteration proceeds. The iteration
will automatically stop when there are no more recorded events to be returned.
The streaming of events, and hence the iterator, can also be stopped by calling
the `stop()` method on the "read response" object.

The `get_stream()` method can be used to get events that have been appended
to a stream. This method returns a Python `tuple` of recorded event objects.
The recorded event objects are instances of the `RecordedEvent` class. It
calls `read_stream()` and passes the "read response" iterator into a Python
`tuple`, so that the streaming will complete before the method returns.

The `read_stream()` and `get_stream()` methods have one required argument, `stream_name`.

The required `stream_name` argument is a Python `str` that uniquely identifies a
stream from which recorded events will be returned.

The `read_stream()` and `get_stream()` methods also have six optional arguments,
`stream_position`, `backwards`, `resolve_links`, `limit`, `timeout`, and `credentials`.

The optional `stream_position` argument is a Python `int` that can be used to
indicate the position in the stream from which to start reading. The default value
of `stream_position` is `None`. When reading a stream from a specific position in the
stream, the recorded event at that position will be included, both when reading
forwards from that position, and when reading backwards.

The optional `backwards` argument is a Python `bool`. The default value of `backwards`
is `False`, which means the stream will be read forwards, so that events are returned
in the order they were recorded. If `backwards` is `True`, the events are returned in
reverse order.

If `backwards` is `False` and `stream_position` is `None`, the stream's events will be
returned in the order they were recorded, starting from the first recorded event. If
`backwards` is `True` and `stream_position` is `None`, the stream's events will be
returned in reverse order, starting from the last recorded event.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `limit` argument is a Python `int` which restricts the number of events
that will be returned. The default value of `limit` is `sys.maxint`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to override call credentials derived
from the connection string URI. A suitable value for this argument can be constructed
by calling the client method `construct_call_credentials()`.

The example below shows the default behavior, which is to return all the recorded
events of a stream forwards from the first recorded events to the last.

```python
events = client.get_stream(
    stream_name=stream_name1
)

assert len(events) == 3
assert events[0] == event1
assert events[1] == event2
assert events[2] == event3
```

The example below shows how to use the `stream_position` argument to read a stream
from a specific stream position to the end of the stream. Stream positions are
zero-based, and so `stream_position=1` corresponds to the second event that was
recorded in the stream, in this case `event2`.

```python
events = client.get_stream(
    stream_name=stream_name1,
    stream_position=1,
)

assert len(events) == 2
assert events[0] == event2
assert events[1] == event3
```

The example below shows how to use the `backwards` argument to read a stream backwards.

```python
events = client.get_stream(
    stream_name=stream_name1,
    backwards=True,
)

assert len(events) == 3
assert events[0] == event3
assert events[1] == event2
assert events[2] == event1
```

The example below shows how to use the `limit` argument to read a limited number of
events.

```python
events = client.get_stream(
    stream_name=stream_name1,
    limit=2,
)

assert len(events) == 2
assert events[0] == event1
assert events[1] == event2
```

The `read_stream()` and `get_stream()` methods will raise a `NotFoundError` exception if the
named stream has never existed or has been deleted.

```python
from kurrentdbclient.exceptions import NotFoundError

try:
    client.get_stream('does-not-exist')
except NotFoundError:
    pass  # The stream does not exist.
else:
    raise Exception("Shouldn't get here")
```

Please note, the `get_stream()` method is decorated with the `@autoreconnect` and
`@retrygrpc` decorators, whilst the `read_stream()` method is not. This means that
all errors due to connection issues will be caught by the retry and reconnect decorators
when calling the `get_stream()` method, but not when calling `read_stream()`. The
`read_stream()` method has no such decorators because the streaming only starts
when iterating over the "read response" starts, which means that the method returns
before the streaming starts, and so there is no chance for any decorators to catch
any connection issues.

For the same reason, `read_stream()` will not raise a `NotFoundError` exception when
the stream does not exist, until iterating over the "read response" object begins.

If you are reading a very large stream, then you might prefer to call `read_stream()`,
and begin iterating through the recorded events whilst they are being streamed from
the server, rather than both waiting and having them all accumulate in memory.

### Get current version<a id="get-current-version"></a>

The `get_current_version()` method is a convenience method that essentially calls
`get_stream()` with `backwards=True` and `limit=1`. This method returns
the value of the `stream_position` attribute of the last recorded event in a
stream. If a stream does not exist, the returned value is `StreamState.NO_STREAM`.
The returned value is the correct value of `current_version` when appending events
to a stream, and when deleting or tombstoning a stream.

This method has one required argument, `stream_name`.

The required `stream_name` argument is a Python `str` that uniquely identifies a
stream from which a stream position will be returned.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, the last stream position of `stream_name1` is obtained.
Since three events have been appended to `stream_name1`, and because positions
in a stream are zero-based and gapless, so the current version is `2`.

```python
current_version = client.get_current_version(
    stream_name=stream_name1
)

assert current_version == 2
```

If a stream has never existed or has been deleted, the returned value is
`StreamState.NO_STREAM`, which is the correct value of the `current_version`
argument both when appending the first event of a new stream, and also when
appending events to a stream that has been deleted.

```python
current_version = client.get_current_version(
    stream_name='does-not-exist'
)

assert current_version is StreamState.NO_STREAM
```

### How to implement snapshotting with KurrentDB<a id="how-to-implement-snapshotting-with-kurrentdb"></a>

Snapshots can improve the performance of aggregates that would otherwise be
reconstructed from very long streams. However, it is generally recommended to design
aggregates to have a finite lifecycle, and so to have relatively short streams,
thereby avoiding the need for snapshotting. This "how to" section is intended merely
to show how snapshotting of aggregates can be implemented with KurrentDB using
this Python client.

Event-sourced aggregates are typically reconstructed from recorded events by calling
a mutator function for each recorded event, evolving from an initial state
`None` to the current state of the aggregate. The function `get_aggregate()` shows
how this can be done. The aggregate ID is used as a stream name. The exception
`AggregateNotFoundError` is raised if the aggregate stream is not found.

```python
class AggregateNotFoundError(Exception):
    """Raised when an aggregate is not found."""


def get_aggregate(aggregate_id, mutator_func):
    stream_name = aggregate_id

    # Get recorded events.
    try:
        events = client.get_stream(
            stream_name=stream_name,
            stream_position=None
        )
    except NotFoundError as e:
        raise AggregateNotFoundError(aggregate_id) from e
    else:
        # Reconstruct aggregate from recorded events.
        aggregate = None
        for event in events:
            aggregate = mutator_func(aggregate, event)
        return aggregate
```

Snapshotting of aggregates can be implemented by recording the current state of
an aggregate as a new event.

If an aggregate object has a version number that corresponds to the stream position of
the last event that was used to reconstruct the aggregate, and this version number
is recorded in the snapshot metadata, then any events that are recorded after the
snapshot can be selected using this version number. The aggregate can then be
reconstructed from the last snapshot and any subsequent events, without having
to replay the entire history.

We will use a separate stream for an aggregate's snapshots that is named after the
stream used for recording its events. The name of the snapshot stream will be
constructed by prefixing the aggregate's stream name with `'snapshot-$'`.

```python
SNAPSHOT_STREAM_NAME_PREFIX = 'snapshot-$'

def make_snapshot_stream_name(stream_name):
    return f'{SNAPSHOT_STREAM_NAME_PREFIX}{stream_name}'


def remove_snapshot_stream_prefix(snapshot_stream_name):
    assert snapshot_stream_name.startswith(SNAPSHOT_STREAM_NAME_PREFIX)
    return snapshot_stream_name[len(SNAPSHOT_STREAM_NAME_PREFIX):]
```

Now, let's redefine the `get_aggregate()` function, so that it looks for a snapshot event,
then selects subsequent aggregate events, and then calls a mutator function for each
recorded event.

Notice that the aggregate events are read from a stream for serialized aggregate
events, whilst the snapshot is read from a separate stream for serialized aggregate
snapshots. We will use JSON to serialize and deserialize event data.


```python
import json


def get_aggregate(aggregate_id, mutator_func):
    stream_name = aggregate_id
    recorded_events = []

    # Look for a snapshot.
    try:
        snapshots = client.get_stream(
            stream_name=make_snapshot_stream_name(stream_name),
            backwards=True,
            limit=1
        )
    except NotFoundError:
        stream_position = None
    else:
        assert len(snapshots) == 1
        snapshot = snapshots[0]
        stream_position = deserialize(snapshot.metadata)['version'] + 1
        recorded_events.append(snapshot)

    # Get subsequent events.
    try:
        events = client.get_stream(
            stream_name=stream_name,
            stream_position=stream_position
        )
    except NotFoundError as e:
        raise AggregateNotFoundError(aggregate_id) from e
    else:
        recorded_events += events

    # Reconstruct aggregate from recorded events.
    aggregate = None
    for event in recorded_events:
        aggregate = mutator_func(aggregate, event)

    return aggregate


def serialize(d):
    return json.dumps(d).encode('utf8')


def deserialize(s):
    return json.loads(s.decode('utf8'))
```

To show how `get_aggregate()` can be used, let's define a `Dog` aggregate class, with
attributes `name` and `tricks`. The attributes `id` and `version` will indicate an
aggregate object's ID and version number. The attribute `is_from_snapshot` is added
here merely to demonstrate below when an aggregate object has been reconstructed using
a snapshot.

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class Aggregate:
    id: str
    version: int
    is_from_snapshot: bool


@dataclass(frozen=True)
class Dog(Aggregate):
    name: str
    tricks: list
```

Let's also define a mutator function `mutate_dog()` that evolves the state of a
`Dog` aggregate given various different types of events, `'DogRegistered'`,
`'DogLearnedTrick'`, and `'Snapshot'`.

```python
def mutate_dog(dog, event):
    data = deserialize(event.data)
    if event.type == 'DogRegistered':
        return Dog(
            id=event.stream_name,
            version=event.stream_position,
            is_from_snapshot=False,
            name=data['name'],
            tricks=[],
        )
    elif event.type == 'DogLearnedTrick':
        assert event.stream_position == dog.version + 1
        assert event.stream_name == dog.id, (event.stream_name, dog.id)
        return Dog(
            id=dog.id,
            version=event.stream_position,
            is_from_snapshot=dog.is_from_snapshot,
            name=dog.name,
            tricks=dog.tricks + [data['trick']],
        )
    elif event.type == 'Snapshot':
        return Dog(
            id=remove_snapshot_stream_prefix(event.stream_name),
            version=deserialize(event.metadata)['version'],
            is_from_snapshot=True,
            name=data['name'],
            tricks=data['tricks'],
        )
    else:
        raise Exception(f"Unknown event type: {event.type}")
```

For convenience, let's also define a `get_dog()` function that calls `get_aggregate()`
with the `mutate_dog()` function as the value of its `mutator_func` argument.

```python
def get_dog(dog_id):
    return get_aggregate(
        aggregate_id=dog_id,
        mutator_func=mutate_dog,
    )
```

We can also define some "command" functions that append new events to the
database. The `register_dog()` function appends a `DogRegistered` event. The
`record_trick_learned()` appends a `DogLearnedTrick` event. The function
`snapshot_dog()` appends a `Snapshot` event. Notice that the
`record_trick_learned()` and `snapshot_dog()` functions use `get_dog()`.

Notice also that the `DogRegistered` and `DogLearnedTrick` events are appended to a
stream for aggregate events, whilst the `Snapshot` event is appended to a separate
stream for aggregate snapshots.

```python
def register_dog(name):
    dog_id = str(uuid.uuid4())
    event = NewEvent(
        type='DogRegistered',
        data=serialize({'name': name}),
    )
    client.append_to_stream(
        stream_name=dog_id,
        current_version=StreamState.NO_STREAM,
        events=event,
    )
    return dog_id


def record_trick_learned(dog_id, trick):
    dog = get_dog(dog_id)
    event = NewEvent(
        type='DogLearnedTrick',
        data=serialize({'trick': trick}),
    )
    client.append_to_stream(
        stream_name=dog_id,
        current_version=dog.version,
        events=event,
    )


def snapshot_dog(dog_id):
    dog = get_dog(dog_id)
    event = NewEvent(
        type='Snapshot',
        data=serialize({'name': dog.name, 'tricks': dog.tricks}),
        metadata=serialize({'version': dog.version}),
    )
    client.append_to_stream(
        stream_name=make_snapshot_stream_name(dog_id),
        current_version=StreamState.ANY,
        events=event,
    )
```

We can call `register_dog()` to register a new dog.

```python
# Register a new dog.
dog_id = register_dog('Fido')

dog = get_dog(dog_id)
assert dog.name == 'Fido'
assert dog.tricks == []
assert dog.version == 0
assert dog.is_from_snapshot is False

```

We can call `record_trick_learned()` to record that some tricks have been learned.

```python

# Record that 'Fido' learned a new trick.
record_trick_learned(dog_id, trick='roll over')

dog = get_dog(dog_id)
assert dog.name == 'Fido'
assert dog.tricks == ['roll over']
assert dog.version == 1
assert dog.is_from_snapshot is False


# Record that 'Fido' learned another new trick.
record_trick_learned(dog_id, trick='fetch ball')

dog = get_dog(dog_id)
assert dog.name == 'Fido'
assert dog.tricks == ['roll over', 'fetch ball']
assert dog.version == 2
assert dog.is_from_snapshot is False
```

We can call `snapshot_dog()` to record a snapshot of the current state of the `Dog`
aggregate. After we call `snapshot_dog()`, the `get_dog()` function will return a `Dog`
object that has been constructed using the `Snapshot` event.

```python
# Snapshot 'Fido'.
snapshot_dog(dog_id)

dog = get_dog(dog_id)
assert dog.name == 'Fido'
assert dog.tricks == ['roll over', 'fetch ball']
assert dog.version == 2
assert dog.is_from_snapshot is True
```

We can continue to evolve the state of the `Dog` aggregate, using
the snapshot both during the call to `record_trick_learned()` and
when calling `get_dog()` directly.

```python
record_trick_learned(dog_id, trick='sit')

dog = get_dog(dog_id)
assert dog.name == 'Fido'
assert dog.tricks == ['roll over', 'fetch ball', 'sit']
assert dog.version == 3
assert dog.is_from_snapshot is True
```

We can see from the `is_from_snapshot` attribute that the `Dog` object was indeed
reconstructed from the snapshot.

Snapshots can be created at fixed version number intervals, fixed time
periods, after a particular type of event, immediately after events are
appended, or as a background process.


### Read all events<a id="read-all-events"></a>

The `read_all()` method can be used to get all recorded events
in the database in the order they were recorded. This method returns
a "read response" object, just like `read_stream()`.

A "read response" is an iterator, and not a sequence. Recorded events can be
obtained by iterating over the "read response" object. Recorded events are
streamed from the server to the client as the iteration proceeds. The iteration
will automatically stop when there are no more recorded events to be returned.
The streaming of events, and hence the iterator, can also be stopped by calling
the `stop()` method on the "read response" object. The recorded event objects
are instances of the `RecordedEvent` class.

This method has nine optional arguments, `commit_position`, `backwards`, `resolve_links`,
`filter_exclude`, `filter_include`, `filter_by_stream_name`, `limit`, `timeout`,
and `credentials`.

The optional `commit_position` argument is a Python `int` that can be used to
specify a commit position from which to start reading. The default value of
`commit_position` is `None`. Please note, if a commit position is specified,
it must be an actually existing commit position in the database. When reading
forwards, the event at the commit position may be included, depending upon the
filter. When reading backwards, the event at the commit position will not be
included.

The optional `backwards` argument is a Python `bool`. The default of `backwards` is
`False`, which means events are returned in the order they were recorded, If
`backwards` is `True`, then events are returned in reverse order.

If `backwards` is `False` and `commit_position` is `None`, the database's events will
be returned in the order they were recorded, starting from the first recorded event.
This is the default behavior of `read_all()`. If `backwards` is `True` and
`commit_position` is `None`, the database's events will be returned in reverse order,
starting from the last recorded event.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `filter_exclude` argument is a sequence of regular expressions that
specifies which recorded events should be returned. This argument is ignored
if `filter_include` is set to a non-empty sequence. The default value of this
argument matches the event types of KurrentDB "system events", so that system
events will not normally be included. See the Notes section below for more
information about filter expressions.

The optional `filter_include` argument is a sequence of regular expressions
that specifies which recorded events should be returned. By default, this
argument is an empty tuple. If this argument is set to a non-empty sequence,
the `filter_exclude` argument is ignored.

The optional `filter_by_stream_name` argument is a Python `bool` that indicates
whether the filtering will apply to event types or stream names. By default, this
value is `False` and so the filtering will apply to the event type strings of
recorded events.

The optional `limit` argument is an integer which restricts the number of events that
will be returned. The default value is `sys.maxint`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The filtering of events is done on the KurrentDB server. The
`limit` argument is applied on the server after filtering.

The example below shows how to get all the events we have recorded in the database
so far, in the order they were recorded. We can see the three events of `stream_name1`
(`event1`, `event2` and `event3`) are included, along with others.

```python
# Read all events (creates a streaming gRPC call).
read_response = client.read_all()

# Convert the iterator into a sequence of recorded events.
events = tuple(read_response)
assert len(events) > 3  # more than three

# Convert the sequence of recorded events into a set of event IDs.
event_ids = set(e.id for e in events)
assert event1.id in event_ids
assert event2.id in event_ids
assert event3.id in event_ids
```

The example below shows how to read all recorded events in the database from
a particular commit position, in this case `commit_position1`. When reading
forwards from a specific commit position, the event at the specified position
will be included. The value of `commit_position1` is the position we obtained
when appending `event1`. And so `event1` is the first recorded event we shall
receive, `event2` is the second, and `event3` is the third.

```python
# Read all events forwards from a commit position.
read_response = client.read_all(
    commit_position=commit_position1
)

# Step through the "read response" iterator.
assert next(read_response) == event1
assert next(read_response) == event2
assert next(read_response) == event3

# Stop the iterator.
read_response.stop()
```

The example below shows how to read all events recorded in the database in reverse
order. We can see that the first events we receive are the last events that were
recorded: the events of the `Dog` aggregate from the section about snapshotting
and the snapshot.

```python
# Read all events backwards from the end.
read_response = client.read_all(
    backwards=True
)

# Step through the "read response" iterator.
assert next(read_response).type == "DogLearnedTrick"
assert next(read_response).type == "Snapshot"
assert next(read_response).type == "DogLearnedTrick"
assert next(read_response).type == "DogLearnedTrick"
assert next(read_response).type == "DogRegistered"

# Stop the iterator.
read_response.stop()
```

The example below shows how to read a limited number of events
forwards from a specific commit position.

```python
events = tuple(
    client.read_all(
        commit_position=commit_position1,
        limit=1,
    )
)

assert len(events) == 1
assert events[0] == event1
```

The example below shows how to read a limited number of the recorded events
in the database backwards from the end. In this case, the limit is 1, and
so we receive the last recorded event.

```python
events = tuple(
    client.read_all(
        backwards=True,
        limit=1,
    )
)

assert len(events) == 1

assert events[0].type == 'DogLearnedTrick'
assert deserialize(events[0].data)['trick'] == 'sit'
```

Please note, like the `read_stream()` method, the `read_all()` method
is not decorated with retry and reconnect decorators, because the streaming of recorded
events from the server only starts when iterating over the "read response" starts, which
means that the method returns before the streaming starts, and so there is no chance for
any decorators to catch any connection issues.

### Get commit position<a id="get-commit-position"></a>

The `get_commit_position()` method can be used to get the commit position of the
last recorded event in the database. It simply calls `read_all()` with
`backwards=True` and `limit=1`, and returns the value of the `commit_position`
attribute of the last recorded event.

```python
commit_position = client.get_commit_position()
```

This method has five optional arguments, `filter_exclude`, `filter_include`,
`filter_by_stream_name`, `timeout` and `credentials`. These values are passed to
`read_all()`.

The optional `filter_exclude`, `filter_include` and `filter_by_stream_name` arguments
work in the same way as they do in the `read_all()` method.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to override call credentials
derived from the connection string URI.

This method might be used to measure progress of a downstream component
that is processing all recorded events, by comparing the current commit
position with the recorded commit position of the last successfully processed
event in a downstream component. In this case, the value of the `filter_exclude`,
`filter_include` and `filter_by_stream_name` arguments should equal those used
by the downstream component to obtain recorded events.


### Get stream metadata<a id="get-stream-metadata"></a>

The `get_stream_metadata()` method returns the metadata for a stream, along
with the version of the stream metadata.

This method has one required argument, `stream_name`, which is a Python `str` that
uniquely identifies a stream for which a stream metadata will be obtained.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, metadata for `stream_name1` is obtained.

```python
metadata, metadata_version = client.get_stream_metadata(stream_name=stream_name1)
```

The returned `metadata` value is a Python `dict`. The returned `metadata_version`
value is either an `int` if the stream exists, or `StreamState.NO_STREAM` if the stream
does not exist and no metadata has been set. These values can be used as the arguments
of `set_stream_metadata()`.

### Set stream metadata<a id="set-stream-metadata"></a>

*requires leader*

The method `set_stream_metadata()` sets metadata for a stream. Stream metadata
can be set before appending events to a stream.

This method has one required argument, `stream_name`, which is a Python `str` that
uniquely identifies a stream for which a stream metadata will be set.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, metadata for `stream_name1` is set.


```python
metadata["foo"] = "bar"

client.set_stream_metadata(
    stream_name=stream_name1,
    metadata=metadata,
    current_version=metadata_version,
)
```

The `current_version` argument should be the current version of the stream metadata
obtained from `get_stream_metadata()`.

Please refer to the KurrentDB documentation for more information about stream
metadata.

### Delete stream<a id="delete-stream"></a>

*requires leader*

The method `delete_stream()` can be used to "delete" a stream.

This method has two required arguments, `stream_name` and `current_version`.

The required `stream_name` argument is a Python `str` that uniquely identifies a
stream to which a sequence of events will be appended.

The required `current_version` argument is expected to be either a Python `int`
that indicates the stream position of the last recorded event in the stream.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, `stream_name1` is deleted.

```python
commit_position = client.delete_stream(stream_name=stream_name1, current_version=2)
```

After deleting a stream, it's still possible to append new events. Reading from a
deleted stream will return only events that have been appended after it was
deleted.

### Tombstone stream<a id="tombstone-stream"></a>

*requires leader*

The method `tombstone_stream()` can be used to "tombstone" a stream.

This method has two required arguments, `stream_name` and `current_version`.

The required `stream_name` argument is a Python `str` that uniquely identifies a
stream to which a sequence of events will be appended.

The required `current_version` argument is expected to be either a Python `int`
that indicates the stream position of the last recorded event in the stream.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, `stream_name1` is tombstoned.

```python
commit_position = client.tombstone_stream(stream_name=stream_name1, current_version=2)
```

After tombstoning a stream, it's not possible to append new events.


## Catch-up subscriptions<a id="catch-up-subscriptions"></a>

A "catch-up" subscription can be used to receive events that have already been
recorded and events that are recorded subsequently. A catch-up subscription can
be used by an event-processing component that processes recorded events with
"exactly-once" semantics.

The `subscribe_to_all()` method starts a catch-up subscription that can receive
all events in the database. The `subscribe_to_stream()` method starts a catch-up
subscription that can receive events from a specific stream. Both methods return a
"catch-up subscription" object, which is a Python iterator. Recorded events can be
obtained by iteration. Recorded event objects obtained in this way are instances
of the `RecordedEvent` class.

Before the "catch-up subscription" object is returned to the caller, the client will
firstly obtain a "confirmation" response from the server, which allows the client to
detect that both the gRPC connection and the streaming gRPC call is operational. For
this reason, the `subscribe_to_all()` and `subscribe_to_stream()` methods are both
usefully decorated with the reconnect and retry decorators. However, once the method
has returned, the decorators will have exited, and any exceptions that are raised
due to connection issues whilst iterating over the subscription object will have to
be handled by your code.

A "catch-up subscription" iterator will not automatically stop when there are no more
events to be returned, but instead the iteration will block until new events are
subsequently recorded in the database. Any subsequently recorded events will then be
immediately streamed to the client, and the iteration will then continue. The streaming
of events, and hence the iteration, can be stopped by calling the `stop()` method on the
"catch-up subscription" object.

### Subscribe to all events<a id="subscribe-to-all-events"></a>

The`subscribe_to_all()` method can be used to start a catch-up subscription
from which all events recorded in the database can be obtained in the order
they were recorded. This method returns a "catch-up subscription" iterator.

This method also has ten optional arguments, `commit_position`, `from_end`, `resolve_links`,
`filter_exclude`, `filter_include`, `filter_by_stream_name`, `include_checkpoints`,
`include_caught_up`, `timeout` and `credentials`.

The optional `commit_position` argument specifies a commit position. The default
value of `commit_position` is `None`, which means the catch-up subscription will
start from the first recorded event in the database. If a commit position is given,
it must match an actually existing commit position in the database. Only events
recorded after that position will be obtained.

The optional `from_end` argument specifies whether or not the catch-up subscription
will start from the last recorded event in the database. By default, this argument
is `False`. If `from_end` is `True`, only events recorded after the subscription
is started will be obtained. This argument will be disregarded if `commit_position`
is not `None`.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `filter_exclude` argument is a sequence of regular expressions that
specifies which recorded events should be returned. This argument is ignored
if `filter_include` is set to a non-empty sequence. The default value of this
argument matches the event types of KurrentDB "system events", so that system
events will not normally be included. See the Notes section below for more
information about filter expressions.

The optional `filter_include` argument is a sequence of regular expressions
that specifies which recorded events should be returned. By default, this
argument is an empty tuple. If this argument is set to a non-empty sequence,
the `filter_exclude` argument is ignored.

The optional `filter_by_stream_name` argument is a Python `bool` that indicates
whether the filtering will apply to event types or stream names. By default, this
value is `False` and so the filtering will apply to the event type strings of
recorded events.

The optional `include_checkpoints` argument is a Python `bool` which indicates
whether "checkpoint" messages should be included when recorded events are received.
Checkpoints have a `commit_position` value that can be used by an event processing component to
update its recorded commit position value, so that, when lots of events are being
filter out, the subscriber does not have to start from the same old position when
the event processing component is restarted.

The optional `include_caught_up` argument is a Python `bool` which indicates
whether "caught up" messages should be included when recorded events are
received. The default value of `include_caught_up` is `False`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The example below shows how to start a catch-up subscription that starts
from the first recorded event in the database.

```python
# Subscribe from the first recorded event in the database.
catchup_subscription = client.subscribe_to_all()
```

The example below shows that catch-up subscriptions do not stop
automatically, but block when the last recorded event is received,
and then continue when subsequent events are recorded.

```python
from time import sleep
from threading import Thread


# Append a new event to a new stream.
stream_name2 = str(uuid.uuid4())
event4 = NewEvent(type='OrderCreated', data=b'{}')

client.append_to_stream(
    stream_name=stream_name2,
    current_version=StreamState.NO_STREAM,
    events=[event4],
)


# Receive events from the catch-up subscription in a different thread.
received_events = []

def receive_events():
    for event in catchup_subscription:
        received_events.append(event)


def wait_for_event(event):
    for _ in range(100):
        for received in reversed(received_events):
            if event == received:
                return
        else:
            sleep(0.1)
    else:
        raise AssertionError("Event wasn't received")


thread = Thread(target=receive_events, daemon=True)
thread.start()

# Wait to receive event4.
wait_for_event(event4)

# Append another event whilst the subscription is running.
event5 = NewEvent(type='OrderUpdated', data=b'{}')
client.append_to_stream(
    stream_name=stream_name2,
    current_version=0,
    events=[event5],
)

# Wait for the subscription to block.
wait_for_event(event5)

# Stop the subscription.
catchup_subscription.stop()
thread.join()
```

The example below shows how to subscribe to events recorded after a
particular commit position, in this case from the commit position of
the last recorded event that was received above. Then, another event is
recorded before the subscription is restarted. And three more events are
recorded whilst the subscription is running. These four events are
received in the order they were recorded.


```python

# Append another event.
event6 = NewEvent(type='OrderDeleted', data=b'{}')
client.append_to_stream(
    stream_name=stream_name2,
    current_version=1,
    events=[event6],
)

# Restart subscribing to all events after the
# commit position of the last received event.
catchup_subscription = client.subscribe_to_all(
    commit_position=received_events[-1].commit_position
)

thread = Thread(target=receive_events, daemon=True)
thread.start()

# Wait for event6.
wait_for_event(event6)

# Append three more events to a new stream.
stream_name3 = str(uuid.uuid4())
event7 = NewEvent(type='OrderCreated', data=b'{}')
event8 = NewEvent(type='OrderUpdated', data=b'{}')
event9 = NewEvent(type='OrderDeleted', data=b'{}')

client.append_to_stream(
    stream_name=stream_name3,
    current_version=StreamState.NO_STREAM,
    events=[event7, event8, event9],
)

# Wait for events 7, 8 and 9.
wait_for_event(event7)
wait_for_event(event8)
wait_for_event(event9)

# Stop the subscription.
catchup_subscription.stop()
thread.join()
```

The catch-up subscription call is ended as soon as the subscription object's
`stop()` method is called. This happens automatically when it goes out of scope,
or when it is explicitly deleted from memory using the Python `del` keyword.

### Subscribe to stream events<a id="subscribe-to-stream-events"></a>

The `subscribe_to_stream()` method can be used to start a catch-up subscription
from which events recorded in a single stream can be obtained. This method
returns a "catch-up subscription" iterator.

This method has a required `stream_name` argument, which specifies the name of the
stream from which recorded events will be received.

This method also has six optional arguments, `stream_position`, `from_end`,
`resolve_links`, `include_caught_up`, `timeout` and `credentials`.

The optional `stream_position` argument specifies a position in the stream from
which to start subscribing. The default value of `stream_position` is `None`,
which means that all events recorded in the stream will be obtained in the
order they were recorded, unless `from_end` is set to `True`. If a stream
position is given, then only events recorded after that position will be obtained.

The optional `from_end` argument specifies that the subscription will start
from the last position in the stream. The default value of `from_end` is `False`.
If `from_end` is `True`, then only events recorded after the subscription was
created will be obtained. This argument if ignored is `stream_position` is set.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `include_caught_up` argument is a Python `bool` which indicates
whether "caught up" messages should be included when recorded events are
received. The default value of `include_caught_up` is `False`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The example below shows how to start a catch-up subscription from
the first recorded event in a stream.

```python
# Subscribe from the start of 'stream2'.
subscription = client.subscribe_to_stream(stream_name=stream_name2)
```

The example below shows how to start a catch-up subscription from
a particular stream position.

```python
# Subscribe to stream2, from the second recorded event.
subscription = client.subscribe_to_stream(
    stream_name=stream_name2,
    stream_position=1,
)
```

### How to implement exactly-once event processing<a id="how-to-implement-exactly-once-event-processing"></a>

The commit positions of recorded events that are received and processed by a
downstream component are usefully recorded by the downstream component, so that
the commit position of last processed event can be determined when processing is
resumed.

The last recorded commit position can be used to specify the commit position from which
to subscribe when processing is resumed. Since this commit position will represent the
position of the last successfully processed event in a downstream component, so it
will be usual to want the next event after this position, because that is the next
event that has not yet been processed. For this reason, when subscribing for events
from a specific commit position using a catch-up subscription in KurrentDB, the
recorded event at the specified commit position will NOT be included in the sequence
of recorded events that are received.

To accomplish "exactly-once" processing of recorded events in a downstream
component when using a catch-up subscription, the commit position of a recorded
event should be recorded atomically and uniquely along with the result of processing
recorded events, for example in the same database as materialised views when
implementing eventually-consistent CQRS, or in the same database as a downstream
analytics or reporting or archiving application. By recording the commit position
of recorded events atomically with the new state that results from processing
recorded events, "dual writing" in the consumption of recorded events can be
avoided. By also recording the commit position uniquely, the new state cannot be
recorded twice, and hence the recorded state of the downstream component will be
updated only once for any recorded event. By using the greatest recorded commit
position to resume a catch-up subscription, all recorded events will eventually
be processed. The combination of the "at-most-once" condition and the "at-least-once"
condition gives the "exactly-once" condition.

The danger with "dual writing" in the consumption of recorded events is that if a
recorded event is successfully processed and new state recorded atomically in one
transaction with the commit position recorded in a separate transaction, one may
happen and not the other. If the new state is recorded but the position is lost,
and then the processing is stopped and resumed, the recorded event may be processed
twice. On the other hand, if the commit position is recorded but the new state is
lost, the recorded event may effectively not be processed at all. By either
processing an event more than once, or by failing to process an event, the recorded
state of the downstream component might be inaccurate, or possibly inconsistent, and
perhaps catastrophically so. Such consequences may or may not matter in your situation.
But sometimes inconsistencies may halt processing until the issue is resolved. You can
avoid "dual writing" in the consumption of events by atomically recording the commit
position of a recorded event along with the new state that results from processing that
event in the same atomic transaction. By making the recording of the commit positions
unique, so that transactions will be rolled back when there is a conflict, you will
prevent the results of any duplicate processing of a recorded event being committed.

Recorded events received from a catch-up subscription cannot be acknowledged back
to the KurrentDB server. Acknowledging events, however, is an aspect of "persistent
subscriptions". Hoping to rely on acknowledging events to an upstream
component is an example of dual writing.


## Persistent subscriptions<a id="persistent-subscriptions"></a>

In KurrentDB, "persistent" subscriptions are similar to catch-up subscriptions,
in that reading a persistent subscription will block when there are no more recorded
events to be received, and then continue when new events are subsequently recorded.

Persistent subscriptions can be consumed by a group of consumers operating with one
of the supported "consumer strategies".

The significant different with persistent subscriptions is the server will keep track
of the progress of the consumers. The consumer of a persistent subscription will
therefore need to "acknowledge" when a recorded event has been processed successfully,
and otherwise "negatively acknowledge" a recorded event that has been received but was
not successfully processed.

All of this means that for persistent subscriptions there are "create", "read", "update"
"delete", "ack", and "nack" operations to consider.

Whilst there are some advantages of persistent subscriptions, in particular the
concurrent processing of recorded events by a group of consumers, by tracking in
the server the position in the commit sequence of events that have been processed,
the issue of "dual writing" in the consumption of events arises. Reliability in the
processing of recorded events by a group of persistent subscription consumers will
rely on their idempotent handling of duplicate messages, and their resilience to
out-of-order delivery.


### Create subscription to all<a id="create-subscription-to-all"></a>

*requires leader*

The `create_subscription_to_all()` method can be used to create a "persistent subscription"
to all the recorded events in the database across all streams.

This method has a required `group_name` argument, which is the
name of a "group" of consumers of the subscription.

This method has nineteen optional arguments, `from_end`, `commit_position`, `resolve_links`,
`filter_exclude`, `filter_include`, `filter_by_stream_name`, `consumer_strategy`,
`message_timeout`, `max_retry_count`, `min_checkpoint_count`, `max_checkpoint_count`,
`checkpoint_after`, `max_subscriber_count`, `live_buffer_size`, `read_batch_size`,
`history_buffer_size`, `extra_statistics`, `timeout` and `credentials`.

The optional `from_end` argument can be used to specify that the group of consumers
of the subscription should only receive events that were recorded after the subscription
was created.

Alternatively, the optional `commit_position` argument can be used to specify a commit
position from which the group of consumers of the subscription should
receive events. Please note, the recorded event at the specified commit position might
be included in the recorded events received by the group of consumers.

If neither `from_end` nor `commit_position` are specified, the group of consumers
of the subscription will potentially receive all recorded events in the database.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `filter_exclude` argument is a sequence of regular expressions that
specifies which recorded events should be returned. This argument is ignored
if `filter_include` is set to a non-empty sequence. The default value of this
argument matches the event types of KurrentDB "system events", so that system
events will not normally be included. See the Notes section below for more
information about filter expressions.

The optional `filter_include` argument is a sequence of regular expressions
that specifies which recorded events should be returned. By default, this
argument is an empty tuple. If this argument is set to a non-empty sequence,
the `filter_exclude` argument is ignored.

The optional `filter_by_stream_name` argument is a Python `bool` that indicates
whether the filtering will apply to event types or stream names. By default, this
value is `False` and so the filtering will apply to the event type strings of
recorded events.

The optional `consumer_strategy` argument is a Python `str` that defines
the consumer strategy for this persistent subscription. The value of this argument
can be `'DispatchToSingle'`, `'RoundRobin'`, `'Pinned'`, or `'PinnedByCorrelation'`. The
default value is `'DispatchToSingle'`.

The optional `message_timeout` argument is a Python `float` which sets a maximum duration,
in seconds, from the server sending a recorded event to a consumer of the persistent
subscription until either an "acknowledgement" (ack) or a "negative acknowledgement"
(nack) is received by the server, after which the server will retry to send the event.
The default value of `message_timeout` is `30.0`.

The optional `max_retry_count` argument is a Python `int` which sets the number of times
the server will retry to send an event. The default value of `max_retry_count` is `10`.

The optional `min_checkpoint_count` argument is a Python `int` which sets the minimum
number of "acknowledgements" (acks) received by the server before the server may record
the acknowledgements. The default value of `min_checkpoint_count` is `10`.

The optional `max_checkpoint_count` argument is a Python `int` which sets the maximum
number of "acknowledgements" (acks) received by the server before the server must
record the acknowledgements. The default value of `max_checkpoint_count` is `1000`.

The optional `checkpoint_after` argument is a Python `float` which sets the maximum
duration in seconds between recording "acknowledgements" (acks). The default value of
`checkpoint_after` is `2.0`.

The optional `max_subscriber_count` argument is a Python `int` which sets the maximum
number of concurrent readers of the persistent subscription, beyond which attempts to
read the persistent subscription will raise a `MaximumSubscriptionsReachedError` exception.

The optional `live_buffer_size` argument is a Python `int` which sets the size of the
buffer (in-memory) holding newly recorded events. The default value of `live_buffer_size`
is 500.

The optional `read_batch_size` argument is a Python `int` which sets the number of
recorded events read from disk when catching up. The default value of `read_batch_size`
is 200.

The optional `history_buffer_size` argument is a Python `int` which sets the number of
recorded events to cache in memory when catching up. The default value of `history_buffer_size`
is 500.

The optional `extra_statistics` argument is a Python `bool` which enables tracking of
extra statistics on this subscription. The default value of `extra_statistics` is `False`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The method `create_subscription_to_all()` does not return a value. Recorded events are
obtained by calling the `read_subscription_to_all()` method.

In the example below, a persistent subscription is created to operate from the
first recorded non-system event in the database.

```python
# Create a persistent subscription.
group_name1 = f"group-{uuid.uuid4()}"
client.create_subscription_to_all(group_name=group_name1)
```

### Read subscription to all<a id="read-subscription-to-all"></a>

*requires leader*

The `read_subscription_to_all()` method can be used by a group of consumers to receive
recorded events from a persistent subscription that has been created using
the `create_subscription_to_all()` method.

This method has a required `group_name` argument, which is
the name of a "group" of consumers of the subscription specified
when `create_subscription_to_all()` was called.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

This method returns a `PersistentSubscription` object, which is an iterator
giving `RecordedEvent` objects. It also has `ack()`, `nack()` and `stop()`
methods.

```python
subscription = client.read_subscription_to_all(group_name=group_name1)
```

The `ack()` method should be used by a consumer to "acknowledge" to the server that
it has received and successfully processed a recorded event. This will prevent that
recorded event being received by another consumer in the same group. The `ack()`
has an `item` argument which can be either a `RecordedEvent` or a `UUID`. If you pass
in a `RecordedEvent`, the value of its `ack_id` attribute will be used to acknowledge
the event to the server. If you pass in a UUID, then used the value of the `ack_id`
of the `RecordedEvent` that is being acknowledged, in case the event has been resolved
from a link event (which can happen both when persistent subscription setting
`resolve_links` is `True` and also when replaying parked events regardless of the
`resolve_links` setting).

The example below iterates over the subscription object, and calls `ack()` with the
received `RecordedEvent` objects. The subscription's `stop()` method is called when
we have received `event9`, stopping the iteration, so that we can continue with the
examples below.

```python
received_events = []

for event in subscription:
    received_events.append(event)

    # Acknowledge the received event.
    subscription.ack(event)

    # Stop when 'event9' has been received.
    if event == event9:
        subscription.stop()
```

The `nack()` should be used by a consumer to "negatively acknowledge" to the server that
it has received but not successfully processed a recorded event. The `nack()` method has
an `item` argument that works in the same way as `ack()`. Use the recorded event or its
`ack_id` attribute. The `nack()` method also has an `action` argument, which should be
a Python `str`: either `'unknown'`, `'park'`, `'retry'`, `'skip'` or `'stop'`.

The `stop()` method can be used to stop the gRPC streaming operation.

### How to write a persistent subscription consumer<a id="how-to-write-a-persistent-subscription-consumer"></a>

The reading of a persistent subscription can be encapsulated in a "consumer" that calls
a "policy" function when a recorded event is received and then automatically calls
`ack()` if the policy function returns normally, and `nack()` if it raises an exception,
perhaps retrying the event for a certain number of times before parking the event.

The simple example below shows how this might be done. We can see that 'event9' is
acknowledged before 'event5' is finally parked.

The  number of time a `RecordedEvent` has been retried is presented by the its
`retry_count` attribute.

```python
acked_events = {}
nacked_events = {}


class ExampleConsumer:
    def __init__(self, subscription, max_retry_count, final_action):
        self.subscription = subscription
        self.max_retry_count = max_retry_count
        self.final_action = final_action
        self.error = None

    def run(self):
        with self.subscription:
            for event in self.subscription:
                try:
                    self.policy(event)
                except Exception:
                    if event.retry_count < self.max_retry_count:
                        action = "retry"
                    else:
                        action = self.final_action
                    self.subscription.nack(event, action)
                    self.after_nack(event, action)
                else:
                    self.subscription.ack(event)
                    self.after_ack(event)

    def stop(self):
        self.subscription.stop()

    def policy(self, event):
        # Raise an exception when we see "event5".
        if event == event5:
            raise Exception()

    def after_ack(self, event):
        # Track retry count of acked events.
        acked_events[event.id] = event.retry_count

    def after_nack(self, event, action):
        # Track retry count of nacked events.
        nacked_events[event.id] = event.retry_count

        if action == self.final_action:
            # Stop the consumer, so we can continue with the examples.
            self.stop()


# Create subscription.
group_name = f"group-{uuid.uuid4()}"
client.create_subscription_to_all(group_name, commit_position=commit_position1)

# Read subscription.
subscription = client.read_subscription_to_all(group_name)

# Construct consumer.
consumer = ExampleConsumer(
    subscription=subscription,
    max_retry_count=5,
    final_action="park",
)

# Run consumer.
consumer.run()

# Check 'event5' was nacked and never acked.
assert event5.id in nacked_events
assert event5.id not in acked_events
assert nacked_events[event5.id] == 5

# Check 'event9' was acked and never nacked.
assert event9.id in acked_events
assert event9.id not in nacked_events
```

### Update subscription to all<a id="update-subscription-to-all"></a>

*requires leader*

The `update_subscription_to_all()` method can be used to update a
"persistent subscription". Please note, the filter options and consumer
strategy cannot be adjusted.

This method has a required `group_name` argument, which is the
name of a "group" of consumers of the subscription.

This method also has sixteen optional arguments, `from_end`, `commit_position`,
`resolve_links`, `consumer_strategy`, `message_timeout`, `max_retry_count`,
`min_checkpoint_count`, `max_checkpoint_count`, `checkpoint_after`,
`max_subscriber_count`, `live_buffer_size`, `read_batch_size`, `history_buffer_size`,
`extra_statistics`, `timeout` and `credentials`.

The optional arguments `from_end`, `commit_position`,
`resolve_links`, `consumer_strategy`, `message_timeout`, `max_retry_count`,
`min_checkpoint_count`, `max_checkpoint_count`, `checkpoint_after`,
`max_subscriber_count`, `live_buffer_size`, `read_batch_size`, `history_buffer_size`,
amd `extra_statistics` can be used to adjust the values set on previous calls to
`create_subscription_to_all()` and `update_subscription_to_all()`. If any of
these arguments are not mentioned in a call to `update_subscription_to_all()`,
the corresponding settings of the persistent subscription will be unchanged.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The method `update_subscription_to_all()` does not return a value.

In the example below, a persistent subscription is updated to run from the end of the
database.

```python
# Create a persistent subscription.
client.update_subscription_to_all(group_name=group_name1, from_end=True)
```

### Create subscription to stream<a id="create-subscription-to-stream"></a>

*requires leader*

The `create_subscription_to_stream()` method can be used to create a persistent
subscription to a stream.

This method has two required arguments, `group_name` and `stream_name`. The
`group_name` argument names the group of consumers that will receive events
from this subscription. The `stream_name` argument specifies which stream
the subscription will follow. The values of both these arguments are expected
to be Python `str` objects.

This method also has sixteen optional arguments, `stream_position`, `from_end`,
`resolve_links`, `consumer_strategy`, `message_timeout`, `max_retry_count`,
`min_checkpoint_count`, `max_checkpoint_count`, `checkpoint_after`,
`max_subscriber_count`, `live_buffer_size`, `read_batch_size`, `history_buffer_size`,
`extra_statistics`, `timeout` and `credentials`.

The optional `stream_position` argument specifies a stream position from
which to subscribe. The recorded event at this stream
position will be received when reading the subscription.

The optional `from_end` argument is a Python `bool`.
By default, the value of this argument is `False`. If this argument is set
to `True`, reading from the subscription will receive only events
recorded after the subscription was created. That is, it is not inclusive
of the current stream position.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `consumer_strategy` argument is a Python `str` that defines
the consumer strategy for this persistent subscription. The value of this argument
can be `'DispatchToSingle'`, `'RoundRobin'`, `'Pinned'`, or `'PinnedByCorrelation'`. The
default value is `'DispatchToSingle'`.

The optional `message_timeout` argument is a Python `float` which sets a maximum duration,
in seconds, from the server sending a recorded event to a consumer of the persistent
subscription until either an "acknowledgement" (ack) or a "negative acknowledgement"
(nack) is received by the server, after which the server will retry to send the event.
The default value of `message_timeout` is `30.0`.

The optional `max_retry_count` argument is a Python `int` which sets the number of times
the server will retry to send an event. The default value of `max_retry_count` is `10`.

The optional `min_checkpoint_count` argument is a Python `int` which sets the minimum
number of "acknowledgements" (acks) received by the server before the server may record
the acknowledgements. The default value of `min_checkpoint_count` is `10`.

The optional `max_checkpoint_count` argument is a Python `int` which sets the maximum
number of "acknowledgements" (acks) received by the server before the server must
record the acknowledgements. The default value of `max_checkpoint_count` is `1000`.

The optional `checkpoint_after` argument is a Python `float` which sets the maximum
duration in seconds between recording "acknowledgements" (acks). The default value of
`checkpoint_after` is `2.0`.

The optional `max_subscriber_count` argument is a Python `int` which sets the maximum
number of concurrent readers of the persistent subscription, beyond which attempts to
read the persistent subscription will raise a `MaximumSubscriptionsReachedError` exception.

The optional `live_buffer_size` argument is a Python `int` which sets the size of the
buffer (in-memory) holding newly recorded events. The default value of `live_buffer_size`
is 500.

The optional `read_batch_size` argument is a Python `int` which sets the number of
recorded events read from disk when catching up. The default value of `read_batch_size`
is 200.

The optional `history_buffer_size` argument is a Python `int` which sets the number of
recorded events to cache in memory when catching up. The default value of `history_buffer_size`
is 500.

The optional `extra_statistics` argument is a Python `bool` which enables tracking of
extra statistics on this subscription. The default value of `extra_statistics` is `False`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

This method does not return a value. Events can be received by calling
`read_subscription_to_stream()`.

The example below creates a persistent stream subscription from the start of the stream.

```python
# Create a persistent stream subscription from start of the stream.
group_name2 = f"group-{uuid.uuid4()}"
client.create_subscription_to_stream(
    group_name=group_name2,
    stream_name=stream_name2,
)
```

### Read subscription to stream<a id="read-subscription-to-stream"></a>

*requires leader*

The `read_subscription_to_stream()` method can be used to read a persistent
subscription to a stream.

This method has two required arguments, `group_name` and `stream_name`, which
should match the values of arguments used when calling `create_subscription_to_stream()`.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

This method returns a `PersistentSubscription` object, which is an iterator
giving `RecordedEvent` objects, that also has `ack()`, `nack()` and `stop()`
methods.

```python
subscription = client.read_subscription_to_stream(
    group_name=group_name2,
    stream_name=stream_name2,
)
```

The example below iterates over the subscription object, and calls `ack()`.
The subscription's `stop()` method is called when we have received `event6`,
stopping the iteration, so that we can continue with the examples below.

```python
events = []
for event in subscription:
    events.append(event)

    # Acknowledge the received event.
    subscription.ack(event)

    # Stop when 'event6' has been received.
    if event == event6:
        subscription.stop()
```

We can check we received all the events that were appended to `stream_name2`
in the examples above.

```python
assert len(events) == 3
assert events[0] == event4
assert events[1] == event5
assert events[2] == event6
```

### Update subscription to stream<a id="update-subscription-to-stream"></a>

*requires leader*

The `update_subscription_to_stream()` method can be used to update a persistent
subscription to a stream. Please note, the consumer strategy cannot be adjusted.

This method has a required `group_name` argument, which is the
name of a "group" of consumers of the subscription, and a required
`stream_name` argument, which is the name of a stream.

This method also has sixteen optional arguments, `from_end`, `stream_position`,
`resolve_links`, `consumer_strategy`, `message_timeout`, `max_retry_count`,
`max_subscriber_count`, `live_buffer_size`, `read_batch_size`, `history_buffer_size`,
`extra_statistics`, `min_checkpoint_count`, `max_checkpoint_count`, `checkpoint_after`,
`timeout` and `credentials`.

The optional arguments `from_end`, `stream_position`,
`resolve_links`, `consumer_strategy`, `message_timeout`, `max_retry_count`,
`min_checkpoint_count`, `max_checkpoint_count`, `checkpoint_after`,
`max_subscriber_count`, `live_buffer_size`, `read_batch_size`, `history_buffer_size`,
and `extra_statistics` can be used to adjust the values set on previous calls to
`create_subscription_to_stream()` and `update_subscription_to_stream()`. If any of
these arguments are not mentioned in a call to `update_subscription_to_stream()`,
the corresponding settings of the persistent subscription will be unchanged.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The `update_subscription_to_stream()` method does not return a value.

In the example below, a persistent subscription to a stream is updated to run from the
end of the stream.

```python
# Create a persistent subscription.
client.update_subscription_to_stream(
    group_name=group_name2,
    stream_name=stream_name2,
    from_end=True,
)
```

### Replay parked events<a id="replay-parked-events"></a>

*requires leader*

The `replay_parked_events()` method can be used to "replay" events that have
been "parked" (negatively acknowledged with the action `'park'`) when reading
a persistent subscription. Parked events will then be received again by consumers
reading from the persistent subscription.

This method has a required `group_name` argument and an optional `stream_name`
argument. The values of these arguments should match those used when calling
`create_subscription_to_all()` or `create_subscription_to_stream()`.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

The example below replays parked events for group `group_name1`.

```python
client.replay_parked_events(
    group_name=group_name1,
)
```

The example below replays parked events for group `group_name2`.

```python
client.replay_parked_events(
    group_name=group_name2,
    stream_name=stream_name2,
)
```

### Get subscription info<a id="get-subscription-info"></a>

*requires leader*

The `get_subscription_info()` method can be used to get information for a
persistent subscription.

This method has a required `group_name` argument and an optional `stream_name`
argument, which should match the values of arguments used when calling either
`create_subscription_to_all()` or `create_subscription_to_stream()`.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

The example below gets information for the persistent subscription `group_name1` which
was created by calling `create_subscription_to_all()`.

```python
subscription_info = client.get_subscription_info(
    group_name=group_name1,
)
```

The example below gets information for the persistent subscription `group_name2` on
`stream_name2` which was created by calling `create_subscription_to_stream()`.

```python
subscription_info = client.get_subscription_info(
    group_name=group_name2,
    stream_name=stream_name2,
)
```

The returned value is a `SubscriptionInfo` object.

### List subscriptions<a id="list-subscriptions"></a>

*requires leader*

The `list_subscriptions()` method can be used to get information for all
existing persistent subscriptions, both "subscriptions to all" and
"subscriptions to stream".

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

The example below lists all the existing persistent subscriptions.

```python
subscriptions = client.list_subscriptions()
```

The returned value is a list of `SubscriptionInfo` objects.


### List subscriptions to stream<a id="list-subscriptions-to-stream"></a>

*requires leader*

The `list_subscriptions_to_stream()` method can be used to get information for all
the persistent subscriptions to a stream.

This method has one required argument, `stream_name`.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

```python
subscriptions = client.list_subscriptions_to_stream(
    stream_name=stream_name2,
)
```

The returned value is a list of `SubscriptionInfo` objects.


### Delete subscription<a id="delete-subscription"></a>

*requires leader*

The `delete_subscription()` method can be used to delete a persistent
subscription.

This method has a required `group_name` argument and an optional `stream_name`
argument, which should match the values of arguments used when calling either
`create_subscription_to_all()` or `create_subscription_to_stream()`.

This method has an optional `timeout` argument, which is a Python `float`
that sets a maximum duration, in seconds, for the completion of the gRPC operation.

This method has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

The example below deletes the persistent subscription `group_name1` which
was created by calling `create_subscription_to_all()`.

```python
client.delete_subscription(
    group_name=group_name1,
)
```

The example below deleted the persistent subscription `group_name2` on
`stream_name2` which was created by calling `create_subscription_to_stream()`.

```python
client.delete_subscription(
    group_name=group_name2,
    stream_name=stream_name2,
)
```


## Projections<a id="projections"></a>

Please refer to the [KurrentDB documentation](https://docs.kurrent.io/server/v24.10/features/projections/)
for more information on projections in KurrentDB.

### Create projection<a id="create-projection"></a>

*requires leader*

The `create_projection()` method can be used to create a "continuous" projection.

This method has two required arguments, `name` and `query`.

This required `name` argument is a Python `str` that specifies the name of the projection.

This required `query` argument is a Python `str` that defines what the projection will do.

This method also has four optional arguments, `emit_enabled`,
`track_emitted_streams`, `timeout`, and `credentials`.

The optional `emit_enabled` argument is a Python `bool` which specifies whether a
projection will be able to emit events. If a `True` value is specified, the projection
will be able to emit events, otherwise the projection will not be able to emit events.
The default value of `emit_enabled` is `False`.

Please note, `emit_enabled` must be `True` if your projection query includes a call to
`emit()`, otherwise the projection will not run.

The optional `track_emitted_streams` argument is a Python `bool` which specifies whether
a projection will have its emitted streams tracked. If a `True` value is specified, the
projection will have its emitted streams tracked, otherwise the projection will not
have its emitted streams tracked. The default value of `track_emitted_streams` is `False`.

The purpose of tracking emitted streams is that they can optionally be deleted when
a projection is deleted (see the `delete_projection()` method for more details).

Please note, if you set `track_emitted_streams` to `True`, then you must also set
`emit_enabled` to `True`, otherwise an error will be raised by this method.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

In the example below, a projection is created that processes events appended to
`stream_name2`. The "state" of the projection is initialised to have a "count" that
is incremented once for each event.

```python
projection_name = str(uuid.uuid4())

projection_query = """fromStream('%s')
.when({
  $init: function(){
    return {
      count: 0
    };
  },
  OrderCreated: function(s,e){
    s.count += 1;
  },
  OrderUpdated: function(s,e){
    s.count += 1;
  },
  OrderDeleted: function(s,e){
    s.count += 1;
  }
})
.outputState()
"""  % stream_name2

client.create_projection(
    name=projection_name,
    query=projection_query,
)
```

Please note, the `outputState()` call is optional, and causes the state of the
projection to be persisted in a "result" stream. If `outputState()` is called, an
event representing the state of the projection will immediately be written to a
"result" stream.

The default name of the "result" stream for a projection with name `projection_name`
is `$projections-{projection_name}-result`. This stream name can be used to read from
and subscribe to the "result" stream, with the `get_stream()`, or `read_stream()`,
or `subscribe_to_stream()`, or `create_subscription_to_stream()` and
`read_subscription_to_stream()` methods.

If your projection does not call `outputState()`, then you won't be able to read or
subscribe to a "result" stream, but you will still be able to get the projection
"state" using the `get_projection_state()` method.

The "type" string of events recorded in "result" streams is `'Result'`. You may want to
include this in a `filter_exclude` argument when filtering events by type whilst reading
or subscribing to "all" events recorded in the database (with `read_all()`,
`subscribe_to_all()`, etc).

Additionally, and in any case, from time to time the state of the projection will be
recorded in a "state" stream, and also the projection will write to a "checkpoint"
stream. The "state" stream, the "checkpoint" stream, and all "emitted" streams that
have been "tracked" (as a consequence of the `track_emitted_streams` argument having
been `True`) can optionally be deleted when the projection is deleted. See
`delete_projection()` for details.

Unlike the "result" and "emitted" streams, the "state" and the "checkpoint" streams
cannot be read or subscribed to by users, or viewed in the "stream browser" view of
KurrentDB's Web interface.

### Get projection state<a id="get-projection-state"></a>

*requires leader*

The `get_projection_state()` method can be used to get a projection's "state".

This method has a required `name` argument, which is a Python `str` that
specifies the name of a projection.

This method also has three optional arguments, `partition`, `timeout` and
`credentials`.

The optional `partition` argument is a Python `str` which can be used to read
the state of a particular partition.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

In the example below, after sleeping for 1 second to allow the projection
to process all the recorded events, the projection "state" is obtained.
We can see that the projection has processed three events.

```python
sleep(1)  # allow time for projection to process recorded events

projection_state = client.get_projection_state(projection_name)

assert projection_state.value == {'count': 3}
```

### Update projection<a id="update-projection"></a>

*requires leader*

The `update_projection()` method can be used to update a projection.

This method has two required arguments, `name` and `query`.

The required `name` argument is a Python `str` which specifies the name of the projection
to be updated.

The required `query` argument is a Python `str` which defines what the projection will do.

This method also has three optional arguments, `emit_enabled`, `timeout`, and `credentials`.

The optional `emit_enabled` argument is a Python `bool` which specifies whether a
projection will be able to emit events. If a `True` value is specified, the projection
will be able to emit events. If a `False` value is specified, the projection will not
be able to emit events. The default value of `emit_enabled` is `False`.

Please note, `emit_enabled` must be `True` if your projection query includes a call
to `emit()`, otherwise the projection will not run.

Please note, it is not possible to update `track_emitted_streams` via the gRPC API.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.update_projection(projection_name, query=projection_query)
```

### Get projection statistics<a id="get-projection-statistics"></a>

*requires leader*

The `get_projection_statistics()` method can be used to get projection statistics.

This method has a required `name` argument, which is a Python `str` that specifies the
name of a projection.

This method also has two optional arguments, `timeout` and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

This method returns a `ProjectionStatistics` object that represents
the named projection.

```python
statistics = client.get_projection_statistics(projection_name)
```

A `ProjectionStatistics` object is returned. The attributes of this object
have values that represent the progress of the projection.


### List all projection statistics<a id="list-all-projection-statistics"></a>

*requires leader*

The `list_all_projection_statistics()` method can be used to get a list of projection statistics for all projections.

This method has two optional arguments, `timeout` and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

This method returns a list of `ProjectionStatistics` objects that each represent
a projection.

```python
statistics = client.list_all_projection_statistics()
```

### List continuous projection statistics<a id="list-continuous-projection-statistics"></a>

*requires leader*

The `list_continuous_projection_statistics()` method can be used to get a list of projection statistics for all continuous projections.

This method has two optional arguments, `timeout` and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

This method returns a list of `ProjectionStatistics` objects that each represent
a projection.

```python
statistics = client.list_continuous_projection_statistics()
```

### Disable projection<a id="disable-projection"></a>

*requires leader*

The `disable_projection()` method can be used to disable (stop running) a projection.
When a projection is stopped using this method, a checkpoint will be written.

This method has a required `name` argument, which is a Python `str` that
specifies the name of the projection to be disabled.

This method also has two optional arguments, `timeout`, and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.disable_projection(projection_name)
```

### Enable projection<a id="enable-projection"></a>

*requires leader*

The `enable_projection()` method can be used to enable (start running) a projection
that was previously disabled (stopped).

This method has a required `name` argument, which is a Python `str` that
specifies the name of the projection to be enabled.

This method also has two optional arguments, `timeout` and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.enable_projection(projection_name)
```

### Abort projection<a id="abort-projection"></a>

*requires leader*

The `abort_projection()` method can be used to abort (stop running) a projection.
When a projection is stopped using this method, it will be stopped without writing
a checkpoint.

This method has a required `name` argument, which is a Python `str` that
specifies the name of the projection to be disabled.

This method also has two optional arguments, `timeout`, and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.abort_projection(projection_name)
```

### Reset projection<a id="reset-projection"></a>

*requires leader*

The `reset_projection()` method can be used to reset a projection.

This method has a required `name` argument, which is a Python `str` that
specifies the name of the projection to be reset.

This method also has two optional arguments, `timeout`, and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.reset_projection(projection_name)
```

Please note, a projection must be disabled before it can be reset.


### Delete projection<a id="delete-projection"></a>

*requires leader*

The `delete_projection()` method can be used to delete a projection.

This method has a required `name` argument, which is a Python `str` that
specifies the name of the projection to be deleted.

This method also has five optional arguments, `delete_emitted_streams`,
`delete_state_stream`, `delete_checkpoint_stream`, `timeout`, and `credentials`.

The optional `delete_emitted_streams` argument is a Python `bool` which specifies
that all "emitted" streams that have been tracked will be deleted. For emitted streams
to be deleted, they must have been tracked (see the `track_emitted_streams` argument of
the `create_projection()` method.)

The optional `delete_state_stream` argument is a Python `bool` which specifies that
the projection's "state" stream should also be deleted. The "state" stream is like
the "result" stream, but events are written to the "state" stream occasionally, along
with events written to the "checkpoint" stream, rather than being written immediately
in the way a call `outputState()` immediately writes events to the "result" stream.

The optional `delete_checkpoint_stream` argument is a Python `bool` which specifies
that the projection's "checkpoint" stream should also be deleted.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.delete_projection(projection_name)
```

Please note, a projection must be disabled before it can be deleted.


### Restart projections subsystem<a id="restart-projections-subsystem"></a>

*requires leader*

The `restart_projections_subsystem()` method can be used to restart the projections subsystem.

This method also has two optional arguments, `timeout` and `credentials`.

The optional `timeout` argument is a Python `float` which sets a
maximum duration, in seconds, for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

```python
client.restart_projections_subsystem()
```


## Call credentials<a id="call-credentials"></a>

Default call credentials are derived by the client from the user info part of the
connection string URI.

Many of the client methods described above have an optional `credentials` argument,
which can be used to set call credentials for an individual method call that override
those derived from the connection string URI.

Call credentials are sent to "secure" servers in a "basic auth" authorization header.
This authorization header is used by the server to authenticate the client. The
authorization header is not sent to "insecure" servers.


### Construct call credentials<a id="construct-call-credentials"></a>

The client method `construct_call_credentials()` can be used to construct a call
credentials object from a username and password.

```python
call_credentials = client.construct_call_credentials(
    username='admin', password='changeit'
)
```

The call credentials object can be used as the value of the `credentials`
argument in other client methods.

## Connection<a id="connection"></a>

### Reconnect<a id="reconnect"></a>

The `reconnect()` method can be used to manually reconnect the client to a
suitable KurrentDB node. This method uses the same routine for reading the
cluster node states and then connecting to a suitable node according to the
client's node preference that is specified in the connection string URI when
the client is constructed. This method is thread-safe, in that when it is called
by several threads at the same time, only one reconnection will occur. Concurrent
attempts to reconnect will block until the client has reconnected successfully,
and then they will all return normally.

```python
client.reconnect()
```

Reconnection will happen automatically in many cases, due to the `@autoreconnect`
decorator.

An example of when it might be desirable to reconnect manually is when (for performance
reasons) the client's node preference is to be connected to a follower node in the
cluster, and, after a cluster leader election, the follower becomes the leader.
Automatic reconnection to a follower node in this case is currently beyond the
capabilities of this client, but this behavior might be implemented in a future release.

### Close<a id="close"></a>

The `close()` method can be used to cleanly close the client's gRPC connection.

```python
client.close()
```


## Asyncio client<a id="asyncio-client"></a>

The `kurrentdbclient` package also provides an asynchronous I/O gRPC Python client for
KurrentDB. It is functionally equivalent to the multithreaded client. It uses
the `grpc.aio` package and the `asyncio` module, instead of `grpc` and `threading`.

It supports both the "kurrentdb" and the "kurrentdb+discover" connection string URI schemes,
and can connect to both "secure" and "insecure" KurrentDB servers.

The class `AsyncKurrentDBClient` can be used to construct an instance of the
asynchronous I/O gRPC Python client. It can be imported from `kurrentdbclient`. The
async method `connect()` should be called after constructing the client.

The asyncio client has exactly the same methods as the multithreaded `KurrentDBClient`.
These methods are defined as `async def` methods, and so calls to these methods will
return Python "awaitables" that must be awaited to obtain the method return values.
The methods have the same behaviors, the same arguments and the same or equivalent
return values. The methods are similarly decorated with reconnect and retry decorators,
that selectively reconnect and retry when connection issues or server errors are
encountered.

When awaited, the methods `read_all()` and `read_stream()` return an `AsyncReadResponse`
object. The methods `subscribe_to_all()` and `subscribe_to_stream()` return an
`AsyncCatchupSubscription` object. The methods `read_subscription_to_all()` and
`read_subscription_to_stream()` return an `AsyncPersistentSubscription` object.
These objects are asyncio iterables, which you can iterate over with Python's `async for`
syntax to obtain `RecordedEvent` objects. They are also asyncio context managers,
supporting the `async with` syntax. They also have a `stop()` method which can be
used to terminate the iterator in a way that actively cancels the streaming gRPC call
to the server. When used as a context manager, the `stop()` method will be called when
the context manager exits.

The methods `read_subscription_to_all()` and `read_subscription_to_stream()` return
instances of the class `AsyncPersistentSubscription`, which has async methods `ack()`,
`nack()` that work in the same way as the methods on `PersistentSubscription`,
supporting the acknowledgement and negative acknowledgement of recorded events that
have been received from a persistent subscription. See above for details.

### Synopsis<a id="synopsis-1"></a>

The example below demonstrates the async `append_to_stream()`, `get_stream()` and
`subscribe_to_all()` methods. These are the most useful methods for writing
an event-sourced application, allowing new aggregate events to be recorded, the
recorded events of an aggregate to be obtained so aggregates can be reconstructed,
and the state of an application to propagated and processed with "exactly-once"
semantics.

```python
import asyncio

from kurrentdbclient import AsyncKurrentDBClient


async def demonstrate_async_client():
    # Construct client.
    client = AsyncKurrentDBClient(
        uri=os.getenv("KDB_URI"),
        root_certificates=os.getenv("KDB_ROOT_CERTIFICATES"),
    )

    # Connect to KurrentDB.
    await client.connect()

    # Append events.
    stream_name = str(uuid.uuid4())
    event1 = NewEvent("OrderCreated", data=b'{}')
    event2 = NewEvent("OrderUpdated", data=b'{}')
    event3 = NewEvent("OrderDeleted", data=b'{}')

    commit_position = await client.append_to_stream(
        stream_name=stream_name,
        current_version=StreamState.NO_STREAM,
        events=[event1, event2, event3]
    )

    # Get stream events.
    recorded = await client.get_stream(stream_name)
    assert len(recorded) == 3
    assert recorded[0] == event1
    assert recorded[1] == event2
    assert recorded[2] == event3

    # Subscribe to all events.
    received = []
    async with await client.subscribe_to_all(commit_position=0) as subscription:
        async for event in subscription:
            received.append(event)
            if event.commit_position == commit_position:
                break
    assert received[-3] == event1
    assert received[-2] == event2
    assert received[-1] == event3

    # Close the client.
    await client.close()


# Run the demo.
asyncio.run(
    demonstrate_async_client()
)
```

### FastAPI example<a id="fastapi"></a>

The example below shows how to use `AsyncKurrentDBClient` with [FastAPI](https://fastapi.tiangolo.com).

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from kurrentdbclient import AsyncKurrentDBClient

client: AsyncKurrentDBClient


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Construct the client.
    global client
    client = AsyncKurrentDBClient(
        uri="kurrentdb+discover://localhost:2113?Tls=false",
    )
    await client.connect()

    yield

    # Close the client.
    await client.close()


app = FastAPI(lifespan=lifespan)


@app.get("/commit_position")
async def commit_position():
    commit_position = await client.get_commit_position()
    return {"commit_position": commit_position}
```

If you put this code in a file called `fastapi_example.py` and then run command
`uvicorn fastapi_example:app --host 0.0.0.0 --port 80`, then the FastAPI application
will return something like `{"commit_position":628917}` when a browser is pointed
to `http://localhost/commit_position`. Use Ctrl-c to exit the process.

## Notes<a id="notes"></a>

### Regular expression filters<a id="regular-expression-filters"></a>

The `read_all()`, `subscribe_to_all()`, `create_subscription_to_all()`
and `get_commit_position()` methods have `filter_exclude` and `filter_include`
arguments. This section provides some more details about the values of these
arguments.

The first thing to note is that the values of these arguments should be sequences
of regular expressions.

Please note, they are concatenated together by the client as bracketed alternatives in a larger
regular expression that is anchored to the start and end of the strings being
matched. So there is no need to include the `'^'` and `'$'` anchor assertions.

You should use wildcards if you want to match substrings, for example `'.*Snapshot'`
to match all strings that end with `'Snapshot`', or `'Order.*'` to match all strings
that start with `'Order'`.

System events generated by KurrentDB have `type` strings that start with
the `$` sign. Persistence subscription events generated when manipulating
persistence subscriptions have `type` strings that start with `PersistentConfig`.

For example, to match the type of KurrentDB system events, use the regular
expression string `r'\$.+'`. Please note, the constant `KDB_SYSTEM_EVENTS_REGEX` is
set to this value. You can import this constant from `kurrentdbclient` and use it when
building longer sequences of regular expressions.

Similarly, to match the type of KurrentDB persistence subscription events, use the
regular expression `r'PersistentConfig\d+'`. The constant `KDB_PERSISTENT_CONFIG_EVENTS_REGEX`
is set to this value. You can import this constant from `kurrentdbclient` and use it when
building longer sequences of regular expressions.

The constant `DEFAULT_EXCLUDE_FILTER` is a sequence of regular expressions that includes
both `KDB_SYSTEM_EVENTS_REGEX` and `KDB_PERSISTENT_CONFIG_EVENTS_REGEX`, as well as
`KDB_RESULT_EVENTS_REGEX` which matches projection "result" events. It is used
as the default value of `filter_exclude` so that the events generated internally by
KurrentDB are excluded by default.

In all methods that have a `filter_exclude` argument, the default value of the argument
is the constant `DEFAULT_EXCLUDE_FILTER`, which is designed to match (and therefore
to exclude) both "system" and "persistence subscription config" event types, which
would otherwise be included.

This value can be extended. For example, if you want to exclude system events and
persistent subscription events and also events that have a type that ends with
`'Snapshot'`, then you can use `(*DEFAULT_EXCLUDE_FILTER, '.*Snapshot')` as the
`filter_exclude` argument.

The `filter_include` and `filter_exclude` arguments are designed to have exactly
the opposite effect from each other, so that a sequence of strings given to
`filter_include` will return exactly those events which would be excluded if
the same argument value were used with `filter_exclude`. And vice versa, so that
a sequence of strings given to `filter_exclude` will return exactly those events
that would not be included if the same argument value were used with `filter_include`.


### Reconnect and retry method decorators<a id="reconnect-and-retry-method-decorators"></a>

Please note, nearly all the client methods are decorated with the `@autoreconnect` and
the `@retrygrpc` decorators.

The `@autoreconnect` decorator will reconnect to a suitable node in the cluster when
the server to which the client has been connected has become unavailable, or when the
client's gRPC channel happens to have been closed. The client will also reconnect when
a method is called that requires a leader, and the client's node preference is to be
connected to a leader, but the node that the client has been connected to stops being
the leader. In this case, the client will reconnect to the current leader. After
reconnecting, the failed operation will be retried.

The `@retrygrpc` decorator selectively retries gRPC operations that have failed due to
a timeout, network error, or server error. It doesn't retry operations that fail due to
bad requests that will certainly fail again.

Please also note, the aspects not covered by the reconnect and retry decorator
behaviours have to do with methods that return iterators. For example, consider
the "read response" iterator returned from the `read_all()` method. The
`read_all()` method will have returned, and the method decorators will therefore
have exited, before iterating over the "read response" begins. Therefore, if a
connection issue occurs whilst iterating over the "read response", it isn't possible
for any decorator on the `read_all()` method to trigger a reconnection.

With the "catch-up subscription" objects, there is an initial "confirmation" response
from the server which is received and checked by the client. And so, when a call is
made to `subscribe_to_all()` or `subscribe_to_stream()`, if the server is unavailable,
or if the channel has somehow been closed, or if the request fails for some other reason,
then the client will reconnect and retry. However, if an exception is raised when iterating over a
successfully returned "catch-up subscription" object, the catch-up subscription will
need to be restarted. Similarly, when reading persistent subscriptions, if there are
connection issues whilst iterating over a successfully received response, the consumer
will need to be restarted.

## Instrumentation<a id="instrumentation"></a>

Instrumentation is the act of modifying software so that analysis can be performed on it.
Instrumentation helps enterprises reveal areas or features where users frequently
encounter errors or slowdowns in their software or platform.

Instrumentation helps you understand the inner state of your software systems.
Instrumented applications measure what code is doing when it responds to active
requests by collecting data such as metrics, events, logs, and traces.

Instrumentation provides immediate visibility into your application, often using
charts and graphs to illustrate what is going on “under the hood.”

This package supports instrumenting the KurrentDB clients with OpenTelemetry.

### OpenTelemetry<a id="open-telemetry"></a>

The [OpenTelemetry](https://opentelemetry.io) project provides a collection of APIs,
SDKs, and tools for instrumenting, generating, collecting, and exporting telemetry data,
that can help you analyze your software’s performance and behavior. It is vendor-neutral,
100% Free and Open Source, and adopted and supported by industry leaders in the
observability space.

This package provides OpenTelemetry instrumentors for both the `KurrentDBClient`
and the `AsyncKurrentDBClient` clients. These instrumentors depend on various
OpenTelemetry Python packages, which you will need to install, preferably with this
project's "opentelemetry" package extra to ensure verified version compatibility.

For example, you can install the "opentelemetry" package extra with pip.

    $ pip install kurrentdbclient[opentelemetry]

Or you can use Poetry to add it to your pyproject.toml file and install it.

    $ poetry add kurrentdbclient[opentelemetry]


You can then use the OpenTelemetry instrumentor `KurrentDBClientInstrumentor` to
instrument the `KurrentDBClient`.

```python
from kurrentdbclient.instrumentation.opentelemetry import KurrentDBClientInstrumentor

# Activate instrumentation.
KurrentDBClientInstrumentor().instrument()

# Deactivate instrumentation.
KurrentDBClientInstrumentor().uninstrument()
```

You can also use the OpenTelemetry instrumentor `AsyncKurrentDBClientInstrumentor`
to instrument the `AsyncKurrentDBClient`.

```python
from kurrentdbclient.instrumentation.opentelemetry import AsyncKurrentDBClientInstrumentor

# Activate instrumentation.
AsyncKurrentDBClientInstrumentor().instrument()

# Deactivate instrumentation.
AsyncKurrentDBClientInstrumentor().uninstrument()
```

The instrumentors use a global OpenTelemetry "tracer provider", which you will need to
initialise in order to export telemetry data.

For example, to export data to the console you will need to install the Python
package `opentelemetry-sdk`, and use the class `TracerProvider`, `BatchSpanProcessor`,
and `ConsoleSpanExporter` in the following way.

```python
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import set_tracer_provider

resource = Resource.create(
    attributes={
        SERVICE_NAME: "kurrentdb",
    }
)
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
set_tracer_provider(provider)
```

Or to export to an OpenTelemetry compatible data collector, such as
[Jaeger](https://www.jaegertracing.io), you will need to install the Python package
`opentelemetry-exporter-otlp-proto-http`, and then use the class `OTLPSpanExporter`
from the `opentelemetry.exporter.otlp.proto.http.trace_exporter` module, with an
appropriate `endpoint` argument for your collector.

```python
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import set_tracer_provider

resource = Resource.create(
    attributes={
        SERVICE_NAME: "kurrentdb",
    }
)
provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")))
set_tracer_provider(provider)
```

You can start Jaeger locally by running the following command.

    $ docker run -d -p 4318:4318 -p 16686:16686 --name jaeger jaegertracing/all-in-one:latest

You can then navigate to `http://localhost:16686` to access the Jaeger UI. And telemetry
data can be sent by an OpenTelemetry tracer provider to `http://localhost:4318/v1/traces`.

At this time, the instrumented methods are `append_to_stream()`, `subscribe_to_stream()`
`subscribe_to_all()`, `read_subscription_to_stream()`, `read_subscription_to_all()`.

The `append_to_stream()` method is instrumented by spanning the method call with a
"producer" span kind. It also adds span context information to the new event metadata
so that consumers can associate "consumer" spans with the "producer" span.

The subscription methods are instrumented by instrumenting the response iterators,
creating a "consumer" span for each recorded event received. It extracts span
context information from the recorded event metadata and associates the "consumer"
spans with a "producer" span, by making the "consumer" span a child of the "producer"
span.


## Communities<a id="communities"></a>

- [Issues](https://github.com/pyeventsourcing/kurrentdbclient/issues)
- [Discuss](https://discuss.eventstore.com/)
- [Discord (Event Store)](https://discord.gg/Phn9pmCw3t)


## Contributors<a id="contributors"></a>

### Install Poetry<a id="install-poetry"></a>

The first thing is to check you have Poetry installed.

    $ poetry --version

If you don't, then please [install Poetry](https://python-poetry.org/docs/#installing-with-the-official-installer).

    $ curl -sSL https://install.python-poetry.org | python3 -

It will help to make sure Poetry's bin directory is in your `PATH` environment variable.

But in any case, make sure you know the path to the `poetry` executable. The Poetry
installer tells you where it has been installed, and how to configure your shell.

Please refer to the [Poetry docs](https://python-poetry.org/docs/) for guidance on
using Poetry.

### Setup for PyCharm users<a id="setup-for-pycharm-users"></a>

You can easily obtain the project files using PyCharm (menu "Git > Clone...").
PyCharm will then usually prompt you to open the project.

Open the project in a new window. PyCharm will then usually prompt you to create
a new virtual environment.

Create a new Poetry virtual environment for the project. If PyCharm doesn't already
know where your `poetry` executable is, then set the path to your `poetry` executable
in the "New Poetry Environment" form input field labelled "Poetry executable". In the
"New Poetry Environment" form, you will also have the opportunity to select which
Python executable will be used by the virtual environment.

PyCharm will then create a new Poetry virtual environment for your project, using
a particular version of Python, and also install into this virtual environment the
project's package dependencies according to the project's `poetry.lock` file.

You can add different Poetry environments for different Python versions, and switch
between them using the "Python Interpreter" settings of PyCharm. If you want to use
a version of Python that isn't installed, either use your favourite package manager,
or install Python by downloading an installer for recent versions of Python directly
from the [Python website](https://www.python.org/downloads/).

Once project dependencies have been installed, you should be able to run tests
from within PyCharm (right-click on the `tests` folder and select the 'Run' option).

Because of a conflict between pytest and PyCharm's debugger and the coverage tool,
you may need to add ``--no-cov`` as an option to the test runner template. Alternatively,
just use the Python Standard Library's ``unittest`` module.

You should also be able to open a terminal window in PyCharm, and run the project's
Makefile commands from the command line (see below).

### Setup from command line<a id="setup-from-command-line"></a>

Obtain the project files, using Git or suitable alternative.

In a terminal application, change your current working directory
to the root folder of the project files. There should be a Makefile
in this folder.

Use the Makefile to create a new Poetry virtual environment for the
project and install the project.

    $ make install

Please note, if you create the virtual environment in this way, and then try to
open the project in PyCharm and configure the project to use this virtual
environment as an "Existing Poetry Environment", PyCharm sometimes has some
issues (don't know why) which might be problematic. If you encounter such
issues, you can resolve these issues by deleting the virtual environment
and creating the Poetry virtual environment using PyCharm (see above).

### Install timeout command

If you are running on a Mac, you may need to install the timeout command. You
can do this by installing GNU Coreutils with Homebrew.

    $ brew install coreutils

### Project Makefile commands<a id="project-makefile-commands"></a>

You can start KurrentDB using the following command.

    $ make start-kurrentdb

You can run tests using the following command (needs KurrentDB to be running).

    $ make test

You can stop KurrentDB using the following command.

    $ make stop-kurrentdb

You can check the formatting of the code using the following command.

    $ make lint

You can reformat the code using the following command.

    $ make fmt

### Making changes

Tests belong in `./tests`.

Code-under-test belongs in `./kurrentdbclient`.

Edit package dependencies in `pyproject.toml`. Update the `poetry.lock` file, and
the project's virtual environment, with the following command.

    $ make update
