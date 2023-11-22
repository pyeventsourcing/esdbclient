# Python gRPC Client for EventStoreDB

This [Python package](https://pypi.org/project/esdbclient/) provides a Python
gRPC client for the [EventStoreDB](https://www.eventstore.com/) database.

This client has been developed in collaboration with the EventStoreDB
team. Although not all the features of EventStoreDB are supported
by this client, many of the most useful features are presented
in an easy-to-use interface.

This client has been tested to work with EventStoreDB LTS versions 21.10,
22.10, and 23.10, without and without SSL/TLS, with single-server and cluster
modes, with Python versions 3.7, 3.8, 3.9, 3.10, 3.11 and 3.12.

The test suite has 100% line and branch coverage. The code has typing annotations
checked strictly with mypy. The code is formatted with black and isort, and checked
with flake8. Poetry is used for package management during development, and for
building and publishing distributions to [PyPI](https://pypi.org/project/esdbclient/).

## Synopsis

You can connect and interact with an EventStoreDB server using the `EventStoreDBClient`
class.

The `EventStoreDBClient` class can be imported from the `esdbclient` package.

Probably the three most useful methods of `EventStoreDBClient` are:

* `append_to_stream()` This method can be used to record new events in a particular
"stream". This is useful, for example, when executing a command in an application
that mutates an aggregate. This method is "atomic" in that either all or none of
the events will be recorded.

* `read_stream()` This method can be used to retrieve all the recorded
events in a "stream". This is useful, for example, when reconstructing
an aggregate from recorded events before executing a command in an
application that creates new events.

* `subscribe_to_all()` This method can be used to receive all recorded events in
the database. This is useful, for example, in event-processing components because
it supports processing events with "exactly-once" semantics.

The example below uses an "insecure" EventStoreDB server running locally on port 2113.

```python
import uuid

from esdbclient import EventStoreDBClient, NewEvent, StreamState


# Construct EventStoreDBClient with an EventStoreDB URI. The
# connection string URI specifies that the client should
# connect to an "insecure" server running on port 2113.

client = EventStoreDBClient(
    uri="esdb://localhost:2113?Tls=false"
)


# Generate new events. Typically, domain events of different
# types are generated in a domain model, and then serialized
# into NewEvent objects. An aggregate ID may be used as the
# name of a stream in EventStoreDB.

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
# version" is 1. The exception 'WrongCurrentVersion' will be
# raised if an incorrect value is given.

commit_position2 = client.append_to_stream(
    stream_name=stream_name1,
    current_version=1,
    events=[event3],
)

# - allocated commit positions increase monotonically
assert commit_position2 > commit_position1


# Read events from a stream. This method returns a
# "read response" iterator, which returns recorded
# events. The iterator will stop when there are no
# more events to be returned.

read_response = client.read_stream(
    stream_name=stream_name1
)

# Iterate over "read response" to get recorded events.
# The recorded events may be deserialized to domain event
# objects of different types and used to reconstruct an
# aggregate in a domain model.
recorded_events = tuple(read_response)

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
# This method returns a "catch-up subscription" iterator,
# which returns recorded events. The iterator will not stop
# when there are no more recorded events to be returned, but
# will block, and continue when further events are recorded.

catchup_subscription = client.subscribe_to_all()


# Iterate over the catch-up subscription. Process each recorded
# event in turn. Within an atomic database transaction, record
# the event's "commit position" along with any new state generated
# by processing the event. Use the component's last recorded commit
# position when restarting the catch-up subscription.

received_events = []
for event in catchup_subscription:
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


# Stop the catch-up subscription iterator.

catchup_subscription.stop()


# Close the client's gRPC connection.

client.close()
```

See below for more details.

For an example of usage, see the [eventsourcing-eventstoredb](
https://github.com/pyeventsourcing/eventsourcing-eventstoredb) package.

## Table of contents

<!-- TOC -->
* [Install package](#install-package)
  * [From PyPI](#from-pypi)
  * [With Poetry](#with-poetry)
* [EventStoreDB server](#eventstoredb-server)
  * [Run container](#run-container)
  * [Stop container](#stop-container)
* [EventStoreDB client](#eventstoredb-client)
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
  * [How to implement snapshotting with EventStoreDB](#how-to-implement-snapshotting-with-eventstoredb)
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
* [Call credentials](#call-credentials)
  * [Construct call credentials](#construct-call-credentials)
* [Connection](#connection)
  * [Reconnect](#reconnect)
  * [Close](#close)
* [Asyncio client](#asyncio-client)
  * [Synopsis](#synopsis-1)
* [Notes](#notes)
  * [Regular expression filters](#regular-expression-filters)
  * [Reconnect and retry method decorators](#reconnect-and-retry-method-decorators)
* [Contributors](#contributors)
  * [Install Poetry](#install-poetry)
  * [Setup for PyCharm users](#setup-for-pycharm-users)
  * [Setup from command line](#setup-from-command-line)
  * [Project Makefile commands](#project-makefile-commands)
<!-- TOC -->

## Install package<a id="install-package"></a>

It is recommended to install Python packages into a Python virtual environment.

### From PyPI<a id="from-pypi"></a>

You can use pip to install this package directly from
[the Python Package Index](https://pypi.org/project/esdbclient/).

    $ pip install esdbclient

### With Poetry<a id="with-poetry"></a>

You can use Poetry to add this package to your pyproject.toml and install it.

    $ poetry add esdbclient

## EventStoreDB server<a id="eventstoredb-server"></a>

The EventStoreDB server can be run locally using the official Docker container image.

### Run container<a id="run-container"></a>

For development, you can run a "secure" EventStoreDB server using the following command.

    $ docker run -d --name eventstoredb-secure -it -p 2113:2113 --env "HOME=/tmp" eventstore/eventstore:21.10.9-buster-slim --dev

As we will see, your client will need an EventStoreDB connection string URI as the value
of its `uri` constructor argument. The connection string for this "secure" EventStoreDB
server would be:

    esdb://admin:changeit@localhost:2113

To connect to a "secure" server, you will usually need to include a "username"
and a "password" in the connection string, so that the server can authenticate the
client. With EventStoreDB, the default username is "admin" and the default password
is "changeit".

When connecting to a "secure" server, your client will also need an SSL/TLS certificate
as the value of its `root_certificates` constructor argument. The client uses the
SSL/TLS certificate to authenticate the server. For development, you can either use the
SSL/TLS certificate of the certificate authority used to create the server's certificate,
or when using a single-node cluster, you can use the server certificate itself. You can
get the server certificate with the following Python code.


```python
import ssl

server_certificate = ssl.get_server_certificate(addr=('localhost', 2113))
```

Alternatively, you can start an "insecure" server using the following command.

    $ docker run -d --name eventstoredb-insecure -it -p 2113:2113 eventstore/eventstore:21.10.9-buster-slim --insecure

The connection string URI for this "insecure" server would be:

    esdb://localhost:2113?Tls=false

As we will see, when connecting to an "insecure" server, there is no need to include
a "username" and a "password" in the connection string. If you do, these values will
be ignored by the client, so that they are not sent over an insecure channel.

Please note, the "insecure" connection string uses a query string with the field-value
`Tls=false`. The value of this field is by default `true`.

### Stop container<a id="stop-container"></a>

To stop and remove the "secure" container, use the following Docker commands.

    $ docker stop eventstoredb-secure
	$ docker rm eventstoredb-secure

To stop and remove the "insecure" container, use the following Docker commands.

    $ docker stop eventstoredb-insecure
	$ docker rm eventstoredb-insecure


## EventStoreDB client<a id="eventstoredb-client"></a>

This EventStoreDB client is implemented in the `esdbclient` package with
the `EventStoreDBClient` class.

### Import class<a id="import-class"></a>

The `EventStoreDBClient` class can be imported from the `esdbclient` package.

```python
from esdbclient import EventStoreDBClient
```

### Construct client<a id="construct-client"></a>

The `EventStoreDBClient` class has one required constructor argument, `uri`, and one
optional constructor argument, `root_certificates`.

The `uri` argument is expected to be an EventStoreDB connection string URI that
conforms with the standard EventStoreDB "esdb" or "esdb+discover" URI schemes.

For example, the following connection string specifies that the client should
attempt to create a "secure" connection to port 2113 on "localhost", and use the
client credentials "username" and "password" when making calls to the server.

    esdb://username:password@localhost:2113?Tls=true

The client must be configured to create a "secure" connection to a "secure" server,
or alternatively an "insecure" connection to an "insecure" server. By default, the
client will attempt to create a "secure" connection. And so, when connecting to an
"insecure" server, the connection string must specify that the client should attempt
to make an "insecure" connection.

The following connection string specifies that the client should
attempt to create an "insecure" connection to port 2113 on "localhost".
When connecting to an "insecure" server, the client will ignore any
username and password information included in the connection string,
so that usernames and passwords are not sent over an "insecure" connection.

    esdb://localhost:2113?Tls=false

Please note, the "insecure" connection string uses a query string with the field-value
`Tls=false`. The value of this field is by default `true`. Unless the connection string
URI includes the field-value `Tls=false` in the query string, the `root_certificates`
constructor argument is also required.

When connecting to a "secure" server, the `root_certificates` argument is expected to
be a Python `str` containing PEM encoded SSL/TLS root certificates. This value is
passed directly to `grpc.ssl_channel_credentials()`. It is used for authenticating the
server to the client. It is commonly the certificate of the certificate authority that
was responsible for generating the SSL/TLS certificate used by the EventStoreDB server.
But, alternatively for development, you can use the server's certificate itself.

In the example below, the constructor argument values are taken from the operating
system environment. This is a typical arrangement in a production environment. It is
done this way here so that the code in this documentation can be tested with both
a "secure" and an "insecure" server.

```python
import os

client = EventStoreDBClient(
    uri=os.getenv("ESDB_URI"),
    root_certificates=os.getenv("ESDB_ROOT_CERTIFICATES"),
)
```

## Connection strings<a id="connection-strings"></a>

An EventStoreDB connection string is a URI that conforms with one of two possible
schemes: either the "esdb" scheme, or the "esdb+discover" scheme.

The syntax and semantics of the EventStoreDB URI schemes are described below. The
syntax is defined using [EBNF](https://en.wikipedia.org/wiki/Extended_Backus–Naur_form).

### Two schemes<a id="two-schemes"></a>

The "esdb" URI scheme can be defined in the following way.

    esdb-uri = "esdb://" , [ user-info , "@" ] , grpc-target, { "," , grpc-target } , [ "?" , query-string ] ;

In the "esdb" URI scheme, after the optional user info string, there must be at least
one gRPC target. If there are several gRPC targets, they must be separated from each
other with the "," character. Each gRPC target should indicate an EventStoreDB gRPC
server socket, by specifying a host and a port number separated with the ":" character.
The host may be a hostname that can be resolved to an IP address, or an IP address.

    grpc-target = ( hostname | ip-address ) , ":" , port-number ;


The "esdb+discover" URI scheme can be defined in the following way.

    esdb-discover-uri = "esdb+discover://" , [ user-info, "@" ] , cluster-domainname , [ "?" , query-string ] ;

In the "esdb+discover" URI scheme, after the optional user info string, there must be a
domain name which should identify a cluster of EventStoreDB servers. The client will use
a DNS server to resolve the domain name to a list of addresses of EventStoreDB servers,
by querying for 'A' records. In this case, the port number "2113" will be used to
construct gRPC targets from the addresses obtained from 'A' records provided by the
DNS server. Therefore, if you want to use the "esdb+discover" URI scheme, you will
need to configure DNS when setting up your EventStoreDB cluster.

With both the "esdb" and "esdb+discover" URI schemes, the client firstly obtains
a list of gRPC targets: either directly from "esdb" connection strings; or indirectly
from "esdb+discover" connection strings via DNS. This list of targets is known as the
"gossip seed". The client will then attempt to connect to each gRPC target in turn,
attempting to call the EventStoreDB Gossip API to obtain information about the
EventStoreDB cluster. A member of the cluster is selected by the client, according
to the "node preference" option. The client may then need to close its
connection and reconnect to the selected server.

### User info string<a id="user-info-string"></a>

In both the "esdb" and "esdb+discover" schemes, the URI may include a user info string.
If it exists in the URI, the user info string must be separated from the rest of the URI
with the "@" character. The user info string must include a username and a password,
separated with the ":" character.

    user-info = username , ":" , password ;

The user info is sent by the client as "call credentials" in each call to a "secure"
server, in a "basic auth" authorization header. This authorization header is used by
the server to authenticate the client. The authorization header is not sent to
"insecure" servers.

### Query string<a id="query-string"></a>

In both the "esdb" and "esdb+discover" schemes, the optional query string must be one
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
                | ( "MaxDiscoverAttempts", "=" , integer )
                | ( "KeepAliveInterval", "=" , integer )
                | ( "KeepAliveInterval", "=" , integer ) ;

The table below describes the query field-values supported by this client.

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
| KeepAliveInterval   | integer (default: `None`)                                             | The value of the "grpc.keepalive_ms" gRPC channel option.                                                                                                         |
| KeepAliveTimeout    | integer (default: `None`)                                             | The value of the "grpc.keepalive_timeout_ms" gRPC channel option.                                                                                                 |


### Examples<a id="examples"></a>

Here are some examples of EventStoreDB connection string URIs.

The following URI will cause the client to connect to, and get
cluster info, from "secure" server socket `localhost:2113`. And
then to connect to a "leader" node. And also to use "admin" and
"changeit" as the username and password when making calls to
EventStoreDB API methods.

    esdb://admin:changeit@localhost:2113


The following URI will cause the client to get cluster info from
"insecure" server socket 127.0.0.1:2113.  And then to connect to
a "leader" node.

    esdb://127.0.0.1:2113?Tls=false


The following URI will cause the client to get cluster info from
addresses in DNS 'A' records for cluster1.example.com. And then
to connect to a "leader" node. And use a default deadline of 5
seconds when making calls to EventStore API "write" methods.

    esdb+discover://admin:changeit@cluster1.example.com?DefaultDeadline=5


The following URI will cause the client to get cluster info from either
localhost:2111, or localhost:2112, or localhost:2113. And then to connect
to a "follower" node.

    esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?NodePreference=follower


The following URI will cause the client to get cluster info from addresses in
DNS 'A' records for cluster1.example.com. And to configure "keep alive" timeout
and interval in the gRPC channel.

    esdb+discover://admin:changeit@cluster1.example.com?KeepAliveInterval=10000&KeepAliveTimeout=10000


Please note, the client is insensitive to the case of fields and values. If fields are
repeated in the query string, the query string will be parsed without error. However,
the connection options used by the client will use the value of the first field. All
the other field-values in the query string with the same field name will be ignored.
Fields without values will also be ignored.

If the client's node preference is "leader" and the node becomes a
"follower", the client will attempt to reconnect to the current leader when a method
is called that expects to call a leader. Methods which mutate the state of the database
expect to call a leader. For such methods, the HTTP header "requires-leader" is set to
"true", and this header is observed by the server, and so a node which is not a leader
that receives such a request will return an error. This error is detected by the client,
which will then close the current gRPC connection and create a new connection to the
leader. The request will then be retried with the leader.

If the client's node preference is "follower" and there are no follower
nodes in the cluster, then the client will raise an exception. Similarly, if the
client's node preference is "readonlyreplica" and there are no read-only replica
nodes in the cluster, then the client will also raise an exception.

The gRPC channel option "grpc.max_receive_message_length" is automatically
configured to the value `17 * 1024 * 1024`. This value cannot be changed.


## Event objects<a id="event-objects"></a>

This package defines a `NewEvent` class and a `RecordedEvent` class. The
`NewEvent` class should be used when writing events to the database. The
`RecordedEvent` class is used when reading events from the database.

### New events<a id="new-events"></a>

The `NewEvent` class should be used when writing events to an EventStoreDB database.
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

The `RecordedEvent` class is used when reading events from an EventStoreDB
database. The client will return event objects of this type from all methods
that return recorded events, such as `get_stream()`, `subscribe_to_all()`,
and `read_subscription_to_all()`. You do not need to construct recorded event objects.

Like `NewEvent`, the `RecordedEvent` class is also a frozen Python dataclass. It has
all the attributes that `NewEvent` has (`type`, `data`, `metadata`, `content_type`, `id`)
and some additional attributes that follow from the fact that an event was recorded
(`stream_name`, `stream_position`, `commit_position`). It has a `retry_count`
which is set only when reading persistence subscriptions. It also has a `link`
attribute, which is set only when resolving "link events".

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

In EventStoreDB, a "stream position" is an integer representing the position of a
recorded event in a stream. Each recorded event is recorded at a position in a stream.
Each stream position is occupied by only one recorded event. New events are recorded at the
next unoccupied position. All sequences of stream positions are zero-based and gapless.

The `commit_position` attribute is a Python `int`, used to indicate the position in the
database at which an event was recorded.

In EventStoreDB, a "commit position" is an integer representing the position of a
recorded event in the database. Each recorded event is recorded at a position in the
database. Each commit position is occupied by only one recorded event. Commit positions
are zero-based and increase monotonically as new events are recorded. But, unlike stream
positions, the sequence of successive commit positions is not gapless. Indeed, there are
usually large differences between the commit positions of successively recorded events.

Please note, in EventStoreDB 21.10, the `commit_position` of all `RecordedEvent` objects
obtained from `read_stream()` is `None`, whereas those obtained from `read_all()` have
the actual commit position of the recorded event. This was changed in version 22.10, so
that event objects obtained from both `get_stream()` and `read_all()` have the actual
commit position. The `commit_position` attribute of the `RecordedEvent` class is
annotated with the type `Optional[int]` for this reason only.

The `retry_count` is a Python `int`, used to indicate the number of times a persistent
subscription has retried sending the event to a consumer.

The `link` attribute is an optional `RecordedEvent` that carries information about
a "link event" that has been resolved. This allows link events to be acknowledged or
negatively acknowledged when using persistence subscriptions with the `resolve_links`
argument set to `True`. See the `ack_id` property.


```python
from dataclasses import dataclass

@dataclass(frozen=True)
class RecordedEvent:
    """
    Encapsulates event data that has been recorded in EventStoreDB.
    """

    type: str
    data: bytes
    metadata: bytes
    content_type: str
    id: UUID
    stream_name: str
    stream_position: int
    commit_position: Optional[int]
    retry_count: Optional[int] = None
    link: Optional["RecordedEvent"] = None

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

In EventStoreDB, a "stream" is a sequence of recorded events that all have
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
`WrongCurrentVersion` exception. This behavior is designed to provide concurrency
control when recording new events. The correct value of `current_version` for any stream
can be obtained by calling `get_current_version()`. However, the typical approach is to
reconstruct an aggregate from the recorded events, so that the version of the aggregate
is the stream position of the last recorded event, then have the aggregate generate new
events, and then use the current version of the aggregate as the value of the
`current_version` argument when appending the new aggregate events. This ensures
the consistency of the recorded aggregate events, because operations that generate
new aggregate events can be retried with a freshly reconstructed aggregate if
a `WrongCurrentVersion` exception is encountered when recording new events. This
controlling behavior can be entirely disabled by setting the value of the `current_version`
argument to the constant `StreamState.ANY`. More selectively, this behaviour can be
disabled for existing streams by setting the value of the `current_version`
argument to the constant `StreamState.EXISTS`.

The required `events` argument is expected to be a sequence of new event objects. The
`NewEvent` class should be used to construct new event objects. The `append_to_stream()`
operation is atomic, so that either all or none of the new events will be recorded. It
is not possible with EventStoreDB atomically to record new events in more than one stream.

This method also has an optional `timeout` argument, which is a Python `float`
that sets a deadline for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

In the example below, a new event, `event1`, is appended to a new stream. The stream
does not yet exist, so `current_version` is `StreamState.NO_STREAM`.

```python
# Construct a new event object.
event1 = NewEvent(type='OrderCreated', data=b'data1')

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
event2 = NewEvent(type='OrderUpdated', data=b'data2')
event3 = NewEvent(type='OrderDeleted', data=b'data3')

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
if a `WrongCurrentVersion` exception is raised when retrying the method call, then we can assume
both that the initial method call did not in fact successfully record the events, and also
that subsequent events have in the meantime been recorded by somebody else. In this case,
an application command which generated the new events may need to be executed again. And
the user of the application may need to be given an opportunity to decide if they still wish to
proceed with their original intention, by displaying a suitable error with an up-to-date view of
the recorded state. In the case where concurrency controls have been disabled, by using `StreamState.ANY` or
`StreamState.EXISTS` as the value of `current_position`, retrying a method call that failed to
return successfully will, more simply, just attempt to ensure the new events are recorded, regardless
of their resulting stream positions. In either case, when the method call does return successfully, we
can be sure the events have been recorded.

The example below shows the `append_to_stream()` method being called again with events
`event2` and `event3`, and with `current_version=0`. We can see that repeating the call
to `append_to_stream()` returns successfully without raising a `WrongCurrentVersion`
exception, as it would if the `append_to_stream() operation were not idempotent.

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

The optional `timeout` argument is a Python `float` which sets a deadline for
the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
override call credentials derived from the connection string URI.

The example below shows the default behavior, which is to return all the recorded
events of a stream forwards from the first recorded events to the last.

```python
events = client.get_stream(
    stream_name=stream_name1
)

assert len(events) == 3
assert events[0].id == event1.id
assert events[1].id == event2.id
assert events[2].id == event3.id
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
assert events[0].id == event2.id
assert events[1].id == event3.id
```

The example below shows how to use the `backwards` argument to read a stream backwards.

```python
events = client.get_stream(
    stream_name=stream_name1,
    backwards=True,
)

assert len(events) == 3
assert events[0].id == event3.id
assert events[1].id == event2.id
assert events[2].id == event1.id
```

The example below shows how to use the `limit` argument to read a limited number of
events.

```python
events = client.get_stream(
    stream_name=stream_name1,
    limit=2,
)

assert len(events) == 2
assert events[0].id == event1.id
assert events[1].id == event2.id
```

The `read_stream()` and `get_stream()` methods will raise a `NotFound` exception if the
named stream has never existed or has been deleted.

```python
from esdbclient.exceptions import NotFound


try:
    client.get_stream('does-not-exist')
except NotFound:
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

For the same reason, `read_stream()` will not raise a `NotFound` exception when
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

This method also has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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

### How to implement snapshotting with EventStoreDB<a id="how-to-implement-snapshotting-with-eventstoredb"></a>

Snapshots can improve the performance of aggregates that would otherwise be
reconstructed from very long streams. However, it is generally recommended to design
aggregates to have a finite lifecycle, and so to have relatively short streams,
thereby avoiding the need for snapshotting. This "how to" section is intended merely
to show how snapshotting of aggregates can be implemented with EventStoreDB using
this Python client.

Event-sourced aggregates are typically reconstructed from recorded events by calling
a mutator function for each recorded event, evolving from an initial state
`None` to the current state of the aggregate. The function `get_aggregate()` shows
how this can be done. The aggregate ID is used as a stream name. The exception
`AggregateNotFound` is raised if the aggregate stream is not found.

```python
class AggregateNotFound(Exception):
    """Raised when an aggregate is not found."""


def get_aggregate(aggregate_id, mutator_func):
    stream_name = aggregate_id

    # Get recorded events.
    try:
        events = client.get_stream(
            stream_name=stream_name,
            stream_position=None
        )
    except NotFound as e:
        raise AggregateNotFound(aggregate_id) from e
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
    except NotFound:
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
    except NotFound as e:
        raise AggregateNotFound(aggregate_id) from e
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
argument matches the event types of EventStoreDB "system events", so that system
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
deadline for the completion of the gRPC operation.

The optional `credentials` argument can be used to
override call credentials derived from the connection string URI.

The filtering of events is done on the EventStoreDB server. The
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
assert next(read_response).id == event1.id
assert next(read_response).id == event2.id
assert next(read_response).id == event3.id

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
assert events[0].id == event1.id
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

The optional `timeout` argument is a Python `float` that sets
a deadline for the completion of the gRPC operation.

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

This method has an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

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

This method has an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

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

Please refer to the EventStoreDB documentation for more information about stream
metadata.

### Delete stream<a id="delete-stream"></a>

*requires leader*

The method `delete_stream()` can be used to "delete" a stream.

This method has two required arguments, `stream_name` and `current_version`.

The required `stream_name` argument is a Python `str` that uniquely identifies a
stream to which a sequence of events will be appended.

The required `current_version` argument is expected to be either a Python `int`
that indicates the stream position of the last recorded event in the stream.

This method has an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

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

This method has an optional `timeout` argument, which is a Python `float` that sets
a deadline for the completion of the gRPC operation.

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

This method also has nine optional arguments, `commit_position`, `from_end`, `resolve_links`,
`filter_exclude`, `filter_include`, `filter_by_stream_name`, `include_checkpoints`,
`timeout` and `credentials`.

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
argument matches the event types of EventStoreDB "system events", so that system
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
whether checkpoints should be included when recorded events are received. Checkpoints
have a `commit_position` value that can be used by an event processing component to
update its recorded commit position value, so that, when lots of events are being
filter out, the subscriber does not have to start from the same old position when
the event processing component is restarted.

The optional `timeout` argument is a Python `float` which sets a
deadline for the completion of the gRPC operation.

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
event4 = NewEvent(type='OrderCreated', data=b'data4')

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


def wait_for_event(event_id):
    for _ in range(100):
        for event in reversed(received_events):
            if event.id == event_id:
                return
        else:
            sleep(0.1)
    else:
        raise AssertionError("Event wasn't received")


thread = Thread(target=receive_events, daemon=True)
thread.start()

# Wait to receive event4.
wait_for_event(event4.id)

# Append another event whilst the subscription is running.
event5 = NewEvent(type='OrderUpdated', data=b'data5')
client.append_to_stream(
    stream_name=stream_name2,
    current_version=0,
    events=[event5],
)

# Wait for the subscription to block.
wait_for_event(event5.id)

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
event6 = NewEvent(type='OrderDeleted', data=b'data6')
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
wait_for_event(event6.id)

# Append three more events to a new stream.
stream_name3 = str(uuid.uuid4())
event7 = NewEvent(type='OrderCreated', data=b'data7')
event8 = NewEvent(type='OrderUpdated', data=b'data8')
event9 = NewEvent(type='OrderDeleted', data=b'data9')

client.append_to_stream(
    stream_name=stream_name3,
    current_version=StreamState.NO_STREAM,
    events=[event7, event8, event9],
)

# Wait for events 7, 8 and 9.
wait_for_event(event7.id)
wait_for_event(event8.id)
wait_for_event(event9.id)

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

This method also has five optional arguments, `stream_position`, `from_end`,
`resolve_links`, `timeout` and `credentials`.

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

The optional `timeout` argument is a Python `float` that sets
a deadline for the completion of the gRPC operation.

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
from a specific commit position using a catch-up subscription in EventStoreDB, the
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
to the EventStoreDB server. Acknowledging events, however, is an aspect of "persistent
subscriptions". Hoping to rely on acknowledging events to an upstream
component is an example of dual writing.


## Persistent subscriptions<a id="persistent-subscriptions"></a>

In EventStoreDB, "persistent" subscriptions are similar to catch-up subscriptions,
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

This method also has nine optional arguments, `from_end`, `commit_position`, `resolve_links`,
`filter_exclude`, `filter_include`, `filter_by_stream_name`, `consumer_strategy`,
`timeout` and `credentials`.

The optional `from_end` argument can be used to specify that the group of consumers
of the subscription should only receive events that were recorded after the subscription
was created.

Alternatively, the optional `commit_position` argument can be used to specify a commit
position from which the group of consumers of the subscription should
receive events. Please note, the recorded event at the specified commit position might
be included in the recorded events received by the group of consumers.

If neither `from_end` or `commit_position` are specified, the group of consumers
of the subscription will potentially receive all recorded events in the database.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

The optional `filter_exclude` argument is a sequence of regular expressions that
specifies which recorded events should be returned. This argument is ignored
if `filter_include` is set to a non-empty sequence. The default value of this
argument matches the event types of EventStoreDB "system events", so that system
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

The optional `timeout` argument is a Python `float` which sets a
deadline for the completion of the gRPC operation.

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

This method has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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
method takes an `event_id`. The value of this argument should be obtained from
the `ack_id` attribute of the recorded event object that is being acknowledged. This
is sometimes not the same as the value of the recorded event's `id` attribute when
subscription has been configured to resolve links.

The example below iterates over the subscription object, and calls `ack()`. The
`stop()` method is called when we have received the last event, so that we can
continue with the examples below.

```python
received_events = []

for event in subscription:
    received_events.append(event)

    # Acknowledge the received event.
    subscription.ack(event_id=event.ack_id)

    # Stop when 'event9' has been received.
    if event.id == event9.id:
        subscription.stop()
```

The events are not guaranteed to be received in the order they were recorded. But
we will have received `event9`.

```python
assert event9.id in [e.id for e in received_events]
```

The `PersistentSubscription` object also has an `nack()` method that should be used
by a consumer to "negatively acknowledge" to the server that it has received but not
successfully processed a recorded event. The `nack()` method has an `event_id`
argument. The value of this argument should be obtained from the `ack_id` attribute
of the recorded event. This is sometimes not the same as the value of the recorded
event's `id` attribute when subscription has been configured to resolve links. The
`nack()` method also has an `action` argument, which should be a Python `str`: either
`'unknown'`, `'park'`, `'retry'`, `'skip'` or `'stop'`.


### How to write a persistent subscription consumer<a id="how-to-write-a-persistent-subscription-consumer"></a>

The reading of a persistent subscription can be encapsulated in a "consumer" that calls
a "policy" function when a recorded event is received and then automatically calls
`ack()` if the policy function returns normally, and `nack()` if it raises an exception,
perhaps retrying the event for a certain number of times before parking the event.

The simple example below shows how this might be done. We can see that 'event9' is
acknowledged before 'event5' is finally parked.


```python
acked_events = {}
nacked_events = {}


class ExampleConsumer:
    def __init__(self, subscription, max_retries, final_action):
        self.subscription = subscription
        self.max_retries = max_retries
        self.final_action = final_action
        self.error = None

    def run(self):
        try:
            for event in self.subscription:
                try:
                    self.policy(event)
                except Exception:
                    if event.retry_count < self.max_retries:
                        action = "retry"
                    else:
                        action = self.final_action
                    self.subscription.nack(event.ack_id, action=action)
                    self.after_nack(event, action)
                else:
                    self.subscription.ack(event.ack_id)
                    self.after_ack(event)
        except Exception:
            self.stop()
            raise

    def stop(self):
        self.subscription.stop()

    def policy(self, event):
        # Raise an exception when we see "event5".
        if event.id == event5.id:
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
    max_retries=5,
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
"persistent subscription".

This method has a required `group_name` argument, which is the
name of a "group" of consumers of the subscription.

This method also has five optional arguments, `from_end`, `commit_position`,
`resolve_links`, `timeout` and `credentials`.

The optional `from_end` argument can be used to specify that the group of consumers
of the subscription should only receive events that were recorded after the subscription
was updated.

Alternatively, the optional `commit_position` argument can be used to specify a commit
position from which commit position the group of consumers of the subscription should
receive events. Please note, the recorded event at the specified commit position might
be included in the recorded events received by the group of consumers.

If neither `from_end` or `commit_position` are specified, the group of consumers
of the subscription will potentially receive all recorded events in the database.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

Please note, the filter options and consumer strategy cannot be adjusted.

The optional `timeout` argument is a Python `float` which sets a
deadline for the completion of the gRPC operation.

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

This method also has six optional arguments, `stream_position`, `from_end`,
`resolve_links`, `consumer_strategy`, `timeout` and `credentials`.

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

The optional `timeout` argument is a Python `float` which sets a deadline
for the completion of the gRPC operation.

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

This method has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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
The for loop breaks when we have received the last event in the stream, so
that we can finish the examples in this documentation.

```python
events = []
for event in subscription:
    events.append(event)

    # Acknowledge the received event.
    subscription.ack(event_id=event.ack_id)

    # Stop when 'event6' has been received.
    if event.id == event6.id:
        subscription.stop()
```

We can check we received all the events that were appended to `stream_name2`
in the examples above.

```python
assert len(events) == 3
assert events[0].stream_name == stream_name2
assert events[0].id == event4.id
assert events[1].stream_name == stream_name2
assert events[1].id == event5.id
assert events[2].stream_name == stream_name2
assert events[2].id == event6.id
```

### Update subscription to stream<a id="update-subscription-to-stream"></a>

*requires leader*

The `update_subscription_to_stream()` method can be used to update a
persistent subscription to a stream.

This method has a required `group_name` argument, which is the
name of a "group" of consumers of the subscription, and a required
`stream_name` argument, which is the name of a stream.

This method also has five optional arguments, `from_end`, `stream_position`,
`resolve_links`, `timeout` and `credentials`.

The optional `from_end` argument can be used to specify that the group of consumers
of the subscription should only receive events that were recorded after the subscription
was updated.

Alternatively, the optional `stream_position` argument can be used to specify a stream
position from which commit position the group of consumers of the subscription should
receive events. Please note, the recorded event at the specified stream position might
be included in the recorded events received by the group of consumers.

If neither `from_end` or `commit_position` are specified, the group of consumers
of the subscription will potentially receive all recorded events in the stream.

The optional `resolve_links` argument is a Python `bool`. The default value of `resolve_links`
is `False`, which means any event links will not be resolved, so that the events that are
returned may represent event links. If `resolve_links` is `True`, any event links will
be resolved, so that the linked events will be returned instead of the event links.

Please note, the consumer strategy cannot be adjusted.

The optional `timeout` argument is a Python `float` which sets a
deadline for the completion of the gRPC operation.

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

This method has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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

This method has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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

This method has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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

This method has an optional `timeout` argument, that is expected to be a
Python `float`, which sets a deadline for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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

This method has an optional `timeout` argument, that
is expected to be a Python `float`, which sets a deadline
for the completion of the gRPC operation.

This method also has an optional `credentials` argument, which can be used to
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

### Call credentials<a id="call-credentials"></a>

Default call credentials are derived by the client from the user info part of the
connection string URI.

Many of the client methods described above have an optional `credentials` argument,
which can be used to set call credentials for an individual method call that override
those derived from the connection string URI.

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
suitable EventStoreDB node. This method uses the same routine for reading the
cluster node states and then connecting to a suitable node according to the
client's node preference that is specified in the connection string URI when
the client is constructed. This method is thread-safe, in that when it is called
by several threads at the same time, only one reconnection will occur. Concurrent
attempts to reconnect will block until the client has reconnected successfully,
and then they will all return normally.

```python
client.reconnect()
```

An example of when it might be desirable to reconnect manually is when (for performance
reasons) the client's node preference is to be connected to a follower node in the
cluster, and, after a cluster leader election, the follower becomes the leader.
Reconnecting to a follower node in this case is currently beyond the capabilities of
this client, but this behavior might be implemented in a future release.

Reconnection will happen automatically in many cases, due to the `@autoreconnect`
decorator.

### Close<a id="close"></a>

The `close()` method can be used to cleanly close the client's gRPC connection.

```python
client.close()
```


## Asyncio client<a id="asyncio-client"></a>

The `esdbclient` package also includes an early version of an asynchronous I/O
gRPC Python client. It follows exactly the same behaviors as the multithreaded
`EventStoreDBClient`, but uses the `grpc.aio` package and the `asyncio` module, instead of
`grpc` and `threading`.

The async function `AsyncioEventStoreDBClient` constructs the client, and connects to
a server. It can be imported from `esdbclient`, and can be called with the same
arguments as `EventStoreDBClient`. It supports both the "esdb" and the "esdb+discover"
connection string URI schemes, and can connect to both "secure" and "insecure"
EventStoreDB servers. It reconnects or retries when connection issues or server
errors are encountered.

```python
from esdbclient import AsyncioEventStoreDBClient
```

The asynchronous I/O client has the following methods: `append_to_stream()`,
`get_stream()`, `read_all()`, `subscribe_to_all()`,
`delete_stream()`, `tombstone_stream()`, and `reconnect()`.

These methods are equivalent to the methods on `EventStoreDBClient`. They have the same
method signatures, and can be called with the same arguments, to the same effect.
The methods which appear on `EventStoreDBClient` but not on `AsyncioEventStoreDBClient` will be
added soon.

### Synopsis<a id="synopsis-1"></a>

The example below demonstrates the `append_to_stream()`, `get_stream()` and
`subscribe_to_all()` methods. These are the most useful methods for writing
an event-sourced application, allowing new aggregate events to be recorded, the
recorded events of an aggregate to be obtained so aggregates can be reconstructed,
and the state of an application to propagated and processed with "exactly-once"
semantics.

```python
import asyncio

async def demonstrate_asyncio_client():

    # Construct client.
    client = await AsyncioEventStoreDBClient(
        uri=os.getenv("ESDB_URI"),
        root_certificates=os.getenv("ESDB_ROOT_CERTIFICATES"),
    )

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

    # Read stream events.
    recorded = await client.get_stream(stream_name)
    assert len(recorded) == 3
    assert recorded[0].id == event1.id
    assert recorded[1].id == event2.id
    assert recorded[2].id == event3.id


    # Subscribe all events.
    received = []
    async for event in await client.subscribe_to_all():
        received.append(event)
        if event.commit_position == commit_position:
            break
    assert received[-3].id == event1.id
    assert received[-2].id == event2.id
    assert received[-1].id == event3.id


    # Close the client.
    await client.close()


# Run the demo.
asyncio.get_event_loop().run_until_complete(
    demonstrate_asyncio_client()
)
```

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

System events generated by EventStoreDB have `type` strings that start with
the `$` sign. Persistence subscription events generated when manipulating
persistence subscriptions have `type` strings that start with `PersistentConfig`.

For example, to match the type of EventStoreDB system events, use the regular
expression string `r'\$.+'`. Please note, the constant `ESDB_SYSTEM_EVENTS_REGEX` is
set to this value. You can import this constant from `esdbclient` and use it when
building longer sequences of regular expressions.

Similarly, to match the type of EventStoreDB persistence subscription events, use the
regular expression `r'PersistentConfig\d+'`. The constant `ESDB_PERSISTENT_CONFIG_EVENTS_REGEX`
is set to this value. You can import this constant from `esdbclient` and use it when
building longer sequences of regular expressions.

The constant `DEFAULT_EXCLUDE_FILTER` is a sequence of regular expressions that includes
both `ESDB_SYSTEM_EVENTS_REGEX` and `ESDB_PERSISTENT_CONFIG_EVENTS_REGEX`. It is used
as the default value of `filter_exclude` so that the events generated internally by
EventStoreDB are excluded by default.

In all methods that have a `filter_exclude` argument, the default value of the argument
is the constant `DEFAULT_EXCLUDE_FILTER`, which is designed to match (and therefore
to exclude) both "system" and "persistence subscription config" event types, which
would otherwise be included.

This value can be extended. For example, if you want to exclude system events and
persistent subscription events and also events that have a type that ends with
`'Snapshot'`, then you can use `DEFAULT_EXCLUDE_FILTER + ['.*Snapshot']` as the
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

The `@retrygrpc` decorator retries operations that have failed due to a deadline being
reached (so that the operation times out), and in case the server throws an exception
when handling a client request.

Please also note, the aspects not covered by the reconnect and retry decorator
behaviours have to do with methods that return iterators. For example, consider
the "read response" iterator returned from the `read_all()` method. The
`read_all()` method will have returned, and the method decorators will therefore
have exited, before iterating over the "read response" begins. Therefore, if a
connection issues occurs whilst iterating over the "read response", it isn't possible
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
project and install the project's package dependencies into it,
using the following command.

    $ make install-packages

It's also possible to also install the project in 'editable mode'.

    $ make install

Please note, if you create the virtual environment in this way, and then try to
open the project in PyCharm and configure the project to use this virtual
environment as an "Existing Poetry Environment", PyCharm sometimes has some
issues (don't know why) which might be problematic. If you encounter such
issues, you can resolve these issues by deleting the virtual environment
and creating the Poetry virtual environment using PyCharm (see above).

### Project Makefile commands<a id="project-makefile-commands"></a>

You can start EventStoreDB using the following command.

    $ make start-eventstoredb

You can run tests using the following command (needs EventStoreDB to be running).

    $ make test

You can stop EventStoreDB using the following command.

    $ make stop-eventstoredb

You can check the formatting of the code using the following command.

    $ make lint

You can reformat the code using the following command.

    $ make fmt

Tests belong in `./tests`. Code-under-test belongs in `./esdbclient`.

Edit package dependencies in `pyproject.toml`. Update installed packages (and the
`poetry.lock` file) using the following command.

    $ make update-packages
