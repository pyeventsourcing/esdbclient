---
order: 9
---

# Observability

This guide explains how to instrument and export telemetry data from the Python clients.

## Introduction

The Python client package provide [OpenTelemetry](https://opentelemetry.io) intrumentors.

This enables you to monitor, trace, and troubleshoot your event store operations with
distributed tracing support, for both the sync and async Python clients.

## Instrumenting a client

The Python client instrumentors depend on various OpenTelemetry Python packages, which
you will need to install.

### Install package

To ensure verified version compatibility,
install `kurrentdbclient` with the `opentelemetry` option.

```bash:no-line-numbers
pip install kurrentdbclient[opentelemetry]
```

### Activate instrumentor

You can then activate the client instrumentors within your application code.

::: tabs
@tab sync
```python:no-line-numbers
from kurrentdbclient.instrumentation.opentelemetry import (
    KurrentDBClientInstrumentor,
)

# Activate sync client instrumentation.
KurrentDBClientInstrumentor().instrument()

# Deactivate sync client instrumentation.
KurrentDBClientInstrumentor().uninstrument()
```

@tab async
```python:no-line-numbers
from kurrentdbclient.instrumentation.opentelemetry import (
    AsyncKurrentDBClientInstrumentor,
)

# Activate async client instrumentation.
AsyncKurrentDBClientInstrumentor().instrument()

# Deactivate async client instrumentation.
AsyncKurrentDBClientInstrumentor().uninstrument()
```
:::


## Exporting telemetry data

In order to export telemetry data, you will need to
initialise the global "tracer provider".

### Console exporter

For example, to export data to the console you will need to install the Python
package `opentelemetry-sdk`, and use the class `TracerProvider`, `BatchSpanProcessor`,
and `ConsoleSpanExporter` in the following way.

```python:no-line-numbers
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.trace import set_tracer_provider

resource = Resource.create(
    attributes={
        SERVICE_NAME: "kurrentdb",
    }
)
provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(
        ConsoleSpanExporter()
    )
)
set_tracer_provider(provider)
```

### OTLP exporter

To export data to an OpenTelemetry compatible data collector, such as
[Jaeger](https://www.jaegertracing.io), you will need to install the Python package
`opentelemetry-exporter-otlp-proto-http`, and then use the class `OTLPSpanExporter`
from the `opentelemetry.exporter.otlp.proto.http.trace_exporter` module, with an
appropriate `endpoint` argument for your collector.

```python:no-line-numbers
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
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
provider.add_span_processor(
    BatchSpanProcessor(
        OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
    )
)
set_tracer_provider(provider)
```

You can start Jaeger locally by running the following command.

```bash:no-line-numbers
docker run \
-d \
-p 4318:4318 \
-p 16686:16686 \
--name jaeger \
jaegertracing/all-in-one:latest
```

Telemetry data from the client instrumentors can then be exported to `http://localhost:4318/v1/traces`.
You can navigate to `http://localhost:16686` to access the Jaeger UI.

You can find a list of available exporters for different platforms in the
[OpenTelemetry Registry](https://opentelemetry.io/ecosystem/registry/?component=exporter&language=python).

For detailed configuration options, refer to the OpenTelemetry [Python documentation](https://opentelemetry.io/docs/languages/python/).

## Understanding traces

### What gets traced

At this time, the instrumented methods are [`append_to_stream()`](./appending-events.md#append-to-stream),
[`multi_append_to_stream()`](./appending-events.md#multi-append-to-stream),
[`subscribe_to_stream()`](./subscriptions.md#subscribe-to-stream),
[`subscribe_to_all()`](./subscriptions.md#subscribe-to-all),
[`read_subscription_to_stream()`](./persistent-subscriptions.md#read-subscription-to-stream),
and [`read_subscription_to_all()`](./persistent-subscriptions.md#read-subscription-to-all).

The append methods are instrumented by spanning the method call with a "producer" span.

The subscription methods are instrumented by instrumenting the response iterators,
creating a "consumer" span for each recorded event received.

The producer spans add span context information to event metadata. The "consumer"
spans extract this information from the recorded event metadata, and make each
"consumer" span a child of a "producer" parent span.


### Producer span

Each span includes attributes to help with monitoring and debugging.

Producer spans for [appending to a single stream](./appending-events.md#append-to-stream) have the following attributes:

| Attribute                    | Description                            | Example             |
|------------------------------|----------------------------------------|---------------------|
| db.operation                 | Type of operation performed            | `"streams.append"`  |
| db.system                    | Database system identifier             | `"kurrentdb"`       |
| db.user                      | Database user name                     | `"admin"`           |
| db.kurrentdb.stream          | Stream name or identifier              | `"user-events-123"` |
| server.address               | KurrentDB server address               | `"localhost"`       |
| server.port                  | KurrentDB server port                  | `"2113"`            |

Producer spans for [appending to multiple streams](./appending-events.md#multi-append-to-stream) have the following attributes:

| Attribute                    | Description                            | Example             |
|------------------------------|----------------------------------------|---------------------|
| db.operation                 | Type of operation performed            | `"streams.append"`  |
| db.system                    | Database system identifier             | `"kurrentdb"`       |
| db.user                      | Database user name                     | `"admin"`           |
| server.address               | KurrentDB server address               | `"localhost"`       |
| server.port                  | KurrentDB server port                  | `"2113"`            |

#### Example

Here's an instrumentor span for a successful [`append_to_stream()`](./appending-events.md#append-to-stream) operation.

```json:no-line-numbers
{
    "name": "streams.append",
    "context": {
        "trace_id": "0x82ac04990e711b6f35348556006fe4cf",
        "span_id": "0x9852ade35f00d350",
        "trace_state": "[]"
    },
    "kind": "SpanKind.PRODUCER",
    "parent_id": null,
    "start_time": "2026-02-17T13:59:23.842871Z",
    "end_time": "2026-02-17T13:59:23.866696Z",
    "status": {
        "status_code": "OK"
    },
    "attributes": {
        "db.operation": "streams.append",
        "db.system": "kurrentdb",
        "db.user": "admin",
        "db.kurrentdb.stream": "user-123",
        "server.address": "localhost",
        "server.port": "2113"
    },
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.39.1",
            "service.name": "kurrentdb"
        },
        "schema_url": ""
    }
}
```


### Consumer span

Consumer spans have the following attributes.

| Attribute                    | Description                            | Example                                  |
|------------------------------|----------------------------------------|------------------------------------------|
| db.operation                 | Type of operation performed            | `"streams.subscribe"`                    |
| db.system                    | Database system identifier             | `"kurrentdb"`                            |
| db.user                      | Database user name                     | `"admin"`                                |
| db.kurrentdb.event.id        | Event identifier                       | `"e7548b90-d79b-4474-b00f-631de4285acc"` |
| db.kurrentdb.event.type      | Event type identifier                  | `"AccountRegistered"`                    |
| db.kurrentdb.stream          | Stream name or identifier              | `"user-123"`                             |
| db.kurrentdb.subscription.id | Subscription identifier                | `"user-123-subscription-1"`       |
| server.address               | KurrentDB server address               | `"localhost"`                            |
| server.port                  | KurrentDB server port                  | `"2113"`                                 |

#### Example

Here's an instrumentor span from a [catch-up subscription](./subscriptions.md) operation.

```json:no-line-numbers
{
    "name": "streams.subscribe",
    "context": {
        "trace_id": "0x5ad5e1bcff7f33cb44b93d470bd34554",
        "span_id": "0x446cf48b1bb9e574",
        "trace_state": "[]"
    },
    "kind": "SpanKind.CONSUMER",
    "parent_id": "0x1496f8ba3507977b",
    "start_time": "2026-02-17T14:16:20.810515Z",
    "end_time": "2026-02-17T14:16:20.810605Z",
    "status": {
        "status_code": "OK"
    },
    "attributes": {
        "db.operation": "streams.subscribe",
        "db.system": "kurrentdb",
        "db.user": "admin",
        "db.kurrentdb.event.id": "4ca26d3e-cbec-477e-9e59-d9248d8a3aef",
        "db.kurrentdb.event.type": "UserRegistered",
        "db.kurrentdb.stream": "user-123",
        "db.kurrentdb.subscription.id": "5da1a8c8-3dec-441e-8b6f-7514c797b1b4",
        "server.address": "localhost",
        "server.port": "2113"
    },
    "events": [],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.39.1",
            "service.name": "kurrentdb"
        },
        "schema_url": ""
    }
}
```


### Span errors

Errors are traced by including a "span event" with the following attributes.

| Attribute            | Description                                             | Example                                                |
|----------------------|---------------------------------------------------------|--------------------------------------------------------|
| exception.type       | Exception type if an error occurred                     | `"ServiceUnavailableError"` |
| exception.message    | Exception message if an error occurred                  | `"failed to connect to all addresses"`                 |
| exception.stacktrace | Stack trace of the exception                            | `"Traceback (most recent call last):\n  File..."`      |
| exception.escaped    | Whether the exception is escaping the scope of the span | `"True"`                                               |

#### Example

Here's an instrumentor span for an errorful [`append_to_stream()`](./appending-events.md#append-to-stream) operation.

```json:no-line-numbers
{
    "name": "streams.append",
    "context": {
        "trace_id": "0xb99bba6da5c45dd2c72cca9f50064edd",
        "span_id": "0x31db5b46eac4a92e",
        "trace_state": "[]"
    },
    "kind": "SpanKind.PRODUCER",
    "parent_id": null,
    "start_time": "2026-02-17T13:59:27.614387Z",
    "end_time": "2026-02-17T13:59:27.940788Z",
    "status": {
        "status_code": "ERROR",
        "description": "ServiceUnavailableError: failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1000: Failed to connect to remote host: connect: Connection refused (61)"
    },
    "attributes": {
        "db.operation": "streams.append",
        "db.system": "kurrentdb",
        "db.user": "admin",
        "db.kurrentdb.stream": "user-123",
        "server.address": "localhost",
        "server.port": "2113"
    },
    "events": [
        {
            "name": "exception",
            "timestamp": "2026-02-17T13:59:27.940712Z",
            "attributes": {
                "exception.type": "kurrentdbclient.exceptions.ServiceUnavailableError",
                "exception.message": "failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1000: Failed to connect to remote host: connect: Connection refused (61)",
                "exception.stacktrace": "Traceback (most recent call last):\n  File \"/venv/kurrentdbclient/client.py\", line 152, in retrygrpc_decorator\n    return f(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^\n  File \"/venv/kurrentdbclient/client.py\", line 140, in autoreconnect_decorator\n    return f(*args, **kwargs)\n           ^^^^^^^^^^^^^^^^^^\n  File \"/venv/kurrentdbclient/client.py\", line 549, in append_to_stream\n    return self.streams.batch_append(\n           ^^^^^^^^^^^^^^^^^^^^^^^^^^\n  File \"/venv/kurrentdbclient/streams.py\", line 1331, in batch_append\n    raise handle_rpc_error(e) from None\nkurrentdbclient.exceptions.ServiceUnavailableError: failed to connect to all addresses; last error: UNKNOWN: ipv4:127.0.0.1:1000: Failed to connect to remote host: connect: Connection refused (61)\n",
                "exception.escaped": "True"
            }
        }
    ],
    "links": [],
    "resource": {
        "attributes": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.39.1",
            "service.name": "kurrentdb"
        },
        "schema_url": ""
    }
}
```
