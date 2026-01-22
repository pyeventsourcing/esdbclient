---
title: Authentication
order: 8
---

# Client x.509 certificate

<Badge type="info" vertical="middle" text="License Required"/>

X.509 certificates are digital certificates that use the X.509 public key infrastructure (PKI) standard to verify the identity of clients and servers. They play a crucial role in establishing a secure connection by providing a way to authenticate identities and establish trust.

## Prerequisites

1. KurrentDB 25.0 or greater, or EventStoreDB 24.10 or later.
2. A valid X.509 certificate configured on the Database. See [configuration steps](@server/security/user-authentication.html#user-x-509-certificates) for more details.

## Connect using an x.509 certificate

To connect using an x.509 certificate, you need to [configure the client](./getting-started.md#client-configuration)
by providing the certificate and the private key to the client.

Use the following client [connection string](./connection-strings.md#options) options:

| Parameter      | Description                                                                    |
|----------------|--------------------------------------------------------------------------------|
| `userCertFile` | The file containing the X.509 user certificate in PEM format.                  |
| `userKeyFile`  | The file containing the user certificate’s matching private key in PEM format. |

### Example

Here's an example for connecting to KurrentDB with a client certificate.

```python:no-line-numbers
connection_string = (
    "kurrentdb://node1.example.com:2113?"
    "userCertFile=/path/to/user_cert.pem&"
    "userKeyFile=/path/to/user_key.pem"
)
```
