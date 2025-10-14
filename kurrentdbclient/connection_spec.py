from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any
from urllib.parse import ParseResult, parse_qs, urlparse
from uuid import uuid4

from kurrentdbclient.common import grpc_target

if TYPE_CHECKING:
    from collections.abc import Sequence

URI_SCHEME_ESDB = "esdb"
URI_SCHEME_ESDB_DISCOVER = "esdb+discover"
URI_SCHEME_KURRENTDB = "kurrentdb"
URI_SCHEME_KURRENTDB_DISCOVER = "kurrentdb+discover"
URI_SCHEME_KDB = "kdb"
URI_SCHEME_KDB_DISCOVER = "kdb+discover"

URI_SCHEMES_NON_DISCOVER = [
    URI_SCHEME_ESDB,
    URI_SCHEME_KURRENTDB,
    URI_SCHEME_KDB,
]

URI_SCHEMES_DISCOVER = [
    URI_SCHEME_ESDB_DISCOVER,
    URI_SCHEME_KURRENTDB_DISCOVER,
    URI_SCHEME_KDB_DISCOVER,
]

URI_SCHEMES_ALL = URI_SCHEMES_NON_DISCOVER + URI_SCHEMES_DISCOVER

NODE_PREFERENCE_LEADER = "leader"
NODE_PREFERENCE_FOLLOWER = "follower"
NODE_PREFERENCE_RANDOM = "random"
NODE_PREFERENCE_REPLICA = "readonlyreplica"
VALID_NODE_PREFERENCES = [
    NODE_PREFERENCE_LEADER,
    NODE_PREFERENCE_FOLLOWER,
    NODE_PREFERENCE_RANDOM,
    NODE_PREFERENCE_REPLICA,
]
VALID_CONNECTION_QUERY_STRING_FIELDS = [
    "Tls",
    "ConnectionName",
    "MaxDiscoverAttempts",
    "DiscoveryInterval",
    "GossipTimeout",
    "NodePreference",
    "TlsVerifyCert",
    "DefaultDeadline",
    "KeepAliveInterval",
    "KeepAliveTimeout",
    "TlsCaFile",
    "UserCertFile",
    "UserKeyFile",
]


class ConnectionOptions:
    __slots__ = [
        "_" + re.sub("(?<!^)(?=[A-Z])", "_", s).lower()
        for s in VALID_CONNECTION_QUERY_STRING_FIELDS
    ]

    def __init__(self, query: str):
        # Parse query string (case insensitivity, assume single values).
        options = {k.upper(): v[0] for k, v in parse_qs(query).items()}

        self._validate_field_names(options)
        self._set_tls(options)
        self._set_connection_name(options)
        self._set_max_discover_attempts(options)
        self._set_discovery_interval(options)
        self._set_gossip_timeout(options)
        self._set_node_preference(options)
        self._set_tls_verify_cert(options)
        self._set_default_deadline(options)
        self._set_keep_alive_interval(options)
        self._set_keep_alive_timeout(options)
        self._set_tls_ca_file(options)
        self._set_user_cert_file(options)
        self._set_user_key_file(options)

    @staticmethod
    def _validate_field_names(options: dict[str, Any]) -> None:
        valid_fields = [s.upper() for s in VALID_CONNECTION_QUERY_STRING_FIELDS]
        invalid_fields = [field for field in options if field not in valid_fields]
        if len(invalid_fields) > 0:
            plural = "s" if len(invalid_fields) > 1 else ""
            joined_fields = ", ".join(invalid_fields)
            msg = f"Unknown field{plural} in connection query string: {joined_fields}"
            raise ValueError(msg)

    def _set_tls(self, options: dict[str, Any]) -> None:
        _tls = options.get("Tls".upper())
        if _tls is None:
            self._tls = True
        else:
            valid_tls_values = ["true", "false"]
            if _tls.lower() not in valid_tls_values:
                msg = f"'{_tls}' not one of: {', '.join(valid_tls_values)}"
                raise ValueError(msg)
            self._tls = _tls.lower() == "true"

    def _set_connection_name(self, options: dict[str, Any]) -> None:
        _connection_name = options.get("ConnectionName".upper())
        if _connection_name is None:
            self._connection_name = str(uuid4())
        else:
            self._connection_name = _connection_name

    def _set_max_discover_attempts(self, options: dict[str, Any]) -> None:
        _max_discover_attempts = options.get("MaxDiscoverAttempts".upper())
        if _max_discover_attempts is None:
            self._max_discover_attempts = 10
        else:
            self._max_discover_attempts = int(_max_discover_attempts)

    def _set_discovery_interval(self, options: dict[str, Any]) -> None:
        _discovery_interval = options.get("DiscoveryInterval".upper())
        if _discovery_interval is None:
            self._discovery_interval = 100
        else:
            self._discovery_interval = int(_discovery_interval)

    def _set_gossip_timeout(self, options: dict[str, Any]) -> None:
        _gossip_timeout = options.get("GossipTimeout".upper())
        if _gossip_timeout is None:
            self._gossip_timeout = 5
        else:
            self._gossip_timeout = int(_gossip_timeout)

    def _set_node_preference(self, options: dict[str, Any]) -> None:
        _node_preference = options.get("NodePreference".upper())
        if _node_preference is None:
            self._node_preference = NODE_PREFERENCE_LEADER
        else:
            if _node_preference.lower() not in VALID_NODE_PREFERENCES:
                msg = (
                    f"'{_node_preference}' not one of:"
                    f" {', '.join(VALID_NODE_PREFERENCES)}"
                )
                raise ValueError(msg)
            self._node_preference = _node_preference.lower()

    def _set_tls_verify_cert(self, options: dict[str, Any]) -> None:
        _tls_verify_cert = options.get("TlsVerifyCert".upper())
        if _tls_verify_cert is None:
            self._tls_verify_cert = True
        else:
            valid_tls_verify_cert_values = ["true", "false"]
            if _tls_verify_cert.lower() not in valid_tls_verify_cert_values:
                msg = (
                    f"'{_tls_verify_cert}' not one of:"
                    f" {', '.join(valid_tls_verify_cert_values)}"
                )
                raise ValueError(msg)
            self._tls_verify_cert = _tls_verify_cert.lower() == "true"

    def _set_default_deadline(self, options: dict[str, Any]) -> None:
        _default_deadline = options.get("DefaultDeadline".upper())
        if _default_deadline is None:
            self._default_deadline: int | None = None
        else:
            self._default_deadline = int(_default_deadline)

    def _set_keep_alive_interval(self, options: dict[str, Any]) -> None:
        _keep_alive_interval = options.get("KeepAliveInterval".upper())
        if _keep_alive_interval is None:
            self._keep_alive_interval: int | None = None
        else:
            self._keep_alive_interval = int(_keep_alive_interval)

    def _set_keep_alive_timeout(self, options: dict[str, Any]) -> None:
        _keep_alive_timeout = options.get("KeepAliveTimeout".upper())
        if _keep_alive_timeout is None:
            self._keep_alive_timeout: int | None = None
        else:
            self._keep_alive_timeout = int(_keep_alive_timeout)

    def _set_tls_ca_file(self, options: dict[str, Any]) -> None:
        _tls_ca_file = options.get("TlsCaFile".upper())
        if _tls_ca_file is None:
            self._tls_ca_file: str | None = None
        else:
            self._tls_ca_file = str(_tls_ca_file)

    def _set_user_cert_file(self, options: dict[str, Any]) -> None:
        _user_cert_file = options.get("UserCertFile".upper())
        if _user_cert_file is None:
            self._user_cert_file: str | None = None
        else:
            self._user_cert_file = str(_user_cert_file)

    def _set_user_key_file(self, options: dict[str, Any]) -> None:
        _user_key_file = options.get("UserKeyFile".upper())
        if _user_key_file is None:
            self._user_key_file: str | None = None
        else:
            self._user_key_file = str(_user_key_file)

    @property
    def tls(self) -> bool:
        """
        Controls whether client will use a secure channel (has to match server).

        Valid values in URI: 'true', 'false'.
        """
        return self._tls

    @property
    def connection_name(self) -> str:
        """
        This value is sent as header 'connection-name' in all calls to server.

        Defaults to a new version 4 UUID string.
        """
        return self._connection_name

    @property
    def max_discover_attempts(self) -> int:
        """
        Number of attempts to connect to gossip before giving up.
        """
        return self._max_discover_attempts

    @property
    def discovery_interval(self) -> int:
        """
        How long to wait (in milliseconds) between gossip retries.
        """
        return self._discovery_interval

    @property
    def gossip_timeout(self) -> int:
        """
        How long to wait (in seconds) for a response to a request to gossip API.
        """
        return self._gossip_timeout

    @property
    def node_preference(
        self,
    ) -> str:
        """
        Controls whether requests are directed to another node.

        Value values: 'leader', 'follower', 'random', 'readonlyreplica'.
        """
        return self._node_preference

    @property
    def tls_verify_cert(self) -> bool:
        """
        Controls whether certificate is verified.

        Valid values in URI: 'true', 'false'.
        """
        return self._tls_verify_cert

    @property
    def default_deadline(self) -> int | None:
        """
        Default deadline (in seconds) for calls to the server that write data.
        """
        return self._default_deadline

    @property
    def keep_alive_interval(self) -> int | None:
        """
        gRPC "keep alive" interval (in milliseconds).
        """
        return self._keep_alive_interval

    @property
    def keep_alive_timeout(self) -> int | None:
        """
        gRPC "keep alive timeout" (in milliseconds).
        """
        return self._keep_alive_timeout

    @property
    def tls_ca_file(self) -> str | None:
        """
        Path to file containing root CA certificate(s) to verify server.
        """
        return self._tls_ca_file

    @property
    def user_cert_file(self) -> str | None:
        """
        Path to file containing user X.509 certificate.
        """
        return self._user_cert_file

    @property
    def user_key_file(self) -> str | None:
        """
        Path to file containing user X.509 key.
        """
        return self._user_key_file


class ConnectionSpec:
    __slots__ = [
        "_uri",
        "_scheme",
        "_netloc",
        "_username",
        "_password",
        "_targets",
        "_options",
    ]

    def __init__(self, uri: str | None = None):
        self._uri = uri or ""
        parse_result: ParseResult = urlparse(self._uri)
        if parse_result.scheme not in URI_SCHEMES_ALL:
            msg = (
                f"Invalid URI scheme: '{parse_result.scheme}' not in:"
                f" {', '.join(URI_SCHEMES_ALL)}: {uri}"
            )
            raise ValueError(msg)
        self._scheme = parse_result.scheme
        self._netloc = parse_result.netloc
        self._username = parse_result.username
        self._password = parse_result.password
        if "@" in self._netloc:
            _, _, targets = self._netloc.partition("@")
        else:
            targets = self._netloc
        self._targets = [t.strip() for t in targets.split(",") if t.strip()]

        if len(self._targets) == 0:
            msg = f"No targets specified: {uri}"
            raise ValueError(msg)
        if self._scheme in URI_SCHEMES_DISCOVER and len(self._targets) > 1:
            msg = f"More than one target specified: {uri}"
            raise ValueError(msg)

        for i, target in enumerate(self._targets):
            host, _, port = target.partition(":")
            if port == "":
                self._targets[i] = grpc_target(host, 2113)

        self._options = ConnectionOptions(parse_result.query)
        if self._options.tls is True and not (self._username and self._password):
            msg = f"Username and password are required: {uri}"
            raise ValueError(msg)

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def username(self) -> str | None:
        return self._username

    @property
    def password(self) -> str | None:
        return self._password

    @property
    def targets(self) -> Sequence[str]:
        return self._targets

    @property
    def options(self) -> ConnectionOptions:
        return self._options
