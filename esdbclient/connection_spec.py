# -*- coding: utf-8 -*-
from typing import Any, Dict, Optional, Sequence
from urllib.parse import ParseResult, parse_qs, urlparse
from uuid import uuid4

URI_SCHEME_ESDB = "esdb"
URI_SCHEME_ESDB_DISCOVER = "esdb+discover"
VALID_URI_SCHEMES = [
    URI_SCHEME_ESDB,
    URI_SCHEME_ESDB_DISCOVER,
]
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
]


class ConnectionOptions:
    __slots__ = [f"_{s}" for s in VALID_CONNECTION_QUERY_STRING_FIELDS]

    def __init__(self, query: str):
        # Parse query string (case insensitivity, assume single values).
        options = {k.upper(): v[0] for k, v in parse_qs(query).items()}

        self._validate_field_names(options)
        self._set_Tls(options)
        self._set_ConnectionName(options)
        self._set_MaxDiscoverAttempts(options)
        self._set_DiscoveryInterval(options)
        self._set_GossipTimeout(options)
        self._set_NodePreference(options)
        self._set_TlsVerifyCert(options)
        self._set_DefaultDeadline(options)
        self._set_KeepAliveInterval(options)
        self._set_KeepAliveTimeout(options)

    @staticmethod
    def _validate_field_names(options: Dict[str, Any]) -> None:
        valid_fields = [s.upper() for s in VALID_CONNECTION_QUERY_STRING_FIELDS]
        invalid_fields = []
        for field in options.keys():
            if field not in valid_fields:
                invalid_fields.append(field)
        if len(invalid_fields) > 0:
            plural = "s" if len(invalid_fields) > 1 else ""
            joined_fields = ", ".join(invalid_fields)
            raise ValueError(
                f"Unknown field{plural} in connection query string: {joined_fields}"
            )

    def _set_Tls(self, options: Dict[str, Any]) -> None:
        _Tls = options.get("Tls".upper())
        if _Tls is None:
            self._Tls = True
        else:
            validTlsValues = ["true", "false"]
            if _Tls.lower() not in validTlsValues:
                raise ValueError(f"'{_Tls}' not one of: {', '.join(validTlsValues)}")
            elif _Tls.lower() == "true":
                self._Tls = True
            else:
                self._Tls = False

    def _set_ConnectionName(self, options: Dict[str, Any]) -> None:
        _ConnectionName = options.get("ConnectionName".upper())
        if _ConnectionName is None:
            self._ConnectionName = str(uuid4())
        else:
            self._ConnectionName = _ConnectionName

    def _set_MaxDiscoverAttempts(self, options: Dict[str, Any]) -> None:
        _MaxDiscoverAttempts = options.get("MaxDiscoverAttempts".upper())
        if _MaxDiscoverAttempts is None:
            self._MaxDiscoverAttempts = 10
        else:
            self._MaxDiscoverAttempts = int(_MaxDiscoverAttempts)

    def _set_DiscoveryInterval(self, options: Dict[str, Any]) -> None:
        _DiscoveryInterval = options.get("DiscoveryInterval".upper())
        if _DiscoveryInterval is None:
            self._DiscoveryInterval = 100
        else:
            self._DiscoveryInterval = int(_DiscoveryInterval)

    def _set_GossipTimeout(self, options: Dict[str, Any]) -> None:
        _GossipTimeout = options.get("GossipTimeout".upper())
        if _GossipTimeout is None:
            self._GossipTimeout = 5
        else:
            self._GossipTimeout = int(_GossipTimeout)

    def _set_NodePreference(self, options: Dict[str, Any]) -> None:
        _NodePreference = options.get("NodePreference".upper())
        if _NodePreference is None:
            self._NodePreference = NODE_PREFERENCE_LEADER
        else:
            if _NodePreference.lower() not in VALID_NODE_PREFERENCES:
                raise ValueError(
                    f"'{_NodePreference}' not one of:"
                    f" {', '.join(VALID_NODE_PREFERENCES)}"
                )
            self._NodePreference = _NodePreference.lower()

    def _set_TlsVerifyCert(self, options: Dict[str, Any]) -> None:
        _TlsVerifyCert = options.get("TlsVerifyCert".upper())
        if _TlsVerifyCert is None:
            self._TlsVerifyCert = True
        else:
            validTlsVerifyCertValues = ["true", "false"]
            if _TlsVerifyCert.lower() not in validTlsVerifyCertValues:
                raise ValueError(
                    f"'{_TlsVerifyCert}' not one of:"
                    f" {', '.join(validTlsVerifyCertValues)}"
                )
            elif _TlsVerifyCert.lower() == "true":
                self._TlsVerifyCert = True
            else:
                self._TlsVerifyCert = False

    def _set_DefaultDeadline(self, options: Dict[str, Any]) -> None:
        _DefaultDeadline = options.get("DefaultDeadline".upper())
        if _DefaultDeadline is None:
            self._DefaultDeadline: Optional[int] = None
        else:
            self._DefaultDeadline = int(_DefaultDeadline)

    def _set_KeepAliveInterval(self, options: Dict[str, Any]) -> None:
        _KeepAliveInterval = options.get("KeepAliveInterval".upper())
        if _KeepAliveInterval is None:
            self._KeepAliveInterval: Optional[int] = None
        else:
            self._KeepAliveInterval = int(_KeepAliveInterval)

    def _set_KeepAliveTimeout(self, options: Dict[str, Any]) -> None:
        _KeepAliveTimeout = options.get("KeepAliveTimeout".upper())
        if _KeepAliveTimeout is None:
            self._KeepAliveTimeout: Optional[int] = None
        else:
            self._KeepAliveTimeout = int(_KeepAliveTimeout)

    @property
    def Tls(self) -> bool:
        """
        Controls whether client will use a secure channel (has to match server).

        Valid values in URI: 'true', 'false'.
        """
        return self._Tls

    @property
    def ConnectionName(self) -> str:
        """
        This value is sent as header 'connection-name' in all calls to server.

        Defaults to a new version 4 UUID string.
        """
        return self._ConnectionName

    @property
    def MaxDiscoverAttempts(self) -> int:
        """
        Number of attempts to connect to gossip before giving up.
        """
        return self._MaxDiscoverAttempts

    @property
    def DiscoveryInterval(self) -> int:
        """
        How long to wait (in milliseconds) between gossip retries.
        """
        return self._DiscoveryInterval

    @property
    def GossipTimeout(self) -> int:
        """
        How long to wait (in seconds) for a response to a request to gossip API.
        """
        return self._GossipTimeout

    @property
    def NodePreference(
        self,
    ) -> str:
        """
        Controls whether requests are directed to another node.

        Value values: 'leader', 'follower', 'random', 'readonlyreplica'.
        """
        return self._NodePreference

    @property
    def TlsVerifyCert(self) -> bool:
        """
        Controls whether certificate is verified.

        Valid values in URI: 'true', 'false'.
        """
        return self._TlsVerifyCert

    @property
    def DefaultDeadline(self) -> Optional[int]:
        """
        Default deadline (in seconds) for calls to the server that write data.
        """
        return self._DefaultDeadline

    @property
    def KeepAliveInterval(self) -> Optional[int]:
        """
        gRPC "keep alive" interval (in milliseconds).
        """
        return self._KeepAliveInterval

    @property
    def KeepAliveTimeout(self) -> Optional[int]:
        """
        gRPC "keep alive timeout" (in milliseconds).
        """
        return self._KeepAliveTimeout


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

    def __init__(self, uri: Optional[str] = None):
        self._uri = uri or ""
        parse_result: ParseResult = urlparse(self._uri)
        if parse_result.scheme not in VALID_URI_SCHEMES:
            raise ValueError(
                f"Invalid URI scheme: '{parse_result.scheme}' not in:"
                f" {', '.join(VALID_URI_SCHEMES)}: {uri}"
            )
        self._scheme = parse_result.scheme
        self._netloc = parse_result.netloc
        self._username = parse_result.username
        self._password = parse_result.password
        if "@" in self._netloc:
            _, _, targets = self._netloc.partition("@")
        else:
            targets = self._netloc
        self._targets = [t.strip() for t in targets.split(",") if t.strip()]
        self._options = ConnectionOptions(parse_result.query)

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return self._scheme

    @property
    def netloc(self) -> str:
        return self._netloc

    @property
    def username(self) -> Optional[str]:
        return self._username

    @property
    def password(self) -> Optional[str]:
        return self._password

    @property
    def targets(self) -> Sequence[str]:
        return self._targets

    @property
    def options(self) -> ConnectionOptions:
        return self._options
