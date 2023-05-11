# -*- coding: utf-8 -*-
from base64 import b64encode
from typing import TYPE_CHECKING, Optional, Tuple

import grpc
from grpc import AuthMetadataContext, AuthMetadataPluginCallback, RpcError, StatusCode
from grpc.aio import AioRpcError

from esdbclient.exceptions import (
    DeadlineExceeded,
    ESDBClientException,
    ExceptionThrownByHandler,
    GrpcError,
    NodeIsNotLeader,
    NotFound,
    ServiceUnavailable,
)

if TYPE_CHECKING:  # pragma: no cover
    from grpc import Metadata

else:
    Metadata = Tuple[Tuple[str, str], ...]

__all__ = ["Metadata"]


class BasicAuthCallCredentials(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        credentials = b64encode(f"{username}:{password}".encode())
        self._metadata = (("authorization", (b"Basic " + credentials)),)

    def __call__(
        self, context: AuthMetadataContext, callback: AuthMetadataPluginCallback
    ) -> None:
        callback(self._metadata, None)


def handle_rpc_error(e: RpcError) -> ESDBClientException:
    """
    Converts gRPC errors to client exceptions.
    """
    if isinstance(e, (grpc.Call, AioRpcError)):
        if e.code() == StatusCode.UNKNOWN and "Exception was thrown by handler" in str(
            e.details()
        ):
            return ExceptionThrownByHandler(e)
        elif e.code() == StatusCode.UNAVAILABLE:
            return ServiceUnavailable(e)
        elif e.code() == StatusCode.DEADLINE_EXCEEDED:
            return DeadlineExceeded(e)
        elif (
            e.code() == StatusCode.NOT_FOUND and e.details() == "Leader info available"
        ):
            return NodeIsNotLeader(e)
        elif e.code() == StatusCode.NOT_FOUND:
            return NotFound()
    return GrpcError(e)


class ESDBService:
    def _metadata(
        self, metadata: Optional[Metadata], requires_leader: bool = False
    ) -> Metadata:
        requires_leader_metadata: Metadata = (
            ("requires-leader", "true" if requires_leader else "false"),
        )
        metadata = tuple() if metadata is None else metadata
        return metadata + requires_leader_metadata
