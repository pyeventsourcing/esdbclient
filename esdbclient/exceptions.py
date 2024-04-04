# -*- coding: utf-8 -*-
from typing import Dict, Optional, Union

import grpc


class EventStoreDBClientException(Exception):
    """
    Base class for exceptions raised by the client.
    """


class ProgrammingError(Exception):
    """
    Raised when programming errors are encountered.
    """


class GrpcError(EventStoreDBClientException):
    """
    Base class for exceptions raised by gRPC.
    """


class ExceptionThrownByHandler(GrpcError):
    """
    Raised when gRPC service returns RpcError with status
    code "UNKNOWN" and details "Exception was thrown by handler.".
    """


class ServiceUnavailable(GrpcError):
    """
    Raised when gRPC service is unavailable.
    """


class SSLError(GrpcError):
    """
    Raised when gRPC service is unavailable.
    """


class DeadlineExceeded(EventStoreDBClientException):
    """
    Base class for exceptions involving deadlines being exceeded.
    """


class GrpcDeadlineExceeded(GrpcError, DeadlineExceeded):
    """
    Raised when gRPC operation times out.
    """


class CancelledByClient(EventStoreDBClientException):
    """
    Raised when gRPC operation is cancelled.
    """


class AbortedByServer(GrpcError):
    """
    Raised when gRPC operation is aborted.
    """


class ConsumerTooSlow(AbortedByServer):
    """
    Raised when buffer is overloaded.
    """


class NodeIsNotLeader(EventStoreDBClientException):
    """
    Raised when client attempts to write to a node that is not a leader.
    """

    @property
    def leader_grpc_target(self) -> Optional[str]:
        if (
            self.args
            and isinstance(self.args[0], (grpc.Call, grpc.aio.AioRpcError))
            and self.args[0].code() == grpc.StatusCode.NOT_FOUND
            and self.args[0].details() == "Leader info available"
        ):
            # The typing of trailing_metadata is a mess.
            rpc_error = self.args[0]
            trailing_metadata: Dict[str, Union[str, bytes]]
            if isinstance(rpc_error, grpc.Call):
                trailing_metadata = {
                    m.key: m.value for m in rpc_error.trailing_metadata()  # type: ignore[attr-defined]
                }
            else:
                assert isinstance(rpc_error, grpc.aio.AioRpcError)
                trailing_metadata = rpc_error.trailing_metadata()  # type: ignore[assignment]

            host = trailing_metadata["leader-endpoint-host"]
            port = trailing_metadata["leader-endpoint-port"]
            if isinstance(host, bytes):
                host = host.decode("utf-8")  # pragma: no cover
            if isinstance(port, bytes):
                port = port.decode("utf-8")  # pragma: no cover
            return f"{host}:{port}"
        else:
            return None


class NotFound(EventStoreDBClientException):
    """
    Raised when stream or subscription is not found.
    """


class AlreadyExists(EventStoreDBClientException):
    """
    Raised when creating something, e.g. a persistent subscription, that already exists.
    """


class SubscriptionConfirmationError(EventStoreDBClientException):
    """
    Raised when subscription confirmation fails.
    """


class WrongCurrentVersion(EventStoreDBClientException):
    """
    Raised when expected position does not match the
    actual position of the last event in a stream.
    """


class AccessDeniedError(EventStoreDBClientException):
    """
    Raised when access is denied by the server.
    """


class StreamIsDeleted(EventStoreDBClientException):
    """
    Raised when reading from or appending to a stream that has been
    tombstoned, and when deleting a stream that has been deleted
    whilst expecting the stream exists, and when getting or setting
    metadata for a stream that has been tombstoned, and when deleting
    a stream that has been tombstoned, and when tombstoning a stream
    that has been tombstoned.
    """


class AppendDeadlineExceeded(DeadlineExceeded):
    """
    Raised when append operation is timed out by the server.
    """


class UnknownError(EventStoreDBClientException):
    """
    Raised when append operation fails with an "unknown" error.
    """


class InvalidTransactionError(EventStoreDBClientException):
    """
    Raised when append operation fails with an "invalid transaction" error.
    """


class MaximumAppendSizeExceededError(EventStoreDBClientException):
    """
    Raised when append operation fails with a "maximum append size exceeded" error.
    """


class BadRequestError(EventStoreDBClientException):
    """
    Raised when append operation fails with a "bad request" error.
    """


class DiscoveryFailed(EventStoreDBClientException):
    """
    Raised when client fails to satisfy node preference using gossip cluster info.
    """


class LeaderNotFound(DiscoveryFailed):
    """
    Raised when NodePreference is 'follower' but the cluster has no such nodes.
    """


class FollowerNotFound(DiscoveryFailed):
    """
    Raised when NodePreference is 'follower' but the cluster has no such nodes.
    """


class ReadOnlyReplicaNotFound(DiscoveryFailed):
    """
    Raised when NodePreference is 'readonlyreplica' but the cluster has no such nodes.
    """


class ExceptionIteratingRequests(EventStoreDBClientException):
    """
    Raised when a persistent subscription errors whilst iterating requests.

    This helps debugging because otherwise we just get a gRPC error
    that says "Exception iterating requests!"
    """


class FailedPrecondition(EventStoreDBClientException):
    """
    Raised when a "failed precondition" status error is encountered.
    """


class MaximumSubscriptionsReached(FailedPrecondition):
    """
    Raised when trying to read from a persistent subscription that
    is already being read by the maximum number of subscribers.
    """
