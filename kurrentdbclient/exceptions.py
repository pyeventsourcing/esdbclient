from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from uuid import UUID


class KurrentDBClientError(Exception):
    """
    Base class for exceptions raised by the client.
    """


class ProgrammingError(Exception):
    """
    Raised when programming errors are encountered.
    """


class GrpcError(KurrentDBClientError):
    """
    Base class for exceptions raised by gRPC.
    """


class ExceptionThrownByHandlerError(GrpcError):
    """
    Raised when gRPC service returns RpcError with status
    code "UNKNOWN" and details "Exception was thrown by handler.".
    """


class ServiceUnavailableError(GrpcError):
    """
    Raised when gRPC service is unavailable.
    """


class SSLError(ServiceUnavailableError):
    """
    Raised when gRPC service is unavailable due to SSL error.
    """


class DeadlineExceededError(KurrentDBClientError):
    """
    Base class for exceptions involving deadlines being exceeded.
    """


class GrpcDeadlineExceededError(GrpcError, DeadlineExceededError):
    """
    Raised when gRPC operation times out.
    """


class CancelledByClientError(KurrentDBClientError):
    """
    Raised when gRPC operation is cancelled.
    """


class AbortedByServerError(KurrentDBClientError):
    """
    Raised when gRPC operation is aborted.
    """


class ConsumerTooSlowError(AbortedByServerError):
    """
    Raised when buffer is overloaded.
    """


class NotFoundError(KurrentDBClientError):
    """
    Raised when stream or subscription or projection is not found.
    """


class AlreadyExistsError(KurrentDBClientError):
    """
    Raised when creating something, e.g. a persistent subscription, that already exists.
    """


class SubscriptionConfirmationError(KurrentDBClientError):
    """
    Raised when subscription confirmation fails.
    """


class WrongCurrentVersionError(KurrentDBClientError):
    """
    Raised when expected position does not match the
    stream position of the last event in a stream.
    """

    def __init__(
        self,
        *args: Any,
        stream_name: str | None = None,
        current_version: int | None = None,
        expected_version: int | None = None,
    ) -> None:
        self.stream_name = stream_name
        self.current_version = current_version
        self.expected_version = expected_version
        super().__init__(*args)


class AccessDeniedError(KurrentDBClientError):
    """
    Raised when access is denied by the server.
    """


class StreamIsDeletedError(KurrentDBClientError):
    """
    Raised when reading from or appending to a stream that has been
    tombstoned, and when deleting a stream that has been deleted
    whilst expecting the stream exists, and when getting or setting
    metadata for a stream that has been tombstoned, and when deleting
    a stream that has been tombstoned, and when tombstoning a stream
    that has been tombstoned.
    """


class AppendDeadlineExceededError(DeadlineExceededError):
    """
    Raised when append operation is timed out by the server.
    """


class UnknownError(KurrentDBClientError):
    """
    Raised when append operation fails with an "unknown" error.
    """


class InvalidTransactionError(KurrentDBClientError):
    """
    Raised when append operation fails with an "invalid transaction" error.
    """


class OperationFailedError(GrpcError):
    """
    Raised when an operation fails (e.g. deleting a projection that isn't disabled).
    """


class MaximumAppendSizeExceededError(KurrentDBClientError):
    """
    Raised when append operation fails with a "maximum append size exceeded" error.
    """


class BadRequestError(KurrentDBClientError):
    """
    Raised when append operation fails with a "bad request" error.
    """


class InvalidArgumentError(KurrentDBClientError):
    """
    Raised when append operation fails with an "invalid argument" error.
    """


class RecordMaxSizeExceededError(InvalidArgumentError):
    """
    Raised when appending an event that is larger than
    the maximum allowed event record size.
    """

    def __init__(
        self,
        *args: Any,
        stream_name: str,
        event_id: UUID,
        size: int,
        max_size: int,
    ):
        super().__init__(*args)
        self.stream_name = stream_name
        self.event_id = event_id
        self.size = size
        self.max_size = max_size


class TransactionMaxSizeExceededError(AbortedByServerError):
    """
    Raised when appending events that together are larger
    than the maximum allowed transaction size.
    """

    def __init__(
        self,
        *args: Any,
        size: int,
        max_size: int,
    ):
        super().__init__(*args)
        self.size = size
        self.max_size = max_size


class MultiAppendToSameStreamError(AbortedByServerError):
    """
    Raised when appending more than one sequence of events to the same stream.
    """

    def __init__(
        self,
        *args: Any,
        stream_name: str,
    ):
        super().__init__(*args)
        self.stream_name = stream_name


class DiscoveryFailedError(KurrentDBClientError):
    """
    Raised when client fails to satisfy node preference using gossip cluster info.
    """


class LeaderNotFoundError(DiscoveryFailedError):
    """
    Raised when NodePreference is 'follower' but the cluster has no such nodes.
    """


class FollowerNotFoundError(DiscoveryFailedError):
    """
    Raised when NodePreference is 'follower' but the cluster has no such nodes.
    """


class ReadOnlyReplicaNotFoundError(DiscoveryFailedError):
    """
    Raised when NodePreference is 'readonlyreplica' but the cluster has no such nodes.
    """


class ExceptionIteratingRequestsError(KurrentDBClientError):
    """
    Raised when a persistent subscription errors whilst iterating requests.

    This helps debugging because otherwise we just get a gRPC error
    that says "Exception iterating requests!"
    """


class FailedPreconditionError(KurrentDBClientError):
    """
    Raised when a "failed precondition" status error is encountered.
    """


class NodeIsNotLeaderError(FailedPreconditionError):
    """
    Raised when client attempts to write to a node that is not a leader.
    """

    def __init__(
        self,
        *args: Any,
        host: str | None = None,
        port: int | None = None,
        node_id: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._node_id = node_id
        super().__init__(*args)

    @property
    def host(self) -> str | None:
        return self._host

    @property
    def port(self) -> int | None:
        return self._port

    @property
    def node_id(self) -> str | None:
        return self._node_id


class StreamTombstonedError(FailedPreconditionError):
    """
    Raised when client attempts to write to a stream that has been tombstoned.
    """

    def __init__(self, *args: Any, stream_name: str) -> None:
        self._stream_name = stream_name
        super().__init__(*args)

    @property
    def stream_name(self) -> str | None:
        return self._stream_name


class UnauthenticatedError(KurrentDBClientError):
    """
    Raised when an "unauthenticated" status error is encountered.
    """


class MaximumSubscriptionsReachedError(FailedPreconditionError):
    """
    Raised when trying to read from a persistent subscription that
    is already being read by the maximum number of subscribers.
    """


class InternalError(GrpcError):
    """
    Raised when a grpc INTERNAL error is encountered.
    """
