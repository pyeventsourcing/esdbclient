# -*- coding: utf-8 -*-
class ESDBClientException(Exception):
    """
    Base class for exceptions raised by the client.
    """


class GrpcError(ESDBClientException):
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


class DeadlineExceeded(GrpcError):
    """
    Raised when gRPC operation times out.
    """


class CancelledByClient(GrpcError):
    """
    Raised when gRPC operation is cancelled.
    """


class NodeIsNotLeader(ESDBClientException):
    """
    Raised when client attempts to write to a node that is not a leader.
    """


class NotFound(ESDBClientException):
    """
    Raised when stream or subscription is not found.
    """


class SubscriptionConfirmationError(ESDBClientException):
    """
    Raised when subscription confirmation fails.
    """


class WrongExpectedPosition(ESDBClientException):
    """
    Raised when expected position does not match the
    actual position of the last event in a stream.
    """


class AccessDeniedError(ESDBClientException):
    """
    Raised when access is denied by the server.
    """


class StreamIsDeleted(ESDBClientException):
    """
    Raised when reading from or appending to a stream that has been
    tombstoned, and when deleting a stream that has been deleted
    whilst expecting the stream exists, and when getting or setting
    metadata for a stream that has been tombstoned, and when deleting
    a stream that has been tombstoned, and when tombstoning a stream
    that has been tombstoned.
    """


class TimeoutError(ESDBClientException):
    """
    Raised when append operation is timed out by the server.
    """


class UnknownError(ESDBClientException):
    """
    Raised when append operation fails with an "unknown" error.
    """


class InvalidTransactionError(ESDBClientException):
    """
    Raised when append operation fails with an "invalid transaction" error.
    """


class MaximumAppendSizeExceededError(ESDBClientException):
    """
    Raised when append operation fails with a "maximum append size exceeded" error.
    """


class BadRequestError(ESDBClientException):
    """
    Raised when append operation fails with a "bad request" error.
    """


class DiscoveryFailed(ESDBClientException):
    """
    Raised when client fails to satisfy node preference using gossip cluster info.
    """


class GossipSeedError(DiscoveryFailed):
    """
    Raised when client has no gossip seeds.
    """


class DNSError(DiscoveryFailed):
    """
    Raised when client request to DNS fails.
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
