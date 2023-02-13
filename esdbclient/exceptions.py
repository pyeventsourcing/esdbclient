# -*- coding: utf-8 -*-
class ESDBClientException(Exception):
    """
    Base class for exceptions raised by the client.
    """


class GrpcError(ESDBClientException):
    """
    Base class for exceptions raised by gRPC.
    """


class ServiceUnavailable(GrpcError):
    """
    Raised when gRPC service is unavailable.
    """


class DeadlineExceeded(GrpcError):
    """
    Raised when gRPC operation times out.
    """


class StreamNotFound(ESDBClientException):
    """
    Raised when EventStoreDB stream is not found.
    """


class ExpectedPositionError(ESDBClientException):
    """
    Raised when expected position does not match the
    actual position of the last event in a stream.
    """
