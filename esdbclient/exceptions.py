# -*- coding: utf-8 -*-
class EsdbClientException(Exception):
    """
    Base class for exceptions raised by the client.
    """


class GrpcError(EsdbClientException):
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


class StreamNotFound(EsdbClientException):
    """
    Raised when EventStoreDB stream is not found.
    """


class ExpectedPositionError(EsdbClientException):
    """
    Raised when expected position does not match the
    actual position of the last event in a stream.
    """
