# -*- coding: utf-8 -*-
class EsdbClientException(Exception):
    pass


class GrpcError(EsdbClientException):
    pass


class ServiceUnavailable(GrpcError):
    pass


class DeadlineExceeded(GrpcError):
    pass


class StreamNotFound(EsdbClientException):
    pass


class ExpectedPositionError(EsdbClientException):
    pass
