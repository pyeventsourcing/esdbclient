# -*- coding: utf-8 -*-
# Can't cancel streaming response when grpc is being instrumented.
# https://github.com/open-telemetry/opentelemetry-python-contrib/issues/2014
from __future__ import annotations

from collections import OrderedDict
from types import FunctionType
from typing import Any, Callable, Mapping, Sequence, Tuple

import grpc
import opentelemetry.trace as trace_api
from grpc._channel import _Rendezvous
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import inject
from opentelemetry.semconv.trace import SpanAttributes
from wrapt import wrap_function_wrapper

try:
    from opentelemetry.instrumentation.grpc._client import (
        OpenTelemetryClientInterceptor,
        _carrier_setter,
    )
    from opentelemetry.instrumentation.grpc._utilities import RpcInfo
    from opentelemetry.instrumentation.grpc.grpcext._interceptor import (
        _StreamClientInfo,
    )

except ImportError:  # pragma: no cover
    OpenTelemetryClientInterceptor = None  # type: ignore


def try_wrap_opentelemetry_intercept_grpc_server_stream() -> None:

    if OpenTelemetryClientInterceptor is not None:
        wrap_function_wrapper(
            OpenTelemetryClientInterceptor,
            "_intercept_server_stream",
            _wrap_grpc_intercept_server_stream(),
        )
    else:
        pass  # pragma: no cover


def try_unwrap_opentelemetry_intercept_grpc_server_stream() -> None:
    if OpenTelemetryClientInterceptor is not None:
        unwrap(OpenTelemetryClientInterceptor, "_intercept_server_stream")
    else:
        pass  # pragma: no cover


def _wrap_grpc_intercept_server_stream() -> Callable[..., InterceptServerStream]:
    def _intercept_server_stream(
        _: FunctionType,
        instance: OpenTelemetryClientInterceptor,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> InterceptServerStream:
        return _replacement_intercept_server_stream(instance, *args, **kwargs)

    return _intercept_server_stream


def _replacement_intercept_server_stream(
    self: OpenTelemetryClientInterceptor,
    request_or_iterator: Any,
    metadata: Tuple[Tuple[str, str], ...],
    client_info: _StreamClientInfo,
    invoker: Callable[..., _Rendezvous],
) -> InterceptServerStream:
    if not metadata:
        mutable_metadata = OrderedDict()  # pragma: no cover
    else:
        mutable_metadata = OrderedDict(metadata)

    with self._start_span(  # type: ignore[no-untyped-call]
        client_info.full_method,
        end_on_exit=False,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        inject(mutable_metadata, setter=_carrier_setter)
        metadata = tuple(mutable_metadata.items())
        rpc_info = RpcInfo(  # type: ignore[no-untyped-call]
            full_method=client_info.full_method,
            metadata=metadata,
            timeout=client_info.timeout,
        )

        if client_info.is_client_stream:
            rpc_info.request = request_or_iterator
        try:
            rendezvous = invoker(request_or_iterator, metadata)
        except grpc.RpcError as err:  # pragma: no cover
            span.set_status(
                trace_api.Status(
                    status_code=trace_api.StatusCode.ERROR,
                    description=f"{type(err).__name__}: {err}",
                )
            )
            err_code_value_int = err.code().value[0]  # type: ignore[index]
            span.set_attribute(SpanAttributes.RPC_GRPC_STATUS_CODE, err_code_value_int)
            span.record_exception(err)
            span.end()
            raise err
        except Exception as err:
            span.set_status(
                trace_api.Status(
                    status_code=trace_api.StatusCode.ERROR,
                    description=f"{type(err).__name__}: {err}",
                )
            )
            span.record_exception(err)
            span.end()
            raise err
        else:
            return InterceptServerStream(rendezvous, span)


class InterceptServerStream:
    def __init__(self, rendezvous: _Rendezvous, span: trace_api.Span) -> None:
        self._rendezvous = rendezvous
        self._span = span

    def __iter__(self) -> InterceptServerStream:
        return self

    def __next__(self) -> Any:
        try:
            return next(self._rendezvous)
        except StopIteration:
            self._span.end()
            raise
        except grpc.RpcError as err:
            err_code_value_int = err.code().value[0]  # type: ignore[index]
            self._span.set_attribute(
                SpanAttributes.RPC_GRPC_STATUS_CODE, err_code_value_int
            )
            self._span.set_status(
                trace_api.Status(
                    status_code=trace_api.StatusCode.ERROR,
                    description=f"{type(err).__name__}: {err}",
                )
            )
            self._span.record_exception(err)
            self._span.end()
            raise
        except Exception as err:  # pragma: no cover
            self._span.set_status(
                trace_api.Status(
                    status_code=trace_api.StatusCode.ERROR,
                    description=f"{type(err).__name__}: {err}",
                )
            )
            self._span.record_exception(err)
            self._span.end()
            raise

    def __del__(self) -> None:
        self.cancel()
        span = self._span
        if span.is_recording():
            span.end()

    def cancel(self) -> None:
        self._rendezvous.cancel()
