# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect
import re
import traceback
from contextlib import contextmanager
from typing import (
    Any,
    Callable,
    Coroutine,
    Iterator,
    MutableMapping,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    cast,
)

import wrapt
from opentelemetry.context import Context
from opentelemetry.trace import Span, SpanKind, Status, StatusCode, Tracer
from opentelemetry.util.types import AttributeValue
from typing_extensions import Concatenate, ParamSpec

from esdbclient.instrumentation.opentelemetry.attributes import Attributes

# Define type variables and type aliases for spanner functions.
T = TypeVar("T")
P = ParamSpec("P")
R = TypeVar("R")
BoundFunc = Callable[P, R]
SpannerResponse = Iterator[R]
SpannerFunc = Callable[Concatenate[Tracer, T, BoundFunc[P, R], P], SpannerResponse[R]]


# Define function type for wrapt-style wrapper functions.
T_contra = TypeVar("T_contra", contravariant=True)


class WraptWrapperFunc(Protocol[T_contra, P, R]):
    def __call__(
        self,
        original: BoundFunc[P, R],
        instance: T_contra,
        args: P.args,
        kwargs: P.kwargs,
    ) -> R:
        pass  # pragma: no cover


# Define type alias for unbound object class methods.
UnboundFunc = Callable[Concatenate[T, P], R]


# Define type-safe function to apply an instrumenting
# spanner function to the method of an object class.
def apply_spanner(
    patched_class: Type[T],
    spanned_func: UnboundFunc[T, P, R],
    spanner_func: SpannerFunc[T, P, R],
    tracer: Tracer,
) -> UnboundFunc[T, P, R]:

    if inspect.iscoroutinefunction(spanned_func):

        # Make a context manager for the spanner function.
        async_spanner_func = cast(
            SpannerFunc[T, P, Coroutine[Any, Any, R]], spanner_func
        )
        async_spanner = contextmanager(async_spanner_func)

        # Define wrapt-style instrumentation wrapper.
        async def async_instrumentation_wrapper(
            original: BoundFunc[P, Coroutine[Any, Any, R]],
            instance: T,
            args: P.kwargs,
            kwargs: P.args,
        ) -> R:
            # Use the spanner function to execute the spanned function.
            with async_spanner(tracer, instance, original, *args, **kwargs) as result:
                return await result

        # Replace spanned function with instrumentation wrapper.
        async_spanned_func = cast(
            UnboundFunc[T, P, Coroutine[Any, Any, R]], spanned_func
        )
        _patch_class(patched_class, async_spanned_func, async_instrumentation_wrapper)

    else:

        # Make a context manager for the spanner function.
        spanner = contextmanager(spanner_func)

        # Define wrapt-style instrumentation wrapper.
        def instrumentation_wrapper(
            original: BoundFunc[P, R],
            instance: T,
            args: P.args,
            kwargs: P.kwargs,
        ) -> R:
            # Use the spanner function to execute the spanned function.
            with spanner(tracer, instance, original, *args, **kwargs) as result:
                return result

        # Replace spanned function with instrumentation wrapper.
        _patch_class(patched_class, spanned_func, instrumentation_wrapper)

    # Satisfy the return type (which brings together the typing for mypy).
    return spanned_func


def _patch_class(
    patched_class: Type[T],
    spanned_func: UnboundFunc[T, P, R],
    instrumentation_wrapper_func: WraptWrapperFunc[T, P, R],
) -> UnboundFunc[T, P, R]:

    # Wrap spanned function with instrumentation wrapper function.
    wrapt.wrap_function_wrapper(
        patched_class,
        spanned_func.__name__,
        instrumentation_wrapper_func,
    )

    # Satisfy the return type (which brings together the typing for mypy).
    return spanned_func


# Define type variables for overloaded spanner functions.
AsyncSpannerResponse = SpannerResponse[Coroutine[Any, Any, R]]
S = TypeVar("S")
OverloadedSpannerResponse = Union[SpannerResponse[R], AsyncSpannerResponse[S]]


@contextmanager
def _start_span(
    tracer: Tracer,
    span_name: str,
    span_kind: SpanKind = SpanKind.CLIENT,
    context: Optional[Context] = None,
    end_on_exit: bool = True,
) -> Iterator[Span]:
    with tracer.start_as_current_span(
        name=span_name,
        kind=span_kind,
        record_exception=False,
        set_status_on_exception=False,
        context=context,
        end_on_exit=end_on_exit,
    ) as span:
        yield span


def _set_span_ok(span: Span) -> None:
    span.set_status(StatusCode.OK)


def _set_span_error(span: Span, error: Exception) -> None:
    # Set span status.
    exc_type = type(error)
    span.set_status(
        Status(
            status_code=StatusCode.ERROR,
            description=f"{exc_type.__name__}: {error}",
        )
    )
    # Log an event for the error.
    exception_type = (
        f"{exc_type.__module__}.{exc_type.__qualname__}"
        if exc_type.__module__ and exc_type.__module__ != "builtins"
        else exc_type.__qualname__
    )
    exception_message = str(error)
    stack = ["Traceback (most recent call last):\n"]
    # stack += traceback.format_stack()
    stack += traceback.format_exception(exc_type, error, error.__traceback__)[1:]
    exception_stacktrace = "".join([line for line in stack if _stack_include(line)])
    exception_escaped = str(True)
    # Gather attributes.
    attributes: MutableMapping[str, AttributeValue] = {
        Attributes.EXCEPTION_TYPE: exception_type,
        Attributes.EXCEPTION_MESSAGE: exception_message,
        Attributes.EXCEPTION_STACKTRACE: exception_stacktrace,
        Attributes.EXCEPTION_ESCAPED: exception_escaped,
    }
    span.add_event(name="exception", attributes=attributes, timestamp=None)


_stack_exclude_patterns = [
    "opentelemetry",
]
_stack_exclude_regex = re.compile(".*(" + "|".join(_stack_exclude_patterns) + ").*")


def _stack_include(line: str) -> bool:
    return _stack_exclude_regex.match(line) is None
