"""Crucible's Python adapter: an OpenTelemetry ``SpanProcessor`` that offers the
moments inside a service.

A service already spans the work it does. This reports the boundaries of those
spans to the framework and, at the moment a run named, waits until the framework
says the service may carry on. The wait blocks the thread that reached the
boundary: returning and reporting later would let the service run on.
"""

from .boundaries import (
    FRAMEWORK_ENV,
    Boundaries,
    Boundary,
    Holding,
    Marks,
    Released,
    Said,
    Watching,
)

__all__ = [
    "FRAMEWORK_ENV",
    "Boundaries",
    "Boundary",
    "Holding",
    "Marks",
    "Released",
    "Said",
    "Watching",
]
