"""The span processor and what the framework tells it to do."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import os
import queue
import threading
from typing import TypeAlias, TypedDict

from opentelemetry.context import Context
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor
import requests

log = logging.getLogger("crucible.span")

# How long a held service waits before carrying on regardless, so a framework
# that has gone away cannot wedge the fleet it was testing.
HELD_TIMEOUT_S: float = 30.0

# Where the framework is, set on every service it brings up.
FRAMEWORK_ENV: str = "CRUCIBLE_SPAN"


class Marks(TypedDict):
    """The moments a holding run names."""

    at: list[str]


class Holding(TypedDict):
    """The framework naming the moments to hold at."""

    Holding: Marks


# What the framework says a run wants. A state with nothing to carry is its own
# name, one that names moments carries them.
Said: TypeAlias = str | Holding


class Boundary(TypedDict):
    """A span boundary, as the framework is told about it."""

    span: str
    side: str
    nth: int


class Released(TypedDict):
    """What the framework says to a service it was holding."""

    at_ns: int


@dataclass(frozen=True)
class Watching:
    """What an adapter loaded into a service does for a run. Report nothing,
    report every boundary, or hold at the moments a run named."""

    reporting: bool = False
    at: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def inert(cls) -> Watching:
        return cls()

    @classmethod
    def told(cls, said: Said) -> Watching:
        """What the framework said, read defensively, arrives as JSON."""
        if said == "Reporting":
            return cls(reporting=True)
        if isinstance(said, dict) and "Holding" in said:
            return cls(at=frozenset(said["Holding"].get("at", [])))
        return cls.inert()

    def holds(self, mark: str) -> bool:
        """Whether the moment `mark` names waits for the framework."""
        return mark in self.at


class Boundaries(SpanProcessor):
    """Reports the span boundaries a service reaches, and waits at the moment
    this run named."""

    def __init__(self, watching: Watching, framework: str) -> None:
        self._watching = watching
        self._url = f"{framework}/boundary"
        self._session = requests.Session()
        self._counts: dict[str, int] = {}
        self._nth: dict[int, int] = {}
        self._lock = threading.Lock()
        # Handed to a thread so the service is not slowed by reporting them.
        self._reports: queue.Queue[Boundary] | None = None
        if watching.reporting:
            self._reports = queue.Queue()
            threading.Thread(target=self._post_reported, daemon=True).start()

    @classmethod
    def joining(cls) -> Boundaries | None:
        """Join the run this service was brought up in, or `None` outside a
        run, so a service can install this unconditionally."""
        framework = os.environ.get(FRAMEWORK_ENV)
        return cls.joined(framework) if framework else None

    @classmethod
    def joined(cls, framework: str) -> Boundaries:
        """Ask the framework what this run wants, and report accordingly."""
        said = requests.get(f"{framework}/watching", timeout=HELD_TIMEOUT_S).json()
        return cls(Watching.told(said), framework)

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        """A span starting, which is before the work it covers runs."""
        context = span.get_span_context()
        if context is None:
            log.debug("a span with no context offers no moment span=%r", span.name)
            return
        nth = self._number(span.name, context.span_id)
        self._reached(span.name, nth, "Started")

    def on_end(self, span: ReadableSpan) -> None:
        """A span ending, which is after the work it covers has run."""
        context = span.get_span_context()
        if context is None:
            return
        nth = self._numbered(context.span_id)
        if nth is not None:
            self._reached(span.name, nth, "Ended")

    def shutdown(self) -> None:
        return None

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True

    def _reached(self, span: str, nth: int, side: str) -> None:
        """Say a boundary was reached, and wait there if this run named it."""
        boundary = Boundary(span=span, side=side, nth=nth)
        mark = f"{span}:{nth}:{'start' if side == 'Started' else 'end'}"
        if not self._watching.holds(mark):
            if self._reports is not None:
                self._reports.put(boundary)
            return
        # Blocking, on the thread that reached the boundary. The service is held
        # here until the framework answers.
        try:
            answer = self._session.post(
                self._url, json=boundary, timeout=HELD_TIMEOUT_S
            )
            released: Released = answer.json()
            log.debug(
                "the framework let the service go mark=%r at_ns=%s",
                mark,
                released["at_ns"],
            )
        except requests.RequestException as e:
            log.warning(
                "the framework could not be reached, so the service carries on "
                "mark=%r error=%s",
                mark,
                e,
            )

    def _post_reported(self) -> None:
        assert self._reports is not None
        while True:
            boundary = self._reports.get()
            try:
                self._session.post(self._url, json=boundary, timeout=HELD_TIMEOUT_S)
            except requests.RequestException as e:
                log.warning(
                    "a moment could not be reported, so the run will not know "
                    "of it span=%r nth=%s error=%s",
                    boundary["span"],
                    boundary["nth"],
                    e,
                )

    def _number(self, span: str, span_id: int) -> int:
        """The number this span is."""
        with self._lock:
            nth = self._counts.get(span, 0) + 1
            self._counts[span] = nth
            self._nth[span_id] = nth
            return nth

    def _numbered(self, span_id: int) -> int | None:
        """What number the span was given, if this reported its start."""
        with self._lock:
            return self._nth.pop(span_id, None)
