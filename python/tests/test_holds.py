"""The adapter against a framework that answers over HTTP.
"""

from __future__ import annotations

from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from socket import socket
from socketserver import BaseServer
import threading
import time

from opentelemetry.sdk.trace import TracerProvider
import pytest

from crucible_span_py import (
    FRAMEWORK_ENV,
    Boundaries,
    Boundary,
    Released,
    Said,
    Watching,
)


class Framework:
    """A stand-in for the framework. Says what a run wants, and holds a service
    at a boundary until it is let go."""

    def __init__(self, watching: Said) -> None:
        self.watching = watching
        self.seen: list[str] = []
        self.holding = threading.Event()
        self.release = threading.Event()
        self.server = HTTPServer(("127.0.0.1", 0), partial(Handler, self))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def held_marks(self) -> set[str]:
        if isinstance(self.watching, dict) and "Holding" in self.watching:
            return set(self.watching["Holding"]["at"])
        return set()

    @property
    def url(self) -> str:
        host, port = self.server.socket.getsockname()[:2]
        return f"http://{host}:{port}"


class Handler(BaseHTTPRequestHandler):
    """One request to the framework."""

    def __init__(
        self,
        framework: Framework,
        request: socket | tuple[bytes, socket],
        client_address: tuple[str, int],
        server: BaseServer,
    ) -> None:
        self.framework = framework
        # This serves the request from its own constructor, so anything the
        # request needs has to be initialised first.
        super().__init__(request, client_address, server)

    def log_message(self, format: str, *args: object) -> None:
        """Nothing, a test says what happened through its assertions."""

    def do_GET(self) -> None:
        self._say(self.framework.watching)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        boundary: Boundary = json.loads(self.rfile.read(length))
        side = "start" if boundary["side"] == "Started" else "end"
        mark = f"{boundary['span']}:{boundary['nth']}:{side}"
        self.framework.seen.append(mark)
        if mark in self.framework.held_marks():
            self.framework.holding.set()
            self.framework.release.wait(timeout=10)
        self._say(Released(at_ns=time.time_ns()))

    def _say(self, body: Said | Released) -> None:
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


@pytest.fixture
def tracer_provider() -> TracerProvider:
    return TracerProvider()


def test_a_named_span_stops_the_service_until_it_is_let_go(
    tracer_provider: TracerProvider,
) -> None:
    """The span body standing still is the fault window: whatever the framework
    does while the service waits, it does to a service that has not moved on."""
    framework = Framework({"Holding": {"at": ["publish:1:start"]}})
    tracer_provider.add_span_processor(Boundaries.joined(framework.url))
    tracer = tracer_provider.get_tracer("test")

    ran: list[str] = []

    def service() -> None:
        with tracer.start_as_current_span("handle"):
            pass
        ran.append("handle done")
        with tracer.start_as_current_span("publish"):
            ran.append("publish body ran")

    thread = threading.Thread(target=service)
    thread.start()

    assert framework.holding.wait(timeout=5), "the service never reached the moment"
    assert ran == ["handle done"], "the span body ran while it was supposed to be held"

    framework.release.set()
    thread.join(timeout=5)
    assert ran == ["handle done", "publish body ran"]
    assert "publish:1:start" in framework.seen


def test_a_reporting_run_lets_the_service_run_through(
    tracer_provider: TracerProvider,
) -> None:
    """The moments a learn run finds must be the moments an unheld run would reach."""
    framework = Framework("Reporting")
    tracer_provider.add_span_processor(Boundaries.joined(framework.url))
    tracer = tracer_provider.get_tracer("test")

    with tracer.start_as_current_span("publish"):
        pass
    with tracer.start_as_current_span("publish"):
        pass

    for _ in range(50):
        if len(framework.seen) >= 4:
            break
        time.sleep(0.02)

    assert sorted(framework.seen) == [
        "publish:1:end",
        "publish:1:start",
        "publish:2:end",
        "publish:2:start",
    ]


@pytest.mark.parametrize(
    ("said", "reporting", "holds"),
    [
        ("Inert", False, False),
        ("Reporting", True, False),
        ({"Holding": {"at": ["publish:1:start"]}}, False, True),
    ],
)
def test_watching_told(said: Said, reporting: bool, holds: bool) -> None:
    watching = Watching.told(said)

    assert watching.reporting is reporting
    assert watching.holds("publish:1:start") is holds


def test_a_service_outside_a_run_offers_no_moments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Installing the adapter in production has to cost nothing."""
    monkeypatch.delenv(FRAMEWORK_ENV, raising=False)
    assert Boundaries.joining() is None


def test_a_service_brought_up_in_a_run_joins_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A service is instrumented without being told anything about the run."""
    framework = Framework("Reporting")
    monkeypatch.setenv(FRAMEWORK_ENV, framework.url)
    boundaries = Boundaries.joining()
    assert boundaries is not None
    assert boundaries._watching.reporting
