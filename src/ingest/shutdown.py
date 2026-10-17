"""Graceful shutdown: turn SIGINT/SIGTERM into a "please drain" request (ADR-0026).

First signal: `StopSignal` is set, so runners stop taking new work, finish what's in flight, flush,
and record the run as 'cancelled'. Second signal: the main task is cancelled (hard stop).
Explainer: docs/explain/ingest/shutdown.md
"""

import asyncio
import signal
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import structlog

log = structlog.get_logger()

SIGNALS = (signal.SIGINT, signal.SIGTERM)


class StopSignal:
    """An asyncio.Event plus callbacks, so a runner can react (e.g. arm a drain deadline)."""

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._callbacks: list[Callable[[], None]] = []
        self.reason: str | None = None

    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()

    def on_stop(self, callback: Callable[[], None]) -> None:
        """Run `callback` when stop is requested (immediately if it already was)."""
        if self._event.is_set():
            callback()
        else:
            self._callbacks.append(callback)

    def request(self, reason: str) -> None:
        if self._event.is_set():
            return
        self.reason = reason
        self._event.set()
        for callback in self._callbacks:
            callback()


@contextmanager
def handle_signals(stop: StopSignal, main_task: asyncio.Task[object]) -> Iterator[None]:
    """Install SIGINT/SIGTERM handlers for the duration of a run, then restore the defaults.

    loop.add_signal_handler replaces the handler asyncio.run installed (which would cancel the
    main task on the FIRST Ctrl-C, i.e. no drain). Handlers run as ordinary loop callbacks, so they
    may safely touch asyncio objects.
    """
    loop = asyncio.get_running_loop()

    def on_signal(sig: signal.Signals) -> None:
        if not stop.is_set():
            log.warning("shutdown_requested", signal=sig.name, mode="drain")
            stop.request(sig.name)
        else:
            log.warning("shutdown_forced", signal=sig.name, mode="cancel")
            main_task.cancel()

    for sig in SIGNALS:
        loop.add_signal_handler(sig, on_signal, sig)
    try:
        yield
    finally:
        for sig in SIGNALS:
            loop.remove_signal_handler(sig)
