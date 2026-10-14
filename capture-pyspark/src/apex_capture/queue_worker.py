"""Non-blocking handoff between the Spark listener bus thread and the real sink
(Kafka producer, in this port). See capture-pyspark/README.md for why this exists:
a listener callback that blocks stalls Spark's whole listener bus, including Spark's
own UI/history-server listeners on the same bus.

`submit()` never blocks the caller. Under backpressure it drops the OLDEST queued
event to make room for the newest one, and always counts the drop — the house rule
this whole repo inherited from upstream is "no silent green"; applied here, that
means no silent data loss.
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass


@dataclass
class QueueStats:
    received: int = 0
    dropped: int = 0
    sent: int = 0
    sink_errors: int = 0


class NonBlockingEventQueue:
    def __init__(self, sink, maxsize: int = 10_000) -> None:
        """`sink(event)` is called from the background worker thread, never from the
        caller of `submit()`. A raising sink is counted, not raised."""
        self._queue: queue.Queue = queue.Queue(maxsize=maxsize)
        self._sink = sink
        self._stats = QueueStats()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._worker = threading.Thread(
            target=self._run, name="apex-capture-worker", daemon=True
        )

    def start(self) -> None:
        self._worker.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._worker.join(timeout=timeout)

    def submit(self, event) -> None:
        with self._lock:
            self._stats.received += 1
        try:
            self._queue.put_nowait(event)
            return
        except queue.Full:
            pass

        # Drop the oldest queued event, then retry once. A second Full means
        # another producer refilled the slot between get_nowait and put_nowait;
        # count the drop and move on rather than spin or block.
        try:
            self._queue.get_nowait()
            with self._lock:
                self._stats.dropped += 1
        except queue.Empty:
            pass

        try:
            self._queue.put_nowait(event)
        except queue.Full:
            with self._lock:
                self._stats.dropped += 1

    def stats(self) -> QueueStats:
        with self._lock:
            return QueueStats(
                received=self._stats.received,
                dropped=self._stats.dropped,
                sent=self._stats.sent,
                sink_errors=self._stats.sink_errors,
            )

    def _run(self) -> None:
        while not (self._stop.is_set() and self._queue.empty()):
            try:
                event = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                self._sink(event)
                with self._lock:
                    self._stats.sent += 1
            except Exception:
                with self._lock:
                    self._stats.sink_errors += 1
