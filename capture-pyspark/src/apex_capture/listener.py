"""Native PySpark SparkListener, bridged through py4j instead of a compiled Scala
jar (see README.md). `org.apache.spark.scheduler.SparkListenerInterface` has ~15
callback methods across Spark 3.5/4.x, and Spark adds new ones across versions —
rather than stub every one by name, CaptureListener implements the interface
dynamically: py4j resolves each Java callback by attribute name at call time, so
`__getattr__` can hand back one generic forwarder for every `on*` method, version
differences included.
"""

from __future__ import annotations

import time
from typing import Callable

from .queue_worker import NonBlockingEventQueue

SparkListenerInterface = "org.apache.spark.scheduler.SparkListenerInterface"


class CaptureListener:
    """py4j-side implementation of SparkListenerInterface. Every `on*` callback is
    forwarded to `queue.submit()` — non-blocking by construction, since that's all
    this class is allowed to do on the listener bus thread."""

    class Java:
        implements = [SparkListenerInterface]

    def __init__(self, event_queue: NonBlockingEventQueue) -> None:
        self._queue = event_queue

    def __getattr__(self, name: str):
        if not name.startswith("on"):
            raise AttributeError(name)

        def _handler(java_event=None, _name: str = name) -> None:
            self._queue.submit(
                {"type": _name, "received_at": time.time(), "event": java_event}
            )

        return _handler

    # py4j's Java-side proxy calls these directly; __getattr__ only fires for
    # missing attributes, so these three must be real methods, not "on*" names.
    def equals(self, other) -> bool:
        return self is other

    def hashCode(self) -> int:
        return id(self)

    def toString(self) -> str:
        return "ApexCaptureListener"


def attach(
    spark_context,
    sink: Callable[[dict], None],
    maxsize: int = 10_000,
) -> NonBlockingEventQueue:
    """Registers the listener on a running SparkContext and returns the queue, so
    the caller can read `.stats()` for drop/error counts and `.stop()` at job end.

    `sink` runs on the background worker thread (see queue_worker.py) — in this
    port it's the stream-kafka/ producer's `send`, injected here so this module
    has no Kafka dependency of its own.
    """
    jvm_gateway = spark_context._gateway
    if jvm_gateway._callback_server is None:
        jvm_gateway.start_callback_server()

    event_queue = NonBlockingEventQueue(sink=sink, maxsize=maxsize)
    event_queue.start()
    listener = CaptureListener(event_queue)
    spark_context._jsc.sc().addSparkListener(listener)
    return event_queue
