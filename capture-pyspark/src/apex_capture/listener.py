"""Native PySpark SparkListener, bridged through py4j instead of a compiled
Scala jar (see README.md). `org.apache.spark.scheduler.SparkListenerInterface`
has ~15 callback methods across Spark 3.5/4.x, and Spark adds new ones across
versions — rather than stub every one by name, CaptureListener implements the
interface dynamically: py4j resolves each Java callback by attribute name at
call time, so `__getattr__` can hand back one generic forwarder for any `on*`
method it doesn't know about, version differences included.

`onTaskEnd` and `onStageCompleted` are the two exceptions: Python resolves a
real method before ever falling back to `__getattr__`, so defining them here
overrides the generic forwarder for exactly these two, which is where the
per-stage percentile aggregation in aggregator.py plugs in.
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from .aggregator import ZERO_STATS, StageAggregator
from .extract import build_stage_contract_event, extract_stage_key, extract_task_duration_ms
from .queue_worker import NonBlockingEventQueue

SparkListenerInterface = "org.apache.spark.scheduler.SparkListenerInterface"


class CaptureListener:
    """py4j-side implementation of SparkListenerInterface."""

    class Java:
        implements = [SparkListenerInterface]

    def __init__(
        self,
        event_queue: NonBlockingEventQueue,
        job_id: str,
        app_id: str,
        aggregator: Optional[StageAggregator] = None,
    ) -> None:
        self._queue = event_queue
        self._job_id = job_id
        self._app_id = app_id
        self._aggregator = aggregator if aggregator is not None else StageAggregator()

    def onTaskEnd(self, java_event) -> None:
        """Pure in-memory bookkeeping — this must NEVER touch the queue; see
        aggregator.py's docstring for why (O(1) work per task, no Kafka
        message per task)."""
        key = extract_stage_key(java_event)
        duration = extract_task_duration_ms(java_event)
        if key is not None and duration is not None:
            self._aggregator.on_task_end(key, duration)

    def onStageCompleted(self, java_event) -> None:
        key = extract_stage_key(java_event)
        stats = self._aggregator.pop_stage(key) if key is not None else ZERO_STATS
        contract_event = build_stage_contract_event(
            job_id=self._job_id,
            app_id=self._app_id,
            stage_completed_event=java_event,
            stats=stats,
            ts_millis=int(time.time() * 1000),
        )
        self._queue.submit(
            {"type": "stage_completed", "received_at": time.time(), "event": contract_event}
        )

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
    job_id: Optional[str] = None,
    maxsize: int = 10_000,
) -> NonBlockingEventQueue:
    """Registers the listener on a running SparkContext and returns the queue,
    so the caller can read `.stats()` for drop/error counts and `.stop()` at
    job end.

    `sink` runs on the background worker thread (queue_worker.py) — in this
    port it's stream-kafka/'s producer `send`, injected here so this module
    has no Kafka dependency of its own. `job_id` defaults to the Spark
    applicationId (see CONTRACT.md's "trace key"); pass it explicitly to use
    `spark.apex.job_id` instead.
    """
    jvm_gateway = spark_context._gateway
    if jvm_gateway._callback_server is None:
        jvm_gateway.start_callback_server()

    app_id = spark_context.applicationId
    resolved_job_id = job_id or app_id

    event_queue = NonBlockingEventQueue(sink=sink, maxsize=maxsize)
    event_queue.start()
    listener = CaptureListener(event_queue, job_id=resolved_job_id, app_id=app_id)
    spark_context._jsc.sc().addSparkListener(listener)
    return event_queue
