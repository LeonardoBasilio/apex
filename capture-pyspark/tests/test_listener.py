import time

import pytest

from apex_capture.listener import CaptureListener
from apex_capture.queue_worker import NonBlockingEventQueue
from fakes import Attr, fake_stage_completed, fake_task_end


def _idle_queue() -> NonBlockingEventQueue:
    """A queue nothing ever drains — these tests only check what gets
    submitted, not delivery, so the worker thread is unnecessary."""
    return NonBlockingEventQueue(sink=lambda _e: None, maxsize=10)


def _listener() -> CaptureListener:
    return CaptureListener(_idle_queue(), job_id="job-1", app_id="app-1")


def test_unrecognized_on_methods_still_forward_generically():
    listener = _listener()
    listener.onSomeFutureSparkVersionEvent(object())
    listener.onApplicationStart(object())

    assert listener._queue.stats().received == 2


def test_on_task_end_never_touches_the_queue():
    listener = _listener()
    for i in range(50):
        listener.onTaskEnd(fake_task_end(stage_id=1, stage_attempt=0, duration_ms=float(i)))

    assert listener._queue.stats().received == 0
    assert listener._aggregator.pending_stage_count() == 1


def test_on_stage_completed_enqueues_one_shaped_contract_event():
    listener = _listener()
    listener.onTaskEnd(fake_task_end(stage_id=1, stage_attempt=0, duration_ms=10.0))
    listener.onTaskEnd(fake_task_end(stage_id=1, stage_attempt=0, duration_ms=20.0))
    listener.onTaskEnd(fake_task_end(stage_id=1, stage_attempt=0, duration_ms=30.0))

    listener.onStageCompleted(fake_stage_completed(stage_id=1, stage_attempt=0, num_tasks=3))

    assert listener._queue.stats().received == 1
    item = listener._queue._queue.get_nowait()
    assert item["type"] == "stage_completed"
    payload = item["event"]
    assert payload["job_id"] == "job-1"
    assert payload["app_id"] == "app-1"
    assert payload["stage_id"] == 1
    assert payload["task_duration_sample_count"] == 3
    assert payload["task_duration_max_ms"] == 30.0


def test_on_stage_completed_clears_the_aggregator_for_that_stage():
    listener = _listener()
    listener.onTaskEnd(fake_task_end(stage_id=2, stage_attempt=0, duration_ms=5.0))
    listener.onStageCompleted(fake_stage_completed(stage_id=2, stage_attempt=0, num_tasks=1))

    assert listener._aggregator.pending_stage_count() == 0


def test_on_stage_completed_with_no_prior_task_end_still_enqueues_zero_stats():
    """A stage can complete with zero observed tasks (e.g. all tasks were
    skipped from a cached/reused shuffle) — this must not crash or be
    silently dropped."""
    listener = _listener()
    listener.onStageCompleted(fake_stage_completed(stage_id=9, stage_attempt=0, num_tasks=0))

    assert listener._queue.stats().received == 1
    payload = listener._queue._queue.get_nowait()["event"]
    assert payload["task_duration_sample_count"] == 0


def test_on_other_event_recognizes_an_aqe_update_and_enqueues_a_transition():
    class FakeAdaptiveUpdate:
        def executionId(self):
            return 0

        def physicalPlanDescription(self):
            return "+- SortMergeJoin Inner (13)\n"

    listener = _listener()
    listener.onOtherEvent(FakeAdaptiveUpdate())

    assert listener._queue.stats().received == 1
    item = listener._queue._queue.get_nowait()
    assert item["type"] == "plan_transition"
    assert item["event"]["execution_id"] == 0
    assert item["event"]["job_id"] == "job-1"


def test_on_other_event_forwards_non_aqe_events_generically():
    listener = _listener()
    listener.onOtherEvent(object())  # no executionId()/physicalPlanDescription()

    assert listener._queue.stats().received == 1
    item = listener._queue._queue.get_nowait()
    assert item["type"] == "onOtherEvent"


def test_non_on_attribute_raises_attribute_error():
    listener = _listener()
    with pytest.raises(AttributeError):
        listener.notAListenerCallback()


def test_java_proxy_identity_methods_are_not_treated_as_callbacks():
    listener = _listener()
    other = _listener()

    assert listener.equals(listener) is True
    assert listener.equals(other) is False
    assert listener.hashCode() == id(listener)
    assert listener.toString() == "ApexCaptureListener"
    assert listener._queue.stats().received == 0


def test_task_end_callback_does_not_block_regardless_of_volume():
    listener = _listener()

    start = time.monotonic()
    for i in range(1000):
        listener.onTaskEnd(fake_task_end(stage_id=1, stage_attempt=0, duration_ms=float(i)))
    elapsed = time.monotonic() - start

    assert elapsed < 1.0
