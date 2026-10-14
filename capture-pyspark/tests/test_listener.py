import time

import pytest

from apex_capture.listener import CaptureListener
from apex_capture.queue_worker import NonBlockingEventQueue


def _idle_queue() -> NonBlockingEventQueue:
    """A queue nothing ever drains — these tests only check what gets submitted,
    not delivery, so the worker thread is unnecessary."""
    return NonBlockingEventQueue(sink=lambda _e: None, maxsize=10)


def test_any_on_method_forwards_to_the_queue():
    q = _idle_queue()
    listener = CaptureListener(q)

    # SparkListenerInterface has ~15 "on*" methods that vary across Spark 3.5/4.x;
    # CaptureListener must handle names it was never told about by name.
    listener.onStageCompleted(object())
    listener.onTaskEnd(object())
    listener.onSomeFutureSparkVersionEvent(object())

    assert q.stats().received == 3


def test_submitted_event_carries_type_and_payload():
    q = _idle_queue()
    listener = CaptureListener(q)
    sentinel_event = {"stageId": 7}

    listener.onStageCompleted(sentinel_event)

    item = q._queue.get_nowait()
    assert item["type"] == "onStageCompleted"
    assert item["event"] is sentinel_event
    assert isinstance(item["received_at"], float)


def test_non_on_attribute_raises_attribute_error():
    q = _idle_queue()
    listener = CaptureListener(q)

    with pytest.raises(AttributeError):
        listener.notAListenerCallback()


def test_java_proxy_identity_methods_are_not_treated_as_callbacks():
    q = _idle_queue()
    listener = CaptureListener(q)
    other = CaptureListener(q)

    assert listener.equals(listener) is True
    assert listener.equals(other) is False
    assert listener.hashCode() == id(listener)
    assert listener.toString() == "ApexCaptureListener"
    assert q.stats().received == 0


def test_callback_submit_does_not_block_regardless_of_queue_state():
    q = _idle_queue()
    listener = CaptureListener(q)

    start = time.monotonic()
    for i in range(100):
        listener.onTaskEnd({"n": i})
    elapsed = time.monotonic() - start

    assert elapsed < 1.0
    # maxsize=10, so most of these 100 were dropped — forwarding must still be
    # non-blocking under backpressure, which is the entire point of the bridge.
    assert q.stats().received == 100
