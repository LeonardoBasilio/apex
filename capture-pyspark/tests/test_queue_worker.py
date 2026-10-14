import threading
import time

import pytest

from apex_capture.queue_worker import NonBlockingEventQueue


def test_submit_never_blocks_even_when_sink_is_slow():
    release = threading.Event()

    def slow_sink(_event):
        release.wait(timeout=2.0)

    q = NonBlockingEventQueue(sink=slow_sink, maxsize=2)
    q.start()

    start = time.monotonic()
    for i in range(50):
        q.submit({"n": i})
    elapsed = time.monotonic() - start

    assert elapsed < 1.0, "submit() must never block the caller"

    release.set()
    q.stop()


def test_drops_oldest_under_backpressure_and_counts_it():
    release = threading.Event()

    def blocked_sink(_event):
        release.wait(timeout=2.0)

    q = NonBlockingEventQueue(sink=blocked_sink, maxsize=1)
    q.start()

    # First event occupies the sole queue slot while the worker blocks on it.
    q.submit({"n": 0})
    time.sleep(0.05)  # let the worker pick it up and start blocking in the sink

    q.submit({"n": 1})
    q.submit({"n": 2})

    stats = q.stats()
    assert stats.received == 3
    assert stats.dropped >= 1

    release.set()
    q.stop()


def test_stats_counts_sink_errors_without_raising():
    def raising_sink(_event):
        raise RuntimeError("boom")

    q = NonBlockingEventQueue(sink=raising_sink, maxsize=10)
    q.start()
    q.submit({"n": 0})

    deadline = time.monotonic() + 2.0
    while q.stats().sink_errors == 0 and time.monotonic() < deadline:
        time.sleep(0.01)

    q.stop()
    assert q.stats().sink_errors == 1
    assert q.stats().sent == 0


def test_all_submitted_events_are_sent_when_sink_keeps_up():
    received = []
    lock = threading.Lock()

    def fast_sink(event):
        with lock:
            received.append(event)

    q = NonBlockingEventQueue(sink=fast_sink, maxsize=100)
    q.start()
    for i in range(20):
        q.submit({"n": i})

    deadline = time.monotonic() + 2.0
    while len(received) < 20 and time.monotonic() < deadline:
        time.sleep(0.01)

    q.stop()
    stats = q.stats()
    assert stats.received == 20
    assert stats.sent == 20
    assert stats.dropped == 0
    assert [e["n"] for e in received] == list(range(20))


@pytest.mark.parametrize("maxsize", [1, 4, 16])
def test_stop_drains_whatever_is_already_queued(maxsize):
    received = []

    def fast_sink(event):
        received.append(event)

    q = NonBlockingEventQueue(sink=fast_sink, maxsize=maxsize)
    q.start()
    q.submit({"n": 1})
    q.stop(timeout=2.0)

    assert received == [{"n": 1}]
