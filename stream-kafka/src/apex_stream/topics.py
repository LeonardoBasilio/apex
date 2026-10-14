"""Topic routing. Only two topics exist in this port — see README.md's
revision note on why there is no per-task topic: capture-pyspark aggregates
task durations into p50/p99/max/count in-process (aggregator.py) before a
stage event is ever produced, so Kafka never sees one message per task.
"""

TOPIC_STAGE_EVENTS = "apex.spark.stage_events"
TOPIC_OTHER_EVENTS = "apex.spark.plan_events"


def topic_for(event: dict) -> str:
    """`event["type"] == "stage_completed"` is the one shape
    capture-pyspark/listener.py currently produces deliberately (via
    extract.py + aggregator.py). Everything else — AQE plan-transition
    events, job start/end, application lifecycle — is still forwarded
    generically by the listener's `__getattr__` fallback and lands here
    until each gets its own extraction pass (deferred, see README.md)."""
    if event.get("type") == "stage_completed":
        return TOPIC_STAGE_EVENTS
    return TOPIC_OTHER_EVENTS
