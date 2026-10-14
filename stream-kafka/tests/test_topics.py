from apex_stream.topics import TOPIC_OTHER_EVENTS, TOPIC_STAGE_EVENTS, topic_for


def test_stage_completed_routes_to_stage_events_topic():
    event = {"type": "stage_completed", "event": {}}
    assert topic_for(event) == TOPIC_STAGE_EVENTS


def test_everything_else_routes_to_other_events_topic():
    for event_type in ["onJobStart", "onApplicationEnd", "onOtherEvent", "unknown_future_type"]:
        assert topic_for({"type": event_type, "event": {}}) == TOPIC_OTHER_EVENTS


def test_missing_type_key_routes_to_other_events_topic():
    assert topic_for({"event": {}}) == TOPIC_OTHER_EVENTS
