from apex_capture.aggregator import StageStats
from apex_capture.extract import (
    build_stage_contract_event,
    extract_stage_key,
    extract_task_duration_ms,
)
from fakes import Attr, fake_stage_completed, fake_task_end


def test_extract_task_duration_from_task_end_event():
    event = fake_task_end(stage_id=3, stage_attempt=0, duration_ms=250.0)
    assert extract_task_duration_ms(event) == 250.0


def test_extract_stage_key_from_task_end_event():
    event = fake_task_end(stage_id=3, stage_attempt=1, duration_ms=1.0)
    assert extract_stage_key(event) == (3, 1)


def test_extract_stage_key_from_stage_completed_event():
    event = fake_stage_completed(stage_id=5, stage_attempt=0, num_tasks=10)
    assert extract_stage_key(event) == (5, 0)


def test_extract_stage_key_returns_none_for_an_unrecognized_shape():
    assert extract_stage_key(Attr(somethingElse=1)) is None
    assert extract_stage_key(object()) is None


def test_extract_task_duration_returns_none_when_accessor_is_missing():
    assert extract_task_duration_ms(Attr()) is None
    assert extract_task_duration_ms(object()) is None


def test_build_stage_contract_event_maps_every_v1_field():
    event = fake_stage_completed(
        stage_id=5,
        stage_attempt=0,
        num_tasks=10,
        shuffle_read_bytes=1000,
        shuffle_write_bytes=2000,
        disk_spill_bytes=0,
        mem_spill_bytes=300,
        gc_time_ms=40,
        input_bytes=500,
        output_bytes=600,
    )
    stats = StageStats(p50_ms=12.5, p99_ms=99.9, max_ms=120.0, count=10)

    contract_event = build_stage_contract_event(
        job_id="ax151sasadds114",
        app_id="application_123",
        stage_completed_event=event,
        stats=stats,
        ts_millis=1_700_000_000_000,
    )

    assert contract_event == {
        "job_id": "ax151sasadds114",
        "app_id": "application_123",
        "stage_id": 5,
        "stage_attempt": 0,
        "ts": 1_700_000_000_000,
        "task_count": 10,
        "task_duration_p50_ms": 12.5,
        "task_duration_p99_ms": 99.9,
        "task_duration_max_ms": 120.0,
        "task_duration_sample_count": 10,
        "shuffle_read_bytes": 1000,
        "shuffle_write_bytes": 2000,
        "spill_disk_bytes": 0,
        "spill_mem_bytes": 300,
        "gc_time_ms": 40,
        "input_bytes": 500,
        "output_bytes": 600,
    }


def test_build_stage_contract_event_degrades_missing_fields_to_none_not_zero():
    """An event shape missing taskMetrics entirely (e.g. a version drift, or
    a pure-RDD stage with no SQL metrics) must not be silently read as 0 —
    0 is a real, meaningful value for a byte count."""
    bare_event = Attr(stageInfo=Attr(stageId=1, attemptNumber=0, numTasks=5))
    stats = StageStats(p50_ms=1.0, p99_ms=2.0, max_ms=3.0, count=1)

    contract_event = build_stage_contract_event(
        job_id="j",
        app_id="a",
        stage_completed_event=bare_event,
        stats=stats,
        ts_millis=0,
    )

    assert contract_event["shuffle_read_bytes"] is None
    assert contract_event["spill_disk_bytes"] is None
    assert contract_event["task_count"] == 5
