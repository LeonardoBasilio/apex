from apex_engine_postgres.query import SELECT_STAGES_FOR_JOB, fetch_stages


class FakeFetcher:
    def __init__(self, rows):
        self._rows = rows
        self.calls = []

    def fetch_all(self, sql, params=None):
        self.calls.append((sql, params))
        return self._rows


def test_fetch_stages_maps_columns_to_stagerow_fields_in_order():
    fetcher = FakeFetcher(
        rows=[
            ("job-1", "app-1", 5, 0, 100, 50.0, 1035.0, 1040.0, 100,
             113_527_779, 0, 0, 0, 10, 0, 0),
        ]
    )

    stages = fetch_stages(fetcher, "job-1")

    assert len(stages) == 1
    stage = stages[0]
    assert stage.job_id == "job-1"
    assert stage.app_id == "app-1"
    assert stage.stage_id == 5
    assert stage.stage_attempt == 0
    assert stage.task_count == 100
    assert stage.task_duration_p50_ms == 50.0
    assert stage.task_duration_p99_ms == 1035.0
    assert stage.task_duration_max_ms == 1040.0
    assert stage.task_duration_sample_count == 100
    assert stage.shuffle_read_bytes == 113_527_779
    assert stage.gc_time_ms == 10


def test_fetch_stages_passes_the_job_id_as_a_named_parameter():
    fetcher = FakeFetcher(rows=[])
    fetch_stages(fetcher, "job-42")

    sql, params = fetcher.calls[0]
    assert sql is SELECT_STAGES_FOR_JOB
    assert params == {"job_id": "job-42"}


def test_fetch_stages_returns_empty_list_for_no_rows():
    assert fetch_stages(FakeFetcher(rows=[]), "job-missing") == []
