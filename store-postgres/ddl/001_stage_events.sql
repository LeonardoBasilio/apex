-- stage_events: one row per completed Spark stage per run. Replaces
-- upstream's bronze-equivalent ClickHouse table (CONTRACT.md's
-- `apex.spark_events`), scoped to the v1 field subset capture-pyspark
-- actually emits today -- see capture-pyspark/README.md and
-- stream-kafka/README.md for what's deferred (plan fingerprint, AQE
-- plan-transition fields, the v0.5/v0.6 retry-safe task fields).
--
-- (job_id, stage_id, stage_attempt) is the natural idempotency key: a given
-- stage attempt of a given run completes exactly once, and Kafka's
-- at-least-once delivery means this consumer may see the same message more
-- than once after a restart before an offset commit -- see consumer.py's
-- ON CONFLICT DO NOTHING.

CREATE TABLE IF NOT EXISTS stage_events (
    job_id                       TEXT    NOT NULL,
    app_id                       TEXT    NOT NULL,
    stage_id                     INTEGER NOT NULL,
    stage_attempt                INTEGER NOT NULL,
    ts                           BIGINT  NOT NULL,  -- epoch millis, matches CONTRACT.md's `ts`

    task_count                   INTEGER,
    task_duration_p50_ms         DOUBLE PRECISION,
    task_duration_p99_ms         DOUBLE PRECISION,
    task_duration_max_ms         DOUBLE PRECISION,
    task_duration_sample_count   INTEGER,

    shuffle_read_bytes           BIGINT,
    shuffle_write_bytes          BIGINT,
    spill_disk_bytes             BIGINT,
    spill_mem_bytes              BIGINT,
    gc_time_ms                   BIGINT,
    input_bytes                  BIGINT,
    output_bytes                 BIGINT,

    inserted_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (job_id, stage_id, stage_attempt)
);

-- engine/'s detectors filter by job_id first ("the stages for this run");
-- everything else is a small per-job scan once that index narrows it.
CREATE INDEX IF NOT EXISTS idx_stage_events_job_id ON stage_events (job_id);
