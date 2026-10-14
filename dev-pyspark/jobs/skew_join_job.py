"""A deliberately skewed join, run for real -- the pathology this port's
`dev-pyspark/` lane exists to generate. Replaces upstream `dev/`'s Delta/MinIO
lab with the minimum needed to produce a genuinely skewed stage through this
port's capture-pyspark listener: no S3/Delta dependency, just a hot key.

90% of a 2,000,000-row fact table shares one join key; the other 10% spread
across 9 keys. Joining against a 10-row dimension table and grouping by key
forces a shuffle where one partition carries ~1.8M rows and the rest carry
~22K each -- a real task-duration skew, not a crafted number.

`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` is lowered from
its 256 MiB default to 1 MiB for this job's conf. That's not fudging the
result: at this demo's intentionally small data volume (built to run in
seconds, not minutes), the skewed partition's byte size would never cross
the production default, and AQE's skew detection wouldn't fire at all. An
operator tuning this for a real workload's actual partition sizes is doing
the same thing for the same reason.
"""

from __future__ import annotations

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from apex_capture.listener import attach
from apex_stream.producer import from_bootstrap_servers


def main() -> None:
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")
    producer = from_bootstrap_servers(bootstrap_servers)

    spark = (
        SparkSession.builder.appName("apex-dev-pyspark-skew-join")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1048576")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
        # First run of this job (no overrides below) hit CONTRACT.md rule 7
        # for real, not as a thought experiment: with only two distinct keys,
        # AQE's own partition coalescing merged all 8 shuffle partitions down
        # to 1 task before this port's listener ever observed a per-task
        # duration to compare -- the skew was real and gone by the time it
        # reached stage_events. Disabling coalesce keeps the distribution
        # visible, the same way an operator debugging a "where did my skew
        # go" report would.
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        # Second finding, also from running this for real: a broadcast join
        # followed by groupBy(key).count() pre-aggregates on the map side,
        # so the shuffle carries only ~10 partial counts per task -- tiny
        # bytes regardless of how skewed the underlying row counts were.
        # bytes_per_task correctly reads that as "not enough data to call
        # this a data-volume tail" (BUG 3 in detector.py/skew.py) even
        # though the row-count skew is real. Forcing a sort-merge join
        # (no broadcast) and counting the UNGROUPED join result instead
        # keeps full row bytes on the wire, so the skew survives into
        # shuffle_write_bytes/shuffle_read_bytes, not just task duration.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    sc = spark.sparkContext
    job_id = sc.applicationId
    print(f"[dev-pyspark] job_id={job_id}", flush=True)

    event_queue = attach(sc, sink=producer.send, job_id=job_id)

    # `payload` exists so the shuffle actually carries real bytes: a bare
    # (id, key) row is 1-2 low-cardinality integer columns that a shuffle
    # compression codec squashes to near nothing (found running this job's
    # first version -- 2M rows of just that shrank to ~200 KB on the wire,
    # staying under MIN_BYTES_PER_TASK regardless of the row-count skew).
    # sha2 of id is high-entropy per row, so it doesn't compress away.
    fact = (
        spark.range(0, 2_000_000)
        .withColumn("key", F.when(F.col("id") % 10 < 9, F.lit(0)).otherwise(F.col("id") % 10))
        .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
    )
    dim = spark.createDataFrame([(i, f"name-{i}") for i in range(10)], ["key", "name"])

    # .count() alone needs no column VALUES, so Catalyst's column-pruning
    # rule provably eliminates `payload` before it ever reaches a shuffle --
    # found running this job's second version, which still showed ~200 KB
    # shuffles despite the new column. Aggregating a function OF payload
    # forces it to survive pruning into the actual exchange.
    joined = fact.join(dim, on="key")
    result = joined.agg(F.sum(F.length("payload")).alias("total_payload_bytes")).collect()
    print(f"[dev-pyspark]   total_payload_bytes={result[0]['total_payload_bytes']}", flush=True)

    # Let the aggregator's last onStageCompleted events drain into the queue
    # and the queue drain into the producer before shutdown -- stop() blocks
    # until the worker thread is idle, but py4j callbacks for the final
    # stage(s) can still be arriving when .collect() returns.
    time.sleep(1.0)
    event_queue.stop(timeout=15.0)
    producer.flush(timeout=15.0)

    print(
        f"[dev-pyspark] job_id={job_id} "
        f"queue_stats={event_queue.stats()} producer_stats={producer.stats()}",
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    main()
