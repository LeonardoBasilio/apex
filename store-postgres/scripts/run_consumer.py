#!/usr/bin/env python3
"""Continuous Kafka -> Postgres loop. Thin process wiring only -- every
piece of actual logic it calls (StageEventConsumer, apply_ddl) is
unit-tested without any of this; this script just keeps a real KafkaConsumer
fed into it, forever, inside the `consumer` container defined in
docker-compose.yml.
"""

from __future__ import annotations

import os
import sys

from kafka import KafkaConsumer

from apex_store.connection import from_dsn
from apex_store.consumer import STAGE_EVENTS_TOPIC, StageEventConsumer
from apex_store.migrate import apply_ddl


def main() -> None:
    dsn = os.environ["POSTGRES_DSN"]
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"].split(",")

    connection = from_dsn(dsn)
    applied = apply_ddl(connection)  # idempotent re-apply; a no-op once already applied
    print(f"[store-postgres] migrations applied: {applied}", flush=True)

    stage_consumer = StageEventConsumer(connection)
    kafka_consumer = KafkaConsumer(
        STAGE_EVENTS_TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="apex-store-postgres",
    )

    print(
        f"[store-postgres] consuming '{STAGE_EVENTS_TOPIC}' from {bootstrap_servers}",
        flush=True,
    )
    for message in kafka_consumer:
        stage_consumer.consume_one(message)
        stats = stage_consumer.stats()
        if stats.inserted and stats.inserted % 50 == 0:
            print(f"[store-postgres] stats={stats}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
