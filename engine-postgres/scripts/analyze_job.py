#!/usr/bin/env python3
"""Fetch every stage_events row for one job_id from store-postgres's table
and run the v1 skew detector over each. Thin CLI wiring only -- proves the
chain capture-pyspark -> stream-kafka -> store-postgres -> engine-postgres
holds end to end; all the actual logic is unit-tested without any of this.

Usage:
    POSTGRES_DSN=postgresql://apex:apex_local_dev@localhost:5432/apex \
        python scripts/analyze_job.py <job_id> [--slots N]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict

from apex_engine_postgres.connection import from_dsn
from apex_engine_postgres.detector import evaluate
from apex_engine_postgres.physics import UNKNOWN_WIDTH, operator_width
from apex_engine_postgres.query import fetch_stages


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job_id")
    parser.add_argument(
        "--slots",
        type=int,
        default=None,
        help="cluster width (slots) to evaluate rule 1 against; omit to run with "
        "an unknown width (see physics.py's module docstring)",
    )
    args = parser.parse_args()

    dsn = os.environ["POSTGRES_DSN"]
    fetcher = from_dsn(dsn)
    width = operator_width(args.slots) if args.slots else UNKNOWN_WIDTH

    stages = fetch_stages(fetcher, args.job_id)
    if not stages:
        print(f"no stage_events rows for job_id={args.job_id!r}", file=sys.stderr)
        return 1

    findings = [f for stage in stages for f in [evaluate(stage, width=width)] if f is not None]

    print(json.dumps({"job_id": args.job_id, "stages_examined": len(stages),
                       "findings": [_finding_dict(f) for f in findings]}, indent=2))
    return 0


def _finding_dict(finding) -> dict:
    payload = asdict(finding)
    payload["severity"] = finding.severity.value
    return payload


if __name__ == "__main__":
    sys.exit(main())
