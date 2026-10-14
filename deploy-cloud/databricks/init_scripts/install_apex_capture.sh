#!/usr/bin/env bash
# Databricks cluster-scoped init script. Runs on every node (driver +
# workers) before Spark starts, which is required here: capture-pyspark's
# listener only needs to run on the driver, but the wheel itself must be
# importable wherever the job's Python interpreter is launched, and
# Databricks runs init scripts cluster-wide, not driver-only, by design.
#
# Installs from wheels staged in a Unity Catalog volume (or DBFS on a
# workspace without UC), NOT from PyPI -- these are this branch's own
# unpublished lanes. See ../README.md for how the wheels get there
# (`python -m build` + `databricks fs cp`, documented, not automated by
# this script).
set -euo pipefail

WHEEL_DIR="${APEX_WHEEL_DIR:-/Volumes/apex_portfolio/capture/wheels}"

/databricks/python/bin/pip install --no-index --find-links "${WHEEL_DIR}" \
  apex-capture-pyspark \
  apex-stream-kafka \
  kafka-python
