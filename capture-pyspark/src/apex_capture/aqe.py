"""AQE plan-transition capture — CONTRACT.md's v0.2 `plan_transitions` event,
ported to this port's scope. Upstream's Scala jar hooks
`SparkListenerSQLAdaptiveExecutionUpdate` the same way: it fires on
`onOtherEvent` (Spark's catch-all for events without a dedicated callback),
carries the SQL execution's current physical-plan snapshot, and arrives once
per AQE re-plan for a given `execution_id`.

The classification heuristics below are grounded in a REAL captured run, not
guessed: running `dev-pyspark/`'s skew-join job with
`spark.sql.adaptive.skewJoin.enabled=true` produced this exact transition,
verbatim —

    update 0: "... SortMergeJoin Inner (13) ..."
    update 1: "... SortMergeJoin(skew=true) Inner (15) ...
                  AQEShuffleRead (6) ... ShuffleQueryStage (5),
                  Statistics(sizeInBytes=167.8 MiB, rowCount=2.00E+6) ..."

`(skew=true)` appearing on the dominant join node between two consecutive
snapshots is `skew_split`'s real, observed signature. `join_switch`
(SortMergeJoin/ShuffledHashJoin -> BroadcastHashJoin) and `coalesce` (an
AQEShuffleRead descriptor with no skew annotation) are implemented from
upstream's documented behavior and Spark's own operator-naming conventions,
but were NOT exercised by a real run in this session — this job's conf
disables broadcast joins entirely and never shrank partition counts, so
neither path has fired here yet. Flagged in README.md rather than presented
as equally verified.

Every field extracted is a structural descriptor (operator names, node ids,
byte/row statistics) — never a literal value from the data, matching
upstream's `plan_json`/`plan_transitions` redaction rule.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

JOIN_NODE_PATTERN = re.compile(
    r"(SortMergeJoin|ShuffledHashJoin|BroadcastHashJoin)(\(skew=true\))?"
)

SKEW_SPLIT = "skew_split"
JOIN_SWITCH = "join_switch"
COALESCE = "coalesce"
OTHER = "other"

HIGH = "HIGH"
BEST_EFFORT = "BEST_EFFORT"


@dataclass(frozen=True)
class PlanTransition:
    job_id: str
    execution_id: int
    update_seq: int
    transition_type: str
    detail: str
    before: Optional[str]
    after: str
    confidence: str
    ts: int


def _dominant_join_descriptor(plan_text: str) -> Optional[str]:
    match = JOIN_NODE_PATTERN.search(plan_text)
    return match.group(0) if match else None


def _base_join_type(descriptor: Optional[str]) -> Optional[str]:
    return descriptor.split("(")[0] if descriptor else None


def _classify(prior_text: Optional[str], new_text: str) -> tuple[str, str, str]:
    """Returns (transition_type, confidence, detail). `detail` is a
    structural descriptor only — see module docstring on redaction.

    A transition needs something to transition FROM: `prior_text is None`
    means this is the first snapshot ever observed for this execution_id,
    so even if it already shows `(skew=true)` or a coalesced descriptor,
    that was true before this listener started watching — it is not a
    transition this run witnessed."""
    if prior_text is None:
        return OTHER, BEST_EFFORT, "initial plan snapshot for this execution"

    prior_join = _dominant_join_descriptor(prior_text)
    new_join = _dominant_join_descriptor(new_text)

    new_has_skew = bool(new_join) and "(skew=true)" in new_join
    prior_had_skew = bool(prior_join) and "(skew=true)" in prior_join
    if new_has_skew and not prior_had_skew:
        return SKEW_SPLIT, HIGH, f"{new_join} -- AQEShuffleRead split a skewed partition"

    prior_base, new_base = _base_join_type(prior_join), _base_join_type(new_join)
    if prior_base and new_base and prior_base != new_base:
        return JOIN_SWITCH, HIGH, f"{prior_base} -> {new_base}"

    new_coalesced = "coalesced" in new_text.lower()
    prior_coalesced = bool(prior_text) and "coalesced" in prior_text.lower()
    if new_coalesced and not prior_coalesced:
        return COALESCE, BEST_EFFORT, "AQEShuffleRead shows a coalesced-partition descriptor"

    if prior_text != new_text:
        return OTHER, BEST_EFFORT, "plan snapshot changed, no recognized transition pattern"
    return OTHER, BEST_EFFORT, "plan snapshot unchanged"


class AQETracker:
    """Keeps the prior plan-text snapshot and update_seq counter per
    execution_id. Not thread-safe by design, same reasoning as
    aggregator.StageAggregator: Spark's listener bus dispatches to one
    listener sequentially, so this only needs to be correct under that
    single-threaded calling convention."""

    def __init__(self) -> None:
        self._prior_text: Dict[int, str] = {}
        self._next_seq: Dict[int, int] = {}

    def process(self, java_event: Any, job_id: str) -> Optional[PlanTransition]:
        """Returns None when `java_event` isn't a
        SparkListenerSQLAdaptiveExecutionUpdate (anything else that arrives
        via onOtherEvent) -- detected by duck-typing the two methods this
        event type exposes, not by matching a JVM class name string, so a
        renamed/moved class across Spark versions degrades to "not
        recognized" rather than raising."""
        try:
            execution_id = int(java_event.executionId())
            plan_text = str(java_event.physicalPlanDescription())
        except Exception:
            return None

        update_seq = self._next_seq.get(execution_id, 0)
        self._next_seq[execution_id] = update_seq + 1
        prior_text = self._prior_text.get(execution_id)
        self._prior_text[execution_id] = plan_text

        transition_type, confidence, detail = _classify(prior_text, plan_text)

        return PlanTransition(
            job_id=job_id,
            execution_id=execution_id,
            update_seq=update_seq,
            transition_type=transition_type,
            detail=detail,
            before=_dominant_join_descriptor(prior_text) if prior_text else None,
            after=_dominant_join_descriptor(plan_text) or "",
            confidence=confidence,
            ts=int(time.time() * 1000),
        )
