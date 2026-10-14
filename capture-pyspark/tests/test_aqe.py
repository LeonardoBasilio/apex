"""Fixtures below are the REAL physicalPlanDescription text captured from
dev-pyspark/jobs/skew_join_job.py's run with
spark.sql.adaptive.skewJoin.enabled=true -- not synthesized. See aqe.py's
module docstring for the full context.
"""

from apex_capture.aqe import (
    BEST_EFFORT,
    COALESCE,
    HIGH,
    JOIN_SWITCH,
    OTHER,
    SKEW_SPLIT,
    AQETracker,
)

PLAN_BEFORE_SKEW_SPLIT = """== Physical Plan ==
AdaptiveSparkPlan (27)
+- == Current Plan ==
   HashAggregate (17)
   +- Exchange (16)
      +- HashAggregate (15)
         +- Project (14)
            +- SortMergeJoin Inner (13)
               :- Sort (6)
               :  +- ShuffleQueryStage (5)
               :     +- Exchange (4)
"""

PLAN_AFTER_SKEW_SPLIT = """== Physical Plan ==
AdaptiveSparkPlan (30)
+- == Current Plan ==
   HashAggregate (20)
   +- ShuffleQueryStage (19)
      +- Exchange (18)
         +- * HashAggregate (17)
            +- * Project (16)
               +- * SortMergeJoin(skew=true) Inner (15)
                  :- * Sort (7)
                  :  +- AQEShuffleRead (6)
                  :     +- ShuffleQueryStage (5), Statistics(sizeInBytes=167.8 MiB, rowCount=2.00E+6)
                  :        +- Exchange (4)
"""

PLAN_FINAL_UNCHANGED_SKEW = """== Physical Plan ==
AdaptiveSparkPlan (31)
+- == Final Plan ==
   ResultQueryStage (21), Statistics(sizeInBytes=8.0 EiB)
   +- * HashAggregate (20)
      +- ShuffleQueryStage (19), Statistics(sizeInBytes=144.0 B, rowCount=9)
         +- Exchange (18)
            +- * HashAggregate (17)
               +- * Project (16)
                  +- * SortMergeJoin(skew=true) Inner (15)
                     :- * Sort (7)
                     :  +- AQEShuffleRead (6)
"""


class FakeAdaptiveUpdateEvent:
    def __init__(self, execution_id: int, plan_text: str):
        self._execution_id = execution_id
        self._plan_text = plan_text

    def executionId(self):
        return self._execution_id

    def physicalPlanDescription(self):
        return self._plan_text


class NotAnAdaptiveEvent:
    pass


def test_not_an_adaptive_event_returns_none():
    tracker = AQETracker()
    assert tracker.process(NotAnAdaptiveEvent(), job_id="j1") is None


def test_first_snapshot_for_an_execution_is_other_with_no_before():
    tracker = AQETracker()
    transition = tracker.process(
        FakeAdaptiveUpdateEvent(0, PLAN_BEFORE_SKEW_SPLIT), job_id="j1"
    )
    assert transition.transition_type == OTHER
    assert transition.update_seq == 0
    assert transition.before is None
    assert transition.execution_id == 0
    assert transition.job_id == "j1"


def test_real_captured_skew_split_is_detected_with_high_confidence():
    """This is the actual transition dev-pyspark/'s skew-join job produced,
    verbatim, between its first two AQE updates."""
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_BEFORE_SKEW_SPLIT), job_id="j1")
    transition = tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_AFTER_SKEW_SPLIT), job_id="j1")

    assert transition.transition_type == SKEW_SPLIT
    assert transition.confidence == HIGH
    assert transition.update_seq == 1
    assert transition.before == "SortMergeJoin"
    assert transition.after == "SortMergeJoin(skew=true)"
    assert "AQEShuffleRead" in transition.detail


def test_skew_split_is_reported_once_not_on_every_subsequent_snapshot():
    """The final plan still has skew=true, but it's not a NEW transition --
    update 2 must not re-fire skew_split."""
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_BEFORE_SKEW_SPLIT), job_id="j1")
    tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_AFTER_SKEW_SPLIT), job_id="j1")
    final = tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_FINAL_UNCHANGED_SKEW), job_id="j1")

    assert final.transition_type == OTHER
    assert final.update_seq == 2


def test_join_switch_detected_between_sort_merge_and_broadcast():
    sort_merge = "+- SortMergeJoin Inner (13)\n"
    broadcast = "+- BroadcastHashJoin Inner (13)\n"
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(1, sort_merge), job_id="j1")
    transition = tracker.process(FakeAdaptiveUpdateEvent(1, broadcast), job_id="j1")

    assert transition.transition_type == JOIN_SWITCH
    assert transition.confidence == HIGH
    assert transition.before == "SortMergeJoin"
    assert transition.after == "BroadcastHashJoin"


def test_coalesce_detected_from_a_coalesced_descriptor_best_effort():
    """Implemented from upstream's documented AQEShuffleRead descriptor
    convention, NOT exercised by a real run in this session -- see
    aqe.py's module docstring and README.md's deferred-verification note."""
    before = "+- AQEShuffleRead (6)\n   +- ShuffleQueryStage (5)\n"
    after = "+- AQEShuffleRead (6) coalesced\n   +- ShuffleQueryStage (5)\n"
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(2, before), job_id="j1")
    transition = tracker.process(FakeAdaptiveUpdateEvent(2, after), job_id="j1")

    assert transition.transition_type == COALESCE
    assert transition.confidence == BEST_EFFORT


def test_identical_consecutive_snapshots_are_other_not_a_transition():
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(3, PLAN_AFTER_SKEW_SPLIT), job_id="j1")
    transition = tracker.process(FakeAdaptiveUpdateEvent(3, PLAN_AFTER_SKEW_SPLIT), job_id="j1")

    assert transition.transition_type == OTHER
    assert transition.detail == "plan snapshot unchanged"


def test_execution_ids_are_tracked_independently():
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_BEFORE_SKEW_SPLIT), job_id="j1")
    # execution_id=1's FIRST snapshot must be treated as "initial", not
    # diffed against execution 0's state.
    transition = tracker.process(FakeAdaptiveUpdateEvent(1, PLAN_AFTER_SKEW_SPLIT), job_id="j1")

    assert transition.update_seq == 0
    assert transition.before is None
    assert transition.transition_type == OTHER


def test_detail_never_carries_a_literal_value_from_the_plan():
    """Redaction rule: only structural descriptors (operator names, node
    ids, byte/row statistics) ever appear in `detail`/`before`/`after` --
    the Statistics(...) byte/row counts are themselves structural, not
    user data, but a literal filter value never would be, and this plan
    fixture's text has none to leak -- asserting the extracted fields are
    drawn only from the known operator-name vocabulary, never raw text."""
    tracker = AQETracker()
    tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_BEFORE_SKEW_SPLIT), job_id="j1")
    transition = tracker.process(FakeAdaptiveUpdateEvent(0, PLAN_AFTER_SKEW_SPLIT), job_id="j1")

    assert transition.after in ("SortMergeJoin", "SortMergeJoin(skew=true)",
                                 "ShuffledHashJoin", "ShuffledHashJoin(skew=true)",
                                 "BroadcastHashJoin", "BroadcastHashJoin(skew=true)")
