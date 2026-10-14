from apex_engine_postgres.detector import MIN_BYTES_PER_TASK, evaluate
from apex_engine_postgres.physics import SLOTS_UNKNOWN, operator_width
from apex_engine_postgres.schema import Severity, StageRow


def stage(**overrides) -> StageRow:
    payload = dict(
        job_id="app-20260729182801-0045",
        app_id="app-20260729182801-0045",
        stage_id=21,
        stage_attempt=0,
        task_count=100,
        task_duration_p50_ms=50.0,
        task_duration_p99_ms=1035.0,  # 20.7x
        task_duration_max_ms=1040.0,
        task_duration_sample_count=100,
        shuffle_read_bytes=113_527_779,  # ~1.08 MiB/task, above the floor
        shuffle_write_bytes=0,
        spill_disk_bytes=0,
        spill_mem_bytes=0,
        gc_time_ms=10,
        input_bytes=0,
        output_bytes=0,
    )
    payload.update(overrides)
    return StageRow(**payload)


def test_a_genuinely_skewed_stage_with_known_width_is_critical():
    finding = evaluate(stage(), width=operator_width(8))
    assert finding is not None
    assert finding.finding_type == "TASK_SKEW"
    assert finding.severity == Severity.CRITICAL
    assert finding.details["tail_bound_verdict"] == "tail_bound"
    assert "could remove at most" in finding.evidence


def test_the_celebrated_false_positive_is_work_bound_and_produces_no_finding():
    """21.62x on 50 tasks / 2 slots needs > 49x -- WORK_BOUND, no finding."""
    skewed = stage(
        stage_id=6, task_count=50, task_duration_p50_ms=21.0, task_duration_p99_ms=454.0,
        shuffle_read_bytes=50 * MIN_BYTES_PER_TASK,
    )
    assert evaluate(skewed, width=operator_width(2)) is None


def test_below_the_byte_floor_produces_no_finding_regardless_of_ratio():
    """Tiny Delta-metadata stages must not be promoted to a claim (BUG 3)."""
    tiny = stage(shuffle_read_bytes=1000, input_bytes=0)
    assert evaluate(tiny, width=operator_width(8)) is None


def test_too_few_tasks_is_not_a_distribution():
    small = stage(task_count=3)
    assert evaluate(small, width=operator_width(8)) is None


def test_unknown_width_can_still_report_when_the_win_dominates():
    finding = evaluate(stage())  # no width supplied -> UNKNOWN_WIDTH
    assert finding is not None
    assert finding.details["tail_bound_verdict"] == SLOTS_UNKNOWN
    assert finding.details["slots"] is None
    assert finding.confidence_score == 0.50


def test_degenerate_wide_cluster_regime_needs_a_dominant_win():
    """8 tasks on 8 slots, 1.01x, a 0.8% win -- must not be reported (rule 6
    + no-floor gate): bar is 1.0, so the ratio 'passes' rule 1 trivially, but
    0.8% is far below DOMINANT_GAIN with no measured floor to compare to."""
    tiny_tail = stage(
        task_count=8, task_duration_p50_ms=2276, task_duration_p99_ms=2294,
        input_bytes=240 * 1024 * 1024,
    )
    assert evaluate(tiny_tail, width=operator_width(8)) is None


def test_degenerate_wide_cluster_regime_reports_a_truly_dominant_win():
    """DOMINANT_GAIN (0.5) is a higher bar than STRONG_HEADROOM (0.25), so any
    win that survives this gate at all is always >= STRONG_HEADROOM too --
    every finding that makes it through the degenerate-regime path is
    CRITICAL, never WARNING. That's a real asymmetry worth a test, not an
    assumption."""
    dominant = stage(
        task_count=8, task_duration_p50_ms=100, task_duration_p99_ms=1000,
        input_bytes=240 * 1024 * 1024,
    )
    finding = evaluate(dominant, width=operator_width(8))
    assert finding is not None
    assert finding.severity == Severity.CRITICAL


def test_missing_task_stats_produce_no_finding_not_a_crash():
    incomplete = stage(task_duration_p50_ms=None, task_duration_p99_ms=None)
    assert evaluate(incomplete, width=operator_width(8)) is None


def test_confidence_always_carries_the_no_floor_penalty():
    """This v1 port never has a measured floor, so every finding takes
    upstream's floor-unknown penalty -- never the floor-known score."""
    finding = evaluate(stage(), width=operator_width(8))
    assert finding.confidence_score == 0.80 - 0.03  # CONF_OPERATOR_STRONG, strong headroom


def test_fix_never_recommends_enabling_skewjoin_without_plan_evidence():
    """The fix text may explain why it's withholding the skewJoin
    recommendation (that's the whole point), but must never phrase it as a
    recommendation to set/enable it -- this port has no plan evidence to
    justify that the way upstream's join-evidenced watcher does."""
    finding = evaluate(stage(), width=operator_width(8))
    assert "will not recommend" in finding.fix
    assert "set spark.sql.adaptive.skewJoin" not in finding.fix
    assert "enable spark.sql.adaptive.skewJoin" not in finding.fix
    assert "cannot distinguish task-level skew from join skew" in finding.fix
