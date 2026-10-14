"""Ported from upstream's engine/tests/test_physics_rules.py (rule-1
section only — physics.py is a verbatim-equivalent copy, so these anchors
must still hold). Every number here is one upstream measured on a real run,
not invented to make a test pass:

  * 21.62x / 50 tasks / 2 slots -- the marquee false positive (needs > 49x);
  * 9.3, 11.9, 7.7 / 200 tasks  -- a balanced control stage's pure jitter
                                   (bar: 28.43, must never pass);
  * 17.7-20.6 / 100 tasks       -- a genuinely skewed stage (bar: 14.14).
"""

from __future__ import annotations

import pytest

from apex_engine_postgres.physics import (
    SERIAL,
    SLOTS_UNKNOWN,
    TAIL_BOUND,
    WORK_BOUND,
    evaluate_tail_bound,
    min_slots_for_tail_bound,
    operator_width,
    tail_bound_threshold,
)


def test_threshold_is_task_count_over_cluster_width():
    assert tail_bound_threshold(50, 2) == 49.0
    assert tail_bound_threshold(100, 8) == pytest.approx(14.142857, rel=1e-6)
    assert tail_bound_threshold(200, 8) == pytest.approx(28.4285, rel=1e-4)
    assert tail_bound_threshold(50, 50) == 1.0


def test_volume_cancels_out_of_the_threshold():
    """The bar depends on task count and width only -- never on bytes."""
    tiny = evaluate_tail_bound(n_tasks=50, p50_ms=21, p99_ms=454, width=operator_width(2))
    huge = evaluate_tail_bound(n_tasks=50, p50_ms=21_000, p99_ms=454_000, width=operator_width(2))
    assert tiny.threshold == huge.threshold == 49.0
    assert tiny.verdict == huge.verdict == WORK_BOUND


def test_the_celebrated_ratio_is_work_bound_on_two_slots():
    """21.62x needed > 49x. A fixed 10x threshold would call it CRITICAL."""
    verdict = evaluate_tail_bound(n_tasks=50, p50_ms=21, p99_ms=454, width=operator_width(2))
    assert verdict.verdict == WORK_BOUND
    assert verdict.headroom_frac == 0.0, "a perfect skew fix returns nothing here"


def test_balanced_control_stage_never_passes():
    """8-12x manufactured from jitter alone at 200 tasks; the bar is 28.43."""
    for ratio in (9.3, 11.9, 7.7):
        verdict = evaluate_tail_bound(
            n_tasks=200, p50_ms=100, p99_ms=100 * ratio, width=operator_width(8)
        )
        assert verdict.verdict == WORK_BOUND, f"{ratio}x must not pass a 28.43 bar"


def test_genuinely_skewed_stage_always_passes():
    """17.7-20.6x at 100 tasks / 8 slots; the bar is 14.14."""
    for ratio in (17.7, 18.7, 19.4, 20.6):
        verdict = evaluate_tail_bound(
            n_tasks=100, p50_ms=50, p99_ms=50 * ratio, width=operator_width(8)
        )
        assert verdict.verdict == TAIL_BOUND
        assert verdict.margin > 1.0


def test_one_slot_is_serial_and_can_never_be_tail_bound():
    verdict = evaluate_tail_bound(n_tasks=50, p50_ms=10, p99_ms=10_000, width=operator_width(1))
    assert verdict.verdict == SERIAL
    assert verdict.threshold is None


def test_an_unknown_width_is_never_replaced_by_a_number():
    verdict = evaluate_tail_bound(n_tasks=100, p50_ms=50, p99_ms=1035)
    assert verdict.verdict == SLOTS_UNKNOWN
    assert verdict.threshold is None and verdict.width.slots is None
    assert verdict.min_slots_required == pytest.approx(min_slots_for_tail_bound(100, 20.7), rel=1e-6)
    assert verdict.min_slots_required == pytest.approx(5.78, abs=0.01)


def test_the_width_free_headroom_bound_survives_an_unknown_width():
    verdict = evaluate_tail_bound(n_tasks=100, p50_ms=50, p99_ms=1035)
    assert verdict.headroom_frac is None
    assert verdict.claimed_gain_frac == pytest.approx(1 - 50 / 1035)


def test_degenerate_regime_slots_at_least_n_tasks_still_passes_rule_1():
    """bar = (n-1)/(slots-1) <= 1 exactly when n <= slots, and p99/p50 >= 1
    always -- so EVERY such stage satisfies rule 1, including a perfectly
    uniform one (CONTRACT.md rule 6). physics.py does not filter this; it's
    detector.py's job (the DOMINANT_GAIN gate) to not treat it as a claim."""
    verdict = evaluate_tail_bound(n_tasks=8, p50_ms=2276, p99_ms=2294, width=operator_width(8))
    assert verdict.verdict == TAIL_BOUND
    assert verdict.threshold == 1.0
