from apex_capture.aggregator import StageAggregator, ZERO_STATS


def test_pop_stage_with_no_data_returns_zero_stats():
    agg = StageAggregator()
    assert agg.pop_stage((1, 0)) == ZERO_STATS


def test_percentiles_match_hand_computed_values():
    agg = StageAggregator()
    key = (3, 0)
    for duration in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        agg.on_task_end(key, float(duration))

    stats = agg.pop_stage(key)
    # rank(0.50) = 0.5 * 9 = 4.5 -> interpolate values[4]=5, values[5]=6
    assert stats.p50_ms == 5.5
    # rank(0.99) = 0.99 * 9 = 8.91 -> interpolate values[8]=9, values[9]=10
    assert round(stats.p99_ms, 2) == 9.91
    assert stats.max_ms == 10.0
    assert stats.count == 10


def test_single_sample_returns_that_value_for_every_percentile():
    agg = StageAggregator()
    agg.on_task_end((1, 0), 42.0)
    stats = agg.pop_stage((1, 0))
    assert stats.p50_ms == stats.p99_ms == stats.max_ms == 42.0
    assert stats.count == 1


def test_pop_stage_discards_state_after_read():
    agg = StageAggregator()
    key = (1, 0)
    agg.on_task_end(key, 10.0)
    agg.pop_stage(key)

    assert agg.pop_stage(key) == ZERO_STATS
    assert agg.pending_stage_count() == 0


def test_durations_arrive_out_of_order_and_still_sort_correctly():
    agg = StageAggregator()
    key = (1, 0)
    for duration in [50.0, 10.0, 30.0, 20.0, 40.0]:
        agg.on_task_end(key, duration)

    stats = agg.pop_stage(key)
    assert stats.max_ms == 50.0
    assert stats.count == 5


def test_stage_attempts_are_tracked_independently():
    agg = StageAggregator()
    agg.on_task_end((1, 0), 100.0)
    agg.on_task_end((1, 1), 999.0)  # a retried attempt of the same stage_id

    assert agg.pending_stage_count() == 2
    assert agg.pop_stage((1, 0)).max_ms == 100.0
    assert agg.pop_stage((1, 1)).max_ms == 999.0
    assert agg.pending_stage_count() == 0
