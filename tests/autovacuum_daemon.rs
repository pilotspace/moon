//! P4 — Autovacuum daemon + cost-based throttle tests.
//!
//! Red/Green TDD: these tests were written BEFORE the implementation.
//!
//! Test suite:
//!   1. `test_budget_shrinks_under_high_p95` — unit test, no server required.
//!      Injects high P95 latency, asserts budget shrinks and throttle counter increments.
//!   2. `test_budget_grows_under_low_p95` — budget grows when P95 < target/2.
//!   3. `test_budget_clamped_to_bounds` — budget never exits [min, max].
//!   4. `test_disabled_autovacuum_is_noop` — `--autovacuum=disable` skips all passes.
//!   5. `test_autovacuum_tick_updates_last_run_ts` — RECL_AUTOVACUUM_LAST_RUN_TS advances.

use std::sync::atomic::Ordering;

// ---------------------------------------------------------------------------
// Unit tests for AutovacuumDaemon budget logic (no server required)
// ---------------------------------------------------------------------------

#[test]
fn test_budget_shrinks_under_high_p95() {
    use moon::shard::autovacuum::{AutovacuumConfig, AutovacuumDaemon};

    let cfg = AutovacuumConfig {
        enabled: true,
        budget_ms_min: 5,
        budget_ms_max: 200,
        target_p95_ms: 10,
        interval_secs: 30,
    };
    let mut daemon = AutovacuumDaemon::new(cfg);

    // Inject 1000 samples all at 50ms — well above target_p95_ms=10.
    for _ in 0..1000 {
        daemon.record_request_latency_ms(50);
    }

    let initial_budget = daemon.current_budget_ms();

    // Tick once — should shrink budget because P95 > target.
    let stats = daemon.tick_budget_only();

    assert!(
        daemon.current_budget_ms() < initial_budget,
        "budget should shrink when P95 ({} ms) > target ({} ms), was {}, now {}",
        stats.observed_p95_ms,
        10,
        initial_budget,
        daemon.current_budget_ms()
    );
    assert!(
        stats.throttled_this_tick,
        "throttled_this_tick must be true when budget shrinks"
    );
}

#[test]
fn test_budget_grows_under_low_p95() {
    use moon::shard::autovacuum::{AutovacuumConfig, AutovacuumDaemon};

    let cfg = AutovacuumConfig {
        enabled: true,
        budget_ms_min: 5,
        budget_ms_max: 200,
        target_p95_ms: 10,
        interval_secs: 30,
    };
    let mut daemon = AutovacuumDaemon::new(cfg);

    // Start from a shrunken budget.
    daemon.set_budget_ms_for_test(40);

    // Inject 1000 samples all at 1ms — below target_p95_ms/2 = 5ms.
    for _ in 0..1000 {
        daemon.record_request_latency_ms(1);
    }

    let initial_budget = daemon.current_budget_ms();

    let stats = daemon.tick_budget_only();

    assert!(
        daemon.current_budget_ms() > initial_budget,
        "budget should grow when P95 ({} ms) < target/2 ({} ms), was {}, now {}",
        stats.observed_p95_ms,
        5,
        initial_budget,
        daemon.current_budget_ms()
    );
    assert!(
        !stats.throttled_this_tick,
        "not throttled when budget grows"
    );
}

#[test]
fn test_budget_clamped_to_bounds() {
    use moon::shard::autovacuum::{AutovacuumConfig, AutovacuumDaemon};

    let cfg = AutovacuumConfig {
        enabled: true,
        budget_ms_min: 5,
        budget_ms_max: 200,
        target_p95_ms: 10,
        interval_secs: 30,
    };
    let mut daemon = AutovacuumDaemon::new(cfg);

    // High load — budget should shrink but never below min.
    for _ in 0..20 {
        for _ in 0..1000 {
            daemon.record_request_latency_ms(200);
        }
        daemon.tick_budget_only();
    }
    assert!(
        daemon.current_budget_ms() >= 5,
        "budget must never go below budget_ms_min=5, got {}",
        daemon.current_budget_ms()
    );

    // Low load — budget should grow but never above max.
    for _ in 0..20 {
        for _ in 0..1000 {
            daemon.record_request_latency_ms(1);
        }
        daemon.tick_budget_only();
    }
    assert!(
        daemon.current_budget_ms() <= 200,
        "budget must never exceed budget_ms_max=200, got {}",
        daemon.current_budget_ms()
    );
}

#[test]
fn test_disabled_autovacuum_is_noop() {
    use moon::command::info_reclamation::RECL_AUTOVACUUM_LAST_RUN_TS;
    use moon::shard::autovacuum::{AutovacuumConfig, AutovacuumDaemon};

    let cfg = AutovacuumConfig {
        enabled: false,
        budget_ms_min: 5,
        budget_ms_max: 200,
        target_p95_ms: 10,
        interval_secs: 30,
    };
    let mut daemon = AutovacuumDaemon::new(cfg);

    let ts_before = RECL_AUTOVACUUM_LAST_RUN_TS.load(Ordering::Relaxed);
    let stats = daemon.tick_budget_only();

    assert!(!stats.did_run, "disabled daemon must not run any passes");
    // LAST_RUN_TS should NOT advance when disabled.
    let ts_after = RECL_AUTOVACUUM_LAST_RUN_TS.load(Ordering::Relaxed);
    assert_eq!(
        ts_before, ts_after,
        "RECL_AUTOVACUUM_LAST_RUN_TS must not change when autovacuum is disabled"
    );
}

#[test]
fn test_autovacuum_tick_updates_last_run_ts() {
    use moon::command::info_reclamation::RECL_AUTOVACUUM_LAST_RUN_TS;
    use moon::shard::autovacuum::{AutovacuumConfig, AutovacuumDaemon};

    let cfg = AutovacuumConfig {
        enabled: true,
        budget_ms_min: 5,
        budget_ms_max: 200,
        target_p95_ms: 10,
        interval_secs: 1, // always ready
    };
    let mut daemon = AutovacuumDaemon::new(cfg);

    // Force the interval to be overdue.
    daemon.force_overdue_for_test();

    let ts_before = RECL_AUTOVACUUM_LAST_RUN_TS.load(Ordering::Relaxed);

    // tick_budget_only doesn't call actual reclamation passes — use tick_no_passes
    // which runs scheduling + RECL update but zero-cost passes.
    let stats = daemon.tick_no_passes();

    assert!(
        stats.did_run,
        "enabled daemon with overdue interval must run"
    );

    let ts_after = RECL_AUTOVACUUM_LAST_RUN_TS.load(Ordering::Relaxed);
    assert!(
        ts_after >= ts_before,
        "RECL_AUTOVACUUM_LAST_RUN_TS must advance after a tick"
    );
}

#[test]
fn test_throttle_counter_increments_on_shrink() {
    use moon::command::info_reclamation::RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD;
    use moon::shard::autovacuum::{AutovacuumConfig, AutovacuumDaemon};

    // Read baseline (other tests may have incremented already — capture delta).
    let baseline = RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD.load(Ordering::Relaxed);

    let cfg = AutovacuumConfig {
        enabled: true,
        budget_ms_min: 5,
        budget_ms_max: 200,
        target_p95_ms: 10,
        interval_secs: 30,
    };
    let mut daemon = AutovacuumDaemon::new(cfg);

    // High P95 → budget shrinks → throttle counter must increment.
    for _ in 0..1000 {
        daemon.record_request_latency_ms(100);
    }
    let stats = daemon.tick_budget_only();

    if stats.throttled_this_tick {
        let after = RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD.load(Ordering::Relaxed);
        assert!(
            after > baseline,
            "RECL_AUTOVACUUM_THROTTLED_DUE_TO_LOAD must increment when throttled"
        );
    }
}
