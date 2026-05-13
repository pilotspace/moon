//! MA5 — Maintenance-window scheduler tests.
//!
//! Red/Green TDD: these tests were written BEFORE the implementation.
//!
//! Test suite:
//!  1. `test_schedule_always_matches` — `* * * * *` returns multiplier at any time.
//!  2. `test_schedule_hour_window` — `0 2 * * *` matches hour=2 only.
//!  3. `test_schedule_business_hours` — `0 9-17 * * 1-5` matches weekdays 9-17,
//!     not weekends or outside hours.
//!  4. `test_schedule_multiple_windows` — two windows; highest multiplier wins
//!     when both match.
//!  5. `test_schedule_no_match_returns_1x` — no matching window returns 1.0.
//!  6. `test_schedule_invalid_cron_error` — bad expression returns error.
//!  7. `test_schedule_list_and_clear` — list returns all windows; clear empties.
//!  8. `test_schedule_persistence_roundtrip` — save/load preserves all windows.

use moon::shard::maintenance_schedule::{MaintenanceSchedule, ParseError};
use std::time::{SystemTime, UNIX_EPOCH};

// Helper: build a SystemTime for a specific UTC time.
// (year,month 1-based,day,hour,min,sec) using a simple epoch calculation.
fn make_utc(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> SystemTime {
    // Use time crate approach: compute epoch seconds manually.
    // Days from epoch (1970-01-01) to the given date.
    let days = days_from_epoch(year, month, day);
    let secs = days as u64 * 86400 + hour as u64 * 3600 + min as u64 * 60 + sec as u64;
    UNIX_EPOCH + std::time::Duration::from_secs(secs)
}

fn is_leap(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap(y) => 29,
        2 => 28,
        _ => 30,
    }
}

fn days_from_epoch(year: i32, month: u32, day: u32) -> i64 {
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }
    for m in 1..month {
        days += days_in_month(year, m) as i64;
    }
    days + day as i64 - 1
}

// Wednesday 2026-05-13 02:30:00 UTC
fn wed_2am() -> SystemTime {
    make_utc(2026, 5, 13, 2, 30, 0)
}

// Wednesday 2026-05-13 09:00:00 UTC  (business hours)
fn wed_9am() -> SystemTime {
    make_utc(2026, 5, 13, 9, 0, 0)
}

// Saturday 2026-05-16 09:00:00 UTC (weekend)
fn sat_9am() -> SystemTime {
    make_utc(2026, 5, 16, 9, 0, 0)
}

// ---------------------------------------------------------------------------
// 1. Wildcard schedule always matches
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_always_matches() {
    let mut sched = MaintenanceSchedule::new();
    sched.add("* * * * *", 0.5).expect("valid cron");

    // Should match at any time.
    let mul = sched.current_budget_multiplier(wed_2am());
    assert!(
        (mul - 0.5).abs() < 1e-6,
        "wildcard must return 0.5 at any time, got {mul}"
    );
}

// ---------------------------------------------------------------------------
// 2. Hour-specific window (0 2 * * * = every minute of hour 2)
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_hour_window() {
    let mut sched = MaintenanceSchedule::new();
    sched.add("* 2 * * *", 2.0).expect("valid cron");

    // 02:30 → matches.
    let mul = sched.current_budget_multiplier(wed_2am());
    assert!(
        (mul - 2.0).abs() < 1e-6,
        "should match at 02:30, got {mul}"
    );

    // 09:00 → no match.
    let mul2 = sched.current_budget_multiplier(wed_9am());
    assert!(
        (mul2 - 1.0).abs() < 1e-6,
        "should not match at 09:00, got {mul2}"
    );
}

// ---------------------------------------------------------------------------
// 3. Business-hours window (weekdays 9-17, Mon-Fri)
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_business_hours() {
    let mut sched = MaintenanceSchedule::new();
    // 0.1x during business hours (throttle background work).
    sched.add("* 9-17 * * 1-5", 0.1).expect("valid cron");

    // Wednesday 09:00 → matches (weekday, in hour range).
    let mul = sched.current_budget_multiplier(wed_9am());
    assert!(
        (mul - 0.1).abs() < 1e-6,
        "should match on Wednesday 09:00, got {mul}"
    );

    // Wednesday 02:30 → no match (outside hours).
    let mul2 = sched.current_budget_multiplier(wed_2am());
    assert!(
        (mul2 - 1.0).abs() < 1e-6,
        "should not match on Wednesday 02:30, got {mul2}"
    );

    // Saturday 09:00 → no match (weekend).
    let mul3 = sched.current_budget_multiplier(sat_9am());
    assert!(
        (mul3 - 1.0).abs() < 1e-6,
        "should not match on Saturday 09:00, got {mul3}"
    );
}

// ---------------------------------------------------------------------------
// 4. Multiple windows: highest multiplier wins when both match
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_multiple_windows_highest_wins() {
    let mut sched = MaintenanceSchedule::new();
    sched.add("* * * * *", 0.5).expect("window 1");
    sched.add("* 2 * * *", 2.0).expect("window 2");

    // At 02:30 both match; 2.0 > 0.5 so 2.0 wins.
    let mul = sched.current_budget_multiplier(wed_2am());
    assert!(
        (mul - 2.0).abs() < 1e-6,
        "highest multiplier should win when two windows match, got {mul}"
    );

    // At 09:00 only window 1 matches.
    let mul2 = sched.current_budget_multiplier(wed_9am());
    assert!(
        (mul2 - 0.5).abs() < 1e-6,
        "only wildcard should match at 09:00, got {mul2}"
    );
}

// ---------------------------------------------------------------------------
// 5. No matching window returns 1.0 (base multiplier)
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_no_match_returns_1x() {
    let sched = MaintenanceSchedule::new();
    let mul = sched.current_budget_multiplier(wed_2am());
    assert!(
        (mul - 1.0).abs() < 1e-6,
        "empty schedule must return 1.0, got {mul}"
    );
}

// ---------------------------------------------------------------------------
// 6. Invalid cron expression returns ParseError
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_invalid_cron_error() {
    let mut sched = MaintenanceSchedule::new();
    // Too few fields.
    assert!(
        matches!(sched.add("* * *", 1.0), Err(ParseError::InvalidField(_))),
        "too few fields must be an error"
    );
    // Invalid range.
    assert!(
        sched.add("70 * * * *", 1.0).is_err(),
        "minute > 59 must be an error"
    );
    // Non-numeric token.
    assert!(
        sched.add("abc * * * *", 1.0).is_err(),
        "non-numeric token must be an error"
    );
}

// ---------------------------------------------------------------------------
// 7. List and clear
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_list_and_clear() {
    let mut sched = MaintenanceSchedule::new();
    sched.add("* 2 * * *", 2.0).expect("add 1");
    sched.add("* 9-17 * * 1-5", 0.1).expect("add 2");

    let windows = sched.list();
    assert_eq!(windows.len(), 2, "should have 2 windows");

    sched.clear();
    assert!(sched.list().is_empty(), "clear must remove all windows");

    // After clear, current_budget_multiplier returns 1.0.
    let mul = sched.current_budget_multiplier(wed_2am());
    assert!((mul - 1.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// 8. Persistence round-trip (save + load)
// ---------------------------------------------------------------------------

#[test]
fn test_schedule_persistence_roundtrip() {
    let dir = tempfile::TempDir::new().expect("tmpdir");
    let path = dir.path().join("reclamation-schedule.toml");

    let mut sched = MaintenanceSchedule::new();
    sched.add("* 2 * * *", 2.0).expect("add 1");
    sched.add("* 9-17 * * 1-5", 0.1).expect("add 2");
    sched.save_to_file(&path).expect("save ok");

    let loaded = MaintenanceSchedule::load_from_file(&path).expect("load ok");
    assert_eq!(loaded.list().len(), 2, "loaded schedule must have 2 windows");

    // Verify semantics preserved.
    let mul = loaded.current_budget_multiplier(wed_2am());
    assert!((mul - 2.0).abs() < 1e-6, "hour-2 window preserved after reload");
}
