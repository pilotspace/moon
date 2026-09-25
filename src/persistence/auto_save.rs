//! Auto-save timer: triggers BGSAVE based on configured rules (N changes in M seconds).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::runtime::cancel::CancellationToken;
use tracing::info;

use crate::command::persistence::{
    BGSAVE_LAST_STATUS, SAVE_IN_PROGRESS, bgsave_start, bgsave_start_sharded,
};
use crate::storage::Database;

/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Parse save rules from config string.
///
/// Format: "seconds changes [seconds changes ...]"
/// Example: "900 1 300 10" -> [(900, 1), (300, 10)]
///
/// Returns empty vec for None or invalid input.
pub fn parse_save_rules(save_arg: &Option<String>) -> Vec<(u64, u64)> {
    let Some(s) = save_arg else {
        return vec![];
    };

    let parts: Vec<&str> = s.split_whitespace().collect();
    if !parts.len().is_multiple_of(2) {
        return vec![];
    }

    let mut rules = Vec::new();
    for pair in parts.chunks(2) {
        if let (Ok(secs), Ok(changes)) = (pair[0].parse::<u64>(), pair[1].parse::<u64>()) {
            rules.push((secs, changes));
        }
    }
    rules
}

/// Background auto-save task that triggers BGSAVE based on configured rules.
///
/// Checks every second whether any rule's conditions are met:
/// - Elapsed time >= rule seconds
/// - Changes count >= rule threshold
///
/// Uses the same SAVE_IN_PROGRESS guard as BGSAVE to prevent concurrent saves.
pub async fn run_auto_save(
    db: SharedDatabases,
    rules: Vec<(u64, u64)>,
    dir: String,
    dbfilename: String,
    change_counter: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    #[cfg(feature = "runtime-tokio")]
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut last_save = Instant::now();

    loop {
        #[cfg(feature = "runtime-tokio")]
        tokio::select! {
            _ = interval.tick() => {
                let elapsed = last_save.elapsed().as_secs();
                let changes = change_counter.load(Ordering::Relaxed);

                // Check if any rule triggers
                let should_save = rules.iter().any(|&(secs, threshold)| {
                    elapsed >= secs && changes >= threshold
                });

                if should_save && !SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
                    info!("Auto-save triggered: {} changes in {}s", changes, elapsed);
                    let _ = bgsave_start(db.clone(), dir.clone(), dbfilename.clone());
                    change_counter.store(0, Ordering::Relaxed);
                    last_save = Instant::now();
                }
            }
            _ = cancel.cancelled() => {
                info!("Auto-save task shutting down");
                break;
            }
        }

        #[cfg(feature = "runtime-monoio")]
        {
            use crate::runtime::{TimerImpl, traits::RuntimeTimer};
            let sleep = TimerImpl::sleep(std::time::Duration::from_secs(1));
            sleep.await;
            let elapsed = last_save.elapsed().as_secs();
            let changes = change_counter.load(Ordering::Relaxed);
            let should_save = rules
                .iter()
                .any(|&(secs, threshold)| elapsed >= secs && changes >= threshold);
            if should_save && !SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
                info!("Auto-save triggered: {} changes in {}s", changes, elapsed);
                let _ = bgsave_start(db.clone(), dir.clone(), dbfilename.clone());
                change_counter.store(0, Ordering::Relaxed);
                last_save = Instant::now();
            }
            if cancel.is_cancelled() {
                info!("Auto-save task shutting down");
                break;
            }
        }
    }
}

/// Start a sharded auto-save through the same entry point BGSAVE uses
/// ([`bgsave_start_sharded`]). `false` when a save is already running.
pub(crate) fn start_counted_auto_save(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
) -> bool {
    matches!(
        bgsave_start_sharded(snapshot_trigger, crate::command::connection::shard_count()),
        crate::protocol::Frame::SimpleString(_)
    )
}

/// Redis `CONFIG_BGSAVE_RETRY_DELAY`: after a failed background save, a save
/// rule starts the next one only this long after the previous attempt, so a
/// persistently failing disk is retried every few seconds, not every tick.
pub const BGSAVE_RETRY_DELAY_SECS: u64 = 5;

/// Whether the sharded auto-save should start a save now (moon#1232). Pure,
/// so both runtime arms share it and it is unit tested without a thread.
///
/// - `changes`: keyspace changes since the last successful save — the number
///   INFO reports as `rdb_changes_since_last_save`.
/// - `since_last_attempt_secs`: seconds since this task last started a save
///   (or started).
/// - `last_save_ok`: `rdb_last_bgsave_status`.
///
/// A rule `(secs, changes)` holds when both are reached, as in redis
/// (`serverCron`); after a failed save redis also waits
/// `CONFIG_BGSAVE_RETRY_DELAY` since the last attempt.
#[must_use]
pub fn save_rule_due(
    rules: &[(u64, u64)],
    since_last_attempt_secs: u64,
    changes: u64,
    last_save_ok: bool,
) -> bool {
    let rule_holds = rules
        .iter()
        .any(|&(secs, threshold)| since_last_attempt_secs >= secs && changes >= threshold);
    rule_holds && (last_save_ok || since_last_attempt_secs > BGSAVE_RETRY_DELAY_SECS)
}

/// One tick of [`run_auto_save_sharded`]: start a counted save if a rule is
/// due. `last_attempt` moves only when a save actually started.
fn auto_save_tick(
    rules: &[(u64, u64)],
    last_attempt: &mut Instant,
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
) {
    // moon#1232: the per-shard dirty counts summed on read (padded
    // per-thread slots, one relaxed add per write at the storage funnels —
    // `admin::metrics_setup`), minus their value at the last successful
    // save. It is the number INFO shows, and it moves back only when a save
    // SUCCEEDS (`mark_save_completed`), so nothing here resets it.
    let changes = crate::admin::metrics_setup::rdb_changes_since_last_save();
    let elapsed = last_attempt.elapsed().as_secs();
    let last_ok = BGSAVE_LAST_STATUS.load(Ordering::SeqCst);
    if save_rule_due(rules, elapsed, changes, last_ok)
        && !SAVE_IN_PROGRESS.load(Ordering::SeqCst)
        && start_counted_auto_save(snapshot_trigger)
    {
        info!("Auto-save triggered: {} changes in {}s", changes, elapsed);
        *last_attempt = Instant::now();
    }
}

/// Background auto-save task for sharded mode.
///
/// Instead of calling `bgsave_start` (which clones data under locks), this
/// starts the same cooperative per-shard save BGSAVE does: each shard's event
/// loop picks the epoch up from the watch channel and snapshots. Going
/// through that entry point (moon#1230) is what makes an auto-save a COUNTED
/// save: before, it bumped the epoch itself without arming the per-shard
/// fan-in, so its completions arrived at a zero counter and were dropped —
/// `LASTSAVE` / `rdb_last_save_time` never moved under auto-save, and one
/// failed auto-save latched `rdb_last_bgsave_status:err` for good. It also
/// sets `SAVE_IN_PROGRESS`, so a BGSAVE issued while an auto-save runs is
/// refused, as in redis.
///
/// moon#1232: the change count is `rdb_changes_since_last_save` (see
/// [`auto_save_tick`]). It used to be an `Arc<AtomicU64>` that only the
/// legacy single-listener handler ever incremented, so in the shipped
/// sharded server no `--save` rule with a change threshold ever fired.
pub async fn run_auto_save_sharded(
    rules: Vec<(u64, u64)>,
    cancel: CancellationToken,
    snapshot_trigger: crate::runtime::channel::WatchSender<u64>,
) {
    #[cfg(feature = "runtime-tokio")]
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    let mut last_attempt = Instant::now();

    loop {
        #[cfg(feature = "runtime-tokio")]
        tokio::select! {
            _ = interval.tick() => {
                auto_save_tick(&rules, &mut last_attempt, &snapshot_trigger);
            }
            _ = cancel.cancelled() => {
                info!("Auto-save task shutting down");
                break;
            }
        }

        #[cfg(feature = "runtime-monoio")]
        {
            use crate::runtime::{TimerImpl, traits::RuntimeTimer};
            let sleep = TimerImpl::sleep(std::time::Duration::from_secs(1));
            sleep.await;
            auto_save_tick(&rules, &mut last_attempt, &snapshot_trigger);
            if cancel.is_cancelled() {
                info!("Auto-save task shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_save_rules_standard() {
        let input = Some("900 1 300 10".to_string());
        let rules = parse_save_rules(&input);
        assert_eq!(rules, vec![(900, 1), (300, 10)]);
    }

    #[test]
    fn test_parse_save_rules_single() {
        let input = Some("3600 1".to_string());
        let rules = parse_save_rules(&input);
        assert_eq!(rules, vec![(3600, 1)]);
    }

    #[test]
    fn test_parse_save_rules_none() {
        let rules = parse_save_rules(&None);
        assert!(rules.is_empty());
    }

    #[test]
    fn test_parse_save_rules_empty_string() {
        let input = Some("".to_string());
        let rules = parse_save_rules(&input);
        assert!(rules.is_empty());
    }

    #[test]
    fn test_parse_save_rules_odd_count() {
        let input = Some("900 1 300".to_string());
        let rules = parse_save_rules(&input);
        assert!(rules.is_empty());
    }

    #[test]
    fn test_parse_save_rules_three_pairs() {
        let input = Some("900 1 300 10 60 10000".to_string());
        let rules = parse_save_rules(&input);
        assert_eq!(rules, vec![(900, 1), (300, 10), (60, 10000)]);
    }

    /// moon#1232: a rule holds when its time AND its change count are both
    /// reached; nine changes do not fire a "1 10" rule, ten do.
    #[test]
    fn a_rule_fires_on_its_change_count_and_its_time() {
        let rules = [(1, 10)];
        assert!(!save_rule_due(&rules, 1, 9, true), "9 changes < 10");
        assert!(save_rule_due(&rules, 1, 10, true));
        assert!(
            !save_rule_due(&rules, 0, 10, true),
            "the second has not passed"
        );
        assert!(!save_rule_due(&[], 100, 100, true), "no rule, no save");
        // Any rule of several.
        let rules = [(900, 1), (300, 10), (60, 10_000)];
        assert!(!save_rule_due(&rules, 299, 9_999, true));
        assert!(save_rule_due(&rules, 300, 10, true));
        assert!(save_rule_due(&rules, 900, 1, true));
    }

    /// After a failed save a rule waits `CONFIG_BGSAVE_RETRY_DELAY` since the
    /// last attempt, as redis does, instead of retrying every tick.
    #[test]
    fn a_failed_save_is_retried_after_the_redis_delay() {
        let rules = [(1, 1)];
        assert!(!save_rule_due(&rules, 1, 5, false));
        assert!(!save_rule_due(&rules, BGSAVE_RETRY_DELAY_SECS, 5, false));
        assert!(save_rule_due(&rules, BGSAVE_RETRY_DELAY_SECS + 1, 5, false));
    }
}
