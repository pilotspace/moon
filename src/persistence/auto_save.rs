//! Auto-save timer: triggers BGSAVE based on configured rules (N changes in M seconds).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crate::runtime::cancel::CancellationToken;
use tracing::info;

use crate::command::persistence::{SAVE_IN_PROGRESS, SNAPSHOT_EPOCH, bgsave_start};
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

/// Background auto-save task for sharded mode.
///
/// Instead of calling `bgsave_start` (which clones data under locks), this bumps
/// the snapshot epoch via a `tokio::sync::watch::Sender<u64>`. Each shard's event
/// loop subscribes to the watch channel and initiates a cooperative snapshot when
/// the epoch changes.
pub async fn run_auto_save_sharded(
    rules: Vec<(u64, u64)>,
    change_counter: Arc<AtomicU64>,
    cancel: CancellationToken,
    snapshot_trigger: crate::runtime::channel::WatchSender<u64>,
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

                let should_save = rules.iter().any(|&(secs, threshold)| {
                    elapsed >= secs && changes >= threshold
                });

                if should_save && !SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
                    let epoch = SNAPSHOT_EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
                    info!("Auto-save triggered: {} changes in {}s, epoch {}", changes, elapsed, epoch);
                    let _ = snapshot_trigger.send(epoch);
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
                let epoch = SNAPSHOT_EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
                info!(
                    "Auto-save triggered: {} changes in {}s, epoch {}",
                    changes, elapsed, epoch
                );
                let _ = snapshot_trigger.send(epoch);
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
}
