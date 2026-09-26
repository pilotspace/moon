//! Which hot-keyspace sources a boot's per-shard recovery loads (moon#1267
//! review F3).

use std::path::Path;

use tracing::info;

/// What `Shard::restore_from_persistence` — the v3 disk-offload path and the
/// legacy v2 path alike — loads into the hot keyspace. Recovery that is not
/// KV history runs in every case: the offload manifest and the cold index,
/// warm vector segments, FPI torn-page repair, the WAL's `last_lsn`, CLOG
/// rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvSources {
    /// The snapshot, then the KV logs over it: the WAL v3 KV records and the
    /// legacy `appendonly.aof` (else the legacy-mode WAL v3 last resort).
    /// `--appendonly yes` with no multi-part AOF authority.
    SnapshotAndLogs,
    /// The snapshot only: `--appendonly no`. Nothing in that mode writes a
    /// KV log (no AOF writer, no WAL), so one on disk was left by an earlier
    /// `--appendonly yes` run (or a redis dir) and is OLDER than a snapshot
    /// this mode saved: replayed over it, it reverted keys. redis with
    /// `appendonly no` loads `dump.rdb` and ignores the AOF.
    SnapshotOnly,
    /// Nothing: the caller wipes every database and replays the multi-part
    /// AOF right after this pass (main.rs), so no hot load here survives.
    Elsewhere,
}

impl KvSources {
    /// The sources for a boot. `kv_authority_elsewhere` (the multi-part AOF
    /// is replayed after this pass) wins; otherwise `appendonly` decides.
    pub fn for_boot(kv_authority_elsewhere: bool, appendonly: bool) -> Self {
        match (kv_authority_elsewhere, appendonly) {
            (true, _) => Self::Elsewhere,
            (false, true) => Self::SnapshotAndLogs,
            (false, false) => Self::SnapshotOnly,
        }
    }

    /// Load the snapshot.
    pub fn snapshot(self) -> bool {
        self != Self::Elsewhere
    }

    /// Replay the KV logs over it.
    pub fn logs(self) -> bool {
        self == Self::SnapshotAndLogs
    }

    /// Why the KV logs are not replayed, for the boot log.
    pub fn why_no_logs(self) -> &'static str {
        match self {
            Self::Elsewhere => {
                "the multi-part AOF is the KV authority and is replayed after this pass"
            }
            _ => "--appendonly no writes no KV log, so one on disk predates the snapshot",
        }
    }

    /// Say so when a legacy `appendonly.aof` is left unreplayed because of
    /// `--appendonly no` (the operator may expect it to load, as it did).
    pub fn note_unreplayed_aof(self, shard_id: usize, aof: &Path) {
        if self == Self::SnapshotOnly && aof.exists() {
            info!(
                "Shard {shard_id}: {} not replayed — {}",
                aof.display(),
                self.why_no_logs()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appendonly_no_loads_the_snapshot_and_no_log() {
        let kv = KvSources::for_boot(false, false);
        assert_eq!(kv, KvSources::SnapshotOnly);
        assert!(kv.snapshot() && !kv.logs());
        let kv = KvSources::for_boot(false, true);
        assert!(kv.snapshot() && kv.logs());
        let kv = KvSources::for_boot(true, true);
        assert!(!kv.snapshot() && !kv.logs());
    }
}
