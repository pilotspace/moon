//! Which hot-keyspace sources a boot's per-shard recovery loads (moon#1267
//! review F3).

use std::path::Path;

use tracing::warn;

/// What `Shard::restore_from_persistence` — the v3 disk-offload path and the
/// legacy v2 path alike — loads into the hot keyspace. Recovery that is not
/// KV history runs in every case: the offload manifest and the cold index,
/// warm vector segments, FPI torn-page repair, the WAL's `last_lsn`, CLOG
/// rollback.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvSources {
    /// The snapshot, then the KV logs over it: the WAL v3 KV records, else
    /// the legacy-mode WAL v3 last resort. `--appendonly yes` with no
    /// multi-part AOF authority and no `appendonly.aof` holding a record
    /// ([`KvSources::with_flat_aof`] turns that into [`KvSources::AofOnly`]).
    SnapshotAndLogs,
    /// The legacy single-file `appendonly.aof` alone: `--appendonly yes`, no
    /// multi-part AOF authority, and the file holds at least one byte. It is
    /// the complete KV history of this dataset — written from the boot that
    /// opened it (whose keyspace, when not empty, it carries as an RDB
    /// preamble, `aof::fresh_generation`) or by a rewrite (an RDB preamble of
    /// the whole keyspace). A snapshot saved while it was being appended
    /// already holds a prefix of its records, so loading both applied that
    /// prefix twice: `RPUSH l a b; INCR c; BGSAVE; kill -9` came back as
    /// `l = a b a b`, `c = 2` (R2b review P1). redis with `appendonly yes`
    /// likewise loads the AOF and never `dump.rdb` (`loadDataFromDisk`). The
    /// WAL v3 KV records are skipped too: every write they carry is in the
    /// AOF, and replaying both double-applied in the same way.
    AofOnly,
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

    /// [`KvSources::SnapshotAndLogs`] becomes [`KvSources::AofOnly`] when
    /// `dir` holds an `appendonly.aof` with at least one byte — the file the
    /// legacy replay rungs read. An empty file holds no record (a generation
    /// whose head was never written), so the snapshot stays the base and the
    /// boot opens the generation over it. Every other value is returned as is.
    pub fn with_flat_aof(self, dir: Option<&Path>) -> Self {
        let holds_records = |d: &Path| {
            std::fs::metadata(d.join("appendonly.aof")).is_ok_and(|m| m.is_file() && m.len() > 0)
        };
        match (self, dir) {
            (Self::SnapshotAndLogs, Some(d)) if holds_records(d) => Self::AofOnly,
            (kv, _) => kv,
        }
    }

    /// Load the snapshot.
    pub fn snapshot(self) -> bool {
        matches!(self, Self::SnapshotAndLogs | Self::SnapshotOnly)
    }

    /// Run the legacy log rungs: `appendonly.aof`, else (no AOF at all) the
    /// legacy-mode WAL v3 last resort.
    pub fn logs(self) -> bool {
        matches!(self, Self::SnapshotAndLogs | Self::AofOnly)
    }

    /// Apply the disk-offload WAL v3's KV `Command` records.
    pub fn wal_kv(self) -> bool {
        self == Self::SnapshotAndLogs
    }

    /// This pass builds the hot keyspace (no wipe and replay follows it).
    pub fn authority_here(self) -> bool {
        self != Self::Elsewhere
    }

    /// Why the snapshot is not loaded, for the boot log.
    pub fn why_no_snapshot(self) -> &'static str {
        match self {
            Self::AofOnly => {
                "appendonly.aof is the KV authority (redis loads only the AOF under \
                 appendonly yes)"
            }
            _ => {
                "the multi-part AOF is the KV authority and is replayed after this pass \
                 (loading it here was discarded)"
            }
        }
    }

    /// Why the KV logs are not replayed, for the boot log.
    pub fn why_no_logs(self) -> &'static str {
        match self {
            Self::Elsewhere => {
                "the multi-part AOF is the KV authority and is replayed after this pass"
            }
            Self::AofOnly => "appendonly.aof is the KV authority and carries every one of them",
            _ => "--appendonly no writes no KV log, so one on disk predates the snapshot",
        }
    }

    /// Say so when `--appendonly no` leaves a KV log in `dir` unreplayed: a
    /// legacy `appendonly.aof`, or a multi-part `appendonlydir/` left by an
    /// `--appendonly yes` run (round 3, A5: switching `yes` → `no` without a
    /// snapshot boots EMPTY, and said nothing). Once, from shard 0.
    pub fn note_unreplayed_logs(self, shard_id: usize, dir: &Path) {
        if self != Self::SnapshotOnly || shard_id != 0 {
            return;
        }
        let multi_part = dir.join("appendonlydir").join("moon.aof.manifest");
        for log in [dir.join("appendonly.aof"), multi_part] {
            if log.exists() {
                warn!(
                    "{} is NOT loaded: {}. Switching --appendonly yes -> no? Run BGSAVE (or \
                     SHUTDOWN SAVE) under `yes` first, so the snapshot holds the dataset",
                    log.display(),
                    self.why_no_logs()
                );
            }
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

    /// R2b review P1: a non-empty `appendonly.aof` is the only KV source; an
    /// absent or empty one leaves the snapshot as the base. `--appendonly no`
    /// and the multi-part authority are never changed by it.
    #[test]
    fn a_flat_aof_with_records_is_the_only_kv_source() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let logs = KvSources::SnapshotAndLogs;
        assert_eq!(logs.with_flat_aof(Some(dir)), logs, "no AOF");
        assert_eq!(logs.with_flat_aof(None), logs, "no dir");
        std::fs::write(dir.join("appendonly.aof"), b"").unwrap();
        assert_eq!(logs.with_flat_aof(Some(dir)), logs, "empty AOF");
        std::fs::write(dir.join("appendonly.aof"), b"*1\r\n$4\r\nPING\r\n").unwrap();
        let kv = logs.with_flat_aof(Some(dir));
        assert_eq!(kv, KvSources::AofOnly);
        assert!(!kv.snapshot() && kv.logs() && !kv.wal_kv() && kv.authority_here());
        for other in [KvSources::SnapshotOnly, KvSources::Elsewhere] {
            assert_eq!(other.with_flat_aof(Some(dir)), other);
        }
    }
}
