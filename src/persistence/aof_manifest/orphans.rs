//! The AOF orphan sweep, and the one load that runs it (moon#1271).
//!
//! Split out of the `aof_manifest` parent module to keep it under the
//! 1500-line cap. These are inherent `AofManifest` methods; `use super::*`
//! brings the type, its private fields and the std/tracing imports into
//! scope.
//!
//! The sweep deletes every `moon.aof.*` file whose sequence is not the
//! committed one, in every shard's directory. Run while a rewrite is in
//! flight, it deletes that rewrite's `seq + 1` files: a temp base another
//! shard is still writing (the rewrite aborts on the rename), or a base and
//! incr it has already published ahead of the manifest commit (the commit
//! then names files that are gone). So it runs only at boot, in main.rs's
//! recovery load, before any AOF writer or rewrite exists;
//! [`AofManifest::load`] itself deletes nothing.

use super::*;

impl AofManifest {
    /// [`Self::load`], then the best-effort orphan sweep: delete the stray
    /// base/incr/tmp files of aborted rewrites. A crash between advance()
    /// steps 1-3 leaves a new base RDB on disk that the active manifest never
    /// references; without the sweep, repeated crashes during rewrite can fill
    /// the disk with zombie files.
    ///
    /// **Boot only**: see the module doc. A rewrite that aborts at runtime
    /// leaves at most one `seq + 1` generation behind (its seq does not
    /// advance), which the next attempt at the same `seq + 1` overwrites and
    /// the next boot sweeps.
    ///
    /// Safe on the committed files: `parse_*` verified the manifest has all
    /// the required records, so the sweep never deletes the active files.
    pub fn load_and_sweep_orphans(dir: &Path) -> std::io::Result<Option<Self>> {
        let manifest = Self::load(dir)?;
        if let Some(ref m) = manifest {
            m.cleanup_orphans();
        }
        Ok(manifest)
    }

    /// Delete any base/incr files in `appendonlydir/` that do not match the
    /// current sequence. Best-effort — logs but does not propagate errors.
    ///
    /// For `PerShard` layout, also recurses into every `shard-N/` subdirectory
    /// and removes stale/tmp files there. Aborted BGREWRITEAOF runs leave
    /// `.rdb.tmp` files in the shard subdirs that otherwise accumulate forever.
    fn cleanup_orphans(&self) {
        match self.layout {
            AofLayout::TopLevel => {
                self.cleanup_orphans_dir(&self.aof_dir(), self.seq);
            }
            AofLayout::PerShard => {
                // Top-level appendonlydir/ holds only the manifest — no data files
                // to clean up there. All data lives in shard-N/ subdirs.
                for shard in &self.shards {
                    self.cleanup_orphans_shard(shard.shard_id);
                }
            }
        }
    }

    /// Scan a single shard's directory for orphan base/incr/tmp files that do
    /// not correspond to the current manifest sequence. Best-effort.
    fn cleanup_orphans_shard(&self, shard_id: u16) {
        self.cleanup_orphans_dir(&self.shard_dir(shard_id), self.seq);
    }

    /// Core orphan sweep: scan `dir` and remove any `moon.aof.*` files whose
    /// sequence is not `keep_seq`. Skips the manifest file itself.
    fn cleanup_orphans_dir(&self, dir: &Path, keep_seq: u64) {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        let current_base = format!("moon.aof.{}.base.rdb", keep_seq);
        let current_incr = format!("moon.aof.{}.incr.aof", keep_seq);
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = match name.to_str() {
                Some(s) => s,
                None => continue,
            };
            // Keep manifest, current base, current incr. Delete any other moon.aof.*.
            if name_str == MANIFEST_NAME || name_str == current_base || name_str == current_incr {
                continue;
            }
            let is_moon_aof = name_str.starts_with("moon.aof.")
                && (name_str.ends_with(".base.rdb")
                    || name_str.ends_with(".incr.aof")
                    || name_str.ends_with(".rdb.tmp")
                    || name_str.ends_with(".tmp"));
            if !is_moon_aof {
                continue;
            }
            let path = entry.path();
            match std::fs::remove_file(&path) {
                Ok(()) => info!("AOF orphan cleanup: removed {}", path.display()),
                Err(e) => warn!(
                    "AOF orphan cleanup: failed to remove {}: {}",
                    path.display(),
                    e
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `seq + 1` files of a per-shard rewrite in flight: shard 0 is still
    /// writing its temp base, shard 1 has already published its base and incr
    /// (renamed, ahead of the manifest commit).
    fn rewrite_in_flight(dir: &Path) -> (AofManifest, PathBuf, u64) {
        let mut m = AofManifest::initialize_multi(dir, 2).expect("initialize_multi");
        let bytes = crate::persistence::rdb::save_to_bytes(&[] as &[crate::storage::Database])
            .expect("empty rdb");
        let tmp0 = m.shard_base_staging(0, 2).expect("staging 0");
        std::fs::write(&tmp0, &bytes).expect("write shard 0's temp base");
        m.advance_shard(1, 2, &bytes)
            .expect("shard 1 publishes seq 2");
        (m, tmp0, bytes.len() as u64)
    }

    /// moon#1271: a per-shard AOF writer that starts late loads the manifest
    /// while the others are mid-rewrite. That load must leave the rewrite's
    /// files alone, so the rewrite still commits. Before the fix it deleted
    /// them: shard 0's publish failed with the issue's error, "advance_shard
    /// 0: rename base …/shard-0/moon.aof.2.base.rdb.tmp: No such file or
    /// directory", and shard 1's published generation was gone.
    #[test]
    fn a_manifest_load_leaves_a_rewrite_in_flight_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut m, tmp0, len) = rewrite_in_flight(dir.path());

        // The late writer's startup load (or the dispatch's, or the size
        // monitor's): it sees the committed seq 1.
        let loaded = AofManifest::load(dir.path())
            .expect("load")
            .expect("manifest present");
        assert_eq!(loaded.seq, 1);

        assert!(tmp0.exists(), "the load deleted shard 0's temp base");
        for f in [m.shard_base_path_seq(1, 2), m.shard_incr_path_seq(1, 2)] {
            assert!(f.exists(), "the load deleted shard 1's published {f:?}");
        }
        m.advance_shard_staged(0, 2, &tmp0, len)
            .expect("shard 0 publishes its base after the load");
        m.seq = 2;
        m.write_manifest().expect("commit seq 2");
        let committed = AofManifest::load(dir.path())
            .expect("load")
            .expect("present");
        for shard in 0..2 {
            assert!(
                committed.shard_base_path(shard).exists(),
                "shard {shard} base"
            );
            assert!(
                committed.shard_incr_path(shard).exists(),
                "shard {shard} incr"
            );
        }
    }

    /// The boot sweep keeps its job: an aborted rewrite's `seq + 1` files,
    /// never committed, are gone after [`AofManifest::load_and_sweep_orphans`],
    /// and the committed generation is untouched.
    #[test]
    fn the_boot_load_sweeps_an_aborted_rewrite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (m, tmp0, _) = rewrite_in_flight(dir.path());
        let swept = AofManifest::load_and_sweep_orphans(dir.path())
            .expect("load")
            .expect("manifest present");
        assert_eq!(swept.seq, 1);
        assert!(
            !tmp0.exists(),
            "an aborted temp base survived the boot sweep"
        );
        assert!(!m.shard_base_path_seq(1, 2).exists());
        assert!(!m.shard_incr_path_seq(1, 2).exists());
        for shard in 0..2 {
            assert!(swept.shard_base_path(shard).exists(), "shard {shard} base");
            assert!(swept.shard_incr_path(shard).exists(), "shard {shard} incr");
        }
    }
}
