//! Per-shard manifest rewrite helpers: `initialize_multi` /
//! `try_initialize_multi` and `advance_shard` / `prune_shard_files`.
//!
//! Split out of the `aof_manifest` parent module (issue #143) to keep each file
//! under the 1500-line cap. These are inherent `AofManifest` methods; `use
//! super::*` brings the type, its private fields (a child module sees parent
//! privates), and the std/tracing imports into scope.

use super::*;

impl AofManifest {
    /// Create the `appendonlydir/` and write an initial v2 manifest for the
    /// given shard count.
    ///
    /// Each shard gets its own `shard-{N}/` subdirectory with an empty base
    /// RDB and an empty incr file. Mirrors `initialize()` semantics: the
    /// `(base + incr)` invariant holds from the first boot, so recovery can
    /// replay incr-only state without complaint.
    ///
    /// **Idempotency pre-flight:** if `appendonlydir/moon.aof.manifest` already
    /// exists, returns `Err(AlreadyExists)` without modifying any files. A
    /// mid-loop crash followed by a retry would otherwise overwrite the already-
    /// written shard-0 base RDB with an empty RDB, losing state. Callers that
    /// want resume-or-skip semantics should use [`Self::try_initialize_multi`].
    ///
    /// **Rollback on partial failure:** if the per-shard loop fails mid-way (e.g.
    /// shard-1 write fails after shard-0 succeeded), all already-created shard
    /// base RDB files are deleted before returning the error.
    pub fn initialize_multi(dir: &Path, num_shards: u16) -> std::io::Result<Self> {
        if num_shards == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "initialize_multi requires num_shards >= 1",
            ));
        }
        let manifest = Self {
            dir: dir.to_path_buf(),
            seq: 1,
            layout: AofLayout::PerShard,
            shards: (0..num_shards)
                .map(|id| ShardManifest {
                    shard_id: id,
                    max_lsn: 0,
                })
                .collect(),
        };
        std::fs::create_dir_all(manifest.aof_dir())?;

        // Pre-flight: refuse if manifest already exists to avoid overwriting
        // already-written shard base RDB files (idempotency guard).
        let manifest_path = manifest.manifest_path();
        if manifest_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!(
                    "initialize_multi: manifest already exists at {}; \
                     use try_initialize_multi() for idempotent initialization",
                    manifest_path.display()
                ),
            ));
        }

        // Per-shard empty RDB. Single Database::default() inside a 1-element
        // slice matches `initialize()`'s empty-RDB shape for each shard.
        let empty_dbs: [crate::storage::Database; 0] = [];
        let empty_rdb = crate::persistence::rdb::save_to_bytes(&empty_dbs)
            .map_err(|e| std::io::Error::other(format!("empty RDB serialize: {e}")))?;

        // Track which shard directories were successfully created so we can
        // roll them back on partial failure.
        let mut created_shards: Vec<u16> = Vec::with_capacity(num_shards as usize);

        let loop_result = (|| -> std::io::Result<()> {
            for shard_id in 0..num_shards {
                let shard_dir = manifest.shard_dir(shard_id);
                std::fs::create_dir_all(&shard_dir)?;

                let base_path = manifest.shard_base_path(shard_id);
                let tmp_path = base_path.with_extension("rdb.tmp");
                {
                    let mut f = std::fs::File::create(&tmp_path)?;
                    f.write_all(&empty_rdb)?;
                    f.sync_data()?;
                }
                std::fs::rename(&tmp_path, &base_path)?;
                fsync_parent_best_effort(&base_path);
                std::fs::File::create(manifest.shard_incr_path(shard_id))?;
                created_shards.push(shard_id);
            }
            Ok(())
        })();

        if let Err(e) = loop_result {
            // Rollback: remove base RDB files for all successfully-created shards.
            for sid in created_shards {
                let base = manifest.shard_base_path(sid);
                if let Err(re) = std::fs::remove_file(&base) {
                    warn!(
                        "initialize_multi rollback: failed to remove {}: {}",
                        base.display(),
                        re
                    );
                }
            }
            return Err(e);
        }

        manifest.write_manifest()?;
        Ok(manifest)
    }
    /// Initialize a v2 multi-shard manifest only if one does not already exist.
    ///
    /// Returns `Ok(Some(manifest))` on successful creation, or `Ok(None)` if the
    /// manifest file already existed (already initialized — no files modified).
    /// Returns `Err(_)` only on actual I/O failures.
    pub fn try_initialize_multi(dir: &Path, num_shards: u16) -> std::io::Result<Option<Self>> {
        match Self::initialize_multi(dir, num_shards) {
            Ok(m) => Ok(Some(m)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Advance a single shard to a new sequence: write the shard's new base RDB,
    /// create a new empty incr file, then update the shard's `max_lsn` in the
    /// in-memory manifest.
    ///
    /// **Does NOT delete the old generation's files.** Deletion is deferred to
    /// the coordinator via [`prune_shard_files`](Self::prune_shard_files),
    /// called only AFTER `write_manifest()` durably commits the new seq.
    /// Deleting before the commit would leave a crash window where the
    /// persisted (old) seq points at files that are already gone — recovery
    /// resolves base/incr by `self.seq`, so a crash mid-fan-out would lose data
    /// for any shard that had already advanced. This matches the post-commit
    /// deletion ordering in [`advance`](Self::advance) (TopLevel layout).
    ///
    /// **Caller MUST call `write_manifest()` after all shards have been advanced**
    /// (and set `self.seq` to the new seq) to persist the updated manifest
    /// atomically — this is the single durable commit point for the rewrite.
    /// Advancing shards one at a time and writing the manifest per-shard would
    /// leave the manifest in an inconsistent state between calls.
    ///
    /// For `TopLevel` layout, `shard_id` must be 0 and this delegates to
    /// `advance()` (which deletes post-commit internally). For `PerShard`
    /// layout, files are written to `shard_dir(shard_id)/`.
    ///
    /// Returns the path to the new incremental file for this shard.
    pub fn advance_shard(
        &mut self,
        shard_id: u16,
        new_seq: u64,
        rdb_bytes: &[u8],
    ) -> Result<PathBuf, crate::error::MoonError> {
        if self.layout == AofLayout::TopLevel {
            debug_assert_eq!(shard_id, 0, "TopLevel layout only has shard 0");
            return self.advance(rdb_bytes);
        }

        // Validate shard_id is known in this manifest.
        let shard_idx = self
            .shards
            .iter()
            .position(|s| s.shard_id == shard_id)
            .ok_or_else(|| crate::error::AofError::RewriteFailed {
                detail: format!(
                    "advance_shard: shard_id {} not in manifest (shards: {})",
                    shard_id,
                    self.shards.len()
                ),
            })?;

        let shard_dir = self.shard_dir(shard_id);
        std::fs::create_dir_all(&shard_dir).map_err(|e| crate::error::AofError::Io {
            path: shard_dir.clone(),
            source: e,
        })?;

        // 1. Write new base RDB atomically: tmp + fsync + rename.
        let new_base = self.shard_base_path_seq(shard_id, new_seq);
        let tmp_base = new_base.with_extension("rdb.tmp");
        {
            let mut f =
                std::fs::File::create(&tmp_base).map_err(|e| crate::error::AofError::Io {
                    path: tmp_base.clone(),
                    source: e,
                })?;
            f.write_all(rdb_bytes)
                .map_err(|e| crate::error::AofError::Io {
                    path: tmp_base.clone(),
                    source: e,
                })?;
            f.sync_data().map_err(|e| crate::error::AofError::Io {
                path: tmp_base.clone(),
                source: e,
            })?;
        }
        std::fs::rename(&tmp_base, &new_base).map_err(|e| {
            crate::error::AofError::RewriteFailed {
                detail: format!(
                    "advance_shard {}: rename base {}: {}",
                    shard_id,
                    tmp_base.display(),
                    e
                ),
            }
        })?;
        fsync_parent_best_effort(&new_base);

        // 2. Create empty new incremental file.
        let new_incr = self.shard_incr_path_seq(shard_id, new_seq);
        std::fs::File::create(&new_incr).map_err(|e| crate::error::AofError::Io {
            path: new_incr.clone(),
            source: e,
        })?;

        // 3. Update per-shard LSN in-memory (manifest write is the caller's job).
        //    Old-generation files are intentionally NOT deleted here — the
        //    coordinator prunes them via `prune_shard_files` only after
        //    `write_manifest()` durably commits the new seq (see the fn doc;
        //    delete-before-commit would lose data on a mid-fan-out crash).
        self.shards[shard_idx].max_lsn = self.shards[shard_idx].max_lsn.max(new_seq);

        info!(
            "AOF shard {} advanced to seq {}: base={} bytes, incr={}",
            shard_id,
            new_seq,
            rdb_bytes.len(),
            new_incr.display()
        );

        Ok(new_incr)
    }
    /// Delete a shard's base + incr files for a specific `seq`. Best-effort.
    ///
    /// **Crash-safety contract:** the rewrite coordinator MUST call this only
    /// AFTER `write_manifest()` has durably committed the new seq. Deleting an
    /// old generation's files before the manifest flips would orphan the
    /// persisted (old) seq whose files are already gone — recovery resolves
    /// base/incr by `self.seq`, so it would read a missing base and lose data
    /// for any shard that completed before the crash. This mirrors the
    /// post-commit deletion ordering in `advance()` (TopLevel layout).
    pub fn prune_shard_files(&self, shard_id: u16, seq: u64) {
        let base = self.shard_base_path_seq(shard_id, seq);
        let incr = self.shard_incr_path_seq(shard_id, seq);
        if base.exists() {
            if let Err(e) = std::fs::remove_file(&base) {
                warn!(
                    "prune_shard_files {}: failed to delete old base {}: {}",
                    shard_id,
                    base.display(),
                    e
                );
            }
        }
        if incr.exists() {
            if let Err(e) = std::fs::remove_file(&incr) {
                warn!(
                    "prune_shard_files {}: failed to delete old incr {}: {}",
                    shard_id,
                    incr.display(),
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        // Use a global atomic counter so parallel test threads (cargo test runs
        // unit tests in parallel) never produce the same directory name even
        // when PID and nanosecond clock resolution are the same for two threads.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!(
            "moon-aof-manifest-test-rewrite-{}-{}",
            std::process::id(),
            n,
        ));
        fs::create_dir_all(&d).expect("temp dir create");
        d
    }

    /// Build a minimal PerShard manifest fixture on disk at `seq` without
    /// needing `advance_shard`. Directly creates the expected directory layout
    /// so the test is self-contained and doesn't depend on FIX-W2-3 methods.
    fn write_per_shard_manifest_at_seq(dir: &Path, num_shards: u16, seq: u64) -> AofManifest {
        let aof_dir = dir.join(AOF_DIR_NAME);
        fs::create_dir_all(&aof_dir).unwrap();
        let empty_rdb = crate::persistence::rdb::save_to_bytes(&[] as &[crate::storage::Database])
            .expect("empty rdb");
        let shards: Vec<ShardManifest> = (0..num_shards)
            .map(|id| ShardManifest {
                shard_id: id,
                max_lsn: 0,
            })
            .collect();
        let manifest = AofManifest {
            dir: dir.to_path_buf(),
            seq,
            layout: AofLayout::PerShard,
            shards,
        };
        for shard_id in 0..num_shards {
            let shard_dir = manifest.shard_dir(shard_id);
            fs::create_dir_all(&shard_dir).unwrap();
            let base = manifest.shard_base_path(shard_id);
            let tmp = base.with_extension("rdb.tmp");
            fs::write(&tmp, &empty_rdb).unwrap();
            fs::rename(&tmp, &base).unwrap();
            fs::write(manifest.shard_incr_path(shard_id), b"").unwrap();
        }
        manifest.write_manifest().unwrap();
        manifest
    }

    #[test]
    fn cleanup_orphans_removes_stale_files_in_shard_subdirs() {
        let dir = temp_dir();

        // Build a 2-shard PerShard manifest at seq=2.
        let manifest = write_per_shard_manifest_at_seq(&dir, 2, 2);

        // Inject orphan files in shard-0/ that a crashed BGREWRITEAOF would leave.
        // seq=1 tmp (aborted write) and a seq=5 incr (future zombie).
        let shard0_dir = manifest.shard_dir(0);
        let orphan_tmp = shard0_dir.join("moon.aof.1.base.rdb.tmp");
        let orphan_old_incr = shard0_dir.join("moon.aof.5.incr.aof");
        fs::write(&orphan_tmp, b"").expect("write orphan tmp");
        fs::write(&orphan_old_incr, b"").expect("write orphan incr");

        // Active files for seq=2 must survive.
        let active_base = manifest.shard_base_path(0);
        let active_incr = manifest.shard_incr_path(0);
        assert!(
            active_base.exists(),
            "active base must exist before cleanup"
        );
        assert!(
            active_incr.exists(),
            "active incr must exist before cleanup"
        );

        // Reload the manifest — this triggers cleanup_orphans.
        let _reloaded = AofManifest::load(&dir).expect("load").expect("present");

        assert!(
            !orphan_tmp.exists(),
            "orphan .rdb.tmp in shard-0/ must be deleted by cleanup_orphans"
        );
        assert!(
            !orphan_old_incr.exists(),
            "orphan old incr in shard-0/ must be deleted by cleanup_orphans"
        );
        assert!(
            active_base.exists(),
            "active seq=2 base must survive cleanup"
        );
        assert!(
            active_incr.exists(),
            "active seq=2 incr must survive cleanup"
        );

        fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------
    // FIX-W2-2: initialize_multi idempotency — second call returns error
    // -----------------------------------------------------------------------
    #[test]
    fn initialize_multi_second_call_returns_already_initialized_error() {
        let dir = temp_dir();

        // First call must succeed.
        let _m = AofManifest::initialize_multi(&dir, 4).expect("first call ok");

        // Count files before second call.
        let aof_dir = dir.join(AOF_DIR_NAME);
        let count_before: usize = (0..4u16)
            .map(|sid| {
                let shard_dir = aof_dir.join(format!("shard-{}", sid));
                fs::read_dir(&shard_dir).map(|e| e.count()).unwrap_or(0)
            })
            .sum();

        // Second call must return an error with the manifest already present.
        let result = AofManifest::initialize_multi(&dir, 4);
        assert!(
            result.is_err(),
            "second initialize_multi must fail when manifest already exists"
        );
        let err = result.unwrap_err();
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::AlreadyExists,
            "error kind must be AlreadyExists; got {:?}: {}",
            err.kind(),
            err
        );

        // File count must be unchanged — no files were overwritten.
        let count_after: usize = (0..4u16)
            .map(|sid| {
                let shard_dir = aof_dir.join(format!("shard-{}", sid));
                fs::read_dir(&shard_dir).map(|e| e.count()).unwrap_or(0)
            })
            .sum();
        assert_eq!(
            count_before, count_after,
            "second call must not create or overwrite any shard files"
        );

        fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------
    // F6 crash-safety ordering: advance_shard writes new base+incr but MUST
    // NOT delete old files. Deleting before the manifest durably commits the
    // new seq leaves a window where a crash orphans the persisted (old) seq
    // whose files are already gone → recovery reads a missing base → data
    // loss for completed shards. Deletion is the coordinator's job, AFTER
    // write_manifest(), via prune_shard_files(). This mirrors the proven
    // ordering in advance() (TopLevel), which deletes only post-commit.
    // -----------------------------------------------------------------------
    #[test]
    fn advance_shard_defers_delete_until_after_commit() {
        let dir = temp_dir();

        // Initialize 2-shard manifest at seq=1.
        let mut manifest = AofManifest::initialize_multi(&dir, 2).expect("initialize_multi");
        assert_eq!(manifest.seq, 1);

        let empty_rdb = crate::persistence::rdb::save_to_bytes(&[] as &[crate::storage::Database])
            .expect("empty rdb");

        // Old shard files at seq=1 must exist before advance.
        let old_base_s0 = manifest.shard_base_path_seq(0, 1);
        let old_incr_s0 = manifest.shard_incr_path_seq(0, 1);
        let old_base_s1 = manifest.shard_base_path_seq(1, 1);
        let old_incr_s1 = manifest.shard_incr_path_seq(1, 1);
        assert!(old_base_s0.exists(), "seq=1 base must exist for shard 0");
        assert!(old_incr_s0.exists(), "seq=1 incr must exist for shard 0");

        // Fan out: coordinator picks new_seq=2 once and advances every shard
        // to it. No manifest write, no deletion, happens inside the fan-out.
        let new_incr_s0 = manifest
            .advance_shard(0, 2, &empty_rdb)
            .expect("advance_shard 0 → seq=2");
        let new_incr_s1 = manifest
            .advance_shard(1, 2, &empty_rdb)
            .expect("advance_shard 1 → seq=2");
        assert!(new_incr_s0.exists(), "new incr file must be created (s0)");
        assert!(new_incr_s1.exists(), "new incr file must be created (s1)");

        // PRE-COMMIT INVARIANT: new files written, OLD files NOT yet deleted.
        // This is the regression guard against delete-before-commit.
        assert!(
            manifest.shard_base_path_seq(0, 2).exists(),
            "new seq=2 base must exist for shard 0"
        );
        assert!(
            manifest.shard_base_path_seq(1, 2).exists(),
            "new seq=2 base must exist for shard 1"
        );
        assert!(
            old_base_s0.exists(),
            "old seq=1 base (s0) MUST survive until the manifest commits"
        );
        assert!(
            old_incr_s0.exists(),
            "old seq=1 incr (s0) MUST survive until the manifest commits"
        );
        assert!(
            old_base_s1.exists(),
            "old seq=1 base (s1) MUST survive until the manifest commits"
        );

        // COMMIT: coordinator bumps seq and persists the manifest atomically.
        // This is the single durable commit point for the whole rewrite.
        manifest.seq = 2;
        manifest
            .write_manifest()
            .expect("write manifest after advance");

        // POST-COMMIT: coordinator prunes old files — safe now that recovery
        // resolves base/incr by the durably-committed new seq.
        manifest.prune_shard_files(0, 1);
        manifest.prune_shard_files(1, 1);
        assert!(
            !old_base_s0.exists(),
            "old seq=1 base (s0) pruned post-commit"
        );
        assert!(
            !old_incr_s0.exists(),
            "old seq=1 incr (s0) pruned post-commit"
        );
        assert!(
            !old_base_s1.exists(),
            "old seq=1 base (s1) pruned post-commit"
        );
        assert!(
            !old_incr_s1.exists(),
            "old seq=1 incr (s1) pruned post-commit"
        );
        assert!(
            manifest.shard_base_path_seq(0, 2).exists(),
            "new seq=2 base (s0) must remain after prune"
        );

        // Recovery reads base by manifest.seq — must resolve to the new seq.
        let reloaded = AofManifest::load(&dir).expect("load").expect("present");
        assert_eq!(reloaded.seq, 2);

        fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------
    // FIX-W2-7: smoke test — fsync helper consolidation did not break
    // initialize_multi. Checks that the post-consolidation manifest has the
    // correct PerShard layout, the expected shard count, and per-shard
    // base/incr files. This is discriminating: a regression that produces a
    // TopLevel manifest or wrong shard count will be caught here.
    // -----------------------------------------------------------------------
    #[test]
    fn initialize_multi_smoke_after_fsync_consolidation() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        let n: u16 = 2;
        let result = AofManifest::initialize_multi(dir, n);
        assert!(
            result.is_ok(),
            "initialize_multi({n} shards) must succeed: {:?}",
            result.err()
        );
        let manifest = result.unwrap();

        // Discriminating: layout must be PerShard, not TopLevel.
        assert_eq!(
            manifest.layout,
            AofLayout::PerShard,
            "initialize_multi must produce a PerShard manifest"
        );
        // Discriminating: shard count must match the requested count.
        assert_eq!(
            manifest.shards.len() as u16,
            n,
            "manifest must record exactly {n} shards, got {}",
            manifest.shards.len()
        );
        // Discriminating: per-shard base RDB and incr files must exist on disk.
        for shard_id in 0..n {
            assert!(
                manifest.shard_base_path(shard_id).exists(),
                "shard-{shard_id} base RDB must exist at {}",
                manifest.shard_base_path(shard_id).display()
            );
            assert!(
                manifest.shard_incr_path(shard_id).exists(),
                "shard-{shard_id} incr file must exist at {}",
                manifest.shard_incr_path(shard_id).display()
            );
        }
        // Discriminating: the on-disk manifest file must contain `version 2`
        // (PerShard v2 header), not be a bare v1 file.
        let manifest_path = dir.join(AOF_DIR_NAME).join("moon.aof.manifest");
        let content =
            std::fs::read_to_string(&manifest_path).expect("manifest file must be readable");
        assert!(
            content.contains("version 2"),
            "manifest file must contain 'version 2' (PerShard v2 header); got:\n{}",
            content
        );
    }
}
