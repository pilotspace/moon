//! Boot-time creation of a fresh AOF generation, in an order no crash can
//! break (moon#1293).
//!
//! A generation a boot opens must start with its head — `MOON.COLDCUT
//! <watermark>` plus a `DEL` for every cold key the boot dropped on a no-AOF
//! snapshot's word (moon#902, moon#1281) — BEFORE its manifest is committed.
//! The manifest is the commit point: once it names the generation, the next
//! boot replays base + incr and skips the snapshot (and its graves trailer).
//! The heads used to be appended AFTER the commit, one shard at a time, so a
//! crash in between left a committed generation whose incr had no head: the
//! next boot replayed it ungated and re-indexed every dead cold slot, and the
//! cold keys deleted before the `--appendonly no` -> `yes` switch came back.
//!
//! [`AofManifest::prepare`] (and its `_with_base` / `_multi_with_bases`
//! twins) writes the bases and the empty incrs and returns an
//! [`UncommittedGeneration`]; its [`UncommittedGeneration::seed_generation_head`]
//! writes and fsyncs every head, and only [`UncommittedGeneration::commit`]
//! writes the manifest. A crash before the commit leaves no manifest, so the
//! next boot redoes the whole initialization from the snapshot it still has
//! (bases via tmp + rename, incrs truncated by `File::create`).

use super::*;

/// A fresh AOF generation whose base and incr files are on disk but whose
/// manifest is NOT written yet: nothing reads it as the KV authority until
/// [`Self::commit`].
#[derive(Debug)]
#[must_use = "a generation is not the AOF authority until `commit` writes its manifest"]
pub struct UncommittedGeneration {
    manifest: AofManifest,
}

impl UncommittedGeneration {
    pub(super) fn new(manifest: AofManifest) -> Self {
        Self { manifest }
    }

    /// The manifest [`Self::commit`] will write.
    pub fn manifest(&self) -> &AofManifest {
        &self.manifest
    }

    /// moon#902: write `MOON.COLDCUT <watermark>` as the head of every incr
    /// file of this generation. `watermark_for_shard` answers the shard's
    /// next cold file id — every cold file that already exists is below it
    /// and therefore a valid base for this generation.
    pub fn seed_cold_cut(&self, watermark_for_shard: impl Fn(u16) -> u64) -> std::io::Result<()> {
        self.seed_generation_head(watermark_for_shard, |_| {
            crate::persistence::cold_records::ColdDeletes::default()
        })
    }

    /// [`Self::seed_cold_cut`] with the `DEL`s each shard's head must carry
    /// (moon#1281 round 2: the dead spill slots a boot dropped on a no-AOF
    /// snapshot's word — `aof::fold_stream::fresh_generation_deletes`).
    ///
    /// Each head is fsynced, and so is its directory entry, before the next
    /// shard's is written — all of them before [`Self::commit`] (moon#1293),
    /// so no committed generation can lack its head, even after a power loss.
    pub fn seed_generation_head(
        &self,
        watermark_for_shard: impl Fn(u16) -> u64,
        mut deletes_for_shard: impl FnMut(u16) -> crate::persistence::cold_records::ColdDeletes,
    ) -> std::io::Result<()> {
        use crate::persistence::cold_records::write_generation_head_to;
        let m = &self.manifest;
        let write = |path: PathBuf,
                     watermark: u64,
                     deletes: crate::persistence::cold_records::ColdDeletes,
                     framed: bool|
         -> std::io::Result<()> {
            let mut f = std::fs::OpenOptions::new().append(true).open(&path)?;
            let mut buf = std::io::BufWriter::new(&mut f);
            write_generation_head_to(&mut buf, watermark, deletes, framed)?;
            buf.flush()?;
            drop(buf);
            f.sync_data()?;
            // `File::create` made the incr's directory entry; the manifest
            // commit fsyncs `appendonlydir/`, not a PerShard `shard-N/`.
            fsync_parent_best_effort(&path);
            Ok(())
        };
        let shards: Vec<(u16, PathBuf, bool)> = match m.layout {
            AofLayout::TopLevel => vec![(0, m.incr_path(), false)],
            AofLayout::PerShard => m
                .shards
                .iter()
                .map(|s| (s.shard_id, m.shard_incr_path(s.shard_id), true))
                .collect(),
        };
        for (sid, path, framed) in shards {
            write(
                path,
                watermark_for_shard(sid),
                deletes_for_shard(sid),
                framed,
            )?;
            test_hooks::crash_point(test_hooks::InitCrashPoint::AfterHead(sid));
        }
        Ok(())
    }

    /// Write the manifest: the single durable commit point of the
    /// generation. Every file it names — bases, incrs and, when
    /// [`Self::seed_generation_head`] ran, their heads — is already durable.
    pub fn commit(self) -> std::io::Result<AofManifest> {
        self.manifest.write_manifest()?;
        test_hooks::crash_point(test_hooks::InitCrashPoint::AfterCommit);
        Ok(self.manifest)
    }
}

impl AofManifest {
    /// [`Self::initialize`] up to, not including, the manifest commit: the
    /// `appendonlydir/`, an EMPTY base RDB (B4: the `(base + incr)`
    /// invariant holds from the first boot) and an empty incr.
    pub fn prepare(dir: &Path) -> std::io::Result<UncommittedGeneration> {
        let empty_dbs: [crate::storage::Database; 0] = [];
        let empty_rdb = crate::persistence::rdb::save_to_bytes(&empty_dbs)
            .map_err(|e| std::io::Error::other(format!("empty RDB serialize: {e}")))?;
        Self::prepare_with_base(dir, &empty_rdb)
    }

    /// [`Self::initialize_with_base`] up to, not including, the manifest
    /// commit: the `appendonlydir/`, the seq-1 base RDB holding `rdb_bytes`
    /// (tmp + fsync + rename) and an empty incr.
    pub fn prepare_with_base(
        dir: &Path,
        rdb_bytes: &[u8],
    ) -> std::io::Result<UncommittedGeneration> {
        let manifest = Self {
            dir: dir.to_path_buf(),
            seq: 1,
            layout: AofLayout::TopLevel,
            shards: vec![AofShardManifest {
                shard_id: 0,
                max_lsn: 0,
            }],
        };
        std::fs::create_dir_all(manifest.aof_dir())?;
        // The new `appendonlydir/` entry itself must survive a power loss, or a
        // committed manifest could sit in a directory the next boot cannot see
        // (review of PR #1301: nothing fsynced its parent).
        fsync_directory(&manifest.dir)?;

        // Write base RDB atomically: tmp file + fsync + rename.
        let base_path = manifest.base_path();
        let tmp_path = base_path.with_extension("rdb.tmp");
        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(rdb_bytes)?;
            f.sync_data()?;
        }
        std::fs::rename(&tmp_path, &base_path)?;
        fsync_parent_best_effort(&base_path);

        // Create (or truncate, after a crash before a commit) the empty incr
        // file so the writer has something to append to.
        std::fs::File::create(manifest.incr_path())?;
        Ok(UncommittedGeneration::new(manifest))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heads_in(path: &Path) -> usize {
        let bytes = std::fs::read(path).unwrap_or_default();
        bytes
            .windows(b"MOON.COLDCUT".len())
            .filter(|w| *w == b"MOON.COLDCUT")
            .count()
    }

    /// moon#1293: no manifest exists until every head is durable, so a crash
    /// anywhere before `commit` leaves a directory the next boot initializes
    /// from scratch — and that redo truncates the half-written incrs instead
    /// of appending a second head behind the first.
    #[test]
    fn heads_are_written_before_the_manifest_and_a_redo_starts_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        for layout_shards in [1u16, 3] {
            let sub = dir.join(format!("s{layout_shards}"));
            let prepare = |sub: &Path| {
                if layout_shards == 1 {
                    AofManifest::prepare(sub).unwrap()
                } else {
                    AofManifest::prepare_multi(sub, layout_shards).unwrap()
                }
            };
            let incrs = |m: &AofManifest| -> Vec<PathBuf> {
                match m.layout {
                    AofLayout::TopLevel => vec![m.incr_path()],
                    AofLayout::PerShard => m
                        .shards
                        .iter()
                        .map(|s| m.shard_incr_path(s.shard_id))
                        .collect(),
                }
            };

            // Boot 1 dies after the heads, before the commit.
            let first = prepare(&sub);
            first.seed_cold_cut(|sid| 10 + u64::from(sid)).unwrap();
            for p in incrs(first.manifest()) {
                assert_eq!(heads_in(&p), 1, "head written before the commit: {p:?}");
            }
            assert!(
                AofManifest::load(&sub).unwrap().is_none(),
                "no manifest may exist before the heads' generation commits"
            );
            drop(first);

            // Boot 2 redoes it: one head per incr, then the manifest.
            let second = prepare(&sub);
            for p in incrs(second.manifest()) {
                assert_eq!(heads_in(&p), 0, "a redo truncates the incr: {p:?}");
            }
            second.seed_cold_cut(|sid| 20 + u64::from(sid)).unwrap();
            let committed = second.commit().unwrap();
            let loaded = AofManifest::load(&sub).unwrap().expect("committed");
            assert_eq!(loaded.seq, 1);
            assert_eq!(loaded.shards.len(), usize::from(layout_shards));
            for p in incrs(&committed) {
                assert_eq!(heads_in(&p), 1, "exactly one head after the redo: {p:?}");
            }
            // A second initialization over a committed PerShard generation is
            // still refused (the idempotency pre-flight keys on the manifest).
            if layout_shards > 1 {
                let again = AofManifest::prepare_multi(&sub, layout_shards);
                assert_eq!(
                    again.err().map(|e| e.kind()),
                    Some(std::io::ErrorKind::AlreadyExists)
                );
            }
        }
    }
}
