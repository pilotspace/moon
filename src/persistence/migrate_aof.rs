//! AOF v1→v2 migration: a single-shard AOF into the per-shard layout
//! (`moon --migrate-aof-from <dir> --migrate-aof-to <dir> --migrate-aof-shards N`).
//!
//! # Algorithm (R2b round 4 F1)
//!
//! 1. Replay the source into ONE keyspace (all 16 logical databases) with
//!    the reader a `--shards 1` boot uses: the flat `appendonly.aof` (RDB
//!    preamble + RESP) through `aof::replay_aof`, or a single-shard
//!    manifest's base RDB + incr. Every record means what it meant to the
//!    server that wrote it: multi-key `DEL` / `MSET` / `RENAME`, `SELECT`,
//!    stream groups, `MOON.TXN` blocks (an unterminated one is rolled back),
//!    clock stamps. Mid-file corruption, or a record over the parser's
//!    limits, fails the migration; a torn tail is left out (logged), as the
//!    boot would.
//! 2. Partition every key by `key_to_shard(key, N)`, keeping its database,
//!    and write each shard's keys as its base RDB (incrs empty) — in a
//!    staging dir under the target.
//! 3. Publish: rename the staged `appendonlydir/` into the target and fsync
//!    the target. A failure before that leaves no AOF in the target (the
//!    staging dir is removed), never a half-written one that boots.
//!
//! The previous tool routed each logged command by its first argument: a
//! multi-key `DEL` / `MSET` / `RENAME` or an `XGROUP CREATE` landed on the
//! wrong shard (resurrected keys, lost groups, keys visible to `SCAN` that
//! `GET` misses), and `SELECT n > 0` failed half way leaving a target that
//! booted with part of the data (moon#1326 tracks the remaining gaps).
//!
//! # Limitations (refused, never migrated wrong)
//!
//! - A source with keys in the disk-offload cold tier (`MOON.SPILLED`
//!   records): their values live in the source's cold files, which this
//!   tool does not carry.
//! - A source that is already per-shard.
//! - The whole dataset is held in memory once (plus one database's copy
//!   while it is partitioned).
//!
//! # Usage
//!
//! ```text
//! moon --migrate-aof-from /old/dir --migrate-aof-to /new/dir --migrate-aof-shards 4
//! ```
//!
//! Stop the server that owns `/old/dir` first. The tool exits after the
//! migration; start the server with `--dir /new/dir --shards 4 --appendonly
//! yes`.

use std::io::Write;
use std::path::Path;

use tracing::{info, warn};

use crate::persistence::aof_manifest::{AofLayout, AofManifest};
use crate::persistence::replay::DispatchReplayEngine;
use crate::storage::Database;

/// Logical databases the scratch keyspace holds (the server's maximum).
const MAX_DBS: usize = 16;

/// Outcome of a migrate-aof run.
#[derive(Debug)]
pub struct MigrateAofResult {
    /// Records (and base keys) the source replay applied.
    pub records_replayed: usize,
    /// Keys written to the target, over every shard and database.
    pub keys_migrated: usize,
    /// Keys per target shard.
    pub keys_per_shard: Vec<usize>,
}

fn fail(detail: String) -> crate::error::MoonError {
    crate::error::MoonError::Other(detail)
}

fn io_err(path: &Path, e: std::io::Error) -> crate::error::MoonError {
    crate::error::MoonError::from(crate::error::AofError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Migrate the single-shard AOF in `from_dir` into a per-shard layout of
/// `num_shards` shards in `to_dir` (see the module doc).
///
/// `from_dir` holds `appendonly.aof` (flat, with or without an RDB
/// preamble) or `appendonlydir/` with a single-shard manifest. `to_dir`
/// must not hold an AOF yet.
pub fn migrate_aof(
    from_dir: &Path,
    to_dir: &Path,
    num_shards: u16,
) -> Result<MigrateAofResult, crate::error::MoonError> {
    if num_shards == 0 {
        return Err(fail("migrate_aof: num_shards must be >= 1".to_owned()));
    }
    // Migrating into the same directory would clobber the source layout.
    if from_dir == to_dir {
        return Err(fail(format!(
            "migrate_aof: from_dir and to_dir must differ (both are {}). \
             Specify a separate empty directory for --migrate-aof-to.",
            from_dir.display()
        )));
    }
    // A manifest or an AOF directory in to_dir: a previous migration ran, or
    // this is a live data directory. Refuse rather than overwrite.
    if matches!(AofManifest::load(to_dir), Ok(Some(_))) || to_dir.join("appendonlydir").exists() {
        return Err(fail(format!(
            "migrate_aof: to_dir ({}) already contains an AOF manifest. Use a fresh, \
             non-existent or empty directory for --migrate-aof-to.",
            to_dir.display()
        )));
    }

    // ── 1. Replay the source into one keyspace ─────────────────────────────
    let mut scratch: Vec<Database> = (0..MAX_DBS).map(|_| Database::new()).collect();
    let records_replayed = replay_source(from_dir, &mut scratch)?;

    // ── 2. Partition into a staged per-shard layout ────────────────────────
    std::fs::create_dir_all(to_dir).map_err(|e| io_err(to_dir, e))?;
    let staging = to_dir.join(".migrate-staging");
    if staging.exists() {
        std::fs::remove_dir_all(&staging).map_err(|e| io_err(&staging, e))?;
    }
    let staged = stage(&staging, scratch, num_shards);
    let keys_per_shard = match staged {
        Ok(k) => k,
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging);
            return Err(e);
        }
    };

    // ── 3. Publish ─────────────────────────────────────────────────────────
    let from = staging.join("appendonlydir");
    let to = to_dir.join("appendonlydir");
    let published = std::fs::rename(&from, &to)
        .and_then(|()| crate::persistence::fsync::fsync_directory(to_dir));
    let _ = std::fs::remove_dir_all(&staging);
    published.map_err(|e| io_err(&to, e))?;

    let keys_migrated = keys_per_shard.iter().sum();
    info!(
        "migrate_aof complete: {} source records replayed, {} keys written across {} shards {:?}",
        records_replayed, keys_migrated, num_shards, keys_per_shard
    );
    Ok(MigrateAofResult {
        records_replayed,
        keys_migrated,
        keys_per_shard,
    })
}

/// Replay the source in `from_dir` into `scratch` (step 1).
fn replay_source(
    from_dir: &Path,
    scratch: &mut [Database],
) -> Result<usize, crate::error::MoonError> {
    let engine = DispatchReplayEngine::new();
    let flat = crate::persistence::aof::flat_file::flat_aof_path(from_dir);
    if flat.exists() {
        info!("migrate_aof: replaying {}", flat.display());
        refuse_cold_records(&flat)?;
        let n = crate::persistence::aof::replay_aof(scratch, &flat, &engine)?;
        // A `MOON.COLDCUT` head installs a replay gate: close it, as a boot does.
        let _ = crate::storage::db::close_replay_generation(scratch);
        return Ok(n);
    }
    if let Some(m) = AofManifest::load(from_dir).map_err(|e| io_err(from_dir, e))? {
        if m.layout != AofLayout::TopLevel {
            return Err(fail(format!(
                "migrate_aof: {} already holds a per-shard AOF; this tool migrates a \
                 single-shard AOF only (moon#1326)",
                from_dir.display()
            )));
        }
        let mut n = 0;
        let base = m.base_path();
        if base.exists() {
            info!("migrate_aof: loading base {}", base.display());
            n += crate::persistence::rdb::load(scratch, &base)?;
        }
        let incr = m.incr_path();
        if incr.exists() {
            info!("migrate_aof: replaying {}", incr.display());
            refuse_cold_records(&incr)?;
            n += crate::persistence::aof::replay_aof(scratch, &incr, &engine)?;
            let _ = crate::storage::db::close_replay_generation(scratch);
        }
        return Ok(n);
    }
    Err(fail(format!(
        "migrate_aof: no AOF source found in {}. Expected appendonly.aof or \
         appendonlydir/ with a single-shard manifest",
        from_dir.display()
    )))
}

/// A source with `MOON.SPILLED` records holds keys whose values live in its
/// cold-tier files, which this tool does not carry: refuse.
fn refuse_cold_records(path: &Path) -> Result<(), crate::error::MoonError> {
    const NEEDLE: &[u8] = b"MOON.SPILLED";
    let mut file = std::fs::File::open(path).map_err(|e| io_err(path, e))?;
    let mut buf = vec![0u8; 1 << 20];
    let mut carry: Vec<u8> = Vec::new();
    loop {
        let n = std::io::Read::read(&mut file, &mut buf).map_err(|e| io_err(path, e))?;
        if n == 0 {
            return Ok(());
        }
        carry.extend_from_slice(&buf[..n]);
        if carry.windows(NEEDLE.len()).any(|w| w == NEEDLE) {
            return Err(fail(format!(
                "migrate_aof: {} holds keys in the disk-offload cold tier (MOON.SPILLED \
                 records), whose values live in the source's cold files; this tool does not \
                 carry them (moon#1326)",
                path.display()
            )));
        }
        let keep = carry.len().min(NEEDLE.len() - 1);
        carry.drain(..carry.len() - keep);
    }
}

/// Partition `scratch` into `num_shards` shards and write each shard's base
/// under `staging` (step 2). Returns the keys per shard.
fn stage(
    staging: &Path,
    mut scratch: Vec<Database>,
    num_shards: u16,
) -> Result<Vec<usize>, crate::error::MoonError> {
    std::fs::create_dir_all(staging).map_err(|e| io_err(staging, e))?;
    let manifest =
        AofManifest::initialize_multi(staging, num_shards).map_err(|e| io_err(staging, e))?;
    let mut shard_dbs: Vec<Vec<Database>> = (0..num_shards)
        .map(|_| (0..MAX_DBS).map(|_| Database::new()).collect())
        .collect();
    let mut per_shard = vec![0usize; usize::from(num_shards)];
    for (db_idx, slot) in scratch.iter_mut().enumerate() {
        // One database's copy at a time: drop the source as soon as it is
        // partitioned.
        let db = std::mem::take(slot);
        for (key, entry) in db.data().iter() {
            let key_bytes = key.as_bytes();
            let sid = crate::shard::dispatch::key_to_shard(key_bytes, usize::from(num_shards));
            shard_dbs[sid][db_idx].set(key_bytes, entry.clone());
            per_shard[sid] += 1;
        }
    }
    for (sid, dbs) in shard_dbs.iter().enumerate() {
        let sid16 = sid as u16;
        let base_path = manifest.shard_base_path(sid16);
        let rdb = crate::persistence::rdb::save_to_bytes(dbs)?;
        let tmp = base_path.with_extension("rdb.tmp");
        {
            let mut f = std::fs::File::create(&tmp).map_err(|e| io_err(&tmp, e))?;
            f.write_all(&rdb).map_err(|e| io_err(&tmp, e))?;
            f.sync_all().map_err(|e| io_err(&tmp, e))?;
        }
        std::fs::rename(&tmp, &base_path).map_err(|e| io_err(&base_path, e))?;
        crate::persistence::fsync::fsync_directory(&manifest.shard_dir(sid16))
            .map_err(|e| io_err(&base_path, e))?;
        info!(
            "migrate_aof: shard-{} base written ({} keys, {} bytes)",
            sid,
            per_shard[sid],
            rdb.len()
        );
    }
    if per_shard.iter().all(|&n| n == 0) {
        warn!("migrate_aof: the source holds no keys; the target is an empty layout");
    }
    Ok(per_shard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frame;
    use bytes::{Bytes, BytesMut};

    fn cmd_resp(parts: &[&str]) -> Vec<u8> {
        let mut buf = BytesMut::new();
        let frames: Vec<Frame> = parts
            .iter()
            .map(|s| Frame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect();
        crate::protocol::serialize::serialize(&Frame::Array(frames.into()), &mut buf);
        buf.to_vec()
    }

    /// The migrated layout, booted as a `--shards N` server would: each
    /// shard's base + incr.
    fn boot_target(dir: &Path, n: u16) -> Vec<Vec<Database>> {
        use crate::persistence::aof_manifest::replay_per_shard;
        let manifest = AofManifest::load(dir).expect("load").expect("manifest");
        assert_eq!(manifest.layout, AofLayout::PerShard);
        assert_eq!(manifest.shards.len(), usize::from(n));
        let mut shard_dbs: Vec<Vec<Database>> = (0..n)
            .map(|_| (0..MAX_DBS).map(|_| Database::new()).collect())
            .collect();
        let mut slices: Vec<&mut [Database]> =
            shard_dbs.iter_mut().map(|v| v.as_mut_slice()).collect();
        replay_per_shard(
            &mut slices,
            &manifest,
            &(|| {
                Box::new(DispatchReplayEngine::new())
                    as Box<dyn crate::persistence::replay::CommandReplayEngine + Send>
            }),
        )
        .expect("replay");
        shard_dbs
    }

    /// `key` in database `db` as the `n`-shard server routes it.
    fn get(shards: &mut [Vec<Database>], n: u16, db: usize, key: &str) -> Option<Vec<u8>> {
        let sid = crate::shard::dispatch::key_to_shard(key.as_bytes(), usize::from(n));
        shards[sid][db]
            .get(key.as_bytes())
            .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec))
    }

    fn total_keys(shards: &[Vec<Database>]) -> usize {
        shards.iter().flatten().map(|db| db.len()).sum()
    }

    #[test]
    fn migrate_aof_same_dir_returns_err() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("appendonly.aof"), b"").unwrap();
        let msg = migrate_aof(dir.path(), dir.path(), 2)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("must differ"), "{msg}");
    }

    #[test]
    fn migrate_aof_existing_manifest_returns_err() {
        let src_dir = tempfile::tempdir().unwrap();
        let dst_dir = tempfile::tempdir().unwrap();
        std::fs::write(src_dir.path().join("appendonly.aof"), b"").unwrap();
        AofManifest::initialize_multi(dst_dir.path(), 2).expect("first initialize_multi");
        let msg = migrate_aof(src_dir.path(), dst_dir.path(), 2)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("already"), "{msg}");
    }

    /// R2b round 4 F1: every record means what it meant to the server that
    /// wrote it — multi-key `DEL` / `MSET` / `RENAME`, `SELECT`, a consumer
    /// group — where the old router sent each to its first argument's shard.
    #[test]
    fn multi_key_commands_select_and_groups_migrate_exactly() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut aof = Vec::new();
        for i in 0..12 {
            aof.extend(cmd_resp(&["SET", &format!("k{i}"), "v"]));
        }
        aof.extend(cmd_resp(&["DEL", "k0", "k1", "k2", "k3"]));
        aof.extend(cmd_resp(&[
            "MSET", "m1", "a", "m2", "b", "m3", "c", "m4", "d",
        ]));
        aof.extend(cmd_resp(&["RENAME", "k4", "renamed"]));
        aof.extend(cmd_resp(&["XADD", "st", "1-1", "f", "v"]));
        aof.extend(cmd_resp(&["XGROUP", "CREATE", "st", "g1", "0"]));
        aof.extend(cmd_resp(&["SELECT", "3"]));
        aof.extend(cmd_resp(&["SET", "db3key", "x"]));
        aof.extend(cmd_resp(&["SELECT", "0"]));
        std::fs::write(src.path().join("appendonly.aof"), &aof).unwrap();

        let r = migrate_aof(src.path(), dst.path(), 4).expect("migration");
        let mut shards = boot_target(dst.path(), 4);
        // k5..k11 (7) + m1..m4 (4) + renamed + st in db 0, db3key in db 3.
        assert_eq!(total_keys(&shards), 14);
        assert_eq!(r.keys_migrated, 14);
        for k in ["k0", "k1", "k2", "k3", "k4"] {
            assert_eq!(get(&mut shards, 4, 0, k), None, "{k} stays deleted");
        }
        assert_eq!(get(&mut shards, 4, 0, "m4"), Some(b"d".to_vec()));
        assert_eq!(get(&mut shards, 4, 0, "renamed"), Some(b"v".to_vec()));
        assert_eq!(get(&mut shards, 4, 3, "db3key"), Some(b"x".to_vec()));
        let sid = crate::shard::dispatch::key_to_shard(b"st", 4);
        let st = shards[sid][0].get_stream(b"st").unwrap().unwrap();
        assert!(st.groups.contains_key(b"g1".as_ref()), "the group survives");
    }

    /// moon#1300: a `MOON.TXN` block the crash cut is rolled back by the
    /// source replay, so no shard gets its writes.
    #[test]
    fn an_unterminated_txn_block_is_not_migrated() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut aof = Vec::new();
        for i in 0..8 {
            aof.extend(cmd_resp(&["SET", &format!("key{i}"), "original"]));
        }
        aof.extend(cmd_resp(&["MOON.TS", "1790000000000"]));
        aof.extend(cmd_resp(&["MOON.TXN", "BEGIN", "7"]));
        for i in 0..8 {
            aof.extend(cmd_resp(&["SET", &format!("key{i}"), "uncommitted"]));
        }
        std::fs::write(src.path().join("appendonly.aof"), &aof).unwrap();
        migrate_aof(src.path(), dst.path(), 4).expect("migration");
        let mut shards = boot_target(dst.path(), 4);
        for i in 0..8 {
            let k = format!("key{i}");
            assert_eq!(
                get(&mut shards, 4, 0, &k),
                Some(b"original".to_vec()),
                "{k}"
            );
        }
    }

    /// An RDB preamble (a rewritten AOF) and the RESP after it.
    #[test]
    fn a_preamble_and_its_tail_migrate() {
        use crate::storage::entry::Entry;
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut base = vec![Database::new()];
        for i in 0..20 {
            base[0].set(
                format!("rdb_key:{i}").as_bytes(),
                Entry::new_string(Bytes::from_static(b"base")),
            );
        }
        let mut aof = crate::persistence::rdb::save_to_bytes(&base).unwrap();
        aof.extend(cmd_resp(&["DEL", "rdb_key:0", "rdb_key:1"]));
        aof.extend(cmd_resp(&["SET", "tail", "t"]));
        std::fs::write(src.path().join("appendonly.aof"), &aof).unwrap();
        migrate_aof(src.path(), dst.path(), 4).expect("migration");
        let mut shards = boot_target(dst.path(), 4);
        assert_eq!(total_keys(&shards), 19);
        assert_eq!(get(&mut shards, 4, 0, "rdb_key:5"), Some(b"base".to_vec()));
        assert_eq!(get(&mut shards, 4, 0, "tail"), Some(b"t".to_vec()));
    }

    /// Damage in the middle of the source fails the migration and leaves no
    /// AOF in the target — never a half-written one that boots.
    #[test]
    fn a_damaged_source_fails_and_leaves_no_target() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut aof = cmd_resp(&["SET", "a", "1"]);
        aof.extend_from_slice(b"*2\r\n$zz\r\n");
        aof.extend(cmd_resp(&["SET", "b", "2"]));
        std::fs::write(src.path().join("appendonly.aof"), &aof).unwrap();
        assert!(migrate_aof(src.path(), dst.path(), 4).is_err());
        assert!(!dst.path().join("appendonlydir").exists());
        assert!(!dst.path().join(".migrate-staging").exists());
    }

    /// Keys in the cold tier are refused, not lost.
    #[test]
    fn a_source_with_cold_keys_is_refused() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let mut aof = cmd_resp(&["SET", "a", "1"]);
        aof.extend(cmd_resp(&["MOON.SPILLED", "3", "a"]));
        std::fs::write(src.path().join("appendonly.aof"), &aof).unwrap();
        let msg = migrate_aof(src.path(), dst.path(), 4)
            .unwrap_err()
            .to_string();
        assert!(msg.contains("cold tier"), "{msg}");
        assert!(!dst.path().join("appendonlydir").exists());
    }

    #[test]
    fn an_empty_source_produces_an_empty_layout() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("appendonly.aof"), b"").unwrap();
        let r = migrate_aof(src.path(), dst.path(), 2).expect("migration");
        assert_eq!(r.keys_migrated, 0);
        let shards = boot_target(dst.path(), 2);
        assert_eq!(total_keys(&shards), 0);
    }

    /// A single-shard manifest source (monoio `--shards 1`): base + incr.
    #[test]
    fn a_single_shard_manifest_source_migrates() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        let m = AofManifest::initialize(src.path()).expect("initialize");
        let mut incr = Vec::new();
        for i in 0..10 {
            incr.extend(cmd_resp(&["SET", &format!("{{0}}:key{i}"), "v"]));
        }
        std::fs::write(m.incr_path(), &incr).unwrap();
        migrate_aof(src.path(), dst.path(), 4).expect("migration");
        let mut shards = boot_target(dst.path(), 4);
        let sid = crate::shard::dispatch::key_to_shard(b"{0}:key0", 4);
        assert_eq!(shards[sid][0].len(), 10, "one hash tag, one shard");
        assert_eq!(get(&mut shards, 4, 0, "{0}:key9"), Some(b"v".to_vec()));
    }
}
