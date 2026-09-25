//! moon#1231: a key that was cold at an AOF fold and later left the cold
//! index without a log record — a read promotion, or a read-modify-write that
//! promoted it — keeps its spill file until a later fold captures it.
//!
//! The production pieces, end to end: the fold snapshot (`advance_epoch` at
//! the instant `stream_fold_image` runs, as the `AofFold` arm does), the commit
//! (`FoldOutcome::adopt`), the commands (`command::dispatch` through
//! `StorageEngine::execute_command`), the orphan sweep
//! (`run_cold_orphan_sweep`, with this shard's AOF pool and spill counter), and
//! production recovery of the committed generation from what is left on disk.
//!
//! Red before the fix: the sweep unlinked the file once its last key left, so
//! the "crash" below lost `k08` (read-promoted) and replayed `APPEND k09 x`
//! onto nothing (`Some("x")` instead of `Some("v09x")`).

use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;

use super::run_cold_orphan_sweep;
use crate::persistence::aof::fold_stream::{
    fold_image_channel, stream_fold_image, write_fold_image,
};
use crate::persistence::aof::rewrite::FoldOutcome;
use crate::persistence::aof::{AofMessage, AofWriterPool, FoldEpoch};
use crate::persistence::cold_records::{serialize_cold_cut, write_generation_head_to};
use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::protocol::Frame;
use crate::shard::shared_databases::ShardDatabases;
use crate::shard::slice::{ShardSlice, init_shard, with_shard, with_shard_db};
use crate::storage::Database;
use crate::storage::engine::StorageEngine;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

/// The spill file the keys are cold in when the first fold cuts.
const OLD: u64 = 5;
/// The shard's spill counter at that fold: its `MOON.COLDCUT`.
const CUT: u64 = 20;

fn keys(prefix: &str, n: usize) -> Vec<(String, String)> {
    (0..n)
        .map(|i| (format!("{prefix}{i:02}"), format!("v{i:02}")))
        .collect()
}

fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn heap(shard_dir: &Path, file_id: u64) -> PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"))
}

fn listed(manifest: &ShardManifest, file_id: u64) -> bool {
    manifest
        .files()
        .iter()
        .any(|f| f.file_id == file_id && f.status == FileStatus::Active)
}

/// Write spill file `file_id` holding `kvs` and list it (durably).
fn spill(shard_dir: &Path, manifest: &mut ShardManifest, file_id: u64, kvs: &[(String, String)]) {
    let entries: Vec<SpillEntry> = kvs
        .iter()
        .map(|(k, v)| SpillEntry {
            key: Bytes::copy_from_slice(k.as_bytes()),
            value_bytes: Bytes::copy_from_slice(v.as_bytes()),
            value_type: ValueType::String,
            flags: 0,
            ttl_ms: None,
        })
        .collect();
    let batch = build_kv_spill_batch(&entries, file_id).expect("build batch");
    let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).expect("write batch");
    manifest
        .add_file(FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.pages.len() as u32,
            byte_size,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        })
        .expect("unique ids");
    manifest.commit().expect("commit");
}

/// Production recovery of shard 0 from `shard_dir` (manifest + heap files)
/// and the flat AOF `aof` (the tokio `--shards 1` layout).
fn recover(root: &Path, shard_dir: &Path, aof: &[u8]) -> Database {
    let legacy = root.join("legacy");
    std::fs::create_dir_all(&legacy).expect("aof dir");
    std::fs::write(legacy.join("appendonly.aof"), aof).expect("aof");
    let mut dbs = vec![Database::new()];
    crate::persistence::recovery::recover_shard_v3_with_fallback(
        &mut dbs,
        0,
        shard_dir,
        &crate::persistence::replay::DispatchReplayEngine::new(),
        Some(&legacy),
        false,
    )
    .expect("recovery");
    dbs.remove(0)
}

fn value(db: &mut Database, key: &str) -> Option<String> {
    db.promote_cold_if_present(key.as_bytes(), 0);
    db.get(key.as_bytes())
        .and_then(|e| e.value.as_bytes_owned())
        .map(|b| String::from_utf8_lossy(&b).into_owned())
}

fn bulk(s: &str) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
}

/// Run `cmd args` on the live shard's db 0 through the command dispatcher.
fn run(cmd: &str, args: &[&str]) -> Frame {
    let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
    with_shard_db(0, |db| {
        let mut idx = 0usize;
        db.execute_command(cmd.as_bytes(), &frames, &mut idx, 1)
    })
}

/// One live shard with an AOF writer pool, a spill counter and a manifest.
struct Live {
    tmp: tempfile::TempDir,
    dir: PathBuf,
    manifest: ShardManifest,
    shared: Arc<ShardDatabases>,
    pool: Arc<AofWriterPool>,
    counter: Cell<u64>,
    /// Keeps the writer channel open (nothing reads it).
    _rx: crate::runtime::channel::MpscReceiver<AofMessage>,
}

impl Live {
    /// Ten keys cold in `OLD` (recovered from their own log, as a restart
    /// leaves them) on a fresh shard thread's slice.
    fn start() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&dir).expect("dir");
        let mut manifest = ShardManifest::create(&dir.join("shard-0.manifest")).expect("manifest");
        let kvs = keys("k", 10);
        spill(&dir, &mut manifest, OLD, &kvs);
        let mut log = serialize_cold_cut(1).to_vec();
        for (k, v) in &kvs {
            log.extend_from_slice(&resp(&[b"SET", k.as_bytes(), v.as_bytes()]));
        }
        let mut marker: Vec<&[u8]> = vec![b"MOON.SPILLED", b"5"];
        marker.extend(kvs.iter().map(|(k, _)| k.as_bytes()));
        log.extend_from_slice(&resp(&marker));
        let recovered = recover(tmp.path(), &dir, &log);
        let (shared, mut inits) = ShardDatabases::new(vec![vec![Database::new()]]);
        init_shard(ShardSlice::new(inits.remove(0)));
        with_shard_db(0, |db| *db = recovered);
        for (k, _) in &kvs {
            assert_eq!(
                with_shard_db(0, |db| db
                    .cold_index
                    .as_ref()
                    .and_then(|ci| ci.lookup(k.as_bytes()))
                    .map(|l| l.file_id)),
                Some(OLD),
                "fixture: {k} is cold in file {OLD}"
            );
        }
        let (tx, rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(16);
        Live {
            tmp,
            dir,
            manifest,
            shared,
            pool: AofWriterPool::top_level(tx),
            counter: Cell::new(CUT),
            _rx: rx,
        }
    }

    /// A BGREWRITEAOF fold, committed: the snapshot epoch opens at the instant
    /// the image is streamed (the `AofFold` arm), the head carries the cut and
    /// the ledger's DELs, and the writer adopts the committed floor. Returns
    /// the new generation (base + head).
    fn fold_and_commit(&self) -> Vec<u8> {
        let overflow = self.pool.overflow_for(0);
        let floor = overflow.advance_epoch();
        let (sink, image) = fold_image_channel();
        let now = crate::storage::entry::current_time_ms();
        with_shard(|s| {
            s.databases
                .with_all_read(|all| stream_fold_image(all, now, sink))
        });
        let mut aof = Vec::new();
        let (_, deletes) = write_fold_image(image, &mut aof, "moon#1231 test").expect("image");
        write_generation_head_to(&mut aof, self.counter.get(), deletes, false).expect("head");
        let _ = FoldOutcome::Committed { floor }.adopt(FoldEpoch::INITIAL, overflow);
        aof
    }

    /// The orphan sweep, as the shard's timer runs it.
    fn sweep(&mut self, with_aof: bool) {
        run_cold_orphan_sweep(
            &self.shared,
            0,
            &self.dir,
            Some(&mut self.manifest),
            crate::storage::entry::current_time_ms(),
            with_aof.then_some(&self.pool),
            &self.counter,
        );
    }

    fn held(&self, file_id: u64) -> bool {
        with_shard_db(0, |db| {
            db.cold_index
                .as_ref()
                .is_some_and(|ci| ci.is_unlink_held(file_id))
        })
    }
}

/// After the fold: DEL the eight neighbours, GET-promote `k08`, APPEND to
/// `k09`. Returns the records the post-fold incr holds.
fn delete_promote_append() -> Vec<u8> {
    let mut incr = Vec::new();
    for (k, _) in keys("k", 8) {
        assert_eq!(run("DEL", &[&k]), Frame::Integer(1), "DEL {k}");
        incr.extend_from_slice(&resp(&[b"DEL", k.as_bytes()]));
    }
    assert_eq!(
        run("GET", &["k08"]),
        Frame::BulkString(Bytes::from_static(b"v08"))
    );
    assert_eq!(run("APPEND", &["k09", "x"]), Frame::Integer(4));
    incr.extend_from_slice(&resp(&[b"APPEND", b"k09", b"x"]));
    let refs = with_shard_db(0, |db| {
        db.cold_index
            .as_ref()
            .map_or(usize::MAX, |ci| ci.referenced_file_count())
    });
    assert_eq!(refs, 0, "fixture: file {OLD} has no key left in the index");
    incr
}

#[test]
fn a_key_promoted_after_a_fold_keeps_its_spill_file_until_the_next_fold_commits() {
    std::thread::spawn(|| {
        let mut live = Live::start();
        let mut generation = live.fold_and_commit();
        generation.extend_from_slice(&delete_promote_append());

        // The sweep runs: the file backed k08 and k09 at the fold.
        live.sweep(true);

        // Crash now: the committed generation replays onto what is on disk.
        let mut db = recover(live.tmp.path(), &live.dir, &generation);
        assert_eq!(
            (value(&mut db, "k08"), value(&mut db, "k09")),
            (Some("v08".into()), Some("v09x".into())),
            "(k08 read-promoted, k09 after APPEND k09 x) after the sweep and a crash"
        );
        for (k, _) in keys("k", 8) {
            assert_eq!(value(&mut db, &k), None, "{k} was deleted");
        }
        assert!(
            heap(&live.dir, OLD).exists() && listed(&live.manifest, OLD),
            "file {OLD} was unlinked while the committed generation still reads it"
        );
        assert!(live.held(OLD));

        // A second fold captures the promoted keys; once it has committed the
        // next sweep releases the file.
        let second = live.fold_and_commit();
        live.sweep(true);
        assert!(
            !heap(&live.dir, OLD).exists() && !listed(&live.manifest, OLD),
            "a committed fold after the promotion releases the file"
        );
        assert!(!live.held(OLD));
        let mut db = recover(live.tmp.path(), &live.dir, &second);
        assert_eq!(value(&mut db, "k08"), Some("v08".into()));
        assert_eq!(value(&mut db, "k09"), Some("v09x".into()));
        for (k, _) in keys("k", 8) {
            assert_eq!(value(&mut db, &k), None, "{k} after the second fold");
        }
    })
    .join()
    .expect("test thread");
}

/// A fold that has STARTED (its snapshot epoch is open) but not committed
/// does not release anything: if it aborts, the old generation still reads
/// the file.
#[test]
fn an_uncommitted_fold_releases_nothing() {
    std::thread::spawn(|| {
        let mut live = Live::start();
        let _generation = live.fold_and_commit();
        let _ = delete_promote_append();
        live.sweep(true);
        assert!(live.held(OLD));
        // A second fold cuts, then aborts: the floor does not move.
        let _ = live.pool.overflow_for(0).advance_epoch();
        live.sweep(true);
        assert!(heap(&live.dir, OLD).exists() && live.held(OLD));
    })
    .join()
    .expect("test thread");
}

/// A file minted after the latest cut is unlinked as soon as its last key
/// leaves: no generation reads it below its cut (its keys' records are in the
/// incr). The hold must not turn every delete into a wait for a fold.
#[test]
fn a_file_spilled_after_the_latest_cut_is_unlinked_at_once() {
    std::thread::spawn(|| {
        let mut live = Live::start();
        let _generation = live.fold_and_commit();
        // The sweep after the fold sees the cut (bound = the counter, 20).
        live.sweep(true);
        // A later spill, file 25, of two keys written after the fold.
        live.counter.set(30);
        let later = keys("n", 2);
        spill(&live.dir, &mut live.manifest, 25, &later);
        with_shard_db(0, |db| {
            let ci = db.cold_index.as_mut().expect("cold index");
            for (slot, (k, _)) in later.iter().enumerate() {
                ci.insert(
                    Bytes::copy_from_slice(k.as_bytes()),
                    crate::storage::tiered::cold_index::ColdLocation {
                        file_id: 25,
                        page_idx: 0,
                        slot_idx: slot as u16,
                        ttl_ms: None,
                        value_type: ValueType::String,
                    },
                );
            }
        });
        for (k, _) in &later {
            assert_eq!(run("DEL", &[k]), Frame::Integer(1));
        }
        live.sweep(true);
        assert!(
            !heap(&live.dir, 25).exists() && !listed(&live.manifest, 25),
            "file 25 is above every cut and must go at once"
        );
        assert!(heap(&live.dir, OLD).exists(), "file {OLD} is untouched");
    })
    .join()
    .expect("test thread");
}

/// Without an AOF writer there is no fold to protect and none could ever
/// release a hold: files go as soon as their last key does, as before.
#[test]
fn without_an_aof_writer_nothing_is_held() {
    std::thread::spawn(|| {
        let mut live = Live::start();
        for (k, _) in keys("k", 10) {
            assert_eq!(run("DEL", &[&k]), Frame::Integer(1));
        }
        live.sweep(false);
        assert!(!heap(&live.dir, OLD).exists() && !listed(&live.manifest, OLD));
    })
    .join()
    .expect("test thread");
}
