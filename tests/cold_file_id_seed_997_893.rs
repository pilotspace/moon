//! moon#997 + moon#893: a shard's file_id counter must resume above EVERY id
//! already in use — or refuse to start.
//!
//! ## The defect
//!
//! One per-shard counter names both cold-tier artifacts: KV spill files
//! (`data/heap-{id:06}.mpf`, manifest `KvLeaf`) and warm vector segments
//! (`vectors/segment-{id}/`, manifest `VecCodes`). On restart the counter was
//! seeded by `eviction::next_spill_file_id_seed`, which
//!
//! * scanned `data/heap-*.mpf` ONLY — vector segments and the manifest were
//!   invisible to it (moon#893), and
//! * answered `1` when the scan failed for any reason but `NotFound`, and
//!   skipped every directory entry it could not read (moon#997).
//!
//! ## The tests
//!
//! * `vector_segment_id_is_never_reissued_to_a_spill_file_*` — the #893
//!   consequence triage raised from P2 on a code read. The corpus is a data
//!   dir whose highest id belongs to a warm vector segment. The server
//!   re-issues that id to its next spill file, so the manifest now holds a
//!   `KvLeaf` and a `VecCodes` entry with the same id. The segment directory
//!   is then retired by the server's own recovery (a partially-swept segment
//!   with no `mvcc.mpf`, `VectorStore::register_warm_segments`), and on the
//!   next boot `recover_shard_v3` retires the now-dirless `VecCodes` entry
//!   with `manifest.remove_file(id)` — which tombstoned EVERY entry with that
//!   id, the live spill file's included. Its keys read as absent.
//!
//! * `already_collided_manifest_keeps_its_spill_file_*` — the upgrade path.
//!   A data dir a pre-fix build already wrote the collision into (a live
//!   spill file and a dirless warm segment under one id) must not lose the
//!   spill file on its first boot: `remove_file` now matches `(id, type)`.
//!
//! * `unreadable_cold_dir_refuses_to_start_*` — the #997 failed scan. The
//!   corpus's `data/` directory is write+search but not readable (`-wx`), so
//!   `read_dir` fails with `EACCES` while files inside can still be created
//!   and renamed. The old seed answered `1`, and the next spill renamed its
//!   batch onto the LIVE `heap-000001.mpf`. The fixed server refuses to
//!   start; once the operator restores the permission it starts and serves
//!   every key.
//!
//! * `torn_manifest_create_does_not_block_startup_*` — a manifest shorter
//!   than its two root pages (a `create` that died part-way) holds no entry
//!   and must not make the fail-closed seed refuse every boot.
//!
//! The per-entry half of #997 (`read_dir(..).flatten()` dropping an entry the
//! kernel could not return) cannot be produced on demand on a real filesystem;
//! it is covered by the injected-error unit tests in
//! `src/storage/tiered/file_id_seed.rs`.
//!
//! Run with:
//!   cargo test --release --test cold_file_id_seed_997_893
//!   cargo test --release --no-default-features --features runtime-tokio,jemalloc \
//!     --test cold_file_id_seed_997_893

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use bytes::Bytes;
use moon::persistence::kv_page::{ValueType, read_datafile};
use moon::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use moon::persistence::page::PageType;
use moon::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

use common::{Conn, ServerGuard, find_moon_binary, wait_for_port_down};

/// Small enough that filler crosses it quickly, large enough that the reads
/// under test are not answered `-OOM`.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const FILLER_VALUE_LEN: usize = 1024;
const FILLER_PER_ROUND: usize = 100;
const MAX_FILLER_ROUNDS: usize = 1000;
const CORPUS_VALUE_LEN: usize = 300;

// ===========================================================================
// Server harness.
// ===========================================================================

fn offload_dir(dir: &Path) -> PathBuf {
    dir.join("off")
}

fn shard_dir(dir: &Path, shard: usize) -> PathBuf {
    offload_dir(dir).join(format!("shard-{shard}"))
}

fn manifest_path(dir: &Path, shard: usize) -> PathBuf {
    shard_dir(dir, shard).join(format!("shard-{shard}.manifest"))
}

/// What a server is started with. `aof` is the durability backstop: with it
/// every write-path victim is spilled; without it only the memory-pressure
/// tick spills, and the cold plane is the ONLY copy of what it spills.
#[derive(Clone, Copy, Debug)]
struct Mode {
    shards: usize,
    aof: bool,
}

fn moon_args(dir: &Path, port: u16, mode: Mode) -> Vec<String> {
    vec![
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        dir.to_string_lossy().into_owned(),
        "--shards".into(),
        mode.shards.to_string(),
        "--disk-offload".into(),
        "enable".into(),
        "--disk-offload-dir".into(),
        offload_dir(dir).to_string_lossy().into_owned(),
        "--appendonly".into(),
        if mode.aof { "yes" } else { "no" }.into(),
        "--appendfsync".into(),
        "everysec".into(),
        "--maxmemory".into(),
        MAXMEMORY_BYTES.to_string(),
        "--maxmemory-policy".into(),
        "allkeys-lru".into(),
        // Under test is file_id allocation, not the disk guard.
        "--disk-free-min-pct".into(),
        "0".into(),
        "--protected-mode".into(),
        "no".into(),
    ]
}

struct Server {
    guard: ServerGuard,
    port: u16,
    dir: PathBuf,
    mode: Mode,
}

fn spawn(dir: &Path, mode: Mode) -> Server {
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args(moon_args(dir, port, mode))
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let server = Server {
        guard,
        port,
        dir: dir.to_path_buf(),
        mode,
    };
    assert!(
        server.pings(),
        "moon never answered PING on port {port} after start-up"
    );
    server
}

impl Server {
    /// A real `+PONG` — `SO_REUSEPORT` means a connect alone proves nothing.
    fn pings(&self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(s) = std::net::TcpStream::connect(("127.0.0.1", self.port)) {
                drop(s);
                let mut c = Conn::open(self.port);
                if c.send(&["PING"]).starts_with("+PONG") {
                    return true;
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }

    /// `SIGKILL` and restart on the same `--dir` and port.
    fn crash_and_restart(self) -> Server {
        let Server {
            mut guard,
            port,
            dir,
            mode,
        } = self;
        guard.kill_now();
        drop(guard);
        wait_for_port_down(port);
        let child = Command::new(find_moon_binary())
            .args(moon_args(&dir, port, mode))
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("restart moon");
        let server = Server {
            guard: ServerGuard::new(child),
            port,
            dir,
            mode,
        };
        assert!(
            server.pings(),
            "restarted moon on port {port} never answered PING"
        );
        server
    }
}

// ===========================================================================
// Corpus — built with the same writers the server uses.
// ===========================================================================

/// A key that routes to `shard` under `shards`, so its cold entry lands in the
/// cold index of the shard that will be asked for it.
fn key_on_shard(prefix: &str, shard: usize, shards: usize) -> String {
    (0u64..)
        .map(|n| format!("{prefix}:{n}"))
        .find(|k| moon::shard::dispatch::key_to_shard(k.as_bytes(), shards) == shard)
        .unwrap()
}

fn corpus_value(key: &str) -> String {
    let mut v = format!("corpus-value-of-{key}-");
    while v.len() < CORPUS_VALUE_LEN {
        v.push('c');
    }
    v
}

fn kv_entry(file_id: u64, page_count: u32, byte_size: u64) -> FileEntry {
    FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12,
        page_count,
        byte_size,
        created_lsn: 0,
        db_index: 0,
        max_key_hash: 0,
        last_modified_lsn: 0,
    }
}

/// Write, for every shard, one Active `KvLeaf` spill file per id in
/// `heap_ids` (each holding one corpus key routed to that shard), and —
/// when `warm_segment_id` is set — a warm vector segment under that id via
/// the real `transition_to_warm`. Returns the corpus keys with their values.
fn build_corpus(
    dir: &Path,
    shards: usize,
    heap_ids: &[u64],
    warm_segment_id: Option<u64>,
) -> BTreeMap<String, String> {
    let mut keys = BTreeMap::new();
    for shard in 0..shards {
        let sdir = shard_dir(dir, shard);
        std::fs::create_dir_all(&sdir).unwrap();
        let mut manifest = ShardManifest::create(&manifest_path(dir, shard)).unwrap();
        for &file_id in heap_ids {
            let key = key_on_shard(&format!("corpus:{shard}:{file_id}"), shard, shards);
            let value = corpus_value(&key);
            let entries = [SpillEntry {
                key: Bytes::copy_from_slice(key.as_bytes()),
                value_bytes: Bytes::copy_from_slice(value.as_bytes()),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
            }];
            let batch = build_kv_spill_batch(&entries, file_id).unwrap();
            let bytes = write_kv_spill_batch(&sdir, file_id, &batch).unwrap();
            manifest.add_file(kv_entry(file_id, batch.pages.len() as u32, bytes));
            keys.insert(key, value);
        }
        manifest.commit().unwrap();
        if let Some(seg_id) = warm_segment_id {
            moon::storage::tiered::warm_tier::transition_to_warm(
                &sdir,
                seg_id,
                seg_id,
                &[7u8; 256],
                &[0u8; 64],
                None,
                &[0u8; 64],
                &mut manifest,
                None,
            )
            .unwrap();
        }
    }
    keys
}

// ===========================================================================
// Observation.
// ===========================================================================

fn get(c: &mut Conn, key: &str) -> Option<String> {
    let raw = c.send(&["GET", key]);
    if raw.starts_with("$-1") || raw.starts_with("_\r\n") {
        return None;
    }
    assert!(raw.starts_with('$'), "GET {key} answered {raw:?}");
    let body = raw.split_once("\r\n").map(|x| x.1).unwrap();
    Some(body.trim_end_matches("\r\n").to_string())
}

fn write_filler(c: &mut Conn, round: usize) {
    let val = "f".repeat(FILLER_VALUE_LEN);
    let keys: Vec<String> = (0..FILLER_PER_ROUND)
        .map(|i| format!("filler:{round:05}:{i:03}"))
        .collect();
    let cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), &val]).collect();
    let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    let replies = c.pipeline(&refs);
    for line in replies.split("\r\n").filter(|l| l.starts_with('-')) {
        assert!(
            line.contains("OOM") || line.contains("backpressure"),
            "filler SET answered an unexpected error: {line:?}"
        );
    }
}

/// Every Active manifest entry of `shard`, as `(file_id, file_type)`.
fn active_entries(dir: &Path, shard: usize) -> Vec<(u64, u8)> {
    let m = ShardManifest::open(&manifest_path(dir, shard)).unwrap();
    m.files()
        .iter()
        .filter(|e| e.status == FileStatus::Active)
        .map(|e| (e.file_id, e.file_type))
        .collect()
}

/// The keys held by every Active `KvLeaf` file of every shard — ground truth
/// for "durably in the cold plane", read straight from the files.
fn durable_cold_keys(dir: &Path, shards: usize) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for shard in 0..shards {
        let data = shard_dir(dir, shard).join("data");
        for (file_id, file_type) in active_entries(dir, shard) {
            if file_type != PageType::KvLeaf as u8 {
                continue;
            }
            let path = data.join(format!("heap-{file_id:06}.mpf"));
            let pages = read_datafile(&path)
                .unwrap_or_else(|e| panic!("Active KvLeaf {path:?} unreadable: {e}"));
            for page in &pages {
                for slot in 0..page.slot_count() {
                    if let Some(e) = page.get(slot) {
                        keys.insert(String::from_utf8_lossy(&e.key).into_owned());
                    }
                }
            }
        }
    }
    keys
}

/// Keys among `keys` that `GET` answers null for.
fn absent_keys(port: u16, keys: &BTreeSet<String>) -> Vec<String> {
    let mut c = Conn::open(port);
    keys.iter()
        .filter(|k| get(&mut c, k).is_none())
        .cloned()
        .collect()
}

/// `BGREWRITEAOF` and wait, bounded, for it to finish successfully.
fn rewrite_aof(c: &mut Conn) {
    let reply = c.send(&["BGREWRITEAOF"]);
    assert!(reply.starts_with('+'), "BGREWRITEAOF answered {reply:?}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let info = c.send(&["INFO", "persistence"]);
        if info.contains("aof_rewrite_in_progress:0") {
            assert!(
                info.contains("aof_last_bgrewrite_status:ok"),
                "BGREWRITEAOF did not succeed:\n{info}"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "BGREWRITEAOF still running after 60s"
        );
    }
}

/// Settle for spill completions to reach the manifest — the spill thread
/// flushes on a 100 ms guard and completions apply on the eviction tick.
fn settle() {
    std::thread::sleep(Duration::from_secs(2));
}

// ===========================================================================
// Test 1 — moon#893: a vector segment id re-issued to a spill file.
// ===========================================================================

/// The corpus's heap files take ids 1 and 2; the warm segment takes 3, the
/// highest id in use.
const CORPUS_HEAP_IDS: [u64; 2] = [1, 2];
const WARM_SEGMENT_ID: u64 = 3;

fn vector_segment_id_reuse(mode: Mode) {
    let shards = mode.shards;
    let dir = common::unique_test_dir(&format!("fileid-893-s{shards}-aof{}", mode.aof));
    let corpus = build_corpus(&dir, shards, &CORPUS_HEAP_IDS, Some(WARM_SEGMENT_ID));

    // Model the segment directory the vector engine's GC half-swept: the
    // directory and its codes remain (so recovery reports it as a warm
    // segment), its `mvcc.mpf` does not (so `register_warm_segments` retires
    // the directory at this boot — the server's own code path, not the
    // test's). The manifest entry stays Active, exactly as in production.
    for shard in 0..shards {
        let seg = shard_dir(&dir, shard)
            .join("vectors")
            .join(format!("segment-{WARM_SEGMENT_ID}"));
        std::fs::remove_file(seg.join("mvcc.mpf")).unwrap();
    }

    let server = spawn(&dir, mode);
    let mut c = Conn::open(server.port);

    // Drive spills until EVERY shard has written at least one spill file of
    // its own (an Active KvLeaf the corpus did not create).
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut round = 0;
    loop {
        write_filler(&mut c, round);
        round += 1;
        settle_if(round % 10 == 0);
        let all_spilled = (0..shards).all(|s| {
            active_entries(&dir, s)
                .iter()
                .any(|&(id, t)| t == PageType::KvLeaf as u8 && !CORPUS_HEAP_IDS.contains(&id))
        });
        if all_spilled {
            break;
        }
        assert!(
            round < MAX_FILLER_ROUNDS && Instant::now() < deadline,
            "not every shard spilled within the filler budget — eviction is not tiering keys"
        );
    }
    settle();

    // Rewrite the AOF so the cold plane is the ONLY copy of every spilled
    // key — the rewritten base is hot-only (see `cold_replay_gate`). Without
    // this the pre-rewrite AOF replays the spilled keys hot on restart and
    // hides a lost cold entry; production reaches the same state on every
    // auto-rewrite.
    //
    // Not under tokio --shards 1: there the rewrite is in-place with an RDB
    // preamble whose load wipes the whole cold plane on restart (moon#1007,
    // PR #1008) — every cold key would read absent for a reason unrelated to
    // this test. That quadrant keeps the collision assertion below; the
    // `aof: false` variant carries the read-back consequence for it.
    let rewrite_wipes_cold_plane = cfg!(feature = "runtime-tokio") && shards == 1;
    if mode.aof && !rewrite_wipes_cold_plane {
        rewrite_aof(&mut c);
    }

    // The mechanism: no id may name two artifacts. Pre-fix the first spill
    // file after boot is named after the seed (3), the warm segment's id.
    let mut shared: Vec<(usize, u64)> = Vec::new();
    for shard in 0..shards {
        let entries = active_entries(&dir, shard);
        let vec_ids: BTreeSet<u64> = entries
            .iter()
            .filter(|&&(_, t)| t == PageType::VecCodes as u8)
            .map(|&(id, _)| id)
            .collect();
        for &(id, t) in &entries {
            if t == PageType::KvLeaf as u8 && vec_ids.contains(&id) {
                shared.push((shard, id));
            }
        }
    }

    let before_crash = durable_cold_keys(&dir, shards);
    for k in corpus.keys() {
        assert!(
            before_crash.contains(k),
            "corpus key {k} must be cold before the crash"
        );
    }
    assert!(
        before_crash.len() > corpus.len(),
        "the server must have spilled keys of its own before the crash"
    );

    let active_kv_before: Vec<(usize, u64)> = (0..shards)
        .flat_map(|s| {
            active_entries(&dir, s)
                .into_iter()
                .filter(|&(_, t)| t == PageType::KvLeaf as u8)
                .map(move |(id, _)| (s, id))
        })
        .collect();

    let server = server.crash_and_restart();

    // The consequence, read back through the server.
    let absent = absent_keys(server.port, &before_crash);
    // Which of the spill files that were live before the crash did recovery
    // tombstone? Reported, not asserted: the consequence above is the claim.
    let tombstoned_by_recovery: Vec<(usize, u64)> = active_kv_before
        .iter()
        .copied()
        .filter(|&(s, id)| {
            !active_entries(&dir, s)
                .iter()
                .any(|&(i, t)| i == id && t == PageType::KvLeaf as u8)
        })
        .collect();
    // Non-vacuity: the trigger fired — the server retired the segment's
    // directory at the first boot and its manifest entry at this one, so a
    // retirement by id alone had its chance to take a spill file with it.
    for shard in 0..shards {
        assert!(
            !active_entries(&dir, shard)
                .iter()
                .any(|&(id, t)| id == WARM_SEGMENT_ID && t == PageType::VecCodes as u8),
            "shard {shard}: the dirless warm segment entry must have been retired"
        );
    }
    let mut c = Conn::open(server.port);
    for (k, v) in &corpus {
        assert_eq!(
            get(&mut c, k).as_deref(),
            Some(v.as_str()),
            "corpus key {k} must survive (cold plane sanity)"
        );
    }
    assert!(
        shared.is_empty(),
        "--shards {shards}: a spill file was given an id a warm vector segment already \
         holds ((shard, id) = {shared:?}) — the restart seed ignored vector segments \
         (moon#893). After the restart {} of {} durable cold keys read as ABSENT \
         ({:?}…); live spill files tombstoned by recovery (shard, id): {:?}",
        absent.len(),
        before_crash.len(),
        absent.iter().take(3).collect::<Vec<_>>(),
        tombstoned_by_recovery
    );
    assert!(
        absent.is_empty(),
        "--shards {shards}: {} of {} keys that were durably in an Active spill file \
         before the crash read as ABSENT after it: {:?}; live spill files tombstoned \
         by recovery (shard, id): {:?}",
        absent.len(),
        before_crash.len(),
        absent.iter().take(5).collect::<Vec<_>>(),
        tombstoned_by_recovery
    );
    drop(server);
}

fn settle_if(yes: bool) {
    if yes {
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[test]
fn vector_segment_id_is_never_reissued_to_a_spill_file_1_shard() {
    vector_segment_id_reuse(Mode {
        shards: 1,
        aof: false,
    });
}

#[test]
fn vector_segment_id_is_never_reissued_to_a_spill_file_4_shards() {
    vector_segment_id_reuse(Mode {
        shards: 4,
        aof: false,
    });
}

#[test]
fn vector_segment_id_is_never_reissued_to_a_spill_file_1_shard_aof() {
    vector_segment_id_reuse(Mode {
        shards: 1,
        aof: true,
    });
}

#[test]
fn vector_segment_id_is_never_reissued_to_a_spill_file_4_shards_aof() {
    vector_segment_id_reuse(Mode {
        shards: 4,
        aof: true,
    });
}

// ===========================================================================
// Test 1b — moon#893 upgrade path: a data dir ALREADY holding the collision.
// ===========================================================================

/// The fixed seed stops new collisions; it cannot undo the ones a pre-fix
/// build already wrote. Here the manifest arrives holding a live spill file
/// and a dirless warm segment under the same id — exactly what test 1 leaves
/// behind — and the very first boot retires the segment entry. Retiring it
/// by id alone took the spill file with it.
fn already_collided(mode: Mode) {
    let shards = mode.shards;
    let dir = common::unique_test_dir(&format!("fileid-893-upgrade-s{shards}"));
    let corpus = build_corpus(&dir, shards, &[1, 2, 3], None);
    for shard in 0..shards {
        let mut m = ShardManifest::open(&manifest_path(&dir, shard)).unwrap();
        m.add_file(FileEntry {
            file_id: 3,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Active,
            tier: StorageTier::Warm,
            page_size_log2: 16,
            page_count: 1,
            byte_size: 256,
            created_lsn: 0,
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: 0,
        });
        m.commit().unwrap();
        assert!(
            !shard_dir(&dir, shard).join("vectors/segment-3").exists(),
            "precondition: the warm segment's directory is gone"
        );
    }

    let server = spawn(&dir, mode);
    let mut c = Conn::open(server.port);
    let absent: Vec<&String> = corpus
        .iter()
        .filter(|(k, v)| get(&mut c, k).as_deref() != Some(v.as_str()))
        .map(|(k, _)| k)
        .collect();
    assert!(
        absent.is_empty(),
        "--shards {shards}: corpus keys whose spill file shares id 3 with a retired \
         warm segment entry read as ABSENT or wrong after boot: {absent:?}"
    );
    drop(server);
}

#[test]
fn already_collided_manifest_keeps_its_spill_file_1_shard() {
    already_collided(Mode {
        shards: 1,
        aof: true,
    });
}

#[test]
fn already_collided_manifest_keeps_its_spill_file_4_shards() {
    already_collided(Mode {
        shards: 4,
        aof: true,
    });
}

// ===========================================================================
// Test 2 — moon#997: a cold dir that cannot be scanned.
// ===========================================================================

#[cfg(unix)]
struct RestorePerms(Vec<PathBuf>);

#[cfg(unix)]
impl Drop for RestorePerms {
    fn drop(&mut self) {
        use std::os::unix::fs::PermissionsExt;
        for p in &self.0 {
            let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o755));
        }
    }
}

// ===========================================================================
// Test 3 — moon#997 review: a torn manifest create must not block startup.
// ===========================================================================

/// A manifest shorter than its two root pages can only be a `create` that
/// died part-way (builds before create became atomic wrote it in place). It
/// holds no entry, so it must not make the file_id seed refuse every boot:
/// the server starts, re-creates it, spills through it, and recovers from it.
fn torn_manifest_create(shards: usize) {
    let dir = common::unique_test_dir(&format!("fileid-torn-manifest-s{shards}"));
    for shard in 0..shards {
        std::fs::create_dir_all(shard_dir(&dir, shard)).unwrap();
        std::fs::write(manifest_path(&dir, shard), vec![0u8; 100]).unwrap();
    }
    let mode = Mode { shards, aof: true };

    let port = common::reserve_port();
    let mut child = Command::new(find_moon_binary())
        .args(moon_args(&dir, port, mode))
        .stdout(Stdio::null())
        .stderr(common::server_stderr(&dir))
        .spawn()
        .expect("spawn moon");
    if let Some(code) = exited_within(&mut child, Duration::from_secs(10)) {
        let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
        panic!(
            "--shards {shards}: moon refused to start (exit {code}) over a 100-byte \
             manifest — a torn create that holds no entry. server.err:\n{log}"
        );
    }
    let server = Server {
        guard: ServerGuard::new(child),
        port,
        dir: dir.clone(),
        mode,
    };
    assert!(server.pings(), "moon never answered PING");

    // The manifest works again: spills register in it.
    let mut c = Conn::open(server.port);
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut round = 0;
    while !(0..shards).all(|s| {
        active_entries(&dir, s)
            .iter()
            .any(|&(_, t)| t == PageType::KvLeaf as u8)
    }) {
        write_filler(&mut c, round);
        round += 1;
        assert!(
            round < MAX_FILLER_ROUNDS && Instant::now() < deadline,
            "no shard registered a spill file in its re-created manifest"
        );
    }
    settle();
    let cold = durable_cold_keys(&dir, shards);
    let server = server.crash_and_restart();
    let absent = absent_keys(server.port, &cold);
    assert!(
        absent.is_empty(),
        "--shards {shards}: {} of {} keys spilled through the re-created manifest read \
         as absent after a restart: {:?}",
        absent.len(),
        cold.len(),
        absent.iter().take(3).collect::<Vec<_>>()
    );
    drop(server);
}

#[test]
fn torn_manifest_create_does_not_block_startup_1_shard() {
    torn_manifest_create(1);
}

#[test]
fn torn_manifest_create_does_not_block_startup_4_shards() {
    torn_manifest_create(4);
}

/// Wait for the child to exit on its own; `None` if it is still running.
fn exited_within(child: &mut std::process::Child, within: Duration) -> Option<i32> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status.code().unwrap_or(-1));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

#[cfg(unix)]
fn unreadable_cold_dir(shards: usize) {
    use std::os::unix::fs::PermissionsExt;

    let dir = common::unique_test_dir(&format!("fileid-997-s{shards}"));
    let corpus = build_corpus(&dir, shards, &[1, 2, 3], None);
    let originals: Vec<(PathBuf, Vec<u8>)> = (0..shards)
        .flat_map(|s| {
            let data = shard_dir(&dir, s).join("data");
            [1u64, 2, 3].map(|id| {
                let p = data.join(format!("heap-{id:06}.mpf"));
                let b = std::fs::read(&p).unwrap();
                (p, b)
            })
        })
        .collect();

    // `-wx`: the directory cannot be listed, but files in it can be created,
    // opened and renamed — exactly what a spill does.
    let data_dirs: Vec<PathBuf> = (0..shards)
        .map(|s| shard_dir(&dir, s).join("data"))
        .collect();
    let restore = RestorePerms(data_dirs.clone());
    for d in &data_dirs {
        std::fs::set_permissions(d, std::fs::Permissions::from_mode(0o300)).unwrap();
    }
    if std::fs::read_dir(&data_dirs[0]).is_ok() {
        // Permission bits do not bind this user (root). The condition under
        // test cannot be created here; the unit tests in file_id_seed.rs
        // cover the same decision with an injected error.
        eprintln!("SKIP: read_dir on a 0o300 directory succeeded (running as root?)");
        return;
    }

    let port = common::reserve_port();
    let mut child = Command::new(find_moon_binary())
        .args(moon_args(&dir, port, Mode { shards, aof: true }))
        .stdout(Stdio::null())
        .stderr(common::server_stderr(&dir))
        .spawn()
        .expect("spawn moon");

    if let Some(code) = exited_within(&mut child, Duration::from_secs(15)) {
        // Fail-closed: the server refused to start.
        let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
        assert_ne!(code, 0, "refusing to start must exit non-zero");
        assert!(
            log.contains("file_id"),
            "the refusal must name the file_id seed; server.err:\n{log}"
        );
        for (p, b) in &originals {
            assert_eq!(&std::fs::read(p).unwrap(), b, "{p:?} must be untouched");
        }
        // And it is recoverable: restore the permission, start, read.
        drop(restore);
        let server = spawn(&dir, Mode { shards, aof: true });
        let mut c = Conn::open(server.port);
        for (k, v) in &corpus {
            assert_eq!(
                get(&mut c, k).as_deref(),
                Some(v.as_str()),
                "corpus key {k}"
            );
        }
        return;
    }

    // The server started on a seed it could not prove. Drive spills and
    // watch the live corpus files.
    let guard = ServerGuard::new(child);
    let mut c = Conn::open(port);
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut round = 0;
    let clobbered = loop {
        write_filler(&mut c, round);
        round += 1;
        let changed: Vec<&PathBuf> = originals
            .iter()
            .filter(|(p, b)| std::fs::read(p).map(|now| &now != b).unwrap_or(true))
            .map(|(p, _)| p)
            .collect();
        if !changed.is_empty() {
            break format!("{changed:?}");
        }
        let spilled_past_corpus = data_dirs.iter().all(|d| d.join("heap-000004.mpf").exists());
        if spilled_past_corpus || round >= MAX_FILLER_ROUNDS || Instant::now() >= deadline {
            break String::new();
        }
    };
    drop(guard);
    drop(restore);
    panic!(
        "--shards {shards}: moon STARTED although it could not scan its cold directory \
         (moon#997 fail-open seed). Live spill files overwritten while it ran: [{clobbered}]"
    );
}

#[cfg(unix)]
#[test]
fn unreadable_cold_dir_refuses_to_start_1_shard() {
    unreadable_cold_dir(1);
}

#[cfg(unix)]
#[test]
fn unreadable_cold_dir_refuses_to_start_4_shards() {
    unreadable_cold_dir(4);
}
