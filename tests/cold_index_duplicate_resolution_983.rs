//! moon#983: a key with two on-disk copies must recover to the NEWER one.
//!
//! ## The bug
//!
//! `ColdIndex::rebuild_from_manifest_per_db` resolved a key present in two
//! Active heap files by "last one seen wins", where "last" was MANIFEST PUSH
//! ORDER. Push order is not recency order: the async spill path pushes a file
//! when its background completion is applied, the durable-batch path pushes at
//! eviction time, so a `CONFIG SET appendonly` flip with a completion still in
//! flight registers a HIGHER `file_id` ahead of a LOWER one — and the manifest
//! preserves that order across every restart. The rebuild then pointed the key
//! at the older file, and cold read-through answered the superseded value
//! after recovery, with no error and no log line. The fix orders by
//! `ColdLocation::recency_key()` — `(file_id, page_idx, slot_idx)`, the spill
//! allocation sequence — and ignores manifest order.
//!
//! ## Two tests, deliberately different in what they can prove
//!
//! * `respilled_key_recovers_to_its_newest_copy_after_sigkill_*` drives the
//!   REAL lifecycle through the server: spill, overwrite, re-spill, `SIGKILL`,
//!   restart. On the normal in-order path this was already correct before
//!   the fix (manifest order and `file_id` order agree there), so this is the
//!   regression guard for the lifecycle itself — it proves the fix did not
//!   break the common case, and it refuses a vacuous pass (the key must have
//!   two copies on disk before the crash).
//!
//! * `newest_first_manifest_recovers_to_higher_file_id_*` is the one that
//!   FAILS on the pre-fix binary. It builds the exact on-disk state the race
//!   above leaves behind — two Active files, the newer one registered first —
//!   with the same library calls the spill thread uses, then boots the REAL
//!   server binary on it. Pre-fix: `GET` answers the stale value. A private
//!   key that lives only in the older file is read alongside, so a pass can
//!   never come from the older file being ignored altogether.
//!
//! The lifecycle test waits for what it needs on disk — the key's copies,
//! and their manifest entries — up to [`slow_host::CONDITION_DEADLINE`],
//! never for a fixed round count or a fixed sleep, and re-sends a filler
//! write moon refused for AOF backpressure (moon#1065). A Windows runner
//! stall failed the fixed versions 3 tries of 3: 1000 rounds went by with the
//! spill writer stalled, and the filler asserted on the refusal.
//!
//! Run with:
//!   MOON_BIN=/path/to/moon cargo test --release \
//!     --test cold_index_duplicate_resolution_983

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::slow_host::{self, WriteStats};
use common::{Conn, ServerGuard, find_moon_binary, wait_for_port_down};

/// Small enough that the filler crosses it quickly, large enough that the
/// operations under test are not answered `-OOM`.
const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
/// Past `CompactValue`'s inline limit, so the key is a heap value that tiers.
const VALUE_LEN: usize = 256;
const FILLER_VALUE_LEN: usize = 1024;
/// Filler keys per pipeline before re-checking the disk.
const FILLER_PER_ROUND: usize = 100;
/// Filler rounds per phase at full speed — ~100 MiB through an 8 MiB budget,
/// far more than a spill needs. Past it more filler cannot help: the key is
/// evicted and its spill has to land, which a stalled spill writer delays
/// (moon#1065 — the old hard cap of 1000 rounds went by in seconds with the
/// writer stalled, every write answered `-OOM`). The filler then continues at
/// a trickle until [`slow_host::CONDITION_DEADLINE`].
const FULL_SPEED_ROUNDS: usize = 1000;
/// The key under test. Unique enough that a raw byte search of the heap
/// files cannot match a filler key or a value.
const KEY: &str = "k983:respilled:key";

// ===========================================================================
// Server harness — same shape as cold_reconciliation_property_660.
// ===========================================================================

struct Server {
    guard: ServerGuard,
    port: u16,
    dir: PathBuf,
    shards: usize,
}

fn offload_dir(dir: &Path) -> PathBuf {
    dir.join("off")
}

fn moon_args(dir: &Path, port: u16, shards: usize) -> Vec<String> {
    vec![
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        dir.to_string_lossy().into_owned(),
        "--shards".into(),
        shards.to_string(),
        "--disk-offload".into(),
        "enable".into(),
        "--disk-offload-dir".into(),
        offload_dir(dir).to_string_lossy().into_owned(),
        // The durability backstop: without it victims are DROPPED, not
        // spilled, and no cold state exists to recover.
        "--appendonly".into(),
        "yes".into(),
        "--appendfsync".into(),
        "everysec".into(),
        "--maxmemory".into(),
        MAXMEMORY_BYTES.to_string(),
        "--maxmemory-policy".into(),
        "allkeys-lru".into(),
        // Under test is recovery ordering, not the disk guard.
        "--disk-free-min-pct".into(),
        "0".into(),
        "--protected-mode".into(),
        "no".into(),
    ]
}

fn spawn(dir: &Path, shards: usize) -> Server {
    std::fs::create_dir_all(offload_dir(dir)).expect("create offload dir");
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args(moon_args(dir, port, shards))
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    assert!(
        serving(port),
        "moon never answered PING on port {port} after start-up"
    );
    Server {
        guard,
        port,
        dir: dir.to_path_buf(),
        shards,
    }
}

/// Poll until a real `+PONG` comes back — `SO_REUSEPORT` means a successful
/// connect proves nothing about which process, if any, is serving.
fn serving(port: u16) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut s) = TcpStream::connect_timeout(
            &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            Duration::from_millis(200),
        ) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 7];
            if s.write_all(b"PING\r\n").is_ok()
                && s.read_exact(&mut buf).is_ok()
                && buf.starts_with(b"+PONG")
            {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

impl Server {
    /// `SIGKILL` and restart on the same `--dir` — the only way to drive
    /// Phase 3 (cold index rebuild from the manifest). A graceful shutdown
    /// would let the server tidy up and would not exercise the seam.
    fn crash_and_restart(self) -> Server {
        let Server {
            mut guard,
            port,
            dir,
            shards,
        } = self;
        guard.kill_now();
        drop(guard);
        wait_for_port_down(port);

        let child = Command::new(find_moon_binary())
            .args(moon_args(&dir, port, shards))
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("restart moon");
        let guard = ServerGuard::new(child);
        assert!(
            serving(port),
            "restarted moon on port {port} never answered PING; every assertion \
             below would be measuring a server that is not up"
        );
        Server {
            guard,
            port,
            dir,
            shards,
        }
    }
}

// ===========================================================================
// RESP helpers.
// ===========================================================================

/// `Some(value)` for a bulk reply, `None` for a null. Anything else is a
/// harness bug or an error reply and must not read as "absent".
fn parse_get(raw: &str) -> Option<String> {
    if raw.starts_with("$-1") || raw.starts_with("_\r\n") {
        return None;
    }
    assert!(
        raw.starts_with('$'),
        "GET answered neither a bulk string nor a null: {raw:?}"
    );
    let body = raw.split_once("\r\n").map(|x| x.1).expect("bulk body");
    Some(body.trim_end_matches("\r\n").to_string())
}

fn get(c: &mut Conn, key: &str) -> Option<String> {
    parse_get(&c.send(&["GET", key]))
}

/// `SET key value`, which the test needs APPLIED: an AOF backpressure
/// refusal is re-sent (moon#1065), and `-OOM` fails the test, as it did.
fn set_applied(c: &mut Conn, key: &str, value: &str, dir: &Path, what: &str) {
    let mut stats = WriteStats::default();
    let cmd = vec!["SET".to_string(), key.to_string(), value.to_string()];
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &[cmd], 1, &mut stats, deadline, dir);
    assert_eq!(
        stats.accepted, 1,
        "{what} must be applied or the test measures nothing ({stats})"
    );
}

/// A value whose first bytes name the generation, padded past the inline
/// limit. Distinct per generation so a stale answer is unmistakable.
fn value_for(generation: &str) -> String {
    let mut s = format!("{generation}-");
    while s.len() < VALUE_LEN {
        s.push('x');
    }
    s
}

/// Write one pipeline of filler. `-OOM` is a legitimate answer under
/// pressure (the write did not happen); an AOF backpressure refusal is
/// re-sent after a backoff (moon#1065). Both are counted in `stats`; any
/// other error is a hard failure.
fn write_filler(c: &mut Conn, phase: usize, round: usize, dir: &Path, stats: &mut WriteStats) {
    let val = "f".repeat(FILLER_VALUE_LEN);
    let cmds: Vec<Vec<String>> = (0..FILLER_PER_ROUND)
        .map(|i| {
            vec![
                "SET".to_string(),
                format!("filler:{phase}:{round:04}:{i:03}"),
                val.clone(),
            ]
        })
        .collect();
    let deadline = Instant::now() + slow_host::CONDITION_DEADLINE;
    slow_host::write_all(c, &cmds, FILLER_PER_ROUND, stats, deadline, dir);
}

// ===========================================================================
// On-disk evidence.
// ===========================================================================

/// Every heap file under `<off>/shard-*/data/` whose raw bytes contain
/// `needle`. Keys are stored verbatim in `KvLeafPage` slots, so this finds
/// the on-disk copies of a key without depending on any in-process index.
/// See [`registered_copies`] for the ones the rebuild will read.
fn heap_files_holding(off: &Path, needle: &[u8]) -> Vec<PathBuf> {
    let mut hits = Vec::new();
    let Ok(shards) = std::fs::read_dir(off) else {
        return hits;
    };
    for shard in shards.flatten() {
        let data = shard.path().join("data");
        let Ok(files) = std::fs::read_dir(&data) else {
            continue;
        };
        for f in files.flatten() {
            let p = f.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !(name.starts_with("heap-") && name.ends_with(".mpf")) {
                continue;
            }
            if let Ok(bytes) = std::fs::read(&p)
                && bytes.windows(needle.len()).any(|w| w == needle)
            {
                hits.push(p);
            }
        }
    }
    hits.sort();
    hits
}

/// Keep writing filler until `KEY` has at least `copies` registered on-disk
/// copies ([`registered_copies`]).
/// Bounded by [`slow_host::CONDITION_DEADLINE`], not a round count: a phase
/// that never tiers the key is a harness failure, reported with the filler's
/// counts and the server's INFO and log, never an infinite loop.
fn fill_until_copies(
    c: &mut Conn,
    dir: &Path,
    phase: usize,
    copies: usize,
    stats: &mut WriteStats,
) -> Vec<PathBuf> {
    let off = offload_dir(dir);
    let start = Instant::now();
    let mut round = 0;
    loop {
        let applied_before = stats.accepted;
        write_filler(c, phase, round, dir, stats);
        round += 1;
        let hits = registered_copies(&off);
        if hits.len() >= copies {
            eprintln!(
                "phase {phase}: {KEY} has {} registered on-disk copies after {round} filler \
                 rounds, {:?} ({stats})",
                hits.len(),
                start.elapsed()
            );
            return hits;
        }
        if start.elapsed() >= slow_host::CONDITION_DEADLINE {
            panic!(
                "phase {phase}: {KEY} did not reach {copies} registered on-disk copies within \
                 {:?} (have {}, and {} heap files hold its bytes; {round} filler rounds, \
                 {stats}) — eviction is not tiering the key{}",
                start.elapsed(),
                hits.len(),
                heap_files_holding(&off, KEY.as_bytes()).len(),
                slow_host::diagnostics(slow_host::peer_port(c), dir)
            );
        }
        // Past the full-speed budget, or when the server could absorb none
        // of the round, more filler only floods a stalled disk: trickle.
        if round >= FULL_SPEED_ROUNDS || stats.accepted == applied_before {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// The manifest of the shard directory holding `heap` (`<off>/shard-N/data/
/// heap-ID.mpf`) lists `heap` as an Active file — the spill completion has
/// landed, so a restart rebuilds the cold index with it.
fn registered_active(heap: &Path) -> bool {
    use moon::persistence::manifest::{FileStatus, ShardManifest};
    let Some(id) = heap.file_name().and_then(|n| n.to_str()).and_then(|n| {
        n.strip_prefix("heap-")?
            .strip_suffix(".mpf")?
            .parse::<u64>()
            .ok()
    }) else {
        return false;
    };
    let Some(shard_dir) = heap.parent().and_then(Path::parent) else {
        return false;
    };
    let Some(shard) = shard_dir.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    ShardManifest::open(&shard_dir.join(format!("{shard}.manifest"))).is_ok_and(|m| {
        m.files()
            .iter()
            .any(|e| e.file_id == id && e.status == FileStatus::Active)
    })
}

/// The copies of `KEY` the restart's cold-index rebuild will read: heap files
/// holding it that their manifest lists as Active, i.e. whose spill
/// completion has landed (a fixed one-second sleep stood for that). A spill
/// whose completion was withdrawn — its `MOON.SPILLED` marker met AOF
/// backpressure — leaves an unregistered file holding the key's bytes that
/// the rebuild ignores and the orphan sweep removes: not a copy (moon#1065).
fn registered_copies(off: &Path) -> Vec<PathBuf> {
    heap_files_holding(off, KEY.as_bytes())
        .into_iter()
        .filter(|h| registered_active(h))
        .collect()
}

// ===========================================================================
// Test 1 — the real lifecycle: spill, overwrite, re-spill, SIGKILL, restart.
// ===========================================================================

fn respilled_key_lifecycle(shards: usize) {
    let dir = common::unique_test_dir(&format!("cold-dup-983-life-s{shards}"));
    let server = spawn(&dir, shards);
    let mut c = Conn::open(server.port);
    let mut stats = WriteStats::default();

    let v1 = value_for("v1-first-spill");
    let v2 = value_for("v2-after-overwrite");

    // Generation 1: write, then push it out to the cold tier.
    set_applied(&mut c, KEY, &v1, &dir, "SET v1");
    let first = fill_until_copies(&mut c, &dir, 1, 1, &mut stats);

    // Overwrite while the first copy stays on disk (its file also holds
    // filler keys, so nothing reclaims it), then push the new value out too.
    set_applied(&mut c, KEY, &v2, &dir, "the overwrite SET v2");
    let both = fill_until_copies(&mut c, &dir, 2, 2, &mut stats);

    // Non-vacuity: two DISTINCT files hold the key before the crash, and the
    // first one is still among them (not reclaimed, not rewritten).
    assert!(
        both.len() >= 2,
        "expected the key in >= 2 heap files before the crash, found {both:?}"
    );
    assert!(
        both.contains(&first[0]),
        "the first spill file {:?} must still exist alongside the re-spill ({both:?})",
        first[0]
    );

    // Live: the key is present (EXISTS does not promote; GET would, and a
    // hot copy at crash time would let the AOF replay mask the cold plane).
    assert!(
        c.send(&["EXISTS", KEY]).starts_with(":1"),
        "key must exist live before the crash"
    );

    // The SIGKILL must not lose an acknowledged write the checks below count
    // on: wait for the AOF writer to have written the last one.
    slow_host::wait_aof_holds_last(&stats, server.port, &dir);
    eprintln!("filler over both phases: {stats}");

    let server = server.crash_and_restart();
    let mut c = Conn::open(server.port);
    let got = get(&mut c, KEY);
    assert_eq!(
        got.as_deref(),
        Some(v2.as_str()),
        "after SIGKILL + restart at --shards {shards}, {KEY} must answer the NEWER \
         value (v2-after-overwrite…); got {:?}",
        got.as_deref().map(|v| &v[..v.len().min(24)])
    );
    // The cold plane works at all: a phase-1 filler key that was tiered
    // long before the crash still reads back.
    let filler_key = "filler:1:0000:000";
    let f = get(&mut c, filler_key);
    assert_eq!(
        f.as_deref().map(|v| v.len()),
        Some(FILLER_VALUE_LEN),
        "phase-1 filler key {filler_key} must read back from the cold tier after restart; got {:?}",
        f.as_deref().map(|v| &v[..v.len().min(24)])
    );
    drop(server);
    // Passed: the data dir is evidence only on failure.
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn respilled_key_recovers_to_its_newest_copy_after_sigkill_1_shard() {
    respilled_key_lifecycle(1);
}

#[test]
fn respilled_key_recovers_to_its_newest_copy_after_sigkill_4_shards() {
    respilled_key_lifecycle(4);
}

// ===========================================================================
// Test 2 — the reordered manifest: the state the race leaves behind.
// ===========================================================================

const PRIVATE_KEY: &str = "k983:only-in-older-file";
const STALE: &str = "stale-value-from-file-1";
const FRESH: &str = "fresh-value-from-file-2";
const PRIVATE: &str = "private-value-from-file-1";

/// Build, for every shard, two Active KvLeaf files — 1 (older: `KEY` =
/// stale + `PRIVATE_KEY`) and 2 (newer: `KEY` = fresh) — registered in the
/// manifest NEWEST FIRST. Uses the same writers the spill thread does, so
/// the pages are byte-for-byte what a real spill produces; only the
/// registration order is chosen here, and that is the whole point.
fn build_newest_first_corpus(off: &Path, shards: usize) {
    use bytes::Bytes;
    use moon::persistence::kv_page::ValueType;
    use moon::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
    use moon::persistence::page::PageType;
    use moon::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

    let entry = |k: &str, v: &str| SpillEntry {
        key: Bytes::copy_from_slice(k.as_bytes()),
        value_bytes: Bytes::copy_from_slice(v.as_bytes()),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
    };

    for shard in 0..shards {
        let shard_dir = off.join(format!("shard-{shard}"));
        std::fs::create_dir_all(&shard_dir).unwrap();
        let mut written: Vec<(u64, u32, u64)> = Vec::new(); // (file_id, pages, bytes)
        for (file_id, entries) in [
            (1u64, vec![entry(KEY, STALE), entry(PRIVATE_KEY, PRIVATE)]),
            (2u64, vec![entry(KEY, FRESH)]),
        ] {
            let batch = build_kv_spill_batch(&entries, file_id).unwrap();
            let bytes = write_kv_spill_batch(&shard_dir, file_id, &batch).unwrap();
            written.push((file_id, batch.pages.len() as u32, bytes));
        }
        let manifest_path = shard_dir.join(format!("shard-{shard}.manifest"));
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        // NEWEST FIRST — the order a durable batch committed while an async
        // completion was still in flight leaves in the manifest.
        for &(file_id, page_count, byte_size) in written.iter().rev() {
            manifest
                .add_file(FileEntry {
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
                })
                .unwrap();
        }
        manifest.commit().unwrap();
        let ids: Vec<u64> = manifest.files().iter().map(|e| e.file_id).collect();
        assert_eq!(
            ids,
            vec![2, 1],
            "precondition: manifest registers the newer file first"
        );
    }
}

fn newest_first_manifest(shards: usize) {
    let dir = common::unique_test_dir(&format!("cold-dup-983-order-s{shards}"));
    let off = offload_dir(&dir);
    std::fs::create_dir_all(&off).unwrap();
    build_newest_first_corpus(&off, shards);

    let server = spawn(&dir, shards);
    let mut c = Conn::open(server.port);

    // Guard against a vacuous pass: the OLDER file must be readable at all,
    // or "fresh" could mean "file 1 was never opened".
    assert_eq!(
        get(&mut c, PRIVATE_KEY).as_deref(),
        Some(PRIVATE),
        "the older file's private key must be served — file 1 is Active and \
         must be part of the rebuilt index"
    );
    let got = get(&mut c, KEY);
    assert_eq!(
        got.as_deref(),
        Some(FRESH),
        "at --shards {shards}, a key held by two Active files must recover to the \
         HIGHER file_id (the re-spill) even though the manifest lists it first; \
         got {got:?} (moon#983)"
    );

    // And again across a crash: the running server has since committed its
    // own manifest epochs on top; the ordering must survive that too.
    let server = server.crash_and_restart();
    let mut c = Conn::open(server.port);
    let got = get(&mut c, KEY);
    assert_eq!(
        got.as_deref(),
        Some(FRESH),
        "after a further SIGKILL + restart at --shards {shards}, {KEY} must still \
         answer the newer copy; got {got:?}"
    );
    drop(server);
}

#[test]
fn newest_first_manifest_recovers_to_higher_file_id_1_shard() {
    newest_first_manifest(1);
}

#[test]
fn newest_first_manifest_recovers_to_higher_file_id_4_shards() {
    newest_first_manifest(4);
}
