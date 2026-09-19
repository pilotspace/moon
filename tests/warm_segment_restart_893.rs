//! moon#893: boot recovery must never delete the warm vector segment it is
//! serving from, and must supersede a warm segment PER KEY.
//!
//! Every test drives a real server through a HOT -> WARM transition and then
//! restarts it TWICE on the same data dir, asserting after each boot that:
//!
//! * the `vectors/segment-<id>/` directories survive,
//! * no vector was re-encoded (`vector_version_token` counts the rescan's
//!   re-inserts; a boot that attaches every warm segment leaves it near 0),
//! * `FT.SEARCH` serves every key's CURRENT vector, identically on both boots.
//!
//! The three scenarios:
//!
//! * `one_reinserted_key_keeps_its_warm_siblings_*` — 1000 keys go warm, ONE
//!   is re-inserted and compacted into a HOT segment. Recovery's
//!   `already_covered` was `.any()` over the segment's key_hashes: the single
//!   overlap retired the whole directory, and the other 999 keys were
//!   re-encoded on every boot.
//! * `a_key_in_two_warm_segments_serves_its_current_vector_*` — the same, but
//!   the re-inserted key's segment goes warm too. `.any()` attached the older
//!   segment, registered the key under its NEW global_id, then deleted the
//!   NEWER directory as "covered": after the restart the key answered to its
//!   OVERWRITTEN vector and its current one was gone.
//! * `a_manifest_listing_a_segment_twice_*` — the upgrade path. A manifest an
//!   older build wrote (id counter re-issuing live ids, `add_file` appending
//!   without a check) lists the segment id twice. Recovery attached the
//!   directory on the first entry and `remove_dir_all`ed it on the second.
//!   The corpus is produced by appending a duplicate entry to the manifest
//!   byte for byte, exactly as the old writer laid it down.
//!
//! Run with (pin the binary you just built):
//!   MOON_BIN=target/release/moon cargo test --release --test warm_segment_restart_893
//!   cargo test --release --no-default-features --features runtime-tokio,jemalloc \
//!     --test warm_segment_restart_893

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use moon::persistence::manifest::{FileEntry, FileStatus, MAX_INLINE_ENTRIES, ShardManifest};
use moon::persistence::page::{MOONPAGE_HEADER_SIZE, MoonPageHeader, PAGE_4K, PageType};

use common::{Conn, ServerGuard, find_moon_binary, server_stderr, spawn_listening_guarded};

const DIM: usize = 16;
const N: usize = 1000;
/// The key re-inserted with a new vector after its segment went warm.
const REINSERTED: usize = 7;
/// A rescan that re-encodes the warm tier bumps the token once per key (~N);
/// a boot that attaches it leaves the token near zero.
const NO_REENCODE_MAX_TOKEN: i64 = (N / 10) as i64;

// ---------------------------------------------------------------------------
// Vectors: printable-ASCII bytes, so they travel through the shared `Conn`
// (&str) unchanged. Each f32 is [ascii, ascii, ascii, 0x3F..=0x41] — a finite
// value between ~0.5 and ~12, so L2 distances never overflow.
// ---------------------------------------------------------------------------

fn vector(seed: u64) -> String {
    let mut out = String::with_capacity(DIM * 4);
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
    for _ in 0..DIM {
        for _ in 0..3 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            out.push(char::from(0x21 + (s % 94) as u8));
        }
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        out.push(char::from(0x3F + (s % 3) as u8));
    }
    out
}

fn original(i: usize) -> String {
    vector(i as u64 + 1)
}

fn reinserted() -> String {
    vector(1_000_003)
}

// ---------------------------------------------------------------------------
// Minimal RESP reader over the framed raw reply `Conn` returns.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum V {
    Str(String),
    Int(i64),
    Arr(Vec<V>),
    Nil,
}

fn parse(raw: &str) -> V {
    fn one(s: &str, i: &mut usize) -> V {
        let end = *i + s[*i..].find("\r\n").unwrap();
        let (tag, line) = (&s[*i..*i + 1], &s[*i + 1..end]);
        *i = end + 2;
        match tag {
            "+" => V::Str(line.to_owned()),
            "-" if line.starts_with("LOADING") => panic!(
                "server error reply: {line} — a command reached the server before \
                 `await_loaded` saw loading finish"
            ),
            "-" => panic!("server error reply: {line}"),
            ":" => V::Int(line.parse().unwrap()),
            "$" => {
                let n: i64 = line.parse().unwrap();
                if n < 0 {
                    return V::Nil;
                }
                let v = s[*i..*i + n as usize].to_owned();
                *i += n as usize + 2;
                V::Str(v)
            }
            "*" => {
                let n: i64 = line.parse().unwrap();
                V::Arr((0..n.max(0)).map(|_| one(s, i)).collect())
            }
            other => panic!("unexpected RESP tag {other:?} in {s:?}"),
        }
    }
    let mut i = 0;
    one(raw, &mut i)
}

// ---------------------------------------------------------------------------
// Server lifecycle.
// ---------------------------------------------------------------------------

struct Server {
    guard: ServerGuard,
    port: u16,
}

fn start(dir: &Path, warm_after_secs: u64) -> Server {
    let (guard, port) = spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "yes",
                "--segment-warm-after",
                &warm_after_secs.to_string(),
                "--engine-offload-idle-secs",
                "0",
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(dir)
            .env("RUST_LOG", "moon=info")
            // The recovery log lines (what was attached, what was retired) are
            // the first thing to read when an assertion here fires.
            .stdout(
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join("moon.log"))
                    .map(Stdio::from)
                    .unwrap_or_else(|_| Stdio::null()),
            )
            .stderr(server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    await_loaded(port);
    Server { guard, port }
}

/// Block until the server has finished loading, not merely until it accepts.
///
/// The listener accepts connections BEFORE recovery ends — vector recovery
/// (warm reattach, keyspace rescan) runs after it — and until then every data
/// and FT.* command is refused with `-LOADING` (moon#476). That window is
/// milliseconds on a fast macOS host and long enough on a loaded Linux runner
/// to swallow the first command of every boot. Done means both: INFO reports
/// `loading:0`, and a keyspace read is answered rather than refused.
fn await_loaded(port: u16) {
    const DEADLINE: Duration = Duration::from_secs(180);
    let start = Instant::now();
    let mut refused = 0u32;
    loop {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "persistence"]);
        let read = c.send(&["EXISTS", "moon893:probe"]);
        if info.contains("loading:0\r\n") && read.starts_with(':') {
            // Printed with the test's output, so a CI log shows how long the
            // loading window really was on that host.
            eprintln!(
                "await_loaded: port {port} loaded after {:?} ({refused} polls answered loading)",
                start.elapsed()
            );
            return;
        }
        refused += 1;
        let last = format!("INFO loading line: {:?}; EXISTS: {read:?}", {
            info.lines()
                .find(|l| l.starts_with("loading:"))
                .unwrap_or("<absent>")
        });
        assert!(
            start.elapsed() < DEADLINE,
            "server still loading after {DEADLINE:?}; last replies: {last}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

impl Server {
    fn conn(&self) -> Conn {
        Conn::open(self.port)
    }

    /// Crash: SIGKILL, no shutdown path at all.
    fn kill9(mut self) {
        self.guard.kill_now();
    }

    /// Stop the server the way `crash` asks for.
    fn stop(self, crash: bool) {
        if crash { self.kill9() } else { self.shutdown() }
    }

    /// Clean shutdown: SHUTDOWN, then wait (bounded) for the process to exit.
    fn shutdown(mut self) {
        use std::io::Write as _;
        let mut c = self.conn();
        let _ = c.sock.write_all(&common::encode(&["SHUTDOWN"]));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.guard.as_mut().try_wait().unwrap().is_some() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "server did not exit within 30s of SHUTDOWN"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

// ---------------------------------------------------------------------------
// FT.* helpers.
// ---------------------------------------------------------------------------

fn ft_info(c: &mut Conn) -> Vec<(String, V)> {
    let V::Arr(items) = parse(&c.send(&["FT.INFO", "idx"])) else {
        panic!("FT.INFO is not an array");
    };
    items
        .chunks(2)
        .filter_map(|kv| match kv {
            [V::Str(k), v] => Some((k.clone(), v.clone())),
            _ => None,
        })
        .collect()
}

fn info_int(c: &mut Conn, field: &str) -> i64 {
    match ft_info(c).into_iter().find(|(k, _)| k == field) {
        Some((_, V::Int(n))) => n,
        other => panic!("FT.INFO {field}: {other:?}"),
    }
}

/// Top-3 keys for `blob`, nearest first.
fn knn(c: &mut Conn, blob: &str) -> Vec<String> {
    let V::Arr(items) = parse(&c.send(&[
        "FT.SEARCH",
        "idx",
        "*=>[KNN 3 @vec $q]",
        "PARAMS",
        "2",
        "q",
        blob,
        "DIALECT",
        "2",
    ])) else {
        panic!("FT.SEARCH is not an array");
    };
    items
        .iter()
        .skip(1)
        .filter_map(|v| match v {
            V::Str(k) if k.starts_with("doc:") => Some(k.clone()),
            _ => None,
        })
        .collect()
}

fn wait_for(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn shard_dir(dir: &Path) -> PathBuf {
    dir.join("shard-0")
}

fn segment_dirs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(shard_dir(dir).join("vectors"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("segment-"))
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// The index's Stack-B directory (`shard-0/idx-<hex>`).
fn index_dir(dir: &Path) -> PathBuf {
    std::fs::read_dir(shard_dir(dir))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("idx-"))
        })
        .expect("no idx-* directory: the index was never persisted")
}

fn hot_segment_ids(dir: &Path) -> Vec<u64> {
    moon::vector::persistence::manifest::read_manifest_tolerant(&index_dir(dir))
        .map(|m| m.segment_ids)
        .unwrap_or_default()
}

/// Boot 0: create the index, insert N keys, compact, and wait until the
/// segment is WARM. Returns the server, still running.
fn seed_warm_index(dir: &Path) -> Server {
    let srv = start(dir, 1);
    let mut c = srv.conn();
    let r = c.send(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "vec",
        "VECTOR",
        "HNSW",
        "6",
        "TYPE",
        "FLOAT32",
        "DIM",
        &DIM.to_string(),
        "DISTANCE_METRIC",
        "L2",
    ]);
    assert_eq!(r, "+OK\r\n", "FT.CREATE: {r}");
    for chunk in (0..N).collect::<Vec<_>>().chunks(100) {
        let keys: Vec<String> = chunk.iter().map(|i| format!("doc:{i}")).collect();
        let vecs: Vec<String> = chunk.iter().map(|&i| original(i)).collect();
        let cmds: Vec<[&str; 4]> = keys
            .iter()
            .zip(&vecs)
            .map(|(k, v)| ["HSET", k.as_str(), "vec", v.as_str()])
            .collect();
        let refs: Vec<&[&str]> = cmds.iter().map(|c| &c[..]).collect();
        let replies = c.pipeline(&refs);
        assert!(!replies.contains('-'), "HSET failed: {replies}");
    }
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    wait_for("the segment to go WARM", 120, || {
        info_int(&mut c, "warm_segments") == 1
    });
    assert_eq!(segment_dirs(dir).len(), 1, "one warm segment directory");
    srv
}

/// Re-insert `doc:REINSERTED` with a new vector and compact it into a HOT
/// segment. With `wait_persisted`, also wait until Stack B has persisted that
/// segment (so the restart reloads it rather than re-deriving it).
fn reinsert_into_hot(srv: &Server, dir: &Path, wait_persisted: bool) {
    let mut c = srv.conn();
    let key = format!("doc:{REINSERTED}");
    let r = c.send(&["HSET", &key, "vec", &reinserted()]);
    assert_eq!(r, ":0\r\n", "HSET of an existing field: {r}");
    assert_eq!(c.send(&["FT.COMPACT", "idx"]), "+OK\r\n");
    if wait_persisted {
        wait_for("the re-inserted key's HOT segment", 120, || {
            info_int(&mut c, "graph_segments") == 1
        });
        wait_for("Stack B to persist the HOT segment", 60, || {
            hot_segment_ids(dir).len() == 1
        });
    }
}

/// Restart on `dir` and run the assertions every boot must pass. Returns the
/// still-running server and the probe results, so two boots can be compared.
fn assert_boot(dir: &Path, boot: &str, expect_warm: usize) -> (Server, Vec<Vec<String>>) {
    let before = segment_dirs(dir);
    let srv = start(dir, 3600);
    let mut c = srv.conn();

    let after = segment_dirs(dir);
    assert_eq!(
        after, before,
        "{boot}: recovery deleted warm segment directories it should have attached"
    );
    for d in &after {
        assert!(d.join("mvcc.mpf").exists(), "{boot}: {d:?} lost its files");
    }
    assert_eq!(
        info_int(&mut c, "warm_segments"),
        expect_warm as i64,
        "{boot}: every warm segment must be attached"
    );
    assert_eq!(info_int(&mut c, "num_docs"), N as i64, "{boot}: num_docs");
    let token = info_int(&mut c, "vector_version_token");
    assert!(
        token < NO_REENCODE_MAX_TOKEN,
        "{boot}: the rescan re-encoded vectors (vector_version_token = {token}); a boot \
         that attaches its warm segments re-encodes none"
    );

    let mut probes = Vec::new();
    for i in (0..N).step_by(37).chain([REINSERTED + 1, N - 1]) {
        if i == REINSERTED {
            continue;
        }
        let got = knn(&mut c, &original(i));
        assert_eq!(
            got.first().map(String::as_str),
            Some(format!("doc:{i}").as_str()),
            "{boot}: doc:{i} is not served by its own vector: {got:?}"
        );
        probes.push(got);
    }
    (srv, probes)
}

/// The re-inserted key answers to its CURRENT vector, never its overwritten one.
fn assert_reinserted_is_current(srv: &Server, boot: &str) {
    let mut c = srv.conn();
    let key = format!("doc:{REINSERTED}");
    let current = knn(&mut c, &reinserted());
    assert_eq!(
        current.first(),
        Some(&key),
        "{boot}: the re-inserted key is not served by its current vector: {current:?}"
    );
    let stale = knn(&mut c, &original(REINSERTED));
    assert_ne!(
        stale.first(),
        Some(&key),
        "{boot}: the re-inserted key is served by its OVERWRITTEN vector"
    );
}

fn fresh_dir(tag: &str) -> PathBuf {
    let d = common::unique_test_dir(&format!("moon-893-{tag}"));
    std::fs::create_dir_all(&d).unwrap();
    d
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

fn reinserted_key_keeps_its_warm_siblings(tag: &str, crash: bool) {
    let dir = fresh_dir(tag);
    seed_warm_index(&dir).shutdown();

    let srv = start(&dir, 3600);
    reinsert_into_hot(&srv, &dir, true);
    if crash {
        // Let the everysec AOF fsync cover the re-insert before the kill.
        std::thread::sleep(Duration::from_millis(1500));
    }
    srv.stop(crash);

    let mut runs = Vec::new();
    for boot in ["boot 1", "boot 2"] {
        let (srv, probes) = assert_boot(&dir, boot, 1);
        assert_reinserted_is_current(&srv, boot);
        srv.stop(crash);
        runs.push(probes);
    }
    assert_eq!(
        runs[0], runs[1],
        "FT.SEARCH differs between two identical boots"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn one_reinserted_key_keeps_its_warm_siblings_across_restarts() {
    reinserted_key_keeps_its_warm_siblings("sibling", false);
}

/// The same, with every stop after the re-insert a SIGKILL: the per-key
/// decision must not depend on a clean shutdown having run.
#[test]
fn one_reinserted_key_keeps_its_warm_siblings_across_kill9() {
    reinserted_key_keeps_its_warm_siblings("sibling-kill9", true);
}

#[test]
fn a_key_in_two_warm_segments_serves_its_current_vector_across_restarts() {
    let dir = fresh_dir("twowarm");
    seed_warm_index(&dir).shutdown();

    // The re-inserted key's segment goes WARM too.
    let srv = start(&dir, 1);
    reinsert_into_hot(&srv, &dir, false);
    let mut c = srv.conn();
    wait_for("the second segment to go WARM", 120, || {
        info_int(&mut c, "warm_segments") == 2
    });
    wait_for("Stack B to drop the transitioned HOT segment", 60, || {
        hot_segment_ids(&dir).is_empty()
    });
    srv.shutdown();
    assert_eq!(segment_dirs(&dir).len(), 2);

    let mut runs = Vec::new();
    for boot in ["boot 1", "boot 2"] {
        let (srv, probes) = assert_boot(&dir, boot, 2);
        assert_reinserted_is_current(&srv, boot);
        srv.shutdown();
        runs.push(probes);
    }
    assert_eq!(
        runs[0], runs[1],
        "FT.SEARCH differs between two identical boots"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Append a second copy of `file_id`'s entry to the manifest's active root, as
/// the pre-#893 `add_file` did: same bytes, `entry_count`/`file_count`/
/// `payload_bytes` bumped, epoch advanced, CRC recomputed.
fn append_duplicate_entry_like_an_old_build(manifest: &Path, file_id: u64) {
    let mut buf = std::fs::read(manifest).unwrap();
    let meta = MOONPAGE_HEADER_SIZE;
    let epoch_of = |page: &[u8]| u64::from_le_bytes(page[meta..meta + 8].try_into().unwrap());
    let slot = [0usize, PAGE_4K]
        .into_iter()
        .filter(|&s| MoonPageHeader::verify_checksum(&buf[s..s + PAGE_4K]))
        .max_by_key(|&s| epoch_of(&buf[s..s + PAGE_4K]))
        .expect("no valid manifest root");
    let page = &mut buf[slot..slot + PAGE_4K];
    let mut hdr = MoonPageHeader::read_from(page).unwrap();
    assert_eq!(hdr.next_page, 0, "fixture expects an inline-only root");
    let count = hdr.entry_count as usize;
    assert!(count < MAX_INLINE_ENTRIES);
    let entries = meta + 64;
    let src = (0..count)
        .map(|i| entries + i * FileEntry::SIZE)
        .find(|&off| {
            FileEntry::read_from(&page[off..off + FileEntry::SIZE])
                .is_some_and(|e| e.file_id == file_id)
        })
        .expect("entry to duplicate");
    let dst = entries + count * FileEntry::SIZE;
    page.copy_within(src..src + FileEntry::SIZE, dst);
    let epoch = epoch_of(page) + 1;
    page[meta..meta + 8].copy_from_slice(&epoch.to_le_bytes());
    let file_count = u32::from_le_bytes(page[meta + 24..meta + 28].try_into().unwrap()) + 1;
    page[meta + 24..meta + 28].copy_from_slice(&file_count.to_le_bytes());
    hdr.entry_count += 1;
    hdr.payload_bytes += FileEntry::SIZE as u32;
    hdr.write_to(page);
    MoonPageHeader::compute_checksum(page);
    std::fs::write(manifest, &buf).unwrap();
}

fn active_entries(manifest: &Path, file_id: u64) -> usize {
    ShardManifest::open(manifest)
        .unwrap()
        .files()
        .iter()
        .filter(|e| {
            e.file_id == file_id
                && e.file_type == PageType::VecCodes as u8
                && e.status == FileStatus::Active
        })
        .count()
}

#[test]
fn a_manifest_listing_a_segment_twice_heals_and_keeps_the_directory() {
    let dir = fresh_dir("dupentry");
    let srv = seed_warm_index(&dir);
    let mut c = srv.conn();
    let baseline: Vec<Vec<String>> = (0..N)
        .step_by(37)
        .map(|i| knn(&mut c, &original(i)))
        .collect();
    srv.shutdown();

    let seg = segment_dirs(&dir).remove(0);
    let id: u64 = seg
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_prefix("segment-"))
        .and_then(|n| n.parse().ok())
        .unwrap();
    let manifest = shard_dir(&dir).join("shard-0.manifest");
    assert_eq!(active_entries(&manifest, id), 1);
    append_duplicate_entry_like_an_old_build(&manifest, id);
    assert_eq!(
        active_entries(&manifest, id),
        2,
        "fixture: the manifest must now list the segment twice"
    );

    for boot in ["boot 1", "boot 2"] {
        let (srv, _) = assert_boot(&dir, boot, 1);
        let mut c = srv.conn();
        let now: Vec<Vec<String>> = (0..N)
            .step_by(37)
            .map(|i| knn(&mut c, &original(i)))
            .collect();
        assert_eq!(
            now, baseline,
            "{boot}: FT.SEARCH differs from before the restart"
        );
        srv.shutdown();
        assert_eq!(
            active_entries(&manifest, id),
            1,
            "{boot}: the duplicate entry must be healed on disk"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
