//! moon#875: the cold-index rebuild must never drop an entry SILENTLY.
//!
//! ## The bug
//!
//! `ColdIndex::rebuild_from_manifest_per_db` skipped, without a log line or
//! a counter: a heap file that could not be read (any `io::Error`), a page
//! that failed its magic/type/CRC check, and a trailing partial page
//! (`chunks_exact` discards the remainder). Every entry lost that way then
//! read as an ABSENT key — `GET` answered nil, exactly as it does for a key
//! that was never written — so no client, and no operator, could tell a
//! clean recovery from one that had lost a slice of the keyspace.
//!
//! ## What this suite proves
//!
//! It drives the REAL lifecycle: five keys are tiered to five DISTINCT heap
//! files through eviction, the AOF is rewritten (`BGREWRITEAOF` — after it
//! the base RDB is hot-only and the cold file is the ONLY copy of a spilled
//! key, which is the state of every server that has ever auto-rewritten;
//! before it the AOF's own `SET` masks any cold damage), the server is
//! `SIGKILL`ed, and the on-disk state is damaged the way a disk (not moon)
//! damages it — one file removed, one made unreadable, one page's CRC
//! broken, one file cut mid-page. Then the server restarts on that state.
//!
//! The damaged keys answer nil on BOTH binaries: their bytes are gone and no
//! rebuild can conjure them back. That is printed, deliberately, as the
//! "absent" evidence. What discriminates the pre-fix binary is the
//! EVIDENCE: after the fix the restart leaves `INFO` counters and `server.err`
//! lines naming every damaged file; before it there is nothing at all.
//!
//! The last leg proves the fail-open choice destroyed nothing: the
//! unreadable file's permissions are restored, the server restarts, and its
//! key reads back — the index entry was skipped, never tombstoned.
//!
//! Run with:
//!   MOON_BIN=/path/to/moon cargo test --release \
//!     --test cold_index_rebuild_silent_drops_875

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary, wait_for_port_down};

const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
/// Past `CompactValue`'s inline limit, so the key is a heap value that tiers.
const VALUE_LEN: usize = 256;
const FILLER_VALUE_LEN: usize = 1024;
const FILLER_PER_ROUND: usize = 100;
const MAX_FILLER_ROUNDS: usize = 1000;
const PAGE_4K: usize = 4096;

/// The key kept in an undamaged file: proves the cold plane serves at all.
const KEY_CONTROL: &str = "k875:control:untouched";
/// One key per damage class. Unique enough that a raw byte search of the
/// heap files cannot match a filler key or a value.
const KEY_MISSING: &str = "k875:file-removed";
const KEY_UNREADABLE: &str = "k875:file-chmod-000";
const KEY_CORRUPT: &str = "k875:page-crc-broken";
const KEY_PARTIAL: &str = "k875:file-cut-mid-page";

// ===========================================================================
// Server harness — same shape as cold_index_duplicate_resolution_983.
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
        "--appendonly".into(),
        "yes".into(),
        "--appendfsync".into(),
        "everysec".into(),
        "--maxmemory".into(),
        MAXMEMORY_BYTES.to_string(),
        "--maxmemory-policy".into(),
        "allkeys-lru".into(),
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
            // `tracing_subscriber::fmt()` writes to STDOUT; the evidence this
            // suite asserts on lives there, so both streams go to the log.
            .stdout(common::server_stderr(dir))
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
    /// `SIGKILL`, leaving the on-disk state exactly as the crash left it.
    fn crash(self) -> Crashed {
        let Server {
            mut guard,
            port,
            dir,
            shards,
        } = self;
        guard.kill_now();
        drop(guard);
        wait_for_port_down(port);
        Crashed { port, dir, shards }
    }
}

/// A crashed server: the window in which the test damages the disk.
struct Crashed {
    port: u16,
    dir: PathBuf,
    shards: usize,
}

impl Crashed {
    fn restart(self) -> Server {
        let Crashed { port, dir, shards } = self;
        let child = Command::new(find_moon_binary())
            .args(moon_args(&dir, port, shards))
            .stdout(common::server_stderr(&dir))
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

fn exists(c: &mut Conn, key: &str) -> bool {
    let raw = c.send(&["EXISTS", key]);
    assert!(raw.starts_with(':'), "EXISTS answered {raw:?}");
    raw.starts_with(":1")
}

fn accepted(reply: &str, what: &str) -> bool {
    if reply.starts_with('-') {
        assert!(
            reply.contains("OOM") || reply.contains("backpressure"),
            "{what} answered an unexpected error: {reply:?}"
        );
        return false;
    }
    true
}

/// `SET` that tolerates a transient `-OOM`: the instant a key lands on disk
/// the shard is still at its memory cap, and eviction needs a moment to
/// make room. Bounded — a SET that is never accepted is a harness failure.
fn set_retrying(c: &mut Conn, key: &str, value: &str) {
    for _ in 0..100 {
        if accepted(&c.send(&["SET", key, value]), "SET") {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("SET {key} was refused (-OOM / backpressure) 100 times in a row");
}

fn value_for(tag: &str) -> String {
    let mut s = format!("{tag}-");
    while s.len() < VALUE_LEN {
        s.push('x');
    }
    s
}

fn write_filler(c: &mut Conn, phase: usize, round: usize) -> usize {
    let val = "f".repeat(FILLER_VALUE_LEN);
    let keys: Vec<String> = (0..FILLER_PER_ROUND)
        .map(|i| format!("filler:{phase}:{round:04}:{i:03}"))
        .collect();
    let cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), &val]).collect();
    let cmd_refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    let replies = c.pipeline(&cmd_refs);
    replies
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .filter(|l| accepted(l, "filler SET"))
        .count()
}

/// `INFO` body as one string (the bulk payload, CRLF lines).
fn info(c: &mut Conn) -> String {
    let raw = c.send(&["INFO"]);
    assert!(raw.starts_with('$'), "INFO answered {raw:?}");
    raw.split_once("\r\n").map(|x| x.1.to_string()).unwrap()
}

fn info_u64(body: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    body.lines()
        .find(|l| l.starts_with(&prefix))
        .and_then(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse::<u64>().ok())
}

// ===========================================================================
// On-disk evidence.
// ===========================================================================

/// Every heap file under `<off>/shard-*/data/` whose raw bytes contain
/// `needle`. Keys are stored verbatim in `KvLeafPage` slots, so this locates
/// the on-disk copy of a key without any in-process index — the same files
/// the rebuild will read.
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

/// Keep writing filler until `key` is on disk. Returns the file holding it.
fn fill_until_spilled(c: &mut Conn, off: &Path, phase: usize, key: &str) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(120);
    for round in 0..MAX_FILLER_ROUNDS {
        write_filler(c, phase, round);
        let hits = heap_files_holding(off, key.as_bytes());
        if let Some(first) = hits.first() {
            return first.clone();
        }
        assert!(
            Instant::now() < deadline,
            "phase {phase}: {key} did not reach disk within 120s — eviction is not \
             tiering the key"
        );
    }
    panic!("phase {phase}: {key} did not reach disk after {MAX_FILLER_ROUNDS} filler rounds");
}

fn settle() {
    std::thread::sleep(Duration::from_secs(1));
}

/// Every AOF base the server has published, keyed by manifest path: the
/// per-shard layout (`appendonlydir/shard-N/moon.aof.manifest`, `seq`), the
/// legacy top-level manifest, or the flat `appendonly.aof` (len + mtime).
/// Same detection as `restart_preserves_compact_encoding::wait_for_rewrite`:
/// `BGREWRITEAOF` acks at enqueue and `aof_rewrite_in_progress:0` can be
/// observed before it starts, so the published base is what is waited on.
fn aof_bases(dir: &Path) -> std::collections::BTreeMap<PathBuf, (u64, bool)> {
    let mut out = std::collections::BTreeMap::new();
    let aof_dir = dir.join("appendonlydir");
    let mut manifests = vec![aof_dir.join("moon.aof.manifest")];
    if let Ok(rd) = std::fs::read_dir(&aof_dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                manifests.push(p.join("moon.aof.manifest"));
            }
        }
    }
    for m in manifests {
        let Ok(text) = std::fs::read_to_string(&m) else {
            continue;
        };
        if let Some(seq) = text
            .lines()
            .find_map(|l| l.strip_prefix("seq ")?.trim().parse::<u64>().ok())
        {
            // The base sits next to the manifest (single-shard layout) or
            // in EVERY `shard-N/` beneath it (per-shard layout, one
            // top-level manifest).
            let parent = m.parent().unwrap();
            let base_name = format!("moon.aof.{seq}.base.rdb");
            let shard_dirs: Vec<PathBuf> = std::fs::read_dir(parent)
                .map(|rd| {
                    rd.flatten()
                        .map(|e| e.path())
                        .filter(|p| {
                            p.is_dir()
                                && p.file_name()
                                    .and_then(|n| n.to_str())
                                    .is_some_and(|n| n.starts_with("shard-"))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let base = parent.join(&base_name).exists()
                || (!shard_dirs.is_empty()
                    && shard_dirs.iter().all(|d| d.join(&base_name).exists()));
            out.insert(m, (seq, base));
        }
    }
    if let Ok(md) = std::fs::metadata(dir.join("appendonly.aof")) {
        let mtime = md
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        out.insert(dir.join("appendonly.aof"), (md.len() ^ mtime, true));
    }
    out
}

/// `BGREWRITEAOF`, then wait until every AOF base has advanced past
/// `before` and its base file exists. Panics with the last observation.
fn rewrite_aof(c: &mut Conn, dir: &Path) {
    let before = aof_bases(dir);
    let ack = c.send(&["BGREWRITEAOF"]);
    assert!(!ack.starts_with('-'), "BGREWRITEAOF refused: {ack:?}");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = before.clone();
    while Instant::now() < deadline {
        let info = c.send(&["INFO", "persistence"]);
        let now = aof_bases(dir);
        let advanced = !now.is_empty()
            && now.iter().all(|(path, (seq, ready))| {
                *ready && before.get(path).is_none_or(|(b, _)| seq != b)
            })
            && before.keys().all(|k| now.contains_key(k));
        if info.contains("aof_rewrite_in_progress:0") && advanced {
            return;
        }
        last = now;
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("BGREWRITEAOF did not publish new bases within 60s: before {before:?}, last {last:?}");
}

/// Remove `ESC [ … <letter>` control sequences so log fields match as text.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            chars.next();
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `heap-000042.mpf` -> 42.
fn file_id_of(p: &Path) -> u64 {
    let name = p.file_name().unwrap().to_str().unwrap();
    name.trim_start_matches("heap-")
        .trim_end_matches(".mpf")
        .parse()
        .unwrap()
}

/// Byte offset of `needle` inside the file.
fn offset_of(bytes: &[u8], needle: &[u8]) -> usize {
    bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("needle is in the file — heap_files_holding said so")
}

// ===========================================================================
// Damage, the way a disk does it.
// ===========================================================================

/// Flip one byte inside the payload region of the page holding `key`, so
/// that page's CRC32C no longer matches. The header is left intact — this
/// must exercise the CRC path, not the magic path.
fn break_page_crc(file: &Path, key: &str) -> u32 {
    let mut bytes = std::fs::read(file).unwrap();
    let page_idx = offset_of(&bytes, key.as_bytes()) / PAGE_4K;
    let victim = page_idx * PAGE_4K + 64 + 1;
    bytes[victim] ^= 0xFF;
    std::fs::write(file, &bytes).unwrap();
    page_idx as u32
}

/// Cut the file 100 bytes into the page holding `key`: every page after it
/// is gone whole, and 100 bytes of it survive as a trailing partial page.
fn cut_mid_page(file: &Path, key: &str) -> (u64, u64) {
    let bytes = std::fs::read(file).unwrap();
    let page_idx = offset_of(&bytes, key.as_bytes()) / PAGE_4K;
    let new_len = (page_idx * PAGE_4K + 100) as u64;
    let f = std::fs::OpenOptions::new().write(true).open(file).unwrap();
    f.set_len(new_len).unwrap();
    (bytes.len() as u64, new_len)
}

fn chmod(file: &Path, mode: u32) {
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(mode)).unwrap();
}

// ===========================================================================
// The test.
// ===========================================================================

fn rebuild_reports_every_drop(shards: usize) {
    let dir = common::unique_test_dir(&format!("cold-drops-875-s{shards}"));
    let off = offload_dir(&dir);
    let server = spawn(&dir, shards);
    let mut c = Conn::open(server.port);

    // Five keys, five DISTINCT files. Each key is written after the previous
    // one is already on disk, and a heap file is immutable once written, so
    // no two of them can share a file.
    let keys = [
        KEY_CONTROL,
        KEY_MISSING,
        KEY_UNREADABLE,
        KEY_CORRUPT,
        KEY_PARTIAL,
    ];
    let mut files: Vec<PathBuf> = Vec::with_capacity(keys.len());
    for (phase, key) in keys.iter().enumerate() {
        let v = value_for(key);
        set_retrying(&mut c, key, &v);
        files.push(fill_until_spilled(&mut c, &off, phase + 1, key));
    }
    settle();
    for (i, key) in keys.iter().enumerate() {
        let hits = heap_files_holding(&off, key.as_bytes());
        assert_eq!(
            hits.len(),
            1,
            "{key} must have exactly one on-disk copy before the crash, found {hits:?}"
        );
        assert_eq!(hits[0], files[i]);
        assert!(
            exists(&mut c, key),
            "{key} must exist live before the crash"
        );
    }
    {
        let mut distinct = files.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(
            distinct.len(),
            files.len(),
            "harness precondition: the five keys must sit in five distinct files: {files:?}"
        );
    }
    let [f_control, f_missing, f_unreadable, f_corrupt, f_partial] =
        <[PathBuf; 5]>::try_from(files).unwrap();
    let id_missing = file_id_of(&f_missing);
    let id_unreadable = file_id_of(&f_unreadable);
    let id_corrupt = file_id_of(&f_corrupt);
    let id_partial = file_id_of(&f_partial);
    eprintln!(
        "[875 s{shards}] control={} missing={id_missing} unreadable={id_unreadable} \
         corrupt={id_corrupt} partial={id_partial}",
        file_id_of(&f_control)
    );

    // Make the cold files the ONLY copy: after a rewrite the base RDB is
    // hot-only and the new generation's `MOON.COLDCUT` authorizes every
    // existing cold file. Without this the AOF's own `SET` records rebuild
    // the keys hot on restart and the damage below is invisible.
    rewrite_aof(&mut c, &dir);
    for key in keys {
        assert!(
            exists(&mut c, key),
            "{key} must still exist after the AOF rewrite"
        );
        assert_eq!(
            heap_files_holding(&off, key.as_bytes()).len(),
            1,
            "{key} must still have exactly one on-disk copy after the rewrite"
        );
    }

    // ── Crash, then damage the disk the way a disk does. ─────────────────
    let crashed = server.crash();
    std::fs::remove_file(&f_missing).unwrap();
    chmod(&f_unreadable, 0o000);
    let broken_page = break_page_crc(&f_corrupt, KEY_CORRUPT);
    let (was, now) = cut_mid_page(&f_partial, KEY_PARTIAL);
    eprintln!(
        "[875 s{shards}] broke page {broken_page} of file {id_corrupt}; cut file {id_partial} {was}->{now} bytes"
    );

    // ── Restart on the damaged state. ────────────────────────────────────
    let server = crashed.restart();
    let mut c = Conn::open(server.port);

    // The cold plane serves at all: the untouched file's key reads back.
    assert_eq!(
        get(&mut c, KEY_CONTROL).as_deref(),
        Some(value_for(KEY_CONTROL).as_str()),
        "the undamaged file's key must read back after restart"
    );

    // The evidence of loss, as the CLIENT sees it — identical on both
    // binaries, and exactly the problem: nil, indistinguishable from a key
    // that was never written.
    for key in [KEY_MISSING, KEY_UNREADABLE, KEY_CORRUPT, KEY_PARTIAL] {
        let got = get(&mut c, key);
        let ex = exists(&mut c, key);
        eprintln!("[875 s{shards}] after restart: GET {key} -> {got:?}; EXISTS -> {ex}");
        assert!(
            got.is_none() && !ex,
            "{key}'s bytes are gone; it must read as absent (got {got:?}, exists={ex})"
        );
    }

    // ── The discriminator: after the fix, the loss leaves evidence. ──────
    let body = info(&mut c);
    let field = |name: &str| -> u64 {
        info_u64(&body, name).unwrap_or_else(|| {
            panic!(
                "INFO reclamation has no `{name}` field — the rebuild reported nothing \
                 about {id_missing}/{id_unreadable}/{id_corrupt}/{id_partial} (moon#875)"
            )
        })
    };
    let files_missing = field("reclamation_cold_recovery_files_missing_total");
    let files_unreadable = field("reclamation_cold_recovery_files_unreadable_total");
    let pages_rejected = field("reclamation_cold_recovery_pages_rejected_total");
    let partial_bytes = field("reclamation_cold_recovery_partial_page_bytes_total");
    let files_short = field("reclamation_cold_recovery_files_short_total");
    eprintln!(
        "[875 s{shards}] INFO: files_missing={files_missing} files_unreadable={files_unreadable} \
         pages_rejected={pages_rejected} partial_page_bytes={partial_bytes} files_short={files_short}"
    );
    assert_eq!(files_missing, 1, "one file was removed");
    assert_eq!(files_unreadable, 1, "one file was made unreadable");
    assert_eq!(pages_rejected, 1, "one page had its CRC broken");
    assert_eq!(
        partial_bytes, 100,
        "the cut left a 100-byte trailing partial page"
    );
    assert_eq!(
        files_short, 1,
        "the cut file is shorter than its manifest entry says"
    );

    // And a human can find it: every damaged file is named in the log, and
    // the rebuild summary says it was degraded.
    // `tracing_subscriber::fmt` writes ANSI colour codes even into a file,
    // so `file_id=15212` arrives as `\x1b[3mfile_id\x1b[0m\x1b[2m=\x1b[0m15212`.
    let log = strip_ansi(&std::fs::read_to_string(dir.join("server.err")).unwrap_or_default());
    for (what, id, file) in [
        ("missing", id_missing, &f_missing),
        ("unreadable", id_unreadable, &f_unreadable),
        ("corrupt", id_corrupt, &f_corrupt),
        ("partial", id_partial, &f_partial),
    ] {
        // `file_id` is per shard — `heap-000769.mpf` can exist in two shard
        // dirs at once — so the line must also name THIS file's shard: the
        // per-file line carries the path, the summary carries `shard_id`.
        let shard = file
            .parent()
            .and_then(|d| d.parent())
            .and_then(|s| s.file_name())
            .and_then(|n| n.to_str())
            .unwrap();
        let needle = format!("file_id={id}");
        let shard_path = format!("/{shard}/");
        assert!(
            log.lines().any(|l| l.contains("cold recovery")
                && l.contains(&needle)
                && l.contains(&shard_path)),
            "server.err has no `cold recovery` line naming the {what} file ({needle} in \
             {shard}); the operator has nothing to find"
        );
    }
    assert!(
        log.lines()
            .any(|l| l.contains("cold index rebuild") && l.contains("DEGRADED")),
        "server.err has no DEGRADED rebuild summary"
    );

    // ── Fail-open destroyed nothing: fix the permissions, restart, read. ─
    let crashed = server.crash();
    chmod(&f_unreadable, 0o644);
    let server = crashed.restart();
    let mut c = Conn::open(server.port);
    assert_eq!(
        get(&mut c, KEY_UNREADABLE).as_deref(),
        Some(value_for(KEY_UNREADABLE).as_str()),
        "once the file is readable again its key must come back — the rebuild must \
         skip an unreadable file, never tombstone it"
    );
    assert_eq!(
        get(&mut c, KEY_CONTROL).as_deref(),
        Some(value_for(KEY_CONTROL).as_str())
    );
    drop(server);
}

#[test]
fn rebuild_reports_every_drop_1_shard() {
    rebuild_reports_every_drop(1);
}

#[test]
fn rebuild_reports_every_drop_4_shards() {
    rebuild_reports_every_drop(4);
}

// ===========================================================================
// The read side: damage while SERVING. The key stays indexed, so the reply
// must be an error, never nil.
// ===========================================================================

const KEY_LIVE_CONTROL: &str = "k875:live:control";
const KEY_LIVE_MISSING: &str = "k875:live:file-removed";
const KEY_LIVE_LOCKED: &str = "k875:live:file-chmod-000";

fn raw(c: &mut Conn, parts: &[&str]) -> String {
    c.send(parts)
}

fn live_damage_answers_ioerr_not_nil(shards: usize) {
    let dir = common::unique_test_dir(&format!("cold-live-875-s{shards}"));
    let off = offload_dir(&dir);
    let server = spawn(&dir, shards);
    let mut c = Conn::open(server.port);

    let keys = [KEY_LIVE_CONTROL, KEY_LIVE_MISSING, KEY_LIVE_LOCKED];
    let mut files: Vec<PathBuf> = Vec::new();
    for (phase, key) in keys.iter().enumerate() {
        set_retrying(&mut c, key, &value_for(key));
        files.push(fill_until_spilled(&mut c, &off, phase + 1, key));
    }
    settle();
    {
        let mut d = files.clone();
        d.sort();
        d.dedup();
        assert_eq!(d.len(), 3, "three distinct files: {files:?}");
    }
    let [f_control, f_missing, f_locked] = <[PathBuf; 3]>::try_from(files).unwrap();
    let _ = f_control;

    // Damage the disk under a RUNNING server: the index still has the keys.
    std::fs::remove_file(&f_missing).unwrap();
    chmod(&f_locked, 0o000);
    for key in [KEY_LIVE_MISSING, KEY_LIVE_LOCKED] {
        assert!(
            exists(&mut c, key),
            "{key} is still indexed — EXISTS must say 1"
        );
    }

    // The client must NOT be told the keys are absent.
    for key in [KEY_LIVE_MISSING, KEY_LIVE_LOCKED] {
        let r = raw(&mut c, &["GET", key]);
        eprintln!("[875 live s{shards}] GET {key} -> {:?}", r.trim_end());
        assert!(
            r.starts_with("-IOERR"),
            "GET {key}: indexed but unreadable must answer -IOERR, got {r:?} (moon#875)"
        );
    }
    // A write that depends on the old value is refused BEFORE mutating.
    let r = raw(&mut c, &["APPEND", KEY_LIVE_LOCKED, "-tail"]);
    eprintln!(
        "[875 live s{shards}] APPEND {KEY_LIVE_LOCKED} -> {:?}",
        r.trim_end()
    );
    assert!(
        r.starts_with("-IOERR"),
        "APPEND on an unreadable cold key: {r:?}"
    );
    let r = raw(&mut c, &["INCR", KEY_LIVE_MISSING]);
    eprintln!(
        "[875 live s{shards}] INCR {KEY_LIVE_MISSING} -> {:?}",
        r.trim_end()
    );
    assert!(
        r.starts_with("-IOERR"),
        "INCR on an unreadable cold key: {r:?}"
    );
    assert!(
        exists(&mut c, KEY_LIVE_LOCKED),
        "the refused APPEND left the key indexed"
    );

    // The fault is counted, the untouched key still serves, and the next
    // command is unaffected (the flag does not leak).
    let body = info(&mut c);
    let unreadable = info_u64(&body, "reclamation_cold_read_unreadable_total").unwrap_or(0);
    eprintln!("[875 live s{shards}] reclamation_cold_read_unreadable_total={unreadable}");
    assert!(
        unreadable >= 4,
        "every unreadable read must be counted, got {unreadable}"
    );
    assert_eq!(
        get(&mut c, KEY_LIVE_CONTROL).as_deref(),
        Some(value_for(KEY_LIVE_CONTROL).as_str())
    );
    assert!(raw(&mut c, &["PING"]).starts_with("+PONG"));

    // The retained index entry heals the read once the bytes are back, and
    // the refused APPEND fabricated nothing: the ORIGINAL value comes back.
    chmod(&f_locked, 0o644);
    assert_eq!(
        get(&mut c, KEY_LIVE_LOCKED).as_deref(),
        Some(value_for(KEY_LIVE_LOCKED).as_str()),
        "once readable again the original value must be served"
    );
    // And the operator's escape hatch for bytes that are really gone.
    assert!(raw(&mut c, &["DEL", KEY_LIVE_MISSING]).starts_with(":1"));
    assert_eq!(
        get(&mut c, KEY_LIVE_MISSING),
        None,
        "deleted: now genuinely absent"
    );
    drop(server);
}

#[test]
fn live_damage_answers_ioerr_not_nil_1_shard() {
    live_damage_answers_ioerr_not_nil(1);
}

#[test]
fn live_damage_answers_ioerr_not_nil_4_shards() {
    live_damage_answers_ioerr_not_nil(4);
}
