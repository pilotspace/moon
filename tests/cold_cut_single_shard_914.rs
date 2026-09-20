//! moon#914: the `MOON.COLDCUT` watermark must open every replay generation
//! on the configuration `main.rs` deliberately leaves without an
//! `AofManifest` — `--shards 1` under `runtime-tokio`.
//!
//! That configuration recovers through the legacy single-file
//! `appendonly.aof` (or, when the WAL carries KV records, through WAL v3 —
//! the "WAL-authority" path). Neither ever carried a `MOON.COLDCUT`: the
//! manifest's `seed_cold_cut` is the only fresh-boot writer and the tokio
//! TopLevel `BGREWRITEAOF` wrote a bare RDB preamble. With no cut, replay-time
//! cold reads are UNGATED, so every replayed `get_or_create_*` promotes the
//! cold copy the command itself already produced and mutates it again —
//! moon#902's double-apply, live on this configuration even after moon#965
//! (#1011), which fixed only the end-of-replay RESOLUTION.
//!
//! Legs (families as in `tests/cold_tier_aof_double_apply_902.rs`):
//!
//!   1. **Fresh boot, AOF authority** — seven non-idempotent families plus
//!      two idempotent controls, three `kill -9` cycles; every probe must
//!      equal its single-application value after every cycle.
//!   2. **Post-`BGREWRITEAOF` write to a cold key** — the rewritten base is
//!      hot-only, so the cold copy is the ONLY copy. 2a is moon#912's exact
//!      SET-only shape (an acknowledged `v2` came back `v1`); 2b spills the
//!      post-rewrite values again before the kill (double-apply), which needs
//!      the rewrite to re-open the cut. 2c is 2b with every post-rewrite
//!      `MOON.SPILLED` marker removed from the crash image (moon#1140): a
//!      marker is best-effort (a SIGKILL before the writer flushes it, or
//!      backpressure, loses it) while its file's manifest entry commits on
//!      its own, so recovery must not depend on it to find the pre-rewrite
//!      copy of a key.
//!   3. **`--wal-kv-log on`** — leg 1 with the WAL leg of `ColdMarkerSink`
//!      live. A WAL holding only markers must not displace the AOF as the
//!      recovery authority (it has never recorded a client write here).
//!
//! It runs in the ordinary suite: `common::find_moon_binary()` resolves
//! `CARGO_BIN_EXE_moon`, built with this test's own features, so the
//! `runtime-tokio` legs drive a tokio binary — the configuration the bug
//! lives in. Under default features it drives monoio, where every leg is
//! still a valid guard (the manifest path has carried the cut since #911).
//! Each leg asserts cold data files existed at the kill point, so a run that
//! spilled nothing cannot pass. `--disk-free-min-pct 0` is REQUIRED: the
//! disk-free guard silently guts a crash test on a nearly-full volume.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::process::{Child, Command};
use std::time::{Duration, Instant};

const PROBES: usize = 24;
const FILLER_COUNT: usize = 200;
const FILLER_LEN: usize = 4096;
const MAXMEMORY: &str = "524288"; // 512 KiB — far below the filler wave.

fn start_moon(dir: &std::path::Path, extra: &[&str]) -> (Child, u16) {
    let extra: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    common::spawn_listening(move |port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--dir",
                dir.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--disk-offload",
                "enable",
                "--maxmemory",
                MAXMEMORY,
                "--maxmemory-policy",
                "allkeys-lru",
                "--maxmemory-samples",
                "200",
                // REQUIRED — see the module doc.
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

fn count_cold_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                let path = e.path();
                if path.is_dir() {
                    walk(&path, acc);
                } else if path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
                {
                    *acc += 1;
                }
            }
        }
    }
    let mut acc = 0;
    walk(dir, &mut acc);
    acc
}

/// One RESP reply -> a stable string. `None` == nil (key gone), which is a
/// legitimate outcome for a plain-dropped victim.
fn scalar(reply: &str) -> Option<String> {
    let mut lines = reply.lines();
    let head = lines.next()?;
    if head.starts_with("$-1") || head.starts_with('_') {
        return None;
    }
    if let Some(n) = head.strip_prefix(':') {
        return Some(n.to_string());
    }
    if head.starts_with('$') {
        return lines.next().map(|s| s.to_string());
    }
    if let Some(simple) = head.strip_prefix('+') {
        return Some(simple.to_string());
    }
    Some(head.to_string())
}

/// An array reply -> its items joined by ',' (ints lose their ':').
fn items(reply: &str) -> Option<String> {
    let v: Vec<String> = reply
        .lines()
        .skip(1)
        .filter(|l| !l.starts_with('$'))
        .map(|l| l.strip_prefix(':').unwrap_or(l).to_string())
        .filter(|l| !l.is_empty())
        .collect();
    if v.is_empty() {
        None
    } else {
        Some(v.join(","))
    }
}

const FAMILIES: &[&str] = &["l", "lx", "ap", "n", "hi", "zi", "bf", "h", "s"];

fn write_probe(c: &mut common::Conn, fam: &str, key: &str) {
    match fam {
        "l" => {
            c.send(&["RPUSH", key, "a", "b", "c", "d", "e"]);
        }
        "lx" => {
            c.send(&["RPUSH", key, "a"]);
            c.send(&["RPUSHX", key, "b"]);
            c.send(&["LPUSHX", key, "z"]);
        }
        "ap" => {
            c.send(&["APPEND", key, "hello"]);
        }
        "n" => {
            c.send(&["INCRBY", key, "7"]);
        }
        "hi" => {
            c.send(&["HINCRBY", key, "f", "3"]);
        }
        "zi" => {
            c.send(&["ZINCRBY", key, "2.5", "m"]);
        }
        "bf" => {
            c.send(&["BITFIELD", key, "INCRBY", "u8", "0", "5"]);
        }
        "h" => {
            c.send(&["HSET", key, "f1", "v1", "f2", "v2", "f3", "v3"]);
        }
        "s" => {
            c.send(&["SET", key, "v1"]);
        }
        other => panic!("unknown family {other}"),
    }
}

fn read_probe(c: &mut common::Conn, fam: &str, key: &str) -> Option<String> {
    match fam {
        "l" | "lx" => items(&c.send(&["LRANGE", key, "0", "-1"])),
        "ap" | "n" | "s" => scalar(&c.send(&["GET", key])),
        "hi" => scalar(&c.send(&["HGET", key, "f"])),
        "zi" => scalar(&c.send(&["ZSCORE", key, "m"])),
        "bf" => {
            if scalar(&c.send(&["EXISTS", key])).as_deref() != Some("1") {
                return None;
            }
            items(&c.send(&["BITFIELD", key, "GET", "u8", "0"]))
        }
        "h" => items(&c.send(&["HGETALL", key])).map(|s| {
            let mut fields: Vec<&str> = s.split(',').step_by(2).collect();
            fields.sort_unstable();
            fields.join(",")
        }),
        other => panic!("unknown family {other}"),
    }
}

fn expected_once(fam: &str) -> &'static str {
    match fam {
        "l" => "a,b,c,d,e",
        "lx" => "z,a,b",
        "ap" => "hello",
        "n" => "7",
        "hi" => "3",
        "zi" => "2.5",
        "bf" => "5",
        "h" => "f1,f2,f3",
        "s" => "v1",
        other => panic!("unknown family {other}"),
    }
}

fn write_all(c: &mut common::Conn, fams: &[&str]) {
    for fam in fams {
        for i in 0..PROBES {
            write_probe(c, fam, &format!("{fam}:{i}"));
        }
    }
}

fn drive_filler(c: &mut common::Conn, tag: &str) {
    let value = "f".repeat(FILLER_LEN);
    for i in 0..FILLER_COUNT {
        c.send(&["SET", &format!("filler:{tag}:{i}"), &value]);
    }
}

/// Every present probe must hold its expected value. Returns (present,
/// mismatches) so the caller decides vacuity and reports the corrupted list.
fn audit(
    c: &mut common::Conn,
    fams: &[&str],
    expect: impl Fn(&str) -> String,
) -> (usize, Vec<String>) {
    let mut present = 0;
    let mut bad = Vec::new();
    for fam in fams {
        for i in 0..PROBES {
            let key = format!("{fam}:{i}");
            if let Some(got) = read_probe(c, fam, &key) {
                present += 1;
                let want = expect(fam);
                if got != want {
                    bad.push(format!("{key}={got:?} (want {want:?})"));
                }
            }
        }
    }
    (present, bad)
}

/// A `BGREWRITEAOF` has landed. Runtime-agnostic: the manifest layout
/// (monoio `--shards 1`) advances `moon.aof.manifest`; the legacy
/// single-file layout (tokio `--shards 1`) replaces `appendonly.aof` with a
/// file that opens with the `MOON` RDB-preamble magic.
fn rewrite_landed(dir: &std::path::Path, manifest_before: Option<&str>) -> bool {
    let manifest = dir.join("appendonlydir").join("moon.aof.manifest");
    if let Some(before) = manifest_before {
        return std::fs::read_to_string(&manifest).ok().as_deref() != Some(before);
    }
    std::fs::read(dir.join("appendonly.aof")).is_ok_and(|b| b.starts_with(b"MOON"))
}

fn bgrewriteaof_and_wait(c: &mut common::Conn, dir: &std::path::Path) {
    let manifest = dir.join("appendonlydir").join("moon.aof.manifest");
    let before = std::fs::read_to_string(&manifest).ok();
    let reply = c.send(&["BGREWRITEAOF"]);
    assert!(reply.starts_with('+'), "BGREWRITEAOF refused: {reply:?}");
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if rewrite_landed(dir, before.as_deref()) {
            // The tokio writer renames the rewritten file into place, then
            // reopens it; give it a beat before the next append.
            std::thread::sleep(Duration::from_millis(200));
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("BGREWRITEAOF did not land under {}", dir.display());
}

fn kill_cycles(leg: &str, extra: &[&str]) {
    let dir = common::unique_test_dir(leg);
    std::fs::create_dir_all(&dir).expect("create dir");

    let (child, mut port) = start_moon(&dir, extra);
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        write_all(&mut c, FAMILIES);
        let (present, bad) = audit(&mut c, FAMILIES, |f| expected_once(f).to_string());
        assert_eq!(present, FAMILIES.len() * PROBES);
        assert!(
            bad.is_empty(),
            "{leg}: pre-crash sanity: {}",
            bad.join("; ")
        );
        drive_filler(&mut c, "0");
        std::thread::sleep(Duration::from_secs(1));
    }

    for cycle in 1..=3 {
        let cold_files = count_cold_files(&dir);
        guard.kill_now();
        common::wait_for_port_down(port);
        assert!(
            cold_files > 0,
            "{leg} cycle {cycle}: no cold data files at the SIGKILL — nothing spilled, \
             the run would be vacuous"
        );

        let (child, new_port) = start_moon(&dir, extra);
        guard = common::ServerGuard::new(child);
        port = new_port;
        {
            let mut c = common::Conn::open(port);
            let (present, bad) = audit(&mut c, FAMILIES, |f| expected_once(f).to_string());
            assert!(
                present >= FAMILIES.len() * PROBES / 2,
                "{leg} cycle {cycle}: only {present} of {} probes survived — recovery lost \
                 keys wholesale (moon#914 (c): a WAL holding only a MOON.SPILLED marker was \
                 taken as the KV authority and the AOF was never replayed)",
                FAMILIES.len() * PROBES
            );
            assert!(
                bad.is_empty(),
                "{leg} cycle {cycle}: {} probe(s) came back with a write applied more than \
                 once (moon#914 — no MOON.COLDCUT opened this replay generation, so the \
                 replay read the cold copy the command itself produced): {}",
                bad.len(),
                bad.join("; ")
            );
        }
        // Let the replay-driven eviction re-spill before the next kill: this
        // is what makes the defect COMPOUND (k grows by one per cycle).
        std::thread::sleep(Duration::from_millis(1500));
    }
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Leg 1: fresh boot, the AOF is the recovery authority.
#[test]
fn fresh_boot_kill9_cycles_apply_every_write_exactly_once() {
    kill_cycles("cold-cut-914-fresh", &[]);
}

/// Leg 3: `--wal-kv-log on`. On tokio `--shards 1` no connection-local write
/// reaches the WAL, but `ColdMarkerSink` mirrors every `MOON.SPILLED` into it,
/// and Phase 4b only falls back to the AOF when Phase 4 replayed zero KV
/// commands. Before the fix a single marker counted as one, so the AOF was
/// skipped and its whole history lost (timing-dependent here — the marker
/// must reach the WAL before the kill; the deterministic guard is the
/// `recovery` unit test `cold_plane_records_in_the_wal_do_not_suppress_the_aof_fallback`).
#[test]
fn wal_kv_log_on_kill9_cycles_apply_every_write_exactly_once() {
    kill_cycles("cold-cut-914-wal", &["--wal-kv-log", "on"]);
}

const COLD_CUT_HEAD: &[u8] = b"*2\r\n$12\r\nMOON.COLDCUT\r\n";

/// Every file that can hold the rewritten generation's RESP records: the
/// legacy single file (tokio `--shards 1`) and the manifest layout's incr
/// files (monoio).
fn aof_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = vec![dir.join("appendonly.aof")];
    if let Ok(rd) = std::fs::read_dir(dir.join("appendonlydir")) {
        out.extend(
            rd.flatten()
                .map(|e| e.path())
                .filter(|p| p.to_str().is_some_and(|n| n.ends_with(".incr.aof"))),
        );
    }
    out.retain(|p| p.is_file());
    out
}

/// One RESP array record at `pos`: its parts and the offset after it.
/// `None` for anything else, including a torn tail.
fn resp_record(buf: &[u8], pos: usize) -> Option<(Vec<&[u8]>, usize)> {
    fn line(buf: &[u8], pos: usize, tag: u8) -> Option<(usize, usize)> {
        if buf.get(pos) != Some(&tag) {
            return None;
        }
        let end = pos + buf.get(pos..)?.windows(2).position(|w| w == b"\r\n")?;
        let n = std::str::from_utf8(&buf[pos + 1..end]).ok()?.parse().ok()?;
        Some((n, end + 2))
    }
    let (count, mut p) = line(buf, pos, b'*')?;
    let mut parts = Vec::with_capacity(count);
    for _ in 0..count {
        let (len, start) = line(buf, p, b'$')?;
        parts.push(buf.get(start..start + len)?);
        p = start + len + 2;
        if p > buf.len() {
            return None;
        }
    }
    Some((parts, p))
}

/// Post-rewrite `MOON.SPILLED` records on disk that name a probe key.
fn probe_markers_on_disk(dir: &std::path::Path, fams: &[&str]) -> usize {
    let is_probe = |k: &[u8]| {
        fams.iter()
            .any(|f| k.starts_with(format!("{f}:").as_bytes()))
    };
    let mut n = 0;
    for path in aof_files(dir) {
        let buf = std::fs::read(&path).unwrap_or_default();
        let Some(mut pos) = buf
            .windows(COLD_CUT_HEAD.len())
            .position(|w| w == COLD_CUT_HEAD)
        else {
            continue;
        };
        while let Some((parts, next)) = resp_record(&buf, pos) {
            if parts.first() == Some(&&b"MOON.SPILLED"[..])
                && parts.iter().skip(2).any(|k| is_probe(k))
            {
                n += 1;
            }
            pos = next;
        }
    }
    n
}

/// Remove every `MOON.SPILLED` record after the rewritten generation's
/// `MOON.COLDCUT` head from the crash image, as if each had died with the
/// process. Returns how many were removed.
fn drop_spilled_markers(dir: &std::path::Path) -> usize {
    let mut removed = 0;
    for path in aof_files(dir) {
        let buf = std::fs::read(&path).expect("read AOF");
        let Some(head) = buf
            .windows(COLD_CUT_HEAD.len())
            .position(|w| w == COLD_CUT_HEAD)
        else {
            continue;
        };
        let mut out = buf[..head].to_vec();
        let mut pos = head;
        while let Some((parts, next)) = resp_record(&buf, pos) {
            if parts.first() == Some(&&b"MOON.SPILLED"[..]) {
                removed += 1;
            } else {
                out.extend_from_slice(&buf[pos..next]);
            }
            pos = next;
        }
        out.extend_from_slice(&buf[pos..]);
        std::fs::write(&path, out).expect("rewrite AOF");
    }
    removed
}

fn post_rewrite_expected(fam: &str) -> String {
    match fam {
        "l" => "a,b,c,d,e,f",
        "n" => "8",
        "ap" => "hello!",
        "s" => "v2",
        other => expected_once(other),
    }
    .to_string()
}

/// Leg 2: a write acknowledged after a `BGREWRITEAOF` taken while its key was
/// cold must survive a `kill -9` exactly once. The rewritten base is
/// hot-only, so the cold copy is the ONLY copy of the pre-rewrite state.
///
/// `respill = false` is moon#912's exact shape: SET-only, killed while the
/// post-rewrite values are hot. It must stay SET-only: a promoting write
/// (RPUSH/INCRBY/APPEND on a cold key) grows the hot set, the next eviction
/// logs a `MOON.SPILLED` into the new generation, and a marker-bearing
/// generation resolves hot-wins since moon#965 — which masks the missing cut
/// and turns this leg green on the unfixed code. `respill = true` spills the
/// post-rewrite values AGAIN before the kill, so replay meets a cold file
/// newer than the rewrite — the case that needs the rewritten generation to
/// re-open the cut rather than inherit none.
fn post_rewrite(leg: &str, fams: &[&str], respill: bool, lose_markers: bool) {
    let dir = common::unique_test_dir(leg);
    std::fs::create_dir_all(&dir).expect("create dir");

    let (child, port) = start_moon(&dir, &[]);
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        write_all(&mut c, fams);
        drive_filler(&mut c, "0");
        std::thread::sleep(Duration::from_secs(1));
        assert!(
            count_cold_files(&dir) > 0,
            "{leg}: nothing spilled before the rewrite"
        );

        bgrewriteaof_and_wait(&mut c, &dir);

        for i in 0..PROBES {
            for fam in fams {
                let key = format!("{fam}:{i}");
                match *fam {
                    "l" => c.send(&["RPUSH", &key, "f"]),
                    "n" => c.send(&["INCRBY", &key, "1"]),
                    "ap" => c.send(&["APPEND", &key, "!"]),
                    "s" => c.send(&["SET", &key, "v2"]),
                    other => panic!("no post-rewrite write for family {other}"),
                };
            }
        }
        let (_, bad) = audit(&mut c, fams, post_rewrite_expected);
        assert!(
            bad.is_empty(),
            "{leg}: live post-rewrite sanity: {}",
            bad.join("; ")
        );
        if respill {
            drive_filler(&mut c, "1");
        }
        std::thread::sleep(Duration::from_secs(1));
        if lose_markers {
            // Wait for a respill of a probe key to be logged: dropping it
            // below is what this leg is about, so it must exist first.
            let deadline = Instant::now() + Duration::from_secs(30);
            while probe_markers_on_disk(&dir, fams) == 0 {
                assert!(
                    Instant::now() < deadline,
                    "{leg}: no post-rewrite MOON.SPILLED naming a probe key reached the AOF \
                     — nothing was respilled, the run would be vacuous"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
    let cold_files = count_cold_files(&dir);
    guard.kill_now();
    common::wait_for_port_down(port);
    assert!(
        cold_files > 0,
        "{leg}: no cold data files at the SIGKILL — vacuous run"
    );
    if lose_markers {
        let removed = drop_spilled_markers(&dir);
        assert!(removed > 0, "{leg}: no marker to drop — vacuous run");
    }

    let (child, port2) = start_moon(&dir, &[]);
    let mut guard2 = common::ServerGuard::new(child);
    let (present, bad) = {
        let mut c = common::Conn::open(port2);
        audit(&mut c, fams, post_rewrite_expected)
    };
    guard2.kill_now();
    // Keep the data dir (server.err, AOF, manifest, heap files) when the
    // audit fails: a lost write is only explainable from the crash image.
    if present >= fams.len() * PROBES / 2 && bad.is_empty() {
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        present >= fams.len() * PROBES / 2,
        "{leg}: only {present} probes survived (data dir kept: {})",
        dir.display()
    );
    assert!(
        bad.is_empty(),
        "{leg}: {} acknowledged post-rewrite write(s) did not survive a kill -9 exactly \
         once (moon#914 — the rewritten generation opened without a MOON.COLDCUT; \
         moon#1140 — a respill whose marker was lost hid the pre-rewrite copy): {} \
         (data dir kept: {})",
        bad.len(),
        bad.join("; "),
        dir.display()
    );
}

/// Leg 2a — moon#912's shape: kill while the post-rewrite writes are hot.
#[test]
fn post_rewrite_write_to_a_cold_key_survives_kill9() {
    post_rewrite("cold-cut-914-rewrite", &["s"], false, false);
}

/// Leg 2b — the post-rewrite values are spilled again before the kill.
#[test]
fn post_rewrite_write_respilled_survives_kill9_exactly_once() {
    post_rewrite(
        "cold-cut-914-rewrite-respill",
        &["l", "n", "ap", "s"],
        true,
        false,
    );
}

/// Leg 2c — moon#1140: 2b with the respill's `MOON.SPILLED` markers lost.
/// The respilled files are in the manifest, so the rebuilt index points each
/// respilled key at a file past the cut that no marker authorizes. Before
/// the fix the replay then read no base for the key at all: `l:17` came back
/// `"f"` (want `"a,b,c,d,e,f"`), the shape a hosted-macOS run hit when its
/// SIGKILL beat the writer's flush of those markers.
#[test]
fn post_rewrite_respill_survives_its_spill_markers_being_lost() {
    post_rewrite(
        "cold-cut-914-rewrite-lost-markers",
        &["l", "n", "ap", "s"],
        true,
        true,
    );
}
