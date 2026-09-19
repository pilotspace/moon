//! moon#914 quadrant (b): an `appendonly.aof` written before the
//! `MOON.COLDCUT` head existed must stop compounding after the first boot of
//! a fixed binary.
//!
//! `runtime-tokio` + `--shards 1` is the one configuration with no
//! `AofManifest`: recovery replays the legacy single-file `appendonly.aof`.
//! A file written by a binary from before #1017 opens with no cut, so replay
//! reads every cold file UNGATED. A replayed non-idempotent write then finds
//! the cold copy its own earlier execution produced, promotes it, and applies
//! itself again (moon#902's double-apply). Nothing rewrites that file, so
//! every later boot replays the same headless log over the re-spilled result
//! and the damage compounds: a five-element list reads 10, then 15, then 20.
//!
//! The FIRST boot of such a file can't be corrected: the log holds no record
//! of which writes preceded which spill. What the fix guarantees is that
//! boot 1 is the last ungated replay. It rewrites the AOF once, so the file
//! then opens with its cut, and boots 2..N hold exactly what boot 1 served.
//!
//! Synthesising the legacy file: a #1017+ binary writes `MOON.COLDCUT` into a
//! fresh `appendonly.aof` as its FIRST record, and that head is the only
//! record #1017 added to this file at fresh boot. Stripping it leaves the
//! byte stream a pre-#1017 binary would have written for the same session:
//! the writes plus the `MOON.SPILLED` markers this file has carried since
//! #911. The test asserts the head is there before stripping it, which also
//! proves the binary under test runs the legacy single-file layout (a monoio
//! binary writes a manifest, not this file).
//!
//! tokio-only: the configuration the bug lives in. Pin `MOON_BIN` to a tokio
//! build when running it by hand. `--disk-free-min-pct 0` is REQUIRED: the
//! disk-free guard silently guts a crash test on a nearly-full volume.

#![cfg(all(feature = "runtime-tokio", not(feature = "runtime-monoio")))]
#![allow(clippy::unwrap_used)]

mod common;

use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const PROBES: usize = 24;
const FILLER_COUNT: usize = 200;
const FILLER_LEN: usize = 4096;
const MAXMEMORY: &str = "524288"; // 512 KiB — far below the filler wave.

/// Non-idempotent families (any double-apply shows) plus one idempotent
/// control (`s`).
const FAMILIES: &[&str] = &["l", "n", "ap", "hi", "s"];

/// The head #1017 writes as the first record of a fresh `appendonly.aof`.
const COLD_CUT_HEAD: &[u8] = b"*2\r\n$12\r\nMOON.COLDCUT\r\n";

fn start_moon(dir: &Path) -> (Child, u16) {
    let dir = dir.to_path_buf();
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
            // tracing writes to stdout; the test counts the moon#914 lines.
            .stdout(server_log(&dir))
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// Every boot's stdout (the tracing log), appended to one file.
fn server_log(dir: &Path) -> std::process::Stdio {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("server.log"))
        .map_or_else(|_| std::process::Stdio::null(), std::process::Stdio::from)
}

fn count_cold_files(dir: &Path) -> usize {
    fn walk(p: &Path, acc: &mut usize) {
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

fn write_probe(c: &mut common::Conn, fam: &str, key: &str) {
    match fam {
        "l" => c.send(&["RPUSH", key, "a", "b", "c", "d", "e"]),
        "n" => c.send(&["INCRBY", key, "7"]),
        "ap" => c.send(&["APPEND", key, "hello"]),
        "hi" => c.send(&["HINCRBY", key, "f", "3"]),
        "s" => c.send(&["SET", key, "v1"]),
        other => panic!("unknown family {other}"),
    };
}

/// One probe's observable state, stable across restarts. `None` == absent.
fn read_probe(c: &mut common::Conn, fam: &str, key: &str) -> Option<String> {
    let reply = match fam {
        "l" => c.send(&["LLEN", key]),
        "n" | "ap" | "s" => c.send(&["GET", key]),
        "hi" => c.send(&["HGET", key, "f"]),
        other => panic!("unknown family {other}"),
    };
    let mut lines = reply.lines();
    let head = lines.next()?;
    if head.starts_with("$-1") || head.starts_with('_') || head == ":0" && fam == "l" {
        return None;
    }
    if let Some(n) = head.strip_prefix(':') {
        return Some(n.to_string());
    }
    if head.starts_with('$') {
        return lines.next().map(str::to_string);
    }
    Some(head.to_string())
}

fn expected_once(fam: &str) -> &'static str {
    match fam {
        "l" => "5",
        "n" => "7",
        "ap" => "hello",
        "hi" => "3",
        "s" => "v1",
        other => panic!("unknown family {other}"),
    }
}

/// Write enough large filler keys to push the probes out to the cold tier.
fn drive_filler(c: &mut common::Conn, tag: &str) {
    let value = "f".repeat(FILLER_LEN);
    for i in 0..FILLER_COUNT {
        c.send(&["SET", &format!("filler:{tag}:{i}"), &value]);
    }
}

/// Every probe's state, in a fixed order.
fn snapshot(c: &mut common::Conn) -> Vec<(String, Option<String>)> {
    let mut out = Vec::with_capacity(FAMILIES.len() * PROBES);
    for fam in FAMILIES {
        for i in 0..PROBES {
            let key = format!("{fam}:{i}");
            let got = read_probe(c, fam, &key);
            out.push((key, got));
        }
    }
    out
}

fn diff(
    a: &[(String, Option<String>)],
    b: &[(String, Option<String>)],
    a_name: &str,
    b_name: &str,
) -> Vec<String> {
    a.iter()
        .zip(b)
        .filter(|((_, x), (_, y))| x != y)
        .map(|((k, x), (_, y))| format!("{k}: {a_name}={x:?} {b_name}={y:?}"))
        .collect()
}

/// Strip the `MOON.COLDCUT` head from `appendonly.aof`, leaving what a
/// pre-#1017 binary would have written. Panics unless the file opens with
/// the head (the precondition that makes this a legacy-file synthesis at all).
fn strip_cold_cut_head(aof: &Path) {
    let bytes = std::fs::read(aof).expect("read appendonly.aof");
    assert!(
        bytes.starts_with(COLD_CUT_HEAD),
        "precondition: {} must open with the #1017 MOON.COLDCUT head — is MOON_BIN a \
         runtime-tokio build of #1017 or later? (first bytes: {:?})",
        aof.display(),
        String::from_utf8_lossy(&bytes[..bytes.len().min(48)])
    );
    // `$<n>\r\n<watermark>\r\n` follows the command name.
    let rest = &bytes[COLD_CUT_HEAD.len()..];
    assert_eq!(rest.first(), Some(&b'$'), "malformed head");
    let len_end = rest.windows(2).position(|w| w == b"\r\n").unwrap();
    let n: usize = std::str::from_utf8(&rest[1..len_end])
        .unwrap()
        .parse()
        .unwrap();
    let head_len = COLD_CUT_HEAD.len() + len_end + 2 + n + 2;
    let legacy = &bytes[head_len..];
    assert!(
        legacy.first() == Some(&b'*'),
        "the record after the head must be a RESP command"
    );
    assert!(
        !legacy.windows(12).any(|w| w == b"MOON.COLDCUT"),
        "the synthesized legacy AOF still carries a MOON.COLDCUT"
    );
    std::fs::write(aof, legacy).expect("write legacy appendonly.aof");
}

/// Whether `appendonly.aof` now opens with a cut: a rewrite's RDB preamble
/// (which #1017 follows with the head) or the head itself.
fn aof_has_cut(aof: &Path) -> bool {
    std::fs::read(aof).is_ok_and(|b| b.starts_with(b"MOON") || b.starts_with(COLD_CUT_HEAD))
}

fn log_count(dir: &Path, needle: &str) -> usize {
    std::fs::read_to_string(dir.join("server.log"))
        .map(|s| s.matches(needle).count())
        .unwrap_or(0)
}

/// Boot, read every probe, give the boot-time rewrite (if any) a bounded
/// window to land, let replay-driven eviction re-spill, then SIGKILL.
fn boot_read_kill(dir: &Path, label: &str) -> Vec<(String, Option<String>)> {
    let aof = dir.join("appendonly.aof");
    let (child, port) = start_moon(dir);
    let mut guard = common::ServerGuard::new(child);
    let snap = {
        let mut c = common::Conn::open(port);
        snapshot(&mut c)
    };
    // The fixed binary schedules its one rewrite right after it starts
    // accepting. Wait for it with a deadline, so the unfixed binary (which
    // never rewrites) still reaches its assertion.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !aof_has_cut(&aof) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    // Spill this boot's values again, so the next replay meets a cold file
    // holding what this boot served. That is what lets an ungated replay
    // compound instead of re-deriving the same answer from the same files.
    {
        let mut c = common::Conn::open(port);
        drive_filler(&mut c, label);
    }
    // The tail (spill markers) reaches disk within ~1s.
    std::thread::sleep(Duration::from_millis(1500));
    let cold_files = count_cold_files(dir);
    guard.kill_now();
    common::wait_for_port_down(port);
    assert!(
        cold_files > 0,
        "{label}: no cold data files at the SIGKILL — the run would be vacuous"
    );
    snap
}

/// Boots 2 and 3 of a legacy (cut-less) AOF must hold exactly what boot 1
/// served. Before the fix every boot re-replayed the same headless log over
/// the re-spilled result and the non-idempotent probes grew again.
#[test]
fn legacy_cutless_aof_stops_compounding_after_the_first_boot() {
    let dir = common::unique_test_dir("legacy-aof-rewrite-914");
    std::fs::create_dir_all(&dir).expect("create dir");
    let aof = dir.join("appendonly.aof");

    // Session 0: write the probes, spill them, SIGKILL.
    let (child, port) = start_moon(&dir);
    let mut guard = common::ServerGuard::new(child);
    {
        let mut c = common::Conn::open(port);
        for fam in FAMILIES {
            for i in 0..PROBES {
                write_probe(&mut c, fam, &format!("{fam}:{i}"));
            }
        }
        drive_filler(&mut c, "session-0");
        std::thread::sleep(Duration::from_secs(1));
        let snap = snapshot(&mut c);
        let bad: Vec<_> = snap
            .iter()
            .filter(|(k, v)| {
                let fam = k.split(':').next().unwrap();
                v.as_deref() != Some(expected_once(fam))
            })
            .collect();
        assert!(bad.is_empty(), "pre-crash sanity: {bad:?}");
    }
    let cold_files = count_cold_files(&dir);
    guard.kill_now();
    common::wait_for_port_down(port);
    assert!(cold_files > 0, "session 0: nothing spilled");

    strip_cold_cut_head(&aof);

    let boot1 = boot_read_kill(&dir, "boot 1");
    let boot2 = boot_read_kill(&dir, "boot 2");
    let boot3 = boot_read_kill(&dir, "boot 3");
    let rewrites = log_count(&dir, "moon#914: replayed a legacy appendonly.aof");
    let completed = log_count(&dir, "boot-time moon#914 rewrite complete");
    eprintln!(
        "legacy-AOF WARN logged on {rewrites} boot(s); boot-time rewrite completed {completed} time(s)"
    );
    let _ = std::fs::remove_dir_all(&dir);

    let present = boot1.iter().filter(|(_, v)| v.is_some()).count();
    assert!(
        present >= FAMILIES.len() * PROBES / 2,
        "boot 1: only {present} probes survived — recovery lost keys wholesale"
    );
    // Boot 1 is documented, not asserted: its damage is uncorrectable.
    let boot1_damage = boot1
        .iter()
        .filter(|(k, v)| {
            let fam = k.split(':').next().unwrap();
            v.is_some() && v.as_deref() != Some(expected_once(fam))
        })
        .count();
    eprintln!(
        "boot 1 of the legacy AOF: {boot1_damage}/{present} probes already carry the \
         uncorrectable first-boot double-apply"
    );

    let d12 = diff(&boot1, &boot2, "boot1", "boot2");
    let d23 = diff(&boot2, &boot3, "boot2", "boot3");
    assert!(
        d12.is_empty() && d23.is_empty(),
        "a legacy cut-less AOF kept compounding after its first boot (moon#914 (b)): \
         {} probe(s) changed boot1->boot2, {} boot2->boot3:\n{}\n{}",
        d12.len(),
        d23.len(),
        d12.join("\n"),
        d23.join("\n")
    );
    assert_eq!(
        (rewrites, completed),
        (1, 1),
        "the legacy AOF must be detected on boot 1 only, and rewritten exactly once: the \
         rewritten file opens with its cut, so later boots must not detect it again"
    );
}
