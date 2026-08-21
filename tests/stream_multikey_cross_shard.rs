//! A multi-stream `XREAD`/`XREADGROUP` must never answer from a subset of its
//! streams (moon#605).
//!
//! moon routes a command to ONE shard — the owner of `extract_primary_key`'s
//! answer — and that shard runs the whole command against its own slice. For a
//! multi-stream read the other streams live on other shards and are simply
//! invisible, read as "does not exist".
//!
//! For `XREAD` that is an under-read. For `XREADGROUP` it is **data loss**:
//! the routed shard's `read_group_new` CLAIMS its local stream's entries into
//! the consumer's PEL, and the command then under-reports, so those entries
//! are marked delivered-and-unacked to a consumer that never received them.
//! `XREADGROUP >` will never return them again; only `XPENDING`/`XAUTOCLAIM`
//! recovers them.
//!
//! Measured, `--shards 4`, 12 key pairs each holding one undelivered entry:
//!
//! ```text
//! redis 8.6.1   stranded  0 / 24
//! moon          stranded  7 / 24
//! ```
//!
//! The invariant asserted here is the one that matters and that holds for
//! BOTH possible fixes (serve every stream, or refuse the command): **an entry
//! is never left PENDING unless the reply that claimed it named it.** A server
//! that answers a loud error and claims nothing satisfies it; a server that
//! silently answers half satisfies neither it nor its clients.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards, so a pair of untagged keys usually spans two of them.
const SHARDS: &str = "4";
/// Key pairs per assertion. At p(same shard)=0.25 the chance that all 12 pairs
/// are co-located — and vacuously pass — is under 6e-8.
const PAIRS: usize = 12;

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-smx-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap_or("/tmp"),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-smx-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port}\n--- stderr ---\n{log}");
}

/// Seed one undelivered entry into `key` and create group `g` at `0`.
fn seed(c: &mut Conn, key: &str, id: &str) {
    let created = c.send(&["XGROUP", "CREATE", key, "g", "0", "MKSTREAM"]);
    assert!(
        created.starts_with('+'),
        "XGROUP CREATE {key} failed: {created:?}"
    );
    let added = c.send(&["XADD", key, id, "f", "v"]);
    assert!(added.starts_with('$'), "XADD {key} failed: {added:?}");
}

/// Every entry id `XPENDING` reports for `key`.
fn pending_ids(c: &mut Conn, key: &str) -> Vec<String> {
    let reply = c.send(&["XPENDING", key, "g", "-", "+", "10"]);
    // `*N` of `*4` entries whose first element is the id. Scan for the bulk
    // strings that look like stream ids rather than decoding the whole shape —
    // this is a probe, and an id is unambiguous here (`<ms>-<seq>`).
    reply
        .split("\r\n")
        .filter(|t| {
            t.split_once('-').is_some_and(|(a, b)| {
                !a.is_empty()
                    && a.bytes().all(|c| c.is_ascii_digit())
                    && !b.is_empty()
                    && b.bytes().all(|c| c.is_ascii_digit())
            })
        })
        .map(str::to_string)
        .collect()
}

/// moon#605: an entry must never be left PENDING unless the reply that claimed
/// it named it.
///
/// Satisfied by serving every stream, and satisfied by refusing the command
/// outright — but NOT by silently answering from the streams one shard happens
/// to own, which is what claims entries nobody receives.
#[test]
fn smx1_multi_stream_xreadgroup_never_strands_an_entry_it_did_not_deliver() {
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);

    let mut stranded: Vec<String> = Vec::new();
    let mut refused = 0usize;
    for i in 0..PAIRS {
        let (a, b) = (format!("smx1a{i}"), format!("smx1b{i}"));
        seed(&mut c, &a, "1-1");
        seed(&mut c, &b, "1-1");

        let reply = c.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "cc",
            "COUNT",
            "10",
            "STREAMS",
            &a,
            &b,
            ">",
            ">",
        ]);
        if reply.starts_with('-') {
            // A loud refusal is an acceptable answer — but it must not have
            // claimed anything on the way out.
            refused += 1;
        }

        for key in [&a, &b] {
            for id in pending_ids(&mut c, key) {
                // Delivered ids appear in the reply; a pending id that does not
                // is an entry claimed for a consumer that never saw it.
                if !reply.contains(&id) || reply.starts_with('-') {
                    stranded.push(format!("  {key} entry {id} pending, reply {reply:?}"));
                }
            }
        }
    }
    assert!(
        stranded.is_empty(),
        "{} of {} entries were claimed but never delivered ({refused} of {PAIRS} pairs were \
         refused outright, which is fine — claiming is not):\n{}",
        stranded.len(),
        PAIRS * 2,
        stranded.join("\n")
    );
}

/// The read-only half: a multi-stream `XREAD` must not answer from a subset.
/// Either both streams appear, or the command is refused — silently returning
/// only the streams one shard owns tells the client the others are empty.
#[test]
fn smx2_multi_stream_xread_does_not_silently_answer_from_one_shard() {
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);

    let mut wrong: Vec<String> = Vec::new();
    for i in 0..PAIRS {
        let (a, b) = (format!("smx2a{i}"), format!("smx2b{i}"));
        for k in [&a, &b] {
            let added = c.send(&["XADD", k, "1-1", "f", "v"]);
            assert!(added.starts_with('$'), "XADD {k} failed: {added:?}");
        }
        let reply = c.send(&["XREAD", "COUNT", "10", "STREAMS", &a, &b, "0", "0"]);
        if reply.starts_with('-') {
            continue; // refused outright — acceptable
        }
        let saw_a = reply.contains(&a);
        let saw_b = reply.contains(&b);
        if !(saw_a && saw_b) {
            wrong.push(format!(
                "  pair ({a}, {b}): saw_a={saw_a} saw_b={saw_b} reply {reply:?}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {PAIRS} multi-stream XREADs answered from a subset of their streams:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// Co-located streams (a shared hash tag) are provably on one shard, so they
/// must be served in FULL — a fix that refuses cross-shard reads must not
/// refuse these too.
#[test]
fn smx3_hash_tagged_streams_are_served_in_full() {
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);

    for i in 0..PAIRS {
        let (a, b) = (format!("{{t{i}}}:a"), format!("{{t{i}}}:b"));
        seed(&mut c, &a, "1-1");
        seed(&mut c, &b, "1-1");
        let reply = c.send(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "cc",
            "COUNT",
            "10",
            "STREAMS",
            &a,
            &b,
            ">",
            ">",
        ]);
        assert!(
            reply.contains(&a) && reply.contains(&b),
            "co-located streams must both be served, got {reply:?}"
        );
    }
}

/// A single-stream read is routed to its own owner and was never affected;
/// pin it so a cross-shard guard cannot regress the common case.
#[test]
fn smx4_single_stream_reads_are_unaffected() {
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);

    for i in 0..PAIRS {
        let k = format!("smx4{i}");
        seed(&mut c, &k, "1-1");
        let reply = c.send(&["XREADGROUP", "GROUP", "g", "cc", "STREAMS", &k, ">"]);
        assert!(
            reply.contains(&k) && reply.contains("1-1"),
            "single-stream XREADGROUP on {k} must be served, got {reply:?}"
        );
    }
}
