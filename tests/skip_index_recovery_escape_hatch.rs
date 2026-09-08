//! `MOON_SKIP_INDEX_RECOVERY=1` trades the boot-time index rebuild for an
//! immediately-serving shard — and must not cost anything permanently.
//!
//! ## Why the hatch exists
//!
//! `recover_indexes_task` restores index definitions from the sidecars and then
//! walks every key matching an index prefix, re-deriving its postings and
//! vectors. Until that walk finishes the shard answers `-LOADING` to everything
//! that matters, so from a client's point of view the server is down. On a
//! production instance the walk was measured at ~200 ms of CPU per key over
//! 293,439 keys. An operator who needs the KV plane back before the indexes are
//! rebuilt currently has no way to ask for it.
//!
//! ## The property this test actually defends
//!
//! Skipping the walk is only an acceptable trade if it is REVERSIBLE. Phase 3
//! (`RecoveryState::finish`) runs a deletion probe that tombstones every
//! key_hash the manifest loaded which the walk did not OBSERVE. A skipped walk
//! observes nothing, so running the probe anyway would tombstone the entire
//! index — turning "this boot has no indexes" into "these indexes are gone".
//! That failure is invisible at the moment it happens: the hatch boot looks
//! exactly the same either way. It only shows up on the NEXT normal boot.
//!
//! So the assertion order matters: search must come back on a plain restart
//! AFTER a hatch boot. A test that only checked "the hatch boot returns zero
//! results" would pass just as happily against a build that had erased the
//! durable state on disk.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const DOCS: usize = 40;

fn spawn(port: u16, dir: &std::path::Path, skip: bool) -> Child {
    let mut cmd = Command::new(common::find_moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--shards",
        "1",
        "--appendonly",
        "yes",
        "--appendfsync",
        "always",
        "--disk-free-min-pct",
        "0",
        "--dir",
    ])
    .arg(dir)
    .stdout(std::process::Stdio::null())
    .stderr(common::server_stderr(dir));
    // Absent, not "0": the point is that an unset variable is the default
    // path, and inheriting a stray value from the test runner's own
    // environment would make the control leg silently measure the hatch.
    if skip {
        cmd.env("MOON_SKIP_INDEX_RECOVERY", "1");
    } else {
        cmd.env_remove("MOON_SKIP_INDEX_RECOVERY");
    }
    cmd.spawn()
        .expect("spawn moon (run `cargo build --release` first)")
}

/// One connection reused for the whole leg. A fresh connection per probe hides
/// shard-local state, and moon has shipped bugs that only reproduce on a
/// connection that has already been used.
struct Conn(TcpStream);

impl Conn {
    fn open(port: u16) -> Self {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            assert!(Instant::now() < deadline, "server never answered PING");
            if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
                s.set_read_timeout(Some(Duration::from_secs(30))).ok();
                if s.write_all(b"PING\r\n").is_ok() {
                    let mut buf = [0u8; 64];
                    if let Ok(n) = s.read(&mut buf)
                        && buf[..n].starts_with(b"+PONG")
                    {
                        return Conn(s);
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn cmd(&mut self, parts: &[&str]) -> String {
        let mut out = format!("*{}\r\n", parts.len());
        for p in parts {
            out.push_str(&format!("${}\r\n{p}\r\n", p.len()));
        }
        self.0.write_all(out.as_bytes()).expect("write");
        let mut buf = vec![0u8; 64 * 1024];
        let n = self.0.read(&mut buf).expect("read reply");
        String::from_utf8_lossy(&buf[..n]).to_string()
    }
}

/// Wait out `-LOADING`, then return the reply.
///
/// `Conn::open` only waits for `PING`, which is on the loading allowlist — so
/// a data command sent straight after it races the recovery task and comes back
/// `-LOADING`. That race made an earlier version of this test detect a broken
/// build once and then pass against the same build, which is worse than not
/// detecting it: a flaky guard reads as a fixed bug.
fn when_ready(c: &mut Conn, parts: &[&str]) -> String {
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let r = c.cmd(parts);
        if !r.contains("LOADING") {
            return r;
        }
        assert!(
            Instant::now() < deadline,
            "still -LOADING after 90s: {parts:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// End a leg CLEANLY.
///
/// `ServerGuard::drop` SIGKILLs, which is right for most suites and fatal for
/// this one: nothing re-persists its index state on the way out, so the
/// on-disk artefacts cannot change no matter what the boot did, and a
/// fingerprint comparison across a SIGKILLed leg is trivially equal. Measured:
/// with SIGKILL teardown a build that deliberately ran the deletion probe
/// during a hatch boot passed this test 3 times out of 3.
fn shutdown_cleanly(c: &mut Conn, port: u16) {
    // SHUTDOWN never replies on success; the connection just closes.
    let _ = c.0.write_all(b"SHUTDOWN\r\n");
    common::wait_for_port_down(port);
}

/// Content fingerprint of every durable index artefact under `dir`.
///
/// Path -> (length, sha-ish content hash). The hatch's contract to the operator
/// is literally "the durable index state on disk is untouched"; this is the
/// only way to assert that claim rather than infer it from a search result.
/// Search results cannot see it: the next normal boot walks the whole keyspace
/// and re-derives every document from the hashes, so a boot that trashed the
/// durable state and one that left it alone answer FT.SEARCH identically.
fn index_state_fingerprint(dir: &std::path::Path) -> Vec<(String, u64, u64)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, u64, u64)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
                continue;
            }
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            // Index artefacts only. The WAL, AOF and checkpoint files change on
            // every boot by design, and including them would make this assert
            // "nothing happened at all", which is not the claim.
            let is_index = name.ends_with(".tpost")
                || name.ends_with(".tfst")
                || name.ends_with(".meta")
                || name.ends_with(".mpf")
                || name.ends_with(".keymap");
            if !is_index {
                continue;
            }
            let Ok(bytes) = std::fs::read(&p) else {
                continue;
            };
            // FNV-1a: no dependency, and collisions are irrelevant here — the
            // comparison is against the same file a moment earlier.
            let mut h: u64 = 0xcbf29ce484222325;
            for b in &bytes {
                h ^= *b as u64;
                h = h.wrapping_mul(0x100000001b3);
            }
            out.push((
                p.strip_prefix(dir).unwrap_or(&p).display().to_string(),
                bytes.len() as u64,
                h,
            ));
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out.sort();
    out
}

/// Match count from an `FT.SEARCH` reply.
///
/// RESP2 shape is `*<1 + 2n>` then `:<n>` then n * (key, field-array). The
/// FIRST ELEMENT is the count; the array header is `1 + 2n` and reading THAT
/// as the count is off by one plus a factor of two — which is exactly how this
/// test first failed (81 where 40 documents were indexed). Parse the element,
/// and refuse to guess if it is not the integer the protocol says it is.
fn hit_count(reply: &str) -> i64 {
    let mut lines = reply.lines();
    let head = lines.next().unwrap_or("");
    assert!(
        head.starts_with('*'),
        "FT.SEARCH did not return an array: {reply:?}"
    );
    let count = lines.next().unwrap_or("");
    assert!(
        count.starts_with(':'),
        "FT.SEARCH's first element was not the integer match count: {reply:?}"
    );
    count[1..].trim().parse::<i64>().expect("match count")
}

#[test]
fn the_skip_hatch_is_reversible_and_never_erases_durable_index_state() {
    let dir = common::unique_test_dir("moon-882-skip-hatch");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let port = common::reserve_port();

    // ---- leg 1: build the index and prove it answers ----------------------
    {
        let guard = common::ServerGuard::new(spawn(port, &dir, false));
        let mut c = Conn::open(port);
        assert!(
            c.cmd(&[
                "FT.CREATE",
                "tidx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "d:",
                "SCHEMA",
                "body",
                "TEXT",
            ])
            .starts_with("+OK"),
            "FT.CREATE failed"
        );
        for i in 0..DOCS {
            c.cmd(&["HSET", &format!("d:{i}"), "body", "alpha beta gamma"]);
        }
        assert_eq!(
            hit_count(&c.cmd(&["FT.SEARCH", "tidx", "alpha"])),
            DOCS as i64,
            "baseline: every document must be findable before any restart"
        );
        shutdown_cleanly(&mut c, port);
        drop(guard);
    }

    // The state a hatch boot promises not to touch, captured while the server
    // is DOWN so nothing is mid-write.
    let before = index_state_fingerprint(&dir);
    assert!(
        !before.is_empty(),
        "no durable index artefacts were written at all, so leg 2 would assert \
         that nothing changed about nothing -- the test would be vacuous"
    );

    // ---- leg 2: hatch boot -- serves, and does not erase anything ---------
    {
        let guard = common::ServerGuard::new(spawn(port, &dir, true));
        let mut c = Conn::open(port);
        // The KV plane is whole regardless: the hatch only skips the index walk.
        assert!(
            when_ready(&mut c, &["HGET", "d:0", "body"]).contains("alpha beta gamma"),
            "hatch boot lost KV data, which it must never touch"
        );
        // Whether search answers 0 or DOCS here depends on what a persisted
        // snapshot covered (.tpost, moon#879), so this leg deliberately does
        // NOT assert a hit count -- asserting one would encode today's
        // snapshot coverage as the contract. What it must never do is error.
        let r = when_ready(&mut c, &["FT.SEARCH", "tidx", "alpha"]);
        assert!(
            !r.starts_with("-ERR"),
            "hatch boot made FT.SEARCH an error rather than a (possibly empty) result: {r:?}"
        );
        shutdown_cleanly(&mut c, port);
        drop(guard);
    }

    // The claim in the WARN line the operator reads, asserted directly.
    // Running the deletion probe here would tombstone every key_hash the
    // manifest loaded (the walk observed none), rewriting exactly these files
    // -- during a boot the operator invoked to AVOID that work.
    let after = index_state_fingerprint(&dir);
    assert_eq!(
        before, after,
        "the hatch boot rewrote durable index artefacts. It promises the \
         opposite in its own WARN line, and a boot that tombstones every \
         key in every index is not the fast boot the operator asked for.\n\
         before: {before:?}\nafter:  {after:?}"
    );

    // ---- leg 3: the property -- a plain restart brings search back --------
    {
        let guard = common::ServerGuard::new(spawn(port, &dir, false));
        let mut c = Conn::open(port);
        let hits = hit_count(&when_ready(&mut c, &["FT.SEARCH", "tidx", "alpha"]));
        assert_eq!(
            hits, DOCS as i64,
            "a normal restart after a hatch boot must rebuild the FULL index. \
             Getting {hits} instead of {} means the hatch boot ran the deletion \
             probe with nothing observed and tombstoned the durable state -- the \
             hatch cost the indexes permanently, not for one boot.",
            DOCS
        );
        drop(guard);
        common::wait_for_port_down(port);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
