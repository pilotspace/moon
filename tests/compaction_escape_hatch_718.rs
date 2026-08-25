//! A compaction backlog must not refuse the commands that cure it. (moon#718)
//!
//! Reported as a permanent write outage with no in-band recovery: background
//! merges fail, immutable segments accumulate to
//! `--max-unflushed-immutable-segments`, and every foreground write is refused
//! with `MOONERR busy: compaction backlog`. Reads keep working. So does the
//! server. But **both documented escapes are themselves foreground writes** —
//! `FT.COMPACT` drains the backlog and `FT.CONFIG SET <idx>
//! MERGE_RECALL_TOLERANCE 0` relaxes the gate a repeatedly-failing merge is
//! stuck behind, which is the command the rejection log line recommends by
//! name. The registry flags both `W`, so the backlog refused its own remedy and
//! the only exit was a restart, which replays into the same state.
//!
//! `src/shard/segment_stall.rs` had claimed the exemption already existed
//! ("`FT.COMPACT` / `GRAPH.COMPACT` commands bypass this guard"). It did not,
//! and `GRAPH.COMPACT` is not a command at all.
//!
//! This test builds a real backlog rather than poking the stall atomic, so it
//! fails if the exemption is correct in the predicate but never reaches
//! dispatch — which is exactly the shape of the original bug.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

/// Stall at 2 immutable segments instead of 20, so the backlog is cheap.
const MAX_UNFLUSHED: &str = "1";
/// Compact every 100 documents — the minimum the server accepts
/// ("COMPACT_THRESHOLD must be 100-100000"), so DOCS below must clear several
/// multiples of it to exceed MAX_UNFLUSHED immutable segments.
const COMPACT_THRESHOLD: &str = "100";
/// Documents per round. Comfortably over COMPACT_THRESHOLD so each round's
/// explicit FT.COMPACT produces a segment.
const DOCS_PER_ROUND: u16 = 150;
/// Rounds. The stall needs MORE immutable segments than MAX_UNFLUSHED, so two
/// rounds only reaches the boundary; three clears it.
const ROUNDS: u8 = 3;
const DIM: usize = 4;

fn encode(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// One request, one reply. A fresh connection per probe would hide a
/// shard-local stall; one connection throughout keeps every probe on the same
/// shard as the writes that built the backlog.
fn call(s: &mut TcpStream, parts: &[&[u8]]) -> String {
    s.write_all(&encode(parts)).expect("write");
    let mut buf = [0u8; 8192];
    let n = s.read(&mut buf).expect("read");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// FLOAT32 vector whose bytes are all printable ASCII — a blob containing NUL
/// survives a raw socket fine, but keeps the failure output readable.
fn vec_blob(seed: u16) -> Vec<u8> {
    let mut v = Vec::with_capacity(DIM * 4);
    for i in 0..DIM {
        let f = 1.0f32 + f32::from(seed) + i as f32;
        v.extend_from_slice(&f.to_le_bytes());
    }
    v
}

#[test]
fn eh718_a_compaction_backlog_does_not_refuse_its_own_remedy() {
    let dir = std::env::temp_dir().join(format!(
        "moon-eh718-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    std::fs::create_dir_all(&dir).expect("create dir");

    let (mut guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--appendonly",
                "no",
                "--max-unflushed-immutable-segments",
                MAX_UNFLUSHED,
                // The diskfull guard would refuse writes for its OWN reason and
                // this test would then pass without ever building a backlog.
                "--disk-free-min-pct",
                "0",
                "--dir",
            ])
            .arg(&dir)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
            .spawn()
            .expect("spawn moon (run `cargo build --release` first)")
    });

    let mut c = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    c.set_read_timeout(Some(Duration::from_secs(10))).ok();

    let dim = DIM.to_string();
    let created = call(
        &mut c,
        &[
            b"FT.CREATE",
            b"eh718",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"doc:",
            b"SCHEMA",
            b"vec",
            b"VECTOR",
            b"HNSW",
            b"8",
            b"DIM",
            dim.as_bytes(),
            b"TYPE",
            b"FLOAT32",
            b"DISTANCE_METRIC",
            b"L2",
            b"COMPACT_THRESHOLD",
            COMPACT_THRESHOLD.as_bytes(),
        ],
    );
    assert!(created.contains("OK"), "FT.CREATE: {created:?}");

    // Build the backlog. Background compaction does NOT fire on document count
    // alone — 350 documents at COMPACT_THRESHOLD 100 left `graph_segments 0` —
    // so each round compacts explicitly. Three rounds because the stall needs
    // MORE immutable segments than MAX_UNFLUSHED, not merely as many.
    //
    // Using FT.COMPACT to BUILD the fixture is safe on both sides of the fix:
    // the stall cannot engage until two segments exist, which is the end of
    // round 1, and the assertions below do not depend on round 2's compact
    // succeeding.
    let mut doc = 0u16;
    for round in 0..ROUNDS {
        let mut pipelined = Vec::new();
        for _ in 0..DOCS_PER_ROUND {
            let key = format!("doc:{doc}");
            let blob = vec_blob(doc);
            pipelined.extend_from_slice(&encode(&[b"HSET", key.as_bytes(), b"vec", &blob]));
            doc += 1;
        }
        c.write_all(&pipelined).expect("write batch");
        // Drain exactly one reply per HSET: `:0`/`:1` plus CRLF.
        let mut seen = 0usize;
        let mut buf = [0u8; 1 << 16];
        while seen < usize::from(DOCS_PER_ROUND) {
            let n = c.read(&mut buf).expect("drain batch");
            assert!(n > 0, "connection closed while loading round {round}");
            let chunk = String::from_utf8_lossy(&buf[..n]);
            assert!(
                !chunk.contains("MOONERR"),
                "an HSET was refused while building the fixture, so the probes \
                 below would measure a different state: {chunk:?}"
            );
            seen += chunk.matches("\r\n").count();
        }
        let compacted = call(&mut c, &[b"FT.COMPACT", b"eh718"]);
        assert!(
            compacted.contains("OK") || compacted.contains("busy"),
            "round {round} FT.COMPACT: {compacted:?}"
        );
    }

    // The stall bit is set by the 1s MVCC sweep, not by the write itself.
    let deadline = Instant::now() + Duration::from_secs(20);
    let stalled = loop {
        let r = call(&mut c, &[b"SET", b"eh718:probe", b"1"]);
        if r.contains("compaction backlog") {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(200));
    };

    // Control. Without a real stall every assertion below passes vacuously, so
    // failing to REACH the outage is a failed test, not a skipped one.
    assert!(
        stalled,
        "never reached the segment-backlog stall within 20s, so this test would \
         prove nothing. Fixture drift: check that COMPACT_THRESHOLD \
         ({COMPACT_THRESHOLD}) still produces more than \
         --max-unflushed-immutable-segments ({MAX_UNFLUSHED}) immutable \
         segments for {ROUNDS} rounds of {DOCS_PER_ROUND} documents. stderr: {:?}",
        std::fs::read_to_string(dir.join("moon.stderr.log")).unwrap_or_default()
    );

    // --- the bug ---
    let compact = call(&mut c, &[b"FT.COMPACT", b"eh718"]);
    assert!(
        !compact.contains("compaction backlog"),
        "FT.COMPACT is the command that DRAINS the backlog and it was refused \
         BY the backlog. That is the moon#718 outage: no in-band recovery, \
         because the remedy is gated on the condition it cures: {compact:?}"
    );

    let cfg = call(
        &mut c,
        &[
            b"FT.CONFIG",
            b"SET",
            b"eh718",
            b"MERGE_RECALL_TOLERANCE",
            b"0",
        ],
    );
    assert!(
        !cfg.contains("compaction backlog"),
        "FT.CONFIG SET MERGE_RECALL_TOLERANCE is the remedy the rejection log \
         line recommends by name, and the backlog refused it too: {cfg:?}"
    );

    // --- the exemption is not a general bypass ---
    let set = call(&mut c, &[b"SET", b"eh718:after", b"1"]);
    assert!(
        set.contains("compaction backlog"),
        "an ordinary write must STILL be refused — otherwise the fix turned a \
         backpressure guard into a suggestion: {set:?}"
    );
    let hset = call(&mut c, &[b"HSET", b"doc:999", b"vec", &vec_blob(99)]);
    assert!(
        hset.contains("compaction backlog"),
        "indexing another document is what built the backlog; it must still be \
         refused: {hset:?}"
    );

    // Reads were never affected, and must stay that way.
    let info = call(&mut c, &[b"FT.INFO", b"eh718"]);
    assert!(
        !info.contains("MOONERR"),
        "reads must keep working during a stall: {info:?}"
    );

    drop(c);
    guard.kill_now();
    std::fs::remove_dir_all(&dir).ok();
}
