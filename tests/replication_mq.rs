//! Wave B stage 2b: MQ-plane replication. A replica must synchronize durable
//! queue / trigger-registry state from its master — both the live MQ.*
//! mutation stream and the PSYNC full-resync snapshot backfill.
//!
//! Single-shard master scope (matches graph's own R0/R0.5 live-stream
//! precedent — see `replication_graph.rs`). MQ effect records are streamed as
//! synthetic `MQ._REPL.*` pseudo-commands carrying the SAME encoded payload
//! bytes `shard::mq_exec::wal_append_on_slice` already builds for the WAL, so
//! the replica's `apply_mq` reuses the identical `decode_mq_*` /
//! `apply_mq_*` engine boot-time WAL replay uses (`shard::mq_exec::
//! replicate_mq_record`, `replication::apply::apply_mq`). A multi-shard
//! master does not live-stream MQ writes yet (the same `num_shards == 1` gate
//! graph's own live path uses) — `multi_shard_live_mq_write_not_streamed`
//! pins that limitation down explicitly rather than leaving it untested.
//!
//! Run: `MOON_BIN=./target/release/moon cargo test --test replication_mq -- --ignored`

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

fn start_moon(port: u16, dir: &str, shards: usize) -> Child {
    start_moon_logged(port, dir, shards, None)
}

/// Like `start_moon`, optionally teeing stderr (tracing output) to a file so
/// tests can assert stream-health properties — e.g. that the replica held ONE
/// live connection instead of masking a dead stream behind a reconnect/
/// full-resync loop (`replication_graph.rs` precedent).
fn start_moon_logged(
    port: u16,
    dir: &str,
    shards: usize,
    stderr_log: Option<&std::path::Path>,
) -> Child {
    let stderr = match stderr_log {
        Some(p) => Stdio::from(std::fs::File::create(p).expect("create log file")),
        None => Stdio::null(),
    };
    Command::new(moon_bin())
        .env("RUST_LOG", "moon=warn")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--dir",
            dir,
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

/// Kill-on-drop guard so a panicking assertion never leaks a live server.
struct Killer(Child);
impl Drop for Killer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Send one inline command, return the raw reply text.
fn send_cmd(addr: &str, cmd: &str) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    if stream.write_all(format!("{cmd}\r\n").as_bytes()).is_err() {
        return String::new();
    }
    read_quiet(&mut stream)
}

/// RESP array of bulk strings — binary-safe, for multiword MQ commands.
fn send_resp(addr: &str, parts: &[&str]) -> String {
    let Ok(mut stream) = TcpStream::connect(addr) else {
        return String::new();
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .ok();
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    if stream.write_all(&out).is_err() {
        return String::new();
    }
    read_quiet(&mut stream)
}

fn read_quiet(stream: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_millis(600);
    while Instant::now() < deadline {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => {
                if !buf.is_empty() {
                    break;
                }
            }
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// Parse the payload out of a raw RESP bulk-string reply (`$<len>\r\n<data>\r\n`).
/// `MQ PUSH` returns the assigned stream id as a `Frame::BulkString` (NOT an
/// Integer/SimpleString), so callers that need the actual id value (e.g. to
/// pass to a later `MQ ACK`) must parse it out rather than trim a `:` prefix.
fn parse_bulk_string(raw: &str) -> Option<String> {
    let mut lines = raw.split("\r\n");
    let first = lines.next()?;
    let len: i64 = first.strip_prefix('$')?.parse().ok()?;
    if len < 0 {
        return None; // $-1 nil
    }
    Some(lines.next()?.to_string())
}

fn wait_until<F: Fn() -> bool>(timeout: Duration, f: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    f()
}

/// REPL-MQ-01: live MQ.* mutations on the master reach the replica. The
/// replica attaches to an EMPTY master, then MQ.CREATE / MQ.PUSH / MQ.ACK
/// must all show up — isolating the live streaming leg from snapshot
/// backfill.
#[test]
#[ignore]
fn replica_syncs_mq_live_stream() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16740";
    let replica_addr = "127.0.0.1:16741";

    let replica_log = replica_dir.path().join("replica-stderr.log");

    let _master = Killer(start_moon(16740, master_dir.path().to_str().unwrap(), 1));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon_logged(
        16741,
        replica_dir.path().to_str().unwrap(),
        1,
        Some(&replica_log),
    ));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16740").starts_with("+OK"),
        "REPLICAOF failed"
    );
    // Let the handshake settle before the first write.
    std::thread::sleep(Duration::from_millis(500));

    // 1. MQ.CREATE + MQ.PUSH stream to the replica.
    assert!(
        send_cmd(master_addr, "MQ CREATE mq1 MAXDELIVERY 3").starts_with("+OK"),
        "MQ CREATE failed"
    );
    let push = send_resp(master_addr, &["MQ", "PUSH", "mq1", "f1", "v1"]);
    assert!(
        parse_bulk_string(&push).is_some_and(|id| id.contains('-')),
        "MQ PUSH must return a stream id: {push}"
    );

    // 2. Read-only content check: `MQ POP` is a WRITE (claims PEL entries)
    // and readonly enforcement (PR #292) correctly rejects it on a replica,
    // so replica-side observation uses plain stream READS instead — XRANGE
    // shows the pushed fields once MQ.CREATE + MQ.PUSH applied.
    let seen = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &["XRANGE", "mq1", "-", "+"]).contains("v1")
    });
    assert!(
        seen,
        "replica never saw the streamed MQ.PUSH message: {}",
        send_resp(replica_addr, &["XRANGE", "mq1", "-", "+"])
    );

    // 3. MqPop replication: the MASTER pops (claiming the entry into the
    // consumer-group PEL and advancing last_delivered) — the replica's PEL,
    // read via the XPENDING summary form, must converge to 1 pending.
    let pop = send_resp(master_addr, &["MQ", "POP", "mq1", "COUNT", "1"]);
    assert!(pop.contains("v1"), "master MQ POP failed: {pop}");
    let pel_grew = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &["XPENDING", "mq1", "__mq_consumers"]).starts_with("*4\r\n:1")
    });
    assert!(
        pel_grew,
        "replica PEL never showed the master's claim: {}",
        send_resp(replica_addr, &["XPENDING", "mq1", "__mq_consumers"])
    );

    // 4. MqAck replication: acking the claimed id on the master must drain
    // the replica's PEL back to 0.
    let id1 = parse_bulk_string(&push).unwrap_or_default();
    let ack = send_resp(master_addr, &["MQ", "ACK", "mq1", &id1]);
    assert!(ack.starts_with(':'), "MQ ACK failed: {ack}");
    let pel_drained = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &["XPENDING", "mq1", "__mq_consumers"]).starts_with("*4\r\n:0")
    });
    assert!(
        pel_drained,
        "replica MQ.ACK never applied — PEL still shows the acked entry: {}",
        send_resp(replica_addr, &["XPENDING", "mq1", "__mq_consumers"])
    );

    // 5. A second push keeps streaming past the pop/ack cycle.
    let push2 = send_resp(master_addr, &["MQ", "PUSH", "mq1", "f2", "v2"]);
    assert!(
        parse_bulk_string(&push2).is_some_and(|id| id.contains('-')),
        "MQ PUSH #2 must return a stream id: {push2}"
    );
    let seen2 = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &["XRANGE", "mq1", "-", "+"]).contains("v2")
    });
    assert!(seen2, "second MQ.PUSH never reached the replica");

    // 6. STREAM-HEALTH: everything above must have arrived on ONE live
    // stream (no reconnect/full-resync loop masking a dead live stream —
    // `replication_graph.rs`'s REPL-GRAPH-01 precedent).
    let log = std::fs::read_to_string(&replica_log).unwrap_or_default();
    let reconnects = log.matches("reconnecting").count();
    assert_eq!(
        reconnects, 0,
        "replica reconnected {reconnects}x — live stream did not hold:\n{log}"
    );

    // 7. FAILOVER proof: promote the replica — a consumer must be able to
    // POP the un-consumed message (v2) from it. This is the operational
    // point of the whole plane: the replicated durable-flag, consumer-group
    // state, and last_delivered cursor (advanced past v1 by the master's
    // replicated pop) all have to be correct for the claim to work.
    assert!(
        send_cmd(replica_addr, "REPLICAOF NO ONE").starts_with("+OK"),
        "promotion failed"
    );
    let promoted_pop = wait_until(Duration::from_secs(5), || {
        send_resp(replica_addr, &["MQ", "POP", "mq1", "COUNT", "1"]).contains("v2")
    });
    assert!(
        promoted_pop,
        "promoted replica could not POP the unconsumed message: {}",
        send_resp(replica_addr, &["MQ", "POP", "mq1", "COUNT", "1"])
    );
}

/// REPL-MQ-02: a replica attaching to a master that ALREADY has MQ state
/// (durable queue + pushed messages) must backfill it from the PSYNC
/// full-resync snapshot — both the Stream CONTENT (RDB `RDB_TYPE_STREAM_MOON`
/// fix landed alongside this stage) and the shard-level durable-queue
/// registry (`MOON_AUX_MQ_REGISTRY` blob, `replication::mq_sync`).
#[test]
#[ignore]
fn replica_backfills_mq_from_snapshot() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16742";
    let replica_addr = "127.0.0.1:16743";

    let _master = Killer(start_moon(16742, master_dir.path().to_str().unwrap(), 1));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );

    // Build MQ state on the master BEFORE any replica exists.
    assert!(
        send_cmd(master_addr, "MQ CREATE mqsnap MAXDELIVERY 5").starts_with("+OK"),
        "seed MQ CREATE failed"
    );
    for (f, v) in [("f1", "v1"), ("f2", "v2"), ("f3", "v3")] {
        let r = send_resp(master_addr, &["MQ", "PUSH", "mqsnap", f, v]);
        assert!(
            parse_bulk_string(&r).is_some_and(|id| id.contains('-')),
            "seed MQ PUSH {f} failed: {r}"
        );
    }

    // Now attach a fresh replica — it must learn mqsnap's durable-queue
    // registration AND its 3 pushed messages purely from the snapshot.
    let _replica = Killer(start_moon(16743, replica_dir.path().to_str().unwrap(), 1));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16742").starts_with("+OK"),
        "REPLICAOF failed"
    );

    // Read-only observation (MQ POP is a write and is READONLY-rejected on
    // a replica, per PR #292): the snapshot's stream content must appear
    // via XRANGE.
    let backfilled = wait_until(Duration::from_secs(10), || {
        let range = send_resp(replica_addr, &["XRANGE", "mqsnap", "-", "+"]);
        range.contains("v1") && range.contains("v2") && range.contains("v3")
    });
    assert!(
        backfilled,
        "replica never backfilled the 3 pre-existing snapshot messages: {}",
        send_resp(replica_addr, &["XRANGE", "mqsnap", "-", "+"])
    );

    // A live write AFTER the snapshot must also land (stream continues past
    // the backfill boundary, same as REPL-GRAPH-02).
    let r = send_resp(master_addr, &["MQ", "PUSH", "mqsnap", "f4", "v4"]);
    assert!(
        parse_bulk_string(&r).is_some_and(|id| id.contains('-')),
        "post-snapshot MQ PUSH failed: {r}"
    );
    let grew = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &["XRANGE", "mqsnap", "-", "+"]).contains("v4")
    });
    assert!(grew, "post-snapshot live MQ.PUSH did not replicate");

    // FAILOVER proof: a promoted replica must serve real consumers — POP
    // has to claim all 4 messages, which exercises the snapshot-carried
    // `durable` flag + consumer group (RDB_TYPE_STREAM_MOON codec) and the
    // MQ registry aux blob end-to-end.
    assert!(
        send_cmd(replica_addr, "REPLICAOF NO ONE").starts_with("+OK"),
        "promotion failed"
    );
    let promoted_pop = wait_until(Duration::from_secs(5), || {
        let popped = send_resp(replica_addr, &["MQ", "POP", "mqsnap", "COUNT", "4"]);
        popped.contains("v1")
            && popped.contains("v2")
            && popped.contains("v3")
            && popped.contains("v4")
    });
    assert!(
        promoted_pop,
        "promoted replica could not POP the backfilled messages: {}",
        send_resp(replica_addr, &["XRANGE", "mqsnap", "-", "+"])
    );
}

/// REPL-MQ-03 (documented scope limitation): a multi-shard master does not
/// live-stream MQ.* writes yet — `shard::mq_exec::replicate_mq_record` gates
/// on `num_shards == 1`, the same posture graph's own live path takes for
/// its multi-shard gap (durability is unaffected: the write is still
/// WAL-durable and FULLRESYNC-covered, it just doesn't arrive incrementally).
/// This test pins the gap down explicitly rather than leaving it silently
/// untested, so a future multi-shard MQ live-stream fix has a RED test to
/// flip to green instead of discovering the gap by accident.
#[test]
#[ignore]
fn multi_shard_live_mq_write_not_streamed() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16744";
    let replica_addr = "127.0.0.1:16745";

    let _master = Killer(start_moon(16744, master_dir.path().to_str().unwrap(), 2));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16745, replica_dir.path().to_str().unwrap(), 1));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16744").starts_with("+OK"),
        "REPLICAOF failed"
    );
    std::thread::sleep(Duration::from_millis(500));

    assert!(
        send_cmd(master_addr, "MQ CREATE mqms MAXDELIVERY 3").starts_with("+OK"),
        "MQ CREATE failed"
    );
    let push = send_resp(master_addr, &["MQ", "PUSH", "mqms", "f1", "v1"]);
    assert!(
        parse_bulk_string(&push).is_some_and(|id| id.contains('-')),
        "MQ PUSH failed: {push}"
    );

    // Short bounded wait (not the full 10s convergence window used by the
    // positive tests) — a live-stream regression would show up within a
    // couple of poll intervals; this is a negative assertion, not a
    // performance timing test.
    let never_arrived = !wait_until(Duration::from_secs(3), || {
        send_resp(replica_addr, &["MQ", "POP", "mqms", "COUNT", "1"]).contains("v1")
    });
    assert!(
        never_arrived,
        "multi-shard MQ live write unexpectedly replicated — if this now \
         passes, the num_shards==1 gate in `replicate_mq_record` was lifted; \
         update this test's doc comment and promote it out of the \
         known-limitation suite"
    );
}

/// Extract every `:`-prefixed integer reply line from a raw RESP blob, in
/// order. XPENDING's extended form (`XPENDING key group - + count`) is the
/// only integer-bearing element in a claimed entry (id + consumer are bulk
/// strings) — `[idle_ms, delivery_count]` for a single-entry reply.
fn extract_ints(raw: &str) -> Vec<i64> {
    raw.split("\r\n")
        .filter_map(|line| line.strip_prefix(':'))
        .filter_map(|n| n.parse::<i64>().ok())
        .collect()
}

/// task #47: MqPop WAL v2 replicates the ORIGINAL `delivery_time`/
/// `seen_time` (not a replica-local "just applied" timestamp), so a
/// promoted replica's `XPENDING` idle time reflects when the message was
/// REALLY claimed on the master — matching what `XPENDING` would report on
/// the master itself. Pre-task-#47 the replica applied `apply_mq_pop` with
/// `delivery_time: 0` regardless of payload content, which (since
/// `idle = now - delivery_time`) would report an idle time on the order of
/// "milliseconds since the Unix epoch" instead of the real elapsed wall
/// time since the claim.
#[test]
#[ignore]
fn promoted_replica_reports_real_pending_idle_time() {
    let master_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    let master_addr = "127.0.0.1:16746";
    let replica_addr = "127.0.0.1:16747";

    let _master = Killer(start_moon(16746, master_dir.path().to_str().unwrap(), 1));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(master_addr, "PING")
            .starts_with("+PONG")),
        "master never became ready"
    );
    let _replica = Killer(start_moon(16747, replica_dir.path().to_str().unwrap(), 1));
    assert!(
        wait_until(Duration::from_secs(5), || send_cmd(replica_addr, "PING")
            .starts_with("+PONG")),
        "replica never became ready"
    );
    assert!(
        send_cmd(replica_addr, "REPLICAOF 127.0.0.1 16746").starts_with("+OK"),
        "REPLICAOF failed"
    );
    std::thread::sleep(Duration::from_millis(500));

    assert!(
        send_cmd(master_addr, "MQ CREATE mqidle MAXDELIVERY 0").starts_with("+OK"),
        "MQ CREATE failed"
    );
    let push = send_resp(master_addr, &["MQ", "PUSH", "mqidle", "f", "v"]);
    assert!(
        parse_bulk_string(&push).is_some_and(|id| id.contains('-')),
        "MQ PUSH failed: {push}"
    );

    // Master claims the message NOW -- this is the delivery_time that must
    // survive the replication hop unchanged.
    let pop = send_resp(master_addr, &["MQ", "POP", "mqidle", "COUNT", "1"]);
    assert!(pop.contains("v"), "master MQ POP failed: {pop}");

    // Wait for the replica's PEL to converge to 1 pending BEFORE the real
    // idle window elapses, so the accumulated idle time below is measured
    // on the (already-applied) replica entry, not on catch-up latency.
    let pel_grew = wait_until(Duration::from_secs(10), || {
        send_resp(replica_addr, &["XPENDING", "mqidle", "__mq_consumers"]).starts_with("*4\r\n:1")
    });
    assert!(pel_grew, "replica PEL never showed the master's claim");

    // Accumulate real wall-clock idle time BEFORE promoting/reading.
    let idle_floor_ms: i64 = 1200;
    std::thread::sleep(Duration::from_millis(idle_floor_ms as u64));

    assert!(
        send_cmd(replica_addr, "REPLICAOF NO ONE").starts_with("+OK"),
        "promotion failed"
    );

    let extended = send_resp(
        replica_addr,
        &["XPENDING", "mqidle", "__mq_consumers", "-", "+", "10"],
    );
    let ints = extract_ints(&extended);
    assert_eq!(
        ints.len(),
        2,
        "expected exactly [idle_ms, delivery_count] from a single-entry \
         extended XPENDING reply: {extended}"
    );
    let idle_ms = ints[0];
    let delivery_count = ints[1];

    assert_eq!(delivery_count, 1, "delivery_count must replicate verbatim");
    assert!(
        idle_ms >= idle_floor_ms,
        "RED: promoted replica's idle_ms ({idle_ms}) must be >= the real \
         idle floor ({idle_floor_ms}ms) accumulated since the master's \
         claim -- a replica that used its OWN apply-time instead of the \
         replicated delivery_time would report an idle time close to the \
         (much shorter) time since promotion instead"
    );
    assert!(
        idle_ms < 60_000,
        "RED: promoted replica's idle_ms ({idle_ms}) must be a plausible \
         small elapsed duration, not an epoch-relative reading"
    );
}
