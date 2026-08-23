//! Wave B stage 2a (task #34): MQ effect-record WAL durability + replay,
//! live kill-9 round trip.
//!
//! `tests/crash_recovery_temporal_mq.rs` documents why a live MQ round trip
//! was NOT possible before this change: MQ.* commands are intercepted before
//! the generic AOF-logging dispatch path, so under `--appendonly yes` the
//! AOF-authority recovery (`main.rs`: `db.clear()` on every shard, rebuild
//! solely from the AOF manifest) discarded every durable Stream on every
//! restart, regardless of whether `replay_mq_wal`'s two task #42 defects
//! were fixed. This stage closes that gap: MQ.CREATE/PUSH/POP/ACK/TRIGGER
//! each emit their own versioned WAL v3 effect record at the owner-shard
//! execution site (`src/shard/mq_exec.rs`), and `replay_mq_wal` (which runs
//! AFTER the AOF-authority wipe in `main.rs`, see the ordering assertion in
//! `src/shard/shared_databases.rs::tests::test_replay_mq_wal_survives_prior_db_clear`)
//! rebuilds the full stream content, consumer-group PEL, DLQ routing, and
//! trigger registrations from that log alone.
//!
//! RED (pre-fix) behavior: every assertion below about restored content
//! fails — XLEN/XRANGE/XPENDING/DLQLEN/POP-cursor all read an EMPTY stream
//! after restart (AOF-authority `db.clear()` wiped it and nothing replayed
//! it back), and the trigger notification never fires (TriggerRegistry was
//! never persisted at all pre-fix).
//!
//! Run with (release binary required):
//!   cargo build --release
//!   cargo test --release --test crash_recovery_mq_effects -- --ignored --test-threads=1

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution — honours MOON_BIN, falls back to release then debug.
// Pattern: tests/shardslice_live.rs::find_moon_binary
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

// ---------------------------------------------------------------------------
// Server spawn helpers (pattern: tests/crash_recovery_temporal_mq.rs)
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path, shards: u32, extra: &[&str]) -> (Child, u16) {
    common::spawn_listening(|port| {
        let mut args: Vec<String> = vec![
            "--port".into(),
            port.to_string(),
            "--dir".into(),
            dir.to_string_lossy().into_owned(),
            "--shards".into(),
            shards.to_string(),
        ];
        for &e in extra {
            args.push(e.into());
        }
        Command::new(find_moon_binary())
            .args(&args)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
            .env("RUST_LOG", "moon=info")
            .spawn()
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to spawn moon binary at '{}': {e}. Build with \
                     `cargo build [--release]` or set MOON_BIN.",
                    find_moon_binary().display()
                )
            })
    })
}

/// RAII guard: SIGKILLs the server process when dropped (`Child::kill` is
/// SIGKILL on Unix — no graceful shutdown, matching the "kill -9" scenario).
struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("parse addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(10))).ok();
                s.set_write_timeout(Some(Duration::from_secs(10))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on port {port}: {e}"),
        }
    }
}

fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf)
            && n > 0
            && buf[..n].windows(4).any(|w| w == b"PONG")
        {
            return s;
        }
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "server accepted TCP but never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP2 client (self-contained; copied from tests/shardslice_live.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
}

impl Resp {
    fn flat(&self) -> String {
        match self {
            Resp::Simple(s) | Resp::Error(s) => s.clone(),
            Resp::Int(i) => i.to_string(),
            Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Resp::Bulk(None) => "<nil>".into(),
            Resp::Array(Some(items)) => items.iter().map(Resp::flat).collect::<Vec<_>>().join(" "),
            Resp::Array(None) => "<nil-array>".into(),
        }
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(s: TcpStream) -> Self {
        Conn {
            s,
            buf: Vec::with_capacity(16 * 1024),
            pos: 0,
        }
    }

    fn open(port: u16) -> Self {
        Conn::new(connect(port, Duration::from_secs(10)))
    }

    fn cmd(&mut self, parts: &[&[u8]]) -> Resp {
        let mut req = Vec::with_capacity(128);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn cmd_s(&mut self, parts: &[&str]) -> Resp {
        let v: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.cmd(&v)
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read from server");
        assert!(n > 0, "connection closed mid-frame");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return line;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        out
    }

    /// Read one RESP frame. Used both for command replies and for
    /// unsolicited pub/sub pushes on a SUBSCRIBEd connection.
    fn frame(&mut self) -> Resp {
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().unwrap_or(0)),
            "$" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    Resp::Bulk(Some(self.exact(n as usize)))
                }
            }
            "*" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(self.frame());
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

fn as_array(r: &Resp) -> &[Resp] {
    match r {
        Resp::Array(Some(items)) => items,
        other => panic!("expected array, got {other:?}"),
    }
}

fn as_int(r: &Resp) -> i64 {
    match r {
        Resp::Int(i) => *i,
        other => panic!("expected integer, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// MQ effect-record durability: full lifecycle survives kill -9 under
// `--appendonly yes` (the AOF-authority path that previously discarded
// every durable Stream unconditionally, see the module doc comment).
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn mq_effect_records_survive_kill9() {
    let dir = tempfile::tempdir().expect("tempdir");
    let extra: &[&str] = &["--appendonly", "yes", "--disk-free-min-pct", "0"];

    // --- Round 1: build up durable MQ state across 3 queues, kill -9. ---
    {
        let (child, port) = spawn_moon(dir.path(), 1, extra);
        let _guard = ServerGuard(child);
        drop(wait_ready(port));

        let mut c = Conn::open(port);

        // `ordersq`: MAXDELIVERY 0 disables DLQ routing entirely (the
        // `mdc > 0` guard in `handle_pop`) AND keeps `request_count == count`
        // (no over-claim padding — `request_count = count + mdc`), so POP
        // claims EXACTLY the requested count with no side effects on
        // messages beyond it. Exercises PUSH content, POP cursor, and
        // ACK-by-id (subset acked, rest stays pending in the PEL).
        assert_eq!(
            c.cmd_s(&["MQ", "CREATE", "ordersq", "MAXDELIVERY", "0"]),
            Resp::Simple("OK".into())
        );
        for i in 1..=5 {
            let seq = i.to_string();
            let reply = c.cmd_s(&["MQ", "PUSH", "ordersq", "seq", &seq]);
            match reply {
                Resp::Bulk(Some(_)) => {}
                other => panic!("MQ PUSH must return the assigned id: {other:?}"),
            }
        }
        // Claim the first 3 (seq 1..3); last_delivered_id cursor should land
        // on msg 3's id.
        let popped = c.cmd_s(&["MQ", "POP", "ordersq", "COUNT", "3"]);
        let popped_items = as_array(&popped);
        assert_eq!(popped_items.len(), 3, "must claim exactly 3 messages");
        let claimed_ids: Vec<String> = popped_items
            .iter()
            .map(|entry| as_array(entry)[0].flat())
            .collect();
        let claimed_seq_fields: Vec<String> = popped_items
            .iter()
            .map(|entry| as_array(&as_array(entry)[1])[1].flat())
            .collect();
        assert_eq!(
            claimed_seq_fields,
            vec!["1", "2", "3"],
            "claim order must match push order (proves field content, not just count)"
        );

        // ACK the first two, leaving the third pending in the PEL.
        let acked = c.cmd_s(&["MQ", "ACK", "ordersq", &claimed_ids[0], &claimed_ids[1]]);
        assert_eq!(as_int(&acked), 2, "must ack exactly 2 messages");

        // `dlqtestq`: MAXDELIVERY 1 forces immediate DLQ routing on first
        // claim (read_group_new always claims with delivery_count=1).
        assert_eq!(
            c.cmd_s(&["MQ", "CREATE", "dlqtestq", "MAXDELIVERY", "1"]),
            Resp::Simple("OK".into())
        );
        c.cmd_s(&["MQ", "PUSH", "dlqtestq", "f", "a"]);
        c.cmd_s(&["MQ", "PUSH", "dlqtestq", "f", "b"]);
        let dlq_popped = c.cmd_s(&["MQ", "POP", "dlqtestq", "COUNT", "2"]);
        // Both entries route straight to the DLQ (delivery_count 1 >= mdc 1),
        // so the POP reply itself is empty.
        assert_eq!(as_array(&dlq_popped).len(), 0);
        assert_eq!(as_int(&c.cmd_s(&["MQ", "DLQLEN", "dlqtestq"])), 2);

        // `trigq`: register a debounced trigger. Not yet armed (no PUSH),
        // so `pending_fire_ms` is 0 at crash time — restoring the
        // registration itself (queue_key/callback/debounce) is what's
        // under test, not an in-flight timer.
        assert_eq!(
            c.cmd_s(&["MQ", "CREATE", "trigq"]),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            c.cmd_s(&["MQ", "TRIGGER", "trigq", "FIRED", "DEBOUNCE", "200"]),
            Resp::Simple("OK".into())
        );

        // Let the 1ms WAL flush tick drain every MQ effect record before the
        // kill -- mirrors tests/crash_recovery_temporal_mq.rs's settle wait.
        std::thread::sleep(Duration::from_millis(1500));
        // _guard dropped here -> SIGKILL
    }

    // --- Round 2: restart on the SAME dir. Assert every plane restored. ---
    {
        let (child2, port2) = spawn_moon(dir.path(), 1, extra);
        let _guard2 = ServerGuard(child2);
        drop(wait_ready(port2));

        let mut c = Conn::open(port2);

        // (1) Stream CONTENT: all 5 pushed messages must still exist with
        // their original field values, in id order -- proves MqPush replay
        // rebuilt `entries` from the WAL after the AOF-authority db.clear().
        let xlen = c.cmd_s(&["XLEN", "ordersq"]);
        assert_eq!(
            as_int(&xlen),
            5,
            "RED: all 5 MQ.PUSH'd messages must survive kill-9 (AOF-authority \
             db.clear() wipes the keyspace; only a correct MqPush WAL replay \
             rebuilds it)"
        );
        let range = c.cmd_s(&["XRANGE", "ordersq", "-", "+"]);
        let entries = as_array(&range);
        assert_eq!(entries.len(), 5);
        let seqs: Vec<String> = entries
            .iter()
            .map(|e| {
                let fields = as_array(&as_array(e)[1]);
                fields[1].flat() // [field, value] pairs; "seq" is the only field
            })
            .collect();
        assert_eq!(
            seqs,
            vec!["1", "2", "3", "4", "5"],
            "field VALUES must survive verbatim, in original id order"
        );

        // (2) ACK-subset restored: msg 3 (never acked) must be the ONLY
        // entry left pending after Round 1 -- proves MqAck replay applied
        // by ID (removing exactly msg 1 & msg 2 from the PEL), not by a
        // count-based heuristic. MUST run BEFORE the cursor-resume POP below
        // -- MQ.POP is a claiming read (it inserts every newly-claimed id
        // into the PEL), so checking XPENDING after it would conflate "ack
        // replay worked" with "the live POP below also added msg 4 + msg 5
        // to the pending set", making 3 the RIGHT answer for the wrong
        // reason (or masking a real ack-replay regression entirely).
        let pending = c.cmd_s(&["XPENDING", "ordersq", "__mq_consumers"]);
        let pending_fields = as_array(&pending);
        assert_eq!(
            as_int(&pending_fields[0]),
            1,
            "RED: exactly 1 message (msg 3) must still be pending after \
             restart -- ack-by-count would roll back ALL 3 claimed messages \
             to pending instead of respecting the 2 that were actually acked"
        );

        // (3) Cursor position: POP after restart must resume at msg 4, NOT
        // re-deliver msg 1..3 -- proves the consumer group's
        // `last_delivered_id` was restored from the MqPop effect record.
        let resumed = c.cmd_s(&["MQ", "POP", "ordersq", "COUNT", "10"]);
        let resumed_items = as_array(&resumed);
        assert_eq!(
            resumed_items.len(),
            2,
            "RED: cursor must resume after msg 3 (2 remaining: msg 4, msg 5) \
             -- a lost/rolled-back cursor would either re-deliver msg 1..3 \
             (count > 2) or (if the stream itself was lost) return 0"
        );
        let resumed_seqs: Vec<String> = resumed_items
            .iter()
            .map(|entry| {
                let fields = as_array(&as_array(entry)[1]);
                fields[1].flat()
            })
            .collect();
        assert_eq!(resumed_seqs, vec!["4", "5"]);

        // (4) DLQ routing restored: both dlqtestq entries must still be in
        // the dead-letter stream -- proves MqPop's DLQ routing decision
        // (source id -> assigned DLQ id) replayed deterministically.
        assert_eq!(
            as_int(&c.cmd_s(&["MQ", "DLQLEN", "dlqtestq"])),
            2,
            "RED: DLQ-routed entries must survive restart"
        );

        // (5) Trigger registration restored: subscribe to the trigger's
        // notification channel, arm it with a fresh PUSH, and wait for the
        // debounce window to elapse. If the TriggerRegistry entry did not
        // survive, PUSH finds no registered trigger for `trigq` and nothing
        // is ever published on `mq:trigger:trigq`.
        let mut sub = Conn::open(port2);
        let sub_reply = sub.cmd_s(&["SUBSCRIBE", "mq:trigger:trigq"]);
        assert_eq!(as_array(&sub_reply)[0].flat(), "subscribe");

        c.cmd_s(&["MQ", "PUSH", "trigq", "f", "v"]);

        // Debounce window is 200ms; allow generous slack for a loaded CI box.
        sub.s
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("set read timeout");
        let push_frame = sub.frame();
        let push_items = as_array(&push_frame);
        assert_eq!(
            push_items[0].flat(),
            "message",
            "RED: trigger registration must survive restart -- without it, \
             MQ.PUSH never re-arms a pending fire and no notification is \
             ever published on mq:trigger:trigq (got {push_frame:?})"
        );
        assert_eq!(push_items[1].flat(), "mq:trigger:trigq");
        assert_eq!(
            push_items[2].flat(),
            "FIRED",
            "callback_cmd payload must survive verbatim"
        );
    }
}

// ---------------------------------------------------------------------------
// task #47: MqPop WAL v2 carries the ORIGINAL PEL delivery_time/seen_time,
// so a kill-9 restart reports REAL XPENDING idle metadata instead of
// resetting the PEL entry's clock (pre-task-#47: apply_mq_pop hardcoded
// `delivery_time: 0` / `seen_time: 0`, which — since XPENDING computes
// `idle = now - delivery_time` — reported "idle since the Unix epoch"
// instead of "idle since it was actually claimed").
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Requires built release binary; run explicitly.
fn xpending_idle_metadata_survives_kill9() {
    let dir = tempfile::tempdir().expect("tempdir");
    let extra: &[&str] = &["--appendonly", "yes", "--disk-free-min-pct", "0"];

    // How long the claimed message sits pending BEFORE the kill -- this is
    // the real idle time a correct restart must approximately reproduce.
    // Generous enough to dwarf process-restart + WAL-flush jitter.
    let pre_kill_idle_floor_ms: i64 = 1200;

    {
        let (child, port) = spawn_moon(dir.path(), 1, extra);
        let _guard = ServerGuard(child);
        drop(wait_ready(port));

        let mut c = Conn::open(port);
        assert_eq!(
            c.cmd_s(&["MQ", "CREATE", "idleq", "MAXDELIVERY", "0"]),
            Resp::Simple("OK".into())
        );
        c.cmd_s(&["MQ", "PUSH", "idleq", "f", "v"]);
        // Claim it -- this is the delivery_time/seen_time that must survive.
        let popped = c.cmd_s(&["MQ", "POP", "idleq", "COUNT", "1"]);
        assert_eq!(as_array(&popped).len(), 1, "must claim exactly 1 message");

        // Let real wall-clock idle time accumulate BEFORE the kill, then let
        // the 1ms WAL flush tick drain the MqPop effect record.
        std::thread::sleep(Duration::from_millis(pre_kill_idle_floor_ms as u64));
        std::thread::sleep(Duration::from_millis(500));
        // _guard dropped here -> SIGKILL
    }

    {
        let (child2, port2) = spawn_moon(dir.path(), 1, extra);
        let _guard2 = ServerGuard(child2);
        drop(wait_ready(port2));

        let mut c = Conn::open(port2);

        // Extended form: XPENDING key group - + count -> per-entry
        // [id, consumer, idle_ms, delivery_count].
        let pending = c.cmd_s(&["XPENDING", "idleq", "__mq_consumers", "-", "+", "10"]);
        let entries = as_array(&pending);
        assert_eq!(
            entries.len(),
            1,
            "the one claimed message must still be pending"
        );
        let entry_fields = as_array(&entries[0]);
        let idle_ms = as_int(&entry_fields[2]);
        let delivery_count = as_int(&entry_fields[3]);

        assert_eq!(delivery_count, 1, "delivery_count must survive verbatim");

        // RED (pre-task-#47): delivery_time was hardcoded to 0 on replay,
        // so `idle = now - 0` reports a HUGE idle time (milliseconds since
        // the Unix epoch, i.e. tens of trillions of ms) -- not zero, and
        // not close to `pre_kill_idle_floor_ms` either. GREEN: idle_ms must
        // be in the same order of magnitude as the real elapsed wall time
        // (>= the floor established before the kill, and bounded well below
        // an epoch-relative reading).
        assert!(
            idle_ms >= pre_kill_idle_floor_ms,
            "RED: idle_ms ({idle_ms}) must be >= the real pre-kill idle floor \
             ({pre_kill_idle_floor_ms}ms) -- a lost delivery_time would show \
             an idle time close to 0 (reset-to-now) or astronomically large \
             (reset-to-0/epoch), never a plausible elapsed duration"
        );
        assert!(
            idle_ms < 60_000,
            "RED: idle_ms ({idle_ms}) must be a plausible small elapsed \
             duration, not an epoch-relative reading (delivery_time reset to \
             0 would make `now - 0` on the order of 10^12 ms)"
        );
    }
}

/// moon#652 durability leg: entries a POP claimed-then-released must come back
/// from the WAL as UNCLAIMED, not as pending-forever.
///
/// `handle_pop` over-claims by `max_delivery_count` and releases the surplus
/// (un-claims it from the PEL and rewinds the group cursor). Replay rebuilds
/// the PEL from the `MqPop` record's claimed-id list and restores its
/// `last_delivered_id`, so if the record still carried the released ids — or
/// the cursor from before the rewind — a restart would silently re-strand
/// exactly the messages the fix rescued. The in-memory fix and the WAL record
/// have to agree, and only a kill -9 round trip proves they do.
#[test]
#[ignore] // Requires built release binary; run explicitly.
fn popped_surplus_is_released_across_kill9() {
    let dir = tempfile::tempdir().expect("tempdir");
    let extra: &[&str] = &["--appendonly", "yes", "--disk-free-min-pct", "0"];

    let mut pushed: Vec<String> = Vec::new();

    {
        let (child, port) = spawn_moon(dir.path(), 1, extra);
        let _guard = ServerGuard(child);
        drop(wait_ready(port));

        let mut c = Conn::open(port);
        // Default MAXDELIVERY (3): POP COUNT 1 asks read_group_new for 4.
        assert_eq!(
            c.cmd_s(&["MQ", "CREATE", "surq"]),
            Resp::Simple("OK".into())
        );
        for i in 1..=4 {
            pushed.push(
                c.cmd_s(&["MQ", "PUSH", "surq", "f", &format!("m{i}")])
                    .flat(),
            );
        }

        let popped = c.cmd_s(&["MQ", "POP", "surq", "COUNT", "1"]);
        let ids = as_array(&popped);
        assert_eq!(ids.len(), 1, "POP COUNT 1 returns 1");

        // Exactly the delivered message is pending; the other three were
        // claimed by read_group_new and must have been released again.
        let pending = c.cmd_s(&["XPENDING", "surq", "__mq_consumers", "-", "+", "10"]);
        assert_eq!(
            as_array(&pending).len(),
            1,
            "only the delivered message may be pending pre-restart"
        );

        // Let the 1ms WAL flush tick drain every MQ effect record before the
        // kill -- same settle wait as `mq_effect_records_survive_kill9`.
        std::thread::sleep(Duration::from_millis(1500));
        // _guard dropped here -> SIGKILL
    }

    {
        let (child2, port2) = spawn_moon(dir.path(), 1, extra);
        let _guard2 = ServerGuard(child2);
        drop(wait_ready(port2));

        let mut c = Conn::open(port2);

        assert_eq!(
            as_int(&c.cmd_s(&["XLEN", "surq"])),
            4,
            "sanity: all 4 pushes must have replayed before judging the PEL"
        );

        let pending = c.cmd_s(&["XPENDING", "surq", "__mq_consumers", "-", "+", "10"]);
        assert_eq!(
            as_array(&pending).len(),
            1,
            "replay must restore exactly the one delivered message as pending \
             -- rebuilding the PEL from released ids re-strands them"
        );

        // The three released messages must still be claimable after recovery.
        let rest = c.cmd_s(&["MQ", "POP", "surq", "COUNT", "10"]);
        let rest_ids: Vec<String> = as_array(&rest)
            .iter()
            .map(|entry| as_array(entry)[0].flat())
            .collect();
        assert_eq!(
            rest_ids,
            pushed[1..].to_vec(),
            "the released surplus must be re-claimable after recovery -- a \
             last_delivered_id restored from before the rewind advances the \
             cursor past them and they are unreachable forever"
        );
    }
}
