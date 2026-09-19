//! CLIENT TRACKING / pub-sub follow-ups to moon#1048/#1049.
//!
//! Every expected byte string was captured from redis-server 8.6.1 over a raw
//! socket; the wire is compared, not a decoded value.
//!
//! | issue | case                                             | redis 8.6.1                          | moon before the fix              |
//! |-------|--------------------------------------------------|--------------------------------------|----------------------------------|
//! | #1088 | 400-key MSET, RESP3 tracker                      | 400/400 pushes, connection open      | 256/400, open                    |
//! | #1088 | 400-key MSET, RESP2 REDIRECT target              | 400/400 messages                     | 256/400                          |
//! | #1088 | 400 pipelined SETs, RESP3 tracker                | 400/400 pushes                       | 256/400                          |
//! | #1088 | 400 pipelined SETs, RESP2 REDIRECT target        | 400/400 messages                     | 256/400, target still connected  |
//! | #1088 | same, output-buffer limit 8 KiB                  | connection closed                    | 256 pushes, open                 |
//! | #1088 | tracker stops reading, limit 8 KiB               | connection closed                    | open, pushes dropped             |
//! | #1088 | RESP2 tracker pipelines `HELLO 3`/`GET k`/`BLPOP` | `>2 invalidate [k]` + BLPOP reply   | delivered                        |
//! | #1088 | RESP3 tracker in MONITOR, key it read is written | `>2 invalidate [k]` + feed line      | nothing (monoio)                 |
//! | #1089 | `GET k` then `SET k` in the caller's own script  | `:1` then `>2 invalidate [k]`        | `:1` only                        |
//! | #1089 | tracker GETs k; other client EVAL SET k          | `>2 invalidate [k]`                  | nothing                          |
//! | #1089 | tracker EVAL GET k; other client SET k           | `>2 invalidate [k]`                  | nothing                          |
//! | #1089 | same through EVALSHA, EVAL_RO, FCALL, FCALL_RO   | pushed                               | nothing                          |
//! | #1089 | FCALL of a function that only READS k            | nothing                              | nothing                          |
//! | #1089 | tracker runs EVAL SET on a key it tracks         | `+OK` then `>2 invalidate [k]`       | `+OK` only                       |
//! | #1090 | `SUBSCRIBE x/UNSUBSCRIBE/SET k v/GET k`, one write | `+OK` `$1 v`                       | subscriber-context errors (monoio), no reply at all (tokio) |
//! | #1090 | `SUBSCRIBE x/RESET/SET/GET`, one write           | `+RESET +OK $1 v`                    | `+RESET`, then nothing           |
//! | #1078 | CLIENT INFO of a tracking client                 | `flags=t ... redir=<id>`             | `flags=N ... redir=-1`           |
//!
//! Every test runs at `--shards 1` and `--shards 4`; the suite is run once per
//! runtime by pinning `MOON_BIN` to a monoio and to a tokio build.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

fn moon_binary() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(shards: &str, extra: &[&str]) -> Moon {
    let bin = moon_binary();
    let dir_for = |port: u16| std::env::temp_dir().join(format!("moon-trk1088-{port}"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = dir_for(port);
        let _ = std::fs::create_dir_all(&tmp_dir);
        let port_s = port.to_string();
        let mut args = vec![
            "--port",
            &port_s,
            "--shards",
            shards,
            "--admin-port",
            "0",
            "--appendonly",
            "no",
            "--disk-free-min-pct",
            "0",
            "--dir",
            tmp_dir.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        Command::new(&bin)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr"))
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir: dir_for(port),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut c = Resp::connect(moon.port);
        c.cmd(&["PING"]);
        if c.saw(b"+PONG") {
            return moon;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon (--shards {shards}) never answered PING\n--- stderr ---\n{log}");
}

/// Minimal RESP client over a blocking TcpStream. The one blocking command in
/// this file (a `BLPOP`) runs on a connection that sends nothing after it, so
/// one connection per role is reused safely.
struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
    closed: bool,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        Self {
            stream,
            buf: Vec::new(),
            closed: false,
        }
    }

    fn send_raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("write");
    }

    fn send(&mut self, args: &[&str]) {
        self.send_raw(&common::encode(args));
    }

    fn pump(&mut self, total: Duration) {
        let deadline = Instant::now() + total;
        let mut chunk = [0u8; 16384];
        while Instant::now() < deadline && !self.closed {
            match self.stream.read(&mut chunk) {
                Ok(0) => self.closed = true,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => self.closed = true,
            }
        }
    }

    fn pump_until(&mut self, done: impl Fn(&[u8]) -> bool, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !done(&self.buf) && !self.closed && Instant::now() < deadline {
            self.pump(Duration::from_millis(50));
        }
    }

    /// Send, then read until something arrives (2 s cap) plus a short settle
    /// so a reply that straddles two reads is whole.
    fn cmd(&mut self, args: &[&str]) {
        let before = self.buf.len();
        self.send(args);
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.buf.len() == before && !self.closed && Instant::now() < deadline {
            self.pump(Duration::from_millis(20));
        }
        self.pump(Duration::from_millis(60));
    }

    /// `cmd`, returning exactly the bytes this command produced.
    fn reply(&mut self, args: &[&str]) -> Vec<u8> {
        self.buf.clear();
        self.cmd(args);
        std::mem::take(&mut self.buf)
    }

    fn saw(&self, needle: &[u8]) -> bool {
        self.buf.windows(needle.len()).any(|w| w == needle)
    }

    fn count(&self, needle: &[u8]) -> usize {
        self.buf
            .windows(needle.len())
            .filter(|w| *w == needle)
            .count()
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

fn client_id(c: &mut Resp) -> String {
    let r = c.reply(&["CLIENT", "ID"]);
    String::from_utf8_lossy(&r)
        .trim_start_matches(':')
        .trim_end()
        .to_string()
}

fn resp3_tracker(port: u16, mode: &[&str]) -> Resp {
    let mut t = Resp::connect(port);
    t.cmd(&["HELLO", "3"]);
    let mut args = vec!["CLIENT", "TRACKING", "on"];
    args.extend_from_slice(mode);
    assert_eq!(t.reply(&args), b"+OK\r\n".to_vec());
    t
}

/// RESP3 push redis sends for a one-key invalidation.
fn push_for(key: &str) -> Vec<u8> {
    format!(
        ">2\r\n$10\r\ninvalidate\r\n*1\r\n${}\r\n{key}\r\n",
        key.len()
    )
    .into_bytes()
}

const PUSH_HEAD: &[u8] = b">2\r\n$10\r\ninvalidate\r\n";
const MESSAGE_HEAD: &[u8] = b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n";

// ═════════════════ moon#1088: no invalidation is ever dropped ═════════════════

const BURST: usize = 400;

/// `GET` every burst key on `reader` in one write, then wait for every reply.
fn read_burst_keys(reader: &mut Resp, tag: &str, null_reply: &[u8]) {
    let mut pipeline = Vec::new();
    for i in 0..BURST {
        pipeline.extend_from_slice(&common::encode(&["GET", &format!("{{{tag}}}:{i}")]));
    }
    reader.buf.clear();
    reader.send_raw(&pipeline);
    let want = null_reply.len() * BURST;
    reader.pump_until(|b| b.len() >= want, Duration::from_secs(5));
    assert_eq!(
        reader.buf.len(),
        want,
        "the {BURST} GET replies did not arrive whole: {:?}",
        reader.text()
    );
    reader.buf.clear();
}

/// One `MSET` over every burst key (hash-tagged, so one shard owns them all).
fn mset_burst(port: u16, tag: &str) {
    let keys: Vec<String> = (0..BURST).map(|i| format!("{{{tag}}}:{i}")).collect();
    let mut args = vec!["MSET"];
    for k in &keys {
        args.push(k);
        args.push("v");
    }
    let mut w = Resp::connect(port);
    assert_eq!(w.reply(&args), b"+OK\r\n".to_vec());
}

fn burst_reaches_a_resp3_tracker(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut t = resp3_tracker(m.port, &[]);
    read_burst_keys(&mut t, "big", b"_\r\n");
    mset_burst(m.port, "big");
    t.pump_until(|b| count_in(b, PUSH_HEAD) >= BURST, Duration::from_secs(10));
    assert_eq!(
        t.count(PUSH_HEAD),
        BURST,
        "--shards {shards}: a {BURST}-key MSET must reach the tracker as {BURST} invalidations \
         (redis 8.6.1 delivers every one)"
    );
    assert!(
        !t.closed,
        "--shards {shards}: the tracker must stay connected"
    );
    // Still usable afterwards.
    assert_eq!(t.reply(&["PING"]), b"+PONG\r\n".to_vec());
}

fn count_in(buf: &[u8], needle: &[u8]) -> usize {
    buf.windows(needle.len()).filter(|w| *w == needle).count()
}

#[test]
fn burst_reaches_a_resp3_tracker_1_shard() {
    burst_reaches_a_resp3_tracker("1");
}

#[test]
fn burst_reaches_a_resp3_tracker_4_shards() {
    burst_reaches_a_resp3_tracker("4");
}

/// How a burst of `BURST` writes is issued.
#[derive(Clone, Copy, Debug)]
enum Burst {
    /// One `MSET` over every key.
    Mset,
    /// One script making `BURST` writing `redis.call`s.
    Script,
    /// One `MULTI` body of `BURST` `SET`s.
    Exec,
}

fn write_burst(port: u16, tag: &str, how: Burst) {
    if matches!(how, Burst::Mset) {
        mset_burst(port, tag);
        return;
    }
    let keys: Vec<String> = (0..BURST).map(|i| format!("{{{tag}}}:{i}")).collect();
    let mut w = Resp::connect(port);
    match how {
        Burst::Mset => {}
        Burst::Script => {
            let n = BURST.to_string();
            let mut args = vec![
                "EVAL",
                "for i = 1, #KEYS do redis.call('SET', KEYS[i], 'v') end return 1",
                &n,
            ];
            args.extend(keys.iter().map(String::as_str));
            assert_eq!(w.reply(&args), b":1\r\n".to_vec());
        }
        Burst::Exec => {
            let mut pipeline = common::encode(&["MULTI"]);
            for k in &keys {
                pipeline.extend_from_slice(&common::encode(&["SET", k, "v"]));
            }
            pipeline.extend_from_slice(&common::encode(&["EXEC"]));
            w.send_raw(&pipeline);
            let want = b"*400\r\n";
            w.pump_until(
                |b| b.windows(want.len()).any(|x| x == want) && b.ends_with(b"+OK\r\n"),
                Duration::from_secs(10),
            );
            assert!(w.saw(want), "EXEC did not commit: {:?}", w.text());
        }
    }
}

fn burst_reaches_a_resp2_redirect_target(shards: &str) {
    for how in [Burst::Mset, Burst::Script, Burst::Exec] {
        burst_reaches_a_resp2_redirect_target_via(shards, how);
    }
}

fn burst_reaches_a_resp2_redirect_target_via(shards: &str, how: Burst) {
    let m = spawn_moon(shards, &[]);
    let mut target = Resp::connect(m.port);
    let tid = client_id(&mut target);
    target.cmd(&["SUBSCRIBE", "__redis__:invalidate"]);
    target.buf.clear();
    let mut source = Resp::connect(m.port);
    assert_eq!(
        source.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", &tid]),
        b"+OK\r\n".to_vec()
    );
    read_burst_keys(&mut source, "rb", b"$-1\r\n");
    write_burst(m.port, "rb", how);
    target.pump_until(
        |b| count_in(b, MESSAGE_HEAD) >= BURST,
        Duration::from_secs(10),
    );
    assert_eq!(
        target.count(MESSAGE_HEAD),
        BURST,
        "--shards {shards}, {how:?}: a RESP2 REDIRECT target must receive all {BURST} messages"
    );
    assert!(
        !target.closed,
        "--shards {shards}, {how:?}: the target must stay connected"
    );
}

#[test]
fn burst_reaches_a_resp2_redirect_target_1_shard() {
    burst_reaches_a_resp2_redirect_target("1");
}

#[test]
fn burst_reaches_a_resp2_redirect_target_4_shards() {
    burst_reaches_a_resp2_redirect_target("4");
}

/// `BURST` separate `SET`s in one pipelined write, so every command queues
/// its own invalidation before anything is read.
fn pipeline_burst_of_sets(port: u16, tag: &str) {
    let mut pipeline = Vec::new();
    for i in 0..BURST {
        pipeline.extend_from_slice(&common::encode(&["SET", &format!("{{{tag}}}:{i}"), "v"]));
    }
    let mut w = Resp::connect(port);
    w.send_raw(&pipeline);
    let want = b"+OK\r\n".len() * BURST;
    w.pump_until(|b| b.len() >= want, Duration::from_secs(10));
    assert_eq!(
        w.buf.len(),
        want,
        "the pipelined SETs were not all answered"
    );
}

/// A tracking connection's own queue has no slot limit: a pipeline of
/// single-key writes reaches it whole, like the one wide MSET does.
fn pipelined_writes_reach_a_resp3_tracker(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut t = resp3_tracker(m.port, &[]);
    read_burst_keys(&mut t, "pw", b"_\r\n");
    pipeline_burst_of_sets(m.port, "pw");
    t.pump_until(|b| count_in(b, PUSH_HEAD) >= BURST, Duration::from_secs(10));
    assert_eq!(t.count(PUSH_HEAD), BURST, "--shards {shards}");
    assert!(
        !t.closed,
        "--shards {shards}: the tracker must stay connected"
    );
}

#[test]
fn pipelined_writes_reach_a_resp3_tracker_1_shard() {
    pipelined_writes_reach_a_resp3_tracker("1");
}

#[test]
fn pipelined_writes_reach_a_resp3_tracker_4_shards() {
    pipelined_writes_reach_a_resp3_tracker("4");
}

/// A subscribed REDIRECT target receives through its pub/sub channel, whose
/// slots are PUBLISH's slow-subscriber policy. Whatever the outcome of a
/// pipeline of writes that outruns it, it is never the silent one: either
/// every message arrives, or the target is disconnected.
fn pipelined_writes_never_silently_short_a_redirect_target(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut target = Resp::connect(m.port);
    let tid = client_id(&mut target);
    target.cmd(&["SUBSCRIBE", "__redis__:invalidate"]);
    target.buf.clear();
    let mut source = Resp::connect(m.port);
    assert_eq!(
        source.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", &tid]),
        b"+OK\r\n".to_vec()
    );
    read_burst_keys(&mut source, "pr", b"$-1\r\n");
    pipeline_burst_of_sets(m.port, "pr");
    target.pump_until(
        |b| count_in(b, MESSAGE_HEAD) >= BURST,
        Duration::from_secs(10),
    );
    let got = target.count(MESSAGE_HEAD);
    assert!(
        got == BURST || target.closed,
        "--shards {shards}: {got}/{BURST} messages and the target still connected — \
         a silent partial delivery"
    );
}

#[test]
fn pipelined_writes_never_silently_short_a_redirect_target_1_shard() {
    pipelined_writes_never_silently_short_a_redirect_target("1");
}

#[test]
fn pipelined_writes_never_silently_short_a_redirect_target_4_shards() {
    pipelined_writes_never_silently_short_a_redirect_target("4");
}

/// One command whose invalidations exceed the output-buffer limit, sent to a
/// tracker that keeps reading. Redis is single-threaded: the whole `MSET`
/// lands in the tracker's output buffer before any of it is written, so with
/// `client-output-buffer-limit normal 8192 0 0` the tracker is always closed.
///
/// moon's limit bounds what is QUEUED for the connection, as redis's bounds
/// its output buffer. At `--shards 1` the writer runs on the tracker's own
/// thread, so the connection cannot drain in between. That is redis's case,
/// and the tracker is closed. At `--shards 4` the tracker's connection may run
/// on another thread and drain the queue while it fills. Then every push is
/// delivered and the queue never holds 8 KiB, which is correct: the limit is a
/// memory bound. Which of the two happens depends on scheduling and on which
/// shard accepted the connection (it differs between platforms), so all this
/// test demands there is the issue's rule: every invalidation, or a closed
/// connection, never a silent short. The deterministic overflow test is
/// `overflow_disconnects_a_tracker_that_stops_reading`.
fn a_burst_past_the_limit_is_never_silently_short(shards: &str) {
    let m = spawn_moon(shards, &["--client-output-buffer-limit-normal", "8192"]);
    let mut t = resp3_tracker(m.port, &[]);
    read_burst_keys(&mut t, "ob", b"_\r\n");
    mset_burst(m.port, "ob");
    t.pump_until(|b| count_in(b, PUSH_HEAD) >= BURST, Duration::from_secs(5));
    // Let a close that follows the last push arrive too.
    t.pump(Duration::from_millis(300));
    let got = t.count(PUSH_HEAD);
    if shards == "1" {
        assert!(
            t.closed && got < BURST,
            "--shards 1: {BURST} invalidations (~16 KiB) queued at once past an 8 KiB \
             output-buffer limit must close the tracker (redis 8.6.1 does); got {got} pushes, \
             closed={}",
            t.closed
        );
    }
    assert!(
        t.closed || got == BURST,
        "--shards {shards}: the tracker got {got} of {BURST} invalidations and is still \
         connected: a silent short"
    );
    // The server itself is unaffected.
    let mut other = Resp::connect(m.port);
    assert_eq!(other.reply(&["PING"]), b"+PONG\r\n".to_vec());
}

#[test]
fn a_burst_past_the_limit_is_never_silently_short_1_shard() {
    a_burst_past_the_limit_is_never_silently_short("1");
}

#[test]
fn a_burst_past_the_limit_is_never_silently_short_4_shards() {
    a_burst_past_the_limit_is_never_silently_short("4");
}

/// Key length for the stalled-reader test. Long keys make each push large, so
/// the kernel's buffering is used up in a few hundred commands.
const STALL_KEY_LEN: usize = 4096;
/// SETs per pipelined write in the stalled-reader test.
const STALL_BATCH: usize = 64;
/// What the writer may send before the test gives up. A loopback connection
/// buffers at most a few MiB in the server's send buffer plus a few MiB in the
/// client's receive buffer. Linux autotunes to 4 + 6 MiB by default (cloud
/// images raise both to 16 MiB at most), and Windows' receive window stops
/// at 16 MiB. Reaching 128 MiB would mean the server queued everything past
/// its limit without bound. On unix the writer stops as soon as the server
/// drops the tracker, far below this.
const STALL_MAX_BYTES: usize = 128 << 20;

/// A tracker that stops reading is disconnected once more than the
/// output-buffer limit is waiting for it, whatever the kernel buffers in
/// between. It never silently loses pushes and stays open. This is the
/// deterministic form of redis's
/// `client-output-buffer-limit normal <n> 0 0` rule.
///
/// BCAST makes every write invalidate. The tracker reads nothing while the
/// writer sends until the server has dropped it. That fills the server's send
/// buffer and the tracker's receive buffer, so moon's queue for the tracker
/// must grow past 8 KiB.
fn overflow_disconnects_a_tracker_that_stops_reading(shards: &str) {
    let m = spawn_moon(shards, &["--client-output-buffer-limit-normal", "8192"]);
    let mut t = resp3_tracker(m.port, &["BCAST", "PREFIX", "ovf:"]);
    let tid = client_id(&mut t);
    // From here the tracker reads nothing until the writer is done.
    let pad = "x".repeat(STALL_KEY_LEN);
    let mut pipeline = Vec::new();
    for i in 0..STALL_BATCH {
        pipeline.extend_from_slice(&common::encode(&["SET", &format!("ovf:{i}:{pad}"), "v"]));
    }
    let mut w = Resp::connect(m.port);
    let (mut sent_bytes, mut writes, mut dropped) = (0usize, 0usize, false);
    while sent_bytes < STALL_MAX_BYTES {
        w.buf.clear();
        w.send_raw(&pipeline);
        w.pump_until(
            |b| count_in(b, b"+OK\r\n") >= STALL_BATCH,
            Duration::from_secs(10),
        );
        assert_eq!(
            w.count(b"+OK\r\n"),
            STALL_BATCH,
            "--shards {shards}: the writer was held up by a tracker that does not read"
        );
        sent_bytes += pipeline.len();
        writes += STALL_BATCH;
        let list = String::from_utf8_lossy(&w.reply(&["CLIENT", "LIST"])).into_owned();
        if !list.contains(&format!("id={tid} ")) {
            dropped = true;
            break;
        }
    }
    // On unix the CLIENT KILL path shuts the socket down, which ends the
    // server's blocked write at once. Elsewhere the kill is cooperative: the
    // connection closes when its pending write completes, i.e. once the
    // tracker reads again, and the queue stops growing in the meantime.
    if cfg!(unix) {
        assert!(
            dropped,
            "--shards {shards}: the server still holds a tracker that stopped reading after \
             {writes} invalidations (~{} MiB), past an 8 KiB output-buffer limit",
            sent_bytes >> 20
        );
    }
    t.pump_until(|_| false, Duration::from_secs(30));
    let got = t.count(PUSH_HEAD);
    assert!(
        t.closed,
        "--shards {shards}: a tracker past its output-buffer limit must be disconnected; it \
         got {got} of {writes} invalidations and is still connected"
    );
    assert!(
        got < writes,
        "--shards {shards}: every one of {writes} invalidations was delivered, so the limit \
         was never reached and the test proves nothing"
    );
    // The server itself is unaffected.
    let mut other = Resp::connect(m.port);
    assert_eq!(other.reply(&["PING"]), b"+PONG\r\n".to_vec());
}

#[test]
fn overflow_disconnects_a_tracker_that_stops_reading_1_shard() {
    overflow_disconnects_a_tracker_that_stops_reading("1");
}

#[test]
fn overflow_disconnects_a_tracker_that_stops_reading_4_shards() {
    overflow_disconnects_a_tracker_that_stops_reading("4");
}

/// A RESP2 tracking connection that switches to RESP3 in the middle of a
/// pipeline must receive the pushes for keys it reads after the switch.
/// `BLPOP` parks the connection right after that read, so it does not get
/// back to the top of its loop before the key is written.
///
/// Measured on redis-server 8.6.1: `CLIENT TRACKING on`, then one write of
/// `HELLO 3` / `GET hk` / `BLPOP hq 0`, then another client runs `SET hk v2`
/// and `RPUSH hq x`. The tracker receives `>2 invalidate [hk]` and the
/// `BLPOP` reply.
fn a_tracker_switching_to_resp3_mid_pipeline_gets_its_push(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let mut t = Resp::connect(m.port);
    assert_eq!(t.reply(&["CLIENT", "TRACKING", "on"]), b"+OK\r\n".to_vec());
    let mut pipeline = common::encode(&["HELLO", "3"]);
    pipeline.extend_from_slice(&common::encode(&["GET", "hk"]));
    pipeline.extend_from_slice(&common::encode(&["BLPOP", "hq", "0"]));
    t.send_raw(&pipeline);
    // The HELLO map and GET's `_` arrive; BLPOP then blocks.
    t.pump_until(|b| b.ends_with(b"_\r\n"), Duration::from_secs(4));
    assert!(
        t.buf.ends_with(b"_\r\n"),
        "--shards {shards}: HELLO 3 + GET did not answer: {:?}",
        t.text()
    );
    t.buf.clear();
    let mut w = Resp::connect(m.port);
    assert_eq!(w.reply(&["SET", "hk", "v2"]), b"+OK\r\n".to_vec());
    assert_eq!(w.reply(&["RPUSH", "hq", "x"]), b":1\r\n".to_vec());
    let push = push_for("hk");
    let popped = b"*2\r\n$2\r\nhq\r\n$1\r\nx\r\n";
    t.pump_until(
        |b| count_in(b, &push) == 1 && count_in(b, popped) == 1,
        Duration::from_secs(4),
    );
    assert_eq!(
        (t.count(&push), t.count(popped)),
        (1, 1),
        "--shards {shards}: redis delivers `invalidate [hk]` and the BLPOP reply; moon sent {:?}",
        t.text()
    );
}

#[test]
fn a_tracker_switching_to_resp3_mid_pipeline_gets_its_push_1_shard() {
    a_tracker_switching_to_resp3_mid_pipeline_gets_its_push("1");
}

#[test]
fn a_tracker_switching_to_resp3_mid_pipeline_gets_its_push_4_shards() {
    a_tracker_switching_to_resp3_mid_pipeline_gets_its_push("4");
}

/// A RESP2 tracker has nowhere to receive its own invalidations (redis writes
/// it nothing), and once it subscribes nothing reads its queue. So its own
/// invalidations must not pile up until the output-buffer limit disconnects
/// it. On redis-server 8.6.1 it stays connected and keeps receiving
/// messages.
fn a_resp2_subscribed_tracker_is_not_disconnected_by_its_own_invalidations(shards: &str) {
    const N: usize = 500;
    let m = spawn_moon(shards, &["--client-output-buffer-limit-normal", "8192"]);
    let mut t = Resp::connect(m.port);
    assert_eq!(
        t.reply(&["CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "r2:"]),
        b"+OK\r\n".to_vec()
    );
    t.cmd(&["SUBSCRIBE", "r2chan"]);
    let pad = "q".repeat(32);
    let mut pipeline = Vec::new();
    for i in 0..N {
        pipeline.extend_from_slice(&common::encode(&["SET", &format!("r2:{i}:{pad}"), "v"]));
    }
    let mut w = Resp::connect(m.port);
    w.send_raw(&pipeline);
    w.pump_until(|b| count_in(b, b"+OK\r\n") >= N, Duration::from_secs(10));
    assert_eq!(
        w.count(b"+OK\r\n"),
        N,
        "the writer's SETs did not all answer"
    );
    t.buf.clear();
    assert_eq!(w.reply(&["PUBLISH", "r2chan", "hi"]), b":1\r\n".to_vec());
    let msg = b"*3\r\n$7\r\nmessage\r\n$6\r\nr2chan\r\n$2\r\nhi\r\n";
    t.pump_until(|b| count_in(b, msg) == 1, Duration::from_secs(4));
    assert_eq!(
        t.text(),
        String::from_utf8_lossy(msg),
        "--shards {shards}: a RESP2 subscriber got something other than the message \
         (closed={})",
        t.closed
    );
}

#[test]
fn a_resp2_subscribed_tracker_is_not_disconnected_by_its_own_invalidations_1_shard() {
    a_resp2_subscribed_tracker_is_not_disconnected_by_its_own_invalidations("1");
}

#[test]
fn a_resp2_subscribed_tracker_is_not_disconnected_by_its_own_invalidations_4_shards() {
    a_resp2_subscribed_tracker_is_not_disconnected_by_its_own_invalidations("4");
}

/// A RESP3 tracker that runs MONITOR keeps receiving its invalidations, as
/// on redis-server 8.6.1 (measured: `HELLO 3`, `CLIENT TRACKING on`,
/// `GET mk`, `MONITOR`, then another client's `SET mk v` delivers
/// `>2 invalidate [mk]` and the feed line).
///
/// It is also never disconnected for pushes that only pile up because
/// nothing drains them while it monitors. The writer below makes one write
/// per round trip, so a tracker whose pushes are delivered never has more
/// than a handful waiting. Together they are ~30 KiB, far past the 8 KiB
/// limit, which a queue that is never read would cross. (A single pipelined
/// burst of the same size would close the tracker on redis too, since its
/// output buffer takes the whole burst at once.)
fn a_monitoring_tracker_keeps_its_pushes(shards: &str) {
    const N: usize = 500;
    let m = spawn_moon(shards, &["--client-output-buffer-limit-normal", "8192"]);
    let mut t = resp3_tracker(m.port, &["BCAST", "PREFIX", "mon:"]);
    assert_eq!(t.reply(&["MONITOR"]), b"+OK\r\n".to_vec());
    let pad = "p".repeat(32);
    let mut w = Resp::connect(m.port);
    // 20 SETs per round trip: ~1.2 KiB of pushes at a time.
    const STEP: usize = 20;
    for chunk in 0..N / STEP {
        let mut pipeline = Vec::new();
        for i in chunk * STEP..(chunk + 1) * STEP {
            pipeline.extend_from_slice(&common::encode(&["SET", &format!("mon:{i}:{pad}"), "v"]));
        }
        w.buf.clear();
        w.send_raw(&pipeline);
        w.pump_until(|b| count_in(b, b"+OK\r\n") >= STEP, Duration::from_secs(4));
        assert_eq!(
            w.count(b"+OK\r\n"),
            STEP,
            "round trip {chunk} did not answer"
        );
    }
    t.pump_until(|b| count_in(b, PUSH_HEAD) >= N, Duration::from_secs(10));
    assert_eq!(
        t.count(PUSH_HEAD),
        N,
        "--shards {shards}: a monitoring tracker must receive all {N} invalidations; closed={}",
        t.closed
    );
    t.pump(Duration::from_millis(200));
    assert!(
        !t.closed,
        "--shards {shards}: the monitoring tracker was disconnected"
    );
}

#[test]
fn a_monitoring_tracker_keeps_its_pushes_1_shard() {
    a_monitoring_tracker_keeps_its_pushes("1");
}

#[test]
fn a_monitoring_tracker_keeps_its_pushes_4_shards() {
    a_monitoring_tracker_keeps_its_pushes("4");
}

// ═══════════════════ moon#1089: scripts are visible to tracking ═══════════════════

const LIB: &str = "#!lua name=trk1089\n\
redis.register_function('w', function(k, a) return redis.call('SET', k[1], 'z') end)\n\
redis.register_function{function_name='r', callback=function(k, a) return redis.call('GET', k[1]) end, flags={'no-writes'}}";

/// A tracker that caches `key` through `read`, a writer that changes it
/// through `write`: the tracker must be told, byte for byte.
fn expect_push(port: u16, key: &str, read: &[&str], write: &[&str], case: &str, shards: &str) {
    let mut t = resp3_tracker(port, &[]);
    t.cmd(read);
    t.buf.clear();
    let mut w = Resp::connect(port);
    w.cmd(write);
    let want = push_for(key);
    t.pump_until(
        |b| b.windows(want.len()).any(|x| x == want),
        Duration::from_secs(4),
    );
    assert!(
        t.saw(&want),
        "{case} at --shards {shards}: redis pushes `invalidate [{key}]`; moon sent {:?}",
        t.text()
    );
}

/// The tracker must NOT be told about `key`. A negative is only meaningful
/// next to a control proving delivery works on the same connection: after
/// `write`, the writer also changes `control` (which the tracker read), and
/// the test waits for THAT push before checking `key` stayed silent.
fn expect_silence(
    t: &mut Resp,
    port: u16,
    key: &str,
    control: &str,
    write: &[&str],
    case: &str,
    shards: &str,
) {
    t.cmd(&["GET", control]);
    t.buf.clear();
    let mut w = Resp::connect(port);
    w.cmd(write);
    w.cmd(&["SET", control, "c"]);
    let ctl = push_for(control);
    t.pump_until(
        |b| b.windows(ctl.len()).any(|x| x == ctl),
        Duration::from_secs(4),
    );
    assert!(t.saw(&ctl), "{case}: the control push never arrived");
    t.pump(Duration::from_millis(200));
    assert!(
        !t.saw(&push_for(key)),
        "{case} at --shards {shards}: redis sends nothing for {key}; moon sent {:?}",
        t.text()
    );
}

fn scripts_invalidate_and_track(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let p = m.port;
    let mut admin = Resp::connect(p);
    let sha = {
        let r = admin.reply(&["SCRIPT", "LOAD", "return redis.call('SET', KEYS[1], 'y')"]);
        let s = String::from_utf8_lossy(&r).into_owned();
        s.split("\r\n").nth(1).expect("sha").to_string()
    };
    assert_eq!(
        admin.reply(&["FUNCTION", "LOAD", "REPLACE", LIB]),
        b"$7\r\ntrk1089\r\n".to_vec()
    );

    // Writes made by a script invalidate.
    let set = "return redis.call('SET', KEYS[1], 'x')";
    let get = "return redis.call('GET', KEYS[1])";
    expect_push(
        p,
        "ev:w",
        &["GET", "ev:w"],
        &["EVAL", set, "1", "ev:w"],
        "EVAL write",
        shards,
    );
    expect_push(
        p,
        "ev:sha",
        &["GET", "ev:sha"],
        &["EVALSHA", &sha, "1", "ev:sha"],
        "EVALSHA write",
        shards,
    );
    expect_push(
        p,
        "fc:w",
        &["GET", "fc:w"],
        &["FCALL", "w", "1", "fc:w"],
        "FCALL write",
        shards,
    );
    // Reads made by a script are tracked for the caller.
    expect_push(
        p,
        "ev:r",
        &["EVAL", get, "1", "ev:r"],
        &["SET", "ev:r", "2"],
        "EVAL read",
        shards,
    );
    expect_push(
        p,
        "ev:ro",
        &["EVAL_RO", get, "1", "ev:ro"],
        &["SET", "ev:ro", "2"],
        "EVAL_RO read",
        shards,
    );
    expect_push(
        p,
        "fc:r",
        &["FCALL_RO", "r", "1", "fc:r"],
        &["SET", "fc:r", "1"],
        "FCALL_RO read",
        shards,
    );

    // A function that only READS its key does not invalidate it, even though
    // FCALL itself is write-flagged.
    let mut t = resp3_tracker(p, &[]);
    t.cmd(&["GET", "fc:ro"]);
    expect_silence(
        &mut t,
        p,
        "fc:ro",
        "fc:ctl",
        &["FCALL", "r", "1", "fc:ro"],
        "FCALL read-only function",
        shards,
    );
}

#[test]
fn scripts_invalidate_and_track_1_shard() {
    scripts_invalidate_and_track("1");
}

#[test]
fn scripts_invalidate_and_track_4_shards() {
    scripts_invalidate_and_track("4");
}

/// Reads and writes inside one script take effect for the caller in the
/// order the script made them. Measured on redis-server 8.6.1 for a RESP3
/// tracker running the script itself:
///
/// | script body           | tracker receives                | then an outside `SET` |
/// |-----------------------|---------------------------------|-----------------------|
/// | `GET k` then `SET k`  | `:1` then `>2 invalidate [k]`   | nothing               |
/// | `SET k` then `GET k`  | `:1`                            | `>2 invalidate [k]`   |
/// | `GET`, `SET`, NOLOOP  | `:1`                            | nothing               |
fn a_script_s_reads_and_writes_apply_in_order(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let p = m.port;
    let get_set = "redis.call('GET', KEYS[1]); redis.call('SET', KEYS[1], 'v'); return 1";
    let set_get = "redis.call('SET', KEYS[1], 'v'); redis.call('GET', KEYS[1]); return 1";
    // (case, tracking mode, body, push right after `:1`, push on an outside SET)
    let cases: [(&str, &[&str], &str, bool, bool); 3] = [
        ("GET then SET", &[], get_set, true, false),
        ("SET then GET", &[], set_get, false, true),
        ("GET then SET, NOLOOP", &["NOLOOP"], get_set, false, false),
    ];
    for (i, (case, mode, body, pushed_now, pushed_after)) in cases.into_iter().enumerate() {
        let key = format!("so:{i}");
        let mut t = resp3_tracker(p, mode);
        let want = push_for(&key);
        let mut eval_reply = b":1\r\n".to_vec();
        if pushed_now {
            eval_reply.extend_from_slice(&want);
        }
        t.buf.clear();
        t.send(&["EVAL", body, "1", &key]);
        t.pump_until(|b| b.len() >= eval_reply.len(), Duration::from_secs(4));
        t.pump(Duration::from_millis(150));
        assert_eq!(
            t.text(),
            String::from_utf8_lossy(&eval_reply),
            "{case} at --shards {shards}: what the script's caller receives"
        );
        t.buf.clear();
        let mut w = Resp::connect(p);
        assert_eq!(w.reply(&["SET", &key, "w"]), b"+OK\r\n".to_vec());
        t.pump_until(|b| !b.is_empty(), Duration::from_millis(600));
        let expected: &[u8] = if pushed_after { &want } else { b"" };
        assert_eq!(
            t.text(),
            String::from_utf8_lossy(expected),
            "{case} at --shards {shards}: what an outside SET then pushes"
        );
    }
}

#[test]
fn a_script_s_reads_and_writes_apply_in_order_1_shard() {
    a_script_s_reads_and_writes_apply_in_order("1");
}

#[test]
fn a_script_s_reads_and_writes_apply_in_order_4_shards() {
    a_script_s_reads_and_writes_apply_in_order("4");
}

fn scripts_in_multi_and_tracking_modes(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let p = m.port;
    let set = "return redis.call('SET', KEYS[1], 'x')";
    let get = "return redis.call('GET', KEYS[1])";

    // A script queued inside MULTI: its write invalidates ...
    {
        let mut t = resp3_tracker(p, &[]);
        t.cmd(&["GET", "mx:w"]);
        t.buf.clear();
        let mut w = Resp::connect(p);
        w.cmd(&["MULTI"]);
        w.cmd(&["EVAL", set, "1", "mx:w"]);
        assert_eq!(w.reply(&["EXEC"]), b"*1\r\n+OK\r\n".to_vec());
        let want = push_for("mx:w");
        t.pump_until(
            |b| b.windows(want.len()).any(|x| x == want),
            Duration::from_secs(4),
        );
        assert!(
            t.saw(&want),
            "MULTI/EVAL write at --shards {shards}: {:?}",
            t.text()
        );
    }
    // ... and its read is tracked for the caller.
    {
        let mut t = resp3_tracker(p, &[]);
        t.cmd(&["MULTI"]);
        t.cmd(&["EVAL", get, "1", "mx:r"]);
        assert_eq!(t.reply(&["EXEC"]), b"*1\r\n_\r\n".to_vec());
        let mut w = Resp::connect(p);
        w.cmd(&["SET", "mx:r", "1"]);
        let want = push_for("mx:r");
        t.pump_until(
            |b| b.windows(want.len()).any(|x| x == want),
            Duration::from_secs(4),
        );
        assert!(
            t.saw(&want),
            "MULTI/EVAL read at --shards {shards}: {:?}",
            t.text()
        );
    }

    // OPTIN: a script read is tracked only after CLIENT CACHING yes.
    {
        let mut t = resp3_tracker(p, &["OPTIN"]);
        t.cmd(&["EVAL", get, "1", "oi:no"]);
        // Control: CACHING yes + a script read of oi:ctl.
        assert_eq!(t.reply(&["CLIENT", "CACHING", "yes"]), b"+OK\r\n".to_vec());
        t.cmd(&["EVAL", get, "1", "oi:ctl"]);
        t.buf.clear();
        let mut w = Resp::connect(p);
        w.cmd(&["SET", "oi:no", "1"]);
        w.cmd(&["SET", "oi:ctl", "1"]);
        let ctl = push_for("oi:ctl");
        t.pump_until(
            |b| b.windows(ctl.len()).any(|x| x == ctl),
            Duration::from_secs(4),
        );
        assert!(
            t.saw(&ctl),
            "OPTIN + CACHING yes script read at --shards {shards}: {:?}",
            t.text()
        );
        t.pump(Duration::from_millis(200));
        assert!(
            !t.saw(&push_for("oi:no")),
            "OPTIN without CACHING tracked a script read"
        );
    }
    // OPTOUT + CACHING no: the script's read is not tracked.
    {
        let mut t = resp3_tracker(p, &["OPTOUT"]);
        assert_eq!(t.reply(&["CLIENT", "CACHING", "no"]), b"+OK\r\n".to_vec());
        t.cmd(&["EVAL", get, "1", "oo:no"]);
        expect_silence(
            &mut t,
            p,
            "oo:no",
            "oo:ctl",
            &["SET", "oo:no", "1"],
            "OPTOUT + CACHING no",
            shards,
        );
    }
    // NOLOOP: the caller's own script write does not come back to it.
    {
        let mut t = resp3_tracker(p, &["NOLOOP"]);
        t.cmd(&["GET", "nl:k"]);
        assert_eq!(t.reply(&["EVAL", set, "1", "nl:k"]), b"+OK\r\n".to_vec());
        expect_silence(
            &mut t,
            p,
            "nl:k",
            "nl:ctl",
            &["PING"],
            "NOLOOP own script write",
            shards,
        );
    }
    // Without NOLOOP it does, AFTER the reply: `+OK` then the push.
    {
        let mut t = resp3_tracker(p, &[]);
        t.cmd(&["GET", "self:k"]);
        t.buf.clear();
        t.send(&["EVAL", set, "1", "self:k"]);
        let want = [b"+OK\r\n".as_slice(), &push_for("self:k")].concat();
        t.pump_until(|b| b.len() >= want.len(), Duration::from_secs(4));
        assert_eq!(
            t.text(),
            String::from_utf8_lossy(&want),
            "self script write at --shards {shards}"
        );
    }
    // BCAST prefix.
    {
        let mut t = resp3_tracker(p, &["BCAST", "PREFIX", "bc:"]);
        t.buf.clear();
        let mut w = Resp::connect(p);
        w.cmd(&["EVAL", set, "1", "bc:1"]);
        let want = push_for("bc:1");
        t.pump_until(
            |b| b.windows(want.len()).any(|x| x == want),
            Duration::from_secs(4),
        );
        assert!(
            t.saw(&want),
            "BCAST script write at --shards {shards}: {:?}",
            t.text()
        );
        t.pump(Duration::from_millis(200));
        assert_eq!(t.count(&want), 1, "one write, one push: {:?}", t.text());
    }
}

#[test]
fn scripts_in_multi_and_tracking_modes_1_shard() {
    scripts_in_multi_and_tracking_modes("1");
}

#[test]
fn scripts_in_multi_and_tracking_modes_4_shards() {
    scripts_in_multi_and_tracking_modes("4");
}

// ═══════════ moon#1090: the subscriber gate follows the live count ═══════════

fn pipelined(port: u16, cmds: &[&[&str]], want_len: usize) -> Vec<u8> {
    let mut c = Resp::connect(port);
    let mut bytes = Vec::new();
    for cmd in cmds {
        bytes.extend_from_slice(&common::encode(cmd));
    }
    c.send_raw(&bytes);
    c.pump_until(|b| b.len() >= want_len, Duration::from_secs(3));
    c.pump(Duration::from_millis(100));
    c.buf
}

fn subscriber_gate_follows_live_count(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let p = m.port;
    let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();

    let want: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$1\r\nx\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\nx\r\n:0\r\n+OK\r\n$1\r\nv\r\n";
    let got = pipelined(
        p,
        &[
            &["SUBSCRIBE", "x"],
            &["UNSUBSCRIBE"],
            &["SET", "k1090", "v"],
            &["GET", "k1090"],
        ],
        want.len(),
    );
    assert_eq!(
        text(&got),
        text(want),
        "SUBSCRIBE/UNSUBSCRIBE/SET/GET at --shards {shards}"
    );

    let want: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$1\r\nx\r\n:1\r\n+RESET\r\n+OK\r\n$1\r\nv\r\n";
    let got = pipelined(
        p,
        &[
            &["SUBSCRIBE", "x"],
            &["RESET"],
            &["SET", "k1090r", "v"],
            &["GET", "k1090r"],
        ],
        want.len(),
    );
    assert_eq!(
        text(&got),
        text(want),
        "SUBSCRIBE/RESET/SET/GET at --shards {shards}"
    );

    let want: &[u8] = b"*3\r\n$10\r\npsubscribe\r\n$2\r\np*\r\n:1\r\n*3\r\n$12\r\npunsubscribe\r\n$2\r\np*\r\n:0\r\n+PONG\r\n";
    let got = pipelined(
        p,
        &[&["PSUBSCRIBE", "p*"], &["PUNSUBSCRIBE"], &["PING"]],
        want.len(),
    );
    assert_eq!(
        text(&got),
        text(want),
        "PSUBSCRIBE/PUNSUBSCRIBE/PING at --shards {shards}"
    );

    // The issue's sequence: HELLO 3 runs, so the last SUBSCRIBE answers as a push.
    let tail: &[u8] = b">3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n";
    let got = pipelined(
        p,
        &[
            &["SUBSCRIBE", "x"],
            &["UNSUBSCRIBE"],
            &["HELLO", "3"],
            &["SUBSCRIBE", "__redis__:invalidate"],
        ],
        200,
    );
    assert!(
        got.ends_with(tail) && !text(&got).contains("-ERR"),
        "SUBSCRIBE/UNSUBSCRIBE/HELLO 3/SUBSCRIBE at --shards {shards}: {:?}",
        text(&got)
    );

    // Control: one channel is still subscribed, so GET is refused.
    let want: &[u8] = b"*3\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:2\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n-ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
    let got = pipelined(
        p,
        &[
            &["SUBSCRIBE", "a", "b"],
            &["UNSUBSCRIBE", "a"],
            &["GET", "k"],
        ],
        want.len(),
    );
    assert_eq!(
        text(&got),
        text(want),
        "still subscribed at --shards {shards}"
    );
}

#[test]
fn subscriber_gate_follows_live_count_1_shard() {
    subscriber_gate_follows_live_count("1");
}

#[test]
fn subscriber_gate_follows_live_count_4_shards() {
    subscriber_gate_follows_live_count("4");
}

// ═══════════════ moon#1078: CLIENT LIST / INFO show tracking ═══════════════

fn field<'a>(line: &'a str, name: &str) -> &'a str {
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(name).and_then(|v| v.strip_prefix('=')))
        .unwrap_or_else(|| panic!("no {name}= in {line:?}"))
}

fn info(c: &mut Resp) -> String {
    String::from_utf8_lossy(&c.reply(&["CLIENT", "INFO"])).into_owned()
}

fn client_list_and_info_show_tracking(shards: &str) {
    let m = spawn_moon(shards, &[]);
    let p = m.port;

    let mut target = Resp::connect(p);
    let tid = client_id(&mut target);
    let mut src = Resp::connect(p);
    let sid = client_id(&mut src);
    src.cmd(&["CLIENT", "TRACKING", "on", "REDIRECT", &tid]);
    let line = info(&mut src);
    assert_eq!(
        (field(&line, "flags"), field(&line, "redir")),
        ("t", tid.as_str()),
        "{line}"
    );

    // CLIENT LIST reports the same, from any connection.
    let mut other = Resp::connect(p);
    let list = String::from_utf8_lossy(&other.reply(&["CLIENT", "LIST"])).into_owned();
    let row = list
        .lines()
        .find(|l| l.contains(&format!("id={sid} ")))
        .unwrap_or_else(|| panic!("source missing from CLIENT LIST: {list}"));
    assert_eq!(
        (field(row, "flags"), field(row, "redir")),
        ("t", tid.as_str()),
        "{row}"
    );

    // Tracking without a redirect: redir=0.
    let mut plain = Resp::connect(p);
    plain.cmd(&["CLIENT", "TRACKING", "on", "OPTIN"]);
    let line = info(&mut plain);
    assert_eq!(
        (field(&line, "flags"), field(&line, "redir")),
        ("t", "0"),
        "{line}"
    );
    // Off again: back to N / -1.
    plain.cmd(&["CLIENT", "TRACKING", "off"]);
    let line = info(&mut plain);
    assert_eq!(
        (field(&line, "flags"), field(&line, "redir")),
        ("N", "-1"),
        "{line}"
    );

    // BCAST, and a redirect whose target went away: flags=tRB.
    let mut gone = Resp::connect(p);
    let gid = client_id(&mut gone);
    let mut b = Resp::connect(p);
    b.cmd(&["HELLO", "3"]);
    b.cmd(&["CLIENT", "TRACKING", "on", "REDIRECT", &gid, "BCAST"]);
    let line = info(&mut b);
    assert!(line.contains("flags=tB "), "{line}");
    drop(gone);
    std::thread::sleep(Duration::from_millis(300));
    let mut w = Resp::connect(p);
    w.cmd(&["SET", "x1078", "1"]);
    b.pump_until(
        |x| x.windows(21).any(|y| y == b"tracking-redir-broken"),
        Duration::from_secs(4),
    );
    let line = info(&mut b);
    assert!(line.contains("flags=tRB "), "{line}");
    assert!(line.contains(&format!("redir={gid} ")), "{line}");
}

#[test]
fn client_list_and_info_show_tracking_1_shard() {
    client_list_and_info_show_tracking("1");
}

#[test]
fn client_list_and_info_show_tracking_4_shards() {
    client_list_and_info_show_tracking("4");
}
