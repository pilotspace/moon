//! moon#1286 residuals (wave-2a R1 review): `INFO stats` `expired_keys` must
//! match redis 7.2.7 where the first fix left gaps.
//!
//! - **Replay (finding 2):** redis counts nothing while it loads. The AOF
//!   holds every reap the live server made (the active cycle's reason `DEL`s,
//!   a client's `DEL` of an expired key, a write over one) and each was
//!   counted once, live; replaying them must not count them again. Oracle,
//!   `--appendonly yes`: 50 active-expired + 500 `HSET` over expired + 500
//!   `DEL` of expired = 1050 before a restart, **0** after a graceful and a
//!   kill -9 restart. f766fc2: 1050 after both, s1 and s4.
//!
//! - **Writes over an expired key (finding 3):** redis's `lookupKeyWrite`
//!   reaps (and counts) the expired key before the write. Oracle: every case
//!   below counts one per key. f766fc2 counted 0 for SET, SETNX, SET NX,
//!   GETSET, APPEND, INCRBYFLOAT, SETBIT, PFADD, the COPY / RENAME
//!   destination, GET→SET and EXISTS→SET (the read hides the key and the SET
//!   lands before the drain), a timing-dependent part of MSET, and INCR at s4.
//!   Also SETRANGE, SET … GET, SETEX, MSETNX, INCRBY, DECR and the
//!   SUNIONSTORE / BITOP destination (0 at s1). HSET, LPUSH, GETDEL, GETEX
//!   and DEL were right and are the controls.
//!
//! - **An absolute deadline already past (finding 4, and finding 9's NITs):**
//!   redis's `checkAlreadyExpired` deletes the key at once — a deletion, so
//!   it publishes `del` and counts 0 expired. f766fc2 stored the past
//!   deadline and let expiry reap it (1 counted, `expired` published) for
//!   `EXPIREAT`/`PEXPIREAT` past, `GETEX … EXAT/PXAT` past and `RESTORE …
//!   ABSTTL` past; `EXPIRE k -1` deleted but published nothing; and `GETEX k
//!   EX -1` answered "value is not an integer" where redis says "invalid
//!   expire time in 'getex' command".
//!
//! - **`CONFIG RESETSTAT` (finding 5):** redis's `resetServerStats` zeroes
//!   `expired_keys`; f766fc2's RESETSTAT was a placeholder that reset
//!   nothing. WS36's `txn_conflicts_refused` statistic is reset with it.
//!
//! Expected values were captured from redis 7.2.7 with the same commands.
//!
//! `MOON_BIN=<moon> cargo test --test expired_keys_parity_1286 -- --include-ignored`

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

use common::Conn;

fn spawn(dir: &Path, shards: usize, appendonly: bool) -> (common::ServerGuard, u16) {
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let (guard, port) = common::spawn_listening_guarded(move |port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                dir.to_str().unwrap(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                if appendonly { "yes" } else { "no" },
                "--appendfsync",
                "always",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(mut c) = std::panic::catch_unwind(|| Conn::open(port))
            && c.send(&["INFO", "persistence"]).contains("loading:0\r\n")
        {
            break;
        }
        assert!(Instant::now() < deadline, "server on {port} never loaded");
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port)
}

fn expired_keys(c: &mut Conn) -> u64 {
    let info = c.send(&["INFO", "stats"]);
    info.lines()
        .find_map(|l| l.strip_prefix("expired_keys:"))
        .unwrap_or_else(|| panic!("no expired_keys in INFO stats: {info}"))
        .trim()
        .parse()
        .unwrap()
}

fn dbsize(c: &mut Conn) -> i64 {
    c.send(&["DBSIZE"])
        .trim_start_matches(':')
        .trim()
        .parse()
        .unwrap()
}

/// Run `verb key args…` for `key` in `prefix0..prefixN`, one pipeline.
fn each(c: &mut Conn, n: usize, prefix: &str, verb: &[&str], tail: &[&str]) -> String {
    let keys: Vec<String> = (0..n).map(|i| format!("{prefix}{i}")).collect();
    let cmds: Vec<Vec<&str>> = keys
        .iter()
        .map(|k| {
            let mut v = verb.to_vec();
            v.push(k);
            v.extend_from_slice(tail);
            v
        })
        .collect();
    let refs: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
    c.pipeline(&refs)
}

/// SIGTERM (a graceful shutdown) and reap.
fn graceful_stop(guard: &mut common::ServerGuard) {
    let pid = guard.id().to_string();
    let _ = std::process::Command::new("kill")
        .args(["-TERM", &pid])
        .status();
    if let Some(mut child) = guard.take() {
        let deadline = Instant::now() + Duration::from_secs(30);
        while child.try_wait().ok().flatten().is_none() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                panic!("the server did not stop on SIGTERM");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Wait until `expired_keys` reaches `want` (or 10 s), returning the last read.
fn wait_for(c: &mut Conn, want: u64) -> u64 {
    let start = Instant::now();
    loop {
        let n = expired_keys(c);
        if n >= want || start.elapsed() >= Duration::from_secs(10) {
            return n;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// Finding 2: AOF replay
// ---------------------------------------------------------------------------

fn replay_does_not_recount(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1286-replay-s{shards}"));
    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    // Reaped by the active cycle: reason DELs in the AOF.
    each(&mut c, 50, "a", &["SET"], &["v", "PX", "50"]);
    // Reaped by a write over the expired key.
    each(&mut c, 500, "h", &["SET"], &["v", "PX", "30"]);
    std::thread::sleep(Duration::from_millis(40));
    each(&mut c, 500, "h", &["HSET"], &["f", "v"]);
    // Reaped by a client's DEL.
    each(&mut c, 500, "d", &["SET"], &["v", "PX", "30"]);
    std::thread::sleep(Duration::from_millis(40));
    each(&mut c, 500, "d", &["DEL"], &[]);
    let before = wait_for(&mut c, 1050);
    assert_eq!(before, 1050, "s{shards}: live count (redis 7.2.7: 1050)");
    assert_eq!(dbsize(&mut c), 500);
    drop(c);
    graceful_stop(&mut guard);

    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    std::thread::sleep(Duration::from_millis(500));
    let after_graceful = expired_keys(&mut c);
    let size_graceful = dbsize(&mut c);
    drop(c);
    guard.kill_now();

    let (mut guard, port) = spawn(&dir, shards, true);
    let mut c = Conn::open(port);
    std::thread::sleep(Duration::from_millis(500));
    let after_kill = expired_keys(&mut c);
    let size_kill = dbsize(&mut c);
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(
        (size_graceful, size_kill),
        (500, 500),
        "s{shards}: the replay restores the hashes"
    );
    assert_eq!(
        (after_graceful, after_kill),
        (0, 0),
        "s{shards}: expired_keys after a graceful and a kill -9 restart (redis 7.2.7: 0, 0): \
         the AOF replay counted the logged reaps again"
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn aof_replay_does_not_recount_expired_keys_1_shard() {
    replay_does_not_recount(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn aof_replay_does_not_recount_expired_keys_4_shards() {
    replay_does_not_recount(4);
}

// ---------------------------------------------------------------------------
// Finding 3: a write over an expired key
// ---------------------------------------------------------------------------

/// Keys per case; the oracle counts exactly one per key.
const OVERWRITE_N: usize = 1000;

/// `(name, commands for key index i)`. Keys share a hash tag per index, so a
/// two-key command stays on one shard at s4.
type Case = (&'static str, fn(usize) -> Vec<Vec<String>>);

fn v(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

fn o(i: usize) -> String {
    format!("{{t{i}}}o")
}

fn src(i: usize) -> String {
    format!("{{t{i}}}s")
}

const OVERWRITE_CASES: &[Case] = &[
    ("SET", |i| vec![v(&["SET", &o(i), "x"])]),
    ("SETNX", |i| vec![v(&["SETNX", &o(i), "x"])]),
    ("SET NX", |i| vec![v(&["SET", &o(i), "x", "NX"])]),
    ("GETSET", |i| vec![v(&["GETSET", &o(i), "x"])]),
    ("APPEND", |i| vec![v(&["APPEND", &o(i), "x"])]),
    ("INCR", |i| vec![v(&["INCR", &o(i)])]),
    ("INCRBYFLOAT", |i| vec![v(&["INCRBYFLOAT", &o(i), "1.5"])]),
    ("SETBIT", |i| vec![v(&["SETBIT", &o(i), "1", "1"])]),
    ("PFADD", |i| vec![v(&["PFADD", &o(i), "x"])]),
    ("MSET", |i| vec![v(&["MSET", &o(i), "x"])]),
    ("COPY destination", |i| {
        vec![v(&["SET", &src(i), "x"]), v(&["COPY", &src(i), &o(i)])]
    }),
    ("RENAME destination", |i| {
        vec![v(&["SET", &src(i), "x"]), v(&["RENAME", &src(i), &o(i)])]
    }),
    ("GET then SET", |i| {
        vec![v(&["GET", &o(i)]), v(&["SET", &o(i), "x"])]
    }),
    ("EXISTS then SET", |i| {
        vec![v(&["EXISTS", &o(i)]), v(&["SET", &o(i), "x"])]
    }),
    ("SETRANGE", |i| vec![v(&["SETRANGE", &o(i), "0", "x"])]),
    ("SET GET", |i| vec![v(&["SET", &o(i), "x", "GET"])]),
    ("SETEX", |i| vec![v(&["SETEX", &o(i), "100", "x"])]),
    ("MSETNX", |i| vec![v(&["MSETNX", &o(i), "x"])]),
    ("INCRBY", |i| vec![v(&["INCRBY", &o(i), "3"])]),
    ("DECR", |i| vec![v(&["DECR", &o(i)])]),
    ("SUNIONSTORE destination", |i| {
        vec![
            v(&["SADD", &src(i), "x"]),
            v(&["SUNIONSTORE", &o(i), &src(i)]),
        ]
    }),
    ("BITOP destination", |i| {
        vec![
            v(&["SET", &src(i), "x"]),
            v(&["BITOP", "NOT", &o(i), &src(i)]),
        ]
    }),
    // A past deadline on an already-expired key: the key does not exist
    // for the command (answers 0) and is reaped as expired.
    ("EXPIREAT past", |i| vec![v(&["EXPIREAT", &o(i), "1"])]),
    // Controls: counted before this fix.
    ("HSET", |i| vec![v(&["HSET", &o(i), "f", "v"])]),
    ("LPUSH", |i| vec![v(&["LPUSH", &o(i), "x"])]),
    ("GETDEL", |i| vec![v(&["GETDEL", &o(i)])]),
    ("GETEX", |i| vec![v(&["GETEX", &o(i), "EX", "100"])]),
    ("DEL", |i| vec![v(&["DEL", &o(i)])]),
    ("PERSIST", |i| vec![v(&["PERSIST", &o(i)])]),
    ("MOVE", |i| vec![v(&["MOVE", &o(i), "1"])]),
];

fn overwrite_matrix(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1286-overwrite-s{shards}"));
    let (mut guard, port) = spawn(&dir, shards, false);
    let mut c = Conn::open(port);
    let mut wrong = Vec::new();
    for (name, mk) in OVERWRITE_CASES {
        assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
        std::thread::sleep(Duration::from_millis(300));
        let before = expired_keys(&mut c);
        let sets: Vec<Vec<String>> = (0..OVERWRITE_N)
            .map(|i| v(&["SET", &o(i), "5", "PX", "30"]))
            .collect();
        pipeline(&mut c, &sets);
        std::thread::sleep(Duration::from_millis(32));
        let writes: Vec<Vec<String>> = (0..OVERWRITE_N).flat_map(mk).collect();
        pipeline(&mut c, &writes);
        // Whatever the write left expired (nothing, here) is reaped by now.
        std::thread::sleep(Duration::from_millis(1200));
        let delta = expired_keys(&mut c) - before;
        if delta != OVERWRITE_N as u64 {
            wrong.push(format!("{name}: {delta}"));
        }
    }
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        wrong.is_empty(),
        "s{shards}: expired_keys per {OVERWRITE_N} writes over an expired key (redis 7.2.7: \
         {OVERWRITE_N} each): {wrong:?}"
    );
}

fn pipeline(c: &mut Conn, cmds: &[Vec<String>]) {
    for chunk in cmds.chunks(500) {
        let owned: Vec<Vec<&str>> = chunk
            .iter()
            .map(|cmd| cmd.iter().map(String::as_str).collect())
            .collect();
        let refs: Vec<&[&str]> = owned.iter().map(Vec::as_slice).collect();
        c.pipeline(&refs);
    }
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_write_over_an_expired_key_counts_it_1_shard() {
    overwrite_matrix(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_write_over_an_expired_key_counts_it_4_shards() {
    overwrite_matrix(4);
}

// ---------------------------------------------------------------------------
// Finding 4 (+ finding 9): an absolute deadline already in the past
// ---------------------------------------------------------------------------

/// A `PSUBSCRIBE __keyevent@0__:*` connection; [`Self::drain`] returns the
/// event names published since the last call.
struct Events {
    sock: std::net::TcpStream,
    buf: Vec<u8>,
}

impl Events {
    fn open(port: u16) -> Self {
        let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        std::io::Write::write_all(
            &mut sock,
            &common::encode(&["PSUBSCRIBE", "__keyevent@0__:*"]),
        )
        .unwrap();
        sock.set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let mut ev = Events {
            sock,
            buf: Vec::new(),
        };
        ev.drain();
        ev
    }

    fn drain(&mut self) -> Vec<String> {
        let mut chunk = [0u8; 4096];
        while let Ok(n) = self.sock.read(&mut chunk) {
            if n == 0 {
                break;
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
        let text = String::from_utf8_lossy(&self.buf).into_owned();
        self.buf.clear();
        text.split("__keyevent@0__:")
            .skip(1)
            .filter_map(|rest| rest.split("\r\n").next())
            .filter(|name| *name != "*")
            .map(str::to_string)
            .collect()
    }
}

/// One command on a fresh connection, byte-exact both ways.
fn raw(port: u16, parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    std::io::Write::write_all(&mut sock, &out).unwrap();
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    while common::framed_len(&buf, 1).is_none() {
        let n = sock.read(&mut chunk).unwrap();
        assert!(n > 0, "server closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
    }
    buf
}

struct PastCase {
    name: &'static str,
    setup: &'static [&'static [&'static str]],
    /// `None` in an argument is replaced by a deadline `offset` ms from now.
    cmd: &'static [Option<&'static str>],
    past_offset_ms: i64,
    /// redis 7.2.7: (reply, EXISTS k after, events, expired_keys delta).
    reply: &'static str,
    exists: bool,
    events: &'static [&'static str],
}

const SET_K: &[&[&str]] = &[&["SET", "k", "v"]];

const PAST_CASES: &[PastCase] = &[
    PastCase {
        name: "EXPIREAT k 1",
        setup: SET_K,
        cmd: &[Some("EXPIREAT"), Some("k"), Some("1")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "EXPIREAT k now-10s",
        setup: SET_K,
        cmd: &[Some("EXPIREAT"), Some("k"), None],
        past_offset_ms: -10_000,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "PEXPIREAT k 1",
        setup: SET_K,
        cmd: &[Some("PEXPIREAT"), Some("k"), Some("1")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "PEXPIREAT k now-1000",
        setup: SET_K,
        cmd: &[Some("PEXPIREAT"), Some("k"), None],
        past_offset_ms: -1000,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "EXPIREAT k 1 LT",
        setup: SET_K,
        cmd: &[Some("EXPIREAT"), Some("k"), Some("1"), Some("LT")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "EXPIREAT k 1 GT",
        setup: SET_K,
        cmd: &[Some("EXPIREAT"), Some("k"), Some("1"), Some("GT")],
        past_offset_ms: 0,
        reply: ":0\r\n",
        exists: true,
        events: &[],
    },
    PastCase {
        name: "EXPIREAT k 1 on a hash",
        setup: &[&["HSET", "k", "f", "v"]],
        cmd: &[Some("EXPIREAT"), Some("k"), Some("1")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "EXPIREAT k 0",
        setup: SET_K,
        cmd: &[Some("EXPIREAT"), Some("k"), Some("0")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "EXPIRE k -1",
        setup: SET_K,
        cmd: &[Some("EXPIRE"), Some("k"), Some("-1")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "EXPIRE k 0",
        setup: SET_K,
        cmd: &[Some("EXPIRE"), Some("k"), Some("0")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "PEXPIRE k -1",
        setup: SET_K,
        cmd: &[Some("PEXPIRE"), Some("k"), Some("-1")],
        past_offset_ms: 0,
        reply: ":1\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "GETEX k PXAT 1",
        setup: SET_K,
        cmd: &[Some("GETEX"), Some("k"), Some("PXAT"), Some("1")],
        past_offset_ms: 0,
        reply: "$1\r\nv\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "GETEX k PXAT now-1000",
        setup: SET_K,
        cmd: &[Some("GETEX"), Some("k"), Some("PXAT"), None],
        past_offset_ms: -1000,
        reply: "$1\r\nv\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "GETEX k EXAT 1",
        setup: SET_K,
        cmd: &[Some("GETEX"), Some("k"), Some("EXAT"), Some("1")],
        past_offset_ms: 0,
        reply: "$1\r\nv\r\n",
        exists: false,
        events: &["del"],
    },
    PastCase {
        name: "GETEX k EX -1",
        setup: SET_K,
        cmd: &[Some("GETEX"), Some("k"), Some("EX"), Some("-1")],
        past_offset_ms: 0,
        reply: "-ERR invalid expire time in 'getex' command\r\n",
        exists: true,
        events: &[],
    },
    PastCase {
        name: "GETEX k PX 0",
        setup: SET_K,
        cmd: &[Some("GETEX"), Some("k"), Some("PX"), Some("0")],
        past_offset_ms: 0,
        reply: "-ERR invalid expire time in 'getex' command\r\n",
        exists: true,
        events: &[],
    },
    PastCase {
        name: "GETEX k EX abc",
        setup: SET_K,
        cmd: &[Some("GETEX"), Some("k"), Some("EX"), Some("abc")],
        past_offset_ms: 0,
        reply: "-ERR value is not an integer or out of range\r\n",
        exists: true,
        events: &[],
    },
];

fn past_deadline_matrix(shards: usize) {
    let dir = common::unique_test_dir(&format!("moon-1286-past-s{shards}"));
    let (mut guard, port) = spawn(&dir, shards, false);
    let mut c = Conn::open(port);
    assert_eq!(
        c.send(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]),
        "+OK\r\n"
    );
    let mut ev = Events::open(port);
    let mut wrong = Vec::new();
    let mut run = |name: &str,
                   c: &mut Conn,
                   setup: &[&[&str]],
                   exec: &dyn Fn(&mut Conn) -> String,
                   want: (&str, bool, &[&str])| {
        c.send(&["FLUSHALL"]);
        for s in setup {
            c.send(s);
        }
        std::thread::sleep(Duration::from_millis(100));
        ev.drain();
        let before = expired_keys(c);
        let reply = exec(c);
        let exists = c.send(&["EXISTS", "k"]) == ":1\r\n";
        std::thread::sleep(Duration::from_millis(300));
        let delta = expired_keys(c) - before;
        let events = ev.drain();
        if reply != want.0 || exists != want.1 || events != want.2 || delta != 0 {
            wrong.push(format!(
                "{name}: got reply {reply:?} exists {exists} events {events:?} expired {delta}; \
                 redis {:?} {} {:?} 0",
                want.0, want.1, want.2
            ));
        }
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    for case in PAST_CASES {
        let seconds = case.cmd[0] == Some("EXPIREAT") || case.cmd.contains(&Some("EXAT"));
        let deadline = now_ms + case.past_offset_ms;
        let deadline = if seconds { deadline / 1000 } else { deadline }.to_string();
        let cmd: Vec<&str> = case.cmd.iter().map(|a| a.unwrap_or(&deadline)).collect();
        run(
            case.name,
            &mut c,
            case.setup,
            &|c: &mut Conn| c.send(&cmd),
            (case.reply, case.exists, case.events),
        );
    }
    // RESTORE … ABSTTL in the past: nothing written; REPLACE deletes (`del`).
    // The DUMP payload is binary, so both go through a byte-exact socket.
    raw(port, &[b"SET", b"src", b"v"]);
    let dump = raw(port, &[b"DUMP", b"src"]);
    let start = dump.iter().position(|b| *b == b'\n').unwrap() + 1;
    let payload = dump[start..dump.len() - 2].to_vec();
    for (name, setup, replace, events) in [
        ("RESTORE k 1 <dump> ABSTTL", &[][..], false, &[][..]),
        (
            "RESTORE k 1 <dump> ABSTTL REPLACE",
            SET_K,
            true,
            &["del"][..],
        ),
    ] {
        let exec = |_: &mut Conn| {
            let mut parts: Vec<&[u8]> = vec![b"RESTORE", b"k", b"1", &payload, b"ABSTTL"];
            if replace {
                parts.push(b"REPLACE");
            }
            String::from_utf8_lossy(&raw(port, &parts)).into_owned()
        };
        run(name, &mut c, setup, &exec, ("+OK\r\n", false, events));
    }
    drop(ev);
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        wrong.is_empty(),
        "s{shards}: an absolute deadline already past, vs redis 7.2.7:\n{}",
        wrong.join("\n")
    );
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_past_absolute_deadline_deletes_without_counting_1_shard() {
    past_deadline_matrix(1);
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn a_past_absolute_deadline_deletes_without_counting_4_shards() {
    past_deadline_matrix(4);
}

// ---------------------------------------------------------------------------
// Finding 5: CONFIG RESETSTAT
// ---------------------------------------------------------------------------

fn info_u64(c: &mut Conn, field: &str) -> Option<u64> {
    let info = c.send(&["INFO", "all"]);
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse().ok())
}

#[test]
#[ignore = "real-server suite: MOON_BIN pinned"]
fn config_resetstat_zeroes_expired_keys() {
    let dir = common::unique_test_dir("moon-1286-resetstat");
    let (mut guard, port) = spawn(&dir, 1, false);
    let mut c = Conn::open(port);
    each(&mut c, 50, "r", &["SET"], &["v", "PX", "20"]);
    let before = wait_for(&mut c, 50);
    // A write refused because an open TXN holds the key: one conflict.
    let mut t = Conn::open(port);
    assert_eq!(t.send(&["TXN", "BEGIN"]), "+OK\r\n");
    assert_eq!(t.send(&["SET", "held", "txn"]), "+OK\r\n");
    let refused = c.send(&["SET", "held", "other"]);
    let conflicts = info_u64(&mut c, "txn_conflicts_refused");
    assert_eq!(t.send(&["TXN", "ABORT"]), "+OK\r\n");
    let reset = c.send(&["CONFIG", "RESETSTAT"]);
    let after = expired_keys(&mut c);
    let conflicts_after = info_u64(&mut c, "txn_conflicts_refused");
    guard.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(before, 50, "precondition");
    assert!(
        refused.starts_with("-TXNCONFLICT"),
        "precondition: {refused:?}"
    );
    assert_eq!(conflicts, Some(1), "precondition");
    assert_eq!(reset, "+OK\r\n");
    assert_eq!(
        after, 0,
        "expired_keys after CONFIG RESETSTAT (redis 7.2.7: 0)"
    );
    assert_eq!(
        conflicts_after,
        Some(0),
        "txn_conflicts_refused is a statistic"
    );
}
