//! moon#1234: `DEL`, `UNLINK` and `GETDEL` must publish the `del` keyspace
//! event (class `g`) once per key they actually remove — and nothing for a key
//! that was absent — on every path that runs them: a plain command, a spanning
//! delete whose keys live on several shards, MULTI/EXEC and Lua `redis.call`.
//! `GETDEL` of a missing key publishes `keymiss` (class `m`), like `GET`.
//!
//! Every expectation below was captured from redis-server 7.0.15 with the same
//! commands (see the WS18 NOTES.md for the probe): redis publishes on
//! `__keyspace@0__:<key>` (payload `del`) and `__keyevent@0__:del` (payload
//! the key) per removed key, in argument order.
//!
//! Red on `ae21476` and on `d155cd6`: no command path queued `del` at all, so
//! the subscriber saw only the sentinel `set`s.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws18_del_notify`.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::ServerGuard;

fn spawn(shards: usize) -> (ServerGuard, u16, std::path::PathBuf) {
    let dir = common::unique_test_dir(&format!("ws18-del-notify-s{shards}"));
    std::fs::create_dir_all(&dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let d = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard::new(child), port, dir)
}

/// A RESP2 value, as much of it as these tests need.
#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<String>),
    Array(Option<Vec<Resp>>),
}

struct Client {
    sock: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn open(port: u16) -> Self {
        let sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        sock.set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        Client {
            sock,
            buf: Vec::new(),
        }
    }

    fn write(&mut self, parts: &[&str]) {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        self.sock.write_all(&out).expect("write");
    }

    /// Parse one complete value from the front of `buf`, if there is one.
    fn parse(buf: &[u8], pos: &mut usize) -> Option<Resp> {
        let line_end = buf[*pos..].windows(2).position(|w| w == b"\r\n")? + *pos;
        let line = std::str::from_utf8(&buf[*pos + 1..line_end])
            .unwrap()
            .to_string();
        let tag = buf[*pos];
        *pos = line_end + 2;
        Some(match tag {
            b'+' => Resp::Simple(line),
            b'-' => Resp::Error(line),
            b':' => Resp::Int(line.parse().unwrap()),
            b'$' => {
                let n: i64 = line.parse().unwrap();
                if n < 0 {
                    Resp::Bulk(None)
                } else {
                    let n = n as usize;
                    if buf.len() < *pos + n + 2 {
                        return None;
                    }
                    let s = String::from_utf8_lossy(&buf[*pos..*pos + n]).into_owned();
                    *pos += n + 2;
                    Resp::Bulk(Some(s))
                }
            }
            b'*' | b'>' => {
                let n: i64 = line.parse().unwrap();
                if n < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(n as usize);
                    for _ in 0..n {
                        items.push(Self::parse(buf, pos)?);
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?}"),
        })
    }

    /// Read one value, waiting at most `budget`.
    fn read_within(&mut self, budget: Duration) -> Option<Resp> {
        let deadline = Instant::now() + budget;
        loop {
            let mut pos = 0;
            if !self.buf.is_empty()
                && let Some(v) = Self::parse(&self.buf, &mut pos)
            {
                self.buf.drain(..pos);
                return Some(v);
            }
            if Instant::now() >= deadline {
                return None;
            }
            let mut chunk = [0u8; 16384];
            match self.sock.read(&mut chunk) {
                Ok(0) => panic!(
                    "server closed the connection; unparsed: {:?}",
                    String::from_utf8_lossy(&self.buf)
                ),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(e) => panic!("read: {e}"),
            }
        }
    }

    fn cmd(&mut self, parts: &[&str]) -> Resp {
        self.write(parts);
        self.read_within(Duration::from_secs(10))
            .unwrap_or_else(|| panic!("no reply to {parts:?}"))
    }
}

/// One published event: `(channel, payload)`.
type Event = (String, String);

/// Keys that cover every shard, one per shard, for the sentinel round.
fn sentinel_keys(shards: usize, round: usize) -> Vec<String> {
    let mut out: Vec<Option<String>> = vec![None; shards];
    let mut i = 0usize;
    while out.iter().any(Option::is_none) {
        let k = format!("ws18:sentinel:{round}:{i}");
        let s = moon::shard::dispatch::key_to_shard(k.as_bytes(), shards);
        if out[s].is_none() {
            out[s] = Some(k);
        }
        i += 1;
    }
    out.into_iter().map(Option::unwrap).collect()
}

struct Probe {
    shards: usize,
    cmd: Client,
    sub: Client,
    round: usize,
}

impl Probe {
    fn new(port: u16, shards: usize, flags: &str) -> Self {
        let mut cmd = Client::open(port);
        assert_eq!(
            cmd.cmd(&["CONFIG", "SET", "notify-keyspace-events", flags]),
            Resp::Simple("OK".into())
        );
        let mut sub = Client::open(port);
        for pat in ["__keyspace@0__:*", "__keyevent@0__:*"] {
            sub.write(&["PSUBSCRIBE", pat]);
            let ack = sub.read_within(Duration::from_secs(10)).unwrap();
            assert!(
                matches!(&ack, Resp::Array(Some(v)) if v[0] == Resp::Bulk(Some("psubscribe".into()))),
                "PSUBSCRIBE ack: {ack:?}"
            );
        }
        let mut p = Probe {
            shards,
            cmd,
            sub,
            round: 0,
        };
        // Sentinels only publish when `$` (or A) is on; every flag set the
        // tests use keeps a string class for exactly this reason.
        p.settle();
        p
    }

    /// Write one sentinel on EVERY shard and read events until each one's
    /// `set` has arrived. Events travel each shard's ring in FIFO order, so
    /// everything a command queued before the sentinels is in hand by then —
    /// no sleep-and-hope window. Returns the non-sentinel events seen.
    fn settle(&mut self) -> Vec<Event> {
        self.round += 1;
        let keys = sentinel_keys(self.shards, self.round);
        for k in &keys {
            assert_eq!(self.cmd.cmd(&["SET", k, "x"]), Resp::Simple("OK".into()));
        }
        let mut pending: BTreeSet<String> = keys.iter().cloned().collect();
        let mut events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(20);
        while !pending.is_empty() {
            assert!(
                Instant::now() < deadline,
                "sentinel events never arrived: missing {pending:?}; got {events:?}"
            );
            let Some(msg) = self.sub.read_within(Duration::from_millis(200)) else {
                continue;
            };
            let Resp::Array(Some(v)) = msg else {
                panic!("unexpected push {msg:?}");
            };
            let (Resp::Bulk(Some(ch)), Resp::Bulk(Some(payload))) = (&v[2], &v[3]) else {
                panic!("unexpected pmessage {v:?}");
            };
            // Either half proves the sentinel's shard drained up to it (a
            // flag set may enable only one of K/E).
            if ch == "__keyevent@0__:set" && pending.remove(payload) {
                continue;
            }
            if payload == "set"
                && let Some(k) = ch.strip_prefix("__keyspace@0__:")
                && pending.remove(k)
            {
                continue;
            }
            if ch.contains("ws18:sentinel:") || payload.contains("ws18:sentinel:") {
                continue;
            }
            events.push((ch.clone(), payload.clone()));
        }
        events
    }

    /// Run `parts`, return its reply and every event it published.
    fn run(&mut self, parts: &[&str]) -> (Resp, Vec<Event>) {
        let reply = self.cmd.cmd(parts);
        (reply, self.settle())
    }
}

fn keyevent_del(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter(|(ch, _)| ch == "__keyevent@0__:del")
        .map(|(_, k)| k.clone())
        .collect()
}

fn keyspace_del(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter(|(ch, p)| ch.starts_with("__keyspace@0__:") && p == "del")
        .map(|(ch, _)| ch["__keyspace@0__:".len()..].to_string())
        .collect()
}

/// Keys spread over every shard: `n` of them, `ws18:<tag>:<i>`.
fn spread_keys(tag: &str, n: usize, shards: usize) -> Vec<String> {
    let keys: Vec<String> = (0..n).map(|i| format!("ws18:{tag}:{i}")).collect();
    let owners: BTreeSet<usize> = keys
        .iter()
        .map(|k| moon::shard::dispatch::key_to_shard(k.as_bytes(), shards))
        .collect();
    assert_eq!(owners.len(), shards, "the keys must cover every shard");
    keys
}

/// Compare as a sequence at `--shards 1` (redis's argument order) and as a
/// set otherwise: a spanning delete publishes from several shards, whose
/// relative order is the mesh's, not the command line's.
fn assert_same(shards: usize, got: Vec<String>, want: Vec<String>, what: &str) {
    if shards == 1 {
        assert_eq!(got, want, "{what}");
    } else {
        let mut g = got.clone();
        let mut w = want.clone();
        g.sort();
        w.sort();
        assert_eq!(g, w, "{what}: got {got:?}");
    }
}

fn check(shards: usize) {
    let (guard, port, dir) = spawn(shards);
    let mut p = Probe::new(port, shards, "KEA");

    // DEL over keys on every shard, half of them absent, one repeated.
    let keys = spread_keys("del", 12, shards);
    for k in keys.iter().step_by(2) {
        p.cmd.cmd(&["SET", k, "v"]);
    }
    p.settle();
    let mut argv: Vec<&str> = vec!["DEL"];
    argv.extend(keys.iter().map(String::as_str));
    argv.push(keys[0].as_str()); // already removed by then: no second event
    let (reply, events) = p.run(&argv);
    let removed: Vec<String> = keys.iter().step_by(2).cloned().collect();
    assert_eq!(reply, Resp::Int(removed.len() as i64), "DEL count");
    assert_same(
        shards,
        keyevent_del(&events),
        removed.clone(),
        &format!("--shards {shards}: DEL keyevent"),
    );
    assert_same(
        shards,
        keyspace_del(&events),
        removed.clone(),
        &format!("--shards {shards}: DEL keyspace"),
    );

    // UNLINK: the same contract, and a collection value takes the lazy path.
    let keys = spread_keys("unlink", 12, shards);
    for (i, k) in keys.iter().enumerate().filter(|(i, _)| i % 3 != 0) {
        if i % 2 == 0 {
            p.cmd.cmd(&["RPUSH", k, "a", "b"]);
        } else {
            p.cmd.cmd(&["SET", k, "v"]);
        }
    }
    p.settle();
    let mut argv: Vec<&str> = vec!["UNLINK"];
    argv.extend(keys.iter().map(String::as_str));
    let (reply, events) = p.run(&argv);
    let removed: Vec<String> = keys
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 != 0)
        .map(|(_, k)| k.clone())
        .collect();
    assert_eq!(reply, Resp::Int(removed.len() as i64), "UNLINK count");
    assert_same(
        shards,
        keyevent_del(&events),
        removed,
        &format!("--shards {shards}: UNLINK keyevent"),
    );

    // Nothing removed: nothing published.
    let (reply, events) = p.run(&["DEL", "ws18:none:1", "ws18:none:2"]);
    assert_eq!(reply, Resp::Int(0));
    assert!(events.is_empty(), "DEL of absent keys published {events:?}");

    // GETDEL: hit -> `del`; wrong type -> nothing.
    p.cmd.cmd(&["SET", "ws18:gd", "v"]);
    p.cmd.cmd(&["RPUSH", "ws18:gdl", "x"]);
    p.settle();
    let (reply, events) = p.run(&["GETDEL", "ws18:gd"]);
    assert_eq!(reply, Resp::Bulk(Some("v".into())));
    assert_eq!(
        events,
        vec![
            ("__keyspace@0__:ws18:gd".into(), "del".into()),
            ("__keyevent@0__:del".into(), "ws18:gd".into()),
        ],
        "--shards {shards}: GETDEL hit"
    );
    let (reply, events) = p.run(&["GETDEL", "ws18:gdl"]);
    assert!(matches!(reply, Resp::Error(ref e) if e.starts_with("WRONGTYPE")));
    assert!(events.is_empty(), "GETDEL on a list published {events:?}");

    // MULTI/EXEC: DEL then UNLINK of the same key -> ONE `del`.
    p.cmd.cmd(&["SET", "ws18:tx", "v"]);
    p.settle();
    assert_eq!(p.cmd.cmd(&["MULTI"]), Resp::Simple("OK".into()));
    assert_eq!(
        p.cmd.cmd(&["DEL", "ws18:tx"]),
        Resp::Simple("QUEUED".into())
    );
    assert_eq!(
        p.cmd.cmd(&["UNLINK", "ws18:tx"]),
        Resp::Simple("QUEUED".into())
    );
    let (reply, events) = p.run(&["EXEC"]);
    assert_eq!(
        reply,
        Resp::Array(Some(vec![Resp::Int(1), Resp::Int(0)])),
        "EXEC"
    );
    assert_eq!(
        keyevent_del(&events),
        vec!["ws18:tx".to_string()],
        "--shards {shards}: MULTI/EXEC"
    );

    // Lua: redis.call('DEL') on two co-located keys, one present.
    p.cmd.cmd(&["SET", "{ws18lua}a", "v"]);
    p.settle();
    let (reply, events) = p.run(&[
        "EVAL",
        "return redis.call('DEL', KEYS[1], KEYS[2])",
        "2",
        "{ws18lua}a",
        "{ws18lua}b",
    ]);
    assert_eq!(reply, Resp::Int(1), "EVAL DEL");
    assert_eq!(
        keyevent_del(&events),
        vec!["{ws18lua}a".to_string()],
        "--shards {shards}: Lua redis.call DEL"
    );
    drop(p);

    // The class decides: `$` without `g` publishes no `del`; `K` alone
    // publishes the keyspace half only; `m` adds GETDEL's `keymiss`.
    let mut p = Probe::new(port, shards, "KE$");
    p.cmd.cmd(&["SET", "ws18:cls", "v"]);
    p.settle();
    let (_, events) = p.run(&["DEL", "ws18:cls"]);
    assert!(events.is_empty(), "`del` is class g: {events:?}");
    drop(p);

    let mut p = Probe::new(port, shards, "Kg$");
    p.cmd.cmd(&["SET", "ws18:cls", "v"]);
    p.settle();
    let (_, events) = p.run(&["DEL", "ws18:cls"]);
    assert_eq!(
        events,
        vec![("__keyspace@0__:ws18:cls".into(), "del".into())],
        "K without E: keyspace half only"
    );
    drop(p);

    let mut p = Probe::new(port, shards, "KE$m");
    let (reply, events) = p.run(&["GETDEL", "ws18:missing"]);
    assert_eq!(reply, Resp::Bulk(None));
    assert_eq!(
        events,
        vec![
            ("__keyspace@0__:ws18:missing".into(), "keymiss".into()),
            ("__keyevent@0__:keymiss".into(), "ws18:missing".into()),
        ],
        "--shards {shards}: GETDEL miss publishes keymiss, as redis does"
    );
    drop(p);

    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Compare as sets: whether the active-expiry tick reaped a key first
/// decides which path published its `expired`, and so its position.
fn assert_same_keys(mut got: Vec<String>, mut want: Vec<String>, what: &str) {
    got.sort();
    want.sort();
    assert_eq!(got, want, "{what}");
}

fn keyevent(events: &[Event], event: &str) -> Vec<String> {
    let ch = format!("__keyevent@0__:{event}");
    events
        .iter()
        .filter(|(c, _)| *c == ch)
        .map(|(_, k)| k.clone())
        .collect()
}

/// Past the moment every key written just before it has expired by the
/// SHARD's clock, too: moon judges expiry against a cached clock that an
/// idle shard refreshes every 10 ms, so a key written with `PX 1` can still
/// read as live a few milliseconds later. 15 ms clears that; the 100 ms
/// active-expiry tick reaps some keys first either way — those publish the
/// same `expired` and answer the same `:0`, so the contract holds for both.
const PAST_EXPIRY: Duration = Duration::from_millis(15);

/// Keys `ws18:<tag>:<i>`, at least `per_shard` on every shard.
fn covering_keys(tag: &str, shards: usize, per_shard: usize) -> Vec<String> {
    let mut on = vec![0usize; shards];
    let mut keys = Vec::new();
    let mut i = 0usize;
    while on.iter().any(|&n| n < per_shard) {
        let k = format!("ws18:{tag}:{i}");
        let s = moon::shard::dispatch::key_to_shard(k.as_bytes(), shards);
        if on[s] < per_shard {
            on[s] += 1;
            keys.push(k);
        }
        i += 1;
    }
    keys
}

/// Keys written with `PX 1`, two on every shard.
fn expired_keys(p: &mut Probe, tag: &str) -> Vec<String> {
    let keys = covering_keys(&format!("exp:{tag}"), p.shards, 2);
    for k in &keys {
        assert_eq!(
            p.cmd.cmd(&["SET", k, "v", "PX", "1"]),
            Resp::Simple("OK".into())
        );
    }
    std::thread::sleep(PAST_EXPIRY);
    keys
}

/// moon#1234 residual (part 3b memory review S1/P2): DEL, UNLINK and GETDEL of
/// a key whose TTL has passed but that active expiry has not reaped yet.
///
/// redis 7.0.15, same bytes (see FIX3B-CM NOTES.md): `expireIfNeeded`
/// deletes the key BEFORE the command looks, so the reply is `:0` (GETDEL
/// nil), the key publishes `expired` (class `x`), and nothing publishes
/// `del`. Red on `5d1a37c`: every such key answered `:1` and published `del`.
fn check_expired(shards: usize) {
    let (guard, port, dir) = spawn(shards);
    let mut p = Probe::new(port, shards, "KEA");

    // Plain DEL and UNLINK, one key at a time.
    let keys: Vec<String> = (0..12).map(|i| format!("ws18:exp:one:{i}")).collect();
    let mut replies = Vec::new();
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            p.cmd.cmd(&["SET", k, "v", "PX", "1"]),
            Resp::Simple("OK".into())
        );
        std::thread::sleep(PAST_EXPIRY);
        replies.push(p.cmd.cmd(&[if i % 2 == 0 { "DEL" } else { "UNLINK" }, k]));
    }
    let events = p.settle();
    assert!(
        replies.iter().all(|r| *r == Resp::Int(0)),
        "--shards {shards}: DEL/UNLINK of an expired key must answer :0, got {replies:?}"
    );
    assert_eq!(
        keyevent(&events, "del"),
        Vec::<String>::new(),
        "--shards {shards}: DEL/UNLINK of an expired key published `del`"
    );
    assert_same_keys(
        keyevent(&events, "expired"),
        keys.clone(),
        &format!("--shards {shards}: one `expired` per expired key"),
    );

    // A spanning DEL and UNLINK: expired keys on every shard, one live key
    // and one absent. Only the live key counts and publishes `del`.
    for cmd in ["DEL", "UNLINK"] {
        let live = format!("ws18:exp:live:{cmd}");
        p.cmd.cmd(&["SET", &live, "v"]);
        p.settle();
        let keys = expired_keys(&mut p, cmd);
        let mut argv: Vec<&str> = vec![cmd];
        argv.extend(keys.iter().map(String::as_str));
        argv.push(&live);
        argv.push("ws18:exp:absent");
        let (reply, events) = p.run(&argv);
        assert_eq!(reply, Resp::Int(1), "--shards {shards}: spanning {cmd}");
        assert_eq!(
            keyevent(&events, "del"),
            vec![live.clone()],
            "--shards {shards}: spanning {cmd} publishes `del` for the live key only"
        );
        assert_same_keys(
            keyevent(&events, "expired"),
            keys,
            &format!("--shards {shards}: spanning {cmd} `expired`"),
        );
    }

    // MULTI/EXEC and Lua run the same body.
    let k = "ws18:exp:tx";
    p.cmd.cmd(&["SET", k, "v", "PX", "1"]);
    std::thread::sleep(PAST_EXPIRY);
    assert_eq!(p.cmd.cmd(&["MULTI"]), Resp::Simple("OK".into()));
    assert_eq!(p.cmd.cmd(&["DEL", k]), Resp::Simple("QUEUED".into()));
    let (reply, events) = p.run(&["EXEC"]);
    assert_eq!(reply, Resp::Array(Some(vec![Resp::Int(0)])), "EXEC");
    assert!(keyevent(&events, "del").is_empty(), "MULTI: {events:?}");
    assert_eq!(keyevent(&events, "expired"), vec![k.to_string()], "MULTI");

    let k = "{ws18exp}lua";
    p.cmd.cmd(&["SET", k, "v", "PX", "1"]);
    std::thread::sleep(PAST_EXPIRY);
    let (reply, events) = p.run(&["EVAL", "return redis.call('DEL', KEYS[1])", "1", k]);
    assert_eq!(reply, Resp::Int(0), "EVAL DEL");
    assert!(keyevent(&events, "del").is_empty(), "Lua: {events:?}");
    assert_eq!(keyevent(&events, "expired"), vec![k.to_string()], "Lua");

    // A script queued inside MULTI runs through the EXEC executor, not the
    // script arm: it must see the same clock.
    let k = "{ws18exp}txlua";
    p.cmd.cmd(&["SET", k, "v", "PX", "1"]);
    std::thread::sleep(PAST_EXPIRY);
    assert_eq!(p.cmd.cmd(&["MULTI"]), Resp::Simple("OK".into()));
    assert_eq!(
        p.cmd
            .cmd(&["EVAL", "return redis.call('DEL', KEYS[1])", "1", k]),
        Resp::Simple("QUEUED".into())
    );
    let (reply, events) = p.run(&["EXEC"]);
    assert_eq!(
        reply,
        Resp::Array(Some(vec![Resp::Int(0)])),
        "MULTI EVAL DEL"
    );
    assert!(keyevent(&events, "del").is_empty(), "MULTI Lua: {events:?}");
    assert_eq!(
        keyevent(&events, "expired"),
        vec![k.to_string()],
        "MULTI Lua"
    );

    // GETDEL looks the key up for READ first, and that path hides an
    // expired key and hands it to the active-expiry drain, which publishes
    // `expired` on its next tick (<= 100 ms) rather than before the reply.
    let k = "ws18:exp:getdel";
    p.cmd.cmd(&["SET", k, "v", "PX", "1"]);
    std::thread::sleep(PAST_EXPIRY);
    let (reply, mut events) = p.run(&["GETDEL", k]);
    assert_eq!(reply, Resp::Bulk(None), "GETDEL of an expired key is nil");
    let deadline = Instant::now() + Duration::from_secs(5);
    while keyevent(&events, "expired").is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        events.extend(p.settle());
    }
    assert!(keyevent(&events, "del").is_empty(), "GETDEL: {events:?}");
    assert_eq!(keyevent(&events, "expired"), vec![k.to_string()], "GETDEL");

    drop(p);
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Every `incr.aof` under `dir`, concatenated.
fn aof_bytes(dir: &std::path::Path) -> Vec<u8> {
    fn walk(d: &std::path::Path, out: &mut Vec<u8>) {
        let Ok(rd) = std::fs::read_dir(d) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if p.to_string_lossy().ends_with(".incr.aof") {
                out.extend_from_slice(&std::fs::read(&p).unwrap_or_default());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, &mut out);
    out
}

/// A DEL/UNLINK that reaps an expired key answers `:0` for it but DID delete
/// it, so the deletion must still reach the AOF (and, through the same
/// record, every replica — which runs no expiry of its own). Every path logs
/// DEL/UNLINK whatever it answers except the multi-key coordinator's
/// in-process leg, which skips a leg that removed nothing: that leg must
/// count a reaped expired key as something (`command::key::expired_reaps`).
/// Each key must therefore appear in the AOF twice — its `SET` and a
/// deletion record.
#[test]
fn a_spanning_delete_that_reaps_expired_keys_still_logs_them_at_shards_4() {
    let shards = 4;
    let dir = common::unique_test_dir("ws18-del-expired-aof");
    std::fs::create_dir_all(&dir).expect("create test dir");
    let bin = common::find_moon_binary();
    let d = dir.clone();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &d.to_string_lossy(),
                "--shards",
                "4",
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--save",
                "",
                "--disk-offload",
                "disable",
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(common::server_stderr(&d))
            .stderr(common::server_stderr(&d))
            .spawn()
            .expect("spawn moon")
    });
    let guard = ServerGuard::new(child);
    let mut c = Client::open(port);
    for cmd in ["DEL", "UNLINK"] {
        let keys = covering_keys(&format!("aof:{cmd}"), shards, 2);
        for k in &keys {
            assert_eq!(
                c.cmd(&["SET", k, "v", "PX", "1"]),
                Resp::Simple("OK".into())
            );
        }
        std::thread::sleep(PAST_EXPIRY);
        let mut argv: Vec<&str> = vec![cmd];
        argv.extend(keys.iter().map(String::as_str));
        assert_eq!(c.cmd(&argv), Resp::Int(0), "{cmd} of expired keys");
        // A key the DEL reaped is logged by the DEL's own leg. A key the
        // active-expiry tick reaped first is logged by the tick
        // (`record_reason_del`), which nothing awaits, so a loaded host can
        // read the file before that record lands: poll, bounded.
        let occurrences = |aof: &[u8], k: &str| {
            let needle = format!("\r\n{k}\r\n");
            aof.windows(needle.len())
                .filter(|w| *w == needle.as_bytes())
                .count()
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let aof = aof_bytes(&dir);
            let missing: Vec<(&String, usize)> = keys
                .iter()
                .map(|k| (k, occurrences(&aof, k)))
                .filter(|(_, n)| *n < 2)
                .collect();
            if missing.is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{cmd}: (key, occurrences) {missing:?} — each appears in the AOF \
                 only as its SET, with no deletion record, so a replica (which \
                 runs no expiry of its own) keeps it"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    drop(guard);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn del_unlink_getdel_of_an_expired_key_publish_expired_at_shards_1() {
    check_expired(1);
}

#[test]
fn del_unlink_getdel_of_an_expired_key_publish_expired_at_shards_4() {
    check_expired(4);
}

#[test]
fn del_unlink_getdel_publish_del_at_shards_1() {
    check(1);
}

#[test]
fn del_unlink_getdel_publish_del_at_shards_4() {
    check(4);
}
