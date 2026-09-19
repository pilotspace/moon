//! Spanning multi-key blocking pops serve exactly once, from the first
//! non-empty key in argument order (moon#1019), and a serve that races the end
//! of a wait is never destroyed (moon#1023).
//!
//! ## moon#1019
//!
//! `BLPOP q1 q2 q3 0` with untagged keys at `--shards > 1` puts the keys on
//! different shards. Before the fix each owner served the waiter on its own:
//! two owners with data both popped and the client kept one reply (the other
//! element was gone), and the client's pre-block scan skipped an earlier
//! REMOTE key to pop a later LOCAL one. The owner decided to keep these
//! commands working across shards (no `CROSSSLOT`), so the contract here is
//! standalone redis's: the first non-empty key in argument order answers,
//! exactly once, and `-WRONGTYPE` is owed for the first existing key of the
//! wrong type before it.
//!
//! ## moon#1023
//!
//! When a wait ended by timeout or by a vanished peer, the connection sent
//! `BlockCancel` and dropped its receivers. A serve landing in between was sent
//! into a still-live receiver, so the owner's undo (which runs only when its
//! send FAILS) never ran, and the element was dropped with the receiver. The
//! window is microseconds wide in production; `MOON_TEST_BLOCK_SETTLE_DELAY_MS`
//! holds it open so a push can land inside it deterministically.
//!
//! ## Placement
//!
//! A test cannot choose which shard its connection lands on (macOS sends every
//! connection to one shard; Linux's `SO_REUSEPORT` hash decides). So every
//! single-key row is run for a key owned by EVERY shard in turn — at least
//! `SHARDS - 1` of them are remote to wherever the connections landed — and the
//! spanning rows put three keys on three different shards.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

const SHARDS: usize = 4;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

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

fn spawn_moon(shards: usize, settle_delay_ms: Option<u64>) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-bsc-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        let mut cmd = Command::new(&bin);
        cmd.args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
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
        );
        match settle_delay_ms {
            Some(ms) => {
                cmd.env("MOON_TEST_BLOCK_SETTLE_DELAY_MS", ms.to_string());
            }
            None => {
                cmd.env_remove("MOON_TEST_BLOCK_SETTLE_DELAY_MS");
            }
        }
        cmd.spawn().expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-bsc-{port}"));
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

/// Render a RESP reply as stable text: a bulk -> its bytes, an array ->
/// `[a,b]`, a null -> `nil`, an error -> `!<text>`, `:3` -> `3`.
fn canon(s: &str) -> String {
    let b = s.as_bytes();
    let mut i = 0usize;
    canon_one(b, &mut i)
}

fn take_line(b: &[u8], i: &mut usize) -> String {
    let start = *i;
    while *i + 1 < b.len() && !(b[*i] == b'\r' && b[*i + 1] == b'\n') {
        *i += 1;
    }
    let s = String::from_utf8_lossy(&b[start..*i]).into_owned();
    *i = (*i + 2).min(b.len());
    s
}

fn canon_one(b: &[u8], i: &mut usize) -> String {
    if *i >= b.len() {
        return "<truncated>".to_string();
    }
    let tag = b[*i];
    *i += 1;
    let head = take_line(b, i);
    match tag {
        b'+' | b':' | b',' | b'#' => head,
        b'-' => format!("!{head}"),
        b'_' => "nil".to_string(),
        b'$' => {
            if head.starts_with('-') {
                return "nil".to_string();
            }
            let n: usize = head.parse().unwrap_or(0);
            let end = (*i + n).min(b.len());
            let s = String::from_utf8_lossy(&b[*i..end]).into_owned();
            *i = (end + 2).min(b.len());
            s
        }
        b'*' => {
            if head.starts_with('-') {
                return "nil".to_string();
            }
            let n: usize = head.parse().unwrap_or(0);
            let parts: Vec<String> = (0..n).map(|_| canon_one(b, i)).collect();
            format!("[{}]", parts.join(","))
        }
        other => format!("<unparsed {}: {}>", other as char, head),
    }
}

fn send(c: &mut Conn, argv: &[String]) -> String {
    let refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    canon(&c.send(&refs))
}

/// A key owned by shard `owner`, unique per `(tag, n)`.
fn key_owned_by(tag: &str, owner: usize, n: usize) -> String {
    (0..100_000)
        .map(|j| format!("bsc:{tag}:{n}:{j}"))
        .find(|k| key_to_shard(k.as_bytes(), SHARDS) == owner)
        .expect("a key for every shard")
}

/// Three untagged key names on three DIFFERENT shards.
fn spanning_three(tag: &str, i: usize) -> [String; 3] {
    let k1 = format!("bsc:{tag}:{i}:a");
    let o1 = key_to_shard(k1.as_bytes(), SHARDS);
    let k2 = (0..1000)
        .map(|j| format!("bsc:{tag}:{i}:b{j}"))
        .find(|k| key_to_shard(k.as_bytes(), SHARDS) != o1)
        .expect("a second shard among 1000 candidates");
    let o2 = key_to_shard(k2.as_bytes(), SHARDS);
    let k3 = (0..1000)
        .map(|j| format!("bsc:{tag}:{i}:c{j}"))
        .find(|k| {
            let o = key_to_shard(k.as_bytes(), SHARDS);
            o != o1 && o != o2
        })
        .expect("a third shard among 1000 candidates");
    [k1, k2, k3]
}

fn subst(argv: &[&str], keys: &[String; 3]) -> Vec<String> {
    argv.iter()
        .map(|a| {
            a.replace("{1}", &keys[0])
                .replace("{2}", &keys[1])
                .replace("{3}", &keys[2])
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    List,
    Zset,
}

fn contents(c: &mut Conn, kind: Kind, key: &str) -> String {
    match kind {
        Kind::List => canon(&c.send(&["LRANGE", key, "0", "-1"])),
        Kind::Zset => canon(&c.send(&["ZRANGE", key, "0", "-1"])),
    }
}

fn card(c: &mut Conn, kind: Kind, key: &str) -> usize {
    let r = match kind {
        Kind::List => canon(&c.send(&["LLEN", key])),
        Kind::Zset => canon(&c.send(&["ZCARD", key])),
    };
    r.parse().unwrap_or_else(|_| panic!("card({key}) = {r}"))
}

fn push_one(c: &mut Conn, kind: Kind, key: &str, member: &str) {
    let r = match kind {
        Kind::List => canon(&c.send(&["RPUSH", key, member])),
        Kind::Zset => canon(&c.send(&["ZADD", key, "1", member])),
    };
    assert!(!r.starts_with('!'), "push to {key} failed: {r}");
}

fn blocked_clients(admin: &mut Conn) -> usize {
    let info = admin.send(&["INFO", "clients"]);
    info.lines()
        .find_map(|l| l.strip_prefix("blocked_clients:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

/// Poll until `INFO clients` reports exactly `want` blocked registrations.
fn await_blocked(admin: &mut Conn, want: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let n = blocked_clients(admin);
        if n == want {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: blocked_clients stuck at {n}, want {want}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Distinct owner shards of `keys` — one blocked registration each.
fn owners(keys: &[String]) -> usize {
    let mut o: Vec<usize> = keys
        .iter()
        .map(|k| key_to_shard(k.as_bytes(), SHARDS))
        .collect();
    o.sort_unstable();
    o.dedup();
    o.len()
}

// ---------------------------------------------------------------------------
// moon#1019 — immediate path: argument order, exactly once, -WRONGTYPE
// ---------------------------------------------------------------------------

struct Row {
    label: &'static str,
    kind: Kind,
    argv: &'static [&'static str],
}

const SPANNING_ROWS: &[Row] = &[
    Row {
        label: "BLPOP",
        kind: Kind::List,
        argv: &["BLPOP", "{1}", "{2}", "{3}", "0.3"],
    },
    Row {
        label: "BRPOP",
        kind: Kind::List,
        argv: &["BRPOP", "{1}", "{2}", "{3}", "0.3"],
    },
    Row {
        label: "BZPOPMIN",
        kind: Kind::Zset,
        argv: &["BZPOPMIN", "{1}", "{2}", "{3}", "0.3"],
    },
    Row {
        label: "BZPOPMAX",
        kind: Kind::Zset,
        argv: &["BZPOPMAX", "{1}", "{2}", "{3}", "0.3"],
    },
];

/// Which of the three keys hold data before the probe.
#[derive(Clone, Copy, Debug)]
enum Seed {
    /// `{1}` empty, `{2}` and `{3}` non-empty — the issue's shape.
    SecondAndThird,
    /// All three non-empty: the first key must answer.
    All,
    /// Only `{3}`: the LAST key answers, nothing else is touched.
    ThirdOnly,
}

fn seed(c: &mut Conn, kind: Kind, keys: &[String; 3], seed: Seed) {
    for k in keys {
        let _ = c.send(&["DEL", k]);
    }
    let which: &[usize] = match seed {
        Seed::SecondAndThird => &[1, 2],
        Seed::All => &[0, 1, 2],
        Seed::ThirdOnly => &[2],
    };
    for &i in which {
        let tag = ["A", "B", "C"][i];
        let r = match kind {
            Kind::List => {
                canon(&c.send(&["RPUSH", &keys[i], &format!("{tag}1"), &format!("{tag}2")]))
            }
            Kind::Zset => canon(&c.send(&[
                "ZADD",
                &keys[i],
                "1",
                &format!("{tag}1"),
                "2",
                &format!("{tag}2"),
            ])),
        };
        assert!(!r.starts_with('!'), "seed failed: {r}");
    }
}

/// The redis 8.6.1 answer for `row` under `seed`, and the three keys after it.
fn expected(row: &Row, keys: &[String; 3], s: Seed) -> (String, [String; 3]) {
    let first = match s {
        Seed::SecondAndThird => 1,
        Seed::All => 0,
        Seed::ThirdOnly => 2,
    };
    let tag = ["A", "B", "C"][first];
    let (reply, left) = match row.label {
        "BLPOP" => (format!("[{},{tag}1]", keys[first]), format!("[{tag}2]")),
        "BRPOP" => (format!("[{},{tag}2]", keys[first]), format!("[{tag}1]")),
        "BZPOPMIN" => (format!("[{},{tag}1,1]", keys[first]), format!("[{tag}2]")),
        "BZPOPMAX" => (format!("[{},{tag}2,2]", keys[first]), format!("[{tag}1]")),
        other => panic!("no oracle for {other}"),
    };
    let mut after: [String; 3] = std::array::from_fn(|i| {
        let t = ["A", "B", "C"][i];
        let seeded = match s {
            Seed::SecondAndThird => i >= 1,
            Seed::All => true,
            Seed::ThirdOnly => i == 2,
        };
        if seeded {
            format!("[{t}1,{t}2]")
        } else {
            "[]".to_string()
        }
    });
    after[first] = left;
    (reply, after)
}

/// Keys on three different shards: every row answers exactly as standalone
/// redis does — the first non-empty key in argument order, and ONLY that key
/// loses an element.
#[test]
fn bsc1_spanning_immediate_pop_answers_like_redis() {
    let m = spawn_moon(SHARDS, None);
    let mut wrong = Vec::new();
    let mut total = 0usize;
    for i in 0..(SHARDS * 4) {
        let keys = spanning_three("imm", i);
        for s in [Seed::SecondAndThird, Seed::All, Seed::ThirdOnly] {
            for row in SPANNING_ROWS {
                total += 1;
                let mut admin = Conn::open(m.port);
                seed(&mut admin, row.kind, &keys, s);
                let reply = {
                    let mut probe = Conn::open(m.port);
                    send(&mut probe, &subst(row.argv, &keys))
                };
                let (want, want_after) = expected(row, &keys, s);
                let after: [String; 3] =
                    std::array::from_fn(|j| contents(&mut admin, row.kind, &keys[j]));
                if reply != want || after != want_after {
                    // Keys whose contents differ from redis's. A right reply
                    // with any difference is an extra pop; a wrong reply
                    // differs in two keys (the one that should have served,
                    // and the one that did), so a third is an extra pop too.
                    let diffs: usize = after
                        .iter()
                        .zip(&want_after)
                        .filter(|(a, w)| a != w)
                        .count();
                    let extra = if reply == want { diffs > 0 } else { diffs > 2 };
                    wrong.push(format!(
                        "  {:<8} {s:?} owners=[{} {} {}]: reply={reply} (want {want}) \
                         keys={after:?} (want {want_after:?}){}",
                        row.label,
                        key_to_shard(keys[0].as_bytes(), SHARDS),
                        key_to_shard(keys[1].as_bytes(), SHARDS),
                        key_to_shard(keys[2].as_bytes(), SHARDS),
                        match (reply != want, extra) {
                            (true, true) => "  <-- WRONG KEY + EXTRA POP",
                            (true, false) => "  <-- WRONG KEY",
                            (false, _) => "  <-- EXTRA POP (element destroyed)",
                        }
                    ));
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {total} spanning blocking pops at --shards {SHARDS} differ from redis 8.6.1 \
         (moon#1019):\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// Redis answers `-WRONGTYPE` for the first existing key of the wrong type,
/// even when a LATER key could serve — across shards too.
#[test]
fn bsc2_spanning_wrong_type_is_answered_like_redis() {
    let m = spawn_moon(SHARDS, None);
    let mut wrong = Vec::new();
    for i in 0..(SHARDS * 2) {
        let keys = spanning_three("wt", i);
        for (argv, kind) in [
            (&["BLPOP", "{1}", "{2}", "{3}", "0.3"][..], Kind::List),
            (&["BZPOPMIN", "{1}", "{2}", "{3}", "0.3"][..], Kind::Zset),
        ] {
            // `{1}` empty, `{2}` a string, `{3}` has data: redis says WRONGTYPE.
            let mut admin = Conn::open(m.port);
            for k in &keys {
                let _ = admin.send(&["DEL", k]);
            }
            let _ = admin.send(&["SET", &keys[1], "x"]);
            push_one(&mut admin, kind, &keys[2], "C1");
            let reply = {
                let mut probe = Conn::open(m.port);
                send(&mut probe, &subst(argv, &keys))
            };
            let k3 = contents(&mut admin, kind, &keys[2]);
            if !reply.starts_with("!WRONGTYPE") || k3 != "[C1]" {
                wrong.push(format!(
                    "  {} owners=[{} {} {}]: reply={reply} (want !WRONGTYPE) {}={k3} (want [C1])",
                    argv[0],
                    key_to_shard(keys[0].as_bytes(), SHARDS),
                    key_to_shard(keys[1].as_bytes(), SHARDS),
                    key_to_shard(keys[2].as_bytes(), SHARDS),
                    keys[2]
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "spanning blocking pops skipped redis's type ladder:\n{}",
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// moon#1019 — wake path: concurrent pushes to several owners, conservation
// ---------------------------------------------------------------------------

/// A client blocks on three keys owned by three shards; three other clients
/// push one element to each key at the same instant. Exactly one element may
/// leave the keyspace — the one in the waiter's reply. Before the fix two or
/// three owners each served the waiter and every reply but one was dropped.
#[test]
fn bsc3_spanning_concurrent_pushes_conserve_every_element() {
    const ITERS: usize = 40;
    let m = spawn_moon(SHARDS, None);
    let mut admin = Conn::open(m.port);
    let mut lost = Vec::new();
    let mut runs = 0usize;
    for (cmd, kind) in [("BLPOP", Kind::List), ("BZPOPMIN", Kind::Zset)] {
        for it in 0..ITERS {
            runs += 1;
            let keys = spanning_three(&format!("wake{cmd}"), it);
            for k in &keys {
                let _ = admin.send(&["DEL", k]);
            }
            await_blocked(&mut admin, 0, "before block");
            let argv: Vec<String> = vec![
                cmd.to_string(),
                keys[0].clone(),
                keys[1].clone(),
                keys[2].clone(),
                "5".to_string(),
            ];
            let port = m.port;
            let waiter = std::thread::spawn(move || {
                let mut probe = Conn::open(port);
                send(&mut probe, &argv)
            });
            await_blocked(&mut admin, owners(&keys), "waiter registration");

            let barrier = Arc::new(Barrier::new(keys.len()));
            let pushers: Vec<_> = keys
                .iter()
                .enumerate()
                .map(|(i, k)| {
                    let k = k.clone();
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let mut c = Conn::open(port);
                        barrier.wait();
                        push_one(&mut c, kind, &k, &format!("v{i}"));
                    })
                })
                .collect();
            for p in pushers {
                p.join().expect("pusher");
            }
            let reply = waiter.join().expect("waiter");
            let popped = usize::from(reply != "nil" && !reply.starts_with('!'));
            let left: usize = keys.iter().map(|k| card(&mut admin, kind, k)).sum();
            // `popped == 1` too: a waiter that was never served would conserve
            // every element trivially.
            if popped != 1 || popped + left != keys.len() {
                lost.push(format!(
                    "  {cmd} iter {it}: pushed {} = popped {popped} + left {left}?  reply={reply}  \
                     <-- {} ELEMENT(S) DESTROYED",
                    keys.len(),
                    keys.len() as isize - (popped + left) as isize
                ));
            }
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {runs} spanning wakes lost elements (moon#1019):\n{}",
        lost.len(),
        lost.join("\n")
    );
}

// ---------------------------------------------------------------------------
// moon#1023 — a serve racing the end of the wait
// ---------------------------------------------------------------------------

/// Window the server holds open between a wait's end and its settle.
const SETTLE_MS: u64 = 400;
/// The blocking timeout used below: the wait ends here, the window opens.
const TIMEOUT: &str = "0.1";
/// When the push lands: after the timeout fired, inside the window.
const PUSH_AT: Duration = Duration::from_millis(250);

/// A wait that TIMES OUT while an owner serves it: the element must either
/// reach the client (redis answers a client served before its timeout was
/// observed) or stay in the key — never neither.
#[test]
fn bsc4_timeout_racing_a_serve_keeps_the_element() {
    let m = spawn_moon(SHARDS, Some(SETTLE_MS));
    let mut admin = Conn::open(m.port);
    let mut lost = Vec::new();
    let mut runs = 0usize;
    let mut served_in_window = 0usize;
    let mut cases: Vec<(String, Vec<String>, String)> = Vec::new();
    for owner in 0..SHARDS {
        for rep in 0..2 {
            let k = key_owned_by("to1", owner, rep);
            cases.push((format!("single owner={owner}"), vec![k.clone()], k));
        }
    }
    for i in 0..(SHARDS * 2) {
        let keys = spanning_three("to3", i);
        cases.push((
            format!(
                "spanning owners=[{} {} {}]",
                key_to_shard(keys[0].as_bytes(), SHARDS),
                key_to_shard(keys[1].as_bytes(), SHARDS),
                key_to_shard(keys[2].as_bytes(), SHARDS)
            ),
            keys.to_vec(),
            keys[1].clone(),
        ));
    }
    for (label, keys, push_key) in cases {
        runs += 1;
        for k in &keys {
            let _ = admin.send(&["DEL", k]);
        }
        await_blocked(&mut admin, 0, "before block");
        let mut argv = vec!["BLPOP".to_string()];
        argv.extend(keys.iter().cloned());
        argv.push(TIMEOUT.to_string());
        let port = m.port;
        let started = Instant::now();
        let waiter = std::thread::spawn(move || {
            let mut probe = Conn::open(port);
            send(&mut probe, &argv)
        });
        std::thread::sleep(PUSH_AT.saturating_sub(started.elapsed()));
        push_one(&mut admin, Kind::List, &push_key, "v");
        let reply = waiter.join().expect("waiter");
        let popped = usize::from(reply != "nil");
        let left = card(&mut admin, Kind::List, &push_key);
        served_in_window += popped;
        if popped + left != 1 {
            lost.push(format!(
                "  {label}: pushed 1 = popped {popped} + left {left}?  reply={reply}  \
                 <-- ELEMENT DESTROYED"
            ));
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {runs} timed-out waits destroyed a racing serve (moon#1023):\n{}",
        lost.len(),
        lost.join("\n")
    );
    // Non-vacuity: the push must really have landed inside the window, while
    // a remote owner could still serve. If the hook were not honoured every
    // push would arrive after the wait settled and this test would pass
    // without exercising anything. At least SHARDS-1 of the single-key owners
    // are remote to wherever the connection landed, twice each.
    assert!(
        served_in_window >= 2 * (SHARDS - 1),
        "only {served_in_window} of {runs} racing serves were delivered — the settle window \
         was not exercised"
    );
}

/// A wait whose client DISCONNECTS while an owner serves it: nobody can
/// receive the element, so it must be back in the key.
#[test]
fn bsc5_disconnect_racing_a_serve_restores_the_element() {
    let m = spawn_moon(SHARDS, Some(SETTLE_MS));
    let mut admin = Conn::open(m.port);
    let mut lost = Vec::new();
    let mut runs = 0usize;
    let mut cases: Vec<(String, Vec<String>, String)> = Vec::new();
    for owner in 0..SHARDS {
        for rep in 0..2 {
            let k = key_owned_by("pg1", owner, rep);
            cases.push((format!("single owner={owner}"), vec![k.clone()], k));
        }
    }
    for i in 0..(SHARDS * 2) {
        let keys = spanning_three("pg3", i);
        cases.push((format!("spanning #{i}"), keys.to_vec(), keys[1].clone()));
    }
    for (label, keys, push_key) in cases {
        runs += 1;
        for k in &keys {
            let _ = admin.send(&["DEL", k]);
        }
        await_blocked(&mut admin, 0, "before block");
        let mut argv: Vec<&str> = vec!["BLPOP"];
        argv.extend(keys.iter().map(String::as_str));
        argv.push("0");
        let mut sock = TcpStream::connect(("127.0.0.1", m.port)).expect("connect");
        sock.write_all(&common::encode(&argv)).expect("write");
        await_blocked(&mut admin, owners(&keys), "waiter registration");
        // The peer goes away; the server notices, and holds the window open.
        let _ = sock.shutdown(std::net::Shutdown::Both);
        drop(sock);
        std::thread::sleep(Duration::from_millis(150));
        push_one(&mut admin, Kind::List, &push_key, "v");
        std::thread::sleep(Duration::from_millis(SETTLE_MS + 400));
        let left = card(&mut admin, Kind::List, &push_key);
        if left != 1 {
            lost.push(format!(
                "  {label}: pushed 1, nobody received it, left {left}  <-- ELEMENT DESTROYED"
            ));
        }
    }
    assert!(
        lost.is_empty(),
        "{} of {runs} disconnected waits destroyed a racing serve (moon#1023):\n{}",
        lost.len(),
        lost.join("\n")
    );
}

// ---------------------------------------------------------------------------
// The review P3 on moon#989 — one push, several elements, several waiters
// ---------------------------------------------------------------------------

/// Two clients parked on one key; one `RPUSH` delivers two elements. Redis
/// serves BOTH waiters from that push. The wakers used to answer exactly one
/// waiter per push, leaving the second parked next to an element it could
/// have had until its timeout.
#[test]
fn bsc6_one_push_serves_every_waiter_its_elements_cover() {
    let m = spawn_moon(SHARDS, None);
    let mut admin = Conn::open(m.port);
    let mut wrong = Vec::new();
    for owner in 0..SHARDS {
        let k = key_owned_by("multi", owner, 0);
        let _ = admin.send(&["DEL", &k]);
        await_blocked(&mut admin, 0, "before block");
        let port = m.port;
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let k = k.clone();
                std::thread::spawn(move || {
                    let mut probe = Conn::open(port);
                    let started = Instant::now();
                    let reply = send(&mut probe, &["BLPOP".to_string(), k, "2".to_string()]);
                    (reply, started.elapsed())
                })
            })
            .collect();
        await_blocked(&mut admin, 2, "two waiters");
        let _ = admin.send(&["RPUSH", &k, "a", "b"]);
        for w in waiters {
            let (reply, took) = w.join().expect("waiter");
            if reply == "nil" || took >= Duration::from_millis(1500) {
                wrong.push(format!(
                    "  owner={owner}: a waiter got {reply} after {took:?} with an element left \
                     in the key"
                ));
            }
        }
        let left = card(&mut admin, Kind::List, &k);
        if left != 0 {
            wrong.push(format!(
                "  owner={owner}: {left} element(s) left beside a parked waiter"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "one push served one waiter:\n{}",
        wrong.join("\n")
    );
}
