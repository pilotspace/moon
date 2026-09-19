//! A multi-key blocking pop serves exactly ONE element, from the first
//! non-empty key in argument order — at every shard count (moon#989).
//!
//! ## The defect
//!
//! `BLMPOP`/`BZMPOP` (and the rest of the multi-key blocking-pop family —
//! `BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`) popped an element from a key they
//! did not answer with. The reply was right; a second key silently lost its
//! head element, which no client ever received:
//!
//! ```text
//! BLMPOP 0.3 3 {t}a {t}b {t}c LEFT     ({t}a empty, {t}b=[B1 B2], {t}c=[C1 C2])
//! redis 8.6.1     -> {t}b [B1]    {t}b=[B2]   {t}c=[C1 C2]
//! moon --shards 4 -> {t}b [B1]    {t}b=[B2]   {t}c=[C2]      <-- C1 destroyed
//! ```
//!
//! The keys are co-located under one `{hash}` tag, so this is not routing. It
//! happens whenever the CLIENT's connection lives on a different shard than
//! the keys. The client's own pre-block scan can only see keys its shard owns,
//! so it found nothing and fell through to the multi-key coordinator, which
//! sent one `BlockRegister` per key to the owner. The owner handled each
//! message in isolation: register the key, see data, serve the waiter. `{t}b`
//! served `B1`; then `{t}c`'s registration arrived, found data, and served the
//! SAME waiter again with `C1`. The client took the first reply and dropped
//! the second, with `C1` already gone from the keyspace.
//!
//! With keys on several shards the same fan-out also popped the WRONG key: the
//! local scan skipped a remote non-empty key and served a later local one.
//!
//! ## Why these placements cannot pass vacuously
//!
//! Whether a probe exercises the bug depends on which shard its CONNECTION
//! landed on, which a test cannot choose — on macOS every connection lands on
//! one shard, on Linux the kernel's `SO_REUSEPORT` hash decides. So instead of
//! sampling, every co-located row is run for tags constructed (with the
//! server's own `key_to_shard`) to be owned by EVERY shard in turn. Wherever
//! the connections land, at least `SHARDS - 1` of the owners are remote to
//! them. The issue's own sweep hit a first-placement-clean run exactly because
//! it did not do this.
//!
//! ## The contract asserted
//!
//! * co-located keys (`{hash}` tag) at `--shards 4`: the reply AND the keyspace
//!   are byte-identical to redis 8.6.1 / `--shards 1`;
//! * spanning `BLMPOP`/`BZMPOP`: either the redis answer with only the answered
//!   key touched, or an error with NOTHING touched — never a pop from a key the
//!   reply does not name (the same answer contract as moon#962's `LMPOP`);
//! * `--shards 1`: every row answers exactly as redis does (control).

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

const SHARDS: usize = 4;
/// Placements per owner shard. Two, not one: the issue observed that the first
/// placement after start can be clean on its own.
const PER_OWNER: usize = 2;

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

fn spawn_moon(shards: usize) -> Moon {
    // `CARGO_BIN_EXE_moon` is the binary cargo built for THIS test run; the
    // `target/release/moon` fallback has unknown provenance.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-bmk-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
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
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-bmk-{port}"));
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

/// Render a RESP reply as stable text: `+OK` -> `OK`, `:3` -> `3`, a bulk ->
/// its bytes, an array -> `[a,b]`, a null -> `nil`, any error -> `!<text>`.
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

/// `{hash}`-tagged key names `{bmk:<tag>:<n>}:1..=3` whose tag is owned by
/// shard `owner`. Found by search with the server's own routing function, so
/// the set of owners a test covers is a fact, not a hope.
fn colocated_owned_by(tag: &str, owner: usize, nth: usize) -> [String; 3] {
    let mut found = 0usize;
    for n in 0..10_000 {
        let hash = format!("bmk:{tag}:{n}");
        if key_to_shard(hash.as_bytes(), SHARDS) != owner {
            continue;
        }
        if found == nth {
            let k = |j: u8| format!("{{{hash}}}:{j}");
            let keys = [k(1), k(2), k(3)];
            for key in &keys {
                assert_eq!(
                    key_to_shard(key.as_bytes(), SHARDS),
                    owner,
                    "a hash-tagged key must route by its tag"
                );
            }
            return keys;
        }
        found += 1;
    }
    panic!("no tag owned by shard {owner} among 10000 candidates");
}

/// Three untagged key names on three DIFFERENT shards.
fn spanning_three(tag: &str, i: usize) -> [String; 3] {
    let k1 = format!("bmk:{tag}:{i}:a");
    let o1 = key_to_shard(k1.as_bytes(), SHARDS);
    let k2 = (0..1000)
        .map(|j| format!("bmk:{tag}:{i}:b{j}"))
        .find(|k| key_to_shard(k.as_bytes(), SHARDS) != o1)
        .expect("a second shard among 1000 candidates");
    let o2 = key_to_shard(k2.as_bytes(), SHARDS);
    let k3 = (0..1000)
        .map(|j| format!("bmk:{tag}:{i}:c{j}"))
        .find(|k| {
            let o = key_to_shard(k.as_bytes(), SHARDS);
            o != o1 && o != o2
        })
        .expect("a third shard among 1000 candidates");
    [k1, k2, k3]
}

// ---------------------------------------------------------------------------
// The family, as rows
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    List,
    Zset,
}

/// One blocking pop, parameterised over its three key names. `{1}` `{2}` `{3}`
/// are substituted.
struct Row {
    label: &'static str,
    kind: Kind,
    argv: &'static [&'static str],
    /// The redis 8.6.1 reply when `{1}` is empty and `{2}`, `{3}` hold
    /// `[B1 B2]` / `[C1 C2]` (zsets: `B1`=1 `B2`=2, `C1`=1 `C2`=2).
    expect: &'static str,
    /// `{2}` after that reply.
    k2_after: &'static str,
}

const ROWS: &[Row] = &[
    Row {
        label: "BLMPOP LEFT",
        kind: Kind::List,
        argv: &["BLMPOP", "0.3", "3", "{1}", "{2}", "{3}", "LEFT"],
        expect: "[{2},[B1]]",
        k2_after: "[B2]",
    },
    Row {
        label: "BLMPOP RIGHT COUNT 5",
        kind: Kind::List,
        argv: &[
            "BLMPOP", "0.3", "3", "{1}", "{2}", "{3}", "RIGHT", "COUNT", "5",
        ],
        expect: "[{2},[B2,B1]]",
        k2_after: "[]",
    },
    Row {
        label: "BZMPOP MIN",
        kind: Kind::Zset,
        argv: &["BZMPOP", "0.3", "3", "{1}", "{2}", "{3}", "MIN"],
        expect: "[{2},[[B1,1]]]",
        k2_after: "[B2]",
    },
    Row {
        label: "BZMPOP MAX COUNT 5",
        kind: Kind::Zset,
        argv: &[
            "BZMPOP", "0.3", "3", "{1}", "{2}", "{3}", "MAX", "COUNT", "5",
        ],
        expect: "[{2},[[B2,2],[B1,1]]]",
        k2_after: "[]",
    },
    Row {
        label: "BLPOP",
        kind: Kind::List,
        argv: &["BLPOP", "{1}", "{2}", "{3}", "0.3"],
        expect: "[{2},B1]",
        k2_after: "[B2]",
    },
    Row {
        label: "BRPOP",
        kind: Kind::List,
        argv: &["BRPOP", "{1}", "{2}", "{3}", "0.3"],
        expect: "[{2},B2]",
        k2_after: "[B1]",
    },
    Row {
        label: "BZPOPMIN",
        kind: Kind::Zset,
        argv: &["BZPOPMIN", "{1}", "{2}", "{3}", "0.3"],
        expect: "[{2},B1,1]",
        k2_after: "[B2]",
    },
    Row {
        label: "BZPOPMAX",
        kind: Kind::Zset,
        argv: &["BZPOPMAX", "{1}", "{2}", "{3}", "0.3"],
        expect: "[{2},B2,2]",
        k2_after: "[B1]",
    },
    // The same key named twice: one pop, not two.
    Row {
        label: "BLMPOP same key twice",
        kind: Kind::List,
        argv: &["BLMPOP", "0.3", "3", "{1}", "{2}", "{2}", "LEFT"],
        expect: "[{2},[B1]]",
        k2_after: "[B2]",
    },
    Row {
        label: "BLPOP same key twice",
        kind: Kind::List,
        argv: &["BLPOP", "{2}", "{2}", "0.3"],
        expect: "[{2},B1]",
        k2_after: "[B2]",
    },
];

fn subst(argv: &[&str], keys: &[String; 3]) -> Vec<String> {
    argv.iter()
        .map(|a| {
            a.replace("{1}", &keys[0])
                .replace("{2}", &keys[1])
                .replace("{3}", &keys[2])
        })
        .collect()
}

fn send(c: &mut Conn, argv: &[String]) -> String {
    let refs: Vec<&str> = argv.iter().map(String::as_str).collect();
    canon(&c.send(&refs))
}

fn seed(c: &mut Conn, kind: Kind, keys: &[String; 3]) {
    for k in keys {
        let _ = c.send(&["DEL", k]);
    }
    let cmds: [Vec<&str>; 2] = match kind {
        Kind::List => [
            vec!["RPUSH", &keys[1], "B1", "B2"],
            vec!["RPUSH", &keys[2], "C1", "C2"],
        ],
        Kind::Zset => [
            vec!["ZADD", &keys[1], "1", "B1", "2", "B2"],
            vec!["ZADD", &keys[2], "1", "C1", "2", "C2"],
        ],
    };
    for cmd in &cmds {
        let r = canon(&c.send(cmd));
        assert!(!r.starts_with('!'), "seeding {cmd:?} failed: {r}");
    }
}

fn contents(c: &mut Conn, kind: Kind, key: &str) -> String {
    match kind {
        Kind::List => canon(&c.send(&["LRANGE", key, "0", "-1"])),
        Kind::Zset => canon(&c.send(&["ZRANGE", key, "0", "-1"])),
    }
}

/// Run every row against `keys`, the blocking probe on a FRESH connection.
/// Returns one line per row whose reply or keyspace differs from redis.
fn run_rows_exact(port: u16, keys: &[String; 3], wrong: &mut Vec<String>) {
    let mut admin = Conn::open(port);
    for row in ROWS {
        seed(&mut admin, row.kind, keys);
        let argv = subst(row.argv, keys);
        let reply = {
            let mut probe = Conn::open(port);
            send(&mut probe, &argv)
        };
        let expect = row.expect.replace("{2}", &keys[1]);
        let k1 = contents(&mut admin, row.kind, &keys[0]);
        let k2 = contents(&mut admin, row.kind, &keys[1]);
        let k3 = contents(&mut admin, row.kind, &keys[2]);
        if reply != expect || k1 != "[]" || k2 != row.k2_after || k3 != "[C1,C2]" {
            wrong.push(format!(
                "  {:<22} owner={} reply={reply} (want {expect}) {}={k2} (want {}) {}={k3} \
                 (want [C1,C2]){}",
                row.label,
                key_to_shard(keys[1].as_bytes(), SHARDS),
                keys[1],
                row.k2_after,
                keys[2],
                if k3 != "[C1,C2]" && reply.contains(&keys[1]) {
                    "  <-- ELEMENT DESTROYED: popped from a key the reply does not name"
                } else {
                    ""
                }
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The issue's shape, for every member of the family and every owner shard.
#[test]
fn bmk1_colocated_multikey_blocking_pop_serves_exactly_once() {
    let m = spawn_moon(SHARDS);
    let mut wrong = Vec::new();
    for owner in 0..SHARDS {
        for nth in 0..PER_OWNER {
            let keys = colocated_owned_by("imm", owner, nth);
            run_rows_exact(m.port, &keys, &mut wrong);
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} co-located multi-key blocking pops at --shards {SHARDS} differ from \
         redis 8.6.1 (moon#989):\n{}",
        wrong.len(),
        SHARDS * PER_OWNER * ROWS.len(),
        wrong.join("\n")
    );
}

/// Control: the same rows at `--shards 1`, where the defect never existed.
/// A fix that broke the family outright fails here.
#[test]
fn bmk2_single_shard_control_every_row_answers_like_redis() {
    let m = spawn_moon(1);
    let mut wrong = Vec::new();
    for owner in 0..SHARDS {
        let keys = colocated_owned_by("s1", owner, 0);
        run_rows_exact(m.port, &keys, &mut wrong);
        let keys = spanning_three("s1", owner);
        run_rows_exact(m.port, &keys, &mut wrong);
    }
    assert!(
        wrong.is_empty(),
        "--shards 1 no longer answers like redis:\n{}",
        wrong.join("\n")
    );
}

/// Redis type-checks each key in argument order and answers `-WRONGTYPE` for
/// the first existing key of the wrong type, even when a LATER key could
/// serve. Co-located keys owned by another shard used to skip the check (the
/// owner could not tell one key of several from the whole command) and pop
/// the later key instead.
#[test]
fn bmk3_colocated_wrong_type_is_answered_like_redis() {
    let m = spawn_moon(SHARDS);
    let mut wrong = Vec::new();
    let cases: &[(&[&str], Kind)] = &[
        (&["BLMPOP", "0.3", "2", "{1}", "{2}", "LEFT"], Kind::List),
        (&["BZMPOP", "0.3", "2", "{1}", "{2}", "MIN"], Kind::Zset),
        (&["BLPOP", "{3}", "{1}", "{2}", "0.3"], Kind::List),
        (&["BZPOPMIN", "{3}", "{1}", "{2}", "0.3"], Kind::Zset),
    ];
    for owner in 0..SHARDS {
        for nth in 0..PER_OWNER {
            let keys = colocated_owned_by("wt", owner, nth);
            for (argv, kind) in cases {
                let mut admin = Conn::open(m.port);
                for k in &keys {
                    let _ = admin.send(&["DEL", k]);
                }
                let _ = admin.send(&["SET", &keys[0], "x"]);
                let _ = match kind {
                    Kind::List => admin.send(&["RPUSH", &keys[1], "B1"]),
                    Kind::Zset => admin.send(&["ZADD", &keys[1], "1", "B1"]),
                };
                let reply = {
                    let mut probe = Conn::open(m.port);
                    send(&mut probe, &subst(argv, &keys))
                };
                let k2 = contents(&mut admin, *kind, &keys[1]);
                if !reply.starts_with("!WRONGTYPE") || k2 != "[B1]" {
                    wrong.push(format!(
                        "  {} owner={owner}: reply={reply} (want !WRONGTYPE ...) {}={k2} (want [B1])",
                        argv[0], keys[1]
                    ));
                }
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "co-located blocking pops skipped redis's type check:\n{}",
        wrong.join("\n")
    );
}

/// Block first, then push: the waiter is woken by the first key to receive
/// data and served exactly once. A control for the registration protocol —
/// the wake path must still find a waiter registered by the owner shard.
#[test]
fn bmk4_colocated_block_then_wake_serves_once() {
    let m = spawn_moon(SHARDS);
    let mut wrong = Vec::new();
    for owner in 0..SHARDS {
        let keys = colocated_owned_by("wake", owner, 0);
        for (label, argv, kind, expect) in [
            (
                "BLMPOP",
                vec!["BLMPOP", "5", "3", "{1}", "{2}", "{3}", "LEFT"],
                Kind::List,
                "[{2},[B1]]",
            ),
            (
                "BZMPOP",
                vec!["BZMPOP", "5", "3", "{1}", "{2}", "{3}", "MIN"],
                Kind::Zset,
                "[{2},[[B1,1]]]",
            ),
            (
                "BLPOP",
                vec!["BLPOP", "{1}", "{2}", "{3}", "5"],
                Kind::List,
                "[{2},B1]",
            ),
        ] {
            let mut admin = Conn::open(m.port);
            for k in &keys {
                let _ = admin.send(&["DEL", k]);
            }
            let argv = subst(&argv, &keys);
            let port = m.port;
            let waiter = std::thread::spawn(move || {
                let mut probe = Conn::open(port);
                send(&mut probe, &argv)
            });
            // Registered = counted in `blocked_clients`. Polling beats a sleep:
            // a slow box cannot turn this into a push-before-block race.
            let deadline = Instant::now() + Duration::from_secs(4);
            loop {
                let info = admin.send(&["INFO", "clients"]);
                if info.contains("blocked_clients:1") {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "{label}: waiter never registered: {info}"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            let (push2, push3): (Vec<&str>, Vec<&str>) = match kind {
                Kind::List => (
                    vec!["RPUSH", &keys[1], "B1", "B2"],
                    vec!["RPUSH", &keys[2], "C1", "C2"],
                ),
                Kind::Zset => (
                    vec!["ZADD", &keys[1], "1", "B1", "2", "B2"],
                    vec!["ZADD", &keys[2], "1", "C1", "2", "C2"],
                ),
            };
            let _ = admin.send(&push2);
            let _ = admin.send(&push3);
            let reply = waiter.join().expect("waiter thread");
            let expect = expect.replace("{2}", &keys[1]);
            let k2 = contents(&mut admin, kind, &keys[1]);
            let k3 = contents(&mut admin, kind, &keys[2]);
            if reply != expect || k2 != "[B2]" || k3 != "[C1,C2]" {
                wrong.push(format!(
                    "  {label} owner={owner}: reply={reply} (want {expect}) k2={k2} (want [B2]) \
                     k3={k3} (want [C1,C2])"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "block-then-wake:\n{}", wrong.join("\n"));
}

/// A co-located waiter that TIMES OUT must leave nothing registered on the
/// owner: a later push stays in the key instead of feeding a ghost.
#[test]
fn bmk5_colocated_timeout_leaves_no_ghost_waiter() {
    let m = spawn_moon(SHARDS);
    let mut wrong = Vec::new();
    for owner in 0..SHARDS {
        let keys = colocated_owned_by("ghost", owner, 0);
        let mut admin = Conn::open(m.port);
        for k in &keys {
            let _ = admin.send(&["DEL", k]);
        }
        let reply = {
            let mut probe = Conn::open(m.port);
            send(
                &mut probe,
                &subst(&["BLMPOP", "0.1", "3", "{1}", "{2}", "{3}", "LEFT"], &keys),
            )
        };
        let _ = admin.send(&["RPUSH", &keys[1], "B1"]);
        let _ = admin.send(&["RPUSH", &keys[2], "C1"]);
        let k2 = contents(&mut admin, Kind::List, &keys[1]);
        let k3 = contents(&mut admin, Kind::List, &keys[2]);
        if reply != "nil" || k2 != "[B1]" || k3 != "[C1]" {
            wrong.push(format!(
                "  owner={owner}: reply={reply} (want nil) k2={k2} k3={k3} (want [B1] [C1])"
            ));
        }
    }
    assert!(wrong.is_empty(), "ghost waiters:\n{}", wrong.join("\n"));
}

/// Keys on three different shards: `BLMPOP`/`BZMPOP` must either answer
/// exactly as redis does, or refuse with the keyspace untouched — never pop a
/// key the reply does not name, and never pop two.
#[test]
fn bmk6_spanning_blmpop_bzmpop_never_pop_an_unanswered_key() {
    let m = spawn_moon(SHARDS);
    let mut wrong = Vec::new();
    for i in 0..(SHARDS * PER_OWNER * 2) {
        let keys = spanning_three("span", i);
        for (label, argv, kind, expect) in [
            (
                "BLMPOP",
                &["BLMPOP", "0.3", "3", "{1}", "{2}", "{3}", "LEFT"][..],
                Kind::List,
                "[{2},[B1]]",
            ),
            (
                "BZMPOP",
                &["BZMPOP", "0.3", "3", "{1}", "{2}", "{3}", "MIN"][..],
                Kind::Zset,
                "[{2},[[B1,1]]]",
            ),
        ] {
            let mut admin = Conn::open(m.port);
            seed(&mut admin, kind, &keys);
            let reply = {
                let mut probe = Conn::open(m.port);
                send(&mut probe, &subst(argv, &keys))
            };
            let k2 = contents(&mut admin, kind, &keys[1]);
            let k3 = contents(&mut admin, kind, &keys[2]);
            let ok = if reply.starts_with('!') {
                k2 == "[B1,B2]" && k3 == "[C1,C2]"
            } else {
                reply == expect.replace("{2}", &keys[1]) && k2 == "[B2]" && k3 == "[C1,C2]"
            };
            if !ok {
                wrong.push(format!(
                    "  {label} [{} | {} | {}]: reply={reply} k2={k2} k3={k3}",
                    keys[0], keys[1], keys[2]
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} spanning BLMPOP/BZMPOP placements popped a key they did not answer with:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

/// Inside MULTI the blocking pop is queued as its non-blocking twin and runs
/// at EXEC. Co-located keys answer exactly like redis; spanning keys follow
/// the same answer contract.
#[test]
fn bmk7_multi_exec_blocking_pop_is_exactly_once() {
    let m = spawn_moon(SHARDS);
    let mut wrong = Vec::new();
    for owner in 0..SHARDS {
        let keys = colocated_owned_by("txn", owner, 0);
        let spanning = spanning_three("txn", owner);
        for (keys, colocated) in [(keys, true), (spanning, false)] {
            let mut c = Conn::open(m.port);
            seed(&mut c, Kind::List, &keys);
            assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
            let q = send(
                &mut c,
                &subst(&["BLMPOP", "0", "3", "{1}", "{2}", "{3}", "LEFT"], &keys),
            );
            let reply = if q.starts_with('!') {
                let _ = c.send(&["DISCARD"]);
                q
            } else {
                canon(&c.send(&["EXEC"]))
            };
            let k2 = contents(&mut c, Kind::List, &keys[1]);
            let k3 = contents(&mut c, Kind::List, &keys[2]);
            let refused = reply.contains('!');
            let ok = if refused {
                !colocated && k2 == "[B1,B2]" && k3 == "[C1,C2]"
            } else {
                reply == format!("[[{},[B1]]]", keys[1]) && k2 == "[B2]" && k3 == "[C1,C2]"
            };
            if !ok {
                wrong.push(format!(
                    "  colocated={colocated} owner={owner}: reply={reply} k2={k2} k3={k3}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "MULTI/EXEC:\n{}", wrong.join("\n"));
}
