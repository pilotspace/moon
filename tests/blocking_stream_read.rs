//! `XREAD` / `XREADGROUP` with `BLOCK` must actually block, and must be woken
//! by a concurrent `XADD` (moon#595) — plus `XREAD`'s reply must omit a stream
//! that had nothing to give (moon#594).
//!
//! Before #595 `BLOCK` was parsed and discarded: both readers stepped over the
//! token and its value with an `// ignored` comment. Measured against
//! redis-server 8.6.1 and against moon, raw socket, 2026-08-20:
//!
//! ```text
//! XADD bt 1-1 f v ; XREAD BLOCK 2000 STREAMS bt $
//!   redis 8.6.1 :  *-1  after 2.044s   <- waited the full budget
//!   moon        :  *-1  after 0.000s   <- the bug
//!
//! parked XREAD BLOCK 5000 STREAMS blk $ , then XADD blk 2-1 g w at t=600ms
//!   redis 8.6.1 :  the entry, at 0.605s
//!   moon        :  *-1, at 0.000s      <- answered before the XADD existed
//! ```
//!
//! Both halves are asserted, because either alone passes on a broken
//! implementation: a server that never blocks passes "was woken by XADD"
//! vacuously if the entry is already there, and a server that blocks but is
//! never woken passes "waits the full budget".
//!
//! ## Why `--shards 4`
//!
//! A connection is pinned to one shard, so ~3 of every 4 keys are owned by
//! ANOTHER shard. The two placements are served by two different code paths —
//! the connection's own pre-registration scan versus the owning shard's
//! `BlockRegister` handler, which is also where `$` is bound and where the
//! post-registration re-check runs. A fix to only one of them passes ~25% of
//! trials, so the placement-sensitive assertions loop over enough distinct
//! keys that both paths are exercised.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards: a key is remote ~75% of the time.
const SHARDS: &str = "4";
/// Distinct keys per placement-sensitive assertion. At p(remote)=0.75 the
/// chance that all 12 land on the connection's own shard — and vacuously test
/// one path — is under 1e-7.
const TRIALS: usize = 12;

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
    // `CARGO_BIN_EXE_moon` is the binary cargo built for THIS test run;
    // `common::find_moon_binary()` would fall back to `target/release/moon`,
    // whose provenance is unknown — a stale one turns a real failure green.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-bsr-{port}"));
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
                // The repo's data volume hovers near the diskfull guard's 5%
                // threshold; without this the server refuses writes and every
                // assertion below fails for an unrelated reason.
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-bsr-{port}"));
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

/// Send one command on a fresh connection and report how long the reply took.
///
/// The elapsed time is the whole point for this suite: the pre-#595 bug is
/// invisible in the reply BYTES (`*-1` is exactly what a legitimate timeout
/// answers) and shows up only as 0.000 s where Redis takes the full budget.
fn timed(port: u16, argv: &[&str]) -> (Duration, String) {
    let mut c = Conn::open(port);
    let start = Instant::now();
    let reply = c.send(argv);
    (start.elapsed(), reply)
}

/// Park `argv` on its own connection and hand back a join handle. The thread
/// is already blocked in `read` by the time `settle` has elapsed.
fn park(port: u16, argv: &[&str]) -> std::thread::JoinHandle<(Duration, String)> {
    let owned: Vec<String> = argv.iter().map(|s| (*s).to_string()).collect();
    std::thread::spawn(move || {
        let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
        timed(port, &refs)
    })
}

/// How long a parked reader is given to reach its blocked state before the
/// waking write is issued. Generous on purpose: too short and the write races
/// ahead of the registration, which the server must ALSO handle (the
/// post-registration re-check) but which would test a different path than the
/// one this suite is aimed at.
const SETTLE: Duration = Duration::from_millis(400);

/// Run `body` once per key placement on a FRESH connection and report every
/// trial that came out wrong.
///
/// Fresh connections on purpose: which shard a connection lands on is what
/// decides whether its key is local, so reusing one would sample a single
/// placement `TRIALS` times instead of the distribution. Reporting the COUNT
/// is the diagnosis — roughly 3-in-4 wrong means the remote
/// (registration-time) path, 12-of-12 means both.
#[track_caller]
fn each_trial(
    port: u16,
    tag: &str,
    why: &str,
    mut body: impl FnMut(&mut Conn, &str) -> Option<String>,
) {
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let mut c = Conn::open(port);
        let key = format!("{tag}{i}");
        if let Some(detail) = body(&mut c, &key) {
            wrong.push(format!("  key {key}: {detail}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} key placements wrong ({why}):\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// moon#595 — BLOCK actually blocks
// ---------------------------------------------------------------------------

/// `XREAD BLOCK <ms>` with nothing new waits out its budget before answering
/// the null array, on EVERY key placement.
///
/// The lower bound is what the bug violated (0.000 s). The upper bound guards
/// the opposite failure — a waiter the deadline sweep never reaps.
#[test]
fn bsr1_xread_block_waits_out_its_budget() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bsr1:",
        "moon#595 — XREAD BLOCK must wait, not answer the null array instantly",
        |c, k| {
            assert!(
                c.send(&["XADD", k, "1-1", "f", "v"]).starts_with('$'),
                "XADD must succeed"
            );
            // `$` = "only entries added from now on", so there is nothing to
            // serve and the command must park.
            let start = Instant::now();
            let reply = c.send(&["XREAD", "BLOCK", "700", "STREAMS", k, "$"]);
            let elapsed = start.elapsed();
            if reply != "*-1\r\n" {
                return Some(format!("expected the null array, got {reply:?}"));
            }
            if elapsed < Duration::from_millis(600) {
                return Some(format!(
                    "returned after {elapsed:?} — BLOCK 700 did not block"
                ));
            }
            if elapsed > Duration::from_secs(5) {
                return Some(format!("returned after {elapsed:?} — never reaped"));
            }
            None
        },
    );
}

/// A parked `XREAD BLOCK` is woken by a concurrent `XADD` and receives the
/// entry, on EVERY key placement.
///
/// This is the half the wake path owns. `bsr1` alone would pass on a server
/// that blocks and is never woken.
#[test]
fn bsr2_xread_block_is_woken_by_xadd() {
    let m = spawn_moon(SHARDS);
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let key = format!("bsr2:{i}");
        let mut writer = Conn::open(m.port);
        assert!(
            writer
                .send(&["XADD", &key, "1-1", "f", "v"])
                .starts_with('$'),
            "seed XADD must succeed"
        );

        let waiter = park(m.port, &["XREAD", "BLOCK", "5000", "STREAMS", &key, "$"]);
        std::thread::sleep(SETTLE);
        assert!(
            writer
                .send(&["XADD", &key, "2-1", "g", "w"])
                .starts_with('$'),
            "waking XADD must succeed"
        );
        let (elapsed, reply) = waiter.join().expect("waiter thread");

        // Only the woken stream appears, carrying only the NEW entry — the
        // seeded 1-1 is below the bound `$` and must not be replayed.
        let expect = format!(
            "*1\r\n*2\r\n${}\r\n{}\r\n*1\r\n*2\r\n$3\r\n2-1\r\n*2\r\n$1\r\ng\r\n$1\r\nw\r\n",
            key.len(),
            key
        );
        if reply != expect {
            wrong.push(format!("  key {key}: got {reply:?}, want {expect:?}"));
        } else if elapsed > Duration::from_secs(4) {
            wrong.push(format!("  key {key}: woken only after {elapsed:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} key placements wrong (moon#595 — XADD must wake a parked XREAD):\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

/// ONE `XADD` wakes EVERY parked reader.
///
/// `XREAD` is non-destructive, so the entry belongs to all of them — measured
/// against redis-server 8.6.1, both clients receive it. The pre-#595 waker was
/// built for the destructive list case: it stopped at the first served waiter
/// and answered every other one a premature `None`. Serving only the first
/// reader would leave the second parked to its deadline.
#[test]
fn bsr3_one_xadd_wakes_every_parked_reader() {
    let m = spawn_moon(SHARDS);
    let key = "bsr3:fanout";
    let mut writer = Conn::open(m.port);
    assert!(
        writer
            .send(&["XADD", key, "1-1", "f", "v"])
            .starts_with('$'),
        "seed XADD must succeed"
    );

    let a = park(m.port, &["XREAD", "BLOCK", "5000", "STREAMS", key, "$"]);
    let b = park(m.port, &["XREAD", "BLOCK", "5000", "STREAMS", key, "$"]);
    std::thread::sleep(SETTLE);
    assert!(
        writer
            .send(&["XADD", key, "7-1", "z", "q"])
            .starts_with('$'),
        "waking XADD must succeed"
    );

    let expect = format!(
        "*1\r\n*2\r\n${}\r\n{}\r\n*1\r\n*2\r\n$3\r\n7-1\r\n*2\r\n$1\r\nz\r\n$1\r\nq\r\n",
        key.len(),
        key
    );
    for (label, handle) in [("first", a), ("second", b)] {
        let (elapsed, reply) = handle.join().expect("waiter thread");
        assert_eq!(
            reply, expect,
            "the {label} reader must receive the entry too (took {elapsed:?})"
        );
    }
}

/// `$` is bound to the stream's last id AT BLOCK TIME, not re-evaluated on
/// every wake.
///
/// Measured against redis-server 8.6.1: park on `$` with the stream at `500-1`,
/// then `DEL` it and `XADD` a fresh `1-1`. The new entry's id is BELOW the
/// bound cursor, so the reader is not woken and times out. A server that
/// re-resolved `$` — or that treated an unbound `$` as `0-0` — would answer
/// the `1-1` entry instead.
///
/// This is the observable consequence of the binding contract that closes the
/// lost-wakeup window, which is why it is asserted rather than assumed.
#[test]
fn bsr4_dollar_binds_at_block_time_not_at_wake_time() {
    let m = spawn_moon(SHARDS);
    let key = "bsr4:rebound";
    let mut writer = Conn::open(m.port);
    assert!(
        writer
            .send(&["XADD", key, "500-1", "f", "v"])
            .starts_with('$'),
        "seed XADD must succeed"
    );

    let waiter = park(m.port, &["XREAD", "BLOCK", "1200", "STREAMS", key, "$"]);
    std::thread::sleep(SETTLE);
    assert_eq!(writer.send(&["DEL", key]), ":1\r\n", "DEL must remove it");
    assert!(
        writer
            .send(&["XADD", key, "1-1", "g", "h"])
            .starts_with('$'),
        "re-created XADD must succeed"
    );

    let (elapsed, reply) = waiter.join().expect("waiter thread");
    assert_eq!(
        reply, "*-1\r\n",
        "an entry BELOW the bound $ must not wake the reader (took {elapsed:?})"
    );
    assert!(
        elapsed >= Duration::from_millis(1000),
        "must have waited out BLOCK 1200, took {elapsed:?}"
    );
}

/// `BLOCK 0` means block forever: still parked well past any finite budget.
///
/// Bounded from the test side by dropping the connection, so a regression to
/// "returns instantly" fails fast instead of the suite hanging.
#[test]
fn bsr5_block_zero_waits_indefinitely_then_is_woken() {
    let m = spawn_moon(SHARDS);
    let key = "bsr5:forever";
    let mut writer = Conn::open(m.port);
    let waiter = park(m.port, &["XREAD", "BLOCK", "0", "STREAMS", key, "$"]);
    // Far longer than any budget this suite uses elsewhere.
    std::thread::sleep(Duration::from_millis(1500));
    assert!(
        writer
            .send(&["XADD", key, "3-1", "a", "b"])
            .starts_with('$'),
        "waking XADD must succeed"
    );
    let (elapsed, reply) = waiter.join().expect("waiter thread");
    assert!(
        reply.contains("3-1"),
        "BLOCK 0 must stay parked until woken, got {reply:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(1400),
        "BLOCK 0 returned after only {elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// moon#595 — XREADGROUP shares the defect and the fix
// ---------------------------------------------------------------------------

/// `XREADGROUP ... BLOCK <ms> ... >` blocks and is woken, exactly like XREAD.
#[test]
fn bsr6_xreadgroup_block_new_messages_blocks_and_wakes() {
    let m = spawn_moon(SHARDS);
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let key = format!("bsr6:{i}");
        let mut writer = Conn::open(m.port);
        assert!(
            writer
                .send(&["XADD", &key, "1-1", "f", "v"])
                .starts_with('$'),
            "seed XADD must succeed"
        );
        assert_eq!(
            writer.send(&["XGROUP", "CREATE", &key, "g1", "$"]),
            "+OK\r\n",
            "XGROUP CREATE must succeed"
        );

        // Nothing new for the group -> must park.
        let start = Instant::now();
        let mut probe = Conn::open(m.port);
        let miss = probe.send(&[
            "XREADGROUP",
            "GROUP",
            "g1",
            "cmiss",
            "BLOCK",
            "700",
            "STREAMS",
            &key,
            ">",
        ]);
        let waited = start.elapsed();
        if miss != "*-1\r\n" || waited < Duration::from_millis(600) {
            wrong.push(format!(
                "  key {key}: miss returned {miss:?} after {waited:?}"
            ));
            continue;
        }

        // ...and a later XADD wakes a parked consumer.
        let waiter = park(
            m.port,
            &[
                "XREADGROUP",
                "GROUP",
                "g1",
                "cwake",
                "BLOCK",
                "5000",
                "STREAMS",
                &key,
                ">",
            ],
        );
        std::thread::sleep(SETTLE);
        assert!(
            writer
                .send(&["XADD", &key, "5-1", "n", "e"])
                .starts_with('$'),
            "waking XADD must succeed"
        );
        let (_, reply) = waiter.join().expect("waiter thread");
        if !reply.contains("5-1") {
            wrong.push(format!("  key {key}: waiter got {reply:?}, want entry 5-1"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} key placements wrong (moon#595 — XREADGROUP BLOCK):\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

/// An `XREADGROUP` history read answers AT ONCE even when `BLOCK` is written.
///
/// Measured against redis-server 8.6.1:
/// `XREADGROUP GROUP g c BLOCK 2000 STREAMS ga gb > 0` returns in 0.000 s
/// carrying only `gb`. So "BLOCK is present" is NOT sufficient to make a
/// stream read blocking, and an over-eager predicate would park a client that
/// Redis answers immediately.
///
/// Note the empty entry list is CORRECT here and must not be swept up by the
/// moon#594 omission rule — that rule is specific to plain `XREAD`.
#[test]
fn bsr7_xreadgroup_history_read_never_blocks() {
    let m = spawn_moon(SHARDS);
    let key = "bsr7:history";
    let mut c = Conn::open(m.port);
    assert!(
        c.send(&["XADD", key, "1-1", "f", "v"]).starts_with('$'),
        "seed XADD must succeed"
    );
    assert_eq!(
        c.send(&["XGROUP", "CREATE", key, "g1", "$"]),
        "+OK\r\n",
        "XGROUP CREATE must succeed"
    );

    let (elapsed, reply) = timed(
        m.port,
        &[
            "XREADGROUP",
            "GROUP",
            "g1",
            "c9",
            "BLOCK",
            "2000",
            "STREAMS",
            key,
            "0",
        ],
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "a history read must not block, took {elapsed:?}"
    );
    let expect = format!("*1\r\n*2\r\n${}\r\n{}\r\n*0\r\n", key.len(), key);
    assert_eq!(
        reply, expect,
        "an empty PEL is reported as a present-but-empty stream"
    );
}

// ---------------------------------------------------------------------------
// moon#595 — errors are answered at once, never parked
// ---------------------------------------------------------------------------

/// `XREAD BLOCK` on an existing key of the wrong type is an immediate
/// `-WRONGTYPE`, on EVERY key placement.
///
/// Parking would be the worse failure: the key cannot become a stream while it
/// holds a string, so the client waits out its whole budget for an answer that
/// was knowable at t=0.
#[test]
fn bsr8_xread_block_on_wrong_type_is_immediate() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bsr8:",
        "moon#595 — a stream read on a string key must be -WRONGTYPE, not a park",
        |c, k| {
            assert_eq!(c.send(&["SET", k, "v"]), "+OK\r\n", "SET must succeed");
            let start = Instant::now();
            let reply = c.send(&["XREAD", "BLOCK", "1500", "STREAMS", k, "$"]);
            let elapsed = start.elapsed();
            if !reply.starts_with("-WRONGTYPE") {
                return Some(format!("expected -WRONGTYPE, got {reply:?}"));
            }
            if elapsed > Duration::from_millis(500) {
                return Some(format!("answered only after {elapsed:?} — it parked"));
            }
            None
        },
    );
}

/// `XREADGROUP BLOCK` naming a group that does not exist is an immediate
/// `-NOGROUP`, on EVERY key placement.
///
/// Placement-sensitive by construction: the group-existence check lives on the
/// shard that OWNS the key, so before the owner-side gate this answered the
/// error when the key happened to hash to the client's own shard and parked
/// for the full budget when it did not. Same command, two answers, decided by
/// a hash.
#[test]
fn bsr9_xreadgroup_block_missing_group_is_immediate() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bsr9:",
        "moon#595 — a missing consumer group must be -NOGROUP on every shard, not a park",
        |c, k| {
            assert!(
                c.send(&["XADD", k, "1-1", "f", "v"]).starts_with('$'),
                "XADD must succeed"
            );
            let start = Instant::now();
            let reply = c.send(&[
                "XREADGROUP",
                "GROUP",
                "nope",
                "c",
                "BLOCK",
                "800",
                "STREAMS",
                k,
                ">",
            ]);
            let elapsed = start.elapsed();
            if !reply.starts_with("-NOGROUP") {
                return Some(format!("expected -NOGROUP, got {reply:?}"));
            }
            if elapsed > Duration::from_millis(400) {
                return Some(format!("answered only after {elapsed:?} — it parked"));
            }
            None
        },
    );
}

/// `BLOCK`'s argument is validated with Redis's own error texts.
///
/// All three answered `*-1` before #595, because the value was never looked at
/// — a client that sent a bad timeout was told "nothing new" instead.
#[test]
fn bsr10_block_argument_errors_match_redis() {
    let m = spawn_moon(SHARDS);
    let cases: &[(&str, &str)] = &[
        ("abc", "-ERR timeout is not an integer or out of range\r\n"),
        ("1.5", "-ERR timeout is not an integer or out of range\r\n"),
        ("-1", "-ERR timeout is negative\r\n"),
    ];
    for (arg, expect) in cases {
        let (_, reply) = timed(m.port, &["XREAD", "BLOCK", arg, "STREAMS", "bsr10:k", "$"]);
        assert_eq!(reply, *expect, "XREAD BLOCK {arg}");
    }
    // `BLOCK` with no value at all stays an arity error, answered by the
    // ordinary dispatch table rather than by the blocking machinery.
    let (_, reply) = timed(m.port, &["XREAD", "BLOCK"]);
    assert_eq!(
        reply, "-ERR wrong number of arguments for 'xread' command\r\n",
        "a dangling BLOCK is an arity error"
    );
}

// ---------------------------------------------------------------------------
// moon#594 — an unserved stream is omitted, not reported empty
// ---------------------------------------------------------------------------

/// `XREAD` over a served and an unserved stream names ONLY the served one.
///
/// Measured against redis-server 8.6.1 with data in `pelA` only:
///
/// ```text
/// XREAD COUNT 10 STREAMS pelA pelB 0 99999
///   redis : *1 …pelA…
///   moon  : *2 …pelA… *2 $4 pelB *0     <- the bug
/// ```
///
/// Co-located via a hash tag so a single `XREAD` can see both streams
/// regardless of shard count — moon routes a multi-stream read by its first
/// key, so untagged keys on different shards would test routing, not the
/// omission rule.
#[test]
fn bsr11_xread_omits_a_stream_that_had_nothing() {
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);
    let a = "{bsr11}:a";
    let b = "{bsr11}:b";
    assert!(
        c.send(&["XADD", a, "1-1", "f", "v"]).starts_with('$'),
        "XADD a must succeed"
    );
    assert!(
        c.send(&["XADD", b, "1-1", "f", "v"]).starts_with('$'),
        "XADD b must succeed"
    );

    // `a` from 0 (serves its entry), `b` from an id far past its last (serves
    // nothing).
    let reply = c.send(&["XREAD", "COUNT", "10", "STREAMS", a, b, "0", "99999"]);
    let expect = format!(
        "*1\r\n*2\r\n${}\r\n{}\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*2\r\n$1\r\nf\r\n$1\r\nv\r\n",
        a.len(),
        a
    );
    assert_eq!(
        reply, expect,
        "the unserved stream must be omitted entirely, not sent as an empty list"
    );

    // A stream that does not exist at all is omitted on the same rule.
    let reply = c.send(&[
        "XREAD",
        "COUNT",
        "10",
        "STREAMS",
        a,
        "{bsr11}:ghost",
        "0",
        "0",
    ]);
    assert_eq!(reply, expect, "a missing stream must be omitted too");

    // And when NOTHING is served the reply is still the null array, not an
    // empty one — the miss shape is unchanged by the omission rule.
    let reply = c.send(&["XREAD", "STREAMS", a, b, "99999", "99999"]);
    assert_eq!(
        reply, "*-1\r\n",
        "an all-unserved read stays the null array"
    );
}

// ---------------------------------------------------------------------------
// An oversized BLOCK/timeout must not kill the server
// ---------------------------------------------------------------------------

/// A timeout too large to become a `Duration` is an ERROR, and the server is
/// still serving afterwards.
///
/// Found while implementing #595 and fixed with it. `Duration::from_secs_f64`
/// panics on a value it cannot represent, and moon turns a panic on a shard
/// thread into a whole-process abort, so `BLPOP nokey 1e300` was an
/// unauthenticated remote kill:
///
/// ```text
/// $ redis-cli -p 7833 BLPOP nokey 1e300
/// Error: Server closed the connection
/// cannot convert float seconds to Duration: value is either too big or NaN
/// FATAL: thread 'shard-0' panicked; aborting the whole process ...
/// ```
///
/// The `BLPOP` route predates #595; `XREAD BLOCK` is the new door onto the same
/// code, which is why the fix lands here. This test is deliberately end-to-end:
/// the unit tests in `server::conn::blocking` prove the parser now returns an
/// error, but only a live server can prove the PROCESS survived — a panicking
/// build passes every in-process assertion right up to the point it aborts.
///
/// The `PING` after each case is the real assertion. Error texts are pinned to
/// redis-server 8.6.1, measured with one fresh connection per case.
#[test]
fn bsr12_oversized_timeout_errors_and_the_server_survives() {
    let m = spawn_moon(SHARDS);
    let cases: &[(&[&str], &str)] = &[
        (
            &["BLPOP", "bsr12:nokey", "1e300"],
            "-ERR timeout is out of range\r\n",
        ),
        (
            &["BLPOP", "bsr12:nokey", "1e16"],
            "-ERR timeout is out of range\r\n",
        ),
        (
            &["BLPOP", "bsr12:nokey", "nan"],
            "-ERR timeout is not a float or out of range\r\n",
        ),
        (
            &["BLPOP", "bsr12:nokey", "-0.5"],
            "-ERR timeout is negative\r\n",
        ),
        (
            &[
                "XREAD",
                "BLOCK",
                "9223372036854775807",
                "STREAMS",
                "bsr12:s",
                "$",
            ],
            "-ERR timeout is out of range\r\n",
        ),
        (
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "9223372036854775807",
                "STREAMS",
                "bsr12:s",
                ">",
            ],
            "-ERR timeout is out of range\r\n",
        ),
    ];
    for (argv, expect) in cases {
        let (elapsed, reply) = timed(m.port, argv);
        assert_eq!(reply, *expect, "{argv:?}");
        assert!(
            elapsed < Duration::from_millis(500),
            "{argv:?} answered in {elapsed:?} — a range error must be immediate, not parked"
        );
        let mut c = Conn::open(m.port);
        assert_eq!(
            c.send(&["PING"]),
            "+PONG\r\n",
            "server died after {argv:?} — this is the abort, not a parity miss"
        );
    }
    // Both sides of the boundary, so the fix reads as a range check and not a
    // blanket ban on large timeouts: one millisecond under the limit is still
    // representable and must actually park. Not joined — it parks for ~292 000
    // years, and `Moon::drop` kills the server, which releases it.
    let parked = park(
        m.port,
        &[
            "XREAD",
            "BLOCK",
            "9223372036854775",
            "STREAMS",
            "bsr12:s",
            "$",
        ],
    );
    std::thread::sleep(SETTLE);
    assert!(
        !parked.is_finished(),
        "a representable huge timeout must park, but it answered immediately"
    );
}

// ---------------------------------------------------------------------------
// The gate is a whitelist: everything it refuses keeps its pre-#595 answer
// ---------------------------------------------------------------------------

/// A multi-stream `BLOCK` does NOT park, and where the read is decidable it
/// serves every stream without stranding an entry in the PEL.
///
/// This is the P0 guard. Moon cannot decide a multi-key command on one shard,
/// so before this gate a multi-stream `XREADGROUP ... BLOCK ... >` was deferred
/// to one `BlockRegister` per key, each on a different thread; every owner ran
/// its own post-registration re-check, and that re-check MUTATES —
/// `read_group_new` moves entries into the consumer's PEL. The client's
/// coordinator kept the first reply and dropped the rest, so the sibling's
/// entries were left marked delivered-and-unacked to a consumer that never saw
/// them, recoverable only via `XPENDING` / `XAUTOCLAIM`. Measured on
/// `--shards 4`, 10 key pairs: 5 lost an entry that way.
///
/// Two halves, because they guard different things:
///
/// * **Co-located** (`{tag}`) — the read is decidable on one shard, so the
///   full invariant holds and is asserted: both streams served, nothing
///   pending that the client was not told about.
/// * **Any placement** — only "it did not park". The cross-shard multi-key
///   under-read is a PRE-EXISTING defect this PR does not fix and must not be
///   blamed for: the same scenario measured on a binary built from the
///   merge-base strands the same 7 of 24 entries, byte-for-byte (redis 8.6.1:
///   0 of 24). Asserting it here would fail on `main` too. It is filed
///   separately; what this PR owes is that `BLOCK` adds nothing to it.
#[test]
fn bsr13_multi_stream_block_does_not_park_and_serves_co_located_streams() {
    let m = spawn_moon(SHARDS);

    // --- decidable: one shard owns both streams ---
    each_trial(
        m.port,
        "bsr13:",
        "co-located multi-stream XREADGROUP BLOCK must serve both and strand nothing",
        |c, key| {
            let a = format!("{{{key}}}:a");
            let b = format!("{{{key}}}:b");
            for k in [&a, &b] {
                c.send(&["XADD", k, "1-1", "f", "v"]);
                c.send(&["XGROUP", "CREATE", k, "g", "0"]);
            }
            let start = Instant::now();
            let reply = c.send(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "cc",
                "BLOCK",
                "700",
                "STREAMS",
                &a,
                &b,
                ">",
                ">",
            ]);
            let elapsed = start.elapsed();
            if elapsed >= Duration::from_millis(400) {
                return Some(format!(
                    "parked for {elapsed:?}; a multi-stream BLOCK must not"
                ));
            }
            for k in [&a, &b] {
                if !reply.contains(k.as_str()) {
                    return Some(format!("{k} missing from {reply:?}"));
                }
                // XPENDING <key> <group> answers `*4 :<count> …`; anything but
                // `:1` here means the entry did not reach this consumer.
                let pending = c.send(&["XPENDING", k, "g"]);
                if !pending.contains(":1") {
                    return Some(format!("{k}: served but XPENDING says {pending:?}"));
                }
            }
            None
        },
    );

    // --- any placement: the gate itself. No PEL claim, see the doc above. ---
    each_trial(
        m.port,
        "bsr13x:",
        "a multi-stream BLOCK must never become a wait, wherever the keys land",
        |c, key| {
            let a = format!("{key}:a");
            let b = format!("{key}:b");
            c.send(&["XADD", &a, "1-1", "f", "v"]);
            let start = Instant::now();
            c.send(&["XREAD", "BLOCK", "700", "STREAMS", &a, &b, "$", "$"]);
            let elapsed = start.elapsed();
            (elapsed >= Duration::from_millis(400)).then(|| format!("parked for {elapsed:?}"))
        },
    );
}

/// Ids the waiter could never be woken at are refused by the gate, not parked.
///
/// `+` is the sharp one: `StreamId::parse` maps it to `StreamId::MAX`, so a
/// waiter registered on it can never be served by any `XADD` and would sit
/// there until its client disconnected — a permanent park where moon used to
/// answer instantly. `>` on a plain `XREAD` is a syntax error that, if
/// registered, would have bound the waiter at `0-0` and replayed the entire
/// stream on the first write. A malformed `COUNT` must be an error, not a wait.
///
/// The assertion is the CLOCK, not the bytes: what each of these answers is
/// the dispatch table's business and unchanged by this PR. What matters is
/// that none of them became a wait.
#[test]
fn bsr14_unwakeable_ids_and_bad_count_are_refused_not_parked() {
    let m = spawn_moon(SHARDS);
    let k = "bsr14:s";
    let mut c = Conn::open(m.port);
    c.send(&["XADD", k, "1-1", "f", "v"]);
    c.send(&["XADD", k, "2-1", "f", "w"]);

    let cases: &[&[&str]] = &[
        &["XREAD", "BLOCK", "700", "STREAMS", "bsr14:s", "+"],
        &["XREAD", "BLOCK", "700", "STREAMS", "bsr14:s", "-"],
        &["XREAD", "BLOCK", "700", "STREAMS", "bsr14:s", ">"],
        &[
            "XREAD", "COUNT", "abc", "BLOCK", "700", "STREAMS", "bsr14:s", "$",
        ],
        // Redis's `string2ll` takes neither a leading `+` nor leading zeros,
        // and moon parked the full budget on both.
        &["XREAD", "BLOCK", "+700", "STREAMS", "bsr14:s", "$"],
        &["XREAD", "BLOCK", "0700", "STREAMS", "bsr14:s", "$"],
    ];
    for argv in cases {
        let (elapsed, reply) = timed(m.port, argv);
        assert!(
            elapsed < Duration::from_millis(400),
            "{argv:?} waited {elapsed:?} — the gate must refuse it, not park it (reply {reply:?})"
        );
    }

    // The fence: the very same command with a WAKEABLE id still parks, so the
    // rows above are not passing because blocking stopped working.
    let (elapsed, _) = timed(
        m.port,
        &["XREAD", "BLOCK", "700", "STREAMS", "bsr14:s", "$"],
    );
    assert!(
        elapsed >= Duration::from_millis(600),
        "a `$` read must still park, but answered in {elapsed:?}"
    );
}
