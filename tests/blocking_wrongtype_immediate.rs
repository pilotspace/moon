//! A blocking pop on an EXISTING key of the WRONG TYPE must answer
//! `-WRONGTYPE` at once, never block (moon#556) — and a populated key must be
//! served at once no matter which shard owns it (moon#557).
//!
//! redis-server 8.x, measured:
//!
//! ```text
//! SET s v
//! BLPOP s 0      -> (error) WRONGTYPE Operation against a key holding the wrong kind of value
//! ```
//!
//! Moon blocked instead. The pop helpers reach the store through
//! `get_mut_if_present(..).ok()??` — an `Err(WRONGTYPE)` collapsed into
//! `None`, indistinguishable from "empty" — so `try_immediate_pop` reported
//! "nothing here" and the connection went on to register a waiter on a key
//! that can never serve it. The client then sat there until its timeout (or
//! forever, at `timeout 0`) and got a null it reads as "queue empty".
//!
//! `--shards 4` is deliberate: a connection is pinned to one shard, so ~3 of
//! every 4 keys are owned by ANOTHER shard and register remotely. The two
//! paths are answered in two different places (the connection's own
//! pre-registration scan vs. the owning shard's `BlockRegister` handler), and
//! a fix to only one of them passes ~25% of trials. Each assertion therefore
//! loops over enough distinct keys that both paths are exercised.
//!
//! The one case still NOT covered end-to-end here is a `BLMOVE` whose
//! DESTINATION lives on another shard: the immediate path can only inspect
//! keys its own shard owns. `bwt4` pins that case at `--shards 1`, where the
//! pair is provably co-located; see the residual note there.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards: a key is remote ~75% of the time.
const SHARDS: &str = "4";
/// Distinct keys per assertion. At p(remote)=0.75 the chance that all 12 land
/// on the connection's own shard — and vacuously test one path — is under 1e-7.
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
        let tmp_dir = std::env::temp_dir().join(format!("moon-bwt-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-bwt-{port}"));
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

/// Run `body` once per key placement on a FRESH connection and report every
/// trial that came out wrong.
///
/// Fresh connections on purpose: which shard a connection lands on is what
/// decides whether its key is local, so reusing one connection would sample a
/// single placement `TRIALS` times instead of the distribution. Reporting the
/// COUNT is the diagnosis — roughly 3-in-4 wrong means the remote
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

/// Every blocking pop that takes a plain key list, against a STRING key.
#[test]
fn bwt1_blocking_pops_reject_a_string_key_immediately() {
    let m = spawn_moon(SHARDS);
    // (label, argv template) — `{k}` is substituted with the trial key.
    let probes: &[(&str, &[&str])] = &[
        ("BLPOP", &["BLPOP", "{k}", "2"]),
        ("BRPOP", &["BRPOP", "{k}", "2"]),
        ("BZPOPMIN", &["BZPOPMIN", "{k}", "2"]),
        ("BZPOPMAX", &["BZPOPMAX", "{k}", "2"]),
        ("BLMPOP", &["BLMPOP", "2", "1", "{k}", "LEFT"]),
        ("BZMPOP", &["BZMPOP", "2", "1", "{k}", "MIN"]),
    ];
    for (label, argv) in probes {
        each_trial(
            m.port,
            &format!("bwt1:{label}:"),
            "moon#556 — a blocking pop on a string key must be -WRONGTYPE, not a \
             block-then-null",
            |c, k| {
                assert_eq!(c.send(&["SET", k, "v"]), "+OK\r\n", "SET must succeed");
                let parts: Vec<String> = argv
                    .iter()
                    .map(|p| {
                        if *p == "{k}" {
                            k.to_string()
                        } else {
                            (*p).to_string()
                        }
                    })
                    .collect();
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                let started = Instant::now();
                let reply = c.send(&refs);
                let elapsed = started.elapsed();
                if !reply.starts_with("-WRONGTYPE") {
                    return Some(format!(
                        "{label} replied {reply:?} after {elapsed:?} (a null here means \
                         the client blocked on a key that can never serve it)"
                    ));
                }
                // moon#560's guarantee: the value the pop refused is untouched.
                let got = c.send(&["GET", k]);
                (got != "$1\r\nv\r\n")
                    .then(|| format!("{label}: value clobbered, GET replied {got:?}"))
            },
        );
    }
}

/// The mirror case: a LIST key under the sorted-set family, and a ZSET key
/// under the list family. Same error, and the collection survives.
#[test]
fn bwt2_wrongtype_across_collection_families() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bwt2:l:",
        "moon#556 — BZPOPMIN on a LIST must be -WRONGTYPE",
        |c, k| {
            assert_eq!(c.send(&["RPUSH", k, "v"]), ":1\r\n");
            let reply = c.send(&["BZPOPMIN", k, "2"]);
            if !reply.starts_with("-WRONGTYPE") {
                return Some(format!("BZPOPMIN replied {reply:?}"));
            }
            let len = c.send(&["LLEN", k]);
            (len != ":1\r\n").then(|| format!("the list lost its element: LLEN {len:?}"))
        },
    );
    each_trial(
        m.port,
        "bwt2:z:",
        "moon#556 — BLPOP on a ZSET must be -WRONGTYPE",
        |c, k| {
            assert_eq!(c.send(&["ZADD", k, "1", "m"]), ":1\r\n");
            let reply = c.send(&["BLPOP", k, "2"]);
            if !reply.starts_with("-WRONGTYPE") {
                return Some(format!("BLPOP replied {reply:?}"));
            }
            let card = c.send(&["ZCARD", k]);
            (card != ":1\r\n").then(|| format!("the zset lost its member: ZCARD {card:?}"))
        },
    );
}

/// moon#557: a populated key owned by ANOTHER shard is still served at once,
/// with no second client pushing to wake it.
///
/// The pre-registration scan runs against the connection's own shard slice,
/// so it now skips keys it does not own outright (before, it consulted the
/// local slice for every key and always missed on the remote ones). What
/// serves those keys is the owning shard's `BlockRegister` handler, which
/// checks for available data the moment the registration lands. This pins that
/// path: at `--shards 4` most placements are remote, the element must come
/// back in milliseconds — not at the 3s timeout — and exactly one element must
/// leave the key.
#[test]
fn bwt5_a_populated_key_is_served_wherever_it_lives() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bwt5:l:",
        "moon#557 — a populated list must be served immediately whether the \
         connection's own shard owns it or not",
        |c, k| {
            assert_eq!(c.send(&["RPUSH", k, "v1", "v2"]), ":2\r\n");
            let started = Instant::now();
            let reply = c.send(&["BLPOP", k, "3"]);
            let elapsed = started.elapsed();
            if !reply.contains("v1") {
                return Some(format!("BLPOP replied {reply:?} after {elapsed:?}"));
            }
            if elapsed > Duration::from_millis(500) {
                return Some(format!(
                    "BLPOP took {elapsed:?} — it blocked instead of being served"
                ));
            }
            let len = c.send(&["LLEN", k]);
            (len != ":1\r\n")
                .then(|| format!("exactly one element must have been consumed, LLEN {len:?}"))
        },
    );
    each_trial(
        m.port,
        "bwt5:z:",
        "moon#557 — a populated zset must be served immediately wherever it lives",
        |c, k| {
            assert_eq!(c.send(&["ZADD", k, "1", "m1", "2", "m2"]), ":2\r\n");
            let started = Instant::now();
            let reply = c.send(&["BZPOPMIN", k, "3"]);
            let elapsed = started.elapsed();
            if !reply.contains("m1") {
                return Some(format!("BZPOPMIN replied {reply:?} after {elapsed:?}"));
            }
            let card = c.send(&["ZCARD", k]);
            (card != ":1\r\n")
                .then(|| format!("exactly one member must have been consumed, ZCARD {card:?}"))
        },
    );
}

/// The negative controls: the fix must not turn a MISS into an error, and a
/// servable pop must still be served.
#[test]
fn bwt3_misses_still_block_and_hits_are_still_served() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "bwt3:miss:",
        "an ABSENT key must still block to its timeout and answer a null array",
        |c, k| {
            let started = Instant::now();
            let reply = c.send(&["BLPOP", k, "1"]);
            let elapsed = started.elapsed();
            if reply != "*-1\r\n" {
                return Some(format!("BLPOP on an absent key replied {reply:?}"));
            }
            // It must have actually waited — an instant null would mean the
            // key was never watched at all.
            (elapsed < Duration::from_millis(700))
                .then(|| format!("returned after only {elapsed:?}, so it never blocked"))
        },
    );
    each_trial(
        m.port,
        "bwt3:hit:",
        "a populated list must still be served immediately",
        |c, k| {
            assert_eq!(c.send(&["RPUSH", k, "v1"]), ":1\r\n");
            let reply = c.send(&["BLPOP", k, "2"]);
            (!reply.contains("v1")).then(|| format!("BLPOP replied {reply:?}, expected v1"))
        },
    );
}

/// `BLMOVE`/`BRPOPLPUSH` consult the DESTINATION's type only when the move is
/// actually about to happen, and then the error arrives INSTEAD of the move —
/// the element stays in the source. (Redis `lmoveGenericCommand`; moon's own
/// non-blocking `LMOVE` already behaves this way.)
///
/// Pre-fix the element was popped from the source and then silently dropped:
/// `list_push_front`/`list_push_back` swallow a wrong-typed destination in an
/// `if let Ok(list)`, so the client got the value in its reply while the value
/// left the keyspace entirely.
///
/// **`--shards 1` on purpose.** The immediate path can only inspect keys its
/// own shard owns, so at `--shards N` a destination owned by another shard is
/// invisible to it and the check cannot run there. That residual is real and
/// intentional: see the module docs. One shard makes source and destination
/// provably co-located, which is exactly the case this pins.
#[test]
fn bwt4_blmove_rejects_a_wrongtype_destination_without_losing_the_element() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    for (label, argv) in [
        (
            "BLMOVE",
            vec!["BLMOVE", "bwt4:src", "bwt4:dst", "LEFT", "LEFT", "2"],
        ),
        (
            "BRPOPLPUSH",
            vec!["BRPOPLPUSH", "bwt4:src2", "bwt4:dst", "2"],
        ),
    ] {
        let src = argv[1];
        assert_eq!(c.send(&["RPUSH", src, "v1"]), ":1\r\n");
        assert_eq!(c.send(&["SET", "bwt4:dst", "iam-a-string"]), "+OK\r\n");
        let reply = c.send(&argv);
        assert!(
            reply.starts_with("-WRONGTYPE"),
            "{label} into a string destination must be -WRONGTYPE, got {reply:?}"
        );
        assert_eq!(
            c.send(&["LLEN", src]),
            ":1\r\n",
            "{label}: the element must still be in the source, not vanished"
        );
        assert_eq!(
            c.send(&["GET", "bwt4:dst"]),
            "$12\r\niam-a-string\r\n",
            "{label}: the destination must be untouched"
        );
    }
}
