//! A READ must not flatten a compact encoding, on ANY dispatch path (moon#832).
//!
//! `Database::get_promoted` (`src/storage/db/accessors.rs`) calls `K::upgrade`
//! unconditionally and returns `K::Shared<'_>` — a *shared* reference. The
//! conversion is one-way: nothing in the tree ever downgrades. So a single read
//! taken on the mutable dispatch path permanently flattened a `listpack` /
//! `intset` key for the rest of its life, and whether that happened depended on
//! which of moon's three dispatch paths the command took rather than on what
//! the command did.
//!
//! Measured on the unmodified binary at b04e8990, `--shards 1`:
//!
//! ```text
//!   SADD s 1 2 3            OBJECT ENCODING s -> intset
//!   SMEMBERS s              OBJECT ENCODING s -> intset      (read path: ok)
//!   MULTI/SMEMBERS s/EXEC   OBJECT ENCODING s -> hashtable   (mutable path)
//!   RPUSH l a b c           OBJECT ENCODING l -> listpack
//!   MULTI/LLEN l/EXEC       OBJECT ENCODING l -> linkedlist
//!   EVAL "redis.call('SCARD',KEYS[1])" 1 s    -> intset -> hashtable
//! ```
//!
//! and 1000 eight-member integer sets went 333,055 -> 1,149,055 bytes of
//! `used_memory` (3.45x) after ONE `SCARD` each. That is the ceiling this test
//! guards: it turns the listpack-encoding memory wins from write-only-workload
//! figures into figures a read-mixed workload keeps.
//!
//! Probe hygiene (the reason this test can be trusted):
//!
//! * `OBJECT ENCODING` reads through `Database::get` / `get_if_alive_any_plane`
//!   on both dispatch paths — neither routes through `get_promoted` — so asking
//!   about the encoding cannot itself change it. Same argument as
//!   `tests/restart_preserves_compact_encoding.rs`.
//! * Every case asserts the fixture is compact BEFORE the read. A fixture that
//!   was never compact would make the post-read assertion vacuous — exactly how
//!   a "0 of N flattened, all green" report gets produced.
//! * The plain read is the NEGATIVE CONTROL. It passed on the unmodified binary
//!   and must keep passing; if it ever fails the probe is measuring something
//!   other than the dispatch path.
//!
//! Not `#[ignore]`d, for the reason spelled out in
//! `tests/restart_preserves_compact_encoding.rs`: every `--ignored` invocation
//! in `.github/workflows/` names a specific `--test` target, so an ignored test
//! here would never run anywhere.
//!
//! Run alone:
//!   cargo test --release --test read_preserves_compact_encoding

mod common;

use std::process::{Child, Command};

fn start_moon(dir: &std::path::Path) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                // One shard: the defect is per-keyspace, and a single shard
                // removes any question about which shard served the probe.
                "--shards",
                "1",
                "--dir",
                dir.to_str().expect("utf8 dir"),
                "--appendonly",
                "no",
                // The diskfull guard trips on a shared volume with little free
                // space and would abort startup before the probe ever runs.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

/// `OBJECT ENCODING <key>` as a plain string.
fn encoding(c: &mut common::Conn, key: &str) -> String {
    let raw = c.send(&["OBJECT", "ENCODING", key]);
    raw.trim_start_matches('$')
        .lines()
        .nth(1)
        .unwrap_or("")
        .trim()
        .to_string()
}

/// The three ways a read reaches the keyspace.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// A bare command on an idle connection — `dispatch_read` or
    /// `server::conn::try_inline_dispatch`, whichever the read guard picks.
    Plain,
    /// Queued in a transaction: the EXEC executor runs it on `&mut Database`
    /// via `command::dispatch`. No intercept, no read guard (moon#639).
    Multi,
    /// Through the Lua bridge, which also takes the mutable path.
    Eval,
}

fn read_via(c: &mut common::Conn, path: Path, cmd: &[&str], key: &str) {
    match path {
        Path::Plain => {
            c.send(cmd);
        }
        Path::Multi => {
            let batch: [&[&str]; 3] = [&["MULTI"], cmd, &["EXEC"]];
            c.pipeline(&batch);
        }
        Path::Eval => {
            // `redis.call` with the command name as the first element. Built
            // as one script so the argument list stays fixed-shape.
            let script = format!(
                "return redis.status_reply(tostring(redis.call({})))",
                cmd.iter()
                    .enumerate()
                    .map(|(i, a)| if i == 1 {
                        "KEYS[1]".to_string()
                    } else {
                        format!("'{a}'")
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            c.send(&["EVAL", &script, "1", key]);
        }
    }
}

/// One container type: the encoding its fixture must produce, the fixture
/// itself, and every read to run against it on all three dispatch paths.
struct Case {
    /// What `OBJECT ENCODING` must report before AND after each read.
    want: &'static str,
    /// The command that builds the fixture at key `k`.
    fixture: &'static [&'static str],
    /// The reads under test.
    reads: &'static [&'static [&'static str]],
}

/// Every read that used to route through `Database::get_set` / `get_list`
/// (moon#853) or `get_sorted_set` (moon#928), plus the hash family as a
/// regression guard on the shape both fixes copy.
fn cases() -> &'static [Case] {
    &[
        // all-integer set -> intset
        Case {
            want: "intset",
            fixture: &["SADD", "k", "1", "2", "3"],
            reads: &[
                &["SMEMBERS", "k"],
                &["SCARD", "k"],
                &["SISMEMBER", "k", "1"],
                &["SMISMEMBER", "k", "1", "2"],
                &["SRANDMEMBER", "k"],
                &["SSCAN", "k", "0"],
                &["SINTER", "k"],
                &["SUNION", "k"],
                &["SDIFF", "k"],
            ],
        },
        // short list -> listpack
        Case {
            want: "listpack",
            fixture: &["RPUSH", "k", "a", "b", "c"],
            reads: &[
                &["LLEN", "k"],
                &["LRANGE", "k", "0", "-1"],
                &["LINDEX", "k", "0"],
                &["LPOS", "k", "b"],
            ],
        },
        // short zset -> listpack (moon#928). moon#878 made `ZADD` produce a
        // `SortedSetListpack`, which is what turned moon#853's written
        // prediction ("the moment #793 makes SortedSetListpack reachable,
        // every get_sorted_set read caller becomes the same defect") into a
        // live defect: all 17 reads below flattened to `skiplist` on the
        // mutable path at ab91a23e, and 1000 eight-member zsets went
        // 293,055 -> 4,749,055 bytes of `used_memory` (16.21x) after ONE
        // `ZCARD` each.
        Case {
            want: "listpack",
            fixture: &["ZADD", "k", "1", "a", "2", "b", "3", "c"],
            reads: &[
                &["ZSCORE", "k", "a"],
                &["ZCARD", "k"],
                &["ZRANK", "k", "a"],
                &["ZREVRANK", "k", "a"],
                &["ZSCAN", "k", "0"],
                &["ZRANGE", "k", "0", "-1"],
                &["ZREVRANGE", "k", "0", "-1"],
                &["ZRANGEBYSCORE", "k", "-inf", "+inf"],
                &["ZREVRANGEBYSCORE", "k", "+inf", "-inf"],
                &["ZCOUNT", "k", "-inf", "+inf"],
                &["ZLEXCOUNT", "k", "-", "+"],
                &["ZMSCORE", "k", "a"],
                &["ZRANDMEMBER", "k"],
                &["ZDIFF", "1", "k"],
                &["ZUNION", "1", "k"],
                &["ZINTER", "1", "k"],
                &["ZINTERCARD", "1", "k"],
            ],
        },
        // short hash -> listpack. Already correct before moon#832 (the hash
        // reads moved to `get_hash_ref_if_alive` earlier); carried here so a
        // regression on the hash family is caught too.
        Case {
            want: "listpack",
            fixture: &["HSET", "k", "f1", "v1", "f2", "v2"],
            reads: &[
                &["HGET", "k", "f1"],
                &["HGETALL", "k"],
                &["HLEN", "k"],
                &["HKEYS", "k"],
                &["HVALS", "k"],
                &["HEXISTS", "k", "f1"],
                &["HSCAN", "k", "0"],
                &["HRANDFIELD", "k"],
            ],
        },
    ]
}

#[test]
fn read_never_flattens_a_compact_encoding_on_any_dispatch_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = start_moon(dir.path());
    let mut c = common::Conn::open(port);

    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;

    for case in cases() {
        let want = case.want;
        for read in case.reads {
            for path in [Path::Plain, Path::Multi, Path::Eval] {
                // SSCAN/HSCAN/SRANDMEMBER/HRANDFIELD answer container shapes
                // that the EVAL wrapper's `tostring` cannot render; the point
                // of the EVAL leg is the dispatch path, not the reply, so the
                // ones that only differ in reply shape run on Plain + Multi.
                //
                // ZDIFF/ZUNION/ZINTER/ZINTERCARD are excluded for a SECOND,
                // independent reason: `read_via` substitutes `KEYS[1]` at
                // argv index 1, which for those four is `numkeys`, not the
                // key. Left in, the EVAL leg would silently run
                // `ZDIFF k k` — a green row measuring nothing.
                if matches!(path, Path::Eval)
                    && matches!(
                        read[0],
                        "SSCAN"
                            | "HSCAN"
                            | "SMEMBERS"
                            | "SINTER"
                            | "SUNION"
                            | "SDIFF"
                            | "LRANGE"
                            | "HGETALL"
                            | "HKEYS"
                            | "HVALS"
                            | "SMISMEMBER"
                            | "ZSCAN"
                            | "ZRANGE"
                            | "ZREVRANGE"
                            | "ZRANGEBYSCORE"
                            | "ZREVRANGEBYSCORE"
                            | "ZMSCORE"
                            | "ZDIFF"
                            | "ZUNION"
                            | "ZINTER"
                            | "ZINTERCARD"
                    )
                {
                    continue;
                }

                c.send(&["DEL", "k"]);
                c.send(case.fixture);

                let before = encoding(&mut c, "k");
                assert_eq!(
                    before, want,
                    "fixture {:?} must start as `{want}` or the case proves nothing",
                    case.fixture
                );

                read_via(&mut c, path, read, "k");

                let after = encoding(&mut c, "k");
                checked += 1;
                if after != want {
                    failures.push(format!(
                        "{:<12} via {path:?}: {before} -> {after} (expected {want})",
                        read.join(" ")
                    ));
                }
            }
        }
    }

    let _ = child.kill();
    let _ = child.wait();

    assert!(
        checked >= 90,
        "only {checked} cases ran — the matrix collapsed, so a green result means nothing"
    );
    assert!(
        failures.is_empty(),
        "{} of {checked} reads flattened the key's compact encoding (moon#832):\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}
