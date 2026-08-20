//! A two-key WRITE must never be acknowledged unless it landed somewhere a
//! normally-routed read can see it (moon#592).
//!
//! ## The defect
//!
//! moon routes a command to ONE shard — the owner of the key
//! `extract_primary_key` picks — and that shard then executes the whole
//! command against its own slice. `first_key` names the ROUTING key, not every
//! key the command writes. So for the entire two-key write family the OTHER
//! key was read from, and written to, the routing key's slice:
//!
//! ```text
//! SET alpha VALUE-1      -> +OK
//! RENAME alpha omega     -> +OK      <-- claims success
//! GET alpha              -> nil      <-- source destroyed
//! GET omega              -> nil      <-- destination never written
//! ```
//!
//! Measured on the pre-fix binary at `--shards 4`, 12 key placements each:
//! `RENAME` 8/12, and `SMOVE` / `SINTERSTORE` / `ZRANGESTORE` 11/12 lost the
//! data outright. At `--shards 1` the loss rate is 0/12 — it is purely a
//! routing defect.
//!
//! ## What these tests assert
//!
//! NOT "moon returns CROSSSLOT". That would freeze today's remedy into the
//! suite and pass vacuously for any future implementation. They assert the
//! **acknowledgement contract**:
//!
//! ```text
//! reply is success  =>  the destination holds the result, read normally
//! reply is an error =>  the source is untouched AND the destination is absent
//! ```
//!
//! A future cross-shard hop that acks only after the destination shard applies
//! satisfies this identically. The defect violates it, and so does any fix
//! that acks before the write is readable.
//!
//! ## Why this is not vacuous
//!
//! Placements are not sampled and hoped over: `cross_shard_pair` uses the
//! server's OWN routing function (`moon::shard::dispatch::key_to_shard`) to
//! pick a destination name that provably lands on a different shard than the
//! source at `--shards 4`. Every trial therefore exercises the split.
//!
//! Two controls keep the suite honest in the other direction:
//!
//! * `t2k2` runs the SAME key names at `--shards 1`, where every one of them
//!   must still land — so a fix that simply broke these commands fails here.
//! * `t2k3` runs `{hash}`-tagged pairs at `--shards 4`, where every one must
//!   still land — so a blanket refusal fails here.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

/// Four shards: enough that a destination name is easy to place on a shard the
/// source does not own, and the same count the issue measured at.
const SHARDS: usize = 4;
/// Distinct key placements per probe. Each one is *constructed* to straddle a
/// shard boundary, so this is breadth (different names, different owners),
/// not a lottery.
const TRIALS: usize = 12;

// ---------------------------------------------------------------------------
// Probe table — one row per command that writes a key it did not route on
// ---------------------------------------------------------------------------

/// One two-key write, with the reads that decide whether it landed.
///
/// `{s}` / `{d}` in any argv are substituted with the source and destination
/// key names before the command is sent.
struct Probe {
    label: &'static str,
    /// Single-key writes that create the SOURCE. Each is routed normally, so
    /// these are never affected by the defect under test.
    seed: &'static [&'static [&'static str]],
    /// The two-key write itself.
    argv: &'static [&'static str],
    /// A normally-routed read of the DESTINATION — i.e. one that goes to the
    /// destination's real owner, which is exactly what the defect is invisible
    /// to.
    dst_probe: &'static [&'static str],
    /// What `dst_probe` answers once the write has landed.
    dst_landed: &'static str,
    /// What `dst_probe` answers when nothing was written.
    dst_absent: &'static str,
    /// A normally-routed read of the SOURCE.
    src_probe: &'static [&'static str],
    /// What `src_probe` answers while the source is untouched.
    src_untouched: &'static str,
    /// `Some(expected)` when a SUCCESSFUL command must also consume the source
    /// (the RENAME family moves rather than copies). `None` = source survives.
    src_after_success: Option<&'static str>,
}

/// Two points ~166km apart, and a search origin within 200km of both, so every
/// geo probe's correct answer is "2 members stored".
const GEO_SEED: &[&str] = &[
    "GEOADD",
    "{s}",
    "13.361389",
    "38.115556",
    "Palermo",
    "15.087269",
    "37.502669",
    "Catania",
];

const PROBES: &[Probe] = &[
    Probe {
        label: "RENAME",
        seed: &[&["SET", "{s}", "VALUE-1"]],
        argv: &["RENAME", "{s}", "{d}"],
        dst_probe: &["GET", "{d}"],
        dst_landed: "$7\r\nVALUE-1\r\n",
        dst_absent: "$-1\r\n",
        src_probe: &["GET", "{s}"],
        src_untouched: "$7\r\nVALUE-1\r\n",
        src_after_success: Some("$-1\r\n"),
    },
    Probe {
        label: "RENAMENX",
        seed: &[&["SET", "{s}", "VALUE-1"]],
        argv: &["RENAMENX", "{s}", "{d}"],
        dst_probe: &["GET", "{d}"],
        dst_landed: "$7\r\nVALUE-1\r\n",
        dst_absent: "$-1\r\n",
        src_probe: &["GET", "{s}"],
        src_untouched: "$7\r\nVALUE-1\r\n",
        src_after_success: Some("$-1\r\n"),
    },
    Probe {
        label: "SMOVE",
        seed: &[&["SADD", "{s}", "m1", "m2"]],
        argv: &["SMOVE", "{s}", "{d}", "m1"],
        dst_probe: &["SISMEMBER", "{d}", "m1"],
        dst_landed: ":1\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["SCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "SINTERSTORE",
        seed: &[&["SADD", "{s}", "m1", "m2"]],
        argv: &["SINTERSTORE", "{d}", "{s}"],
        dst_probe: &["SCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["SCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "SUNIONSTORE",
        seed: &[&["SADD", "{s}", "m1", "m2"]],
        argv: &["SUNIONSTORE", "{d}", "{s}"],
        dst_probe: &["SCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["SCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "SDIFFSTORE",
        seed: &[&["SADD", "{s}", "m1", "m2"]],
        argv: &["SDIFFSTORE", "{d}", "{s}"],
        dst_probe: &["SCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["SCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "ZRANGESTORE",
        seed: &[&["ZADD", "{s}", "1", "a", "2", "b"]],
        argv: &["ZRANGESTORE", "{d}", "{s}", "0", "-1"],
        dst_probe: &["ZCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["ZCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "ZUNIONSTORE",
        seed: &[&["ZADD", "{s}", "1", "a", "2", "b"]],
        argv: &["ZUNIONSTORE", "{d}", "1", "{s}"],
        dst_probe: &["ZCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["ZCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "ZINTERSTORE",
        seed: &[&["ZADD", "{s}", "1", "a", "2", "b"]],
        argv: &["ZINTERSTORE", "{d}", "1", "{s}"],
        dst_probe: &["ZCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["ZCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "PFMERGE",
        seed: &[&["PFADD", "{s}", "a", "b", "c"]],
        argv: &["PFMERGE", "{d}", "{s}"],
        dst_probe: &["PFCOUNT", "{d}"],
        dst_landed: ":3\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["PFCOUNT", "{s}"],
        src_untouched: ":3\r\n",
        src_after_success: None,
    },
    Probe {
        label: "GEOSEARCHSTORE",
        seed: &[GEO_SEED],
        argv: &[
            "GEOSEARCHSTORE",
            "{d}",
            "{s}",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
        ],
        dst_probe: &["ZCARD", "{d}"],
        dst_landed: ":2\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["ZCARD", "{s}"],
        src_untouched: ":2\r\n",
        src_after_success: None,
    },
    Probe {
        label: "SORT-STORE",
        seed: &[&["RPUSH", "{s}", "3", "1", "2"]],
        argv: &["SORT", "{s}", "STORE", "{d}"],
        dst_probe: &["LLEN", "{d}"],
        dst_landed: ":3\r\n",
        dst_absent: ":0\r\n",
        src_probe: &["LLEN", "{s}"],
        src_untouched: ":3\r\n",
        src_after_success: None,
    },
];

// ---------------------------------------------------------------------------
// Key placement
// ---------------------------------------------------------------------------

/// A source/destination pair that PROVABLY lands on two different shards at
/// `--shards 4`, decided with the server's own routing hash rather than hoped
/// for. Without this the suite could pass by accident on a co-located draw.
fn cross_shard_pair(tag: &str, i: usize) -> (String, String) {
    let src = format!("t2k:{tag}:{i}:s");
    let owner = key_to_shard(src.as_bytes(), SHARDS);
    let dst = (0..1000)
        .map(|j| format!("t2k:{tag}:{i}:d{j}"))
        .find(|d| key_to_shard(d.as_bytes(), SHARDS) != owner)
        .expect("a destination on another shard must exist among 1000 candidates");
    (src, dst)
}

/// A pair collapsed onto one shard by a `{hash}` tag — the documented remedy,
/// which must keep working.
fn colocated_pair(tag: &str, i: usize) -> (String, String) {
    (
        format!("{{t2k:{tag}:{i}}}:s"),
        format!("{{t2k:{tag}:{i}}}:d"),
    )
}

// ---------------------------------------------------------------------------
// Server harness
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
    // `CARGO_BIN_EXE_moon` is the binary cargo built for THIS test run;
    // `common::find_moon_binary()` would fall back to `target/release/moon`,
    // whose provenance is unknown — a stale one turns a real failure green.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-t2k-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-t2k-{port}"));
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

// ---------------------------------------------------------------------------
// The invariant
// ---------------------------------------------------------------------------

fn subst(argv: &[&str], src: &str, dst: &str) -> Vec<String> {
    argv.iter()
        .map(|p| match *p {
            "{s}" => src.to_string(),
            "{d}" => dst.to_string(),
            other => other.to_string(),
        })
        .collect()
}

fn send(c: &mut Conn, argv: &[&str], src: &str, dst: &str) -> String {
    let owned = subst(argv, src, dst);
    let refs: Vec<&str> = owned.iter().map(String::as_str).collect();
    c.send(&refs)
}

/// Run one probe against one key placement.
///
/// Returns `Some(diagnosis)` when the acknowledgement contract was broken.
#[must_use]
fn acknowledgement_honoured(
    c: &mut Conn,
    p: &Probe,
    src: &str,
    dst: &str,
    must_land: bool,
) -> Option<String> {
    for seed in p.seed {
        let reply = send(c, seed, src, dst);
        if reply.starts_with('-') {
            return Some(format!("seeding {seed:?} failed: {reply:?}"));
        }
    }

    let reply = send(c, p.argv, src, dst);
    let refused = reply.starts_with('-');
    let dst_now = send(c, p.dst_probe, src, dst);
    let src_now = send(c, p.src_probe, src, dst);

    if refused {
        if must_land {
            return Some(format!(
                "refused on a placement that MUST work: reply={reply:?}"
            ));
        }
        // A refusal is only honest if it changed nothing at all.
        if dst_now != p.dst_absent || src_now != p.src_untouched {
            return Some(format!(
                "refused ({reply:?}) but the keyspace moved: dst={dst_now:?} \
                 (expected absent {:?}), src={src_now:?} (expected untouched {:?})",
                p.dst_absent, p.src_untouched
            ));
        }
        return None;
    }

    // Acked. The write MUST be visible to a normally-routed read.
    if dst_now != p.dst_landed {
        return Some(format!(
            "ACKED {reply:?} but the destination does not hold the write: \
             {:?} answered {dst_now:?}, expected {:?} — this is acknowledged data loss",
            p.dst_probe, p.dst_landed
        ));
    }
    if let Some(expected_src) = p.src_after_success
        && src_now != expected_src
    {
        return Some(format!(
            "ACKED {reply:?} and the destination landed, but the source was not \
             consumed: {:?} answered {src_now:?}, expected {expected_src:?}",
            p.src_probe
        ));
    }
    None
}

/// Run every probe over `TRIALS` placements and report every broken trial at
/// once, rather than stopping at the first.
#[track_caller]
fn run_all(port: u16, why: &str, place: impl Fn(&str, usize) -> (String, String), must_land: bool) {
    let mut wrong: Vec<String> = Vec::new();
    let mut trials = 0usize;
    for p in PROBES {
        for i in 0..TRIALS {
            // A fresh connection per trial: which shard a connection is pinned
            // to is part of the routing state under test, so reusing one would
            // sample a single arrangement over and over.
            let mut c = Conn::open(port);
            let (src, dst) = place(p.label, i);
            trials += 1;
            if let Some(detail) = acknowledgement_honoured(&mut c, p, &src, &dst, must_land) {
                wrong.push(format!("  {} [{src} -> {dst}]: {detail}", p.label));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{trials} placements broke the acknowledgement contract ({why}):\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// moon#592: the whole two-key write family, every key pair split across two
/// shards. A success reply must be readable at the destination; an error must
/// have changed nothing.
#[test]
fn t2k1_cross_shard_two_key_writes_never_ack_a_write_that_did_not_land() {
    let m = spawn_moon(SHARDS);
    run_all(
        m.port,
        "moon#592 — a two-key write whose destination lives on another shard \
         acked success and left the data nowhere",
        cross_shard_pair,
        false,
    );
}

/// The shard-count control. Identical key names, `--shards 1`: there is no
/// boundary to cross, so every one of these commands must still do its job.
/// This is what proves `t2k1` is measuring routing and not a broken command.
#[test]
fn t2k2_single_shard_control_every_two_key_write_still_lands() {
    let m = spawn_moon(1);
    run_all(
        m.port,
        "single shard: every two-key write must still work for arbitrary key names",
        cross_shard_pair,
        true,
    );
}

/// The narrowness control. `--shards 4`, but the pair is collapsed onto one
/// shard with a `{hash}` tag — the documented remedy. A fix that refuses these
/// too has over-reached.
#[test]
fn t2k3_hash_tagged_pairs_still_work_at_four_shards() {
    let m = spawn_moon(SHARDS);
    run_all(
        m.port,
        "{hash}-tagged pairs are co-located and must never be refused",
        colocated_pair,
        true,
    );
}

/// Tripwire for the two members of the family moon does not implement yet.
///
/// `ZDIFFSTORE` is not in the dispatch table, and `GEORADIUS`/
/// `GEORADIUSBYMEMBER` reject `STORE`/`STOREDIST` outright
/// (`geo_cmd::reject_store_clause`). Neither can therefore misplace a
/// destination today, which is the only reason they are absent from
/// `PROBES` and from the routing guard.
///
/// If this test ever fails, one of them started working — and it went in
/// WITHOUT a cross-shard guard, which means it shipped the moon#592 defect.
/// Add it to `PROBES` and to `shared::cross_shard_write_rejection`'s family
/// list in the same change.
#[test]
fn t2k4_unimplemented_store_forms_stay_unimplemented_or_get_a_guard() {
    let m = spawn_moon(SHARDS);
    let mut c = Conn::open(m.port);
    // Seed both geo sources: with a MISSING source both commands short-circuit
    // to an empty array before the argv is ever parsed, so an unseeded probe
    // would assert nothing about STORE support.
    for src in ["t2k:gr:s", "t2k:gm:s"] {
        let seeded = c.send(&[
            "GEOADD",
            src,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ]);
        assert_eq!(seeded, ":2\r\n", "GEOADD must seed {src}");
    }
    let cases: &[(&str, &[&str])] = &[
        ("ZDIFFSTORE", &["ZDIFFSTORE", "t2k:zd:d", "1", "t2k:zd:s"]),
        (
            "GEORADIUS STORE",
            &[
                "GEORADIUS",
                "t2k:gr:s",
                "15",
                "37",
                "200",
                "km",
                "STORE",
                "t2k:gr:d",
            ],
        ),
        (
            "GEORADIUSBYMEMBER STORE",
            &[
                "GEORADIUSBYMEMBER",
                "t2k:gm:s",
                "Catania",
                "200",
                "km",
                "STORE",
                "t2k:gm:d",
            ],
        ),
    ];
    for (label, argv) in cases {
        let reply = c.send(argv);
        assert!(
            reply.starts_with('-'),
            "{label} now answers {reply:?} instead of an error. It writes a key it did \
             not route on, so it must be added to PROBES and to the cross-shard write \
             guard before it can ship (moon#592)."
        );
    }
}

/// The two paths that reach the keyspace WITHOUT passing the connection
/// handlers' pre-routing guard: a queued MULTI/EXEC body, and a `redis.call`
/// from Lua.
///
/// All three shapes were measured by hand at `--shards 4` before this test
/// existed:
///
/// * `MULTI ... RENAME src dst ... EXEC` was already safe —
///   `analyze_txn_locality` walks every key of every queued command, so a
///   straddling body is refused at `EXEC` and never runs. Covered here so it
///   stays that way.
/// * `EVAL` with BOTH keys declared was already safe: `route_script_keys`
///   refuses a straddling key set.
/// * `EVAL` with the destination arriving through `ARGV` was **NOT** safe.
///   Routing never saw that key, the script ran on the source's shard, and
///   `+OK` came back with `src` destroyed and `dst` never created. That is
///   the hole the guard in `bridge::make_redis_call_fn` closes.
#[test]
fn t2k5_transactions_and_scripts_cannot_ack_a_lost_write_either() {
    let m = spawn_moon(SHARDS);
    const RENAME_DECLARED: &str = "return redis.call('RENAME', KEYS[1], KEYS[2])";
    const RENAME_VIA_ARGV: &str = "return redis.call('RENAME', KEYS[1], ARGV[1])";

    let mut wrong: Vec<String> = Vec::new();
    for shape in ["multi", "eval-keys", "eval-argv"] {
        for i in 0..TRIALS {
            let (src, dst) = cross_shard_pair(shape, i);
            let mut c = Conn::open(m.port);
            assert_eq!(c.send(&["SET", &src, "VALUE-1"]), "+OK\r\n", "seed {src}");

            let reply = match shape {
                "multi" => {
                    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
                    assert_eq!(c.send(&["RENAME", &src, &dst]), "+QUEUED\r\n");
                    c.send(&["EXEC"])
                }
                "eval-keys" => c.send(&["EVAL", RENAME_DECLARED, "2", &src, &dst]),
                _ => c.send(&["EVAL", RENAME_VIA_ARGV, "1", &src, &dst]),
            };

            let src_now = c.send(&["GET", &src]);
            let dst_now = c.send(&["GET", &dst]);
            // A refused body is an error frame; a refused EXEC is one too
            // (moon rejects the whole transaction rather than running it).
            let refused = reply.starts_with('-');
            let ok = if refused {
                src_now == "$7\r\nVALUE-1\r\n" && dst_now == "$-1\r\n"
            } else {
                dst_now == "$7\r\nVALUE-1\r\n" && src_now == "$-1\r\n"
            };
            if !ok {
                wrong.push(format!(
                    "  {shape} [{src} -> {dst}]: reply={reply:?} src={src_now:?} \
                     dst={dst_now:?} ({})",
                    if src_now == "$-1\r\n" && dst_now == "$-1\r\n" {
                        "VALUE-1 is GONE from the keyspace"
                    } else {
                        "the reply and the keyspace disagree"
                    }
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {} transaction/script placements broke the acknowledgement contract \
         (moon#592):\n{}",
        wrong.len(),
        TRIALS * 3,
        wrong.join("\n")
    );
}
