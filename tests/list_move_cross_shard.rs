//! A list MOVE must never make an element disappear (moon#570).
//!
//! `BLMOVE`/`BRPOPLPUSH`/`LMOVE`/`RPOPLPUSH` pop from a source list and push to
//! a destination list. moon routes the whole command to the SOURCE's owning
//! shard, which then executed both halves against its own slice — so when the
//! destination belonged to a different shard the push landed in the wrong
//! shard's table, under the right name, where every normally-routed read of
//! the destination (which goes to the DESTINATION's owner) is blind to it.
//!
//! Measured on the pre-fix binary at `--shards 4`:
//!
//! ```text
//! BLMOVE     10 of 12 key placements: reply = the element, src empty, dst empty
//! LMOVE       6 of 6                  same
//! RPOPLPUSH  11 of 12                 same
//! ```
//!
//! The client was told the move succeeded and the element left the keyspace.
//!
//! ## What these tests assert
//!
//! Not "moon returns CROSSSLOT" — that would freeze today's remedy into the
//! suite and pass vacuously for any future implementation. They assert
//! **conservation**: after the command, exactly one copy of the element exists
//! and the reply agrees with where it is.
//!
//! ```text
//! reply == element  =>  dst holds it AND src does not
//! reply is an error =>  src still holds it AND dst does not
//! ```
//!
//! That invariant is satisfied by refusing the move (what moon does now) and
//! equally by a future cross-shard hop that only acks after the destination
//! applies. It is violated by the defect, and by any fix that acks before the
//! push is durable somewhere readable.
//!
//! `--shards 4` is the point: a connection is pinned to one shard, so ~3 of
//! every 4 key placements put the destination on a different shard than the
//! source. `TRIALS` distinct keys per assertion makes an all-co-located
//! (vacuous) run vanishingly unlikely.

mod common;

use common::Conn;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Four shards: source and destination land on different shards ~75% of the
/// time.
const SHARDS: &str = "4";
/// Distinct key placements per assertion. At p(cross-shard)=0.75 the chance
/// that all 12 land co-located — and test nothing — is under 1e-7.
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
        let tmp_dir = std::env::temp_dir().join(format!("moon-lmx-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-lmx-{port}"));
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

/// The conservation check, shared by every probe below.
///
/// `reply` is what the client was told; `src`/`dst` are the two key names.
/// Returns `Some(diagnosis)` when the element is not in exactly one place, or
/// is in a place the reply contradicts.
#[must_use]
fn conserved(c: &mut Conn, reply: &str, src: &str, dst: &str, elem: &str) -> Option<String> {
    let in_src = c.send(&["LRANGE", src, "0", "-1"]).contains(elem);
    let in_dst = c.send(&["LRANGE", dst, "0", "-1"]).contains(elem);
    let acked = reply.contains(elem);
    let refused = reply.starts_with('-');

    if acked && in_dst && !in_src {
        return None; // moved, and the reply says so
    }
    if refused && in_src && !in_dst {
        return None; // refused, and the element never left
    }
    Some(format!(
        "reply={reply:?} in_src={in_src} in_dst={in_dst} \
         (element is {}; a reply of the element with in_dst=false is acked data loss)",
        if in_src || in_dst {
            "misplaced relative to the reply"
        } else {
            "GONE from the keyspace"
        }
    ))
}

/// How many clients the server currently has PARKED on a blocking command.
///
/// Read from `INFO clients`, which is the server's own account rather than the
/// test's guess. `lmx2` uses it as the handshake that proves a waiter reached
/// the registry before the producer runs — see the comment at its call site
/// for why a `sleep` cannot do that job.
fn blocked_clients(c: &mut Conn) -> u32 {
    c.send(&["INFO", "clients"])
        .lines()
        .find_map(|l| l.trim().strip_prefix("blocked_clients:")?.parse().ok())
        .unwrap_or(0)
}

/// Run `body` once per key placement on a FRESH connection and report every
/// trial that came out wrong.
///
/// Fresh connections on purpose: which shard a connection lands on is what
/// decides the placement, so reusing one would sample a single placement
/// `TRIALS` times instead of the distribution.
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

/// `BLMOVE`/`BRPOPLPUSH` served from the IMMEDIATE path — the source already
/// holds data, so the command never blocks and `immediate_scan` answers it.
#[test]
fn lmx1_blocking_move_immediate_path_never_loses_the_element() {
    let m = spawn_moon(SHARDS);
    let probes: &[(&str, &[&str])] = &[
        ("BLMOVE", &["BLMOVE", "{s}", "{d}", "LEFT", "RIGHT", "0"]),
        ("BRPOPLPUSH", &["BRPOPLPUSH", "{s}", "{d}", "0"]),
    ];
    for (label, argv) in probes {
        each_trial(
            m.port,
            &format!("lmx1:{label}:"),
            "moon#570 — a blocking move whose destination lives on another shard \
             acked the element and dropped it",
            |c, k| {
                let (src, dst) = (format!("{k}:s"), format!("{k}:d"));
                assert_eq!(
                    c.send(&["RPUSH", &src, "elem"]),
                    ":1\r\n",
                    "RPUSH must seed the source"
                );
                let parts: Vec<String> = argv
                    .iter()
                    .map(|p| match *p {
                        "{s}" => src.clone(),
                        "{d}" => dst.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                let reply = c.send(&refs);
                conserved(c, &reply, &src, &dst, "elem").map(|d| format!("{label}: {d}"))
            },
        );
    }
}

/// The same two commands served from the WAKE path — the source is empty, the
/// client blocks, and a second connection's `RPUSH` wakes it. This is the arm
/// in `blocking::wakeup`, a different execution site from `lmx1`'s.
#[test]
fn lmx2_blocking_move_wake_path_never_loses_the_element() {
    let m = spawn_moon(SHARDS);
    let probes: &[(&str, &[&str])] = &[
        ("BLMOVE", &["BLMOVE", "{s}", "{d}", "LEFT", "RIGHT", "5"]),
        ("BRPOPLPUSH", &["BRPOPLPUSH", "{s}", "{d}", "5"]),
    ];
    for (label, argv) in probes {
        each_trial(
            m.port,
            &format!("lmx2:{label}:"),
            "moon#570 — a blocking move woken by a producer must not drop the \
             element when its destination is remote",
            |c, k| {
                let (src, dst) = (format!("{k}:s"), format!("{k}:d"));
                let parts: Vec<String> = argv
                    .iter()
                    .map(|p| match *p {
                        "{s}" => src.clone(),
                        "{d}" => dst.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();

                // Issue the move WITHOUT reading its reply: the source is
                // empty, so either it is refused up front (reply waiting now)
                // or the client is parked until the producer below runs.
                let mut waiter = TcpStream::connect(("127.0.0.1", m.port)).expect("connect waiter");
                waiter
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                waiter.write_all(&common::encode(&refs)).expect("write");

                // Do not sleep a fixed interval and hope — a delay too short
                // for a loaded runner lets the producer win the race, the move
                // is served by the producer-side immediate scan instead, and
                // the wake path this test exists for is never entered. The
                // test would still pass, silently covering nothing.
                //
                // Instead wait for the server's own account of what it did
                // with the command. It has exactly two terminal answers and
                // both are observable:
                //
                //   * it PARKED the client — `blocked_clients` rises, and the
                //     element can now only be delivered by the wake path;
                //   * it ANSWERED already — a complete reply frame is readable
                //     on the waiter socket (the refusal path never blocks).
                //
                // Anything else within the deadline is a hang, and is reported
                // as one rather than passing.
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                waiter
                    .set_read_timeout(Some(Duration::from_millis(20)))
                    .expect("poll timeout");
                let ready_by = Instant::now() + Duration::from_secs(10);
                let mut decided = false;
                while Instant::now() < ready_by {
                    match waiter.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        // A read timeout is the EXPECTED state while parked.
                        Err(_) => {}
                    }
                    if common::framed_len(&buf, 1).is_some() || blocked_clients(c) >= 1 {
                        decided = true;
                        break;
                    }
                }
                if !decided {
                    return Some(format!(
                        "{label}: server neither parked nor answered the waiter within 10s"
                    ));
                }

                assert_eq!(
                    c.send(&["RPUSH", &src, "elem"]),
                    ":1\r\n",
                    "producer RPUSH must succeed"
                );

                waiter
                    .set_read_timeout(Some(Duration::from_millis(200)))
                    .expect("read timeout");
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if common::framed_len(&buf, 1).is_some() {
                        break;
                    }
                    match waiter.read(&mut chunk) {
                        Ok(0) => break,
                        Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        Err(_) => {}
                    }
                }
                let reply = String::from_utf8_lossy(&buf).into_owned();
                if reply.is_empty() {
                    return Some(format!("{label}: waiter never answered"));
                }
                conserved(c, &reply, &src, &dst, "elem").map(|d| format!("{label}: {d}"))
            },
        );
    }
}

/// The non-blocking twins. `#570` names only `BLMOVE`/`BRPOPLPUSH`, but the
/// pre-fix binary lost 6 of 6 `LMOVE` placements and 11 of 12 `RPOPLPUSH`
/// placements the same way — same routing rule, same execution site.
#[test]
fn lmx3_plain_move_never_loses_the_element() {
    let m = spawn_moon(SHARDS);
    let probes: &[(&str, &[&str])] = &[
        ("LMOVE", &["LMOVE", "{s}", "{d}", "LEFT", "RIGHT"]),
        ("RPOPLPUSH", &["RPOPLPUSH", "{s}", "{d}"]),
    ];
    for (label, argv) in probes {
        each_trial(
            m.port,
            &format!("lmx3:{label}:"),
            "moon#570 — a non-blocking move whose destination lives on another \
             shard acked the element and dropped it",
            |c, k| {
                let (src, dst) = (format!("{k}:s"), format!("{k}:d"));
                assert_eq!(c.send(&["RPUSH", &src, "elem"]), ":1\r\n");
                let parts: Vec<String> = argv
                    .iter()
                    .map(|p| match *p {
                        "{s}" => src.clone(),
                        "{d}" => dst.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                let reply = c.send(&refs);
                conserved(c, &reply, &src, &dst, "elem").map(|d| format!("{label}: {d}"))
            },
        );
    }
}

/// The refusal must be narrow. A `{hash}`-tagged pair is provably co-located,
/// so every move below MUST still move — at four shards, and for every one of
/// the four commands.
///
/// Without this a fix that refuses every multi-shard move outright would pass
/// `lmx1`-`lmx3` while breaking the documented remedy.
#[test]
fn lmx4_colocated_moves_still_move_at_four_shards() {
    let m = spawn_moon(SHARDS);
    let probes: &[(&str, &[&str])] = &[
        ("LMOVE", &["LMOVE", "{s}", "{d}", "LEFT", "RIGHT"]),
        ("RPOPLPUSH", &["RPOPLPUSH", "{s}", "{d}"]),
        ("BLMOVE", &["BLMOVE", "{s}", "{d}", "LEFT", "RIGHT", "0"]),
        ("BRPOPLPUSH", &["BRPOPLPUSH", "{s}", "{d}", "0"]),
    ];
    for (label, argv) in probes {
        each_trial(
            m.port,
            &format!("lmx4:{label}:"),
            "a {hash}-tagged source/destination pair is co-located and must still move",
            |c, k| {
                // The tag is what routes: both keys hash to the SAME shard.
                let (src, dst) = (format!("{{{k}}}:s"), format!("{{{k}}}:d"));
                assert_eq!(c.send(&["RPUSH", &src, "elem"]), ":1\r\n");
                let parts: Vec<String> = argv
                    .iter()
                    .map(|p| match *p {
                        "{s}" => src.clone(),
                        "{d}" => dst.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                let reply = c.send(&refs);
                if !reply.contains("elem") {
                    return Some(format!(
                        "{label}: co-located move was not served: reply={reply:?}"
                    ));
                }
                conserved(c, &reply, &src, &dst, "elem").map(|d| format!("{label}: {d}"))
            },
        );
    }
}

/// `--shards 1` is the default deployment and has no shard boundary to cross:
/// every move must still work for arbitrary, untagged key names.
#[test]
fn lmx5_single_shard_moves_are_untouched() {
    let m = spawn_moon("1");
    let probes: &[(&str, &[&str])] = &[
        ("LMOVE", &["LMOVE", "{s}", "{d}", "LEFT", "RIGHT"]),
        ("RPOPLPUSH", &["RPOPLPUSH", "{s}", "{d}"]),
        ("BLMOVE", &["BLMOVE", "{s}", "{d}", "LEFT", "RIGHT", "0"]),
        ("BRPOPLPUSH", &["BRPOPLPUSH", "{s}", "{d}", "0"]),
    ];
    for (label, argv) in probes {
        each_trial(
            m.port,
            &format!("lmx5:{label}:"),
            "a single-shard server has no shard boundary: every move must be served",
            |c, k| {
                let (src, dst) = (format!("{k}:s"), format!("{k}:d"));
                assert_eq!(c.send(&["RPUSH", &src, "elem"]), ":1\r\n");
                let parts: Vec<String> = argv
                    .iter()
                    .map(|p| match *p {
                        "{s}" => src.clone(),
                        "{d}" => dst.clone(),
                        other => other.to_string(),
                    })
                    .collect();
                let refs: Vec<&str> = parts.iter().map(String::as_str).collect();
                let reply = c.send(&refs);
                if !reply.contains("elem") {
                    return Some(format!(
                        "{label}: single-shard move was not served: reply={reply:?}"
                    ));
                }
                conserved(c, &reply, &src, &dst, "elem").map(|d| format!("{label}: {d}"))
            },
        );
    }
}

/// The rotate form (`LMOVE k k`) is ONE key. It can never be cross-shard and
/// must not be swept up by a hash comparison that forgets to special-case it.
#[test]
fn lmx6_rotate_form_is_never_refused() {
    let m = spawn_moon(SHARDS);
    each_trial(
        m.port,
        "lmx6:",
        "LMOVE k k RIGHT LEFT rotates a single key and is never cross-shard",
        |c, k| {
            assert_eq!(c.send(&["RPUSH", k, "a", "b", "c"]), ":3\r\n");
            let reply = c.send(&["LMOVE", k, k, "RIGHT", "LEFT"]);
            if !reply.contains('c') {
                return Some(format!("rotate refused or empty: reply={reply:?}"));
            }
            let got = c.send(&["LRANGE", k, "0", "-1"]);
            (!got.contains('c') || !got.contains('a'))
                .then(|| format!("rotate lost elements: LRANGE={got:?}"))
        },
    );
}
