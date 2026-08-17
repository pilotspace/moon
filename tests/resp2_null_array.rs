//! RESP2 Null Array (`*-1`) vs Null Bulk (`$-1`) — moon#482.
//!
//! `Frame` has one `Null` variant that always serialises `$-1`, so every reply
//! whose missing value is an *array* puts the wrong type on the wire. A
//! statically-typed client decodes `*-1` and `$-1` differently, so this is a
//! decode error client-side, not a cosmetic difference.
//!
//! Every expectation here was MEASURED against `redis-server 8.6.1` on
//! 2026-08-17 rather than read from the docs — the probe gives each case its
//! own never-written key and asserts `EXISTS == 0` first. Two results are
//! counter-intuitive enough to call out, because a reviewer checking them
//! against intuition would "correct" them into bugs:
//!
//!   * `BRPOPLPUSH` and `BLMOVE` reply `*-1` on timeout even though a
//!     SUCCESSFUL reply is a bulk string.
//!   * `GEOPOS` of an absent member is `*-1`, but `GEOHASH` of an absent
//!     member — same command family, same source file — is `$-1`.
//!
//! The regression fence (`rna7`) matters as much as the fixes: the audit can
//! fail in both directions, and ~31 sites measured CORRECT today outnumber the
//! 16 that are wrong.

mod common;

use common::Conn;

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

fn spawn_moon(shards: &str) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-nullarr-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-nullarr-{port}"));
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

/// Assert the EXACT reply bytes. The whole defect is a one-character
/// difference in the type byte, so anything that normalises the reply (a
/// client library, a "does it look null" helper) would hide it.
#[track_caller]
fn assert_reply(c: &mut Conn, cmd: &[&str], want: &str, why: &str) {
    let got = c.send(cmd);
    assert_eq!(
        got, want,
        "\n  command : {cmd:?}\n  want    : {want:?}\n  got     : {got:?}\n  why     : {why}"
    );
}

const NULL_ARRAY: &str = "*-1\r\n";
const NULL_BULK: &str = "$-1\r\n";
const EMPTY_ARRAY: &str = "*0\r\n";

#[test]
fn rna1_blocking_timeouts_are_null_array() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    // All eight blocking commands, measured on redis 8.6.1: every one of them
    // replies *-1 on timeout.
    let cases: &[(&[&str], &str)] = &[
        (&["BLPOP", "nb1", "0.05"], "BLPOP"),
        (&["BRPOP", "nb2", "0.05"], "BRPOP"),
        (
            &["BLMOVE", "nb3", "nb3d", "LEFT", "RIGHT", "0.05"],
            "BLMOVE",
        ),
        (&["BRPOPLPUSH", "nb4", "nb4d", "0.05"], "BRPOPLPUSH"),
        (&["BLMPOP", "0.05", "1", "nb5", "LEFT"], "BLMPOP"),
        (&["BZPOPMIN", "nb6", "0.05"], "BZPOPMIN"),
        (&["BZPOPMAX", "nb7", "0.05"], "BZPOPMAX"),
        (&["BZMPOP", "0.05", "1", "nb8", "MIN"], "BZMPOP"),
    ];
    for (cmd, name) in cases {
        assert_reply(
            &mut c,
            cmd,
            NULL_ARRAY,
            &format!("moon#482 — {name} timing out is a missing ARRAY in Redis"),
        );
    }

    // The contrast that proves the fix is targeted, not a blanket rewrite: a
    // missing STRING in the same server on the same connection is still $-1.
    assert_reply(
        &mut c,
        &["GET", "nb-absent-string"],
        NULL_BULK,
        "GET's missing value is a string — it must NOT become a null array",
    );
}

#[test]
fn rna2_resp3_collapses_both_nulls_to_underscore() {
    // RESP3 has one null for both shapes, so the new variant must not leak a
    // `*-1` into a RESP3 conversation.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let hello = c.send(&["HELLO", "3"]);
    assert!(
        hello.starts_with('%'),
        "HELLO 3 should return a RESP3 map, got {hello:?}"
    );

    assert_reply(
        &mut c,
        &["BLPOP", "r3a", "0.05"],
        "_\r\n",
        "moon#482 — under RESP3 the null array is the plain null",
    );
}

#[test]
#[ignore = "moon#522 — inline GET writes a hardcoded \\$-1, bypassing the \
            protocol-version-aware serializer; un-ignore when #522 lands"]
fn rna2b_resp3_get_miss_is_underscore() {
    // Written as the CONTRAST half of rna2 — "the null bulk collapses too" —
    // and it failed, which is how #522 was found. Kept here rather than
    // deleted: the assertion is already correct, only the capability is
    // missing, so un-ignoring this test IS #522's acceptance criterion.
    //
    // 4 shards on purpose. The defect is per-KEY: a key the connection's own
    // shard owns takes the inline path and gets `$-1`, while a key that routes
    // elsewhere comes back through the serializer and gets `_`. A single-shard
    // test would report "always wrong" and miss that it is a split.
    let m = spawn_moon("4");
    let mut c = Conn::open(m.port);
    assert!(c.send(&["HELLO", "3"]).starts_with('%'));

    let mut wrong = Vec::new();
    for i in 0..8 {
        let key = format!("r3get{i}");
        let got = c.send(&["GET", &key]);
        if got != "_\r\n" {
            wrong.push(format!("  {key}: {got:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "moon#522 — RESP3 GET on a missing key must reply `_`; {} of 8 keys \
         replied the RESP2 null bulk instead (the split is by shard \
         ownership, not by server):\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

#[test]
fn rna3_exec_aborted_by_watch_is_null_array() {
    // The highest-impact site in the table: every client library decodes EXEC
    // as an array, so the abort path — the one optimistic-locking code is
    // WRITTEN to handle — is the one that mis-decodes today.
    //
    // Run at 1 and 4 shards because there are two transaction executors
    // (`shared.rs:268` embedded, `shared.rs:380` CAS-gate) and which one runs
    // depends on the deployment shape.
    for shards in ["1", "4"] {
        let m = spawn_moon(shards);
        let mut a = Conn::open(m.port);
        let mut b = Conn::open(m.port);

        assert!(a.send(&["SET", "wk", "1"]).starts_with("+OK"));
        assert!(a.send(&["WATCH", "wk"]).starts_with("+OK"));
        assert!(a.send(&["MULTI"]).starts_with("+OK"));
        let queued = a.send(&["SET", "wk", "from-txn"]);
        assert!(
            queued.starts_with("+QUEUED"),
            "expected +QUEUED at {shards} shards, got {queued:?}"
        );

        // Break the watch from another connection.
        assert!(b.send(&["SET", "wk", "2"]).starts_with("+OK"));

        let exec = a.send(&["EXEC"]);
        assert_eq!(
            exec, NULL_ARRAY,
            "moon#482 — EXEC aborted by a broken WATCH is *-1 in Redis \
             (shards={shards}); got {exec:?}"
        );

        // The abort must be a real abort, not just a differently-typed reply.
        let after = b.send(&["GET", "wk"]);
        assert!(
            after.contains('2') && !after.contains("from-txn"),
            "the queued command must NOT have run (shards={shards}); GET wk = {after:?}"
        );
    }
}

#[test]
fn rna4_pop_with_count_on_an_absent_key_is_null_array() {
    // These two are NOT `Frame::Null` sites today — they return `*0`. A pass
    // that only rewrites `Frame::Null` finds neither, which is why the issue
    // insists the audit is the task.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    assert_reply(
        &mut c,
        &["LPOP", "pc1", "2"],
        NULL_ARRAY,
        "moon#482 — LPOP with a count on an absent key is *-1, not *0",
    );
    assert_reply(
        &mut c,
        &["RPOP", "pc2", "2"],
        NULL_ARRAY,
        "moon#482 — RPOP with a count on an absent key is *-1, not *0",
    );

    // The fence in the same breath: an EMPTY aggregate is a different reply
    // and must stay empty. Fixing the two above by turning `*0` into `*-1`
    // everywhere would break these.
    assert_reply(
        &mut c,
        &["ZPOPMIN", "pc3"],
        EMPTY_ARRAY,
        "ZPOPMIN on an absent key is an EMPTY array in Redis, not a null one",
    );
    assert_reply(
        &mut c,
        &["SPOP", "pc4", "2"],
        EMPTY_ARRAY,
        "SPOP with a count on an absent key is an EMPTY array in Redis",
    );
}

#[test]
fn rna5_geopos_nests_a_null_array_but_geohash_does_not() {
    // The null array must be a normal composable variant: GEOPOS puts one
    // INSIDE the outer array. And the trap in the same source file: GEOHASH's
    // absent member is a null BULK. Both measured on redis 8.6.1.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    let added = c.send(&["GEOADD", "geo1", "13.361389", "38.115556", "here"]);
    assert!(added.starts_with(":1"), "GEOADD did not add: {added:?}");

    assert_reply(
        &mut c,
        &["GEOPOS", "geo1", "no-such-member"],
        "*1\r\n*-1\r\n",
        "moon#482 — GEOPOS nests a NULL ARRAY for an absent member",
    );
    assert_reply(
        &mut c,
        &["GEOHASH", "geo1", "no-such-member"],
        "*1\r\n$-1\r\n",
        "GEOHASH's absent member is a null BULK — same file, opposite null; \
         do not 'fix' it to match GEOPOS",
    );
}

#[test]
fn rna6_lmpop_zmpop_and_xread_are_null_array() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    assert_reply(
        &mut c,
        &["LMPOP", "1", "mp1", "LEFT"],
        NULL_ARRAY,
        "moon#482 — LMPOP finding no non-empty key is *-1",
    );
    assert_reply(
        &mut c,
        &["ZMPOP", "1", "mp2", "MIN"],
        NULL_ARRAY,
        "moon#482 — ZMPOP finding no non-empty key is *-1",
    );
    assert_reply(
        &mut c,
        &["XREAD", "COUNT", "1", "STREAMS", "mp3", "0-0"],
        NULL_ARRAY,
        "moon#482 — XREAD on an absent stream is *-1",
    );

    // A stream that EXISTS but has nothing past the given ID is the same
    // "no data" answer, and takes a different code path.
    let added = c.send(&["XADD", "mp4", "1-1", "f", "v"]);
    assert!(added.starts_with('$'), "XADD failed: {added:?}");
    assert_reply(
        &mut c,
        &["XREAD", "COUNT", "1", "STREAMS", "mp4", "9999-0"],
        NULL_ARRAY,
        "moon#482 — XREAD past the end of a LIVE stream is also *-1",
    );

    // XREADGROUP lives in stream_write.rs while XREAD lives in
    // stream_read.rs, so the first sweep for #482 walked past it. Measured
    // against redis-server 8.6.1: `*-1`, same as XREAD.
    let created = c.send(&["XGROUP", "CREATE", "mp5", "g", "$", "MKSTREAM"]);
    assert!(
        created.starts_with('+'),
        "XGROUP CREATE failed: {created:?}"
    );
    assert_reply(
        &mut c,
        &[
            "XREADGROUP",
            "GROUP",
            "g",
            "consumer",
            "COUNT",
            "1",
            "STREAMS",
            "mp5",
            ">",
        ],
        NULL_ARRAY,
        "moon#482 — XREADGROUP with no new entries is *-1",
    );
}

#[test]
fn rna7_regression_fence_the_already_correct_sites_do_not_move() {
    // This is the test that fails if the audit over-reaches. Every row was
    // measured as ALREADY MATCHING redis 8.6.1; they outnumber the fixes.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);

    // Fixtures so the "wrong shape" cases are about the reply, not the key.
    assert!(c.send(&["RPUSH", "f-list", "a"]).starts_with(":1"));
    assert!(c.send(&["ZADD", "f-zset", "1", "m"]).starts_with(":1"));
    assert!(c.send(&["SADD", "f-set", "x"]).starts_with(":1"));

    let still_null_bulk: &[(&[&str], &str)] = &[
        (&["GET", "fence1"], "GET"),
        (&["HGET", "fence2", "f"], "HGET"),
        (&["LPOP", "fence3"], "LPOP without a count"),
        (&["LMOVE", "fence4", "fence4d", "LEFT", "RIGHT"], "LMOVE"),
        (&["LPOS", "fence5", "a"], "LPOS without COUNT"),
        (&["LINDEX", "fence6", "0"], "LINDEX"),
        (&["ZSCORE", "fence7", "m"], "ZSCORE"),
        (&["ZRANK", "f-zset", "absent-member"], "ZRANK"),
        (&["ZRANDMEMBER", "fence8"], "ZRANDMEMBER without a count"),
        (&["SPOP", "fence9"], "SPOP without a count"),
        (&["SRANDMEMBER", "fence10"], "SRANDMEMBER without a count"),
        (&["HRANDFIELD", "fence11"], "HRANDFIELD without a count"),
        (&["GETDEL", "fence12"], "GETDEL"),
        (&["GETEX", "fence13"], "GETEX"),
        (&["OBJECT", "ENCODING", "fence14"], "OBJECT ENCODING"),
        (&["CLIENT", "GETNAME"], "CLIENT GETNAME with no name set"),
    ];
    for (cmd, name) in still_null_bulk {
        assert_reply(
            &mut c,
            cmd,
            NULL_BULK,
            &format!("{name} is a missing STRING — it must stay $-1 (moon#482 fence)"),
        );
    }

    let still_empty_array: &[(&[&str], &str)] = &[
        (&["ZPOPMIN", "fence20"], "ZPOPMIN"),
        (&["SRANDMEMBER", "fence21", "2"], "SRANDMEMBER with a count"),
        (&["ZRANDMEMBER", "fence22", "2"], "ZRANDMEMBER with a count"),
        (&["HRANDFIELD", "fence23", "2"], "HRANDFIELD with a count"),
        (&["XRANGE", "fence24", "-", "+"], "XRANGE"),
        (&["SMEMBERS", "fence25"], "SMEMBERS"),
        (&["HGETALL", "fence26"], "HGETALL"),
        (&["LPOS", "fence27", "a", "COUNT", "2"], "LPOS with COUNT"),
        (&["KEYS", "no-such-prefix:*"], "KEYS with no match"),
    ];
    for (cmd, name) in still_empty_array {
        assert_reply(
            &mut c,
            cmd,
            EMPTY_ARRAY,
            &format!(
                "{name} on an absent key is an EMPTY array — it must stay *0 (moon#482 fence)"
            ),
        );
    }
}

#[test]
fn rna8_a_bare_null_array_command_is_refused_not_dispatched() {
    // `parse_frame_zerocopy` parses inbound COMMANDS with the same function it
    // uses for peer replies, so teaching it `*-1` changes the command path
    // too. Malformed client input must never crash the server and must never
    // be mistaken for a command.
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    assert!(c.send(&["SET", "canary", "before"]).starts_with("+OK"));

    let mut raw = TcpStream::connect(("127.0.0.1", m.port)).expect("connect");
    raw.set_read_timeout(Some(Duration::from_millis(500)))
        .expect("set timeout");
    raw.write_all(b"*-1\r\n").expect("write null array");
    let mut buf = [0u8; 256];
    let reply = match raw.read(&mut buf) {
        Ok(n) => String::from_utf8_lossy(&buf[..n]).into_owned(),
        Err(_) => String::new(), // no reply at all is an acceptable answer
    };
    assert!(
        !reply.starts_with('+') || reply.starts_with("-ERR"),
        "a bare *-1 must not be answered as a successful command, got {reply:?}"
    );

    // The server is still alive and the keyspace is untouched.
    assert!(
        c.send(&["PING"]).starts_with("+PONG"),
        "server must survive a bare *-1"
    );
    assert!(
        c.send(&["GET", "canary"]).contains("before"),
        "a bare *-1 must not have mutated the keyspace"
    );
}
