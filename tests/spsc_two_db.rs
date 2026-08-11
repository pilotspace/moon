//! Gap A: cross-shard `MOVE`/`COPY ... DB n` two-database intercept must run
//! on EVERY `ShardMessage` SPSC arm, not just the plain `Execute` arm.
//!
//! Before this fix, the two-database intercept
//! (`src/shard/spsc_handler.rs`'s `MOVE`/`COPY` special-casing, now
//! extracted into `src/shard/spsc_two_db::try_two_db_intercept`) existed
//! ONLY in the plain `Execute` arm. Client traffic's actual cross-shard
//! write path — both single-command and pipelined — funnels through
//! `PipelineBatchSlotted` (a batch-of-one for the single-command case), so a
//! remote `COPY src dst DB n` fell through to the generic single-db
//! `key_extra::copy`, which parses (and silently ignores) the `DB` clause
//! and performs a same-db copy instead (confirmed pre-fix via manual probe
//! in `tests/oom_bypass_closure.rs`'s case E: 4/20 remote `COPY`s landed in
//! the wrong db). `MOVE` returned a loud-but-wrong "cross-db not supported"
//! error on the same arm.
//!
//! `Execute`/`MultiExecute`/`ExecuteSlotted`/`MultiExecuteSlotted` are NOT
//! reachable from ordinary client traffic today (`Execute` is the
//! admin-console path; `MultiExecute`/`MultiExecuteSlotted` are the VLL
//! coordinator's multi-key scatter path for MGET/MSET/multi-DEL, which
//! never carries MOVE/COPY; `ExecuteSlotted`/`MultiExecuteSlotted` have no
//! live constructor at all). The two wire-level cases below exercise the
//! one arm real traffic actually uses (`PipelineBatchSlotted`), which calls
//! the shared `try_two_db_intercept` helper (`src/shard/spsc_two_db.rs`)
//! verbatim — the same helper every other arm calls — so this coverage
//! transitively validates the logic the dead/admin-only arms share, though
//! it does not independently exercise those arms' own call sites. No direct
//! unit test of `try_two_db_intercept` exists yet (a documented gap, not a
//! claim of coverage that isn't there).
//!
//! Run alone with:
//!   MOON_BIN=$PWD/target/release/moon cargo test --test spsc_two_db

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Binary resolution + server spawn (pattern: tests/oom_bypass_closure.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> std::path::PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    // Fall back to the binary cargo built for THIS test run: compile-time
    // path with the right profile, CARGO_TARGET_DIR, and .exe suffix on
    // Windows (the old target/{release,debug}/moon probing found nothing on
    // Windows and could pick a stale release binary).
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

/// Rooted under the repo's own volume, not system `$TMPDIR` (diskfull-guard
/// trap — see gotcha_vm_diskfull_shared_volume in project memory).
fn test_tmpdir() -> tempfile::TempDir {
    let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/spsc-2db-tmp");
    std::fs::create_dir_all(&base).expect("create spsc-2db-tmp base dir");
    tempfile::Builder::new()
        .prefix("spsc-2db-")
        .tempdir_in(&base)
        .expect("tempdir_in target/spsc-2db-tmp")
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        // Belt-and-suspenders backstop against a leaked busy-poller (see
        // gotcha_leaked_moon_busypoller_contaminates_xshard in project memory).
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Spawn moon on a self-chosen port (see `common::spawn_listening` for the
/// dead-child respawn semantics) and return the guard + the live port.
fn spawn_moon(dir: &std::path::Path, shards: u32) -> (ServerGuard, u16) {
    let (child, port) = common::spawn_listening(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // COPY/MOVE routing is under test, not the disk guard; a
                // near-full dev volume would otherwise fail every setup SET
                // with MOONERR diskfull.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    });
    (ServerGuard(child), port)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (binary-safe args, full-frame parser)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
enum V {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
    Null,
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().expect("int")),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                V::Arr((0..n).map(|_| self.parse()).collect())
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    /// Send all commands in ONE write (a wire pipeline), then read all replies.
    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut buf = Vec::new();
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            buf.extend_from_slice(&Self::encode(&refs));
        }
        self.writer.write_all(&buf).expect("send pipeline");
        cmds.iter().map(|_| self.parse()).collect()
    }

    fn try_ping(&mut self) -> std::io::Result<bool> {
        self.writer.write_all(b"*1\r\n$4\r\nPING\r\n")?;
        let mut buf = [0u8; 7];
        self.reader.read_exact(&mut buf)?;
        Ok(&buf == b"+PONG\r\n")
    }
}

fn wait_ready(port: u16) -> Client {
    let start = Instant::now();
    loop {
        let mut c = Client::connect(port);
        if let Ok(true) = c.try_ping() {
            return c;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "server never answered PING on port {port}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn blob(size: usize, fill: u8) -> Vec<u8> {
    vec![fill; size]
}

// ---------------------------------------------------------------------------
// Case 1: pipelined cross-shard `COPY src dst DB 1`. THE RED CASE — before
// the fix, `PipelineBatchSlotted` (the arm real client traffic uses for
// remote writes) fell through to the generic single-db `key_extra::copy`,
// so `dst` landed in db 0 instead of db 1.
// ---------------------------------------------------------------------------

#[test]
fn test_pipelined_cross_shard_copy_db_n() {
    let dir = test_tmpdir();
    let (_guard, port) = spawn_moon(dir.path(), 4);
    let mut c = wait_ready(port);

    const N: usize = 20;
    let values: Vec<Vec<u8>> = (0..N).map(|i| blob(32, i as u8)).collect();

    // Keys use a `{i}` hash tag so src and dst hash to the SAME shard for a
    // given pair `i`, while still spreading ~N pairs across all 4 shards
    // (tag `i` varies, so different pairs land on different shards — this
    // still routes through PipelineBatchSlotted for most pairs).
    //
    // This is NOT optional plumbing: COPY's destination key is a genuinely
    // DIFFERENT string than the source key. Moon shards purely by key hash
    // (no db-awareness), and the two-db intercept executes on whichever
    // shard OWNS THE SOURCE KEY (that's the routing key for the whole
    // command) — it does not (and structurally cannot, without a real
    // cross-shard two-phase protocol, which is out of Gap A's scope; see
    // the CHANGELOG "found, not fixed" note) relocate the destination write
    // to whichever shard the destination key's OWN hash would naturally
    // pick. So a later plain `GET dst` — which routes by hash(dst) — only
    // reliably finds the value if src and dst are hash-tag-colocated. This
    // is a pre-existing, inherent property of Moon's per-key-hash sharding
    // model applied to MOVE/COPY (the plain `Execute` arm this fix mirrors
    // has the identical property) — not a Gap A regression. Verified: the
    // original (untagged) form of this test intermittently succeeds for
    // ONLY the ~1-in-4 keys where hash(dst) happens to collide with
    // hash(src) by chance, and passes reliably at `--shards 1` where the
    // question is moot.
    let src_key = |i: usize| format!("{{{i}}}:src");
    let dst_key = |i: usize| format!("{{{i}}}:copy");

    // Setup: SET N keys in db 0.
    let set_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| vec![b"SET".to_vec(), src_key(i).into_bytes(), values[i].clone()])
        .collect();
    let set_replies = c.pipeline(&set_cmds);
    for (i, r) in set_replies.iter().enumerate() {
        assert_eq!(
            *r,
            V::Simple("OK".into()),
            "setup SET {} failed: {r:?}",
            src_key(i)
        );
    }

    // Pipelined COPY ... DB 1 for every key (>= 2 commands in one wire
    // write, so this routes through PipelineBatch/PipelineBatchSlotted, not
    // a lone ExecuteSlotted).
    let copy_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"COPY".to_vec(),
                src_key(i).into_bytes(),
                dst_key(i).into_bytes(),
                b"DB".to_vec(),
                b"1".to_vec(),
            ]
        })
        .collect();
    let copy_replies = c.pipeline(&copy_cmds);
    for (i, r) in copy_replies.iter().enumerate() {
        assert_eq!(
            *r,
            V::Int(1),
            "COPY {} -> {} DB 1 did not report success: {r:?}",
            src_key(i),
            dst_key(i)
        );
    }

    // Verify per key: the copy landed in db 1, NOT db 0.
    for (i, val) in values.iter().enumerate() {
        let dst = dst_key(i);

        assert_eq!(
            c.cmd(&[b"SELECT", b"1"]),
            V::Simple("OK".into()),
            "SELECT 1 failed"
        );
        let in_db1 = c.cmd(&[b"GET", dst.as_bytes()]);
        assert_eq!(
            in_db1,
            V::Bulk(val.clone()),
            "{dst} missing/wrong value in db 1 (target db) — the two-db \
             intercept did not run on the SPSC arm this pipeline used"
        );

        assert_eq!(
            c.cmd(&[b"SELECT", b"0"]),
            V::Simple("OK".into()),
            "SELECT 0 failed"
        );
        let in_db0 = c.cmd(&[b"GET", dst.as_bytes()]);
        assert_eq!(
            in_db0,
            V::Null,
            "{dst} leaked into db 0 — COPY ... DB 1 silently performed \
             a same-db copy instead of a cross-db one (the pre-fix bug)"
        );

        // Source key is untouched by COPY (still in db 0).
        let src_still_present = c.cmd(&[b"GET", src_key(i).as_bytes()]);
        assert_eq!(
            src_still_present,
            V::Bulk(val.clone()),
            "COPY must not remove the source key {} from db 0",
            src_key(i)
        );
    }
}

// ---------------------------------------------------------------------------
// Case 2: pipelined cross-shard `MOVE key 1`. Pre-fix, this arm returned the
// defensive "MOVE requires handler-level dispatch (cross-db operation)"
// error instead of moving the key (loud, but wrong).
// ---------------------------------------------------------------------------

#[test]
fn test_pipelined_cross_shard_move() {
    let dir = test_tmpdir();
    let (_guard, port) = spawn_moon(dir.path(), 4);
    let mut c = wait_ready(port);

    const N: usize = 20;
    let values: Vec<Vec<u8>> = (0..N)
        .map(|i| blob(32, (i as u8).wrapping_add(1)))
        .collect();

    let set_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("mv:{i}").into_bytes(),
                values[i].clone(),
            ]
        })
        .collect();
    let set_replies = c.pipeline(&set_cmds);
    for (i, r) in set_replies.iter().enumerate() {
        assert_eq!(*r, V::Simple("OK".into()), "setup SET mv:{i} failed: {r:?}");
    }

    let move_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"MOVE".to_vec(),
                format!("mv:{i}").into_bytes(),
                b"1".to_vec(),
            ]
        })
        .collect();
    let move_replies = c.pipeline(&move_cmds);
    for (i, r) in move_replies.iter().enumerate() {
        assert_eq!(
            *r,
            V::Int(1),
            "MOVE mv:{i} 1 did not report success: {r:?} — pipelined \
             cross-shard MOVE is not intercepted on this SPSC arm"
        );
    }

    for (i, val) in values.iter().enumerate() {
        let key = format!("mv:{i}");

        assert_eq!(
            c.cmd(&[b"SELECT", b"0"]),
            V::Simple("OK".into()),
            "SELECT 0 failed"
        );
        let in_db0 = c.cmd(&[b"GET", key.as_bytes()]);
        assert_eq!(
            in_db0,
            V::Null,
            "mv:{i} still present in db 0 after MOVE — key was not removed \
             from the source db"
        );

        assert_eq!(
            c.cmd(&[b"SELECT", b"1"]),
            V::Simple("OK".into()),
            "SELECT 1 failed"
        );
        let in_db1 = c.cmd(&[b"GET", key.as_bytes()]);
        assert_eq!(
            in_db1,
            V::Bulk(val.clone()),
            "mv:{i} missing/wrong value in db 1 after MOVE"
        );
    }
}

// ---------------------------------------------------------------------------
// Case 3: control — same-db `COPY` (no `DB` clause) still works pipelined
// cross-shard. Locks the "no DB clause / same-db falls through to the
// generic path" branch of `try_two_db_intercept` against a regression.
// ---------------------------------------------------------------------------

#[test]
fn test_pipelined_same_db_copy_control() {
    let dir = test_tmpdir();
    let (_guard, port) = spawn_moon(dir.path(), 4);
    let mut c = wait_ready(port);

    const N: usize = 20;
    let values: Vec<Vec<u8>> = (0..N)
        .map(|i| blob(32, (i as u8).wrapping_add(2)))
        .collect();

    let set_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"SET".to_vec(),
                format!("sd:{i}").into_bytes(),
                values[i].clone(),
            ]
        })
        .collect();
    let set_replies = c.pipeline(&set_cmds);
    for (i, r) in set_replies.iter().enumerate() {
        assert_eq!(*r, V::Simple("OK".into()), "setup SET sd:{i} failed: {r:?}");
    }

    let copy_cmds: Vec<Vec<Vec<u8>>> = (0..N)
        .map(|i| {
            vec![
                b"COPY".to_vec(),
                format!("sd:{i}").into_bytes(),
                format!("sd:{i}:copy").into_bytes(),
            ]
        })
        .collect();
    let copy_replies = c.pipeline(&copy_cmds);
    for (i, r) in copy_replies.iter().enumerate() {
        assert_eq!(
            *r,
            V::Int(1),
            "same-db COPY sd:{i} -> sd:{i}:copy did not report success: {r:?}"
        );
    }

    for (i, val) in values.iter().enumerate() {
        let dst = format!("sd:{i}:copy");
        let got = c.cmd(&[b"GET", dst.as_bytes()]);
        assert_eq!(
            got,
            V::Bulk(val.clone()),
            "same-db COPY control regressed: sd:{i}:copy missing/wrong value"
        );
    }
}
