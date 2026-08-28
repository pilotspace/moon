//! A pipeline must execute in order — including the commands that reach
//! across shards. (moon#507)
//!
//! Redis executes a pipelined batch in order, so a command must observe every
//! write that acked earlier in its own batch. Moon's sharded pipeline handlers
//! break that: single-key commands whose key lives on another shard are
//! DEFERRED into `remote_groups` and dispatched as one `PipelineBatchSlotted`
//! at the end of the batch ("Phase 2b"), while multi-key and keyless commands
//! execute INLINE, mid-loop. An inline command therefore runs against a shard
//! whose earlier writes have not even been sent yet.
//!
//! Filed as an `MGET`-returns-nulls bug. Measured, it is wider than that and
//! not only a stale read — the write ORDERING inverts:
//!
//! ```text
//! shards=2, 20 trials each, against the unfixed build
//!   SET a, MSET a       7/20   MSET's value LOST — the earlier SET lands after it
//!   SET, FLUSHALL      10/20   key survives a flush it was told succeeded
//!   SET,SET, DEL        6/20   keys survive a DEL that returned success
//!   SET,SET, MGET      10/20   the filed symptom
//!   SET, SCAN (full)          a page taken against a stale shard skips the
//!                             key permanently — the cursor never comes back
//!   SET, DBSIZE        12/20   also KEYS, RANDOMKEY, EXISTS, UNLINK,
//!                              COPY, BITOP, INFO keyspace
//!   SET, TOUCH k        0/20   single-key: correct, and that is the fix's shape
//!   SET, TYPE k         0/20
//! ```
//!
//! Rate rises with shard count (a key is remote with probability
//! `1 - 1/shards`), so these run at `--shards 4` where a single trial is wrong
//! ~75% of the time — but they still loop, because ONE trial that happens to
//! land its keys locally proves nothing. Every assertion is written so that the
//! FIXED state is the passing state.
//!
//! Raw sockets, because the whole bug is about what is in one TCP write: a
//! client library free to split the batch would hide it.

mod common;

use common::{Conn, framed_len};

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Enough shards that a key is remote ~75% of the time, so a run that got
/// lucky on placement is not mistaken for a fix.
const SHARDS: &str = "4";
/// Distinct key groups per assertion. At p(remote) = 0.75 the chance that all
/// 12 land locally — and vacuously pass — is under 1e-7.
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
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-pipeorder-{port}"));
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
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(
                std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr log"),
            )
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-pipeorder-{port}"));
    let mut moon = Moon {
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
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

/// Cursor from a `SCAN` reply (`*2\r\n$<n>\r\n<cursor>\r\n<keys…>`), or
/// `None` when the reply is not a scan page.
fn scan_cursor(reply: &str) -> Option<String> {
    let after = reply.split_once("*2\r\n")?.1;
    let after = after.strip_prefix('$')?;
    let (_len, rest) = after.split_once("\r\n")?;
    let (cursor, _) = rest.split_once("\r\n")?;
    Some(cursor.to_string())
}

/// Run `body` for each of `TRIALS` distinct key groups on a FRESH connection,
/// collecting the trials that came out wrong.
///
/// Fresh connections on purpose: a connection's own shard decides whether its
/// keys are local, and reusing one would sample a single placement over and
/// over instead of the distribution.
fn each_trial(port: u16, tag: &str, mut body: impl FnMut(&mut Conn, &str) -> Option<String>) {
    let mut wrong: Vec<String> = Vec::new();
    for i in 0..TRIALS {
        let mut c = Conn::open(port);
        // A hash tag co-locates the group on one shard — the idiom CLAUDE.md
        // recommends for exactly this kind of multi-key access, and the one
        // the bug report found broken.
        let key_tag = format!("{{{tag}{i}}}");
        if let Some(detail) = body(&mut c, &key_tag) {
            wrong.push(format!("  trial {i} ({key_tag}): {detail}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/{} trials violated pipeline ordering — a command in the batch did \
         not observe a write that acked earlier in the SAME batch:\n{}",
        wrong.len(),
        TRIALS,
        wrong.join("\n")
    );
}

// ---------------------------------------------------------------------------
// The filed symptom
// ---------------------------------------------------------------------------

/// moon#507 as reported: MGET returns nulls for keys its own batch just wrote.
/// The ordering guarantee has a PRICE, and `INFO` now names it — moon#513.
///
/// Every other test in this file asserts the guarantee holds. This one asserts
/// the cost of holding it is observable, because it was not: a client
/// interleaving reads between write groups at `--shards >= 2` pays one extra
/// dispatch/await boundary per interleaving, and nothing reported it. The
/// throughput simply looked bad. Measured on moon-dev, `--shards 2`, one
/// connection: an `MGET` after every two `SET`s runs at 49,203 ops/s against
/// 1,122,573 for the same shape with single-key `GET`s.
///
/// It is also the acceptance criterion for moon#513, stated as a number rather
/// than a description: letting multi-key commands join the slotted batch means
/// `interleaved` stops deferring. When that lands, the assertion below flips
/// from "> 0" to "== 0" and the test stays meaningful either way.
///
/// The `--shards 1` leg is what keeps it honest. There `remote_groups` is
/// always empty, so the guard structurally cannot fire — a counter that moved
/// there would be counting something else, and every claim built on it would
/// be wrong.
#[test]
fn pco12_the_ordering_guard_reports_what_it_costs() {
    fn defers(port: u16) -> u64 {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "stats"]);
        info.split("\r\n")
            .find_map(|l| l.strip_prefix("total_pipeline_remote_defer:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| {
                panic!("INFO stats has no total_pipeline_remote_defer field: {info:?}")
            })
    }

    /// `MGET` after every two `SET`s — the shape that triggers the guard.
    fn interleaved(c: &mut Conn, tag: &str, n: usize) {
        for i in (0..n).step_by(2) {
            let (a, b) = (format!("{tag}:{i}"), format!("{tag}:{}", i + 1));
            c.pipeline(&[&["SET", &a, "v"], &["SET", &b, "v"], &["MGET", &a, &b]]);
        }
    }

    /// The same shape with single-key reads, which route by their own key and
    /// therefore never need to wait.
    fn single_key(c: &mut Conn, tag: &str, n: usize) {
        for i in (0..n).step_by(2) {
            let (a, b) = (format!("{tag}:{i}"), format!("{tag}:{}", i + 1));
            c.pipeline(&[&["SET", &a, "v"], &["SET", &b, "v"], &["GET", &a]]);
        }
    }

    // --- shards = 1: the guard cannot fire, whatever the shape ---
    {
        let m = spawn_moon("1");
        let base = defers(m.port);
        let mut c = Conn::open(m.port);
        interleaved(&mut c, "pco12s1", 64);
        drop(c);
        assert_eq!(
            defers(m.port) - base,
            0,
            "at --shards 1 `remote_groups` is always empty, so the moon#507 \
             guard cannot fire — a non-zero count here means the counter is \
             measuring something other than the deferral"
        );
    }

    // --- shards = 4: the interleaved shape defers, the single-key one does not ---
    let m = spawn_moon(SHARDS);

    let base = defers(m.port);
    let mut c = Conn::open(m.port);
    single_key(&mut c, "pco12ctl", 64);
    drop(c);
    let control_defers = defers(m.port) - base;
    assert_eq!(
        control_defers, 0,
        "a pipeline of single-key commands routes every command by its own key \
         and must never defer — {control_defers} deferrals means the guard is \
         firing on the fast path, which would be a throughput regression for \
         every pipelined client"
    );

    // --- the counter must still be reachable, or the leg below is vacuous ---
    // A KEYLESS aggregation, not a multi-key read: since moon#513 A2a an `MGET`
    // no longer defers whatever its keys do, so it can no longer prove the
    // counter is live. `DBSIZE` touches every shard by construction and is in
    // the guard's INTRINSIC set — it defers now and must keep deferring.
    fn keyless(c: &mut Conn, tag: &str, n: usize) {
        for i in (0..n).step_by(2) {
            let (a, b) = (format!("{tag}:{i}"), format!("{tag}:{}", i + 1));
            c.pipeline(&[&["SET", &a, "v"], &["SET", &b, "v"], &["DBSIZE"]]);
        }
    }
    let base = defers(m.port);
    let mut c = Conn::open(m.port);
    keyless(&mut c, "pco12kl", 64);
    drop(c);
    let keyless_defers = defers(m.port) - base;
    assert!(
        keyless_defers > 0,
        "a DBSIZE interleaved between writes at --shards {SHARDS} must trigger \
         the moon#507 guard, and INFO must say so. Zero here means the counter \
         is not wired to the deferral site, and the leg below would pass \
         vacuously"
    );

    // --- moon#513 landed: the filed shape stops paying the boundary ---
    // This assertion was `> 0` and is FLIPPED, not deleted, exactly as the
    // comment that shipped with it instructed. It is now the acceptance
    // criterion rather than the cost report: A1 (moon#721) took the co-located
    // key sets and A2a took the spanning ones, so an `MGET` between writes
    // routes into the slotted batch either way and the cut is gone.
    let base = defers(m.port);
    let mut c = Conn::open(m.port);
    interleaved(&mut c, "pco12int", 64);
    drop(c);
    let interleaved_defers = defers(m.port) - base;
    assert_eq!(
        interleaved_defers, 0,
        "an MGET interleaved between writes at --shards {SHARDS} must now join \
         the slotted batch instead of cutting it (moon#513). It cut \
         {interleaved_defers} times, each a full phase-2b drain cycle — the \
         keyless leg measured {keyless_defers} on this same harness, so the \
         counter is reachable and this is a real regression"
    );
}

/// moon#513: a multi-key command must not cut the batch over shards it never
/// touches.
///
/// `must_wait_for_pending_remote`'s multi-key arm answered "wait" without asking
/// WHERE the keys are. But `remote_groups` only ever holds FOREIGN shards — the
/// slotting branch is `else if let Some(target) = target_shard`, and
/// `target_shard` is `None` for a local key — so the moon#507 hazard (reading
/// state a pending command is about to write, or writing state it then
/// overwrites) requires the two to meet ON THE SAME SHARD. An `MGET` reading
/// shards the batch has no pending work for cannot be in that hazard, and the
/// cut buys nothing.
///
/// It is not free: a cut ends the batch pass, and the phase-2b drain then
/// dispatches one `PipelineBatchSlotted` per target shard and awaits each reply
/// slot in turn.
///
/// # Why the overlap leg is here
///
/// The connection lands on whichever shard `SO_REUSEPORT` gives it, and this
/// test cannot choose. If every written key happened to be LOCAL, nothing would
/// enter `remote_groups`, the guard would never be consulted, and the disjoint
/// leg would pass while proving nothing. So the writes deliberately span two
/// shards — at `--shards 4` at least one of them is foreign whatever shard the
/// connection got — and the overlap leg asserts that a multi-key command
/// touching THOSE shards still defers. A green disjoint leg means something
/// only because the overlap leg is non-zero on the same harness.
///
/// # Why the probe is an MSET
///
/// It was an `MGET` until moon#513 A2a, which routes a spanning multi-key READ
/// into the slotted batch — so an `MGET` stopped consulting the mask arm at all
/// and could no longer measure it in EITHER direction, silently emptying the
/// overlap control. `MSET` is per-key decomposable but deliberately NOT split
/// (that is A2b: splitting a write adds per-part AOF, replication and barrier
/// work), so it still runs inline on the coordinator and is still judged by the
/// mask. When A2b lands, this probe must move to a command that still takes the
/// coordinator path — `MSETNX`, `BITOP` and `COPY` are not decomposable at all
/// and never will be.
///
/// The co-located case (`{tag}` keys, so the multi-key command and the pending
/// writes share one shard) is not here: it is
/// [`pco15_single_owner_multikey_joins_the_slotted_batch`], and the spanning
/// read is [`pco16_spanning_multikey_joins_the_slotted_batch`].
#[test]
fn pco13_disjoint_shard_multikey_does_not_cut_the_batch() {
    fn defers(port: u16) -> u64 {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "stats"]);
        info.split("\r\n")
            .find_map(|l| l.strip_prefix("total_pipeline_remote_defer:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| {
                panic!("INFO stats has no total_pipeline_remote_defer field: {info:?}")
            })
    }

    let shards: usize = SHARDS.parse().expect("SHARDS is a number");
    assert!(
        shards >= 4,
        "this test needs >= 4 shards to build a disjoint pair of shard sets"
    );
    let owner = |k: &String| moon::shard::dispatch::key_to_shard(k.as_bytes(), shards);
    // Probed, never assumed: a key set that silently collapsed onto one shard
    // would turn the disjoint leg vacuous in exactly the way this test exists
    // to prevent.
    // ONE key per named shard, never "the first n keys landing in this set" —
    // two keys that both hashed to shard 0 would make the overlap leg depend on
    // which shard SO_REUSEPORT gave the connection: with the writes' shard-0 leg
    // local, `remote_groups` would hold only shard 1, the shard-0-only MGET
    // would be disjoint from it, and the leg would measure 0 for a reason that
    // has nothing to do with the code under test.
    let on = |prefix: &str, want: &[usize]| -> Vec<String> {
        let keys: Vec<String> = want
            .iter()
            .map(|&s| {
                (0..)
                    .map(|i| format!("{prefix}{s}:{i}"))
                    .find(|k| owner(k) == s)
                    .expect("a key exists for every shard")
            })
            .collect();
        let got: Vec<usize> = keys.iter().map(owner).collect();
        assert_eq!(got, want, "probe put {prefix} on the wrong shards");
        keys
    };
    // Writes and the overlap read both cover shards 0 AND 1. Whatever shard the
    // connection landed on, at most one of those is local, so `remote_groups`
    // always holds one of them and the overlap read always meets it.
    let w = on("pco13w", &[0, 1]);
    let overlap = on("pco13o", &[0, 1]);
    // Reads only shards 2 and 3, which the writes never touch.
    let disjoint = on("pco13d", &[2, 3]);

    let m = spawn_moon(SHARDS);

    // Runs the same 32 interleavings, touching `read` between each write pair.
    //
    // The probe is a spanning `MSET`, not the `MGET` this test shipped with.
    // moon#513 A2a made a spanning MGET fan out into the slotted batch, so it
    // stopped consulting the mask arm entirely and could no longer measure it —
    // in either direction, which would have made the overlap control silently
    // vacuous. `MSET` is per-key decomposable but NOT split (that is A2b, held
    // back because splitting a write adds per-part AOF, replication and barrier
    // work), so it still takes the coordinator's inline path and is still
    // judged by the mask. Same command family, same guard arm, same claim.
    let run = |read: &[String]| -> (u64, Vec<String>) {
        let base = defers(m.port);
        let mut c = Conn::open(m.port);
        let mut wrong = Vec::new();
        for i in 0..32 {
            let r = c.pipeline(&[
                &["SET", &w[0], "1"],
                &["SET", &w[1], "2"],
                &["MSET", &read[0], "x", &read[1], "y"],
            ]);
            if framed_len(r.as_bytes(), 3).is_none() {
                wrong.push(format!("  i={i}: expected 3 framed replies, got {r:?}"));
            }
        }
        drop(c);
        (defers(m.port) - base, wrong)
    };

    let (overlap_defers, overlap_wrong) = run(&overlap);
    assert!(overlap_wrong.is_empty(), "{}", overlap_wrong.join("\n"));
    assert!(
        overlap_defers > 0,
        "the MSET writes the very shards the pending SETs are writing, so the \
         moon#507 guard MUST still cut the batch. Zero here means either the \
         writes never reached `remote_groups` at all — the harness landed on a \
         shard that made them local — or MSET stopped taking the coordinator's \
         inline path (A2b), in which case this control has to move to another \
         command that still does. Either way the disjoint leg below would \
         prove nothing"
    );

    let (disjoint_defers, disjoint_wrong) = run(&disjoint);
    assert!(disjoint_wrong.is_empty(), "{}", disjoint_wrong.join("\n"));
    assert_eq!(
        disjoint_defers, 0,
        "the MSET writes shards 2 and 3 while the pending SETs write shards 0 \
         and 1, so there is nothing for it to wait on — yet it cut the batch \
         {disjoint_defers} times, once per interleaving, each a full drain \
         cycle. The guard is still answering on the command NAME instead of on \
         where its keys are (the overlap leg above measured \
         {overlap_defers} on this same harness, so the guard is reachable)"
    );

    // The relaxation must not cost the guarantee the guard exists for. These two
    // keys are the WRITTEN ones — spanning shards 0 and 1, so the read meets
    // whatever is pending — and the MGET must observe both values it just acked.
    let mut c = Conn::open(m.port);
    let r = c.pipeline(&[
        &["SET", &w[0], "11"],
        &["SET", &w[1], "22"],
        &["MGET", &w[0], &w[1]],
    ]);
    assert!(
        r.contains("$2\r\n11\r\n") && r.contains("$2\r\n22\r\n"),
        "an MGET reading the keys its own batch just wrote must observe them — \
         moon#507 is the whole reason the guard exists: {r:?}"
    );
    // And the same claim where the read is confined to ONE shard, which is the
    // shape the mask is most tempted to wave through.
    let one = on("pco13x", &[1]);
    let r = c.pipeline(&[&["SET", &one[0], "33"], &["MGET", &one[0], &one[0]]]);
    assert!(
        r.contains("$2\r\n33\r\n"),
        "a single-shard MGET must observe the write that acked before it in the \
         same batch: {r:?}"
    );
}

/// moon#513: inside a workspace, the guard must judge the keys the command will
/// ACTUALLY run against.
///
/// `workspace_rewrite_args` rebinds `cmd_args` further down the batch loop than
/// the ordering guard sits — and it cannot be moved below it, because the
/// connection-level intercepts the guard exists to hold back (`AUTH`, `CLIENT`,
/// `CONFIG`, `INFO`, `SELECT`, …) run in between. So the keys visible AT the
/// guard are the raw ones, while routing later hashes the prefixed ones.
///
/// That is not a small discrepancy here. A workspace key is
/// `{<32-hex>}:<key>` — a hash TAG — so every key in a workspace routes to one
/// shard no matter how the raw names scatter. A shard mask read off the raw
/// names can therefore say "touches nothing pending" about a command that in
/// truth reads the very shard the batch's pending writes are going to, which is
/// moon#507 reopened for exactly the connections that opted into isolation.
///
/// Asserted as CORRECTNESS across many connections rather than as a deferral
/// count, because the count is only wrong when the workspace's shard is foreign
/// to the connection, and `SO_REUSEPORT` decides that. The correctness claim
/// holds for every connection, so twelve of them make the placement question
/// moot.
#[test]
fn pco14_workspace_rewritten_keys_still_wait_for_their_own_batch() {
    let m = spawn_moon(SHARDS);
    let mut c0 = Conn::open(m.port);
    let created = c0.send(&["WS", "CREATE", "pco14ws"]);
    let Some(ws_id) = created
        .strip_prefix('$')
        .and_then(|r| r.split_once("\r\n"))
        .and_then(|(_, rest)| rest.split("\r\n").next())
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
    else {
        panic!("WS CREATE did not answer a workspace id: {created:?}");
    };

    let mut wrong = Vec::new();
    for i in 0..12 {
        let mut c = Conn::open(m.port);
        let authed = c.send(&["WS", "AUTH", &ws_id]);
        assert_eq!(authed, "+OK\r\n", "conn {i}: WS AUTH: {authed:?}");
        // Raw names chosen to scatter across shards; the workspace prefix
        // collapses them onto one. Whatever the guard reads, the MGET must
        // observe the two writes that acked earlier in its own batch.
        let (a, b) = (format!("pco14:{i}:aaaa"), format!("pco14:{i}:zzzz"));
        let r = c.pipeline(&[&["SET", &a, "1"], &["SET", &b, "2"], &["MGET", &a, &b]]);
        if !r.contains("$1\r\n1\r\n") || !r.contains("$1\r\n2\r\n") {
            wrong.push(format!("  conn {i}: {r:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{}/12 workspace connections had an MGET miss a write from its own \
         batch — the ordering guard judged the RAW keys while the command ran \
         against the workspace-prefixed ones:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

#[test]
fn pco1_mget_sees_writes_from_its_own_batch() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco1", |c, t| {
        let (a, b) = (format!("{t}a"), format!("{t}b"));
        c.send(&["DEL", &a, &b]);
        let r = c.pipeline(&[&["SET", &a, "1"], &["SET", &b, "2"], &["MGET", &a, &b]]);
        // Both SETs must ack, or the test is measuring something else.
        assert!(
            r.starts_with("+OK\r\n+OK\r\n"),
            "the SETs themselves failed: {r:?}"
        );
        if r.ends_with("*2\r\n$1\r\n1\r\n$1\r\n2\r\n") {
            None
        } else {
            Some(format!("MGET replied {:?}", &r[8..]))
        }
    });
}

/// The same batch with single-key GETs is CORRECT even unfixed — single-key
/// commands are deferred into the same remote batch and stay in order. Asserted
/// so a "fix" that simply broke batching everywhere is not mistaken for one,
/// and so the contrast that located the bug stays in the suite.
#[test]
fn pco2_single_key_gets_were_always_ordered() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco2", |c, t| {
        let (a, b) = (format!("{t}a"), format!("{t}b"));
        c.send(&["DEL", &a, &b]);
        let r = c.pipeline(&[
            &["SET", &a, "1"],
            &["SET", &b, "2"],
            &["GET", &a],
            &["GET", &b],
        ]);
        if r == "+OK\r\n+OK\r\n$1\r\n1\r\n$1\r\n2\r\n" {
            None
        } else {
            Some(format!("replied {r:?}"))
        }
    });
}

// ---------------------------------------------------------------------------
// Write-ordering inversions — worse than the filed read bug
// ---------------------------------------------------------------------------

/// `SET a old` then `MSET a new` in one batch: the LAST write must win. Unfixed,
/// the inline MSET runs first and the deferred SET lands on top of it, so the
/// value the client wrote SECOND is silently lost.
#[test]
fn pco3_later_mset_wins_over_earlier_set() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco3", |c, t| {
        let (a, b) = (format!("{t}a"), format!("{t}b"));
        c.send(&["DEL", &a, &b]);
        c.pipeline(&[&["SET", &a, "old"], &["MSET", &a, "new", &b, "2"]]);
        let got = c.send(&["GET", &a]);
        if got.starts_with("$3\r\nnew") {
            None
        } else {
            Some(format!(
                "GET returned {got:?} — the MSET issued AFTER the SET lost to it"
            ))
        }
    });
}

/// `SET` then multi-key `DEL` in one batch: the key must be gone. Unfixed, the
/// inline DEL runs before the deferred SET, returns success, and the key is
/// then (re)created by the SET landing afterwards.
#[test]
fn pco4_multi_key_del_removes_writes_from_its_own_batch() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco4", |c, t| {
        let (a, b) = (format!("{t}a"), format!("{t}b"));
        c.send(&["DEL", &a, &b]);
        c.pipeline(&[&["SET", &a, "1"], &["SET", &b, "2"], &["DEL", &a, &b]]);
        let got = c.send(&["MGET", &a, &b]);
        if got == "*2\r\n$-1\r\n$-1\r\n" {
            None
        } else {
            Some(format!("keys survived their own batch's DEL: {got:?}"))
        }
    });
}

/// `SET` then `FLUSHALL` in one batch: nothing may survive. Unfixed, the flush
/// runs first and the write lands into the freshly-emptied keyspace.
#[test]
fn pco5_flushall_clears_writes_from_its_own_batch() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco5", |c, t| {
        let k = format!("{t}k");
        c.pipeline(&[&["SET", &k, "1"], &["FLUSHALL"]]);
        let got = c.send(&["GET", &k]);
        if got.starts_with("$-1") {
            None
        } else {
            Some(format!("key survived FLUSHALL: {got:?}"))
        }
    });
}

// ---------------------------------------------------------------------------
// Keyless aggregation commands — the class a name-list would have missed
// ---------------------------------------------------------------------------

/// DBSIZE, KEYS, SCAN and EXISTS all aggregate across shards inline. None of
/// them is a "multi-key command" in the registry sense, which is why the fix is
/// keyed on ROUTABILITY (`extract_primary_key` returning a key) rather than on
/// a list of command names — a list wrote itself around MGET and would have
/// missed every one of these.
#[test]
fn pco6_cross_shard_aggregations_see_their_own_batch() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco6", |c, t| {
        let k = format!("{t}k");
        c.send(&["FLUSHALL"]);

        let r = c.pipeline(&[&["SET", &k, "1"], &["DBSIZE"]]);
        if !r.ends_with(":1\r\n") {
            return Some(format!("DBSIZE after SET replied {:?}", &r[5..]));
        }

        let r = c.pipeline(&[&["SET", &k, "1"], &["KEYS", "*"]]);
        if !r.contains(k.as_str()) {
            return Some(format!("KEYS after SET replied {:?}", &r[5..]));
        }

        // SCAN is cursor-paged, and at shards>1 page 0 legitimately covers
        // only part of the keyspace — so "the key is not on page 0" is not a
        // defect. The ordering property is that a FULL iteration whose first
        // page was taken in the same batch as the write still finds it: a page
        // taken against a stale shard would skip the key permanently, because
        // the cursor never returns to that shard.
        let r = c.pipeline(&[&["SET", &k, "1"], &["SCAN", "0"]]);
        let mut found = r.contains(k.as_str());
        let mut cursor = scan_cursor(&r).unwrap_or_else(|| "0".to_string());
        let mut pages = 0;
        while cursor != "0" && pages < 64 {
            let page = c.send(&["SCAN", &cursor]);
            found |= page.contains(k.as_str());
            cursor = match scan_cursor(&page) {
                Some(next) => next,
                None => break,
            };
            pages += 1;
        }
        if !found {
            return Some(format!(
                "a full SCAN iteration begun in the write's own batch never \
                 returned {k}"
            ));
        }

        let r = c.pipeline(&[&["SET", &k, "1"], &["EXISTS", &k, "nope"]]);
        if !r.ends_with(":1\r\n") {
            return Some(format!("EXISTS after SET replied {:?}", &r[5..]));
        }
        None
    });
}

/// `INFO keyspace` is neither multi-key nor obviously "cross-shard", and it was
/// wrong. It is here because it is what proves the fix cannot be a curated list
/// of command names — nobody writing a list for an MGET bug would have put INFO
/// on it. `extract_primary_key` reports it keyless, so routability catches it
/// for free.
///
/// `MEMORY USAGE` was also wrong in the same sweep and is deliberately NOT
/// asserted here: it is a DIFFERENT bug (moon#511). It routes by hashing the
/// literal subcommand `"USAGE"` instead of the key, so it answers `$-1` for a
/// key that plainly exists with no pipelining involved at all — 22/24 at
/// `--shards 4` on separate connections and separate batches. Folding it in
/// would make this suite pass or fail for two unrelated reasons.
#[test]
fn pco7_introspection_commands_see_their_own_batch() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco7", |c, t| {
        let k = format!("{t}k");
        c.send(&["FLUSHALL"]);
        let r = c.pipeline(&[&["SET", &k, "1"], &["INFO", "keyspace"]]);
        if r.contains("keys=0") || !r.contains("db0") {
            return Some(format!("INFO keyspace after SET replied {:?}", &r[5..]));
        }
        None
    });
}

// ---------------------------------------------------------------------------
// The fix must not break the batch it is protecting
// ---------------------------------------------------------------------------

/// The fix defers the offending command and the unconsumed tail to the next
/// batch iteration, re-encoding the tail back into the read buffer. That
/// machinery must not drop, duplicate, or reorder anything: a long mixed
/// pipeline has to come back with every reply, in order, exactly once.
#[test]
fn pco8_deferred_tail_is_replayed_intact() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco8", |c, t| {
        c.send(&["FLUSHALL"]);
        let (a, b) = (format!("{t}a"), format!("{t}b"));

        // Interleave the two classes so the batch is split repeatedly: routed
        // single-key writes (deferred) against keyless/multi-key commands
        // (inline).
        let reply = c.pipeline(&[
            &["SET", &a, "1"],
            &["DBSIZE"],
            &["SET", &b, "2"],
            &["MGET", &a, &b],
            &["PING"],
            &["GET", &a],
            &["EXISTS", &a, &b],
            &["ECHO", "tail"],
        ]);

        let expected = concat!(
            "+OK\r\n",                      // SET a
            ":1\r\n",                       // DBSIZE — sees a
            "+OK\r\n",                      // SET b
            "*2\r\n$1\r\n1\r\n$1\r\n2\r\n", // MGET — sees both
            "+PONG\r\n",                    // PING
            "$1\r\n1\r\n",                  // GET a
            ":2\r\n",                       // EXISTS a b
            "$4\r\ntail\r\n",               // ECHO
        );
        if reply == expected {
            None
        } else {
            Some(format!("replied {reply:?}, expected {expected:?}"))
        }
    });
}

/// Inline-protocol commands round-trip through the same defer-and-replay path.
/// The tail is re-encoded with `serialize_resp3`, so a frame that did not
/// arrive as a RESP array is the case most likely to be mangled by it.
#[test]
fn pco9_inline_commands_survive_the_defer_path() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco9", |c, t| {
        let k = format!("{t}k");
        c.send(&["DEL", &k]);
        // SET (routed, possibly deferred) then an INLINE ping in the SAME write.
        let batch = format!(
            "*3\r\n$3\r\nSET\r\n${}\r\n{k}\r\n$1\r\n1\r\nPING\r\n",
            k.len()
        );
        c.sock.write_all(batch.as_bytes()).expect("write");
        let reply = c.read_replies(2);
        if reply != "+OK\r\n+PONG\r\n" {
            return Some(format!(
                "inline PING after a deferred write replied {reply:?}"
            ));
        }
        let got = c.send(&["GET", &k]);
        if got == "$1\r\n1\r\n" {
            None
        } else {
            Some(format!("GET after the inline batch replied {got:?}"))
        }
    });
}

// ---------------------------------------------------------------------------
// The class routability alone cannot see
// ---------------------------------------------------------------------------

/// Some commands are handled INLINE by a `try_handle_*` interceptor before the
/// routing step ever runs, yet have a first argument that `extract_primary_key`
/// would happily hash — so "does it route by its own key?" answers *yes* for
/// them and answers it wrongly. `SWAPDB` (args[0] is a db number) and `EVAL`
/// (args[0] is the script body) are the two that touch real keys.
///
/// This is the guard for the `is_inline_intercepted` list in
/// `server::conn::shared`: delete the SWAPDB entry and this test fails. Without
/// it the list could rot silently the next time an interceptor is added, which
/// is exactly how moon#507 reached a release.
///
/// **EVAL is deliberately not asserted here.** It should be — it is the more
/// interesting of the two — but a single-key `EVAL` is currently rejected
/// outright at `--shards >= 2` with `CROSSSLOT Keys in script don't hash to the
/// same slot and shard` (moon#508; measured here at 7/12 trials, and it affects
/// plain `EVAL`, not only the `EVALSHA` form the issue was filed against). A
/// probe that cannot run its command proves nothing about ordering. Restore the
/// EVAL leg when #508 closes.
#[test]
fn pco10_inline_intercepted_commands_see_their_own_batch() {
    let m = spawn_moon(SHARDS);
    each_trial(m.port, "pco10", |c, t| {
        let k = format!("{t}k");
        // SWAPDB moves the whole database out from under a pending write. The
        // write acked in db 0, so after the swap db 0 must NOT hold it and db 1
        // must — which can only happen if the write landed BEFORE the swap.
        c.send(&["SELECT", "0"]);
        c.send(&["FLUSHALL"]);
        c.pipeline(&[&["SET", &k, "1"], &["SWAPDB", "0", "1"]]);
        let in_db0 = c.send(&["GET", &k]);
        c.send(&["SELECT", "1"]);
        let in_db1 = c.send(&["GET", &k]);
        c.send(&["SELECT", "0"]);
        if !in_db0.starts_with("$-1") || !in_db1.starts_with("$1\r\n1") {
            return Some(format!(
                "after SET+SWAPDB in one batch: db0={in_db0:?} db1={in_db1:?} \
                 — the write did not land before the swap"
            ));
        }
        None
    });
}

/// The reply framer is the one piece of this suite that can make every other
/// case lie — a short read reports a wrong VALUE, not a short read. So it is
/// tested directly, including the boundary where a reply is one byte short.
#[test]
fn pco0_reply_framer_counts_top_level_replies() {
    let cases: &[(&str, usize)] = &[
        ("+OK\r\n", 1),
        (":42\r\n", 1),
        ("-ERR nope\r\n", 1),
        ("$3\r\nabc\r\n", 1),
        ("$-1\r\n", 1),
        ("$0\r\n\r\n", 1),
        ("*2\r\n$1\r\na\r\n$1\r\nb\r\n", 1),
        ("*-1\r\n", 1),
        ("*0\r\n", 1),
        // an MGET whose elements are all null — the shape the BUG produced
        ("*2\r\n$-1\r\n$-1\r\n", 1),
        // nested: SCAN's [cursor, [keys...]]
        ("*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n", 1),
        // a whole pipelined batch: +OK +OK *2
        ("+OK\r\n+OK\r\n*2\r\n$1\r\n1\r\n$1\r\n2\r\n", 3),
        // RESP3 attribute (`|N`): metadata for the reply that FOLLOWS, so the
        // pair AND the attributed reply must all be consumed for ONE reply. If
        // `|` were counted as a reply of its own, this would frame after the
        // header and leave the rest to be misread as the next reply.
        ("|1\r\n$3\r\nttl\r\n:99\r\n+OK\r\n", 1),
        // attributed reply inside an aggregate: the attribute must not eat an
        // element slot either.
        ("*2\r\n|1\r\n$3\r\nttl\r\n:99\r\n$1\r\na\r\n$1\r\nb\r\n", 1),
        // two attributed replies back to back
        (
            "|1\r\n$1\r\nk\r\n:1\r\n+OK\r\n|1\r\n$1\r\nk\r\n:2\r\n:7\r\n",
            2,
        ),
    ];

    for (raw, want) in cases {
        let bytes = raw.as_bytes();

        assert_eq!(
            framed_len(bytes, *want),
            Some(bytes.len()),
            "did not frame {raw:?} as {want} complete repl(y|ies)"
        );

        // Every proper prefix must be judged INCOMPLETE. This is the property
        // that matters: the old silence-based reader accepted any prefix that
        // happened to arrive before a 250ms lull.
        for cut in 1..bytes.len() {
            assert_eq!(
                framed_len(&bytes[..cut], *want),
                None,
                "accepted a {cut}-byte prefix of {raw:?} as {want} complete repl(y|ies)"
            );
        }

        // Trailing bytes belong to the NEXT reply and must not be consumed.
        let mut over = bytes.to_vec();
        over.extend_from_slice(b"+NEXT\r\n");
        assert_eq!(framed_len(&over, *want), Some(bytes.len()));
    }

    // Asking for more replies than the buffer holds never over-reports.
    assert_eq!(framed_len(b"+OK\r\n", 2), None);
    assert_eq!(framed_len(b"", 1), None);
}

/// moon#513 (A1): a multi-key command whose keys ALL live on one shard must
/// join the slotted batch instead of cutting it.
///
/// [`pco13_disjoint_shard_multikey_does_not_cut_the_batch`] relaxed the guard
/// for a multi-key command that reads shards the batch has nothing pending for.
/// It deliberately left the CO-LOCATED case alone — an `MGET` reading the very
/// shard the pending writes are going to — because the coordinator ran such a
/// command INLINE, and waving it through while it still executed inline is
/// exactly moon#507.
///
/// The fix is not to relax the guard further but to change where the command
/// runs: a multi-key command whose key mask has exactly ONE bit has a single
/// owner shard, so it can be appended to `remote_groups[owner]` like a
/// single-key command. Ordering then comes from the slotted batch itself — the
/// same property that has always made `SET k` + `GET k` correct — and the wait
/// is genuinely unnecessary rather than merely skipped.
///
/// This is the shape `CLAUDE.md` tells users to adopt (`{tag}` co-location),
/// so it is the shape most likely to be in a hot pipeline.
///
/// # Why the control leg is here
///
/// The connection lands on whichever shard `SO_REUSEPORT` gives it. A subject
/// leg that measured zero because its shard happened to be LOCAL would prove
/// nothing, so the subject runs once per shard — at `--shards 4` at least three
/// of the four are foreign whatever the connection got — and a control leg
/// using a SHARD-SPANNING read asserts the counter is reachable on this same
/// harness. A green subject means something only because the control is
/// non-zero.
#[test]
fn pco15_single_owner_multikey_joins_the_slotted_batch() {
    fn defers(port: u16) -> u64 {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "stats"]);
        info.split("\r\n")
            .find_map(|l| l.strip_prefix("total_pipeline_remote_defer:"))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| {
                panic!("INFO stats has no total_pipeline_remote_defer field: {info:?}")
            })
    }

    let shards: usize = SHARDS.parse().expect("SHARDS is a number");
    let owner = |k: &String| moon::shard::dispatch::key_to_shard(k.as_bytes(), shards);
    // `n` distinct keys that all hash to shard `s`. Probed, never assumed — a
    // pair that silently landed on two shards would make the subject leg a
    // re-run of pco13 rather than the co-located case.
    let on = |prefix: &str, s: usize, n: usize| -> Vec<String> {
        let keys: Vec<String> = (0..)
            .map(|i| format!("{prefix}{s}:{i}"))
            .filter(|k| owner(k) == s)
            .take(n)
            .collect();
        assert_eq!(keys.len(), n, "could not find {n} keys on shard {s}");
        keys
    };

    let m = spawn_moon(SHARDS);

    // 32 interleavings of: write both keys, then read both in one MGET.
    let run = |read: &[String], write: &[String]| -> (u64, Vec<String>) {
        let base = defers(m.port);
        let mut c = Conn::open(m.port);
        let mut wrong = Vec::new();
        for i in 0..32 {
            let r = c.pipeline(&[
                &["SET", &write[0], "1"],
                &["SET", &write[1], "2"],
                &["MGET", &read[0], &read[1]],
            ]);
            if framed_len(r.as_bytes(), 3).is_none() {
                wrong.push(format!("  i={i}: expected 3 framed replies, got {r:?}"));
            }
        }
        drop(c);
        (defers(m.port) - base, wrong)
    };

    // --- control: a command that still cuts, so the counter is live ---
    // A SPANNING `MSET`, not the spanning `MGET` this leg shipped with: moon#513
    // A2a routes a spanning multi-key READ into the slotted batch too, so an
    // MGET no longer cuts for any key placement and cannot prove the counter is
    // reachable. `MSET` is held back to A2b — splitting a write adds per-part
    // AOF, replication and barrier work — so it still runs inline on the
    // coordinator and still cuts. When A2b lands, move this to `MSETNX`,
    // `BITOP` or `COPY`, which are not per-key decomposable at all.
    let span_w = [on("pco15cw", 0, 1).remove(0), on("pco15cw", 1, 1).remove(0)];
    let control_defers = {
        let base = defers(m.port);
        let mut c = Conn::open(m.port);
        for i in 0..32 {
            let r = c.pipeline(&[
                &["SET", &span_w[0], "1"],
                &["SET", &span_w[1], "2"],
                &["MSET", &span_w[0], "x", &span_w[1], "y"],
            ]);
            assert!(
                framed_len(r.as_bytes(), 3).is_some(),
                "control i={i}: expected 3 framed replies, got {r:?}"
            );
        }
        drop(c);
        defers(m.port) - base
    };
    assert!(
        control_defers > 0,
        "a two-shard MSET interleaved between writes to those same two shards \
         must still cut the batch — it is still consumed inline by the \
         coordinator. Zero here means the counter is not reachable on this \
         harness and every subject leg below would pass vacuously"
    );

    // --- subject: one leg per shard, all keys co-located on that shard ---
    let mut cut: Vec<String> = Vec::new();
    for s in 0..shards {
        let keys = on("pco15s", s, 2);
        let (d, wrong) = run(&keys, &keys);
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
        if d != 0 {
            cut.push(format!("  shard {s}: {d} deferrals over 32 interleavings"));
        }
    }
    assert!(
        cut.is_empty(),
        "every key in these batches lives on ONE shard, so the MGET has a single \
         owner and belongs in that shard's slotted batch — appended BEHIND the \
         pending writes, which is what makes it correct without a cut. Instead \
         it cut the batch, each cut a full phase-2b drain cycle (control leg \
         measured {control_defers} on this same harness, so the guard is \
         reachable):\n{}",
        cut.join("\n")
    );

    // --- the relaxation must not cost the ordering guarantee it replaces ---
    // Read the keys this batch just wrote, on every shard, and demand the acked
    // values. This is the moon#507 claim itself: slotting is only a valid reason
    // to skip the wait if the slotted command really does observe the batch.
    for s in 0..shards {
        let keys = on("pco15v", s, 2);
        let mut c = Conn::open(m.port);
        let r = c.pipeline(&[
            &["SET", &keys[0], "11"],
            &["SET", &keys[1], "22"],
            &["MGET", &keys[0], &keys[1]],
        ]);
        assert!(
            r.contains("$2\r\n11\r\n") && r.contains("$2\r\n22\r\n"),
            "shard {s}: a single-owner MGET must observe the writes that acked \
             before it in its own batch — if slotting lost that, A1 has traded \
             moon#507 back for throughput: {r:?}"
        );
    }
}

/// moon#513 (A2a): a multi-key READ whose keys SPAN shards must fan out into
/// the slotted batch instead of cutting it.
///
/// A1 ([`pco15_single_owner_multikey_joins_the_slotted_batch`]) took the case
/// where one shard owns every key: the command is simply appended to
/// `remote_groups[owner]` like a single-key command. The genuinely SPANNING
/// case had no single destination for the 1:1 `resp_idx -> reply` slot, so it
/// stayed on the coordinator — which pushes its own message into the SPSC ring
/// MID-LOOP, ahead of the batch's still-buffered earlier writes. That inversion
/// is moon#507 itself, so the guard had to keep cutting the batch for it.
///
/// A2a removes the second push for the two SPLITTABLE reads, `MGET` and
/// `EXISTS`: the command is decomposed per owner shard, each part is appended
/// to that shard's `remote_groups` (or executed inline against the local slice)
/// at the command's own position in the batch, and the parts are folded back
/// into ONE client reply at the drain. Ordering is then per-key and comes from
/// the slotted batch itself — for any key `k` every operation on `k` in this
/// batch lands in `remote_groups[owner(k)]` in loop order, and the owner shard
/// executes that vector in order.
///
/// # Both halves, or the test is worthless
///
/// A green deferral count alone would also be produced by a "fix" that simply
/// stopped waiting while the coordinator kept running the command inline —
/// which is moon#507 reopened. So the subject leg asserts BOTH that the batch
/// was not cut AND that the command really took the fan-out route
/// (`total_pipeline_multikey_fanout`), and then re-reads the values the same
/// batch wrote.
///
/// # Why the control leg is KEYLESS
///
/// pco13's and pco15's controls used a shard-SPANNING `MGET` to prove the
/// counter was reachable. A2a is precisely the change that makes that shape
/// stop deferring, so it can no longer serve as a control. `DBSIZE` can: it is
/// keyless, aggregates across every shard, and is named in the guard's
/// INTRINSIC set — it must keep deferring after A2a and forever after.
#[test]
fn pco16_spanning_multikey_joins_the_slotted_batch() {
    fn stat(port: u16, field: &str) -> u64 {
        let mut c = Conn::open(port);
        let info = c.send(&["INFO", "stats"]);
        info.split("\r\n")
            .find_map(|l| l.strip_prefix(field).and_then(|v| v.strip_prefix(':')))
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or_else(|| panic!("INFO stats has no {field} field: {info:?}"))
    }
    let defers = |port| stat(port, "total_pipeline_remote_defer");
    let fanouts = |port| stat(port, "total_pipeline_multikey_fanout");

    let shards: usize = SHARDS.parse().expect("SHARDS is a number");
    assert!(
        shards >= 4,
        "this test needs >= 4 shards to build spanning pairs"
    );
    let owner = |k: &String| moon::shard::dispatch::key_to_shard(k.as_bytes(), shards);
    // One key on each named shard. Probed, never assumed: a pair that quietly
    // collapsed onto ONE shard would re-run pco15 instead of the spanning case
    // this test exists for.
    let on = |prefix: &str, want: &[usize]| -> Vec<String> {
        let keys: Vec<String> = want
            .iter()
            .map(|&s| {
                (0..)
                    .map(|i| format!("{prefix}{s}:{i}"))
                    .find(|k| owner(k) == s)
                    .expect("a key exists for every shard")
            })
            .collect();
        let got: Vec<usize> = keys.iter().map(owner).collect();
        assert_eq!(got, want, "probe put {prefix} on the wrong shards");
        keys
    };

    let m = spawn_moon(SHARDS);

    // --- control: a KEYLESS aggregation still cuts, so the counter is live ---
    {
        let w = on("pco16cw", &[0, 1]);
        let base = defers(m.port);
        let mut c = Conn::open(m.port);
        for _ in 0..32 {
            let r = c.pipeline(&[&["SET", &w[0], "1"], &["SET", &w[1], "2"], &["DBSIZE"]]);
            assert!(
                framed_len(r.as_bytes(), 3).is_some(),
                "expected 3 framed replies, got {r:?}"
            );
        }
        drop(c);
        let control = defers(m.port) - base;
        assert!(
            control > 0,
            "DBSIZE aggregates across every shard, so the moon#507 guard MUST \
             still cut the batch for it. Zero here means the counter is not \
             reachable on this harness and every subject leg below would pass \
             vacuously"
        );
    }

    // --- subject: every spanning pair, read == written, 32 interleavings ---
    let mut cut: Vec<String> = Vec::new();
    let mut not_fanned: Vec<String> = Vec::new();
    for a in 0..shards {
        for b in (a + 1)..shards {
            let k = on(&format!("pco16s{a}{b}"), &[a, b]);
            let base_d = defers(m.port);
            let base_f = fanouts(m.port);
            let mut c = Conn::open(m.port);
            for i in 0..32 {
                let r = c.pipeline(&[
                    &["SET", &k[0], "1"],
                    &["SET", &k[1], "2"],
                    &["MGET", &k[0], &k[1]],
                ]);
                assert!(
                    framed_len(r.as_bytes(), 3).is_some(),
                    "pair ({a},{b}) i={i}: expected 3 framed replies, got {r:?}"
                );
            }
            drop(c);
            let d = defers(m.port) - base_d;
            let f = fanouts(m.port) - base_f;
            if d != 0 {
                cut.push(format!(
                    "  pair ({a},{b}): {d} deferrals over 32 interleavings"
                ));
            }
            if f != 32 {
                not_fanned.push(format!("  pair ({a},{b}): {f} fan-outs, wanted 32"));
            }
        }
    }
    assert!(
        cut.is_empty(),
        "a spanning MGET is decomposable per owner shard, so each part belongs \
         in its owner's slotted batch — appended BEHIND that shard's pending \
         writes, which is what makes it correct without a cut. Instead it cut \
         the batch, each cut a full phase-2b drain cycle:\n{}",
        cut.join("\n")
    );
    assert!(
        not_fanned.is_empty(),
        "the batch was not cut, but the command did not take the fan-out route \
         either — so something else stopped it waiting while it still ran \
         INLINE on the coordinator, which is moon#507 reopened. Both halves \
         must hold:\n{}",
        not_fanned.join("\n")
    );

    // --- the fan-out must not cost the ordering guarantee it replaces ---
    //
    // The key order is INTERLEAVED and starts on the HIGHER shard, and every
    // value is distinct. Both properties are load-bearing. The parts are built
    // in ascending shard order, so a fold that simply CONCATENATED them would
    // answer `hi0,hi1,lo0,lo1` — silently pairing every value with the wrong
    // key, no error anywhere. A probe reading `[lo, hi]` cannot tell the two
    // folds apart: concatenation and merge agree on it. (Measured: mutating the
    // fold to concatenate left the two-key form of this leg green.)
    for a in 0..shards {
        for b in (a + 1)..shards {
            // Distinct PREFIXES, not `&[a, a]`: `on` returns the first key it
            // finds for each named shard, so repeating a shard under one prefix
            // yields the SAME key twice and the assertion would compare a
            // four-element reply built from two keys.
            let lo = [
                on(&format!("pco16vlA{a}{b}"), &[a]).remove(0),
                on(&format!("pco16vlB{a}{b}"), &[a]).remove(0),
            ];
            let hi = [
                on(&format!("pco16vhA{a}{b}"), &[b]).remove(0),
                on(&format!("pco16vhB{a}{b}"), &[b]).remove(0),
            ];
            assert_ne!(lo[0], lo[1], "the two low-shard keys must be distinct");
            assert_ne!(hi[0], hi[1], "the two high-shard keys must be distinct");
            let mut c = Conn::open(m.port);
            let r = c.pipeline(&[
                &["SET", &hi[0], "h0"],
                &["SET", &lo[0], "l0"],
                &["SET", &hi[1], "h1"],
                &["SET", &lo[1], "l1"],
                &["MGET", &hi[0], &lo[0], &hi[1], &lo[1]],
            ]);
            assert!(
                r.ends_with("*4\r\n$2\r\nh0\r\n$2\r\nl0\r\n$2\r\nh1\r\n$2\r\nl1\r\n"),
                "pair ({a},{b}): a spanning MGET must observe the writes that \
                 acked before it in its own batch, IN CLIENT KEY ORDER. Getting \
                 h0,h1,l0,l1 means the fold concatenated the per-shard parts \
                 instead of re-interleaving them, which mis-pairs every value \
                 with its key and reports no error at all: {r:?}"
            );
        }
    }

    // --- EXISTS folds by SUM, and must also see its own batch ---
    for a in 0..shards {
        for b in (a + 1)..shards {
            let k = on(&format!("pco16e{a}{b}"), &[a, b]);
            let mut c = Conn::open(m.port);
            let base_d = defers(m.port);
            let r = c.pipeline(&[
                &["SET", &k[0], "1"],
                &["SET", &k[1], "2"],
                &["EXISTS", &k[0], &k[1], &k[0]],
            ]);
            drop(c);
            assert!(
                r.ends_with(":3\r\n"),
                "pair ({a},{b}): EXISTS over two keys this batch just wrote \
                 (one named twice) must answer 3: {r:?}"
            );
            assert_eq!(
                defers(m.port) - base_d,
                0,
                "pair ({a},{b}): a spanning EXISTS is per-key decomposable and \
                 must not cut the batch"
            );
        }
    }
}
