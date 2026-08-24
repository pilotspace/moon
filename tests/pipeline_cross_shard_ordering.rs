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

    let base = defers(m.port);
    let mut c = Conn::open(m.port);
    interleaved(&mut c, "pco12int", 64);
    drop(c);
    let interleaved_defers = defers(m.port) - base;
    assert!(
        interleaved_defers > 0,
        "an MGET interleaved between writes at --shards {SHARDS} must trigger \
         the moon#507 guard, and INFO must say so. Zero here means either the \
         counter is not wired to the deferral site, or moon#513 has landed — \
         in which case flip this assertion to `== 0` rather than deleting it"
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
