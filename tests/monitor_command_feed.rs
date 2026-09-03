//! `MONITOR` streams every executed command to attached admin clients.
//!
//! Measured against redis-server 8.6.1 over raw sockets (three probes,
//! 2026-08-14). Raw sockets because the feed IS a stream of `+SimpleString`
//! lines whose exact bytes are the contract — every client library reformats
//! them into a struct before a test could see the difference, and the
//! divergences that matter here are byte-level: which byte leads the frame,
//! how a `0x00` is escaped, whether a secret survives redaction.
//!
//! Two measured facts invert the obvious implementation, and each has a test
//! that fails loudly if someone "fixes" it back:
//!
//!   * The feed is a **SimpleString even under RESP3**, not a Push frame. The
//!     instinct straight after `pubsub-resp3-push` is to make it a Push; Redis
//!     does not, and a client reading the feed expects `+`. See `mon2`.
//!   * Administrative commands are hidden at **subcommand** granularity, and
//!     the rule is NOT Moon's `CommandFlags::ADMIN | SKIP_MONITOR`. Moon's
//!     ADMIN is container-granular (it would hide `INFO`, `CLIENT GETNAME`,
//!     `ACL WHOAMI`, which Redis shows) and Redis feeds the whole EVAL family
//!     despite flagging it `skip_monitor`. See `mon20`, which drives the
//!     measured table row by row.
//!
//! # Reading the feed under load, deterministically
//!
//! Every socket read in this file waits on the PROTOCOL, never the clock.
//! `frame_len` bounds a RESP reply/feed-line exactly, so `read_one_frame`
//! returns the instant the promised bytes are complete, however long that
//! takes — bounded only by `CEILING`, which exists solely to convert a
//! genuine hang into a failure. The one thing that cannot be read off the
//! protocol is an ABSENCE (a command that must never appear): there is no
//! frame to wait for when nothing is coming. Those checks either wait for a
//! `feed_barrier` — a fresh, distinguishable command dispatched only after
//! everything under test has already been confirmed processed, whose own
//! feed line proves nothing earlier is still in flight — or, for a
//! connection that can never receive anything again (unattached/detached),
//! bound the wait with `ABSENCE_GRACE` and accept "nothing arrived" as the
//! passing outcome. See the doc comments on each helper below.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
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
    spawn_moon_opts(shards, &[])
}

fn spawn_moon_opts(shards: &str, extra: &[&str]) -> Moon {
    // CARGO_BIN_EXE_moon is the binary cargo built for THIS invocation — fresh
    // and feature-matched. Never probe target/release directly: that path's
    // provenance is unknown and has produced false PASSes before.
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-monitor-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args(extra)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                // This host hovers near the 5% diskfull line; the guard would
                // turn writes into MOONERR and flake the suite.
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-monitor-{port}"));
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
                    // `-NOAUTH` is a READY server that wants a password: the
                    // probe must not treat a password-protected instance as
                    // one that never came up.
                    && (buf.starts_with(b"+PONG") || buf.starts_with(b"-NOAUTH"))
                {
                    return moon;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // Never skip. A silent early return would let this suite report green
    // while exercising no server at all.
    let status = match moon.child.try_wait() {
        Ok(Some(s)) => format!("exited with {s}"),
        Ok(None) => "still running but never answered PING".to_string(),
        Err(e) => format!("status unavailable: {e}"),
    };
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon never became ready on port {port} ({status})\n--- stderr ---\n{log}");
}

/// Ceiling for every deterministic wait in this file that expects a REAL,
/// guaranteed event to eventually happen (a reply, a feed line). It exists
/// ONLY to convert a genuine hang into a test failure — never to signal "no
/// more data is coming", which the RESP frame parser (`frame_len`) already
/// determines exactly. Sized to absorb a fully loaded 8-vCPU CI host running
/// this suite alongside the rest of the monoio leg — the scenario this suite
/// was seen to flake under, where the previous 600ms
/// read-timeout-as-completion-signal treated a reply that was merely LATE as
/// one that would never arrive.
const CEILING: Duration = Duration::from_secs(20);

/// Window for a check that expects NOTHING to ever arrive on a connection
/// that is unattached or has been detached. There is no positive event to
/// wait for in that case — absence is what's under test — so this is a
/// deliberate, generous, but bounded compromise: far larger than the
/// loopback delivery time it needs to cover, smaller than `CEILING` because
/// there is no slow-but-real event behind it to wait out.
const ABSENCE_GRACE: Duration = Duration::from_secs(3);

/// Monotonically increasing token so concurrent tests (and repeated barriers
/// within one test) never collide on a barrier's marker text.
static BARRIER_SEQ: AtomicU64 = AtomicU64::new(0);

/// The length, in bytes, of one complete RESP frame starting at `buf[0]`, or
/// `None` if `buf` does not yet hold a complete frame. This is the single
/// source of truth for "have the promised bytes arrived" everywhere in this
/// file — every read loop below stops IMMEDIATELY once this returns `Some`,
/// and never infers completion from a `read()` timing out. Recurses through
/// containers (arrays/maps/sets/pushes) so a nested reply (e.g. `COMMAND
/// INFO`) is bounded exactly, not guessed at.
fn frame_len(buf: &[u8]) -> Option<usize> {
    fn line_end(buf: &[u8], from: usize) -> Option<usize> {
        buf.get(from..)?
            .windows(2)
            .position(|w| w == b"\r\n")
            .map(|p| from + p)
    }
    fn at(buf: &[u8], pos: usize) -> Option<usize> {
        let tag = *buf.get(pos)?;
        match tag {
            // No length field: the line itself IS the whole frame.
            b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => Some(line_end(buf, pos + 1)? + 2),
            // Length-prefixed byte string: bulk string, bulk error, verbatim
            // string. A negative length is a null with no body.
            b'$' | b'!' | b'=' => {
                let end = line_end(buf, pos + 1)?;
                let len: i64 = std::str::from_utf8(&buf[pos + 1..end]).ok()?.parse().ok()?;
                if len < 0 {
                    return Some(end + 2);
                }
                let body_end = end + 2 + len as usize;
                if buf.len() < body_end + 2 {
                    return None;
                }
                Some(body_end + 2)
            }
            // Length-prefixed container: array, map (2N elements), set, push.
            // A negative count is a null with no elements.
            b'*' | b'%' | b'~' | b'>' => {
                let end = line_end(buf, pos + 1)?;
                let count: i64 = std::str::from_utf8(&buf[pos + 1..end]).ok()?.parse().ok()?;
                let mut cursor = end + 2;
                if count < 0 {
                    return Some(cursor);
                }
                let elems = if tag == b'%' { count * 2 } else { count };
                for _ in 0..elems {
                    cursor = at(buf, cursor)?;
                }
                Some(cursor)
            }
            _ => None,
        }
    }
    at(buf, 0)
}

struct Conn {
    sock: TcpStream,
    /// Bytes already read off the socket but not yet consumed as a complete
    /// frame — carried across calls so a frame split across two `read()`s
    /// (or a monitor line that arrives bundled with the next one) is never
    /// mistaken for "nothing more coming".
    carry: Vec<u8>,
}

impl Conn {
    fn open(port: u16) -> Self {
        let sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        // Short: this is a POLLING granularity for the deadline-driven loops
        // below, never the signal that a reply is complete. A read() timing
        // out here just means "check the deadline and try again" — the
        // frame parser (`frame_len`) is what decides "done".
        sock.set_read_timeout(Some(Duration::from_millis(250)))
            .expect("read timeout");
        sock.set_write_timeout(Some(Duration::from_secs(5)))
            .expect("write timeout");
        Conn {
            sock,
            carry: Vec::new(),
        }
    }

    fn hello3(port: u16) -> Self {
        let mut c = Self::open(port);
        let r = c.send(&["HELLO", "3"]);
        assert!(
            !r.is_empty() && r[0] != b'-',
            "HELLO 3 must be accepted; got {}",
            s(&r)
        );
        c
    }

    fn write_cmd(&mut self, parts: &[&[u8]]) {
        let mut out = format!("*{}\r\n", parts.len()).into_bytes();
        for p in parts {
            out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            out.extend_from_slice(p);
            out.extend_from_slice(b"\r\n");
        }
        self.sock.write_all(&out).expect("write command");
    }

    /// Send one command and read exactly the one RESP frame that is its
    /// reply — no more, no less, however long it takes to arrive (bounded
    /// only by `CEILING`, to catch a genuine hang).
    fn send(&mut self, parts: &[&str]) -> Vec<u8> {
        let owned: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.write_cmd(&owned);
        self.read_one_frame(Instant::now() + CEILING)
    }

    /// Send a command whose arguments may contain arbitrary bytes.
    fn send_bytes(&mut self, parts: &[&[u8]]) -> Vec<u8> {
        self.write_cmd(parts);
        self.read_one_frame(Instant::now() + CEILING)
    }

    /// Send `parts`, then a distinguishing sentinel command on the SAME
    /// connection, and assert `parts` was answered with NOTHING before the
    /// sentinel's reply. Redis is measured to answer a second `MONITOR` with
    /// total silence (no error, no reply) — that can't be waited out with a
    /// timeout (there is nothing to wait FOR), so this proves it instead by
    /// reading exactly one frame and requiring it to be the sentinel's:
    /// since RESP frames are self-delimited, any stray reply to `parts`
    /// would BE that frame instead, and the mismatch below names it.
    fn send_expect_silence(&mut self, parts: &[&str]) {
        let owned: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.write_cmd(&owned);
        let nonce = format!("MONSILENCE{}", BARRIER_SEQ.fetch_add(1, Ordering::Relaxed));
        self.write_cmd(&[b"ECHO", nonce.as_bytes()]);
        let want = format!("${}\r\n{}\r\n", nonce.len(), nonce);
        let deadline = Instant::now() + CEILING;
        loop {
            let frame = self.read_one_frame(deadline);
            let text = s(&frame);
            if text == want {
                // Found the sentinel's own reply. If `self` also monitors
                // itself (it does whenever it is an attached MONITOR
                // connection — a monitor sees its own traffic), the feed
                // line for this same ECHO may still be in flight and would
                // otherwise leak into whatever reads `self`'s feed next.
                // Consume it here rather than trust an arrival order that
                // isn't guaranteed (the feed line travels through a
                // separate channel + consumer task from the direct reply).
                let trailing = self.feed_bounded(Instant::now() + Duration::from_millis(500));
                assert!(
                    trailing.is_empty() || s(&trailing).contains(&nonce),
                    "unexpected trailing data after the sentinel reply: {:?}",
                    s(&trailing)
                );
                return;
            }
            assert!(
                text.contains(&nonce),
                "{parts:?} must be answered with NOTHING before the sentinel \
                 ECHO that follows it; a reply here means the silence \
                 contract broke. Got {text:?}"
            );
            // A self-observed feed line for our own sentinel — expected
            // when this connection also monitors itself; discard it and
            // keep waiting for the actual ECHO reply.
        }
    }

    /// Read exactly one complete RESP frame, however long it takes to
    /// arrive. `deadline` exists ONLY to turn a genuine hang into a failure
    /// — it is never the signal that the reply is finished; `frame_len` is.
    fn read_one_frame(&mut self, deadline: Instant) -> Vec<u8> {
        loop {
            if let Some(n) = frame_len(&self.carry) {
                return self.carry.drain(..n).collect();
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for a complete RESP frame; have {} \
                     byte(s) buffered so far: {:?}",
                    self.carry.len(),
                    s(&self.carry)
                );
            }
            if self.pump() {
                panic!(
                    "connection closed while waiting for a complete RESP \
                     frame; have {} byte(s) buffered: {:?}",
                    self.carry.len(),
                    s(&self.carry)
                );
            }
        }
    }

    /// Read exactly `n` complete RESP frames (e.g. `n` pipelined replies),
    /// bounded by one shared `deadline` for the whole batch.
    fn read_n_frames(&mut self, n: usize, deadline: Instant) -> Vec<u8> {
        let mut got = Vec::new();
        for _ in 0..n {
            got.extend_from_slice(&self.read_one_frame(deadline));
        }
        got
    }

    /// Read monitor feed lines until `done` is satisfied, returning
    /// everything read so far. For presence/ordering assertions where the
    /// exact final line count is not itself under test: `done` returns as
    /// soon as the property holds, so a fast real answer is never held up.
    fn feed_while(&mut self, deadline: Instant, mut done: impl FnMut(&[u8]) -> bool) -> Vec<u8> {
        let mut got = Vec::new();
        loop {
            if done(&got) {
                return got;
            }
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for the monitor feed condition; got {} \
                     line(s) so far: {:?}",
                    lines(&got).len(),
                    s(&got)
                );
            }
            got.extend_from_slice(&self.read_one_frame(deadline));
        }
    }

    /// Read monitor feed lines until every command in `cmds` has appeared
    /// (in any order) — the tool for pure presence checks, safe to use
    /// regardless of shard count since it makes no cross-connection
    /// ordering assumption (unlike `feed_barrier`).
    fn feed_until_all_named(&mut self, cmds: &[&str], deadline: Instant) -> Vec<u8> {
        self.feed_while(deadline, |got| cmds.iter().all(|c| names(got, c)))
    }

    /// Read monitor feed lines up to, but NOT including, the first line
    /// naming `marker`. `marker` must be a command guaranteed to be fed and
    /// to run strictly after everything already sent — see `feed_barrier`
    /// for how that guarantee is built. This is the deterministic
    /// replacement for "wait a while, then assume nothing else is coming":
    /// everything returned arrived strictly before a provably-later,
    /// provably-fed event, so it is safe for an EXACT line-count assertion,
    /// not just a presence one.
    fn feed_until_marker(&mut self, marker: &str, deadline: Instant) -> Vec<u8> {
        let mut got = Vec::new();
        loop {
            if Instant::now() >= deadline {
                panic!(
                    "timed out waiting for barrier marker {marker:?}; got {} \
                     line(s) so far: {:?}",
                    lines(&got).len(),
                    s(&got)
                );
            }
            let frame = self.read_one_frame(deadline);
            if s(&frame).contains(marker) {
                return got;
            }
            got.extend_from_slice(&frame);
        }
    }

    /// Fire a uniquely-tagged `ECHO` on `barrier_conn` — already
    /// authenticated by the caller if the server requires it — and read
    /// `self`'s feed up to (not including) that ECHO's line.
    ///
    /// Why this is an EXACT cutoff, not a guess: every call site sends the
    /// commands under test with a blocking `send()` first, so by the time
    /// the barrier connection is even opened, the server has already
    /// generated a reply for each of them — which happens strictly AFTER
    /// `monitor::feed_frames` runs for that command (feed is called before
    /// dispatch). The barrier's own feed line then travels through the same
    /// single-producer-per-shard channel to the same consumer task as
    /// everything queued before it, so a single-shard MPSC preserves order:
    /// seeing the barrier line on the wire proves every earlier line is
    /// already on the wire too. This is the "exact cutoff" tool for tests
    /// asserting an EXACT set of lines, not just presence — without ever
    /// guessing at how long delivery takes under load.
    fn feed_barrier_via(&mut self, barrier_conn: &mut Conn) -> Vec<u8> {
        let nonce = format!("MONBARRIER{}", BARRIER_SEQ.fetch_add(1, Ordering::Relaxed));
        let r = barrier_conn.send(&["ECHO", &nonce]);
        assert_eq!(
            s(&r),
            format!("${}\r\n{}\r\n", nonce.len(), nonce),
            "barrier ECHO must succeed before it can be used as a feed cutoff"
        );
        self.feed_until_marker(&nonce, Instant::now() + CEILING)
    }

    /// `feed_barrier_via` with a fresh, unauthenticated barrier connection —
    /// the common case for every test whose server has no `--requirepass`.
    fn feed_barrier(&mut self, port: u16) -> Vec<u8> {
        let mut b = Conn::open(port);
        self.feed_barrier_via(&mut b)
    }

    /// Read whatever complete feed lines arrive before `deadline`, WITHOUT
    /// panicking if none do: for a check that expects the feed to stay
    /// silent (an unattached or detached connection), hitting the deadline
    /// with nothing captured IS the passing outcome. A real leak is still
    /// caught the instant its frame completes, not only after the window
    /// elapses — the deadline only bounds how long "nothing happened" takes
    /// to conclude; it never manufactures a false absence the way the old
    /// `drain()` did.
    fn feed_bounded(&mut self, deadline: Instant) -> Vec<u8> {
        loop {
            if let Some(n) = frame_len(&self.carry) {
                return self.carry.drain(..n).collect();
            }
            if Instant::now() >= deadline {
                return Vec::new();
            }
            if self.pump() {
                return Vec::new(); // closed with nothing buffered: still absent.
            }
        }
    }

    /// Discard every complete frame that arrives before `deadline`, unlike
    /// `feed_bounded` (which stops at the FIRST one). Used only to clear a
    /// backlog before a separate, explicit check — e.g. an EOF probe that
    /// must not be confused by leftover buffered lines the peer sent before
    /// this connection was dropped or before this test started reading.
    /// Never an assertion in itself.
    fn discard_until(&mut self, deadline: Instant) {
        loop {
            if let Some(n) = frame_len(&self.carry) {
                self.carry.drain(..n);
                continue;
            }
            if Instant::now() >= deadline {
                return;
            }
            if self.pump() {
                return;
            }
        }
    }

    /// One poll of the socket into `carry`. Returns `true` if the
    /// connection looks closed (EOF or a non-timeout error), so a caller
    /// looping on a deadline can stop immediately instead of spinning until
    /// the clock runs out. A read timeout is just a polling tick — not
    /// reported as closed.
    fn pump(&mut self) -> bool {
        let mut buf = [0u8; 8192];
        match self.sock.read(&mut buf) {
            Ok(0) => true,
            Ok(n) => {
                self.carry.extend_from_slice(&buf[..n]);
                false
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                false
            }
            Err(_) => true,
        }
    }
}

fn s(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_string()
}

/// Attach a MONITOR connection and assert it was accepted.
fn attach(port: u16) -> Conn {
    let mut m = Conn::open(port);
    let r = m.send(&["MONITOR"]);
    assert_eq!(
        s(&r),
        "+OK\r\n",
        "MONITOR must be accepted for an admin connection; got {:?}",
        s(&r)
    );
    m
}

/// Every feed line, split on CRLF, with the leading '+' retained.
fn lines(buf: &[u8]) -> Vec<String> {
    s(buf)
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .map(|l| l.to_string())
        .collect()
}

/// Does any feed line name this command as its first quoted token?
fn names(buf: &[u8], cmd: &str) -> bool {
    lines(buf).iter().any(|l| l.contains(&format!("\"{cmd}\"")))
}

// ── M1 M2 M4 ────────────────────────────────────────────────────────────────

#[test]
fn mon1_monitor_replies_ok_and_attaches() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);

    let got = mon.feed_barrier(m.port);
    let ls = lines(&got);
    assert_eq!(
        ls.len(),
        1,
        "exactly one feed line for one command; got {ls:?}"
    );
    let l = &ls[0];
    assert!(
        l.starts_with('+'),
        "a feed line is a SimpleString; got {l:?}"
    );
    // +<unix>.<micros:06> [<db> <ip>:<port>] "SET" "k" "v"
    let body = &l[1..];
    let (ts, rest) = body.split_once(' ').expect("timestamp then space");
    let (secs, micros) = ts.split_once('.').expect("secs.micros");
    assert!(
        secs.len() >= 10 && secs.chars().all(|c| c.is_ascii_digit()),
        "unix seconds; got {secs:?}"
    );
    assert_eq!(
        micros.len(),
        6,
        "micros zero-padded to exactly 6 digits; got {micros:?} in {l:?}"
    );
    assert!(
        micros.chars().all(|c| c.is_ascii_digit()),
        "micros all digits; got {micros:?}"
    );
    assert!(
        rest.starts_with("[0 127.0.0.1:"),
        "db 0 and the peer address in brackets; got {rest:?}"
    );
    assert!(
        rest.ends_with(r#""SET" "k" "v""#),
        "every token quoted, command name included; got {rest:?}"
    );
}

#[test]
fn mon2_feed_is_simplestring_under_resp3() {
    // The trap this test exists for: pubsub-resp3-push just made confirmations
    // Push frames, so the reflex is to do the same here. Redis does NOT — the
    // feed stays a SimpleString in RESP3, and a client reading it expects '+'.
    let m = spawn_moon("1");
    let mut mon = Conn::hello3(m.port);
    let r = mon.send(&["MONITOR"]);
    assert_eq!(s(&r), "+OK\r\n", "MONITOR answers +OK in RESP3 too");

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);

    let got = mon.feed_barrier(m.port);
    assert!(!got.is_empty(), "the RESP3 monitor must receive the feed");
    assert_eq!(
        got[0],
        b'+',
        "the feed line is a SimpleString under RESP3, not a Push ('>'); got {:?}",
        s(&got)
    );
}

#[test]
fn mon3_reads_are_fed() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);
    let mut mon = attach(m.port);
    c.send(&["GET", "k"]);
    let got = mon.feed_barrier(m.port);
    assert!(
        names(&got, "GET"),
        "reads are fed, not only writes; got {:?}",
        s(&got)
    );
}

#[test]
fn mon4_db_follows_select() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["SELECT", "3"]);
    c.send(&["GET", "k"]);
    let got = mon.feed_barrier(m.port);
    let ls = lines(&got);
    assert!(
        !ls.is_empty(),
        "expected at least one feed line reporting db 3; got none"
    );
    assert!(
        ls.iter().all(|l| l.contains("[3 ")),
        "both lines report the db AFTER SELECT; got {ls:?}"
    );
}

// ── M6: administrative commands ─────────────────────────────────────────────

#[test]
fn mon5_admin_commands_are_not_fed() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["CONFIG", "SET", "maxmemory", "0"]);
    c.send(&["DBSIZE"]);

    let got = mon.feed_barrier(m.port);
    assert!(
        !names(&got, "CONFIG"),
        "CONFIG is administrative and must never reach a monitor; got {:?}",
        s(&got)
    );
    // The half that proves the feed was live rather than simply broken.
    assert!(
        names(&got, "DBSIZE"),
        "DBSIZE is not administrative and must be fed — without this the test \
         would pass against a feed that emits nothing at all; got {:?}",
        s(&got)
    );
}

#[test]
fn mon6_monitor_is_absent_from_its_own_feed() {
    let m = spawn_moon("1");
    let mut mon_a = attach(m.port);
    let mut mon_b = attach(m.port);

    let a = mon_a.feed_barrier(m.port);
    assert!(
        !names(&a, "MONITOR"),
        "MONITOR is administrative, so attaching a second monitor emits nothing; got {:?}",
        s(&a)
    );

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);
    assert!(
        names(&mon_a.feed_barrier(m.port), "SET"),
        "and both monitors are still live"
    );
    assert!(
        names(&mon_b.feed_barrier(m.port), "SET"),
        "including the second one"
    );
}

#[test]
fn mon7_rejected_commands_are_not_fed() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["NOSUCHCMD", "x"]);
    c.send(&["GET"]); // arity violation
    c.send(&["PING"]);

    let got = mon.feed_barrier(m.port);
    assert!(
        !names(&got, "NOSUCHCMD"),
        "an unknown command never executes, so it is never fed; got {:?}",
        s(&got)
    );
    assert!(
        !names(&got, "GET"),
        "an arity-rejected command never executes either; got {:?}",
        s(&got)
    );
    assert!(
        names(&got, "PING"),
        "the following valid command IS fed, proving the feed is live; got {:?}",
        s(&got)
    );
}

// ── M8: redaction ───────────────────────────────────────────────────────────

#[test]
fn mon8_auth_arguments_are_redacted() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["AUTH", "hunter2"]);
    c.send(&["AUTH", "someuser", "s3cret-pw"]);

    let got = mon.feed_barrier(m.port);
    let text = s(&got);
    // The assertion that matters is the ABSENCE of the secret, not the
    // presence of the placeholder: a formatter that appended "(redacted)"
    // after emitting the password would satisfy the weaker check.
    assert!(
        !text.contains("hunter2"),
        "the single-argument AUTH password must not appear anywhere in the feed; got {text:?}"
    );
    assert!(
        !text.contains("s3cret-pw"),
        "the two-argument AUTH password must not appear anywhere in the feed; got {text:?}"
    );
    assert!(
        text.contains(r#""AUTH" "(redacted)""#),
        "AUTH pw renders as \"AUTH\" \"(redacted)\"; got {text:?}"
    );
    assert!(
        text.contains(r#""AUTH" "(redacted)" "(redacted)""#),
        "AUTH user pw redacts BOTH arguments — the username is a credential too; got {text:?}"
    );
}

#[test]
fn mon9_hello_auth_redacts_only_credentials() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["HELLO", "3", "AUTH", "default", "sekrit"]);

    let text = s(&mon.feed_barrier(m.port));
    assert!(
        !text.contains("sekrit"),
        "the HELLO AUTH password must not appear in the feed; got {text:?}"
    );
    assert!(
        text.contains(r#""HELLO" "3" "AUTH" "(redacted)" "(redacted)""#),
        "the protocol version survives; only the two arguments after AUTH are \
         redacted; got {text:?}"
    );
}

// ── M9: transactions ────────────────────────────────────────────────────────

#[test]
fn mon10_transaction_timing() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);

    c.send(&["MULTI"]);
    let at_multi = mon.feed_barrier(m.port);
    assert!(
        names(&at_multi, "MULTI"),
        "MULTI is fed when it is issued; got {:?}",
        s(&at_multi)
    );

    let q = c.send(&["SET", "q", "1"]);
    assert_eq!(s(&q), "+QUEUED\r\n");
    // Not a race, unlike the other absence checks in this file: M9 contracts
    // that a queued command is fed at EXEC and NEVER at queue time, and the
    // `+QUEUED` reply just received IS the proof it has not executed yet —
    // structurally, nothing SET-shaped can appear here no matter how long we
    // wait. `ABSENCE_GRACE` is used only so a regression that fed it early
    // is still caught promptly.
    let at_queue = mon.feed_bounded(Instant::now() + ABSENCE_GRACE);
    assert!(
        !names(&at_queue, "SET"),
        "a QUEUED command has not executed, so it must not be fed yet — this \
         is the half a naive implementation gets wrong; got {:?}",
        s(&at_queue)
    );

    c.send(&["EXEC"]);
    let at_exec = mon.feed_barrier(m.port);
    let ls = lines(&at_exec);
    let set_at = ls.iter().position(|l| l.contains("\"SET\""));
    let exec_at = ls.iter().position(|l| l.contains("\"EXEC\""));
    assert!(
        set_at.is_some() && exec_at.is_some(),
        "at EXEC both the queued command and EXEC are fed; got {ls:?}"
    );
    assert!(
        set_at < exec_at,
        "the queued command is fed BEFORE the EXEC line; got {ls:?}"
    );
}

#[test]
fn mon11_two_monitors_both_receive() {
    let m = spawn_moon("1");
    let mut a = attach(m.port);
    let mut b = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["SET", "dual", "1"]);

    let la = lines(&a.feed_barrier(m.port));
    let lb = lines(&b.feed_barrier(m.port));
    assert!(
        la.iter().any(|l| l.contains(r#""SET" "dual" "1""#)),
        "monitor A receives; got {la:?}"
    );
    assert!(
        lb.iter().any(|l| l.contains(r#""SET" "dual" "1""#)),
        "monitor B receives the same line; got {lb:?}"
    );
}

// ── M2: escaping ────────────────────────────────────────────────────────────

#[test]
fn mon12_argument_escaping_is_byte_exact() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);

    c.send_bytes(&[b"SET", b"esc", b"a\"b\\c\nd\re\tf\x00g\xffh"]);
    c.send_bytes(&[b"SET", b"utf", "h\u{e9}llo".as_bytes()]);
    c.send_bytes(&[b"SET", b"empty", b""]);

    let text = s(&mon.feed_barrier(m.port));
    assert!(
        text.contains(r#""a\"b\\c\nd\re\tf\x00g\xffh""#),
        "quote, backslash, newline, CR, tab, NUL and a high byte each escape \
         per Redis sdscatrepr semantics; got {text:?}"
    );
    assert!(
        text.contains(r#""h\xc3\xa9llo""#),
        "UTF-8 escapes PER BYTE, not per character; got {text:?}"
    );
    assert!(
        text.contains(r#""SET" "empty" """#),
        "an empty argument renders as a pair of quotes; got {text:?}"
    );
}

// ── M13: every dispatch path ────────────────────────────────────────────────

#[test]
fn mon13_inline_fast_path_is_fed() {
    // A plain GET/SET takes try_inline_dispatch under monoio, which is a
    // different code path from everything else here. A feed hook missing there
    // is invisible to any test that uses another command — and GET/SET are
    // what most tests use. This is the shape of the v0.8.6 inline-GET P0.
    //
    // --shards 4: `feed_barrier`'s single-shard FIFO argument does not hold
    // here (a fresh barrier connection could land on a different shard than
    // "inline"'s), so this uses the shard-topology-agnostic presence wait
    // instead — it only needs both commands to EVENTUALLY show up, not an
    // exact cutoff.
    let m = spawn_moon("4");
    let mut mon = attach(m.port);
    let mut c = Conn::open(m.port);
    c.send(&["SET", "inline", "1"]);
    c.send(&["GET", "inline"]);

    let got = mon.feed_until_all_named(&["SET", "GET"], Instant::now() + CEILING);
    assert!(
        names(&got, "SET"),
        "the inline SET must be fed at --shards 4; got {:?}",
        s(&got)
    );
    assert!(
        names(&got, "GET"),
        "and the inline GET, which is the exact path the v0.8.6 P0 slipped \
         through; got {:?}",
        s(&got)
    );
}

// ── Rejections ──────────────────────────────────────────────────────────────

#[test]
fn mon14_non_admin_cannot_attach() {
    let m = spawn_moon("1");
    let mut admin = Conn::open(m.port);
    let r = admin.send(&[
        "ACL", "SETUSER", "lowly", "on", ">pw", "~*", "+@all", "-@admin",
    ]);
    assert_eq!(s(&r), "+OK\r\n", "test fixture: create a non-admin user");

    let mut lo = Conn::open(m.port);
    assert_eq!(s(&lo.send(&["AUTH", "lowly", "pw"])), "+OK\r\n");
    let denied = lo.send(&["MONITOR"]);
    assert!(
        denied.starts_with(b"-NOPERM"),
        "MONITOR requires the admin category; got {:?}",
        s(&denied)
    );

    // The security-relevant half: refusing the reply is worthless if the
    // connection was attached anyway. `lo` was never attached, so there is
    // no barrier line it could ever see — bound the check with
    // `ABSENCE_GRACE` instead and accept silence as the passing outcome.
    let mut c = Conn::open(m.port);
    c.send(&["SET", "secret", "value"]);
    let leaked = lo.feed_bounded(Instant::now() + ABSENCE_GRACE);
    assert!(
        leaked.is_empty(),
        "a refused MONITOR must not be attached — this connection received \
         another client's traffic: {:?}",
        s(&leaked)
    );
}

#[test]
fn mon15_monitor_rejects_arguments() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let r = c.send(&["MONITOR", "extra"]);
    assert_eq!(
        s(&r),
        "-ERR wrong number of arguments for 'monitor' command\r\n",
        "MONITOR has arity 1"
    );
    let mut other = Conn::open(m.port);
    other.send(&["SET", "k", "v"]);
    // `c` was never attached, so — as in mon14 — bound the wait and accept
    // silence as the pass.
    assert!(
        c.feed_bounded(Instant::now() + ABSENCE_GRACE).is_empty(),
        "and the connection was not attached by the failed call"
    );
}

#[test]
fn mon16_monitor_conn_cannot_touch_keyspace() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    let r = mon.send(&["SET", "x", "1"]);
    assert_eq!(
        s(&r),
        "-ERR Replica can't interact with the keyspace\r\n",
        "verbatim Redis text for a keyspace command on a monitor connection"
    );

    let mut c = Conn::open(m.port);
    c.send(&["SET", "still", "flowing"]);
    assert!(
        names(&mon.feed_barrier(m.port), "SET"),
        "and the refusal does not break the feed"
    );
}

#[test]
fn mon17_second_monitor_is_silent() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    mon.send_expect_silence(&["MONITOR"]);

    let mut c = Conn::open(m.port);
    c.send(&["SET", "once", "1"]);
    let ls = lines(&mon.feed_barrier(m.port));
    assert_eq!(
        ls.len(),
        1,
        "and the connection is attached exactly ONCE — a second registration \
         would duplicate every line; got {ls:?}"
    );
}

#[test]
fn mon18_reset_detaches() {
    let m = spawn_moon("1");
    let mut mon = attach(m.port);
    assert_eq!(s(&mon.send(&["RESET"])), "+RESET\r\n");

    let mut c = Conn::open(m.port);
    c.send(&["SET", "postreset", "1"]);
    // `mon` is detached — nothing can ever arrive on it again, so there is
    // no later marker to wait for; bound with `ABSENCE_GRACE` as above.
    assert!(
        mon.feed_bounded(Instant::now() + ABSENCE_GRACE).is_empty(),
        "RESET detaches: the feed stops"
    );
    assert_eq!(
        s(&mon.send(&["GET", "postreset"])),
        "$1\r\n1\r\n",
        "and keyspace access is restored"
    );
}

#[test]
fn mon19_command_info_monitor() {
    let m = spawn_moon("1");
    let mut c = Conn::open(m.port);
    let r = c.send(&["COMMAND", "INFO", "monitor"]);
    let text = s(&r);
    assert!(
        text.contains("monitor"),
        "MONITOR must be registered so COMMAND and ACL can see it; got {text:?}"
    );
    assert!(
        !text.contains("*-1") && !text.starts_with("*1\r\n*-1"),
        "and answer a real spec rather than a Null element; got {text:?}"
    );
    assert!(
        text.contains("admin"),
        "carrying the admin category, or any user could read every other \
         user's traffic; got {text:?}"
    );
}

// ── M6b: the audit table, driven row by row ─────────────────────────────────

#[test]
fn mon20_admin_audit_table() {
    // The regression guard for the §0 audit. Moon's own CommandFlags::ADMIN is
    // CONTAINER-granular and cannot express this table: flagging on it would
    // hide INFO, CLIENT GETNAME, ACL WHOAMI and CLUSTER INFO, all of which
    // Redis feeds. Every row here was measured against redis-server 8.6.1.
    let m = spawn_moon("1");

    // (command, must_be_fed)
    let cases: &[(&[&str], bool)] = &[
        // Fed — not administrative in Redis.
        (&["INFO", "server"], true),
        (&["DBSIZE"], true),
        (&["LASTSAVE"], true),
        (&["COMMAND", "COUNT"], true),
        (&["CLIENT", "GETNAME"], true),
        (&["CLIENT", "ID"], true),
        (&["ACL", "WHOAMI"], true),
        (&["ACL", "CAT"], true),
        (&["CLUSTER", "INFO"], true),
        (&["MEMORY", "USAGE", "nokey"], true),
        // Fed despite Redis's own `skip_monitor` flag — measured directly.
        // Skipping the EVAL family would hide exactly what an operator most
        // wants to see.
        (&["EVAL", "return 1", "0"], true),
        // Hidden — administrative.
        (&["CONFIG", "GET", "maxmemory"], false),
        (&["CONFIG", "SET", "maxmemory", "0"], false),
        (&["CLIENT", "LIST"], false),
        (&["ACL", "LIST"], false),
        (&["SLOWLOG", "LEN"], false),
        (&["SLOWLOG", "RESET"], false),
        (&["LATENCY", "RESET"], false),
    ];

    let mut failures: Vec<String> = Vec::new();
    for (cmd, want_fed) in cases {
        let mut mon = attach(m.port);
        let mut c = Conn::open(m.port);
        c.send(cmd);
        let got = mon.feed_barrier(m.port);
        let fed = names(&got, cmd[0]);
        if fed != *want_fed {
            failures.push(format!(
                "  {:30} expected {:6} got {:6}",
                cmd.join(" "),
                if *want_fed { "fed" } else { "hidden" },
                if fed { "fed" } else { "hidden" }
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "monitor visibility diverges from redis-server 8.6.1 on {} row(s).\n\
         A row that flipped to `fed` is a LEAK of administrative arguments; a \
         row that flipped to `hidden` silently under-reports the feed.\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// ── The contracted backpressure policy ──────────────────────────────────────

#[test]
fn mon21_slow_monitor_is_dropped_not_stalled() {
    // Contracted at freeze: a monitor that cannot keep up has its CONNECTION
    // DROPPED, loudly. Not silent line-dropping (an operator cannot tell a
    // quiet server from a lossy feed) and never blocking (one slow TCP reader
    // must not stall every shard).
    //
    // The elapsed-time bound below (`BURST_CEILING`) is a genuine wall-clock
    // assertion — there is no protocol-level marker for "did not block
    // indefinitely" the way there is for "this reply arrived". It is
    // deliberately NOT scaled to the burst size: dropping a slow reader is
    // an O(1) decision (a bounded queue overflows or it doesn't), so a
    // correct implementation should clear 20,000 pipelined commands in low
    // single-digit seconds even on a loaded host, while the failure mode
    // being guarded against — an indefinite block on a full channel/send
    // buffer — blows through any reasonable ceiling by orders of magnitude.
    // `BURST_CEILING` is widened from the original 20s for CI headroom, not
    // because the property changed; `BURST_READ_CEILING` is a SEPARATE,
    // much larger backstop that exists only to fail this test in finite
    // time if the connection truly never unblocks, so it never masks a
    // `BURST_CEILING` violation the way one shared deadline would.
    const BURST_CEILING: Duration = Duration::from_secs(45);
    const BURST_READ_CEILING: Duration = Duration::from_secs(180);

    let m = spawn_moon("1");
    let mon = attach(m.port);
    // Deliberately never read from `mon` again.

    let mut c = Conn::open(m.port);
    let start = Instant::now();
    for i in 0..20_000 {
        c.write_cmd(&[b"SET", b"burst", i.to_string().as_bytes()]);
    }
    let _ = c.read_n_frames(20_000, Instant::now() + BURST_READ_CEILING);
    let elapsed = start.elapsed();

    assert!(
        elapsed < BURST_CEILING,
        "a monitor that stopped reading must never stall the publishing \
         connection; the burst took {elapsed:?}"
    );

    // And the publisher is still healthy.
    let mut probe = Conn::open(m.port);
    assert_eq!(
        s(&probe.send(&["PING"])),
        "+PONG\r\n",
        "the server is still serving after a slow monitor was shed"
    );

    // The monitor connection itself was closed rather than left half-alive.
    // Assert on END OF STREAM, not on an empty drain: `after.is_empty()` is
    // true whenever no line happened to be buffered, so an earlier version of
    // this assertion was satisfied by a STARVED BUT OPEN connection — exactly
    // the failure mode the policy exists to rule out.
    //
    // Classifying the probe read as `Ok(0) | Err(_)` does not rule that out
    // either, and that is the subtler bug: a starved-but-open socket answers
    // the probe by BLOCKING until the read timeout, and a timeout is an
    // `Err` — so the very state this assertion exists to catch was scored as
    // a pass. The three outcomes have to be separated by error KIND, not
    // collapsed into "not Ok(n>0)". See `classify_probe`.
    let mut mon = mon;
    // Whatever the OS already buffered from the burst is irrelevant to this
    // check — only whether the socket is actually closed matters. Drain the
    // WHOLE backlog (not just one line — `mon` never read during the burst,
    // so plenty may be queued), stopping early if the connection closes
    // while doing so.
    mon.discard_until(Instant::now() + CEILING);
    mon.sock
        .set_read_timeout(Some(CEILING))
        .expect("read timeout");
    let _ = mon.sock.write_all(b"*1\r\n$4\r\nPING\r\n");
    let mut probe_buf = [0u8; 64];
    match classify_probe(mon.sock.read(&mut probe_buf)) {
        ProbeState::Closed => {}
        ProbeState::Starved => panic!(
            "the slow monitor's connection is STARVED BUT OPEN: it accepted the \
             probe and then answered nothing before the {CEILING:?} read timeout. \
             A feed that goes quiet without closing is precisely the lossy-but-\
             undetectable failure mode this policy exists to avoid — an operator \
             cannot tell it from an idle server."
        ),
        ProbeState::Alive(n) => panic!(
            "the slow monitor's connection must be CLOSED, not left serving: the \
             probe got {n} byte(s) back, so the connection is still live and the \
             server never shed the reader it could not keep up with."
        ),
    }
}

/// What a single probe read says about the far end of a socket we have just
/// written to.
///
/// Kept as a pure function over the `io::Result` so the three states can be
/// unit-tested without conjuring three real sockets in three real states —
/// see `classify_probe_*` below. The distinction matters because two of them
/// used to be spelled the same way (`Err(_)`), and the one that was silently
/// absorbed was the failure this file's policy exists to detect.
#[derive(Debug, PartialEq, Eq)]
enum ProbeState {
    /// Orderly close (`Ok(0)`) or a reset/abort/pipe error — the peer is gone.
    Closed,
    /// The read timed out: the socket is OPEN and the peer said nothing.
    Starved,
    /// The peer answered. Carries the byte count for the failure message.
    Alive(usize),
}

fn classify_probe(res: std::io::Result<usize>) -> ProbeState {
    use std::io::ErrorKind::*;
    match res {
        Ok(0) => ProbeState::Closed,
        Ok(n) => ProbeState::Alive(n),
        // A read timeout is reported as WouldBlock on some platforms and
        // TimedOut on others; both mean "open, but nothing came".
        Err(e) if matches!(e.kind(), WouldBlock | TimedOut) => ProbeState::Starved,
        Err(e) if matches!(e.kind(), ConnectionReset | ConnectionAborted | BrokenPipe) => {
            ProbeState::Closed
        }
        // Anything else is genuinely ambiguous. Fail closed — report it as the
        // state that FAILS the test, so an unexpected errno surfaces as a red
        // test with an errno in the message instead of a quiet pass.
        Err(e) => panic!(
            "probe read failed with an unclassifiable error: {e:?} ({:?})",
            e.kind()
        ),
    }
}

#[test]
fn classify_probe_orderly_close_is_closed() {
    assert_eq!(classify_probe(Ok(0)), ProbeState::Closed);
}

#[test]
fn classify_probe_reply_is_alive() {
    assert_eq!(classify_probe(Ok(7)), ProbeState::Alive(7));
}

/// The regression this classifier exists for: before it, both of these were
/// `Err(_)` and therefore scored as "connection closed" — a pass. A monitor
/// that is open and silent is the exact failure `mon21` is written to catch.
#[test]
fn classify_probe_read_timeout_is_starved_not_closed() {
    for kind in [std::io::ErrorKind::WouldBlock, std::io::ErrorKind::TimedOut] {
        assert_eq!(
            classify_probe(Err(std::io::Error::new(kind, "timed out"))),
            ProbeState::Starved,
            "{kind:?} means the socket is OPEN and silent, never that it closed"
        );
    }
}

#[test]
fn classify_probe_peer_gone_errors_are_closed() {
    for kind in [
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::ConnectionAborted,
        std::io::ErrorKind::BrokenPipe,
    ] {
        assert_eq!(
            classify_probe(Err(std::io::Error::new(kind, "gone"))),
            ProbeState::Closed,
            "{kind:?} means the peer is gone"
        );
    }
}

// ── contract: <addr> is the literal `lua` for script-issued commands ────────

#[test]
fn mon22_script_issued_commands_are_fed_with_the_lua_address() {
    // Measured against redis-server 8.6.1 (2026-08-14):
    //   …[0 127.0.0.1:51772] "eval" "redis.call('SET', …)" "1" "lk"
    //   …[0 lua] "SET" "lk" "v"
    //   …[0 lua] "GET" "lk"
    // The EVAL line carries the client's address; each command the SCRIPT
    // issues carries the literal `lua` instead, in execution order, after it.
    //
    // This is the one contract clause with no connection behind it — a script
    // command never passes a connection handler, so the handler-level hooks
    // cannot see it. An implementation that feeds only what clients send looks
    // completely correct until an operator watches a script-driven workload and
    // sees the EVAL but none of its effects.
    let m = spawn_moon("1");
    let mut mon = attach(m.port);

    let mut c = Conn::open(m.port);
    let r = c.send(&[
        "EVAL",
        "redis.call('SET', KEYS[1], 'v') return redis.call('GET', KEYS[1])",
        "1",
        "lk",
    ]);
    assert!(
        s(&r).contains('v'),
        "the script itself must run; got {:?}",
        s(&r)
    );

    let feed = mon.feed_barrier(m.port);
    let ls = lines(&feed);

    let eval_at = ls
        .iter()
        .position(|l| l.contains("\"EVAL\"") || l.contains("\"eval\""))
        .unwrap_or_else(|| panic!("the EVAL itself must be fed; got {ls:#?}"));
    let set_at = ls
        .iter()
        .position(|l| l.contains("\"SET\"") && l.contains("\"lk\""))
        .unwrap_or_else(|| panic!("the script's SET must be fed; got {ls:#?}"));
    let get_at = ls
        .iter()
        .position(|l| l.contains("\"GET\"") && l.contains("\"lk\""))
        .unwrap_or_else(|| panic!("the script's GET must be fed; got {ls:#?}"));

    assert!(
        eval_at < set_at && set_at < get_at,
        "script effects follow the EVAL, in execution order; got {ls:#?}"
    );
    assert!(
        ls[set_at].contains("[0 lua] "),
        "a script-issued command carries the literal `lua` address, not a \
         peer address: {:?}",
        ls[set_at]
    );
    assert!(
        ls[get_at].contains("[0 lua] "),
        "reads issued by a script are fed the same way: {:?}",
        ls[get_at]
    );
}

// ── the refusal rule: write|readonly, NOT `first_key != 0` ──────────────────

#[test]
fn mon23_refusal_rule_is_write_or_readonly_not_first_key() {
    // Re-measured against redis-server 8.6.1 (2026-08-14, one fresh connection
    // per probe — a shared socket desynchronises against the interleaved feed
    // and produced two wrong readings on the first pass).
    //
    // The §3 contract said "keyspace command", and the first implementation
    // read that as `first_key != 0`. That is measurably wrong in BOTH
    // directions' worth of rows: `DBSIZE`, `KEYS`, `SCAN`, `RANDOMKEY`,
    // `FLUSHALL`, `FLUSHDB`, `SWAPDB`, `EVAL` and `PUBLISH` all carry
    // `first_key == 0` and are all REFUSED by Redis. The rule that actually
    // reproduces every measured row is the WRITE-or-READONLY flag pair, plus
    // the script/publish family, which Redis refuses without either flag.
    let m = spawn_moon("1");

    for probe in [
        &["DBSIZE"][..],
        &["KEYS", "*"],
        &["SCAN", "0"],
        &["RANDOMKEY"],
        &["FLUSHALL"],
        &["FLUSHDB"],
        &["SWAPDB", "0", "1"],
        &["EVAL", "return 1", "0"],
        &["PUBLISH", "c", "m"],
        &["TYPE", "k"],
        &["EXISTS", "k"],
        &["TTL", "k"],
        &["MEMORY", "USAGE", "k"],
        &["GET", "k"],
        &["SET", "k", "v"],
        &["EXPIRE", "k", "1"],
    ] {
        let mut mon = attach(m.port);
        let r = mon.send(probe);
        assert_eq!(
            s(&r),
            "-ERR Replica can't interact with the keyspace\r\n",
            "{:?} is refused on a monitor connection (measured)",
            probe
        );
    }

    for probe in [
        &["PING"][..],
        &["INFO", "server"],
        &["CLIENT", "ID"],
        &["ACL", "WHOAMI"],
        &["COMMAND", "COUNT"],
        &["LASTSAVE"],
        &["TIME"],
        &["ECHO", "x"],
        &["SELECT", "1"],
        &["WAIT", "0", "0"],
        &["SCRIPT", "LOAD", "return 1"],
        &["MEMORY", "DOCTOR"],
        &["BGSAVE"],
    ] {
        let mut mon = attach(m.port);
        let r = mon.send(probe);
        assert!(
            !s(&r).starts_with("-ERR Replica can't interact"),
            "{:?} is SERVED on a monitor connection (measured); got {:?}",
            probe,
            s(&r)
        );
    }
}

// ── the first AUTH of a session — the one that carries the password ─────────

#[test]
fn mon24_first_auth_of_a_session_is_fed_and_redacted() {
    // Both handlers gate on `!conn.authenticated` ABOVE the ACL-exempt AUTH /
    // HELLO intercepts, and `continue` out of it. The feed hook sits below that
    // gate, so on a password-protected server the FIRST AUTH — the only one
    // that actually carries a credential — never reached the feed at all.
    //
    // `mon8` and `mon9` did not catch this because they run against a server
    // with no password: `conn.authenticated` is already true there, so their
    // AUTH falls through to the intercept the hook does cover. A redaction test
    // that never exercises an authenticating connection tests the wrong path.
    let m = spawn_moon_opts("1", &["--requirepass", "s3kr1t"]);

    let mut mon = Conn::open(m.port);
    assert_eq!(s(&mon.send(&["AUTH", "s3kr1t"])), "+OK\r\n");
    assert_eq!(
        s(&mon.send(&["MONITOR"])),
        "+OK\r\n",
        "the default user must be able to attach after AUTH"
    );

    // A fresh connection performing its own first AUTH.
    let mut c = Conn::open(m.port);
    assert_eq!(s(&c.send(&["AUTH", "s3kr1t"])), "+OK\r\n");

    // The server requires a password, so the barrier connection must AUTH
    // before its ECHO can run at all.
    let mut barrier = Conn::open(m.port);
    assert_eq!(s(&barrier.send(&["AUTH", "s3kr1t"])), "+OK\r\n");
    let feed = mon.feed_barrier_via(&mut barrier);
    let text = s(&feed);
    assert!(
        names(&feed, "AUTH"),
        "the first AUTH of a session must be fed — it is the one command that \
         carries a credential, so its absence is the least acceptable gap in \
         the feed. Got {text:?}"
    );
    assert!(
        !text.contains("s3kr1t"),
        "and it must be redacted: the password appears in the feed. Got {text:?}"
    );
    assert!(text.contains("\"AUTH\" \"(redacted)\""), "got {text:?}");
}

#[test]
fn mon25_first_hello_auth_of_a_session_is_fed_and_redacted() {
    // The HELLO half of the same gate.
    let m = spawn_moon_opts("1", &["--requirepass", "s3kr1t"]);

    let mut mon = Conn::open(m.port);
    assert_eq!(s(&mon.send(&["AUTH", "s3kr1t"])), "+OK\r\n");
    assert_eq!(s(&mon.send(&["MONITOR"])), "+OK\r\n");

    let mut c = Conn::open(m.port);
    let r = c.send(&["HELLO", "3", "AUTH", "default", "s3kr1t"]);
    assert!(!r.is_empty() && r[0] != b'-', "HELLO AUTH must succeed");

    let mut barrier = Conn::open(m.port);
    assert_eq!(s(&barrier.send(&["AUTH", "s3kr1t"])), "+OK\r\n");
    let feed = mon.feed_barrier_via(&mut barrier);
    let text = s(&feed);
    assert!(names(&feed, "HELLO"), "got {text:?}");
    assert!(
        !text.contains("s3kr1t"),
        "the password must not survive: {text:?}"
    );
    assert!(
        text.contains("\"HELLO\" \"3\" \"AUTH\" \"(redacted)\" \"(redacted)\""),
        "the version survives; only the credentials go. Got {text:?}"
    );
}

// ── ACL: +@all must grant MONITOR ───────────────────────────────────────────

#[test]
fn mon26_plus_at_all_grants_monitor() {
    // `mon14` proves `-@admin` REFUSES MONITOR, which passes just as well when
    // no grant reaches MONITOR at all. Without this positive case, a MONITOR
    // that is ungrantable by any category looks correct.
    let m = spawn_moon("1");
    let mut admin = Conn::open(m.port);
    assert_eq!(
        s(&admin.send(&["ACL", "SETUSER", "opsy", "on", ">pw", "~*", "+@all"])),
        "+OK\r\n",
        "test fixture: create a +@all user"
    );

    let mut ops = Conn::open(m.port);
    assert_eq!(s(&ops.send(&["AUTH", "opsy", "pw"])), "+OK\r\n");
    assert_eq!(
        s(&ops.send(&["MONITOR"])),
        "+OK\r\n",
        "+@all must grant MONITOR — a category expansion that omits it makes \
         the command unreachable by any grant"
    );

    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);
    assert!(
        names(&ops.feed_barrier(m.port), "SET"),
        "and the attach is real, not just an accepted reply"
    );
}

// ── teardown: a monitor must never be silently carried across migration ─────

#[test]
fn mon27_monitor_connection_is_not_migration_eligible() {
    // Connection migration returns from the handler through its own path,
    // BEFORE the disconnect detach block runs. A monitor carried through it
    // would leave its sink registered under the same client_id while the new
    // handler starts unattached and never detaches — the registry then holds a
    // dead sink forever, which also pins `any_attached()` true and holds the
    // inline fast path down for the life of the process.
    //
    // Asserted from the outside, through the only observable the contract
    // gives: RESET detaches, and after RESET a new MONITOR must be answered
    // with `+OK` rather than the silence that means "already attached".
    //
    // --shards 4, same reasoning as mon13: no `feed_barrier` cross-connection
    // ordering assumption, just presence.
    let m = spawn_moon("4");
    let mut mon = attach(m.port);
    assert_eq!(s(&mon.send(&["RESET"])), "+RESET\r\n");
    assert_eq!(
        s(&mon.send(&["MONITOR"])),
        "+OK\r\n",
        "after RESET the registry must no longer know this connection; \
         silence here means a stale registration survived"
    );

    // And a stale registration under a reused id must never silently swallow
    // the attach: the connection is either attached with a live sink or told so.
    let mut c = Conn::open(m.port);
    c.send(&["SET", "k", "v"]);
    assert!(
        names(
            &mon.feed_until_all_named(&["SET"], Instant::now() + CEILING),
            "SET"
        ),
        "the re-attached monitor receives a live feed, not a dead sink"
    );
}
