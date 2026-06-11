//! ADD task `consistency-dispatch-gaps` — failing-first suite.
//!
//! RED tests (fail until the 25 dispatch_read arms + the CDC.READ monoio port
//! land — frozen contract §3 v2):
//!   - cdg1_registry_sweep_no_unknowns — every command in the COMMAND_META
//!     registry answers something other than "unknown command" over a real
//!     connection. On main this fails with 26 violations (XRANGE, XREAD,
//!     XREVRANGE, XINFO, XPENDING, ZDIFF, ZINTER, ZUNION, ZINTERCARD,
//!     ZRANDMEMBER, GEOPOS, GEODIST, GEOHASH, GEOSEARCH, LCS, RANDOMKEY,
//!     EXPIRETIME, PEXPIRETIME, TOUCH, LOLWUT, TIME, WAIT, SLOWLOG, ZMSCORE,
//!     XLEN, CDC.READ): the handlers' local read path consults only
//!     `dispatch_read`, which lacks their arms (CDC.READ: tokio-handler-only
//!     special case, missing on monoio).
//!   - cdg2_twenty_commands_answer_correctly — value asserts per command on a
//!     2-shard server, fixtures spread across hash tags {t0}..{t7} so both the
//!     local and cross-shard read paths are exercised wherever the kernel
//!     lands the connection (macOS REUSEPORT pins; Linux splits).
//!   - cdg3_read_arms_are_pure_reads — the new arms never mutate: expired keys
//!     are invisible (missing-form replies) and AOF does not grow from reads
//!     of live keys.
//!
//! Run alone with: cargo test --test wire_reachability_red

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Shared harness (CARGO_BIN_EXE pattern, mirrors spsc_wake_floor_red.rs)
// ---------------------------------------------------------------------------

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Fresh `--dir` per server (CWD persistence-reload trap: an inherited
/// appendonlydir would replay stale state into a throwaway test server).
fn spawn_moon(port: u16, dir: &std::path::Path, shards: u32, extra: &[&str]) -> Child {
    let mut args: Vec<String> = vec![
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        dir.to_string_lossy().into_owned(),
        "--shards".into(),
        shards.to_string(),
    ];
    for e in extra {
        args.push((*e).into());
    }
    Command::new(moon_binary())
        .args(&args)
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (CARGO_BIN_EXE_moon)")
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).ok();
                s.set_write_timeout(Some(Duration::from_secs(5))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on {port}: {e}"),
        }
    }
}

/// Wait for the server to answer PING. Generous connect deadline: the FIRST
/// exec of a freshly-linked binary on macOS can stall >10s in code-signature
/// validation (dyld/amfi).
fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf) {
            if n > 0 && buf[..n].windows(4).any(|w| w == b"PONG") {
                return s;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "server accepted TCP but never answered PING"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// Minimal RESP2 reader — enough to read one complete reply frame of any
// nesting (the sweep and the value asserts both need real frame boundaries,
// not "read once and hope").
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    /// None = null bulk ($-1)
    Bulk(Option<Vec<u8>>),
    /// None = null array (*-1)
    Array(Option<Vec<Resp>>),
}

impl Resp {
    fn is_error_containing(&self, needle: &str) -> bool {
        matches!(self, Resp::Error(e) if e.to_ascii_lowercase().contains(&needle.to_ascii_lowercase()))
    }
    fn bulk_str(&self) -> Option<String> {
        match self {
            Resp::Bulk(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
            Resp::Simple(s) => Some(s.clone()),
            _ => None,
        }
    }
    /// Flatten every bulk/simple leaf into one lossy string (for contains asserts).
    fn flat(&self) -> String {
        match self {
            Resp::Simple(s) | Resp::Error(s) => s.clone(),
            Resp::Int(i) => i.to_string(),
            Resp::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            Resp::Bulk(None) => "<nil>".into(),
            Resp::Array(Some(items)) => items.iter().map(Resp::flat).collect::<Vec<_>>().join(" "),
            Resp::Array(None) => "<nil-array>".into(),
        }
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(s: TcpStream) -> Self {
        Conn {
            s,
            buf: Vec::with_capacity(64 * 1024),
            pos: 0,
        }
    }

    fn cmd(&mut self, parts: &[&[u8]]) -> Resp {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            req.extend_from_slice(p);
            req.extend_from_slice(b"\r\n");
        }
        self.s.write_all(&req).expect("write cmd");
        self.frame()
    }

    fn cmd_s(&mut self, parts: &[&str]) -> Resp {
        let v: Vec<&[u8]> = parts.iter().map(|p| p.as_bytes()).collect();
        self.cmd(&v)
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.s.read(&mut chunk).expect("read");
        assert!(n > 0, "connection closed mid-frame");
        self.buf.extend_from_slice(&chunk[..n]);
    }

    fn line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let line =
                    String::from_utf8_lossy(&self.buf[self.pos..self.pos + rel]).into_owned();
                self.pos += rel + 2;
                return line;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2; // skip trailing CRLF
        out
    }

    fn frame(&mut self) -> Resp {
        // Compact the buffer between top-level frames so it never grows unbounded.
        if self.pos > 0 && self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
        }
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Resp::Simple(rest.to_string()),
            "-" => Resp::Error(rest.to_string()),
            ":" => Resp::Int(rest.parse().expect("int frame")),
            "$" => {
                let len: i64 = rest.parse().expect("bulk len");
                if len < 0 {
                    Resp::Bulk(None)
                } else {
                    Resp::Bulk(Some(self.exact(len as usize)))
                }
            }
            "*" => {
                let len: i64 = rest.parse().expect("array len");
                if len < 0 {
                    Resp::Array(None)
                } else {
                    let mut items = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        items.push(self.frame());
                    }
                    Resp::Array(Some(items))
                }
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// cdg1 — registry sweep: no implemented command may answer "unknown command"
// ---------------------------------------------------------------------------

/// M1/M5: send EVERY registry command over the wire. Each command gets a
/// FRESH connection so connection-mode commands (MULTI, SUBSCRIBE, MONITOR,
/// RESET, QUIT) cannot pollute later sends. No arguments are passed — a
/// wrong-arity error is a PASS (the dispatcher recognized the command);
/// only "unknown command" (or a dead connection) is a violation.
#[test]
fn cdg1_registry_sweep_no_unknowns() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), 1, &["--appendonly", "no"]));
    drop(wait_ready(port));

    // Backlogged by user decision (contract v2, 2026-06-11 "Fix 26, backlog the
    // 10"): these are advertised in COMMAND_META but implemented NOWHERE —
    // verified unknown on both runtimes including the v0.3.0 release binary.
    // They are missing FEATURES (WATCH semantics, DUMP/RESTORE serialization,
    // sharded pub/sub, latency/module admin), not dispatch-routing bugs, and
    // are tracked as an observe-phase delta: implement or deregister.
    const BACKLOGGED_UNIMPLEMENTED: &[&str] = &[
        "WATCH",
        "UNWATCH",
        "RESET",
        "SSUBSCRIBE",
        "SUNSUBSCRIBE",
        "LATENCY",
        "MODULE",
        "DUMP",
        "RESTORE",
        "RECLAMATION",
    ];

    let mut violations: Vec<String> = Vec::new();
    for name in moon::command::metadata::COMMAND_META.keys() {
        // SHUTDOWN with no args halts the server — the one operational skip.
        if *name == "SHUTDOWN" || BACKLOGGED_UNIMPLEMENTED.contains(name) {
            continue;
        }
        let mut c = Conn::new(connect(port, Duration::from_secs(10)));
        let reply =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| c.cmd(&[name.as_bytes()])));
        match reply {
            Ok(r) if r.is_error_containing("unknown command") => {
                violations.push(format!("{name}: {}", r.flat()));
            }
            Ok(_) => {}
            Err(_) => violations.push(format!("{name}: NO REPLY (read failed/timeout)")),
        }
    }
    assert!(
        violations.is_empty(),
        "{} registry commands are unreachable over the wire:\n  {}",
        violations.len(),
        violations.join("\n  ")
    );

    // Reject pin (unknown_must_stay_unknown): a genuinely unknown command
    // still gets the error, from the single source of truth.
    let mut c = Conn::new(connect(port, Duration::from_secs(10)));
    let r = c.cmd_s(&["FOOBAR"]);
    assert!(
        r.is_error_containing("unknown command"),
        "FOOBAR must remain an unknown command, got: {r:?}"
    );
}

// ---------------------------------------------------------------------------
// cdg2 — the 20 commands answer CORRECTLY (value asserts), local + cross-shard
// ---------------------------------------------------------------------------

#[test]
fn cdg2_twenty_commands_answer_correctly() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), 2, &["--appendonly", "no"]));
    drop(wait_ready(port));
    let mut c = Conn::new(connect(port, Duration::from_secs(10)));

    // Hash tags {t0}..{t7} spread fixtures across both shards: whichever shard
    // this connection landed on, some tags are local and some cross-shard, so
    // both read paths are exercised by the same loop.
    for t in 0..8 {
        let tag = format!("{{t{t}}}");

        // ---- fixtures (write commands — these work today) ----
        let s_key = format!("s:{tag}");
        assert_eq!(
            c.cmd_s(&["XADD", &s_key, "1-1", "f", "v"])
                .bulk_str()
                .as_deref(),
            Some("1-1"),
            "XADD fixture"
        );
        assert_eq!(
            c.cmd_s(&["XADD", &s_key, "2-2", "f", "w"])
                .bulk_str()
                .as_deref(),
            Some("2-2"),
            "XADD fixture"
        );
        let grp = c.cmd_s(&["XGROUP", "CREATE", &s_key, "g", "0"]);
        assert_eq!(
            grp,
            Resp::Simple("OK".into()),
            "XGROUP CREATE fixture: {grp:?}"
        );

        let z1 = format!("z1:{tag}");
        let z2 = format!("z2:{tag}");
        c.cmd_s(&["ZADD", &z1, "1", "a", "2", "b"]);
        c.cmd_s(&["ZADD", &z2, "2", "b"]);

        let g_key = format!("g:{tag}");
        let geoadd = c.cmd_s(&[
            "GEOADD",
            &g_key,
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ]);
        assert_eq!(geoadd, Resp::Int(2), "GEOADD fixture: {geoadd:?}");

        let l1 = format!("l1:{tag}");
        let l2 = format!("l2:{tag}");
        c.cmd_s(&["SET", &l1, "ohmytext"]);
        c.cmd_s(&["SET", &l2, "mynewtext"]);

        let e_key = format!("e:{tag}");
        c.cmd_s(&["SET", &e_key, "v"]);
        assert_eq!(
            c.cmd_s(&["EXPIREAT", &e_key, "9999999999"]),
            Resp::Int(1),
            "EXPIREAT fixture"
        );

        // ---- the 20 reads (RED on main: all "unknown command") ----

        // XRANGE: both entries, in order.
        let xr = c.cmd_s(&["XRANGE", &s_key, "-", "+"]);
        let flat = xr.flat();
        assert!(
            flat.contains("1-1")
                && flat.contains("2-2")
                && flat.contains('v')
                && flat.contains('w'),
            "XRANGE {tag}: {xr:?}"
        );

        // XREVRANGE: newest first.
        let xrev = c.cmd_s(&["XREVRANGE", &s_key, "+", "-"]);
        match &xrev {
            Resp::Array(Some(items)) if items.len() == 2 => {
                assert!(
                    items[0].flat().contains("2-2"),
                    "XREVRANGE order {tag}: {xrev:?}"
                );
            }
            other => panic!("XREVRANGE {tag}: {other:?}"),
        }

        // XREAD (non-blocking form).
        let xread = c.cmd_s(&["XREAD", "COUNT", "10", "STREAMS", &s_key, "0"]);
        let flat = xread.flat();
        assert!(
            flat.contains(&s_key) && flat.contains("1-1") && flat.contains("2-2"),
            "XREAD {tag}: {xread:?}"
        );

        // XINFO STREAM: reports length 2.
        let xinfo = c.cmd_s(&["XINFO", "STREAM", &s_key]);
        let flat = xinfo.flat();
        assert!(flat.contains("length"), "XINFO STREAM {tag}: {xinfo:?}");

        // XPENDING summary form on an idle group: count 0.
        let xp = c.cmd_s(&["XPENDING", &s_key, "g"]);
        match &xp {
            Resp::Array(Some(items)) if !items.is_empty() => {
                assert_eq!(items[0], Resp::Int(0), "XPENDING count {tag}: {xp:?}");
            }
            other => panic!("XPENDING {tag}: {other:?}"),
        }

        // ZDIFF / ZINTER / ZUNION / ZINTERCARD / ZRANDMEMBER.
        let zdiff = c.cmd_s(&["ZDIFF", "2", &z1, &z2]);
        assert_eq!(zdiff.flat(), "a", "ZDIFF {tag}: {zdiff:?}");
        let zinter = c.cmd_s(&["ZINTER", "2", &z1, &z2]);
        assert_eq!(zinter.flat(), "b", "ZINTER {tag}: {zinter:?}");
        let zunion = c.cmd_s(&["ZUNION", "2", &z1, &z2]);
        assert_eq!(zunion.flat(), "a b", "ZUNION {tag}: {zunion:?}");
        let zic = c.cmd_s(&["ZINTERCARD", "2", &z1, &z2]);
        assert_eq!(zic, Resp::Int(1), "ZINTERCARD {tag}: {zic:?}");
        let zrm = c.cmd_s(&["ZRANDMEMBER", &z1]);
        let m = zrm.bulk_str().unwrap_or_default();
        assert!(m == "a" || m == "b", "ZRANDMEMBER {tag}: {zrm:?}");

        // GEOPOS: coordinate prefixes (float formatting is A1 territory — the
        // consistency suite arbitrates exact bytes; here we pin the values).
        let gp = c.cmd_s(&["GEOPOS", &g_key, "Palermo"]);
        let flat = gp.flat();
        assert!(
            flat.contains("13.36") && flat.contains("38.11"),
            "GEOPOS {tag}: {gp:?}"
        );

        // GEODIST in km: Palermo–Catania ≈ 166.27 km.
        let gd = c.cmd_s(&["GEODIST", &g_key, "Palermo", "Catania", "km"]);
        let km: f64 = gd
            .bulk_str()
            .unwrap_or_default()
            .parse()
            .unwrap_or_else(|_| panic!("GEODIST {tag}: {gd:?}"));
        assert!((166.0..167.0).contains(&km), "GEODIST {tag}: {km}");

        // GEOHASH: canonical Redis hash for Palermo starts with sqc8b.
        let gh = c.cmd_s(&["GEOHASH", &g_key, "Palermo"]);
        assert!(gh.flat().starts_with("sqc8b"), "GEOHASH {tag}: {gh:?}");

        // GEOSEARCH: ASC by distance from Palermo → Palermo, Catania.
        let gs = c.cmd_s(&[
            "GEOSEARCH",
            &g_key,
            "FROMMEMBER",
            "Palermo",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
        ]);
        assert_eq!(gs.flat(), "Palermo Catania", "GEOSEARCH {tag}: {gs:?}");

        // LCS.
        let lcs = c.cmd_s(&["LCS", &l1, &l2]);
        assert_eq!(
            lcs.bulk_str().as_deref(),
            Some("mytext"),
            "LCS {tag}: {lcs:?}"
        );

        // EXPIRETIME / PEXPIRETIME.
        let et = c.cmd_s(&["EXPIRETIME", &e_key]);
        assert_eq!(et, Resp::Int(9_999_999_999), "EXPIRETIME {tag}: {et:?}");
        let pet = c.cmd_s(&["PEXPIRETIME", &e_key]);
        assert_eq!(
            pet,
            Resp::Int(9_999_999_999_000),
            "PEXPIRETIME {tag}: {pet:?}"
        );

        // TOUCH: both keys exist.
        let touch = c.cmd_s(&["TOUCH", &e_key, &l1]);
        assert_eq!(touch, Resp::Int(2), "TOUCH {tag}: {touch:?}");

        // (v2) XLEN: the stream holds exactly the two fixture entries.
        let xlen = c.cmd_s(&["XLEN", &s_key]);
        assert_eq!(xlen, Resp::Int(2), "XLEN {tag}: {xlen:?}");

        // (v2) ZMSCORE: existing member scores + nil for a missing member.
        let zms = c.cmd_s(&["ZMSCORE", &z1, "a", "b", "nosuch"]);
        match &zms {
            Resp::Array(Some(items)) if items.len() == 3 => {
                assert_eq!(
                    items[0].bulk_str().as_deref(),
                    Some("1"),
                    "ZMSCORE a {tag}: {zms:?}"
                );
                assert_eq!(
                    items[1].bulk_str().as_deref(),
                    Some("2"),
                    "ZMSCORE b {tag}: {zms:?}"
                );
                assert_eq!(items[2], Resp::Bulk(None), "ZMSCORE missing {tag}: {zms:?}");
            }
            other => panic!("ZMSCORE {tag}: {other:?}"),
        }
    }

    // RANDOMKEY: keyspace is non-empty.
    let rk = c.cmd_s(&["RANDOMKEY"]);
    assert!(rk.bulk_str().is_some(), "RANDOMKEY: {rk:?}");

    // LOLWUT: any non-error, non-empty reply.
    let lol = c.cmd_s(&["LOLWUT"]);
    assert!(
        !matches!(lol, Resp::Error(_)) && !lol.flat().is_empty(),
        "LOLWUT: {lol:?}"
    );

    // (v2) TIME: [unix-seconds, microseconds] as two integer-parseable bulks.
    let time = c.cmd_s(&["TIME"]);
    match &time {
        Resp::Array(Some(items)) if items.len() == 2 => {
            let secs: i64 = items[0]
                .bulk_str()
                .unwrap_or_default()
                .parse()
                .unwrap_or_else(|_| panic!("TIME seconds: {time:?}"));
            assert!(secs > 1_700_000_000, "TIME plausibility: {time:?}");
        }
        other => panic!("TIME: {other:?}"),
    }

    // (v2) WAIT 0 0: no replication → 0 replicas acked.
    let wait = c.cmd_s(&["WAIT", "0", "0"]);
    assert_eq!(wait, Resp::Int(0), "WAIT: {wait:?}");

    // (v2) SLOWLOG LEN / GET.
    let sl_len = c.cmd_s(&["SLOWLOG", "LEN"]);
    assert!(matches!(sl_len, Resp::Int(_)), "SLOWLOG LEN: {sl_len:?}");
    let sl_get = c.cmd_s(&["SLOWLOG", "GET"]);
    assert!(
        matches!(sl_get, Resp::Array(Some(_))),
        "SLOWLOG GET: {sl_get:?}"
    );

    // (v2) CDC.READ with no args: an ARITY error proves the dispatcher knows
    // the command (the monoio gap answers "unknown command" instead).
    let cdc = c.cmd_s(&["CDC.READ"]);
    assert!(
        matches!(&cdc, Resp::Error(_)) && !cdc.is_error_containing("unknown command"),
        "CDC.READ must be recognized (arity error), got: {cdc:?}"
    );

    // Reject pin (fastpath_regression): plain reads on the same connection
    // still answer via the existing arms.
    c.cmd_s(&["SET", "plain", "x"]);
    assert_eq!(c.cmd_s(&["GET", "plain"]).bulk_str().as_deref(), Some("x"));
}

// ---------------------------------------------------------------------------
// cdg3 — the new arms are PURE reads: expired keys invisible, AOF untouched
// ---------------------------------------------------------------------------

/// Sum of all bytes under the server's appendonlydir (multi-part AOF).
fn aof_bytes(dir: &std::path::Path) -> u64 {
    let aof_dir = dir.join("appendonlydir");
    let Ok(rd) = std::fs::read_dir(&aof_dir) else {
        return 0;
    };
    rd.filter_map(|e| e.ok())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum()
}

#[test]
fn cdg3_read_arms_are_pure_reads() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), 1, &["--appendonly", "yes"]));
    drop(wait_ready(port));
    let mut c = Conn::new(connect(port, Duration::from_secs(10)));

    // ---- (a) expired keys are invisible through every new arm ----
    c.cmd_s(&["SET", "exp:k", "v", "PX", "60"]);
    c.cmd_s(&["XADD", "exp:s", "1-1", "f", "v"]);
    c.cmd_s(&["PEXPIRE", "exp:s", "60"]);
    c.cmd_s(&["ZADD", "exp:z", "1", "a"]);
    c.cmd_s(&["PEXPIRE", "exp:z", "60"]);
    c.cmd_s(&["GEOADD", "exp:g", "13.361389", "38.115556", "Palermo"]);
    c.cmd_s(&["PEXPIRE", "exp:g", "60"]);
    std::thread::sleep(Duration::from_millis(200)); // all TTLs lapsed

    let et = c.cmd_s(&["EXPIRETIME", "exp:k"]);
    assert_eq!(et, Resp::Int(-2), "EXPIRETIME on expired key: {et:?}");
    let touch = c.cmd_s(&["TOUCH", "exp:k"]);
    assert_eq!(touch, Resp::Int(0), "TOUCH on expired key: {touch:?}");
    let xr = c.cmd_s(&["XRANGE", "exp:s", "-", "+"]);
    assert_eq!(
        xr,
        Resp::Array(Some(vec![])),
        "XRANGE on expired stream: {xr:?}"
    );
    let zd = c.cmd_s(&["ZDIFF", "1", "exp:z"]);
    assert_eq!(
        zd,
        Resp::Array(Some(vec![])),
        "ZDIFF on expired zset: {zd:?}"
    );
    let gp = c.cmd_s(&["GEOPOS", "exp:g", "Palermo"]);
    match &gp {
        Resp::Array(Some(items)) if items.len() == 1 => assert!(
            matches!(items[0], Resp::Array(None) | Resp::Bulk(None)),
            "GEOPOS on expired geo key must be a nil position: {gp:?}"
        ),
        other => panic!("GEOPOS on expired geo key: {other:?}"),
    }
    let lcs = c.cmd_s(&["LCS", "exp:k", "exp:missing"]);
    assert_eq!(
        lcs.bulk_str().as_deref(),
        Some(""),
        "LCS on missing keys is empty string: {lcs:?}"
    );
    // (v2) XLEN / ZMSCORE on expired keys: missing-form replies.
    let xlen = c.cmd_s(&["XLEN", "exp:s"]);
    assert_eq!(xlen, Resp::Int(0), "XLEN on expired stream: {xlen:?}");
    let zms = c.cmd_s(&["ZMSCORE", "exp:z", "a"]);
    assert_eq!(
        zms,
        Resp::Array(Some(vec![Resp::Bulk(None)])),
        "ZMSCORE on expired zset: {zms:?}"
    );
    // Only expired keys ever existed → RANDOMKEY must not resurrect one.
    let rk = c.cmd_s(&["RANDOMKEY"]);
    assert_eq!(
        rk,
        Resp::Bulk(None),
        "RANDOMKEY over an all-expired keyspace: {rk:?}"
    );

    // ---- (b) reads of LIVE keys never append to the AOF ----
    c.cmd_s(&["SET", "live:k", "v"]);
    c.cmd_s(&["XADD", "live:s", "1-1", "f", "v"]);
    c.cmd_s(&["ZADD", "live:z", "1", "a"]);
    c.cmd_s(&["GEOADD", "live:g", "13.361389", "38.115556", "Palermo"]);
    std::thread::sleep(Duration::from_millis(300)); // let the WAL flush settle
    let before = aof_bytes(dir.path());
    assert!(before > 0, "fixtures must have hit the AOF");
    for _ in 0..200 {
        c.cmd_s(&["TOUCH", "live:k"]);
        c.cmd_s(&["EXPIRETIME", "live:k"]);
        c.cmd_s(&["XRANGE", "live:s", "-", "+"]);
        c.cmd_s(&["ZDIFF", "1", "live:z"]);
        c.cmd_s(&["GEOPOS", "live:g", "Palermo"]);
        c.cmd_s(&["RANDOMKEY"]);
        // (v2) the new arms are reads too.
        c.cmd_s(&["XLEN", "live:s"]);
        c.cmd_s(&["ZMSCORE", "live:z", "a"]);
        c.cmd_s(&["TIME"]);
        c.cmd_s(&["SLOWLOG", "LEN"]);
    }
    std::thread::sleep(Duration::from_millis(300));
    let after = aof_bytes(dir.path());
    assert_eq!(
        before, after,
        "reads must never append to the AOF (before={before} after={after})"
    );

    // ---- (c) the write path is intact: a SET grows the AOF ----
    c.cmd_s(&["SET", "live:k2", "v2"]);
    std::thread::sleep(Duration::from_millis(300));
    let grown = aof_bytes(dir.path());
    assert!(
        grown > after,
        "a write after the read batch must append (after={after} grown={grown})"
    );
}
