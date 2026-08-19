//! ADD task `resp3-type-fidelity` — failing-first suite.
//!
//! Every assertion here is on the RESP **type byte**, which is precisely what
//! Moon's older suites cannot see: `scripts/test-commands.sh` drives both
//! servers through `redis-cli`, which renders replies to human-readable text
//! before any comparison runs, and `tests/redis_compat.rs` compares Moon to a
//! hand-written expectation with no redis-server in the loop. A wrong reply
//! TYPE was structurally invisible to both. That is how ~22 type-level defects
//! reached v0.8.5.
//!
//! The expected values below are transcribed from a live sweep against
//! redis-server 8.6.1 (recorded in the task's §0 oracle table) — not from
//! reading Moon's source, and not from memory of the Redis docs.
//!
//! RED on `main` (2026-08-09, moon 0.8.5+). Expected failures:
//!   r3f1  ZRANGE/ZDIFF/ZUNION/ZINTER WITHSCORES arrive flat, not pair-wrapped
//!   r3f2  ZPOPMIN's score is a BulkString, not a Double
//!   r3f3  HRANDFIELD WITHVALUES / ZRANDMEMBER WITHSCORES arrive as a Map
//!   r3f4  SPOP <count> is an Array, not a Set
//!   r3f5  SISMEMBER & co. are over-converted to Boolean
//!   r3f6  INCRBYFLOAT is a Double, and lossy
//!   r3f7  ZMSCORE / GEOPOS elements are BulkStrings, not Doubles
//!   r3f8  CONFIG GET is an Array not a Map; CLIENT INFO is Bulk not Verbatim
//!   r3f9  every shape changes inside MULTI/EXEC (no inner reply is converted)
//!   r3f12 (expected GREEN today — a pin, so a fix cannot regress it)
//! r3f11 (RESP2 byte-purity) is expected GREEN today: it pins the invariant the
//! build must not break, which is the point of writing it before the build.
//!
//! Run alone with: cargo test --test resp3_type_fidelity

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn spawn_moon(dir: &std::path::Path, shards: u32) -> (Child, u16) {
    common::spawn_listening(|port| {
        Command::new(common::find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                // The shared /Volumes checkout hovers near the 5% diskfull
                // guard; a tripped guard turns every write into MOONERR and
                // would make this suite fail for an unrelated reason.
                "--disk-free-min-pct",
                "0",
            ])
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
            .spawn()
            .expect("spawn moon")
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        common::sigkill(&mut self.0);
    }
}

fn connect(port: u16) -> TcpStream {
    let addr = format!("127.0.0.1:{port}");
    let start = Instant::now();
    loop {
        match TcpStream::connect(&addr) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(10))).ok();
                s.set_write_timeout(Some(Duration::from_secs(10))).ok();
                return s;
            }
            Err(e) => {
                assert!(
                    start.elapsed() < Duration::from_secs(30),
                    "server never accepted on {port}: {e}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Connect and return only once the server has answered a PING on THIS socket.
///
/// The listener can accept before the shard behind it is serving, and under a
/// heavily parallel run (the full suite spawns ~194 test binaries, many of them
/// servers) that first connection comes back RST. Reconnecting is the same
/// pattern `tests/wire_reachability_red.rs::wait_ready` uses.
///
/// Deliberately scoped to SETUP only: a reset during the test body still
/// panics, because there it would be a real finding rather than a startup race.
fn connect_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut s = connect(port);
        // Inline PING — no RESP framing needed, and it cannot half-consume a
        // reply if the peer dies mid-handshake.
        if s.write_all(b"PING\r\n").is_ok() {
            let mut buf = [0u8; 64];
            if let Ok(n) = s.read(&mut buf)
                && n > 0
                && buf[..n].windows(4).any(|w| w == b"PONG")
            {
                return s;
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on {port} accepted TCP but never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// RESP3 reader — keeps the type byte, which is the whole point
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum V {
    Simple(String),
    Error(String),
    Int(i64),
    Double(String),
    Bool(bool),
    Bulk(Option<Vec<u8>>),
    Verbatim(String),
    Array(Vec<V>),
    Set(Vec<V>),
    Push(Vec<V>),
    Map(Vec<(V, V)>),
    Null,
}

impl V {
    /// One character naming the RESP type. This is what the suite asserts on.
    fn tag(&self) -> char {
        match self {
            V::Simple(_) => '+',
            V::Error(_) => '-',
            V::Int(_) => ':',
            V::Double(_) => ',',
            V::Bool(_) => '#',
            V::Bulk(_) => '$',
            V::Verbatim(_) => '=',
            V::Array(_) => '*',
            V::Set(_) => '~',
            V::Push(_) => '>',
            V::Map(_) => '%',
            V::Null => '_',
        }
    }

    /// A compact type-only rendering: `*2[*2[$,]]`. Values are deliberately
    /// absent — two servers may legitimately disagree on values (SPOP is
    /// random) while the SHAPE must still match exactly.
    fn shape(&self) -> String {
        match self {
            V::Array(xs) | V::Set(xs) | V::Push(xs) => {
                let mut kinds: Vec<String> = xs.iter().map(V::shape).collect();
                kinds.dedup();
                format!("{}{}[{}]", self.tag(), xs.len(), kinds.join("|"))
            }
            V::Map(kv) => format!(
                "%{}[{}]",
                kv.len(),
                kv.first()
                    .map(|(k, v)| format!("{}:{}", k.shape(), v.shape()))
                    .unwrap_or_default()
            ),
            other => other.tag().to_string(),
        }
    }

    fn items(&self) -> &[V] {
        match self {
            V::Array(xs) | V::Set(xs) | V::Push(xs) => xs,
            _ => &[],
        }
    }

    fn as_text(&self) -> String {
        match self {
            V::Simple(s) | V::Error(s) | V::Double(s) | V::Verbatim(s) => s.clone(),
            V::Int(i) => i.to_string(),
            V::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            _ => String::new(),
        }
    }
}

struct Conn {
    s: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    fn new(port: u16, proto: u8) -> Self {
        let mut c = Conn {
            s: connect_ready(port),
            buf: Vec::with_capacity(64 * 1024),
            pos: 0,
        };
        if proto == 3 {
            let r = c.cmd(&["HELLO", "3"]);
            assert!(
                !matches!(r, V::Error(_)),
                "HELLO 3 rejected: {r:?} — the server must speak RESP3 for this suite to mean anything"
            );
        }
        c
    }

    fn send(&mut self, parts: &[&str]) {
        let mut req = Vec::with_capacity(64);
        req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            req.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
        }
        self.s.write_all(&req).expect("write cmd");
    }

    fn cmd(&mut self, parts: &[&str]) -> V {
        self.send(parts);
        self.frame()
    }

    /// Run `parts`, but discard the reply — for setup steps.
    fn setup(&mut self, parts: &[&str]) {
        let r = self.cmd(parts);
        assert!(
            !matches!(r, V::Error(_)),
            "setup command {parts:?} failed: {r:?}"
        );
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
                let start = self.pos;
                let end = start + rel;
                let out = String::from_utf8_lossy(&self.buf[start..end]).into_owned();
                self.pos = end + 2;
                return out;
            }
            self.fill();
        }
    }

    fn exact(&mut self, n: usize) -> Vec<u8> {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let out = self.buf[self.pos..self.pos + n].to_vec();
        self.pos += n + 2;
        out
    }

    fn frame(&mut self) -> V {
        let line = self.line();
        let (tag, rest) = line.split_at(1);
        let rest = rest.to_string();
        match tag {
            "+" => V::Simple(rest),
            "-" => V::Error(rest),
            ":" => V::Int(rest.parse().unwrap_or_default()),
            "," => V::Double(rest),
            "#" => V::Bool(rest == "t"),
            "_" => V::Null,
            "$" | "=" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    return V::Bulk(None);
                }
                let b = self.exact(n as usize);
                if tag == "=" {
                    V::Verbatim(String::from_utf8_lossy(&b).into_owned())
                } else {
                    V::Bulk(Some(b))
                }
            }
            "*" | "~" | ">" => {
                let n: i64 = rest.parse().unwrap_or(-1);
                if n < 0 {
                    return V::Null;
                }
                let xs = (0..n).map(|_| self.frame()).collect();
                match tag {
                    "~" => V::Set(xs),
                    ">" => V::Push(xs),
                    _ => V::Array(xs),
                }
            }
            "%" => {
                let n: i64 = rest.parse().unwrap_or(0);
                V::Map((0..n).map(|_| (self.frame(), self.frame())).collect())
            }
            other => panic!("unknown RESP type byte {other:?} in line {line:?}"),
        }
    }
}

/// One table row: a label, the setup commands, and the command under test.
/// Named because the tuple appears in several case tables.
type Case<'a> = (&'a str, &'a [&'a [&'a str]], &'a [&'a str]);

// ---------------------------------------------------------------------------
// Context runners — the same command, three ways.
// ---------------------------------------------------------------------------

/// Standalone: one command, one reply.
fn standalone(port: u16, proto: u8, setup: &[&[&str]], cmd: &[&str]) -> V {
    let mut c = Conn::new(port, proto);
    for s in setup {
        c.setup(s);
    }
    c.cmd(cmd)
}

/// Inside MULTI/EXEC: returns the INNER reply, which is the interesting one.
fn in_multi(port: u16, proto: u8, setup: &[&[&str]], cmd: &[&str]) -> V {
    let mut c = Conn::new(port, proto);
    for s in setup {
        c.setup(s);
    }
    let q = c.cmd(&["MULTI"]);
    assert!(matches!(q, V::Simple(_)), "MULTI refused: {q:?}");
    let queued = c.cmd(cmd);
    assert!(
        matches!(&queued, V::Simple(s) if s == "QUEUED"),
        "{cmd:?} was not QUEUED inside MULTI — it answered {queued:?}. \
         Transaction QUEUEING is owned by task `multi-exec-queue-semantics`, not this one."
    );
    let exec = c.cmd(&["EXEC"]);
    let inner = exec.items();
    assert_eq!(
        inner.len(),
        1,
        "EXEC returned {} replies for one queued command: {exec:?}",
        inner.len()
    );
    inner[0].clone()
}

/// Inside a pipeline: two commands written before either reply is read.
fn in_pipeline(port: u16, proto: u8, setup: &[&[&str]], cmd: &[&str]) -> V {
    let mut c = Conn::new(port, proto);
    for s in setup {
        c.setup(s);
    }
    c.send(&["PING"]);
    c.send(cmd);
    let _pong = c.frame();
    c.frame()
}

/// Assert the shape a command answers, and name the divergence when it fails.
#[track_caller]
fn assert_shape(got: &V, want: &str, what: &str) {
    assert_eq!(
        got.shape(),
        want,
        "\n  {what}\n  want (redis 8.6.1): {want}\n  got  (moon):        {}\n  raw: {got:?}\n",
        got.shape()
    );
}

// ---------------------------------------------------------------------------
// r3f1 — scored ranges are arrays of pairs whose score is a Double
// ---------------------------------------------------------------------------

#[test]
fn r3f1_scored_replies_are_pairs_of_double() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let z: &[&[&str]] = &[&["DEL", "z"], &["ZADD", "z", "1", "a", "2", "b"]];
    let two: &[&[&str]] = &[
        &["DEL", "z", "z2"],
        &["ZADD", "z", "1", "a", "2", "b"],
        &["ZADD", "z2", "1", "b"],
    ];

    // Redis: *2[*2[$|,]] — an outer array of 2-element [member, score] pairs.
    for (setup, cmd, label) in [
        (z, &["ZRANGE", "z", "0", "-1", "WITHSCORES"][..], "ZRANGE"),
        (
            z,
            &["ZREVRANGE", "z", "0", "-1", "WITHSCORES"][..],
            "ZREVRANGE",
        ),
        (
            z,
            &["ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES"][..],
            "ZRANGEBYSCORE",
        ),
        (z, &["ZPOPMIN", "z", "2"][..], "ZPOPMIN <count>"),
        (two, &["ZUNION", "2", "z", "z2", "WITHSCORES"][..], "ZUNION"),
    ] {
        let got = standalone(port, 3, setup, cmd);
        assert_shape(
            &got,
            "*2[*2[$|,]]",
            &format!("{label} WITHSCORES must pair-wrap and send Double scores"),
        );
    }

    // Single-element results, same rule.
    for (cmd, label) in [
        (&["ZDIFF", "2", "z", "z2", "WITHSCORES"][..], "ZDIFF"),
        (&["ZINTER", "2", "z", "z2", "WITHSCORES"][..], "ZINTER"),
    ] {
        let got = standalone(port, 3, two, cmd);
        assert_shape(
            &got,
            "*1[*2[$|,]]",
            &format!("{label} WITHSCORES must pair-wrap and send Double scores"),
        );
    }

    // The control that proves arg-awareness is required, not a blanket rule:
    // the SAME command without WITHSCORES stays a flat array of BulkStrings.
    let plain = standalone(port, 3, z, &["ZRANGE", "z", "0", "-1"]);
    assert_shape(
        &plain,
        "*2[$]",
        "ZRANGE without WITHSCORES must stay a flat array of BulkString",
    );
}

// ---------------------------------------------------------------------------
// r3f2 — ZPOPMIN/ZPOPMAX with no count: flat pair, Double score
// ---------------------------------------------------------------------------

#[test]
fn r3f2_zpopmin_without_count_is_flat_pair() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    for cmd in ["ZPOPMIN", "ZPOPMAX"] {
        let got = standalone(
            port,
            3,
            &[&["DEL", "z"][..], &["ZADD", "z", "1", "a", "2", "b"][..]],
            &[cmd, "z"],
        );
        assert_shape(
            &got,
            "*2[$|,]",
            &format!("{cmd} with no count is a flat [member, score] pair with a Double score"),
        );
    }
}

// ---------------------------------------------------------------------------
// r3f3 — the args decide the shape, not the command name
// ---------------------------------------------------------------------------

#[test]
fn r3f3_arg_awareness_decides_shape() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let h = &[&["DEL", "h"][..], &["HSET", "h", "f1", "v1"][..]];
    let z = &[&["DEL", "z"][..], &["ZADD", "z", "1", "a"][..]];

    let with_values = standalone(port, 3, h, &["HRANDFIELD", "h", "1", "WITHVALUES"]);
    assert_shape(
        &with_values,
        "*1[*2[$]]",
        "HRANDFIELD WITHVALUES is an ARRAY OF PAIRS — Redis does not send a Map here",
    );
    assert_ne!(
        with_values.tag(),
        '%',
        "HRANDFIELD WITHVALUES must never be a Map — that is the inversion this task fixes"
    );

    let with_scores = standalone(port, 3, z, &["ZRANDMEMBER", "z", "1", "WITHSCORES"]);
    assert_shape(
        &with_scores,
        "*1[*2[$|,]]",
        "ZRANDMEMBER WITHSCORES is an array of [member, Double] pairs, not a Map",
    );

    // Without the modifier both stay flat arrays of BulkString.
    assert_shape(
        &standalone(port, 3, h, &["HRANDFIELD", "h", "1"]),
        "*1[$]",
        "HRANDFIELD without WITHVALUES stays a flat array",
    );
    assert_shape(
        &standalone(port, 3, z, &["ZRANDMEMBER", "z", "1"]),
        "*1[$]",
        "ZRANDMEMBER without WITHSCORES stays a flat array",
    );
}

// ---------------------------------------------------------------------------
// r3f4 — SPOP: a count changes the type
// ---------------------------------------------------------------------------

#[test]
fn r3f4_spop_count_is_a_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let s = &[&["DEL", "s"][..], &["SADD", "s", "a", "b", "c"][..]];

    let with_count = standalone(port, 3, s, &["SPOP", "s", "2"]);
    assert_eq!(
        with_count.tag(),
        '~',
        "SPOP <count> must be a Set (~), got {:?}",
        with_count.shape()
    );

    let no_count = standalone(port, 3, s, &["SPOP", "s"]);
    assert_eq!(
        no_count.tag(),
        '$',
        "SPOP without a count must stay a BulkString, got {:?}",
        no_count.shape()
    );
}

// ---------------------------------------------------------------------------
// r3f5 — predicate replies stay Integer (the whole int_to_bool branch is wrong)
// ---------------------------------------------------------------------------

#[test]
fn r3f5_predicates_stay_integer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let cases: &[(&[&[&str]], &[&str])] = &[
        (
            &[&["DEL", "s"], &["SADD", "s", "a"]],
            &["SISMEMBER", "s", "a"],
        ),
        (
            &[&["DEL", "h"], &["HSET", "h", "f", "v"]],
            &["HEXISTS", "h", "f"],
        ),
        (&[&["SET", "k", "v"]], &["EXPIRE", "k", "100"]),
        (&[&["SET", "k2", "v"]], &["PEXPIRE", "k2", "100000"]),
        (
            &[&["SET", "k3", "v"], &["EXPIRE", "k3", "100"]],
            &["PERSIST", "k3"],
        ),
        (&[&["DEL", "nk"]], &["SETNX", "nk", "v"]),
        (&[&["DEL", "m1", "m2"]], &["MSETNX", "m1", "a", "m2", "b"]),
    ];

    for (setup, cmd) in cases {
        let got = standalone(port, 3, setup, cmd);
        assert_eq!(
            got.tag(),
            ':',
            "{cmd:?} must answer Integer in RESP3 — redis 8.6.1 does. Got {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// r3f6 — INCRBYFLOAT stays a BulkString, and keeps every digit
// ---------------------------------------------------------------------------

#[test]
fn r3f6_incrbyfloat_stays_bulk_and_exact() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let r3 = standalone(
        port,
        3,
        &[&["SET", "f", "10.5"][..]],
        &["INCRBYFLOAT", "f", "0.1"],
    );
    assert_eq!(
        r3.tag(),
        '$',
        "INCRBYFLOAT must stay a BulkString in RESP3 (Redis does not promote it to Double). Got {r3:?}"
    );

    let r2 = standalone(
        port,
        2,
        &[&["SET", "f2", "10.5"][..]],
        &["INCRBYFLOAT", "f2", "0.1"],
    );
    assert_eq!(
        r3.as_text(),
        r2.as_text(),
        "the RESP3 INCRBYFLOAT reply must be byte-identical to RESP2 — \
         promoting it to a Double loses precision"
    );

    let h3 = standalone(
        port,
        3,
        &[&["DEL", "h"][..], &["HSET", "h", "f", "10.5"][..]],
        &["HINCRBYFLOAT", "h", "f", "0.1"],
    );
    assert_eq!(
        h3.tag(),
        '$',
        "HINCRBYFLOAT must stay a BulkString. Got {h3:?}"
    );
}

// ---------------------------------------------------------------------------
// r3f7 — ZMSCORE and GEOPOS carry Doubles, and preserve Null
// ---------------------------------------------------------------------------

#[test]
fn r3f7_zmscore_and_geopos_are_doubles() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    // One present member, one absent -> Redis: *2[,|_]
    let zm = standalone(
        port,
        3,
        &[&["DEL", "z"][..], &["ZADD", "z", "1.5", "a"][..]],
        &["ZMSCORE", "z", "a", "missing"],
    );
    assert_shape(
        &zm,
        "*2[,|_]",
        "ZMSCORE returns Double scores, with Null preserved for an absent member",
    );

    let gp = standalone(
        port,
        3,
        &[
            &["DEL", "g"][..],
            &["GEOADD", "g", "13.361389", "38.115556", "p"][..],
        ],
        &["GEOPOS", "g", "p"],
    );
    assert_shape(
        &gp,
        "*1[*2[,]]",
        "GEOPOS returns [longitude, latitude] as Doubles",
    );
}

// ---------------------------------------------------------------------------
// r3f8 — Map and Verbatim replies
// ---------------------------------------------------------------------------

#[test]
fn r3f8_map_replies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let hgetall = standalone(
        port,
        3,
        &[
            &["DEL", "h"][..],
            &["HSET", "h", "f1", "v1", "f2", "v2"][..],
        ],
        &["HGETALL", "h"],
    );
    assert_eq!(
        hgetall.tag(),
        '%',
        "HGETALL is a Map in RESP3. Got {hgetall:?}"
    );

    let config = standalone(port, 3, &[], &["CONFIG", "GET", "maxmemory"]);
    assert_eq!(
        config.tag(),
        '%',
        "CONFIG GET is a Map in RESP3 — today its call site never reaches the converter. Got {config:?}"
    );

    let xinfo = standalone(
        port,
        3,
        &[&["DEL", "st"][..], &["XADD", "st", "*", "f", "v"][..]],
        &["XINFO", "STREAM", "st"],
    );
    assert_eq!(
        xinfo.tag(),
        '%',
        "XINFO STREAM is a Map in RESP3. Got {xinfo:?}"
    );

    let client_info = standalone(port, 3, &[], &["CLIENT", "INFO"]);
    assert_eq!(
        client_info.tag(),
        '=',
        "CLIENT INFO is a Verbatim string in RESP3. Got {client_info:?}"
    );
}

// ---------------------------------------------------------------------------
// r3f9 — a command must not change shape by context
// ---------------------------------------------------------------------------

#[test]
fn r3f9_shape_is_identical_in_multi_and_pipeline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let cases: &[Case] = &[
        (
            "HGETALL",
            &[&["DEL", "h"], &["HSET", "h", "f1", "v1", "f2", "v2"]],
            &["HGETALL", "h"],
        ),
        (
            "SMEMBERS",
            &[&["DEL", "s"], &["SADD", "s", "a", "b"]],
            &["SMEMBERS", "s"],
        ),
        (
            "ZSCORE",
            &[&["DEL", "z"], &["ZADD", "z", "1.5", "a"]],
            &["ZSCORE", "z", "a"],
        ),
        (
            "ZRANGE WITHSCORES",
            &[&["DEL", "z"], &["ZADD", "z", "1", "a", "2", "b"]],
            &["ZRANGE", "z", "0", "-1", "WITHSCORES"],
        ),
        (
            "SISMEMBER",
            &[&["DEL", "s"], &["SADD", "s", "a"]],
            &["SISMEMBER", "s", "a"],
        ),
    ];

    for (label, setup, cmd) in cases {
        let alone = standalone(port, 3, setup, cmd).shape();
        let multi = in_multi(port, 3, setup, cmd).shape();
        let pipe = in_pipeline(port, 3, setup, cmd).shape();
        assert_eq!(
            alone, multi,
            "{label}: shape changes inside MULTI/EXEC — standalone {alone}, inside EXEC {multi}. \
             The same command must answer the same shape in every context."
        );
        assert_eq!(
            alone, pipe,
            "{label}: shape changes inside a pipeline — standalone {alone}, pipelined {pipe}."
        );
    }
}

// ---------------------------------------------------------------------------
// r3f10 — shape does not depend on which shard answered
// ---------------------------------------------------------------------------

#[test]
fn r3f10_shape_is_identical_across_shards() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 4);
    let _g = ServerGuard(child);

    // Hash tags pin each key to a shard; with 4 shards, {t0} and {t7} land on
    // different ones, so one of the two is answered over the cross-shard reply
    // path wherever the kernel placed this connection.
    let mut shapes = Vec::new();
    for tag in ["t0", "t1", "t2", "t3", "t7"] {
        let key = format!("{{{tag}}}z");
        let setup: &[&[&str]] = &[&["DEL", &key], &["ZADD", &key, "1", "a", "2", "b"]];
        let got = standalone(port, 3, setup, &["ZRANGE", &key, "0", "-1", "WITHSCORES"]);
        shapes.push((tag, got.shape()));
    }

    let first = shapes[0].1.clone();
    for (tag, shape) in &shapes {
        assert_eq!(
            *shape, first,
            "ZRANGE WITHSCORES answers shape {shape} for key {{{tag}}} but {first} for {{t0}} — \
             the reply shape must not depend on which shard owns the key"
        );
    }
    assert_eq!(
        first, "*2[*2[$|,]]",
        "and every shard must answer the Redis shape, not merely agree with each other"
    );
}

// ---------------------------------------------------------------------------
// r3f11 — RESP2 must not change at all (expected GREEN today; a regression pin)
// ---------------------------------------------------------------------------

#[test]
fn r3f11_resp2_is_byte_identical() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let cases: &[(&[&[&str]], &[&str])] = &[
        (
            &[&["DEL", "z"], &["ZADD", "z", "1", "a", "2", "b"]],
            &["ZRANGE", "z", "0", "-1", "WITHSCORES"],
        ),
        (
            &[&["DEL", "h"], &["HSET", "h", "f", "v"]],
            &["HGETALL", "h"],
        ),
        (&[&["DEL", "s"], &["SADD", "s", "a"]], &["SMEMBERS", "s"]),
        (
            &[&["DEL", "s2"], &["SADD", "s2", "a"]],
            &["SISMEMBER", "s2", "a"],
        ),
        (&[&["SET", "f", "10.5"]], &["INCRBYFLOAT", "f", "0.1"]),
        (&[], &["CONFIG", "GET", "maxmemory"]),
        (
            &[&["DEL", "z2"], &["ZADD", "z2", "1.5", "a"]],
            &["ZSCORE", "z2", "a"],
        ),
    ];

    for (setup, cmd) in cases {
        let got = standalone(port, 2, setup, cmd);
        let shape = got.shape();
        for forbidden in ['%', '~', ',', '#', '='] {
            assert!(
                !shape.contains(forbidden),
                "RESP2 reply for {cmd:?} contains the RESP3-only type byte '{forbidden}': {shape}. \
                 RESP2 is a hard invariant — a RESP3 fix must never leak into it."
            );
        }
    }
}

// ---------------------------------------------------------------------------
// r3f12 — errors and empty results pass through untouched (pin)
// ---------------------------------------------------------------------------

#[test]
fn r3f12_errors_and_edges_pass_through() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    // WRONGTYPE with WITHSCORES: the Error must arrive as an Error, never
    // pair-wrapped or otherwise mangled by the conversion.
    let err = standalone(
        port,
        3,
        &[&["DEL", "str"][..], &["SET", "str", "notazset"][..]],
        &["ZRANGE", "str", "0", "-1", "WITHSCORES"],
    );
    assert_eq!(
        err.tag(),
        '-',
        "a WRONGTYPE error must pass through the conversion unchanged. Got {err:?}"
    );

    // A missing key with WITHSCORES: an empty array, not a malformed pair.
    let empty = standalone(
        port,
        3,
        &[&["DEL", "gone"][..]],
        &["ZRANGE", "gone", "0", "-1", "WITHSCORES"],
    );
    assert_eq!(
        empty.tag(),
        '*',
        "an empty scored range is an empty Array. Got {empty:?}"
    );
    assert!(
        empty.items().is_empty(),
        "an empty scored range must have no elements. Got {empty:?}"
    );
}

// ---------------------------------------------------------------------------
// r3f13 — emptiness must not change the reply TYPE
//
// The miss path is the one a client hits most and tests least. Oracle
// (redis-server 8.6.1, RESP3, verified 2026-08-10):
//   HGETALL nosuchkey            -> %0     CONFIG GET nosuchparam -> %0
//   SMEMBERS nosuchset           -> ~0     ZRANGE k 0 -1 WITHSCORES (miss) -> *0
// Pinned in BOTH directions: an empty map must not degrade to an Array, and an
// empty array must not be promoted to a Map/Set it never was.
// ---------------------------------------------------------------------------

#[test]
fn r3f13_empty_replies_keep_their_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    // `shape()` is asserted rather than `tag()` because it carries the element
    // count for maps too — `items()` is empty for a Map by construction, so an
    // is_empty() assert would be vacuous on exactly the two cases under test.
    for (cmd, want, what) in [
        (
            &["HGETALL", "r3f13:nokey"][..],
            "%0[]",
            "an empty HGETALL is still a Map",
        ),
        (
            &["CONFIG", "GET", "r3f13-no-such-param"][..],
            "%0[]",
            "an empty CONFIG GET is still a Map",
        ),
        (
            &["SMEMBERS", "r3f13:noset"][..],
            "~0[]",
            "an empty SMEMBERS is still a Set",
        ),
        (
            &["ZRANGE", "r3f13:noz", "0", "-1", "WITHSCORES"][..],
            "*0[]",
            "an empty scored range stays an Array — emptiness must not promote it",
        ),
    ] {
        let got = standalone(port, 3, &[&["DEL", "r3f13:nokey"][..]], cmd);
        assert_eq!(got.shape(), want, "{what}. Got {got:?}");
    }
}

// ---------------------------------------------------------------------------
// r3f14 — the REST of the score family: keyed pops carry Doubles too (moon#559)
//
// Oracle (redis-server 8.x, RESP3). Every one of these replies is built by
// `genericZpopCommand`, which emits the score through `addReplyDouble` — the
// same call ZPOPMIN uses, and `addReplyDouble` is `,<score>` on RESP3 and
// `$<len>\r\n<score>` on RESP2:
//   BZPOPMIN k 0   -> *3  [key(bulk), member(bulk), score(DOUBLE)]
//   BZPOPMAX k 0   -> same
//   ZMPOP 1 k MIN  -> *2  [key(bulk), *N[ *2[member(bulk), score(DOUBLE)] ]]
//   BZMPOP 0 1 k MIN -> same
// The nesting is NOT the ZPOPMIN nesting: a blocking pop prefixes the key, so
// the "is this two elements or a pair list?" rule that governs ZPOPMIN cannot
// classify these — which is exactly why moon answered a BulkString score here
// while getting ZPOPMIN right.
// ---------------------------------------------------------------------------

/// The shape of a keyed single pop: `[key, member, Double]`.
const KEYED_POP: &str = "*3[$|,]";
/// The shape of a keyed multi-pop: `[key, [[member, Double], ...]]`.
const KEYED_MPOP: &str = "*2[$|*1[*2[$|,]]]";

/// Setup that guarantees the pop finds something, per case.
fn zset(key: &str) -> Vec<Vec<String>> {
    vec![
        vec!["DEL".into(), key.into()],
        vec!["ZADD".into(), key.into(), "1.5".into(), "a".into()],
    ]
}

/// `Vec<Vec<String>>` -> the borrowed form the runners take.
fn as_setup(v: &[Vec<String>]) -> Vec<Vec<&str>> {
    v.iter()
        .map(|row| row.iter().map(String::as_str).collect())
        .collect()
}

fn run_case(port: u16, proto: u8, key: &str, cmd: &[&str], ctx: &str) -> V {
    let owned = zset(key);
    let borrowed = as_setup(&owned);
    let setup: Vec<&[&str]> = borrowed.iter().map(|r| r.as_slice()).collect();
    match ctx {
        "standalone" => standalone(port, proto, &setup, cmd),
        "multi" => in_multi(port, proto, &setup, cmd),
        "pipeline" => in_pipeline(port, proto, &setup, cmd),
        other => panic!("unknown context {other}"),
    }
}

#[test]
fn r3f14_keyed_pop_scores_are_doubles_in_every_context() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let cases: &[(&str, &[&str], &str)] = &[
        ("BZPOPMIN", &["BZPOPMIN", "r3f14:z", "0"], KEYED_POP),
        ("BZPOPMAX", &["BZPOPMAX", "r3f14:z", "0"], KEYED_POP),
        // Multi-key form: the reply still names the key that served.
        (
            "BZPOPMIN 2 keys",
            &["BZPOPMIN", "r3f14:z", "r3f14:absent", "0"],
            KEYED_POP,
        ),
        ("ZMPOP", &["ZMPOP", "1", "r3f14:z", "MIN"], KEYED_MPOP),
        (
            "BZMPOP",
            &["BZMPOP", "0", "1", "r3f14:z", "MIN"],
            KEYED_MPOP,
        ),
    ];

    for (label, cmd, want) in cases {
        for ctx in ["standalone", "multi", "pipeline"] {
            let got = run_case(port, 3, "r3f14:z", cmd, ctx);
            assert_shape(
                &got,
                want,
                &format!("{label} ({ctx}) must carry a RESP3 Double score"),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// r3f15 — RESP2 stays byte-identical, and the Double's digits match the bulk
//
// Two separate promises, both easy to break with a re-typing change:
//   (a) RESP2 never sees a `,` (or any other RESP3-only type byte);
//   (b) the RESP3 Double payload is the SAME TEXT as the RESP2 bulk payload —
//       a re-typing that reformats the number is a silent precision change.
// ---------------------------------------------------------------------------

#[test]
fn r3f15_keyed_pops_keep_resp2_bytes_and_score_text() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    // `1.5` is not representable as an integer and `3` is: the two branches of
    // every score formatter in this codebase.
    for (score, key) in [("1.5", "r3f15:a"), ("3", "r3f15:b"), ("-0.25", "r3f15:c")] {
        for cmd_name in ["BZPOPMIN", "BZPOPMAX"] {
            let setup: &[&[&str]] = &[&["DEL", key], &["ZADD", key, score, "m"]];
            let r2 = standalone(port, 2, setup, &[cmd_name, key, "0"]);
            let r3 = standalone(port, 3, setup, &[cmd_name, key, "0"]);

            assert_shape(
                &r2,
                "*3[$]",
                &format!("{cmd_name} under RESP2 must stay three BulkStrings"),
            );
            assert_shape(
                &r3,
                KEYED_POP,
                &format!("{cmd_name} under RESP3 must carry a Double score"),
            );
            assert_eq!(
                r3.items()[2].as_text(),
                r2.items()[2].as_text(),
                "{cmd_name}: the RESP3 Double text must equal the RESP2 bulk text — \
                 re-typing must not reformat the score"
            );
        }

        // ZMPOP: same two promises, one level deeper.
        let setup: &[&[&str]] = &[&["DEL", key], &["ZADD", key, score, "m"]];
        let r2 = standalone(port, 2, setup, &["ZMPOP", "1", key, "MIN"]);
        let r3 = standalone(port, 3, setup, &["ZMPOP", "1", key, "MIN"]);
        assert_shape(
            &r2,
            "*2[$|*1[*2[$]]]",
            "ZMPOP under RESP2 is all BulkString",
        );
        assert_shape(&r3, KEYED_MPOP, "ZMPOP under RESP3 carries a Double score");
        assert_eq!(
            r3.items()[1].items()[0].items()[1].as_text(),
            r2.items()[1].items()[0].items()[1].as_text(),
            "ZMPOP: the RESP3 Double text must equal the RESP2 bulk text"
        );
    }

    // The blanket RESP2 pin, mirroring r3f11 for the commands this task touches.
    let setup: &[&[&str]] = &[&["DEL", "r3f15:z"], &["ZADD", "r3f15:z", "1.5", "a"]];
    for cmd in [
        &["BZPOPMIN", "r3f15:z", "0"][..],
        &["BZMPOP", "0", "1", "r3f15:z", "MIN"][..],
        &["ZMPOP", "1", "r3f15:z", "MIN"][..],
    ] {
        let shape = standalone(port, 2, setup, cmd).shape();
        for forbidden in ['%', '~', ',', '#', '='] {
            assert!(
                !shape.contains(forbidden),
                "RESP2 reply for {cmd:?} contains the RESP3-only type byte '{forbidden}': {shape}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// r3f16 — the LIVE blocking path: a pop that really blocked, then was woken
//
// The immediate-hit path and the woken path both return through
// `BlockingOutcome::Reply`, but only one of them is exercised by every other
// test here. moon#559 was reported against the live path, so the live path is
// asserted directly: connection A parks on an EMPTY key, connection B pushes,
// A's reply must carry the Double.
// ---------------------------------------------------------------------------

#[test]
fn r3f16_woken_blocking_pop_carries_a_double() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let mut a = Conn::new(port, 3);
    a.setup(&["DEL", "r3f16:z"]);
    // 10s is generous enough that a loaded CI box cannot time this out, and
    // finite so a broken wake fails the test instead of hanging the suite.
    a.send(&["BZPOPMIN", "r3f16:z", "10"]);

    // Give A time to park before the push, so this is the WOKEN path and not
    // an immediate hit racing the registration.
    std::thread::sleep(Duration::from_millis(200));
    let mut b = Conn::new(port, 2);
    b.setup(&["ZADD", "r3f16:z", "2.5", "m"]);

    let got = a.frame();
    assert_shape(
        &got,
        KEYED_POP,
        "a BZPOPMIN woken by another client must answer a Double score, \
         exactly like the immediate-hit path",
    );
    assert_eq!(got.items()[2].as_text(), "2.5");
}

// ---------------------------------------------------------------------------
// r3f18 — ZREVRANGE WITHSCORES survives MULTI (found by the #559 sweep)
//
// `ZREVRANGE` carried arity 4 in the command table where Redis has -4, and the
// MULTI queue gate is the only consumer of that number: the optional
// `WITHSCORES` made a legal command a wrong-arity error at QUEUE time, which
// aborts the entire transaction. Standalone it always worked, so nothing in
// the suite saw it.
// ---------------------------------------------------------------------------

#[test]
fn r3f18_zrevrange_withscores_is_queueable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 1);
    let _g = ServerGuard(child);

    let setup: &[&[&str]] = &[
        &["DEL", "r3f18:z"],
        &["ZADD", "r3f18:z", "1", "a", "2", "b"],
    ];
    let cmd: &[&str] = &["ZREVRANGE", "r3f18:z", "0", "-1", "WITHSCORES"];

    for proto in [2u8, 3u8] {
        let alone = standalone(port, proto, setup, cmd).shape();
        let multi = in_multi(port, proto, setup, cmd).shape();
        assert_eq!(
            alone, multi,
            "RESP{proto}: ZREVRANGE WITHSCORES must answer the same shape inside \
             MULTI as it does standalone"
        );
    }
}

// ---------------------------------------------------------------------------
// r3f17 — and the shape does not depend on which shard owned the key
// ---------------------------------------------------------------------------

#[test]
fn r3f17_keyed_pop_shape_is_identical_across_shards() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (child, port) = spawn_moon(dir.path(), 4);
    let _g = ServerGuard(child);

    for tag in ["t0", "t1", "t2", "t3", "t7"] {
        let key = format!("{{{tag}}}r3f17");
        let setup: &[&[&str]] = &[&["DEL", &key], &["ZADD", &key, "1.5", "a"]];
        let got = standalone(port, 3, setup, &["BZPOPMIN", &key, "0"]);
        assert_shape(
            &got,
            KEYED_POP,
            &format!("BZPOPMIN on key {{{tag}}} must answer the Redis shape"),
        );

        // ZMPOP is NOT a blocking intercept: when its key lives on another
        // shard the reply comes back over the cross-shard batch, where the
        // shape is carried as a 1-byte tag classified at ENQUEUE time and
        // applied on arrival. Different code from the local path, same answer.
        let setup: &[&[&str]] = &[&["DEL", &key], &["ZADD", &key, "1.5", "a"]];
        let got = standalone(port, 3, setup, &["ZMPOP", "1", &key, "MIN"]);
        assert_shape(
            &got,
            KEYED_MPOP,
            &format!("ZMPOP on key {{{tag}}} must answer the Redis shape"),
        );
    }
}
