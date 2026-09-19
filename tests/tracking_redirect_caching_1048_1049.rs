//! moon#1048 (CLIENT TRACKING REDIRECT) and moon#1049 (CLIENT CACHING).
//!
//! Every expected byte string below was captured from redis-server 8.6.1 over
//! a raw socket; the wire is compared, not a decoded value.
//!
//! moon#1048 — a RESP2 client caching through the two-connection pattern
//! (`CLIENT TRACKING on REDIRECT <id>` + the target `SUBSCRIBE`d to
//! `__redis__:invalidate`) received NOTHING on origin/main: invalidations were
//! only ever delivered to a connection that itself ran `CLIENT TRACKING on`.
//!
//! | case                                   | redis 8.6.1                                  | moon pre-fix |
//! |----------------------------------------|----------------------------------------------|--------------|
//! | RESP2 target, write                    | `*3 message __redis__:invalidate *1 key`     | nothing      |
//! | RESP2 target, key expires              | same                                         | nothing      |
//! | RESP2 target, FLUSHALL                 | `*3 message __redis__:invalidate $-1`        | nothing      |
//! | RESP3 target                           | `>2 invalidate *1 key`                       | nothing      |
//! | REDIRECT to an id that does not exist  | `-ERR The client ID you want redirect to …`  | `+OK`        |
//! | target disconnects, RESP3 source       | `>2 tracking-redir-broken :<id>`             | nothing      |
//! | RESP2 source, no redirect              | nothing (RESP2 cannot carry a push)          | a `>` push   |
//!
//! moon#1049 — `CLIENT CACHING yes|no` answered "unknown subcommand", so OPTIN
//! and OPTOUT tracked every read.
//!
//! Delivery accounting follows `tracking_expiry_invalidation_1013.rs`: a push
//! to an idle reader on another shard is not guaranteed inside a bounded read
//! window at `--shards 4`, so every positive case tracks N keys and asserts a
//! MAJORITY. A NEGATIVE case ("no push") is only meaningful next to a control
//! that proves delivery is working on the same connection, so each one reads a
//! control key through the path that MUST push, waits for that push, and only
//! then checks the negative key stayed silent.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const N: usize = 6;

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

fn moon_binary() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return std::path::PathBuf::from(p);
    }
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = moon_binary();
    let dir_for = |port: u16| std::env::temp_dir().join(format!("moon-trk1048-{port}"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = dir_for(port);
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
            .stderr(std::fs::File::create(tmp_dir.join("moon.stderr")).expect("create moon stderr"))
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        tmp_dir: dir_for(port),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let mut c = Resp::connect(moon.port);
        c.cmd(&["PING"]);
        if c.saw(b"+PONG") {
            return moon;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let log = std::fs::read_to_string(moon.tmp_dir.join("moon.stderr")).unwrap_or_default();
    panic!("moon (--shards {shards}) never answered PING\n--- stderr ---\n{log}");
}

/// Minimal RESP client over a blocking TcpStream. Every connection in this
/// file issues non-blocking commands only, so one connection per role can be
/// reused without a blocking reply desynchronising it.
struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn send(&mut self, args: &[&str]) {
        self.stream.write_all(&common::encode(args)).expect("write");
    }

    fn pump(&mut self, total: Duration) {
        let deadline = Instant::now() + total;
        let mut chunk = [0u8; 4096];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => {}
            }
        }
    }

    /// Send, then read until something arrives (2 s cap) plus a short settle
    /// so a reply that straddles two reads is whole.
    fn cmd(&mut self, args: &[&str]) {
        let before = self.buf.len();
        self.send(args);
        let deadline = Instant::now() + Duration::from_secs(2);
        while self.buf.len() == before && Instant::now() < deadline {
            self.pump(Duration::from_millis(20));
        }
        self.pump(Duration::from_millis(60));
    }

    /// `cmd`, returning exactly the bytes this command produced.
    fn reply(&mut self, args: &[&str]) -> Vec<u8> {
        self.clear();
        self.cmd(args);
        std::mem::take(&mut self.buf)
    }

    fn pump_until(&mut self, done: impl Fn(&[u8]) -> bool, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while !done(&self.buf) && Instant::now() < deadline {
            self.pump(Duration::from_millis(50));
        }
    }

    fn saw(&self, needle: &[u8]) -> bool {
        self.buf.windows(needle.len()).any(|w| w == needle)
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}

fn client_id(c: &mut Resp) -> String {
    let r = c.reply(&["CLIENT", "ID"]);
    let s = String::from_utf8_lossy(&r).to_string();
    s.trim_start_matches(':').trim_end().to_string()
}

/// RESP3 push redis sends for a one-key invalidation.
fn push_for(key: &str) -> Vec<u8> {
    format!(
        ">2\r\n$10\r\ninvalidate\r\n*1\r\n${}\r\n{key}\r\n",
        key.len()
    )
    .into_bytes()
}

/// RESP2 pub/sub message redis sends a REDIRECT target for a one-key
/// invalidation.
fn message_for(key: &str) -> Vec<u8> {
    format!(
        "*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n*1\r\n${}\r\n{key}\r\n",
        key.len()
    )
    .into_bytes()
}

fn keys(prefix: &str) -> Vec<String> {
    (0..N).map(|i| format!("{prefix}{i}")).collect()
}

/// How many of `keys` produced `frame(key)` on `reader` within `timeout`.
fn delivered(
    reader: &mut Resp,
    keys: &[String],
    frame: fn(&str) -> Vec<u8>,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let got = keys.iter().filter(|k| reader.saw(&frame(k))).count();
        if got == keys.len() || Instant::now() >= deadline {
            return got;
        }
        reader.pump(Duration::from_millis(100));
    }
}

fn assert_majority(case: &str, shards: &str, got: usize, of: usize, reader: &Resp) {
    assert!(
        got * 2 > of,
        "{case} at --shards {shards}: only {got}/{of} keys were delivered \
         (redis 8.6.1 delivers every one). wire: {:?}",
        String::from_utf8_lossy(&reader.buf)
    );
}

/// A RESP2 connection subscribed to `__redis__:invalidate`, and a RESP2
/// source redirecting its invalidations to it.
fn resp2_redirect_pair(port: u16, extra: &[&str]) -> (Resp, Resp) {
    let mut target = Resp::connect(port);
    let tid = client_id(&mut target);
    let sub = target.reply(&["SUBSCRIBE", "__redis__:invalidate"]);
    assert_eq!(
        sub,
        b"*3\r\n$9\r\nsubscribe\r\n$20\r\n__redis__:invalidate\r\n:1\r\n".to_vec()
    );
    let mut source = Resp::connect(port);
    let mut args = vec!["CLIENT", "TRACKING", "on"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["REDIRECT", &tid]);
    assert_eq!(source.reply(&args), b"+OK\r\n".to_vec());
    (target, source)
}

// ═══════════════════════════ moon#1048: REDIRECT ═══════════════════════════

fn redirect_resp2_write(shards: &str) {
    let m = spawn_moon(shards);
    let (mut target, mut source) = resp2_redirect_pair(m.port, &[]);
    let mut writer = Resp::connect(m.port);
    let ks = keys("rw:");
    for k in &ks {
        writer.cmd(&["SET", k, "v"]);
        assert_eq!(source.reply(&["GET", k]), b"$1\r\nv\r\n".to_vec());
    }
    for k in &ks {
        writer.cmd(&["SET", k, "v2"]);
    }
    let got = delivered(&mut target, &ks, message_for, Duration::from_secs(4));
    assert_majority("RESP2 REDIRECT, write", shards, got, N, &target);
    // The source is RESP2: nothing may land in its reply stream.
    source.pump(Duration::from_millis(200));
    assert!(
        source.buf.is_empty(),
        "RESP2 source got unsolicited bytes: {:?}",
        String::from_utf8_lossy(&source.buf)
    );
}

#[test]
fn redirect_resp2_write_1_shard() {
    redirect_resp2_write("1");
}

#[test]
fn redirect_resp2_write_4_shards() {
    redirect_resp2_write("4");
}

fn redirect_resp2_expiry(shards: &str) {
    let m = spawn_moon(shards);
    let (mut target, mut source) = resp2_redirect_pair(m.port, &[]);
    let mut writer = Resp::connect(m.port);
    let ks = keys("re:");
    for k in &ks {
        writer.cmd(&["SET", k, "v", "PX", "900"]);
        assert_eq!(source.reply(&["GET", k]), b"$1\r\nv\r\n".to_vec());
    }
    let got = delivered(&mut target, &ks, message_for, Duration::from_secs(6));
    assert_majority("RESP2 REDIRECT, expiry", shards, got, N, &target);
}

#[test]
fn redirect_resp2_expiry_1_shard() {
    redirect_resp2_expiry("1");
}

#[test]
fn redirect_resp2_expiry_4_shards() {
    redirect_resp2_expiry("4");
}

fn redirect_resp2_bcast(shards: &str) {
    let m = spawn_moon(shards);
    let (mut target, _source) = resp2_redirect_pair(m.port, &["BCAST", "PREFIX", "rb:"]);
    let mut writer = Resp::connect(m.port);
    let ks = keys("rb:");
    for k in &ks {
        writer.cmd(&["SET", k, "v"]);
    }
    let got = delivered(&mut target, &ks, message_for, Duration::from_secs(4));
    assert_majority("RESP2 REDIRECT, BCAST", shards, got, N, &target);
}

#[test]
fn redirect_resp2_bcast_1_shard() {
    redirect_resp2_bcast("1");
}

#[test]
fn redirect_resp2_bcast_4_shards() {
    redirect_resp2_bcast("4");
}

fn redirect_resp2_flushall(shards: &str) {
    let m = spawn_moon(shards);
    let (mut target, mut source) = resp2_redirect_pair(m.port, &[]);
    let mut writer = Resp::connect(m.port);
    writer.cmd(&["SET", "rf:1", "v"]);
    source.cmd(&["GET", "rf:1"]);
    writer.cmd(&["FLUSHALL"]);
    let want = b"*3\r\n$7\r\nmessage\r\n$20\r\n__redis__:invalidate\r\n$-1\r\n";
    target.pump_until(
        |b| b.windows(want.len()).any(|w| w == want),
        Duration::from_secs(4),
    );
    assert!(
        target.saw(want),
        "FLUSHALL must reach a RESP2 redirect target as a null message at --shards {shards}. wire: {:?}",
        String::from_utf8_lossy(&target.buf)
    );
}

#[test]
fn redirect_resp2_flushall_1_shard() {
    redirect_resp2_flushall("1");
}

#[test]
fn redirect_resp2_flushall_4_shards() {
    redirect_resp2_flushall("4");
}

fn redirect_resp3_target(shards: &str) {
    let m = spawn_moon(shards);
    let mut target = Resp::connect(m.port);
    target.cmd(&["HELLO", "3"]);
    let tid = client_id(&mut target);
    target.cmd(&["SUBSCRIBE", "__redis__:invalidate"]);
    target.clear();
    let mut source = Resp::connect(m.port);
    assert_eq!(
        source.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", &tid]),
        b"+OK\r\n".to_vec()
    );
    let mut writer = Resp::connect(m.port);
    let ks = keys("r3:");
    for k in &ks {
        writer.cmd(&["SET", k, "v"]);
        source.cmd(&["GET", k]);
    }
    for k in &ks {
        writer.cmd(&["SET", k, "v2"]);
    }
    let got = delivered(&mut target, &ks, push_for, Duration::from_secs(4));
    assert_majority("RESP3 REDIRECT target", shards, got, N, &target);
}

#[test]
fn redirect_resp3_target_1_shard() {
    redirect_resp3_target("1");
}

#[test]
fn redirect_resp3_target_4_shards() {
    redirect_resp3_target("4");
}

#[test]
fn redirect_to_a_missing_client_is_refused() {
    let m = spawn_moon("1");
    let mut c = Resp::connect(m.port);
    let err: &[u8] = b"-ERR The client ID you want redirect to does not exist\r\n";
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", "987654"]),
        err
    );
    assert_eq!(c.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", "0"]), err);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", "-1"]),
        err
    );
    // The existence check runs where REDIRECT is parsed, before PREFIX's.
    assert_eq!(
        c.reply(&[
            "CLIENT", "TRACKING", "on", "REDIRECT", "987654", "PREFIX", "x"
        ]),
        err
    );
    // Refused means tracking stayed off.
    assert_eq!(
        c.reply(&["CLIENT", "TRACKINGINFO"]),
        b"*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"
            .to_vec()
    );
}

fn redirect_broken_resp3_source(shards: &str) {
    let m = spawn_moon(shards);
    let mut target = Resp::connect(m.port);
    let tid = client_id(&mut target);
    target.cmd(&["SUBSCRIBE", "__redis__:invalidate"]);
    let mut source = Resp::connect(m.port);
    source.cmd(&["HELLO", "3"]);
    assert_eq!(
        source.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", &tid]),
        b"+OK\r\n".to_vec()
    );
    let mut writer = Resp::connect(m.port);
    writer.cmd(&["SET", "rg:1", "v"]);
    source.cmd(&["GET", "rg:1"]);
    source.clear();
    drop(target);
    // The target's teardown is asynchronous; give it a moment.
    std::thread::sleep(Duration::from_millis(300));
    writer.cmd(&["SET", "rg:1", "v2"]);
    let want = format!(">2\r\n$21\r\ntracking-redir-broken\r\n:{tid}\r\n").into_bytes();
    source.pump_until(
        |b| b.windows(want.len()).any(|w| w == want.as_slice()),
        Duration::from_secs(4),
    );
    assert!(
        source.saw(&want),
        "a RESP3 source whose redirect target is gone must be told at --shards {shards}. wire: {:?}",
        String::from_utf8_lossy(&source.buf)
    );
    let info = source.reply(&["CLIENT", "TRACKINGINFO"]);
    let want_info = format!(
        "%3\r\n$5\r\nflags\r\n~2\r\n$2\r\non\r\n$15\r\nbroken_redirect\r\n$8\r\nredirect\r\n:{tid}\r\n$8\r\nprefixes\r\n*0\r\n"
    );
    assert_eq!(String::from_utf8_lossy(&info), want_info);
}

#[test]
fn redirect_broken_resp3_source_1_shard() {
    redirect_broken_resp3_source("1");
}

#[test]
fn redirect_broken_resp3_source_4_shards() {
    redirect_broken_resp3_source("4");
}

/// A RESP2 connection tracking for itself cannot receive a push: redis sends
/// it nothing. moon wrote a RESP3 `>` frame into the RESP2 reply stream.
fn resp2_self_tracking_gets_no_push(shards: &str) {
    let m = spawn_moon(shards);
    let mut c = Resp::connect(m.port);
    assert_eq!(c.reply(&["CLIENT", "TRACKING", "on"]), b"+OK\r\n".to_vec());
    let mut writer = Resp::connect(m.port);
    writer.cmd(&["SET", "r2s:1", "v"]);
    assert_eq!(c.reply(&["GET", "r2s:1"]), b"$1\r\nv\r\n".to_vec());
    writer.cmd(&["SET", "r2s:1", "v2"]);
    c.pump(Duration::from_millis(600));
    assert!(
        c.buf.is_empty(),
        "RESP2 self-tracking connection got unsolicited bytes: {:?}",
        String::from_utf8_lossy(&c.buf)
    );
    assert_eq!(c.reply(&["PING"]), b"+PONG\r\n".to_vec());
}

#[test]
fn resp2_self_tracking_gets_no_push_1_shard() {
    resp2_self_tracking_gets_no_push("1");
}

#[test]
fn resp2_self_tracking_gets_no_push_4_shards() {
    resp2_self_tracking_gets_no_push("4");
}

/// A RESP3 connection that tracks for itself AND is subscribed must still get
/// its invalidations (redis 8.6.1 pushes; moon parked it where only pub/sub
/// deliveries were read).
fn resp3_subscriber_self_tracking(shards: &str) {
    let m = spawn_moon(shards);
    let mut c = Resp::connect(m.port);
    c.cmd(&["HELLO", "3"]);
    c.cmd(&["CLIENT", "TRACKING", "on"]);
    c.cmd(&["SUBSCRIBE", "some-channel"]);
    let mut writer = Resp::connect(m.port);
    let ks = keys("r3s:");
    for k in &ks {
        writer.cmd(&["SET", k, "v"]);
        c.cmd(&["GET", k]);
    }
    for k in &ks {
        writer.cmd(&["SET", k, "v2"]);
    }
    let got = delivered(&mut c, &ks, push_for, Duration::from_secs(4));
    assert_majority("RESP3 subscriber, self-tracking", shards, got, N, &c);
}

#[test]
fn resp3_subscriber_self_tracking_1_shard() {
    resp3_subscriber_self_tracking("1");
}

#[test]
fn resp3_subscriber_self_tracking_4_shards() {
    resp3_subscriber_self_tracking("4");
}

// ═══════════════════════════ moon#1049: CACHING ════════════════════════════

#[test]
fn caching_error_replies_match_redis() {
    let m = spawn_moon("1");
    let not_tracking: &[u8] = b"-ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled\r\n";
    let yes_needs_optin: &[u8] =
        b"-ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.\r\n";
    let no_needs_optout: &[u8] =
        b"-ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.\r\n";
    let arity: &[u8] = b"-ERR wrong number of arguments for 'client|caching' command\r\n";

    let mut c = Resp::connect(m.port);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "yes"]), not_tracking);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "no"]), not_tracking);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "maybe"]), not_tracking);
    assert_eq!(c.reply(&["CLIENT", "CACHING"]), arity);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "yes", "extra"]), arity);
    c.cmd(&["CLIENT", "TRACKING", "on"]);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "yes"]), yes_needs_optin);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "no"]), no_needs_optout);

    let mut c = Resp::connect(m.port);
    c.cmd(&["CLIENT", "TRACKING", "on", "OPTIN"]);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "no"]), no_needs_optout);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "YES"]), b"+OK\r\n".to_vec());
    assert_eq!(
        c.reply(&["CLIENT", "CACHING", "maybe"]),
        b"-ERR syntax error\r\n".to_vec()
    );

    let mut c = Resp::connect(m.port);
    c.cmd(&["CLIENT", "TRACKING", "on", "OPTOUT"]);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "yes"]), yes_needs_optin);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "No"]), b"+OK\r\n".to_vec());

    let mut c = Resp::connect(m.port);
    c.cmd(&["CLIENT", "TRACKING", "on", "BCAST"]);
    assert_eq!(c.reply(&["CLIENT", "CACHING", "yes"]), yes_needs_optin);
}

#[test]
fn tracking_option_errors_match_redis() {
    let m = spawn_moon("1");
    let mut c = Resp::connect(m.port);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "OPTIN", "OPTOUT"]),
        b"-ERR You can't specify both OPTIN mode and OPTOUT mode\r\n".to_vec()
    );
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "BCAST", "OPTIN"]),
        b"-ERR OPTIN and OPTOUT are not compatible with BCAST\r\n".to_vec()
    );
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "FOO"]),
        b"-ERR syntax error\r\n".to_vec()
    );
    let id = client_id(&mut c);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", &id, "REDIRECT", &id]),
        b"-ERR A client can only redirect to a single other client\r\n".to_vec()
    );
    c.cmd(&["CLIENT", "TRACKING", "on", "OPTIN"]);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "OPTOUT"]),
        b"-ERR You can't switch OPTIN/OPTOUT mode before disabling tracking for this client, and then re-enabling it with a different mode.\r\n".to_vec()
    );
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "BCAST"]),
        b"-ERR You can't switch BCAST mode on/off before disabling tracking for this client, and then re-enabling it with a different mode.\r\n".to_vec()
    );
    let mut c = Resp::connect(m.port);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "ab", "PREFIX", "a"]),
        b"-ERR Prefix 'ab' overlaps with another provided prefix 'a'. Prefixes for a single client must not overlap.\r\n".to_vec()
    );
    c.cmd(&["CLIENT", "TRACKING", "on", "BCAST"]);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "x"]),
        b"-ERR Prefix 'x' overlaps with an existing prefix ''. Prefixes for a single client must not overlap.\r\n".to_vec()
    );
}

/// One OPTIN/OPTOUT scenario on a RESP3 connection: run `pre`, read the key
/// (optionally pipelined behind `pre`), and let another client overwrite it.
struct CachingCase {
    name: &'static str,
    mode: &'static str,
    pre: &'static [&'static [&'static str]],
    pipelined: bool,
    tracked: bool,
}

const CACHING_CASES: &[CachingCase] = &[
    CachingCase {
        name: "optin, no CACHING",
        mode: "OPTIN",
        pre: &[],
        pipelined: false,
        tracked: false,
    },
    CachingCase {
        name: "optin, CACHING yes",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"]],
        pipelined: false,
        tracked: true,
    },
    CachingCase {
        name: "optin, CACHING yes pipelined with the read",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"]],
        pipelined: true,
        tracked: true,
    },
    CachingCase {
        name: "optin, CACHING yes then PING: the flag covers the NEXT command only",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"], &["PING"]],
        pipelined: false,
        tracked: false,
    },
    CachingCase {
        name: "optin, CACHING yes then PING, pipelined",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"], &["PING"]],
        pipelined: true,
        tracked: false,
    },
    CachingCase {
        name: "optin, CACHING yes then CLIENT ID: a CLIENT command keeps the flag",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"], &["CLIENT", "ID"]],
        pipelined: false,
        tracked: true,
    },
    CachingCase {
        name: "optin, CACHING yes then a failed CACHING no keeps the flag",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"], &["CLIENT", "CACHING", "no"]],
        pipelined: false,
        tracked: true,
    },
    CachingCase {
        name: "optin, CACHING yes then an unknown command clears the flag",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"], &["NOSUCHCMD"]],
        pipelined: false,
        tracked: false,
    },
    CachingCase {
        name: "optin, CACHING yes then an arity error clears the flag",
        mode: "OPTIN",
        pre: &[&["CLIENT", "CACHING", "yes"], &["GET"]],
        pipelined: false,
        tracked: false,
    },
    CachingCase {
        name: "optout, no CACHING",
        mode: "OPTOUT",
        pre: &[],
        pipelined: false,
        tracked: true,
    },
    CachingCase {
        name: "optout, CACHING no",
        mode: "OPTOUT",
        pre: &[&["CLIENT", "CACHING", "no"]],
        pipelined: false,
        tracked: false,
    },
    CachingCase {
        name: "optout, CACHING no pipelined with the read",
        mode: "OPTOUT",
        pre: &[&["CLIENT", "CACHING", "no"]],
        pipelined: true,
        tracked: false,
    },
    CachingCase {
        name: "optout, CACHING no then PING: tracked again",
        mode: "OPTOUT",
        pre: &[&["CLIENT", "CACHING", "no"], &["PING"]],
        pipelined: false,
        tracked: true,
    },
];

fn caching_semantics(shards: &str) {
    let m = spawn_moon(shards);
    let mut failures = Vec::new();
    for (i, case) in CACHING_CASES.iter().enumerate() {
        let mut c = Resp::connect(m.port);
        c.cmd(&["HELLO", "3"]);
        assert_eq!(
            c.reply(&["CLIENT", "TRACKING", "on", case.mode]),
            b"+OK\r\n".to_vec()
        );
        let mut w = Resp::connect(m.port);
        let key = format!("cc:{i}");
        let control = format!("cc:{i}:control");
        w.cmd(&["MSET", &key, "v", &control, "v"]);
        if case.pipelined {
            let mut out = Vec::new();
            for p in case.pre {
                out.extend_from_slice(&common::encode(p));
            }
            out.extend_from_slice(&common::encode(&["GET", &key]));
            c.stream.write_all(&out).unwrap();
            c.pump(Duration::from_millis(300));
        } else {
            for p in case.pre {
                c.cmd(p);
            }
            c.cmd(&["GET", &key]);
        }
        // Control: a read tracked in either mode (OPTIN via CACHING yes,
        // OPTOUT by default). Its push proves delivery works on this
        // connection, so a missing push for `key` means "not tracked".
        if case.mode == "OPTIN" {
            c.cmd(&["CLIENT", "CACHING", "yes"]);
        }
        c.cmd(&["GET", &control]);
        w.cmd(&["SET", &key, "v2"]);
        w.cmd(&["SET", &control, "v2"]);
        let want = push_for(&control);
        c.pump_until(
            |b| b.windows(want.len()).any(|x| x == want.as_slice()),
            Duration::from_secs(3),
        );
        if !c.saw(&want) {
            failures.push(format!(
                "{}: control key got no push, so no verdict is possible. wire: {:?}",
                case.name,
                String::from_utf8_lossy(&c.buf)
            ));
            continue;
        }
        c.pump(Duration::from_millis(200));
        if c.saw(&push_for(&key)) != case.tracked {
            failures.push(format!(
                "{}: expected tracked={} (redis 8.6.1). wire: {:?}",
                case.name,
                case.tracked,
                String::from_utf8_lossy(&c.buf)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "--shards {shards}:\n{}",
        failures.join("\n")
    );
}

/// One pipelined batch mixing CACHING with several reads. Each read must be
/// judged by the flag as it stood when THAT read ran — a handler that
/// registers reads after the whole batch has been scanned would see the flag
/// already consumed by a later command. At `--shards 4` the keys land on
/// different shards, so local, remote and multi-key read paths all take part.
fn caching_in_one_pipeline(shards: &str) {
    let m = spawn_moon(shards);
    let mut c = Resp::connect(m.port);
    c.cmd(&["HELLO", "3"]);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKING", "on", "OPTIN"]),
        b"+OK\r\n".to_vec()
    );
    let mut w = Resp::connect(m.port);
    let ks: Vec<String> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|s| format!("cp:{s}"))
        .collect();
    for k in &ks {
        w.cmd(&["SET", k, "v"]);
    }
    let batch: Vec<Vec<&str>> = vec![
        vec!["CLIENT", "CACHING", "yes"],
        vec!["GET", &ks[0]], // tracked
        vec!["GET", &ks[1]], // not: the flag was consumed by the GET before
        vec!["CLIENT", "CACHING", "yes"],
        vec!["MGET", &ks[2], &ks[3]], // both tracked
        vec!["PING"],
        vec!["GET", &ks[4]], // not
    ];
    let mut out = Vec::new();
    for cmd in &batch {
        out.extend_from_slice(&common::encode(cmd));
    }
    c.stream.write_all(&out).unwrap();
    c.pump(Duration::from_millis(400));
    c.clear();
    for k in &ks {
        w.cmd(&["SET", k, "v2"]);
    }
    let tracked = [ks[0].clone(), ks[2].clone(), ks[3].clone()];
    let got = delivered(&mut c, &tracked, push_for, Duration::from_secs(4));
    // Majority, per the 4-shard delivery note at the top of the file. A
    // handler that judged the batch by the flag's FINAL state tracks none.
    assert_majority(
        "reads made under CACHING yes in one pipeline",
        shards,
        got,
        tracked.len(),
        &c,
    );
    c.pump(Duration::from_millis(300));
    for k in [&ks[1], &ks[4]] {
        assert!(
            !c.saw(&push_for(k)),
            "--shards {shards}: {k} was read without CACHING yes and must not be tracked. wire: {:?}",
            String::from_utf8_lossy(&c.buf)
        );
    }
}

#[test]
fn caching_in_one_pipeline_1_shard() {
    caching_in_one_pipeline("1");
}

#[test]
fn caching_in_one_pipeline_4_shards() {
    caching_in_one_pipeline("4");
}

#[test]
fn caching_semantics_1_shard() {
    caching_semantics("1");
}

#[test]
fn caching_semantics_4_shards() {
    caching_semantics("4");
}

/// Reads inside MULTI/EXEC are tracked, and CACHING yes before MULTI (or
/// queued inside it) covers the whole transaction (redis 8.6.1).
fn caching_in_transaction(shards: &str) {
    let m = spawn_moon(shards);
    // (mode, CACHING before MULTI, CACHING yes queued inside, reads tracked)
    let cases: &[(&str, Option<&str>, bool, bool)] = &[
        ("", None, false, true),
        ("OPTIN", Some("yes"), false, true),
        ("OPTIN", None, true, true),
        ("OPTIN", None, false, false),
        ("OPTOUT", Some("no"), false, false),
        ("OPTOUT", None, false, true),
    ];
    let mut failures = Vec::new();
    for (i, (mode, pre, inside, tracked)) in cases.iter().enumerate() {
        let mut c = Resp::connect(m.port);
        c.cmd(&["HELLO", "3"]);
        let mut args = vec!["CLIENT", "TRACKING", "on"];
        if !mode.is_empty() {
            args.push(mode);
        }
        assert_eq!(c.reply(&args), b"+OK\r\n".to_vec());
        let mut w = Resp::connect(m.port);
        // Hash-tagged so the transaction is single-slot at --shards 4.
        let a = format!("{{cm{i}}}:a");
        let b = format!("{{cm{i}}}:b");
        w.cmd(&["MSET", &a, "1", &b, "2"]);
        if let Some(p) = pre {
            assert_eq!(c.reply(&["CLIENT", "CACHING", p]), b"+OK\r\n".to_vec());
        }
        c.cmd(&["MULTI"]);
        if *inside {
            c.cmd(&["CLIENT", "CACHING", "yes"]);
        }
        c.cmd(&["GET", &a]);
        c.cmd(&["GET", &b]);
        let reply = c.reply(&["EXEC"]);
        if !reply.ends_with(b"$1\r\n1\r\n$1\r\n2\r\n") {
            failures.push(format!(
                "case {i}: EXEC reply {:?}",
                String::from_utf8_lossy(&reply)
            ));
            continue;
        }
        w.cmd(&["SET", &a, "x"]);
        w.cmd(&["SET", &b, "y"]);
        let ks = vec![a.clone(), b.clone()];
        let got = delivered(
            &mut c,
            &ks,
            push_for,
            Duration::from_millis(if *tracked { 3000 } else { 800 }),
        );
        let ok = if *tracked { got >= 1 } else { got == 0 };
        if !ok {
            failures.push(format!(
                "case {i} (mode {mode:?}, before {pre:?}, queued {inside}): {got}/2 pushed, \
                 expected tracked={tracked}. wire: {:?}",
                String::from_utf8_lossy(&c.buf)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "--shards {shards}:\n{}",
        failures.join("\n")
    );
}

#[test]
fn caching_in_transaction_1_shard() {
    caching_in_transaction("1");
}

#[test]
fn caching_in_transaction_4_shards() {
    caching_in_transaction("4");
}

/// Hash tags for the transaction cases. At `--shards 4` some land on the
/// connection's own shard (the body runs locally) and some on another (the
/// body is routed to the owner), so both EXEC paths are exercised.
const TXN_TAGS: usize = 8;

/// A `CLIENT CACHING` queued in the MIDDLE of a transaction covers only the
/// commands after it (redis 8.6.1, per key):
///
/// | mode   | body                                          | pushed      |
/// |--------|-----------------------------------------------|-------------|
/// | OPTOUT | `GET {t}:a / CLIENT CACHING no  / GET {t}:b`  | `{t}:a` only |
/// | OPTIN  | `GET {t}:a / CLIENT CACHING yes / GET {t}:b`  | `{t}:b` only |
///
/// Moon applied the queued flag to the whole body: OPTOUT went silent on
/// `{t}:a` (a client caching it stays stale forever) and OPTIN tracked
/// `{t}:a` spuriously.
fn caching_mid_transaction(shards: &str) {
    let m = spawn_moon(shards);
    let mut failures = Vec::new();
    for (mode, word, tracked, untracked) in [("OPTOUT", "no", "a", "b"), ("OPTIN", "yes", "b", "a")]
    {
        let mut delivered_positive = 0;
        for t in 0..TXN_TAGS {
            let mut c = Resp::connect(m.port);
            c.cmd(&["HELLO", "3"]);
            assert_eq!(
                c.reply(&["CLIENT", "TRACKING", "on", mode]),
                b"+OK\r\n".to_vec()
            );
            let mut w = Resp::connect(m.port);
            let key = |s: &str| format!("{{mid{mode}{t}}}:{s}");
            let (a, b) = (key("a"), key("b"));
            w.cmd(&["MSET", &a, "1", &b, "2"]);
            c.cmd(&["MULTI"]);
            c.cmd(&["GET", &a]);
            c.cmd(&["CLIENT", "CACHING", word]);
            c.cmd(&["GET", &b]);
            let reply = c.reply(&["EXEC"]);
            if reply != b"*3\r\n$1\r\n1\r\n+OK\r\n$1\r\n2\r\n".to_vec() {
                failures.push(format!(
                    "{mode} tag {t}: EXEC reply {:?}",
                    String::from_utf8_lossy(&reply)
                ));
                continue;
            }
            let (pos, neg) = (key(tracked), key(untracked));
            // The untracked key is written FIRST, so by the time the tracked
            // key's push has arrived its push would have had every chance.
            w.cmd(&["SET", &neg, "x"]);
            w.cmd(&["SET", &pos, "x"]);
            let got = delivered(
                &mut c,
                std::slice::from_ref(&pos),
                push_for,
                Duration::from_secs(3),
            );
            delivered_positive += got;
            c.pump(Duration::from_millis(300));
            if c.saw(&push_for(&neg)) {
                failures.push(format!(
                    "{mode} tag {t}: {neg} is untracked in redis 8.6.1 but was pushed. \
                     wire: {:?}",
                    String::from_utf8_lossy(&c.buf)
                ));
            }
        }
        if delivered_positive * 2 <= TXN_TAGS {
            failures.push(format!(
                "{mode}: the tracked key ({{t}}:{tracked}) was pushed for only \
                 {delivered_positive}/{TXN_TAGS} tags (redis 8.6.1 pushes every one)"
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "--shards {shards}:\n{}",
        failures.join("\n")
    );
}

#[test]
fn caching_mid_transaction_1_shard() {
    caching_mid_transaction("1");
}

#[test]
fn caching_mid_transaction_4_shards() {
    caching_mid_transaction("4");
}

/// `MULTI / CLIENT TRACKING on / GET k / EXEC` tracks `k` (redis 8.6.1). On
/// the routed EXEC path moon bookkept the body BEFORE the queued TRACKING on
/// had run, so nothing was tracked.
fn tracking_on_inside_transaction(shards: &str) {
    let m = spawn_moon(shards);
    let mut w = Resp::connect(m.port);
    let mut got = 0;
    let mut wire = String::new();
    for t in 0..TXN_TAGS {
        let mut c = Resp::connect(m.port);
        c.cmd(&["HELLO", "3"]);
        let k = format!("{{oni{t}}}:k");
        w.cmd(&["SET", &k, "1"]);
        c.cmd(&["MULTI"]);
        c.cmd(&["CLIENT", "TRACKING", "on"]);
        c.cmd(&["GET", &k]);
        assert_eq!(
            c.reply(&["EXEC"]),
            b"*2\r\n+OK\r\n$1\r\n1\r\n".to_vec(),
            "--shards {shards} tag {t}"
        );
        w.cmd(&["SET", &k, "2"]);
        let one = delivered(
            &mut c,
            std::slice::from_ref(&k),
            push_for,
            Duration::from_secs(3),
        );
        if one == 0 {
            wire.push_str(&format!("tag {t}: {:?}\n", String::from_utf8_lossy(&c.buf)));
        }
        got += one;
    }
    assert!(
        got * 2 > TXN_TAGS,
        "--shards {shards}: only {got}/{TXN_TAGS} transactions that enabled tracking \
         had their read tracked (redis 8.6.1: every one)\n{wire}"
    );
}

#[test]
fn tracking_on_inside_transaction_1_shard() {
    tracking_on_inside_transaction("1");
}

#[test]
fn tracking_on_inside_transaction_4_shards() {
    tracking_on_inside_transaction("4");
}

/// A REDIRECT target gets its invalidation framed for the protocol it speaks
/// NOW, not the one it spoke when it first subscribed (redis 8.6.1):
///
/// | target                                              | redis           |
/// |-----------------------------------------------------|-----------------|
/// | `SUBSCRIBE x / UNSUBSCRIBE / HELLO 3 / SUBSCRIBE inv` | `>2 invalidate` |
/// | `HELLO 3 / SUBSCRIBE x / RESET / SUBSCRIBE inv`       | `*3 message`    |
/// | `HELLO 3 / SUBSCRIBE inv / HELLO 2`                   | `*3 message`    |
///
/// Moon fixed the framing at the first SUBSCRIBE, so the RESP3 target got a
/// RESP2 message and the RESP2 targets got a push, which a RESP2 client
/// cannot parse.
fn redirect_target_protocol_changes(shards: &str) {
    let m = spawn_moon(shards);
    type Setup = &'static [&'static [&'static str]];
    type Framing = fn(&str) -> Vec<u8>;
    // (case, target setup, the framing redis sends, the wrong framing)
    let cases: [(&str, Setup, Framing, Framing); 3] = [
        (
            "resubscribed after HELLO 3",
            &[
                &["SUBSCRIBE", "x"],
                &["UNSUBSCRIBE"],
                &["HELLO", "3"],
                &["SUBSCRIBE", "__redis__:invalidate"],
            ],
            push_for,
            message_for,
        ),
        (
            "resubscribed after RESET",
            &[
                &["HELLO", "3"],
                &["SUBSCRIBE", "x"],
                &["RESET"],
                &["SUBSCRIBE", "__redis__:invalidate"],
            ],
            message_for,
            push_for,
        ),
        (
            "HELLO 2 while subscribed",
            &[
                &["HELLO", "3"],
                &["SUBSCRIBE", "__redis__:invalidate"],
                &["HELLO", "2"],
            ],
            message_for,
            push_for,
        ),
    ];
    let mut failures = Vec::new();
    for (n, (name, setup, want, wrong)) in cases.iter().enumerate() {
        let mut target = Resp::connect(m.port);
        let tid = client_id(&mut target);
        for step in setup.iter() {
            target.cmd(step);
        }
        target.clear();
        let mut source = Resp::connect(m.port);
        assert_eq!(
            source.reply(&["CLIENT", "TRACKING", "on", "REDIRECT", &tid]),
            b"+OK\r\n".to_vec()
        );
        let mut writer = Resp::connect(m.port);
        let ks = keys(&format!("rtp{n}:"));
        for k in &ks {
            writer.cmd(&["SET", k, "v"]);
            source.cmd(&["GET", k]);
        }
        for k in &ks {
            writer.cmd(&["SET", k, "v2"]);
        }
        let got = delivered(&mut target, &ks, *want, Duration::from_secs(4));
        let misframed = ks.iter().filter(|k| target.saw(&wrong(k))).count();
        if got * 2 <= N || misframed > 0 {
            failures.push(format!(
                "{name}: {got}/{N} framed for the target's protocol, {misframed} framed for \
                 the wrong one. wire: {:?}",
                String::from_utf8_lossy(&target.buf)
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "--shards {shards}:\n{}",
        failures.join("\n")
    );
}

#[test]
fn redirect_target_protocol_changes_1_shard() {
    redirect_target_protocol_changes("1");
}

#[test]
fn redirect_target_protocol_changes_4_shards() {
    redirect_target_protocol_changes("4");
}

/// A RESP2 target that unsubscribed from everything gets nothing (redis
/// 8.6.1 drops the invalidation). Moon queued it on the target's pub/sub
/// channel and wrote it right after the target's next SUBSCRIBE reply.
fn unsubscribed_redirect_target(shards: &str) {
    let m = spawn_moon(shards);
    let (mut target, mut source) = resp2_redirect_pair(m.port, &[]);
    let mut writer = Resp::connect(m.port);
    let old = keys("uns-old:");
    for k in &old {
        writer.cmd(&["SET", k, "v"]);
        source.cmd(&["GET", k]);
    }
    assert_eq!(
        target.reply(&["UNSUBSCRIBE"]),
        b"*3\r\n$11\r\nunsubscribe\r\n$20\r\n__redis__:invalidate\r\n:0\r\n".to_vec()
    );
    for k in &old {
        writer.cmd(&["SET", k, "v2"]);
    }
    // Give every one of those invalidations time to be routed (or dropped).
    target.pump(Duration::from_millis(500));
    target.cmd(&["SUBSCRIBE", "__redis__:invalidate"]);
    // Control: delivery works again once the target is subscribed.
    let new = keys("uns-new:");
    for k in &new {
        writer.cmd(&["SET", k, "v"]);
        source.cmd(&["GET", k]);
    }
    for k in &new {
        writer.cmd(&["SET", k, "v2"]);
    }
    let got = delivered(&mut target, &new, message_for, Duration::from_secs(4));
    assert_majority("resubscribed REDIRECT target", shards, got, N, &target);
    target.pump(Duration::from_millis(300));
    let leaked: Vec<&String> = old.iter().filter(|k| target.saw(&message_for(k))).collect();
    assert!(
        leaked.is_empty(),
        "--shards {shards}: invalidations raised while the target was unsubscribed were \
         delivered after it resubscribed: {leaked:?}. wire: {:?}",
        String::from_utf8_lossy(&target.buf)
    );
}

#[test]
fn unsubscribed_redirect_target_1_shard() {
    unsubscribed_redirect_target("1");
}

#[test]
fn unsubscribed_redirect_target_4_shards() {
    unsubscribed_redirect_target("4");
}

// ═══════════════════════ TRACKINGINFO / GETREDIR parity ════════════════════

type InfoCase<'a> = (&'a [&'a [&'a str]], &'a [&'a str], &'a str, &'a [&'a str]);

fn resp2_info(flags: &[&str], redirect: &str, prefixes: &[&str]) -> String {
    let mut s = format!("*6\r\n$5\r\nflags\r\n*{}\r\n", flags.len());
    for f in flags {
        s.push_str(&format!("${}\r\n{f}\r\n", f.len()));
    }
    s.push_str(&format!(
        "$8\r\nredirect\r\n:{redirect}\r\n$8\r\nprefixes\r\n*{}\r\n",
        prefixes.len()
    ));
    for p in prefixes {
        s.push_str(&format!("${}\r\n{p}\r\n", p.len()));
    }
    s
}

#[test]
fn trackinginfo_and_getredir_match_redis() {
    let m = spawn_moon("1");
    let cases: &[InfoCase<'_>] = &[
        (&[], &["off"], "-1", &[]),
        (&[&["CLIENT", "TRACKING", "on"]], &["on"], "0", &[]),
        (
            &[&["CLIENT", "TRACKING", "on", "OPTIN"]],
            &["on", "optin"],
            "0",
            &[],
        ),
        (
            &[
                &["CLIENT", "TRACKING", "on", "OPTIN"],
                &["CLIENT", "CACHING", "yes"],
            ],
            &["on", "optin", "caching-yes"],
            "0",
            &[],
        ),
        (
            &[&["CLIENT", "TRACKING", "on", "OPTOUT"]],
            &["on", "optout"],
            "0",
            &[],
        ),
        (
            &[
                &["CLIENT", "TRACKING", "on", "OPTOUT"],
                &["CLIENT", "CACHING", "no"],
            ],
            &["on", "optout", "caching-no"],
            "0",
            &[],
        ),
        (
            &[&["CLIENT", "TRACKING", "on", "NOLOOP"]],
            &["on", "noloop"],
            "0",
            &[],
        ),
        (
            &[&["CLIENT", "TRACKING", "on", "BCAST"]],
            &["on", "bcast"],
            "0",
            &[""],
        ),
        (
            &[
                &[
                    "CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "zz", "PREFIX", "aa",
                ],
                &["CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "mm"],
                // Refused: 'aab' overlaps the existing 'aa'. Nothing added.
                &["CLIENT", "TRACKING", "on", "BCAST", "PREFIX", "aab"],
            ],
            &["on", "bcast"],
            "0",
            &["aa", "mm", "zz"],
        ),
        (
            &[
                &["CLIENT", "TRACKING", "on", "OPTIN"],
                &["CLIENT", "CACHING", "yes"],
                &["CLIENT", "TRACKING", "on", "OPTIN"],
            ],
            &["on", "optin", "caching-yes"],
            "0",
            &[],
        ),
        (
            &[
                &["CLIENT", "TRACKING", "on", "OPTIN"],
                &["CLIENT", "CACHING", "yes"],
                &["CLIENT", "TRACKING", "off"],
            ],
            &["off"],
            "-1",
            &[],
        ),
    ];
    for (setup, flags, redirect, prefixes) in cases {
        let mut c = Resp::connect(m.port);
        for s in *setup {
            c.cmd(s);
        }
        assert_eq!(
            String::from_utf8_lossy(&c.reply(&["CLIENT", "TRACKINGINFO"])),
            resp2_info(flags, redirect, prefixes),
            "TRACKINGINFO after {setup:?}"
        );
        assert_eq!(
            String::from_utf8_lossy(&c.reply(&["CLIENT", "GETREDIR"])),
            format!(":{redirect}\r\n"),
            "GETREDIR after {setup:?}"
        );
    }

    // Redirected, then re-enabled without REDIRECT: redis resets the target to
    // 0 and drops NOLOOP.
    let mut target = Resp::connect(m.port);
    let tid = client_id(&mut target);
    let mut c = Resp::connect(m.port);
    c.cmd(&["CLIENT", "TRACKING", "on", "NOLOOP", "REDIRECT", &tid]);
    assert_eq!(
        String::from_utf8_lossy(&c.reply(&["CLIENT", "TRACKINGINFO"])),
        resp2_info(&["on", "noloop"], &tid, &[])
    );
    assert_eq!(
        String::from_utf8_lossy(&c.reply(&["CLIENT", "GETREDIR"])),
        format!(":{tid}\r\n")
    );
    c.cmd(&["CLIENT", "TRACKING", "on"]);
    assert_eq!(
        String::from_utf8_lossy(&c.reply(&["CLIENT", "TRACKINGINFO"])),
        resp2_info(&["on"], "0", &[])
    );

    // RESP3: a map whose flags are a set.
    let mut c = Resp::connect(m.port);
    c.cmd(&["HELLO", "3"]);
    c.cmd(&["CLIENT", "TRACKING", "on", "OPTIN"]);
    c.cmd(&["CLIENT", "CACHING", "yes"]);
    assert_eq!(
        String::from_utf8_lossy(&c.reply(&["CLIENT", "TRACKINGINFO"])),
        "%3\r\n$5\r\nflags\r\n~3\r\n$2\r\non\r\n$5\r\noptin\r\n$11\r\ncaching-yes\r\n$8\r\nredirect\r\n:0\r\n$8\r\nprefixes\r\n*0\r\n"
    );

    let mut c = Resp::connect(m.port);
    assert_eq!(
        c.reply(&["CLIENT", "TRACKINGINFO", "x"]),
        b"-ERR wrong number of arguments for 'client|trackinginfo' command\r\n".to_vec()
    );
    assert_eq!(
        c.reply(&["CLIENT", "GETREDIR", "x"]),
        b"-ERR wrong number of arguments for 'client|getredir' command\r\n".to_vec()
    );
}

/// The new subcommands queue inside MULTI like any other CLIENT subcommand and
/// answer from EXEC (redis 8.6.1).
#[test]
fn caching_and_trackinginfo_queue_inside_multi() {
    let m = spawn_moon("1");
    let mut c = Resp::connect(m.port);
    c.cmd(&["MULTI"]);
    assert_eq!(
        c.reply(&["CLIENT", "CACHING", "yes"]),
        b"+QUEUED\r\n".to_vec()
    );
    assert_eq!(
        c.reply(&["CLIENT", "TRACKINGINFO"]),
        b"+QUEUED\r\n".to_vec()
    );
    assert_eq!(
        String::from_utf8_lossy(&c.reply(&["EXEC"])),
        "*2\r\n-ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled\r\n*6\r\n$5\r\nflags\r\n*1\r\n$3\r\noff\r\n$8\r\nredirect\r\n:-1\r\n$8\r\nprefixes\r\n*0\r\n"
    );
}
