//! D4 (#438): connection migration must not race the pipelined batch tail.
//!
//! The affinity sampler latches `migration_target` MID-batch (the moment the
//! 16-sample window converges on a dominant remote shard), but migration
//! executes at the END of the batch — and `MigratedConnectionState` carries no
//! `command_queue` / `in_multi` / subscriptions / tracking registration. A
//! pipelined batch shaped `[GET..GET, MULTI, SET]` therefore migrated with the
//! MULTI txn queued: the queue was silently discarded and the follow-up EXEC
//! answered `-ERR EXEC without MULTI`. Same story for a `SUBSCRIBE` tail — the
//! subscription was orphaned on the old shard.
//!
//! The fix re-evaluates `ConnectionState::migration_eligible()` at the
//! batch-end execution point (latch stays armed; migration retries at the
//! first clean batch end, e.g. right after EXEC).
//!
//! Determinism: the MULTI test runs two phases on ONE connection — phase 1
//! samples keys owned by shard 0, phase 2 keys owned by shard 1 (100 GETs:
//! enough for the 64-command re-migration trigger plus the 16-sample window).
//! Whichever shard the connection landed on, at least one phase converges on a
//! REMOTE shard and latches a migration with the MULTI tail pending. On a
//! pre-fix binary that phase's EXEC fails; on non-Linux (can_migrate=false)
//! migration never triggers and the test is trivially green — the real
//! red/green runs on the Linux VM against a monoio release binary.
//!
//! The SUBSCRIBE leg covers the same execute-point gate for subscriptions.
//! Its migration-orphan remoteness is placement-dependent (two fresh
//! connections, one per key set), so that aspect can under-test but never
//! false-fail.
//!
//! Writing these tests ALSO surfaced a pre-existing crash class (#438
//! follow-on, fixed in the same PR): any early-flush command (SUBSCRIBE /
//! blocking / PSYNC) in a batch with pending remote-slotted commands flushed
//! their placeholders and cleared `responses`, and phase 2's drain then
//! indexed an empty vec — shard-thread panic, whole-process abort, remotely
//! triggerable on any `--shards >= 2` deployment. And the parsed batch tail
//! after a SUBSCRIBE/blocking break was silently dropped. Both are covered
//! below.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use moon::shard::dispatch::key_to_shard;

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let cargo_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    if cargo_bin.exists() {
        return Some(cargo_bin);
    }
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for rel in ["target/release/moon", "target/debug/moon"] {
        let p = root.join(rel);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

struct Moon {
    child: Child,
    port: u16,
    _tmp_dir: tempfile::TempDir,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_moon(shards: &str) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir = tempfile::tempdir().expect("tempdir");
    let dir_str = tmp_dir.path().to_str().unwrap().to_string();
    let shards = shards.to_string();
    let (child, port) = common::spawn_listening(|port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                &dir_str,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let moon = Moon {
        child,
        port,
        _tmp_dir: tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if let Ok(mut c) = TcpStream::connect(("127.0.0.1", moon.port)) {
            let _ = c.set_read_timeout(Some(Duration::from_millis(500)));
            if c.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 64];
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && buf.starts_with(b"+PONG")
                {
                    return Some(moon);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not become ready on port {}", moon.port);
    None
}

#[derive(Debug, Clone, PartialEq)]
enum Reply {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<String>),
    Array(Vec<Reply>),
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Client {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(5000)))
            .unwrap();
        Self {
            stream,
            buf: Vec::new(),
            pos: 0,
        }
    }

    fn encode(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        out
    }

    fn send(&mut self, args: &[&str]) {
        let out = Self::encode(args);
        self.stream.write_all(&out).expect("write");
    }

    /// Write many commands in ONE syscall so the server sees them as ONE
    /// pipelined batch (the whole point of these tests: the migration latch
    /// and the batch tail must land in the same handler batch).
    fn send_batch(&mut self, cmds: &[Vec<&str>]) {
        let mut out = Vec::new();
        for c in cmds {
            out.extend_from_slice(&Self::encode(c));
        }
        self.stream.write_all(&out).expect("write batch");
    }

    fn fill(&mut self) {
        let mut chunk = [0u8; 4096];
        match self.stream.read(&mut chunk) {
            Ok(0) => panic!("connection closed by server"),
            Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
            Err(e) => panic!("read error (no reply within timeout): {e}"),
        }
    }

    fn read_line(&mut self) -> String {
        loop {
            if let Some(rel) = self.buf[self.pos..].windows(2).position(|w| w == b"\r\n") {
                let end = self.pos + rel;
                let line = String::from_utf8_lossy(&self.buf[self.pos..end]).to_string();
                self.pos = end + 2;
                return line;
            }
            self.fill();
        }
    }

    fn read_exact_bytes(&mut self, n: usize) -> String {
        while self.buf.len() - self.pos < n + 2 {
            self.fill();
        }
        let s = String::from_utf8_lossy(&self.buf[self.pos..self.pos + n]).to_string();
        self.pos += n + 2;
        s
    }

    fn read_reply(&mut self) -> Reply {
        let line = self.read_line();
        let (tag, rest) = line.split_at(1);
        match tag {
            "+" => Reply::Simple(rest.to_string()),
            "-" => Reply::Error(rest.to_string()),
            ":" => Reply::Int(rest.parse().expect("int reply")),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    Reply::Bulk(None)
                } else {
                    Reply::Bulk(Some(self.read_exact_bytes(n as usize)))
                }
            }
            "*" => {
                let n: i64 = rest.parse().expect("array len");
                let mut items = Vec::new();
                for _ in 0..n.max(0) {
                    items.push(self.read_reply());
                }
                Reply::Array(items)
            }
            other => panic!("unexpected RESP tag {other:?} in line {line:?}"),
        }
    }
}

/// Generate `n` distinct keys owned by `shard` (of `num_shards`).
fn keys_for_shard(shard: usize, n: usize, num_shards: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(n);
    let mut i = 0u64;
    while out.len() < n {
        let k = format!("migk:{i}");
        if key_to_shard(k.as_bytes(), num_shards) == shard {
            out.push(k);
        }
        i += 1;
    }
    out
}

/// One phase of the MULTI leg: pipeline `[GET k0..kN, MULTI, SET tkey tval]`
/// as a single write, verify every reply, then send EXEC + GET and verify the
/// transaction survived (i.e. no migration fired with the txn queued).
fn multi_phase(c: &mut Client, keys: &[String], tkey: &str, tval: &str, phase: &str) {
    let mut cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
    cmds.push(vec!["MULTI"]);
    cmds.push(vec!["SET", tkey, tval]);
    c.send_batch(&cmds);

    for (i, k) in keys.iter().enumerate() {
        assert_eq!(
            c.read_reply(),
            Reply::Bulk(None),
            "{phase}: GET #{i} ({k}) should be nil"
        );
    }
    assert_eq!(
        c.read_reply(),
        Reply::Simple("OK".into()),
        "{phase}: MULTI should be +OK"
    );
    assert_eq!(
        c.read_reply(),
        Reply::Simple("QUEUED".into()),
        "{phase}: SET inside MULTI should be +QUEUED"
    );

    c.send(&["EXEC"]);
    let exec = c.read_reply();
    assert_eq!(
        exec,
        Reply::Array(vec![Reply::Simple("OK".into())]),
        "{phase}: EXEC must run the queued SET — a batch-tail migration \
         discarded the MULTI state (D4 #438)"
    );

    c.send(&["GET", tkey]);
    assert_eq!(
        c.read_reply(),
        Reply::Bulk(Some(tval.to_string())),
        "{phase}: queued SET must be visible after EXEC"
    );
}

/// MULTI leg: a pipelined batch ending `[.., MULTI, SET]` (EXEC arrives in a
/// later batch) must never lose the queued transaction to a migration latched
/// earlier in the same batch.
#[test]
fn multi_batch_tail_survives_migration_convergence() {
    let Some(moon) = spawn_moon("2") else { return };
    let mut c = Client::connect(moon.port);

    // Phase 1: converge the affinity sampler on shard 0 (16-sample window).
    let keys0 = keys_for_shard(0, 80, 2);
    multi_phase(&mut c, &keys0, "migt:{a}:1", "v1", "phase1(shard0)");

    // Phase 2: 100 shard-1 keys — clears the 64-command re-migration trigger
    // and the fresh 16-sample window, converging on shard 1 this time. If
    // phase 1 was local (or migrated us to shard 0), this one is remote.
    let keys1 = keys_for_shard(1, 100, 2);
    multi_phase(&mut c, &keys1, "migt:{b}:2", "v2", "phase2(shard1)");
}

/// #438 crash class: `[GET <remote-key>…, BLPOP]` in one pipelined batch used
/// to flush the remote GETs' Frame::Null placeholders, clear `responses`, and
/// leave phase 2's drain indexing an empty vec — shard-thread panic, WHOLE
/// PROCESS abort. Deterministic: both shard-0 and shard-1 key sets are
/// exercised, so one of them is remote regardless of connection placement.
#[test]
fn blocking_tail_with_pending_remote_does_not_crash() {
    let Some(moon) = spawn_moon("2") else { return };

    for shard in [0usize, 1usize] {
        let mut c = Client::connect(moon.port);
        let keys = keys_for_shard(shard, 10, 2);
        let mut cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
        cmds.push(vec!["BLPOP", "migblkq", "1"]);
        c.send_batch(&cmds);

        for (i, k) in keys.iter().enumerate() {
            assert_eq!(
                c.read_reply(),
                Reply::Bulk(None),
                "shard-{shard} warm-up: GET #{i} ({k}) should be nil"
            );
        }
        // BLPOP times out after 1s → null array reply (server must still be
        // alive to deliver it; pre-fix the shard thread had already panicked).
        match c.read_reply() {
            Reply::Array(items) if items.is_empty() => {}
            Reply::Bulk(None) => {}
            other => panic!("shard-{shard}: expected BLPOP timeout null, got {other:?}"),
        }
        // The server must have survived the batch.
        c.send(&["PING"]);
        assert_eq!(c.read_reply(), Reply::Simple("PONG".into()));
    }
}

/// #438 swallowed tail: `[SUBSCRIBE ch, PING]` in one pipelined write used to
/// drop the PING on the floor (frames.drain(..) discarded the parsed tail on
/// the subscribe break). The carried tail must now reach subscriber mode,
/// which answers PING.
#[test]
fn subscribe_tail_frames_are_not_swallowed() {
    let Some(moon) = spawn_moon("2") else { return };
    let mut c = Client::connect(moon.port);
    c.send_batch(&[vec!["SUBSCRIBE", "migch:t"], vec!["PING"]]);

    match c.read_reply() {
        Reply::Array(items) if items.len() == 3 => {}
        other => panic!("expected subscribe confirmation, got {other:?}"),
    }
    // Subscriber-mode PING answers +PONG (RESP2) or a pong push; accept both
    // shapes — the invariant is that a reply ARRIVES at all.
    match c.read_reply() {
        Reply::Simple(s) if s.eq_ignore_ascii_case("pong") => {}
        Reply::Array(items) if !items.is_empty() => {}
        other => panic!("PING after SUBSCRIBE must be answered, got {other:?}"),
    }
}

/// SUBSCRIBE leg: a subscription made in the tail of a converging batch must
/// keep receiving messages (a batch-tail migration orphaned it pre-fix).
#[test]
fn subscribe_batch_tail_survives_migration_convergence() {
    let Some(moon) = spawn_moon("2") else { return };

    for (shard, channel) in [(0usize, "migch:a"), (1usize, "migch:b")] {
        eprintln!("subscribe leg: warm-up shard {shard}, channel {channel}");
        let mut subscriber = Client::connect(moon.port);
        let keys = keys_for_shard(shard, 80, 2);
        let mut cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["GET", k.as_str()]).collect();
        cmds.push(vec!["SUBSCRIBE", channel]);
        subscriber.send_batch(&cmds);

        for (i, _) in keys.iter().enumerate() {
            let r = subscriber.read_reply();
            assert_eq!(r, Reply::Bulk(None), "shard-{shard} GET #{i}");
        }
        eprintln!("subscribe leg: shard {shard} GET replies ok");
        let confirm = subscriber.read_reply();
        match &confirm {
            Reply::Array(items) if items.len() == 3 => {}
            other => panic!("expected subscribe confirmation array, got {other:?}"),
        }

        let mut publisher = Client::connect(moon.port);
        publisher.send(&["PUBLISH", channel, "hello"]);
        match publisher.read_reply() {
            Reply::Int(_) => {}
            other => panic!("PUBLISH should return an integer, got {other:?}"),
        }

        let msg = subscriber.read_reply();
        assert_eq!(
            msg,
            Reply::Array(vec![
                Reply::Bulk(Some("message".into())),
                Reply::Bulk(Some(channel.to_string())),
                Reply::Bulk(Some("hello".into())),
            ]),
            "subscription (shard-{shard} warm-up) must survive the batch tail \
             — an orphaned subscription means migration fired with subs live \
             (D4 #438)"
        );
    }
}
