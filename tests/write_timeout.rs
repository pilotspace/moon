//! c10k hardening C1 — a reply write must not park forever.
//!
//! `write_all` on a socket whose peer has stopped reading never returns. A
//! client that pipelines a large response and then simply stops reading leaves
//! the handler blocked inside that write, holding the ENTIRE serialized reply
//! — the monoio handler coalesces a whole batch into one `Bytes` before the
//! single write syscall, so that is hundreds of MB for a deep pipeline — plus
//! its `maxclients` slot, for as long as the attacker cares to wait. N such
//! clients is an OOM, and it costs the attacker nothing: no reads, no CPU,
//! just a TCP window it refuses to open.
//!
//! It is also invisible while it happens: `obl`/`oll`/`omem` in CLIENT LIST
//! are hardcoded 0, so the held output does not show up anywhere.
//!
//! The fix bounds the write. When it makes no progress for
//! `--client-write-timeout-ms`, the connection is closed and the reply is
//! dropped. Verified from a THIRD party (an admin connection running CLIENT
//! LIST) rather than from the victim socket, because a victim that never reads
//! cannot observe its own teardown promptly.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("MOON_BIN") {
        return Some(std::path::PathBuf::from(p));
    }
    let cargo_bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    if cargo_bin.exists() {
        return Some(cargo_bin);
    }
    None
}

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

/// `write_timeout_ms` is passed through verbatim so a test can also assert the
/// *disabled* (0) behaviour without a second spawn helper.
fn spawn_moon(tag: &str, shards: &str, write_timeout_ms: &str, extra: &[&str]) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir = std::env::temp_dir().join(format!(
        "moon-write-timeout-{}-{tag}-{shards}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let (child, port) = common::spawn_listening(|port| {
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
                "--maxmemory",
                "536870912",
                "--client-write-timeout-ms",
                write_timeout_ms,
                "--dir",
                tmp_dir.to_str().expect("utf8 tmp dir"),
            ])
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
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
                    return Some(moon);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not become ready on port {port}");
    None
}

fn encode(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
    }
    out
}

struct Resp {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Resp {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn cmd(&mut self, args: &[&str]) -> String {
        self.buf.clear();
        self.stream.write_all(&encode(args)).expect("write");
        let deadline = Instant::now() + Duration::from_millis(500);
        let mut chunk = [0u8; 8192];
        while Instant::now() < deadline {
            match self.stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    self.buf.extend_from_slice(&chunk[..n]);
                    if self.buf.ends_with(b"\r\n") {
                        break;
                    }
                }
                Err(_) => {}
            }
        }
        String::from_utf8_lossy(&self.buf).into_owned()
    }
}

/// Is `id=<n> ` still present in CLIENT LIST?
fn client_present(admin: &mut Resp, id: &str) -> bool {
    admin
        .cmd(&["CLIENT", "LIST"])
        .contains(&format!("id={id} "))
}

/// Pull one `key=value` field out of a client's CLIENT LIST line.
fn client_field(admin: &mut Resp, id: &str, field: &str) -> Option<u64> {
    let list = admin.cmd(&["CLIENT", "LIST"]);
    let line = list.lines().find(|l| l.starts_with(&format!("id={id} ")))?;
    line.split_whitespace()
        .find_map(|kv| kv.strip_prefix(&format!("{field}=")))
        .and_then(|v| v.parse().ok())
}

/// Build a victim that has pipelined a reply far larger than any socket buffer
/// and then stopped reading. Returns its client id.
fn stall_a_victim(port: u16, value_key: &str) -> (TcpStream, String) {
    let mut victim = Resp::connect(port);
    let idreply = victim.cmd(&["CLIENT", "ID"]);
    let id = idreply.trim().trim_start_matches(':').trim().to_string();
    assert!(
        !id.is_empty() && id.chars().all(|c| c.is_ascii_digit()),
        "CLIENT ID must return an integer, got {idreply:?}"
    );

    // ~25 MB of replies: 100 GETs of a 256 KiB value. Comfortably past any
    // kernel socket buffer, so the server's write blocks partway through.
    let mut pipeline = Vec::new();
    for _ in 0..100 {
        pipeline.extend_from_slice(&encode(&["GET", value_key]));
    }
    victim.stream.write_all(&pipeline).expect("pipeline write");
    victim.stream.flush().expect("flush");

    // ...and now the victim never reads again. Dropping the read timeout is
    // not enough — we must not touch the socket at all.
    (victim.stream, id)
}

fn run_stalled_write_is_abandoned(shards: &str) {
    // 1.5s: long enough that no healthy client trips it, short enough to test.
    let Some(moon) = spawn_moon("stall", shards, "1500", &[]) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    let mut admin = Resp::connect(moon.port);
    let big = "x".repeat(256 * 1024);
    let r = admin.cmd(&["SET", "big", &big]);
    assert!(r.contains("+OK"), "{tag} SET big failed: {r:?}");

    let (_victim_sock, id) = stall_a_victim(moon.port, "big");

    assert!(
        client_present(&mut admin, &id),
        "{tag} victim {id} should be registered before the timeout elapses"
    );

    // Give the write timeout time to fire, plus slack for the shard to notice.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut gone = false;
    while Instant::now() < deadline {
        if !client_present(&mut admin, &id) {
            gone = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(
        gone,
        "{tag} a client that stops reading mid-reply must be disconnected by \
         --client-write-timeout-ms; victim {id} was still in CLIENT LIST after 15s"
    );

    // The server must be perfectly healthy afterwards — we closed one
    // connection, not wedged the shard.
    let r = admin.cmd(&["PING"]);
    assert!(
        r.contains("+PONG"),
        "{tag} server must stay healthy after abandoning a stalled write: {r:?}"
    );
}

/// The escape hatch has to actually work: `0` means "wait forever", the
/// pre-C1 behaviour, for operators who would rather stall than drop a reply.
fn run_zero_disables(shards: &str) {
    let Some(moon) = spawn_moon("zero", shards, "0", &[]) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    let mut admin = Resp::connect(moon.port);
    let big = "x".repeat(256 * 1024);
    let r = admin.cmd(&["SET", "big", &big]);
    assert!(r.contains("+OK"), "{tag} SET big failed: {r:?}");

    let (_victim_sock, id) = stall_a_victim(moon.port, "big");

    // Well past the 1500ms the other test uses; with 0 it must NOT be closed.
    std::thread::sleep(Duration::from_secs(5));
    assert!(
        client_present(&mut admin, &id),
        "{tag} --client-write-timeout-ms 0 must preserve the wait-forever behaviour"
    );

    // ...and while it sits there, the memory it is pinning must be VISIBLE.
    // These three were hardcoded to 0, which is why the attack left no trace:
    // an operator watching CLIENT LIST during an output-buffer OOM saw every
    // client reporting zero held bytes. This is the wait-forever server, so
    // the stall is stable and there is no race to lose.
    let omem = client_field(&mut admin, &id, "omem")
        .unwrap_or_else(|| panic!("{tag} victim {id} must have an omem field"));
    assert!(
        omem > 1_000_000,
        "{tag} a client holding ~25MB of unread reply must report it in omem, got omem={omem}"
    );
    let obl = client_field(&mut admin, &id, "obl").expect("obl field");
    assert_eq!(obl, omem, "{tag} obl and omem describe the same buffer");

    // A healthy client that reads its replies must NOT look like a stalled
    // one — otherwise the counter is noise rather than a signal.
    let admin_id = admin.cmd(&["CLIENT", "ID"]);
    let admin_id = admin_id.trim().trim_start_matches(':').trim().to_string();
    let admin_omem = client_field(&mut admin, &admin_id, "omem").expect("admin omem");
    assert_eq!(
        admin_omem, 0,
        "{tag} a client with no reply in flight must report omem=0"
    );
}

#[test]
fn stalled_write_is_abandoned_single_shard() {
    run_stalled_write_is_abandoned("1");
}

#[test]
fn stalled_write_is_abandoned_multi_shard() {
    run_stalled_write_is_abandoned("4");
}

#[test]
fn write_timeout_zero_disables_single_shard() {
    run_zero_disables("1");
}

/// A reply larger than `--client-output-buffer-limit-normal` must be refused
/// outright rather than buffered.
///
/// This is the size half of C1, and it deliberately diverges from Redis, whose
/// normal class defaults to UNLIMITED — which is precisely why an unread
/// socket can OOM it. moon ships a real 256 MiB ceiling; the test drives a
/// 1 MiB one so it can prove the mechanism without allocating a quarter of a
/// gigabyte.
///
/// Note the consequence, which is real and intended: the cap applies to the
/// whole serialized reply, so a SINGLE value larger than the cap is
/// undeliverable, not merely a long pipeline.
fn run_output_cap(shards: &str) {
    let Some(moon) = spawn_moon(
        "outcap",
        shards,
        "0",
        &["--client-output-buffer-limit-normal", "1048576"],
    ) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    let mut c = Resp::connect(moon.port);

    // Comfortably under the 1 MiB cap: must work, so we know the cap is not
    // simply breaking everything.
    let small = "y".repeat(64 * 1024);
    assert!(
        c.cmd(&["SET", "small", &small]).contains("+OK"),
        "{tag} SET small failed"
    );
    let r = c.cmd(&["GET", "small"]);
    assert!(
        r.starts_with(&format!("${}", small.len())),
        "{tag} a reply under the cap must be delivered, got {:?}",
        &r[..r.len().min(64)]
    );

    // Now ask for more than the cap in one reply. 8 x 256 KiB = 2 MiB.
    let big = "z".repeat(256 * 1024);
    assert!(
        c.cmd(&["SET", "big", &big]).contains("+OK"),
        "{tag} SET big failed"
    );
    let mut pipeline = Vec::new();
    for _ in 0..8 {
        pipeline.extend_from_slice(&encode(&["GET", "big"]));
    }
    c.stream.write_all(&pipeline).expect("pipeline write");

    // The server must close rather than buffer 2 MiB. Poke until it stops
    // answering — same reasoning as the ACL revocation suite: on Windows the
    // teardown is observed on the next command, not as a spontaneous EOF.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut closed = false;
    let mut chunk = [0u8; 8192];
    while Instant::now() < deadline {
        match c.stream.read(&mut chunk) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(_) => {}
            Err(ref e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                if c.stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_err() {
                    closed = true;
                    break;
                }
            }
            Err(_) => {
                closed = true;
                break;
            }
        }
    }
    assert!(
        closed,
        "{tag} a reply exceeding --client-output-buffer-limit-normal must close \
         the connection instead of being buffered"
    );
}

#[test]
fn output_buffer_limit_refuses_oversized_reply_single_shard() {
    run_output_cap("1");
}

#[test]
fn output_buffer_limit_refuses_oversized_reply_multi_shard() {
    run_output_cap("4");
}
