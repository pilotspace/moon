//! c10k hardening C2 — a connection's query (input) buffer must be bounded,
//! and bounded *tightly* before the client has authenticated.
//!
//! `read_buf` grows to whatever a frame header declares. The only ceiling was
//! the parser's 512 MiB bulk limit, which it *accepts* — so `$536870911`
//! followed by a dribble of bytes pins half a gigabyte per connection. That
//! memory lives outside `used_memory`, so `maxmemory` never sees it and
//! eviction never fires: 20 connections is 10 GB of invisible RSS.
//!
//! And the auth gate runs AFTER parsing, so none of it costs credentials.
//!
//! Runs at `--shards 1` (monoio TopLevel handler) and `--shards 4` (sharded
//! handler). Skips gracefully when the moon binary is missing.

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

fn spawn_moon(tag: &str, shards: &str, extra: &[&str]) -> Option<Moon> {
    let bin = moon_binary()?;
    let tmp_dir =
        std::env::temp_dir().join(format!("moon-qbuf-{}-{tag}-{shards}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let (child, port) = common::spawn_listening(|port| {
        let mut cmd = Command::new(&bin);
        cmd.args([
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
            "268435456",
            "--dir",
            tmp_dir.to_str().expect("utf8 tmp dir"),
        ]);
        cmd.args(extra);
        cmd.stdout(Stdio::null())
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
                // `-NOAUTH` counts as ready: these servers run with
                // `--requirepass`, so an unauthenticated PING is REFUSED, not
                // ponged. Accepting only `+PONG` made `spawn_moon` return
                // None and every requirepass test silently return `ok`
                // without ever connecting — a green that proved nothing, in
                // both directions.
                if let Ok(n) = c.read(&mut buf)
                    && n > 0
                    && (buf.starts_with(b"+PONG") || buf.starts_with(b"-NOAUTH"))
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

/// Announce a huge bulk string, then dribble `filler` bytes of it. Returns
/// what the server said and whether it closed the connection.
///
/// The declared length is never sent in full — that is the whole attack: the
/// server must buffer everything received so far while it waits for the rest.
struct Verdict {
    reply: String,
    closed: bool,
    sent: usize,
}

fn dribble_incomplete_bulk(port: u16, auth: Option<(&str, &str)>, filler: usize) -> Verdict {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_millis(250)))
        .expect("read timeout");
    s.set_write_timeout(Some(Duration::from_secs(5)))
        .expect("write timeout");

    if let Some((user, pass)) = auth {
        let cmd = format!(
            "*3\r\n$4\r\nAUTH\r\n${}\r\n{user}\r\n${}\r\n{pass}\r\n",
            user.len(),
            pass.len()
        );
        s.write_all(cmd.as_bytes()).expect("write AUTH");
        let mut buf = [0u8; 64];
        let n = s.read(&mut buf).expect("AUTH reply");
        assert!(
            buf[..n].starts_with(b"+OK"),
            "AUTH failed: {:?}",
            String::from_utf8_lossy(&buf[..n])
        );
    }

    // `SET k <512MiB-1>` — accepted by the parser's bulk ceiling, so nothing
    // rejects the header itself.
    s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$536870911\r\n")
        .expect("write header");

    // Non-blocking from here on. A blocking write plus a per-chunk read
    // timeout paces the sender so slowly that the server looks like it
    // stopped accepting when it is really just being fed at a trickle — the
    // pre-fix server happily absorbs 200 MiB on one connection (RSS 4 MB ->
    // 208 MB) and a paced harness never gets far enough to notice.
    s.set_nonblocking(true).expect("nonblocking");

    let chunk = vec![b'x'; 64 * 1024];
    let mut sent = 0usize;
    let mut reply = Vec::new();
    let mut closed = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while sent < filler && Instant::now() < deadline {
        match s.write(&chunk) {
            Ok(n) => sent += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(_) => {
                closed = true;
                break;
            }
        }
        let mut buf = [0u8; 512];
        match s.read(&mut buf) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(n) => {
                reply.extend_from_slice(&buf[..n]);
                closed = true; // an error reply here is always followed by close
                break;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => {
                closed = true;
                break;
            }
        }
    }
    Verdict {
        reply: String::from_utf8_lossy(&reply).into_owned(),
        closed,
        sent,
    }
}

/// An UNAUTHENTICATED client must not be able to pin memory. The pre-auth
/// ceiling (64 KiB by default) has to stop it long before the parser's bulk
/// ceiling would.
fn run_preauth_cap(shards: &str) {
    let Some(moon) = spawn_moon("preauth", shards, &["--requirepass", "pw"]) else {
        return; // binary missing — skip
    };
    let tag = format!("[shards={shards}]");

    // 4 MiB is ~64x the pre-auth ceiling and a rounding error next to the
    // 512 MiB the header asks for — if the server tolerates this, it tolerates
    // the whole half-gigabyte.
    let v = dribble_incomplete_bulk(moon.port, None, 4 * 1024 * 1024);
    assert!(
        v.closed,
        "{tag} an unauthenticated client dribbled {} bytes into a 512 MiB bulk \
         header and the server was still holding the connection open",
        v.sent
    );
    assert!(
        v.reply.contains("ERR") || v.reply.is_empty(),
        "{tag} unexpected reply: {:?}",
        v.reply
    );
    // No assertion on `v.sent`. It counts bytes the KERNEL accepted — client
    // sndbuf + in-flight window + server rcvbuf — not bytes the server
    // buffered, and Linux autotunes those into the megabytes. An earlier
    // `v.sent <= 1MiB` bound passed on macOS and failed on Linux at 2.5 MiB
    // while the server's own `read_buf` never exceeded the 64 KiB ceiling: it
    // was measuring the OS, not moon.
    //
    // The differential control below measures the server instead. Same
    // dribble, same everything, except the pre-auth ceiling is lifted — if
    // that connection survives while the capped one died, the ceiling is what
    // did the killing, and no amount of socket buffering can fake it.
    let Some(uncapped) = spawn_moon(
        "preauth-off",
        shards,
        &[
            "--requirepass",
            "pw",
            "--client-query-buffer-limit-preauth",
            "0",
            "--client-query-buffer-limit",
            "0",
        ],
    ) else {
        return;
    };
    let control = dribble_incomplete_bulk(uncapped.port, None, 4 * 1024 * 1024);
    assert!(
        !control.closed,
        "{tag} control server has NO query-buffer ceiling, so the same 4 MiB          dribble must be absorbed — it closed after {} bytes instead, which          means something other than the ceiling is ending these connections          and the test above proves nothing",
        control.sent
    );
}

/// An AUTHENTICATED client gets the full `--client-query-buffer-limit`, and
/// that limit is enforced too.
fn run_authenticated_cap(shards: &str) {
    let Some(moon) = spawn_moon(
        "auth",
        shards,
        &[
            "--requirepass",
            "pw",
            "--client-query-buffer-limit",
            "1048576", // 1 MiB
        ],
    ) else {
        return;
    };
    let tag = format!("[shards={shards}]");

    let v = dribble_incomplete_bulk(moon.port, Some(("default", "pw")), 8 * 1024 * 1024);
    assert!(
        v.closed,
        "{tag} an authenticated client blew past --client-query-buffer-limit \
         (sent {} bytes) and was not closed",
        v.sent
    );
    assert!(
        v.sent <= 4 * 1024 * 1024,
        "{tag} limit was 1 MiB but {} bytes got through",
        v.sent
    );
}

/// The positive control: a legitimately large value under the limit still
/// works end to end. Without this the tests above would pass on a server that
/// simply refused everything.
fn run_large_value_still_works(shards: &str) {
    let Some(moon) = spawn_moon("ok", shards, &[]) else {
        return;
    };
    let tag = format!("[shards={shards}]");

    let mut s = TcpStream::connect(("127.0.0.1", moon.port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");

    // 2 MiB value: far above the 64 KiB pre-auth ceiling, but this server has
    // no requirepass so the connection is authenticated from the start and
    // gets the full 1 GiB default.
    let value = vec![b'v'; 2 * 1024 * 1024];
    let mut out = format!("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n${}\r\n", value.len()).into_bytes();
    out.extend_from_slice(&value);
    out.extend_from_slice(b"\r\n");
    s.write_all(&out).expect("write SET");

    let mut buf = [0u8; 64];
    let n = s.read(&mut buf).expect("SET reply");
    assert!(
        buf[..n].starts_with(b"+OK"),
        "{tag} a 2 MiB SET under the limit must still succeed, got: {:?}",
        String::from_utf8_lossy(&buf[..n])
    );

    s.write_all(b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n")
        .expect("write STRLEN");
    let n = s.read(&mut buf).expect("STRLEN reply");
    assert_eq!(
        String::from_utf8_lossy(&buf[..n]).trim(),
        format!(":{}", value.len()),
        "{tag} the value must have landed intact"
    );
}

#[test]
fn unauthenticated_query_buffer_is_capped_single_shard() {
    run_preauth_cap("1");
}

#[test]
fn unauthenticated_query_buffer_is_capped_multi_shard() {
    run_preauth_cap("4");
}

#[test]
fn authenticated_query_buffer_respects_the_limit_single_shard() {
    run_authenticated_cap("1");
}

#[test]
fn authenticated_query_buffer_respects_the_limit_multi_shard() {
    run_authenticated_cap("4");
}

#[test]
fn large_value_under_the_limit_still_works_single_shard() {
    run_large_value_still_works("1");
}

#[test]
fn large_value_under_the_limit_still_works_multi_shard() {
    run_large_value_still_works("4");
}
