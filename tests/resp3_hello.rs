//! RESP3 protocol negotiation against the real moon binary.
//!
//! Regression test for the monoio handler ignoring `HELLO 3`: the connection
//! state was updated but the codec kept serializing RESP2, so the HELLO reply
//! (a RESP3 map) went out as a flat array. redis-py >= 8 negotiates RESP3 by
//! default and crashes on connect (`'list' object has no attribute 'get'`).
//!
//! Spawns the actual binary (`CARGO_BIN_EXE_moon`) and speaks raw RESP over a
//! TCP socket, asserting on wire-level type bytes. Exercises whichever runtime
//! the binary was built with (monoio by default, tokio under
//! `--features runtime-tokio`).

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Kill the server on drop so a failing assertion never leaks a process.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Grab a free port by binding to :0 and dropping the listener.
fn free_port() -> u16 {
    #[allow(clippy::unwrap_used)] // test-only: loopback bind cannot reasonably fail
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    #[allow(clippy::unwrap_used)] // test-only
    listener.local_addr().unwrap().port()
}

/// Connect with retries until the server accepts and answers PING.
fn connect_ready(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            #[allow(clippy::unwrap_used)] // test-only
            stream
                .set_read_timeout(Some(Duration::from_millis(2000)))
                .unwrap();
            if stream.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok() {
                let mut buf = [0u8; 16];
                if let Ok(n) = stream.read(&mut buf) {
                    if buf[..n].starts_with(b"+PONG") {
                        return stream;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "moon server did not become ready on port {port} within 15s"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Send one command and read the full reply (drain until a read times out
/// after at least one byte arrived).
fn roundtrip(stream: &mut TcpStream, cmd: &[u8]) -> Vec<u8> {
    #[allow(clippy::unwrap_used)] // test-only
    stream.write_all(cmd).unwrap();
    let mut reply = Vec::new();
    let mut buf = [0u8; 4096];
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // peer closed
            Ok(n) => {
                reply.extend_from_slice(&buf[..n]);
                // Short settle: another chunk may still be in flight.
                #[allow(clippy::unwrap_used)] // test-only
                stream
                    .set_read_timeout(Some(Duration::from_millis(200)))
                    .unwrap();
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                if !reply.is_empty() {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "no reply to {:?} within 5s",
                    String::from_utf8_lossy(cmd)
                );
            }
            Err(e) => panic!("read error: {e}"),
        }
    }
    #[allow(clippy::unwrap_used)] // test-only
    stream
        .set_read_timeout(Some(Duration::from_millis(2000)))
        .unwrap();
    reply
}

#[test]
fn hello_negotiation_switches_wire_protocol() {
    #[allow(clippy::unwrap_used)] // test-only
    let tmp_dir = tempfile::tempdir().unwrap();
    let port = free_port();
    let port_s = port.to_string();
    #[allow(clippy::unwrap_used)] // test-only
    let dir_s = tmp_dir.path().to_str().unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_moon"))
        .args(["--port", &port_s, "--shards", "2", "--dir", dir_s])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon server");
    let _guard = ChildGuard(child);

    // ── RESP3: HELLO 3 must reply with a map frame ('%') and switch the codec ──
    {
        let mut stream = connect_ready(port);
        let reply = roundtrip(&mut stream, b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n");
        assert!(
            reply.first() == Some(&b'%'),
            "HELLO 3 must reply with a RESP3 map ('%'), got: {:?}",
            String::from_utf8_lossy(&reply[..reply.len().min(80)])
        );
        let reply_str = String::from_utf8_lossy(&reply);
        assert!(
            reply_str.contains("proto") && reply_str.contains(":3\r\n"),
            "HELLO 3 reply must report proto 3, got: {reply_str:?}"
        );

        // The connection must stay functional after the switch.
        let pong = roundtrip(&mut stream, b"*1\r\n$4\r\nPING\r\n");
        assert!(
            pong.starts_with(b"+PONG"),
            "PING after HELLO 3 failed: {:?}",
            String::from_utf8_lossy(&pong)
        );

        // A second HELLO on an upgraded connection must still be a map.
        let reply2 = roundtrip(&mut stream, b"*1\r\n$5\r\nHELLO\r\n");
        assert!(
            reply2.first() == Some(&b'%'),
            "HELLO on a RESP3 connection must reply with a map, got: {:?}",
            String::from_utf8_lossy(&reply2[..reply2.len().min(80)])
        );
    }

    // ── RESP2: HELLO 2 must keep the flat-array downgrade ('*') ──
    {
        let mut stream = connect_ready(port);
        let reply = roundtrip(&mut stream, b"*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n");
        assert!(
            reply.first() == Some(&b'*'),
            "HELLO 2 must reply with a flat RESP2 array ('*'), got: {:?}",
            String::from_utf8_lossy(&reply[..reply.len().min(80)])
        );
    }

    // ── Unsupported version must NOPROTO ──
    {
        let mut stream = connect_ready(port);
        let reply = roundtrip(&mut stream, b"*2\r\n$5\r\nHELLO\r\n$1\r\n4\r\n");
        assert!(
            reply.starts_with(b"-NOPROTO"),
            "HELLO 4 must reply -NOPROTO, got: {:?}",
            String::from_utf8_lossy(&reply[..reply.len().min(80)])
        );
    }
}
