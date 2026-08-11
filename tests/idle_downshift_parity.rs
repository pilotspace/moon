//! c10k W11: the idle-connection buffer downshift must be invisible on the
//! wire. A connection idle past the ~1s downshift threshold sheds its 8 KiB
//! working set and re-parks on a 512 B probe buffer — every byte sent
//! afterwards (single commands, >512 B pipelines, multi-KB values) must
//! still be served exactly as before, and an interleaved active connection
//! must never be disturbed by its idle sibling's downshift.
//!
//! (Runtime note: the downshift is monoio-only; under the tokio runtime the
//! sweep never fires and this suite degenerates to a plain parity check —
//! green either way.)

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::Duration;

fn spawn_moon(dir: &std::path::Path, port: u16) -> std::process::Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir.to_str().unwrap(),
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon")
}

fn read_exact_deadline(stream: &mut TcpStream, want: usize) -> Vec<u8> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    let mut out = Vec::with_capacity(want);
    let mut chunk = [0u8; 4096];
    while out.len() < want {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&chunk[..n]),
            Err(e) => panic!("read failed after {} of {} bytes: {}", out.len(), want, e),
        }
    }
    out
}

fn ping(stream: &mut TcpStream) {
    stream.write_all(b"PING\r\n").expect("write PING");
    let r = read_exact_deadline(stream, 7);
    assert_eq!(&r, b"+PONG\r\n");
}

/// Idle past the downshift threshold, then drive traffic shaped to cross
/// every downshift boundary: a probe-sized wake, a >512 B pipeline (forces
/// the probe read to spill into a full-size follow-up), and a multi-KB
/// value (exercises read-buffer regrowth from released capacity).
#[test]
fn idle_connection_serves_all_traffic_after_downshift() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    conn.set_nodelay(true).ok();
    ping(&mut conn);

    // Cross the downshift threshold (1s) with margin for sweep cadence.
    std::thread::sleep(Duration::from_millis(2600));

    // 1. Small wake: fits the 512 B probe buffer.
    ping(&mut conn);

    // 2. Idle again, then a 100-deep PING pipeline (700 B > probe size):
    //    the first probe read must deliver its prefix and the remainder must
    //    arrive via the re-inflated read path with zero loss or reorder.
    std::thread::sleep(Duration::from_millis(2600));
    let pipeline = b"PING\r\n".repeat(100);
    conn.write_all(&pipeline).expect("write pipeline");
    let replies = read_exact_deadline(&mut conn, 7 * 100);
    assert_eq!(replies.len(), 700, "all 100 pipelined replies must arrive");
    assert!(
        replies.chunks(7).all(|c| c == b"+PONG\r\n"),
        "every pipelined reply must be +PONG"
    );

    // 3. Idle again, then a 4 KiB SET + GET round trip: the released
    //    read/write buffers must regrow transparently.
    std::thread::sleep(Duration::from_millis(2600));
    let payload = vec![b'x'; 4096];
    let mut set_cmd = Vec::new();
    set_cmd.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$8\r\nbigvalue\r\n$4096\r\n");
    set_cmd.extend_from_slice(&payload);
    set_cmd.extend_from_slice(b"\r\n");
    conn.write_all(&set_cmd).expect("write SET");
    let r = read_exact_deadline(&mut conn, 5);
    assert_eq!(&r, b"+OK\r\n");
    conn.write_all(b"*2\r\n$3\r\nGET\r\n$8\r\nbigvalue\r\n")
        .expect("write GET");
    let want = format!("$4096\r\n{}\r\n", String::from_utf8(payload).unwrap());
    let r = read_exact_deadline(&mut conn, want.len());
    assert_eq!(
        r,
        want.as_bytes(),
        "4 KiB value must survive the downshifted round trip"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// An active connection must be completely unaffected while an idle sibling
/// on the same shard is downshifted by the sweep.
#[test]
fn active_sibling_is_undisturbed_by_idle_downshift() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    let mut idle = TcpStream::connect(("127.0.0.1", port)).expect("connect idle");
    ping(&mut idle);

    let mut active = TcpStream::connect(("127.0.0.1", port)).expect("connect active");
    active.set_nodelay(true).ok();

    // ~3s of continuous activity spanning multiple sweep firings while the
    // sibling sits idle and gets downshifted.
    let deadline = std::time::Instant::now() + Duration::from_millis(3000);
    let mut rounds = 0u32;
    while std::time::Instant::now() < deadline {
        ping(&mut active);
        rounds += 1;
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(rounds > 100, "active connection must keep full throughput");

    // The downshifted idle sibling still answers.
    ping(&mut idle);

    let _ = child.kill();
    let _ = child.wait();
}
