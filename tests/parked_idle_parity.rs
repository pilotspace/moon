//! c1M P1: task-exit parking must be invisible on the wire. With
//! `--conn-park-secs 2`, an idle plain-TCP connection downshifts (~1s), then
//! its handler task EXITS (~2s more), leaving only a readiness watcher. Every
//! byte sent afterwards must be served exactly as before on the same
//! connection, across multiple park/wake cycles; the parked connection must
//! stay visible to CLIENT LIST and killable by CLIENT KILL.
//!
//! (Runtime note: parking is monoio-plain-TCP only; under the tokio runtime
//! the suite degenerates to a plain parity check — green either way.)

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::Duration;

/// Past downshift (1s) + park threshold (2s) + sweep cadence (1s) + margin.
const PARK_WAIT: Duration = Duration::from_millis(4600);

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
            "--conn-park-secs",
            "2",
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

/// Read one full RESP reply (bulk string or simple line) — enough for the
/// CLIENT LIST / CLIENT KILL helper commands below.
fn command_reply(stream: &mut TcpStream, cmd: &str) -> String {
    stream.write_all(cmd.as_bytes()).expect("write cmd");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    let mut buf = Vec::new();
    let mut chunk = [0u8; 65536];
    loop {
        let n = stream.read(&mut chunk).expect("read reply");
        assert!(n > 0, "connection closed mid-reply");
        buf.extend_from_slice(&chunk[..n]);
        // Bulk replies announce their length; simple replies end at CRLF.
        if buf.starts_with(b"$") {
            if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                let len: usize = std::str::from_utf8(&buf[1..pos - 1])
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if buf.len() >= pos + 1 + len + 2 {
                    break;
                }
            }
        } else if buf.ends_with(b"\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// Multiple park/wake cycles on one connection: probe wake, >512 B pipeline,
/// 4 KiB value round trip.
#[test]
fn parked_connection_serves_all_traffic_after_wake() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    let mut conn = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    conn.set_nodelay(true).ok();
    ping(&mut conn);

    // Cycle 1: park, then a probe-sized wake.
    std::thread::sleep(PARK_WAIT);
    ping(&mut conn);

    // Cycle 2: park again (the resumed handler must re-arm both stages),
    // then a 100-deep pipeline.
    std::thread::sleep(PARK_WAIT);
    let pipeline = b"PING\r\n".repeat(100);
    conn.write_all(&pipeline).expect("write pipeline");
    let replies = read_exact_deadline(&mut conn, 7 * 100);
    assert_eq!(replies.len(), 700, "all 100 pipelined replies must arrive");
    assert!(
        replies.chunks(7).all(|c| c == b"+PONG\r\n"),
        "every pipelined reply must be +PONG"
    );

    // Cycle 3: park again, then a 4 KiB SET + GET round trip.
    std::thread::sleep(PARK_WAIT);
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
        "4 KiB value must survive the park cycle"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// A parked connection must stay in CLIENT LIST, and CLIENT KILL must close
/// it (the kill's shutdown(2) wakes the readiness watcher; the resumed
/// handler sees EOF).
#[test]
fn parked_connection_visible_and_killable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    // Victim: name itself so the control conn can find its id, then park.
    let mut victim = TcpStream::connect(("127.0.0.1", port)).expect("connect victim");
    let r = command_reply(&mut victim, "CLIENT SETNAME parkvictim\r\n");
    assert!(r.starts_with("+OK"), "SETNAME failed: {r}");
    std::thread::sleep(PARK_WAIT);

    // Control connection stays active.
    let mut control = TcpStream::connect(("127.0.0.1", port)).expect("connect control");
    let list = command_reply(&mut control, "CLIENT LIST\r\n");
    let victim_line = list
        .lines()
        .find(|l| l.contains("name=parkvictim"))
        .unwrap_or_else(|| panic!("parked connection missing from CLIENT LIST:\n{list}"));
    let victim_id: u64 = victim_line
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix("id="))
        .expect("id= field")
        .parse()
        .expect("numeric id");

    // Kill the parked victim by id.
    let r = command_reply(&mut control, &format!("CLIENT KILL ID {victim_id}\r\n"));
    assert!(
        r.starts_with(":1"),
        "CLIENT KILL must report 1 killed conn, got: {r}"
    );

    // The victim's socket must observe the close (EOF or reset) promptly.
    victim
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    let mut buf = [0u8; 16];
    match victim.read(&mut buf) {
        Ok(0) => {}  // EOF — clean close
        Err(_) => {} // RST — also a close
        Ok(n) => panic!("expected close, got {n} bytes"),
    }

    // And it must be gone from CLIENT LIST.
    let list = command_reply(&mut control, "CLIENT LIST\r\n");
    assert!(
        !list.contains("name=parkvictim"),
        "killed parked connection still listed:\n{list}"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// Registry identity must survive a park/wake cycle unbroken: the resumed
/// connection keeps its CLIENT SETNAME, its id, and CLIENT LIST never
/// double-counts (registration handoff — the wake must not deregister and
/// re-register across a task-scheduling boundary).
#[test]
fn resumed_connection_keeps_registry_identity() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    let mut victim = TcpStream::connect(("127.0.0.1", port)).expect("connect victim");
    victim.set_nodelay(true).ok();
    let r = command_reply(&mut victim, "CLIENT SETNAME wakekeeper\r\n");
    assert!(r.starts_with("+OK"), "SETNAME failed: {r}");

    let mut control = TcpStream::connect(("127.0.0.1", port)).expect("connect control");
    let list = command_reply(&mut control, "CLIENT LIST\r\n");
    let orig_id: u64 = list
        .lines()
        .find(|l| l.contains("name=wakekeeper"))
        .expect("named conn listed before park")
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix("id="))
        .expect("id= field")
        .parse()
        .expect("numeric id");

    // Two full park → data-wake cycles.
    for cycle in 0..2 {
        std::thread::sleep(PARK_WAIT);
        ping(&mut victim); // data-wake

        let list = command_reply(&mut control, "CLIENT LIST\r\n");
        let line = list
            .lines()
            .find(|l| l.contains("name=wakekeeper"))
            .unwrap_or_else(|| {
                panic!("cycle {cycle}: resumed conn lost its name in CLIENT LIST:\n{list}")
            });
        let id: u64 = line
            .split_whitespace()
            .find_map(|kv| kv.strip_prefix("id="))
            .expect("id= field")
            .parse()
            .expect("numeric id");
        assert_eq!(id, orig_id, "cycle {cycle}: client id changed across wake");
        let entries = list.lines().filter(|l| l.contains("id=")).count();
        assert_eq!(
            entries, 2,
            "cycle {cycle}: CLIENT LIST must hold exactly victim+control:\n{list}"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}

/// A client that dies with an RST while its connection is parked must be
/// torn down promptly — an RST'd fd stays readable forever, so a handler
/// that mistakes the resulting read error for a sweep-cancel would spin the
/// connection through park→wake→park at 100% CPU (the E11 spin, plain-TCP
/// variant) and pin the fd in CLOSE_WAIT.
#[cfg(unix)] // SO_LINGER(0) via libc; std's set_linger is unstable
#[test]
fn rst_while_parked_tears_down_promptly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    let victim = TcpStream::connect(("127.0.0.1", port)).expect("connect victim");
    {
        let mut v = &victim;
        v.write_all(b"CLIENT SETNAME rstvictim\r\n").expect("write");
        let mut buf = [0u8; 8];
        victim
            .set_read_timeout(Some(Duration::from_secs(10)))
            .expect("timeout");
        let _ = (&victim).read(&mut buf);
    }
    std::thread::sleep(PARK_WAIT); // parked now

    // RST close: SO_LINGER(0) makes drop send RST instead of FIN.
    {
        use std::os::unix::io::AsRawFd;
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        let rc = unsafe {
            libc::setsockopt(
                victim.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                &linger as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0, "SO_LINGER(0) must apply");
    }
    drop(victim);

    // The server must fully release the conn: gone from CLIENT LIST and no
    // respawn loop keeping it alive.
    std::thread::sleep(Duration::from_millis(2000));
    let mut control = TcpStream::connect(("127.0.0.1", port)).expect("connect control");
    let list = command_reply(&mut control, "CLIENT LIST\r\n");
    assert!(
        !list.contains("name=rstvictim"),
        "RST'd parked connection still registered:\n{list}"
    );

    let _ = child.kill();
    let _ = child.wait();
}

/// An active sibling must be undisturbed while its neighbor parks and wakes.
#[test]
fn active_sibling_undisturbed_by_parking() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (mut child, port) = common::spawn_listening(|p| spawn_moon(dir.path(), p));

    let mut idle = TcpStream::connect(("127.0.0.1", port)).expect("connect idle");
    ping(&mut idle);

    let mut active = TcpStream::connect(("127.0.0.1", port)).expect("connect active");
    active.set_nodelay(true).ok();

    // ~5s of continuous activity spanning downshift + park of the sibling.
    let deadline = std::time::Instant::now() + PARK_WAIT + Duration::from_millis(500);
    let mut rounds = 0u32;
    while std::time::Instant::now() < deadline {
        ping(&mut active);
        rounds += 1;
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(rounds > 100, "active connection must keep full throughput");

    // The parked sibling still answers.
    ping(&mut idle);

    let _ = child.kill();
    let _ = child.wait();
}
