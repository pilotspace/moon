//! c10k P4b: the TLS idle-connection downshift must be invisible on the
//! wire. A TLS connection idle past the ~1s threshold gets its parked read
//! cancelled (vendored monoio-rustls cancelable read), sheds moon's working
//! set AND the wrapper's two 16 KiB stream buffers, and re-parks on a probe
//! buffer — every byte sent afterwards must still be served exactly as
//! before over the SAME TLS session (a cancel that desyncs the session or
//! replays a stale error surfaces here as a broken read).
//!
//! (Runtime note: the downshift is monoio-only; under the tokio runtime the
//! sweep never fires and this suite degenerates to a plain TLS parity
//! check — green either way.)

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

/// Accept any server certificate — the server uses a throwaway self-signed
/// cert generated per test run; transport privacy is irrelevant here.
#[derive(Debug)]
struct AcceptAnyCert(rustls::crypto::CryptoProvider);

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

struct TlsConn {
    session: rustls::ClientConnection,
    sock: TcpStream,
}

impl TlsConn {
    fn connect(port: u16) -> Self {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(provider.clone()))
            .with_safe_default_protocol_versions()
            .expect("protocol versions")
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
            .with_no_client_auth();
        let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let session =
            rustls::ClientConnection::new(Arc::new(config), server_name).expect("client conn");
        let sock = TcpStream::connect(("127.0.0.1", port)).expect("tcp connect");
        sock.set_nodelay(true).ok();
        sock.set_read_timeout(Some(Duration::from_secs(10))).ok();
        Self { session, sock }
    }

    fn write_all(&mut self, data: &[u8]) {
        rustls::Stream::new(&mut self.session, &mut self.sock)
            .write_all(data)
            .expect("tls write");
    }

    fn read_exact_deadline(&mut self, want: usize) -> Vec<u8> {
        let mut tls = rustls::Stream::new(&mut self.session, &mut self.sock);
        let mut out = Vec::with_capacity(want);
        let mut chunk = [0u8; 4096];
        while out.len() < want {
            match tls.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => out.extend_from_slice(&chunk[..n]),
                Err(e) => panic!(
                    "tls read failed after {} of {} bytes: {}",
                    out.len(),
                    want,
                    e
                ),
            }
        }
        out
    }

    fn ping(&mut self) {
        self.write_all(b"PING\r\n");
        let r = self.read_exact_deadline(7);
        assert_eq!(&r, b"+PONG\r\n");
    }
}

/// Generate a throwaway self-signed cert. Returns false (test skips) if the
/// `openssl` CLI is unavailable on this machine.
fn generate_cert(dir: &std::path::Path) -> bool {
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
        ])
        .arg("-keyout")
        .arg(dir.join("key.pem"))
        .arg("-out")
        .arg(dir.join("cert.pem"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    matches!(status, Ok(s) if s.success())
}

fn spawn_moon_tls(dir: &std::path::Path, port: u16, tls_port: u16) -> std::process::Child {
    spawn_moon_tls_park(dir, port, tls_port, 0)
}

fn spawn_moon_tls_park(
    dir: &std::path::Path,
    port: u16,
    tls_port: u16,
    park_secs: u64,
) -> std::process::Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--dir",
            dir.join("data").to_str().unwrap(),
            "--disk-free-min-pct",
            "0",
            "--tls-port",
            &tls_port.to_string(),
            "--conn-park-secs",
            &park_secs.to_string(),
        ])
        .arg("--tls-cert-file")
        .arg(dir.join("cert.pem"))
        .arg("--tls-key-file")
        .arg(dir.join("key.pem"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn moon with TLS")
}

/// Idle a TLS connection across multiple sweep firings, then drive traffic
/// shaped to cross every downshift boundary: a probe-sized wake, a >512 B
/// pipeline, and a multi-KB value (regrows both moon's buffers and the
/// released TLS wrapper buffers). All on one TLS session — any session
/// desync from the cancel path fails loudly.
#[test]
fn tls_idle_connection_serves_all_traffic_after_downshift() {
    let dir = tempfile::tempdir().expect("tempdir");
    if !generate_cert(dir.path()) {
        eprintln!("SKIP: openssl CLI not available for cert generation");
        return;
    }
    let tls_port = common::reserve_port();
    let (mut child, _port) =
        common::spawn_listening_guarded(|p| spawn_moon_tls(dir.path(), p, tls_port));
    // spawn_listening waits on the plain port; give the TLS listener a
    // moment if it comes up second.
    let mut conn = None;
    for _ in 0..50 {
        match std::panic::catch_unwind(|| TlsConn::connect(tls_port)) {
            Ok(c) => {
                conn = Some(c);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut conn = conn.expect("TLS connect");
    conn.ping();

    // Cross the downshift threshold (1s) with margin for sweep cadence.
    std::thread::sleep(Duration::from_millis(2600));

    // 1. Small wake over the downshifted TLS session.
    conn.ping();

    // 2. Idle again, then a 100-deep PING pipeline (700 B plaintext).
    std::thread::sleep(Duration::from_millis(2600));
    let pipeline = b"PING\r\n".repeat(100);
    conn.write_all(&pipeline);
    let replies = conn.read_exact_deadline(7 * 100);
    assert_eq!(replies.len(), 700, "all 100 pipelined replies must arrive");
    assert!(
        replies.chunks(7).all(|c| c == b"+PONG\r\n"),
        "every pipelined reply must be +PONG"
    );

    // 3. Idle again, then a 4 KiB SET + GET round trip.
    std::thread::sleep(Duration::from_millis(2600));
    let payload = vec![b'x'; 4096];
    let mut set_cmd = Vec::new();
    set_cmd.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$8\r\nbigvalue\r\n$4096\r\n");
    set_cmd.extend_from_slice(&payload);
    set_cmd.extend_from_slice(b"\r\n");
    conn.write_all(&set_cmd);
    let r = conn.read_exact_deadline(5);
    assert_eq!(&r, b"+OK\r\n");
    conn.write_all(b"*2\r\n$3\r\nGET\r\n$8\r\nbigvalue\r\n");
    let want = format!("$4096\r\n{}\r\n", String::from_utf8(payload).unwrap());
    let r = conn.read_exact_deadline(want.len());
    assert_eq!(
        r,
        want.as_bytes(),
        "4 KiB value must survive the downshifted TLS round trip"
    );

    child.kill_now();
}

/// c1M P1-TLS: task-exit parking must be invisible on the TLS wire. With
/// `--conn-park-secs 2`, an idle TLS connection downshifts (~1s), then its
/// handler task EXITS (~2s more), leaving a watcher on the raw fd via the
/// vendored `io_ref()` passthrough. Every byte sent afterwards must be
/// served over the SAME TLS session across repeated park/wake cycles —
/// a park while any TLS-stack buffer held data would hang the read here,
/// and a session desync would fail the decrypt loudly. A plain-TCP control
/// connection proves the parked TLS conn stays visible and killable.
#[test]
fn tls_parked_connection_serves_traffic_and_stays_killable() {
    const PARK_WAIT: Duration = Duration::from_millis(4600);

    let dir = tempfile::tempdir().expect("tempdir");
    if !generate_cert(dir.path()) {
        eprintln!("SKIP: openssl CLI not available for cert generation");
        return;
    }
    let tls_port = common::reserve_port();
    let (mut child, plain_port) =
        common::spawn_listening_guarded(|p| spawn_moon_tls_park(dir.path(), p, tls_port, 2));
    let mut conn = None;
    for _ in 0..50 {
        match std::panic::catch_unwind(|| TlsConn::connect(tls_port)) {
            Ok(c) => {
                conn = Some(c);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut conn = conn.expect("TLS connect");
    conn.write_all(b"CLIENT SETNAME tlspark\r\n");
    let r = conn.read_exact_deadline(5);
    assert_eq!(&r, b"+OK\r\n");

    // Cycle 1: park past the task-exit threshold, then a probe wake.
    std::thread::sleep(PARK_WAIT);
    conn.ping();

    // Cycle 2: re-park (the resumed handler must re-arm both stages), then
    // a 100-deep pipeline over the same session.
    std::thread::sleep(PARK_WAIT);
    let pipeline = b"PING\r\n".repeat(100);
    conn.write_all(&pipeline);
    let replies = conn.read_exact_deadline(7 * 100);
    assert_eq!(replies.len(), 700, "all 100 pipelined replies must arrive");

    // Cycle 3: re-park, then verify the parked conn is visible (with its
    // name — registration handoff) and killable from a plain-TCP control.
    std::thread::sleep(PARK_WAIT);
    let mut control = TcpStream::connect(("127.0.0.1", plain_port)).expect("connect control");
    control
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    let mut list = Vec::new();
    control.write_all(b"CLIENT LIST\r\n").expect("write LIST");
    let mut chunk = [0u8; 65536];
    loop {
        let n = control.read(&mut chunk).expect("read LIST");
        assert!(n > 0, "control closed mid-reply");
        list.extend_from_slice(&chunk[..n]);
        if list.ends_with(b"\r\n") && list.starts_with(b"$") {
            let s = String::from_utf8_lossy(&list);
            if let Some(pos) = s.find('\n')
                && let Ok(len) = s[1..pos - 1].trim().parse::<usize>()
                && list.len() >= pos + 1 + len + 2
            {
                break;
            }
        }
    }
    let list = String::from_utf8_lossy(&list);
    let victim_line = list
        .lines()
        .find(|l| l.contains("name=tlspark"))
        .unwrap_or_else(|| panic!("parked TLS conn missing from CLIENT LIST:\n{list}"));
    let victim_id: u64 = victim_line
        .split_whitespace()
        .find_map(|kv| kv.strip_prefix("id="))
        .expect("id= field")
        .parse()
        .expect("numeric id");

    control
        .write_all(format!("CLIENT KILL ID {victim_id}\r\n").as_bytes())
        .expect("write KILL");
    let mut r = [0u8; 16];
    let n = control.read(&mut r).expect("read KILL reply");
    assert!(
        r[..n].starts_with(b":1"),
        "CLIENT KILL must report 1 killed conn, got: {}",
        String::from_utf8_lossy(&r[..n])
    );

    // The parked TLS victim's socket must observe the close promptly.
    conn.sock
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    let mut buf = [0u8; 16];
    match conn.sock.read(&mut buf) {
        Ok(0) => {}  // EOF
        Err(_) => {} // RST
        Ok(n) => panic!("expected close, got {n} raw bytes"),
    }

    child.kill_now();
}

/// A TLS client that vanishes with a raw TCP FIN (no close_notify) while
/// parked must be torn down promptly. rustls surfaces that FIN as a read
/// ERROR (unexpected EOF), not `Ok(0)` — a handler that lumps read errors
/// in with the sweep-cancel re-parks the dead fd, which stays readable
/// forever: the park→wake→park spin found live in E11 (3k CLOSE_WAIT conns,
/// shard at 100% CPU).
#[test]
fn tls_fin_while_parked_tears_down_promptly() {
    const PARK_WAIT: Duration = Duration::from_millis(4600);

    let dir = tempfile::tempdir().expect("tempdir");
    if !generate_cert(dir.path()) {
        eprintln!("SKIP: openssl CLI not available for cert generation");
        return;
    }
    let tls_port = common::reserve_port();
    let (mut child, plain_port) =
        common::spawn_listening_guarded(|p| spawn_moon_tls_park(dir.path(), p, tls_port, 2));
    let mut conn = None;
    for _ in 0..50 {
        match std::panic::catch_unwind(|| TlsConn::connect(tls_port)) {
            Ok(c) => {
                conn = Some(c);
                break;
            }
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
    let mut conn = conn.expect("TLS connect");
    conn.write_all(b"CLIENT SETNAME finvictim\r\n");
    let r = conn.read_exact_deadline(5);
    assert_eq!(&r, b"+OK\r\n");

    std::thread::sleep(PARK_WAIT); // parked now

    // Raw TCP close WITHOUT a TLS close_notify: drop the socket only.
    drop(conn);

    std::thread::sleep(Duration::from_millis(2000));
    let mut control = TcpStream::connect(("127.0.0.1", plain_port)).expect("connect control");
    control
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set timeout");
    control.write_all(b"CLIENT LIST\r\n").expect("write LIST");
    let mut list = Vec::new();
    let mut chunk = [0u8; 65536];
    loop {
        let n = control.read(&mut chunk).expect("read LIST");
        assert!(n > 0, "control closed mid-reply");
        list.extend_from_slice(&chunk[..n]);
        if list.ends_with(b"\r\n") && list.starts_with(b"$") {
            let s = String::from_utf8_lossy(&list);
            if let Some(pos) = s.find('\n')
                && let Ok(len) = s[1..pos - 1].trim().parse::<usize>()
                && list.len() >= pos + 1 + len + 2
            {
                break;
            }
        }
    }
    let list = String::from_utf8_lossy(&list);
    assert!(
        !list.contains("name=finvictim"),
        "FIN'd parked TLS connection still registered:\n{list}"
    );

    child.kill_now();
}
