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
    let (mut child, _port) = common::spawn_listening(|p| spawn_moon_tls(dir.path(), p, tls_port));
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

    let _ = child.kill();
    let _ = child.wait();
}
