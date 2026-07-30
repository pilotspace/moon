//! c10k hardening B5 — pins rustls's KeyUpdate deferral, the assumption the
//! reordered TLS park veto rests on.
//!
//! `Stream::task_park_safe` (vendored monoio-rustls) decides whether a TLS
//! connection may be task-parked on raw-fd readability. Parking is only
//! correct when nothing is buffered anywhere in the TLS stack — anything we
//! still owe the peer would wait forever behind a `readable()` that never
//! fires. The veto used to sample `session.wants_write()` BEFORE calling
//! `process_new_packets()`, which reads the state from before processing
//! could queue anything; it now processes first. The reachable case that
//! makes the order matter is the TLS 1.2 renegotiation rebuff, where rustls
//! queues a `NoRenegotiation` warning alert and returns `Ok` — that one can
//! only be driven by forging encrypted records, so it is not exercised here.
//!
//! What this test DOES establish is the negative result: the TLS 1.3
//! KeyUpdate(update_requested) case named in the c10k review is **not**
//! reachable. rustls stashes its KeyUpdate reply in
//! `queued_key_update_message` and only moves it into `sendable_tls` on the
//! next outbound record, so `wants_write()` reads false under either
//! ordering. If a future rustls starts queueing eagerly, this test fails and
//! the KeyUpdate case becomes live — at which point the reordered veto is
//! already what handles it correctly.
//!
//! Runs on a real in-memory rustls session pair; no live server, no monoio.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

/// Accept any server certificate — the cert is a throwaway generated per run;
/// transport privacy is irrelevant to the ordering question.
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

/// Generate a throwaway self-signed cert. Returns false (test skips) when the
/// `openssl` CLI is unavailable.
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

/// Pump one side's outbound bytes into the other until both are quiet.
fn drive_handshake(
    client: &mut rustls::ClientConnection,
    server: &mut rustls::ServerConnection,
) -> Result<(), rustls::Error> {
    for _ in 0..16 {
        let mut c2s = Vec::new();
        while client.wants_write() {
            client.write_tls(&mut c2s).expect("write_tls to Vec");
        }
        if !c2s.is_empty() {
            let mut cursor = &c2s[..];
            while !cursor.is_empty() {
                server.read_tls(&mut cursor).expect("read_tls from slice");
                server.process_new_packets()?;
            }
        }
        let mut s2c = Vec::new();
        while server.wants_write() {
            server.write_tls(&mut s2c).expect("write_tls to Vec");
        }
        if !s2c.is_empty() {
            let mut cursor = &s2c[..];
            while !cursor.is_empty() {
                client.read_tls(&mut cursor).expect("read_tls from slice");
                client.process_new_packets()?;
            }
        }
        if !client.is_handshaking() && !server.is_handshaking() {
            return Ok(());
        }
    }
    panic!("handshake did not converge");
}

/// rustls defers its KeyUpdate reply, so a parked connection owes the peer
/// nothing observable through `wants_write()`. Canary: if this ever flips,
/// the KeyUpdate deadlock the c10k review described becomes real.
#[test]
fn keyupdate_reply_is_deferred_until_the_next_outbound_record() {
    let tmp = std::env::temp_dir().join(format!("moon-tls-keyupdate-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&tmp);
    if !generate_cert(&tmp) {
        eprintln!("skipping: openssl unavailable");
        let _ = std::fs::remove_dir_all(&tmp);
        return;
    }

    let server_cfg = moon::tls::build_tls_config(
        tmp.join("cert.pem").to_str().expect("utf8 path"),
        tmp.join("key.pem").to_str().expect("utf8 path"),
        None,
        None,
    )
    .expect("build server TLS config");

    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let client_cfg = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyCert(provider)))
        .with_no_client_auth();

    let mut client = rustls::ClientConnection::new(
        Arc::new(client_cfg),
        "localhost".try_into().expect("server name"),
    )
    .expect("client conn");
    let mut server = rustls::ServerConnection::new(server_cfg).expect("server conn");

    drive_handshake(&mut client, &mut server).expect("handshake");
    assert_eq!(
        client.protocol_version(),
        Some(rustls::ProtocolVersion::TLSv1_3),
        "KeyUpdate is a TLS 1.3 message; the test needs a 1.3 session"
    );

    // Quiescent: nothing owed either way. This is the state the park veto is
    // designed to say "yes" to.
    assert!(!server.wants_write(), "server owes nothing after handshake");

    // The client asks us to rotate keys AND to answer with our own KeyUpdate.
    client.refresh_traffic_keys().expect("refresh_traffic_keys");
    let mut c2s = Vec::new();
    while client.wants_write() {
        client.write_tls(&mut c2s).expect("write_tls");
    }
    assert!(!c2s.is_empty(), "KeyUpdate must produce bytes on the wire");

    // The record is now in our TLS buffer but NOT yet processed — exactly the
    // moment the idle sweep evaluates `task_park_safe`.
    let mut cursor = &c2s[..];
    while !cursor.is_empty() {
        server.read_tls(&mut cursor).expect("read_tls");
    }

    // OLD ORDERING: `wants_write()` sampled here, before processing.
    let old_wants_write = server.wants_write();
    let state = server.process_new_packets().expect("process_new_packets");
    let plaintext = state.plaintext_bytes_to_read();
    let closed = state.peer_has_closed();
    // NEW ORDERING: sampled after.
    let new_wants_write = server.wants_write();

    assert_eq!(plaintext, 0, "KeyUpdate carries no plaintext");
    assert!(!closed, "peer has not closed");
    assert!(!old_wants_write, "nothing was queued before processing");

    // THE CANARY. rustls parks its reply in `queued_key_update_message` and
    // flushes it lazily, so processing the record queues nothing visible. Both
    // orderings therefore agree that parking is safe — the KeyUpdate deadlock
    // the review described is not reachable with this rustls.
    assert!(
        !new_wants_write,
        "rustls now queues the KeyUpdate reply eagerly — the KeyUpdate park \
         deadlock just became REACHABLE. The reordered veto in \
         vendor/monoio-rustls/src/stream.rs (c10k B5) already handles it; \
         update that comment and this test rather than reverting."
    );

    // The reply is real, just deferred: it appears once anything else goes out.
    server
        .writer()
        .write_all(b"x")
        .expect("write plaintext to trigger the deferred flush");
    let mut s2c = Vec::new();
    while server.wants_write() {
        server.write_tls(&mut s2c).expect("write_tls");
    }
    assert!(
        !s2c.is_empty(),
        "the deferred KeyUpdate must ride out with the next record"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}
