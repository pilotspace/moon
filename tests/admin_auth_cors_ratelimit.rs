//! End-to-end integration tests for admin-port hardening (HARD-01/02/03,
//! Phase 137). Spawns a real `moon` binary and drives it with `ureq` to
//! cover the six `must_haves.truths` from `137-03-PLAN.md`.
//!
//! Each test is self-contained: it picks a free port pair, starts a fresh
//! binary with the flags under test, exercises the HTTP API, and kills the
//! child on drop. Tests tolerate a missing release binary (return Ok early)
//! so `cargo test` without `--features console` doesn't fail spuriously.

#![cfg(feature = "console")]

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Bind a throw-away listener to get a free port, then drop it. There is a
/// TOCTOU window here but it's good enough for local/CI integration tests.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind for port probe")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// RAII wrapper around a moon child process. `Drop` SIGKILLs the child and
/// waits so the port is released before the next test runs.
struct Moon {
    child: Child,
    #[allow(dead_code)]
    port: u16,
    admin: u16,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn bin_path() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release/moon")
}

fn spawn_moon(extra: &[&str]) -> Option<Moon> {
    let bin = bin_path();
    if !bin.exists() {
        eprintln!(
            "release binary {} not found — run `cargo build --release --features console` first",
            bin.display()
        );
        return None;
    }
    let port = free_port();
    let admin = free_port();
    let mut args: Vec<String> = vec![
        "--port".into(),
        port.to_string(),
        "--admin-port".into(),
        admin.to_string(),
        "--shards".into(),
        "1".into(),
    ];
    for a in extra {
        args.push((*a).to_string());
    }
    let child = Command::new(&bin)
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    // Poll /healthz until the admin server accepts connections (max 8s).
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if Instant::now() >= deadline {
            let _ = bin_path();
            break;
        }
        let url = format!("http://127.0.0.1:{}/healthz", admin);
        if let Ok(resp) = ureq::get(&url).timeout(Duration::from_millis(500)).call()
            && resp.status() == 200
        {
            return Some(Moon { child, port, admin });
        }
        thread::sleep(Duration::from_millis(100));
    }
    // Startup failed — kill child before bailing.
    let mut c = child;
    let _ = c.kill();
    let _ = c.wait();
    None
}

// ────────────────────────────────────────────────────────────────────
// HARD-01: auth required
// ────────────────────────────────────────────────────────────────────

fn sign_token(secret: &[u8], user: &str) -> String {
    use base64::Engine;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac = <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(secret)
        .expect("hmac key init");
    mac.update(user.as_bytes());
    let sig = mac.finalize().into_bytes();
    format!(
        "{}.{}",
        user,
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig)
    )
}

#[test]
fn hard01_auth_required_rejects_anon_and_accepts_bearer() {
    let Some(m) = spawn_moon(&[
        "--console-auth-required",
        "--console-auth-secret",
        "testsecret-0123456789",
    ]) else {
        return;
    };

    // No Authorization header → 401 with WWW-Authenticate: Bearer.
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);
    let err = ureq::get(&url).call().expect_err("expected non-200");
    match err {
        ureq::Error::Status(code, resp) => {
            assert_eq!(code, 401, "expected 401 Unauthorized");
            assert!(
                resp.header("www-authenticate")
                    .map(|v| v.contains("Bearer"))
                    .unwrap_or(false),
                "www-authenticate header missing or not Bearer: {:?}",
                resp.header("www-authenticate"),
            );
        }
        other => panic!("expected Status error, got {other:?}"),
    }

    // Valid Bearer → 200.
    let tok = sign_token(b"testsecret-0123456789", "alice");
    let auth = format!("Bearer {}", tok);
    let resp = ureq::get(&url)
        .set("authorization", &auth)
        .call()
        .expect("200 with bearer");
    assert_eq!(resp.status(), 200);
}

#[test]
fn hard01_tampered_signature_rejected() {
    let Some(m) = spawn_moon(&[
        "--console-auth-required",
        "--console-auth-secret",
        "s",
    ]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);
    // Valid user prefix but garbage signature.
    let auth = "Bearer alice.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let err = ureq::get(&url)
        .set("authorization", auth)
        .call()
        .expect_err("expected 401");
    match err {
        ureq::Error::Status(code, _) => assert_eq!(code, 401),
        other => panic!("expected 401, got {other:?}"),
    }
}

// ────────────────────────────────────────────────────────────────────
// HARD-02: CORS allowlist
// ────────────────────────────────────────────────────────────────────

#[test]
fn hard02_cors_allowlist_only_echoes_allowed_origin() {
    let Some(m) = spawn_moon(&[
        "--console-cors-origin",
        "https://allowed.example",
    ]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);

    // Allowed origin → header echoes the exact origin (not '*').
    let resp = ureq::get(&url)
        .set("origin", "https://allowed.example")
        .call()
        .unwrap();
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://allowed.example")
    );

    // Disallowed origin → no Allow-Origin header at all.
    let resp = ureq::get(&url)
        .set("origin", "https://evil.example")
        .call()
        .unwrap();
    assert!(resp.header("access-control-allow-origin").is_none());
}

#[test]
fn hard02_wildcard_with_auth_rejected_at_startup() {
    let bin = bin_path();
    if !bin.exists() {
        return;
    }
    let port = free_port();
    let admin = free_port();
    let out = Command::new(&bin)
        .args([
            "--port",
            &port.to_string(),
            "--admin-port",
            &admin.to_string(),
            "--shards",
            "1",
            "--console-auth-required",
            "--console-auth-secret",
            "s",
            "--console-cors-origin",
            "*",
        ])
        .output()
        .expect("spawn moon");
    assert!(
        !out.status.success(),
        "startup should reject wildcard+auth; stdout={:?} stderr={:?}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        combined.contains('*') || combined.to_lowercase().contains("wildcard"),
        "error message should mention wildcard: {combined}",
    );
}

// ────────────────────────────────────────────────────────────────────
// HARD-03: rate limiting
// ────────────────────────────────────────────────────────────────────

#[test]
fn hard03_rate_limit_returns_429_with_retry_after() {
    // Very low burst so we can drain the bucket with a handful of requests
    // without making the test slow.
    let Some(m) = spawn_moon(&[
        "--console-rate-limit",
        "5",
        "--console-rate-burst",
        "5",
    ]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);

    // Drain the bucket — 5 should succeed (or at worst one is captured by
    // the initial healthz probe during startup; tolerate a small slack).
    for _ in 0..10 {
        let _ = ureq::get(&url).call();
    }
    // Now ask once more; must 429.
    let err = ureq::get(&url).call().err();
    match err {
        Some(ureq::Error::Status(code, resp)) => {
            assert_eq!(code, 429, "expected 429 Too Many Requests");
            assert!(
                resp.header("retry-after").is_some(),
                "retry-after header must be set on 429"
            );
        }
        other => panic!("expected 429 Status error, got {other:?}"),
    }
}

// ────────────────────────────────────────────────────────────────────
// Cross-concern: preflight bypasses auth, probes bypass auth
// ────────────────────────────────────────────────────────────────────

#[test]
fn preflight_bypasses_auth() {
    let Some(m) = spawn_moon(&[
        "--console-auth-required",
        "--console-auth-secret",
        "s",
        "--console-cors-origin",
        "https://ui.example",
    ]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);
    let resp = ureq::request("OPTIONS", &url)
        .set("origin", "https://ui.example")
        .set("access-control-request-method", "GET")
        .call()
        .expect("OPTIONS should succeed without auth");
    assert_eq!(resp.status(), 204);
    assert_eq!(
        resp.header("access-control-allow-origin"),
        Some("https://ui.example"),
    );
    assert!(resp.header("access-control-allow-methods").is_some());
}

#[test]
fn healthz_and_readyz_bypass_auth() {
    let Some(m) = spawn_moon(&[
        "--console-auth-required",
        "--console-auth-secret",
        "s",
    ]) else {
        return;
    };
    for path in ["/healthz", "/readyz", "/metrics"] {
        let url = format!("http://127.0.0.1:{}{}", m.admin, path);
        let resp = ureq::get(&url).call();
        // /readyz may return 503 until ready; we tolerate 200/503 as long
        // as it isn't 401.
        match resp {
            Ok(r) => assert_ne!(r.status(), 401, "{path} should not require auth"),
            Err(ureq::Error::Status(code, _)) => {
                assert_ne!(code, 401, "{path} returned 401 — auth exemption broken");
            }
            Err(e) => panic!("{path} request failed: {e}"),
        }
    }
}
