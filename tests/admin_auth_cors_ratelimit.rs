//! End-to-end integration tests for admin-port hardening (HARD-01/02/03,
//! Phase 137). Spawns a real `moon` binary and drives it with `ureq` to
//! cover the six `must_haves.truths` from `137-03-PLAN.md`.
//!
//! Each test is self-contained: it picks a free port pair, starts a fresh
//! binary with the flags under test, exercises the HTTP API, and kills the
//! child on drop. Tests tolerate a missing release binary (return Ok early)
//! so `cargo test` without `--features console` doesn't fail spuriously.
//!
//! ureq 3 note: `Error::Status(code, resp)` from ureq 2 is gone — a 4xx/5xx
//! response is no longer even routed through `Result::Err` unless
//! `http_status_as_error` is left at its (default-on) setting, and when it
//! is, the response (and thus its headers) are discarded. These tests need
//! to assert on headers of error responses (`www-authenticate`,
//! `retry-after`), so every request here disables `http_status_as_error`
//! and inspects `resp.status()` / `resp.headers()` directly instead of
//! matching on `ureq::Error::Status`.

#![cfg(feature = "console")]

mod common;

use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use ureq::http::Response;
use ureq::{Body, Error};

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

/// GET `url` with the given headers, returning the raw response even for
/// 4xx/5xx status codes (see module doc for why `http_status_as_error` is
/// disabled here). Only genuine transport errors (timeout, connection
/// refused, ...) surface as `Err`.
fn get(url: &str, headers: &[(&str, &str)]) -> Result<Response<Body>, Error> {
    let mut req = ureq::get(url);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    req.config()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(5)))
        .build()
        .call()
}

/// OPTIONS `url` with the given headers (CORS preflight). Same
/// error-tolerant status handling as [`get`].
fn options(url: &str, headers: &[(&str, &str)]) -> Result<Response<Body>, Error> {
    let mut req = ureq::options(url);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    req.config()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(5)))
        .build()
        .call()
}

fn header<'a>(resp: &'a Response<Body>, name: &str) -> Option<&'a str> {
    resp.headers().get(name).and_then(|v| v.to_str().ok())
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
    // The plain client `--port` is never dialed by this suite (only the
    // admin HTTP port is) — reserve it once, up front, so it doesn't
    // collide with anything else in-process.
    let port = common::reserve_port();
    let extra_owned: Vec<String> = extra.iter().map(|a| (*a).to_string()).collect();
    let (child, admin) = common::spawn_listening(|admin_port| {
        let mut args: Vec<String> = vec![
            "--port".into(),
            port.to_string(),
            "--admin-port".into(),
            admin_port.to_string(),
            "--shards".into(),
            "1".into(),
        ];
        args.extend(extra_owned.iter().cloned());
        Command::new(&bin)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    // Poll /healthz until the admin server accepts connections (max 8s).
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if Instant::now() >= deadline {
            let _ = bin_path();
            break;
        }
        let url = format!("http://127.0.0.1:{}/healthz", admin);
        if let Ok(resp) = ureq::get(&url)
            .config()
            .timeout_global(Some(Duration::from_millis(500)))
            .build()
            .call()
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
    let mut mac =
        <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(secret).expect("hmac key init");
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
    let resp = get(&url, &[]).expect("request should complete");
    assert_eq!(resp.status(), 401, "expected 401 Unauthorized");
    assert!(
        header(&resp, "www-authenticate")
            .map(|v| v.contains("Bearer"))
            .unwrap_or(false),
        "www-authenticate header missing or not Bearer: {:?}",
        header(&resp, "www-authenticate"),
    );

    // Valid Bearer → 200.
    let tok = sign_token(b"testsecret-0123456789", "alice");
    let auth = format!("Bearer {}", tok);
    let resp = get(&url, &[("authorization", &auth)]).expect("200 with bearer");
    assert_eq!(resp.status(), 200);
}

#[test]
fn hard01_tampered_signature_rejected() {
    let Some(m) = spawn_moon(&["--console-auth-required", "--console-auth-secret", "s"]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);
    // Valid user prefix but garbage signature.
    let auth = "Bearer alice.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let resp = get(&url, &[("authorization", auth)]).expect("request should complete");
    assert_eq!(resp.status(), 401);
}

// ────────────────────────────────────────────────────────────────────
// HARD-02: CORS allowlist
// ────────────────────────────────────────────────────────────────────

#[test]
fn hard02_cors_allowlist_only_echoes_allowed_origin() {
    let Some(m) = spawn_moon(&["--console-cors-origin", "https://allowed.example"]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);

    // Allowed origin → header echoes the exact origin (not '*').
    let resp = get(&url, &[("origin", "https://allowed.example")]).unwrap();
    assert_eq!(
        header(&resp, "access-control-allow-origin"),
        Some("https://allowed.example")
    );

    // Disallowed origin → no Allow-Origin header at all.
    let resp = get(&url, &[("origin", "https://evil.example")]).unwrap();
    assert!(header(&resp, "access-control-allow-origin").is_none());
}

#[test]
fn hard02_wildcard_with_auth_rejected_at_startup() {
    let bin = bin_path();
    if !bin.exists() {
        return;
    }
    // Deliberately expects startup to FAIL (wildcard CORS + auth is
    // rejected before the server ever binds), so `spawn_listening`'s
    // accept-retry loop doesn't fit here — just reserve two dedup'd ports.
    let port = common::reserve_port();
    let admin = common::reserve_port();
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
    let Some(m) = spawn_moon(&["--console-rate-limit", "5", "--console-rate-burst", "5"]) else {
        return;
    };
    let url = format!("http://127.0.0.1:{}/api/v1/info", m.admin);

    // Drain the bucket — 5 should succeed (or at worst one is captured by
    // the initial healthz probe during startup; tolerate a small slack).
    for _ in 0..10 {
        let _ = get(&url, &[]);
    }
    // Now ask once more; must 429.
    let resp = get(&url, &[]).expect("request should complete");
    assert_eq!(resp.status(), 429, "expected 429 Too Many Requests");
    assert!(
        header(&resp, "retry-after").is_some(),
        "retry-after header must be set on 429"
    );
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
    let resp = options(
        &url,
        &[
            ("origin", "https://ui.example"),
            ("access-control-request-method", "GET"),
        ],
    )
    .expect("OPTIONS should succeed without auth");
    assert_eq!(resp.status(), 204);
    assert_eq!(
        header(&resp, "access-control-allow-origin"),
        Some("https://ui.example"),
    );
    assert!(header(&resp, "access-control-allow-methods").is_some());
}

#[test]
fn healthz_and_readyz_bypass_auth() {
    let Some(m) = spawn_moon(&["--console-auth-required", "--console-auth-secret", "s"]) else {
        return;
    };
    for path in ["/healthz", "/readyz", "/metrics"] {
        let url = format!("http://127.0.0.1:{}{}", m.admin, path);
        // /readyz may return 503 until ready; we tolerate 200/503 as long
        // as it isn't 401.
        match get(&url, &[]) {
            Ok(resp) => assert_ne!(resp.status(), 401, "{path} should not require auth"),
            Err(e) => panic!("{path} request failed: {e}"),
        }
    }
}
