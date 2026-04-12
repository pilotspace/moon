//! Integration test for the console gateway REST API.
//!
//! Requires a built Moon binary with the `console` feature enabled.
//! Starts a server process, exercises REST endpoints via curl, and validates responses.
//!
//! Run with: `cargo test --features console,graph --test console_gateway_test`

#![cfg(feature = "console")]

use std::process::{Child, Command};
use std::time::Duration;

fn start_server() -> Child {
    Command::new("./target/release/moon")
        .args(["--port", "16399", "--admin-port", "16400", "--shards", "2"])
        .spawn()
        .expect("Failed to start moon server (build with --release --features console,graph first)")
}

fn wait_for_server(port: u16) {
    for _ in 0..50 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            // Give a moment for full initialization.
            std::thread::sleep(Duration::from_millis(200));
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("Server did not start within 5 seconds on port {}", port);
}

/// Execute a curl command and return (status_code, body).
fn curl_json(method: &str, url: &str, body: Option<&str>) -> (u16, String) {
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "-w", "\n%{http_code}", "-X", method, url]);
    if let Some(b) = body {
        cmd.args(["-H", "Content-Type: application/json", "-d", b]);
    }
    let output = cmd.output().expect("curl failed");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let lines: Vec<&str> = stdout.trim().rsplitn(2, '\n').collect();
    let status: u16 = lines[0].parse().unwrap_or(0);
    let resp_body = lines.get(1).unwrap_or(&"").to_string();
    (status, resp_body)
}

#[test]
#[ignore] // Requires a built release binary; run explicitly
fn test_rest_api_endpoints() {
    let mut server = start_server();
    wait_for_server(16400);

    // 1. POST /api/v1/command -- SET a key
    let (status, body) = curl_json(
        "POST",
        "http://127.0.0.1:16400/api/v1/command",
        Some(r#"{"cmd":"SET","args":["itest_key","hello_world"]}"#),
    );
    assert_eq!(status, 200, "SET command failed: {}", body);

    // 2. POST /api/v1/command -- GET the key
    let (status, body) = curl_json(
        "POST",
        "http://127.0.0.1:16400/api/v1/command",
        Some(r#"{"cmd":"GET","args":["itest_key"]}"#),
    );
    assert_eq!(status, 200, "GET command failed: {}", body);
    assert!(
        body.contains("hello_world"),
        "Expected 'hello_world' in response: {}",
        body
    );

    // 3. GET /api/v1/key/itest_key
    let (status, body) = curl_json("GET", "http://127.0.0.1:16400/api/v1/key/itest_key", None);
    assert_eq!(status, 200, "GET key failed: {}", body);
    assert!(
        body.contains("string"),
        "Expected type 'string' in response: {}",
        body
    );

    // 4. GET /api/v1/keys?pattern=itest_*
    let (status, body) = curl_json(
        "GET",
        "http://127.0.0.1:16400/api/v1/keys?pattern=itest_*",
        None,
    );
    assert_eq!(status, 200, "SCAN keys failed: {}", body);

    // 5. GET /api/v1/key/itest_key/ttl
    let (status, body) = curl_json(
        "GET",
        "http://127.0.0.1:16400/api/v1/key/itest_key/ttl",
        None,
    );
    assert_eq!(status, 200, "GET TTL failed: {}", body);
    assert!(
        body.contains("ttl"),
        "Expected 'ttl' field in response: {}",
        body
    );

    // 6. PUT /api/v1/key/itest_key/ttl
    let (status, body) = curl_json(
        "PUT",
        "http://127.0.0.1:16400/api/v1/key/itest_key/ttl",
        Some(r#"{"ttl":60}"#),
    );
    assert_eq!(status, 200, "SET TTL failed: {}", body);

    // 7. DELETE /api/v1/key/itest_key
    let (status, body) = curl_json(
        "DELETE",
        "http://127.0.0.1:16400/api/v1/key/itest_key",
        None,
    );
    assert_eq!(status, 200, "DELETE key failed: {}", body);
    assert!(
        body.contains("deleted"),
        "Expected 'deleted' field: {}",
        body
    );

    // 8. OPTIONS preflight
    let (status, _) = curl_json("OPTIONS", "http://127.0.0.1:16400/api/v1/command", None);
    assert_eq!(status, 204, "OPTIONS preflight should return 204");

    // 9. GET /api/v1/info
    let (status, body) = curl_json("GET", "http://127.0.0.1:16400/api/v1/info", None);
    assert_eq!(status, 200, "GET info failed: {}", body);
    assert!(
        body.contains("info"),
        "Expected 'info' field in response: {}",
        body
    );

    // Cleanup
    server.kill().ok();
    server.wait().ok();
}
