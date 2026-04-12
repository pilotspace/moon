//! End-to-end integration test for multi-shard SCAN fan-out (UX-01, Phase 137).
//!
//! Starts moon with `--shards 4`, writes 100 keys via redis-cli (random
//! distribution so keys land on every shard), then paginates through
//! `GET /api/v1/keys?cursor=<c>&count=20` and asserts the union equals the
//! inserted keys.
//!
//! Run with:
//!   cargo test --release --features console,graph --test scan_fanout_multishard -- --ignored

#![cfg(feature = "console")]

use std::collections::HashSet;
use std::process::{Child, Command};
use std::time::Duration;

const RESP_PORT: u16 = 16499;
const ADMIN_PORT: u16 = 16500;
const KEY_COUNT: usize = 100;

fn start_server() -> Child {
    Command::new("./target/release/moon")
        .args([
            "--port",
            &RESP_PORT.to_string(),
            "--admin-port",
            &ADMIN_PORT.to_string(),
            "--shards",
            "4",
        ])
        .spawn()
        .expect("Failed to start moon (build with --release --features console first)")
}

fn wait_for_port(port: u16) {
    for _ in 0..80 {
        if std::net::TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            std::thread::sleep(Duration::from_millis(300));
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("Server did not start within 8 seconds on port {}", port);
}

fn redis_cli_set(port: u16, key: &str, value: &str) {
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SET", key, value])
        .output()
        .expect("redis-cli SET failed");
    assert!(
        out.status.success(),
        "redis-cli SET failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn curl_get(url: &str) -> (u16, String) {
    let out = Command::new("curl")
        .args(["-s", "-w", "\n%{http_code}", url])
        .output()
        .expect("curl failed");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let lines: Vec<&str> = stdout.trim().rsplitn(2, '\n').collect();
    let status: u16 = lines[0].parse().unwrap_or(0);
    let body = lines.get(1).unwrap_or(&"").to_string();
    (status, body)
}

#[test]
#[ignore] // Requires built release binary + redis-cli; run explicitly
fn scan_fanout_returns_all_keys_across_shards() {
    let mut server = start_server();
    wait_for_port(ADMIN_PORT);
    wait_for_port(RESP_PORT);

    // Populate KEY_COUNT distinct keys. Varied key names spread across shards.
    let mut inserted: HashSet<String> = HashSet::with_capacity(KEY_COUNT);
    for i in 0..KEY_COUNT {
        let k = format!("fanout:{}:{}", i, i.wrapping_mul(2654435761));
        redis_cli_set(RESP_PORT, &k, "v");
        inserted.insert(k);
    }

    // Paginate with count=20 until cursor == "0".
    let mut found: HashSet<String> = HashSet::new();
    let mut cursor = String::from("0");
    let mut first_call = true;
    let mut iterations = 0usize;

    loop {
        let url = format!(
            "http://127.0.0.1:{}/api/v1/keys?pattern=fanout:*&cursor={}&count=20",
            ADMIN_PORT, cursor
        );
        let (status, body) = curl_get(&url);
        assert_eq!(status, 200, "SCAN call failed: status={} body={}", status, body);
        let json: serde_json::Value = serde_json::from_str(&body)
            .unwrap_or_else(|e| panic!("invalid JSON from SCAN: {} body={}", e, body));
        let next_cursor = json["cursor"]
            .as_str()
            .expect("cursor field missing")
            .to_string();
        let keys = json["keys"]
            .as_array()
            .expect("keys field missing")
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();
        for k in keys {
            found.insert(k);
        }

        // Safety bound on iterations.
        iterations += 1;
        assert!(
            iterations < 256,
            "too many SCAN pages (>{}); cursor looped",
            iterations
        );

        if !first_call && next_cursor == "0" {
            break;
        }
        first_call = false;
        cursor = next_cursor;
        if cursor == "0" {
            // First call came back with immediate "0" (single-shard drain in one go).
            break;
        }
    }

    // Cleanup BEFORE asserting so we don't leak the server on failure.
    server.kill().ok();
    server.wait().ok();

    let missing: Vec<&String> = inserted.difference(&found).collect();
    assert!(
        missing.is_empty(),
        "multi-shard SCAN missed {} keys (found={}, inserted={}): {:?}",
        missing.len(),
        found.len(),
        inserted.len(),
        &missing.iter().take(5).collect::<Vec<_>>()
    );
    assert_eq!(
        found.len(),
        inserted.len(),
        "SCAN returned {} keys, expected {}",
        found.len(),
        inserted.len()
    );
}
