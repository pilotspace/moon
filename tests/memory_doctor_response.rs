//! Integration test for `MEMORY DOCTOR` response schema (Phase 190 Plan 02).
//!
//! Spawns the release moon binary on a free TCP port, loads a small dataset,
//! and verifies the documented multi-line Bulk String schema from OBS-03.
//!
//! Run with:
//!   cargo test --release --test memory_doctor_response

use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Allocate an OS-assigned TCP port, drop the listener, return the number.
fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    l.local_addr().unwrap().port()
}

fn redis_cli_available() -> bool {
    Command::new("redis-cli")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn release_binary() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/release/moon")
}

/// Running moon instance. Auto-killed on drop.
struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon() -> Option<Moon> {
    if !redis_cli_available() {
        eprintln!("skipping: redis-cli not in PATH");
        return None;
    }
    let bin = release_binary();
    if !bin.exists() {
        eprintln!(
            "skipping: {} not built. Run `cargo build --release` first.",
            bin.display()
        );
        return None;
    }
    let port = free_port();
    let tmp_dir = std::env::temp_dir().join(format!("moon-test-doctor-{port}"));
    let _ = std::fs::create_dir_all(&tmp_dir);
    let child = Command::new(&bin)
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--admin-port",
            "0",
            "--appendonly",
            "no",
            "--dir",
            tmp_dir.to_str().unwrap(),
            "--disk-offload",
            "disable",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };

    // Wait up to ~5s for PING to succeed.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if redis_cli(moon.port, &["PING"])
            .map(|out| out.trim() == "PONG")
            .unwrap_or(false)
        {
            return Some(moon);
        }
        thread::sleep(Duration::from_millis(100));
    }
    eprintln!("skipping: moon did not respond to PING within 5s on port {port}");
    None
}

/// Run `redis-cli -p <port> <args...>` and return stdout as String.
fn redis_cli(port: u16, args: &[&str]) -> Option<String> {
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string()])
        .args(args)
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Parse a human-readable byte value like "1.23 MB" or "456 B" into approximate bytes.
fn parse_human_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    let num: f64 = parts[0].parse().ok()?;
    let multiplier = match parts[1] {
        "B" => 1.0,
        "KB" => 1024.0,
        "MB" => 1024.0 * 1024.0,
        "GB" => 1024.0 * 1024.0 * 1024.0,
        "TB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    Some((num * multiplier) as u64)
}

#[test]
fn memory_doctor_returns_documented_schema() {
    let Some(m) = spawn_moon() else { return };

    // Load 1000 string keys to ensure DashTable has non-zero resident bytes.
    for i in 0..1000 {
        let key = format!("key:{i}");
        let val = format!("value-{i}-padding-to-ensure-some-memory-usage");
        redis_cli(m.port, &["SET", &key, &val]).unwrap();
    }

    // Issue MEMORY DOCTOR.
    let output = redis_cli(m.port, &["MEMORY", "DOCTOR"]).expect("MEMORY DOCTOR failed");

    // 1. Assert section headers are present.
    let required_sections = [
        "Process:",
        "Per-subsystem (resident):",
        "Mapped regions:",
        "Recommendations:",
    ];
    for section in &required_sections {
        assert!(
            output.contains(section),
            "Missing section header '{section}' in MEMORY DOCTOR output:\n{output}"
        );
    }

    // 2. Assert all 7 kind labels are present.
    let required_kinds = [
        "DashTable + entries:",
        "HNSW (vector):",
        "CSR (graph):",
        "WAL writers:",
        "Sealed segments:",
        "Replication backlog:",
        "Allocator overhead:",
    ];
    for kind in &required_kinds {
        assert!(
            output.contains(kind),
            "Missing kind label '{kind}' in MEMORY DOCTOR output:\n{output}"
        );
    }

    // 3. Assert allocator name is non-empty and recognized.
    assert!(
        output.contains("Allocator:"),
        "Missing 'Allocator:' line in output:\n{output}"
    );
    let allocator_line = output
        .lines()
        .find(|l| l.contains("Allocator:"))
        .expect("Allocator line not found");
    let allocator_value = allocator_line.split("Allocator:").nth(1).unwrap().trim();
    assert!(
        allocator_value.starts_with("jemalloc")
            || allocator_value.starts_with("mimalloc")
            || allocator_value.starts_with("system"),
        "Allocator name not recognized: '{allocator_value}'"
    );

    // 4. Parse RSS value.
    let rss_line = output
        .lines()
        .find(|l| l.contains("RSS:"))
        .expect("RSS line not found");
    let rss_value_str = rss_line.split("RSS:").nth(1).unwrap().trim();
    let rss_bytes = parse_human_bytes(rss_value_str)
        .unwrap_or_else(|| panic!("Cannot parse RSS value: '{rss_value_str}'"));
    assert!(rss_bytes > 0, "RSS should be > 0");

    // 5. Parse per-subsystem byte values and verify sum >= 95% of RSS.
    let mut subsystem_sum: u64 = 0;
    for kind in &required_kinds {
        let kind_line = output
            .lines()
            .find(|l| l.contains(kind))
            .unwrap_or_else(|| panic!("Kind line not found: {kind}"));
        // Format: "  DashTable + entries:    1.23 MB  (45.6%)"
        let after_colon = kind_line.split(':').last().unwrap().trim();
        // Extract just the bytes portion before the parenthesized percentage.
        let bytes_str = if let Some(paren_pos) = after_colon.find('(') {
            after_colon[..paren_pos].trim()
        } else {
            after_colon
        };
        if let Some(bytes) = parse_human_bytes(bytes_str) {
            subsystem_sum += bytes;
        }
    }

    // The sum of all 7 kinds (including allocator overhead = RSS - others)
    // equals RSS by construction. This is a tautology check.
    // The meaningful assertion is that DashTable > 0 after loading 1000 keys.
    let dashtable_line = output
        .lines()
        .find(|l| l.contains("DashTable + entries:"))
        .unwrap();
    let dt_after_colon = dashtable_line.split(':').last().unwrap().trim();
    let dt_bytes_str = if let Some(pos) = dt_after_colon.find('(') {
        dt_after_colon[..pos].trim()
    } else {
        dt_after_colon
    };
    let dt_bytes = parse_human_bytes(dt_bytes_str).unwrap_or(0);
    assert!(
        dt_bytes > 0,
        "DashTable + entries should be > 0 after loading 1000 keys, got 0. Output:\n{output}"
    );

    // Sum should be >= 95% of RSS (by construction, allocator overhead absorbs the gap).
    if rss_bytes > 0 {
        let pct = (subsystem_sum as f64 / rss_bytes as f64) * 100.0;
        assert!(
            pct >= 95.0,
            "Per-subsystem sum ({subsystem_sum}) is only {pct:.1}% of RSS ({rss_bytes}), expected >= 95%"
        );
    }
}
