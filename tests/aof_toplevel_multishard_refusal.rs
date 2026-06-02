//! Integration test: Moon refuses to start with a TopLevel AOF manifest and
//! --shards >= 2 (FIX-W2-6 r2).
//!
//! Regression guard for:
//! - Hard refusal (exit code 2) when a v1 TopLevel manifest is detected with
//!   --shards >= 2 (the data-loss safety gate).
//! - Correct inclusive range notation in the error message (1..=N-1, not 1..N-1).
//! - Error message refers to the runbook, not a nonexistent CLI flag.
//!
//! Run:
//!   cargo build --release
//!   cargo test --release --test aof_toplevel_multishard_refusal -- --ignored
//!
//! Requires the release binary at ./target/release/moon.

use std::fs;
use std::io::Read as _;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

/// Create a temp dir with a v1 TopLevel AOF manifest (layout: TopLevel).
/// This simulates a pre-PR#129 deployment that still has the legacy single-file
/// layout but is being restarted with --shards 2.
fn setup_toplevel_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "moon-w26-refusal-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ));
    fs::create_dir_all(&dir).expect("create temp dir");

    // Create appendonlydir/ with a minimal v1 (TopLevel) manifest.
    let aof_dir = dir.join("appendonlydir");
    fs::create_dir_all(&aof_dir).expect("create appendonlydir");

    // Minimal v1 manifest content (no `version` line = TopLevel layout).
    // parse_v1 expects lines with prefixes `seq N`, `base <file>`, `incr <file>`.
    // The `file` prefix is NOT a valid v1 keyword — parse_v1 would reject it
    // with "no valid sequence number". The correct format is:
    //   seq 1
    //   base moon.aof.1.base.rdb
    //   incr moon.aof.1.incr.aof
    let manifest_content = "seq 1\nbase moon.aof.1.base.rdb\nincr moon.aof.1.incr.aof\n";
    fs::write(aof_dir.join("moon.aof.manifest"), manifest_content)
        .expect("write manifest");

    // Create stub base and incr files so the manifest path check passes.
    fs::write(aof_dir.join("moon.aof.1.base.rdb"), b"").expect("write stub base");
    fs::write(aof_dir.join("moon.aof.1.incr.aof"), b"").expect("write stub incr");

    dir
}

/// Assert that starting moon with a TopLevel manifest and --shards 2 exits
/// with code 2 and prints a REFUSING TO START message to stderr.
///
/// Red criterion (pre-fix): the error message contained `migrate-aof --dir`
/// (a nonexistent flag) and the range was printed as `1..1` (exclusive, looks
/// empty for --shards 2). The fix uses the inclusive form `1..=1` and removes
/// the nonexistent flag reference.
#[test]
#[ignore]
fn toplevel_manifest_with_multishard_exits_2_and_prints_refusing_to_start() {
    let dir = setup_toplevel_dir("basic");
    let stderr_log = dir.join("moon.stderr.log");
    let stdout_log = dir.join("moon.stdout.log");

    let mut child = Command::new("./target/release/moon")
        .args([
            "--port",
            "17399", // high port unlikely to clash
            "--shards",
            "2",
            "--appendonly",
            "yes",
            "--dir",
        ])
        .arg(&dir)
        .stdout(
            fs::File::create(&stdout_log).expect("create stdout log"),
        )
        .stderr(
            fs::File::create(&stderr_log).expect("create stderr log"),
        )
        .spawn()
        .expect("spawn moon (run `cargo build --release` first)");

    // Moon should exit quickly (< 5 s) with code 2 — it does not even bind
    // the port before the manifest check runs.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                let code = status.code().expect("process terminated by signal");
                assert_eq!(
                    code, 2,
                    "expected exit code 2 (REFUSING TO START), got {}",
                    code
                );
                break;
            }
            None => {
                if std::time::Instant::now() >= deadline {
                    child.kill().ok();
                    panic!(
                        "moon did not exit within 5 s — it should have refused immediately. \
                         Check {}",
                        stderr_log.display()
                    );
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    // Read stderr and verify key phrases.
    let mut stderr_content = String::new();
    fs::File::open(&stderr_log)
        .expect("open stderr log")
        .read_to_string(&mut stderr_content)
        .expect("read stderr log");

    assert!(
        stderr_content.contains("REFUSING TO START"),
        "stderr must contain 'REFUSING TO START'; got:\n{}",
        stderr_content
    );

    // Post-fix: error message must reference the runbook, not a nonexistent
    // CLI flag like `migrate-aof --dir`.
    assert!(
        stderr_content.contains("multi-shard-aof-rewrite.md"),
        "stderr must reference the runbook (multi-shard-aof-rewrite.md); got:\n{}",
        stderr_content
    );

    // Post-fix: range must use inclusive notation (1..=N-1 for --shards 2 → 1..=1).
    // Pre-fix this was `1..1` (exclusive), which looks like an empty range to operators.
    assert!(
        stderr_content.contains("1..=1"),
        "stderr must use inclusive range '1..=1' for --shards 2; got:\n{}",
        stderr_content
    );

    // Cleanup
    fs::remove_dir_all(&dir).ok();
}

/// Sanity check: --shards 1 with a TopLevel manifest must boot normally (no refusal).
/// This exercises the single-shard compatibility path that must remain unaffected.
#[test]
#[ignore]
fn toplevel_manifest_with_single_shard_is_allowed() {
    let dir = setup_toplevel_dir("single");
    let stderr_log = dir.join("moon.stderr.log");
    let stdout_log = dir.join("moon.stdout.log");

    let mut child = Command::new("./target/release/moon")
        .args([
            "--port",
            "17400",
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--dir",
        ])
        .arg(&dir)
        .stdout(fs::File::create(&stdout_log).expect("create stdout log"))
        .stderr(fs::File::create(&stderr_log).expect("create stderr log"))
        .spawn()
        .expect("spawn moon");

    // Give it 3 s to either start or exit.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                let code = status.code().unwrap_or(-1);
                // If it exited with code 2 it incorrectly refused a single-shard TopLevel boot.
                assert_ne!(
                    code, 2,
                    "Moon must NOT refuse single-shard + TopLevel manifest; got exit 2. \
                     stderr: {}",
                    fs::read_to_string(&stderr_log).unwrap_or_default()
                );
                // Any other exit (e.g., port conflict) is fine for this test's purposes.
                break;
            }
            None => {
                if std::time::Instant::now() >= deadline {
                    // Still running — which means it did NOT refuse. That's the correct outcome.
                    child.kill().ok();
                    break;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    fs::remove_dir_all(&dir).ok();
}
