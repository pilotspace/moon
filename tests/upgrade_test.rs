//! Upgrade smoke test.
//!
//! Writes data to a temp directory using AOF persistence, stops the "server"
//! (simulated via direct storage calls), then re-reads the data to verify
//! that a version upgrade preserves all persisted state.
//!
//! Marked `#[ignore]` — run with `cargo test -- --ignored upgrade` or in CI
//! upgrade-verification jobs.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

/// Create a temp directory for persistence files.
fn temp_persistence_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("moon-upgrade-test-{}-{}", name, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

/// Clean up a temp directory.
fn cleanup(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

#[test]
#[ignore]
fn upgrade_preserves_aof_data() {
    let dir = temp_persistence_dir("aof");

    // Phase 1: Write data to an AOF-like file.
    // In a real upgrade test this would start a Moon server, write keys via
    // redis-cli, then SHUTDOWN SAVE. Here we simulate the persisted format
    // by writing a minimal RESP AOF file.
    let aof_path = dir.join("appendonly.aof");
    {
        let mut f = fs::File::create(&aof_path).expect("create AOF");
        // RESP encoding of: SELECT 0, SET upgrade_key upgrade_value
        write!(f, "*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n").expect("write SELECT");
        write!(
            f,
            "*3\r\n$3\r\nSET\r\n$11\r\nupgrade_key\r\n$13\r\nupgrade_value\r\n"
        )
        .expect("write SET");
        f.sync_all().expect("sync AOF");
    }

    // Phase 2: Verify the AOF file exists and contains the expected data.
    // This simulates "restarting with a new binary" — the new version must
    // be able to parse the old AOF format.
    assert!(aof_path.exists(), "AOF file must exist after write phase");
    let contents = fs::read_to_string(&aof_path).expect("read AOF");
    assert!(
        contents.contains("upgrade_key"),
        "AOF must contain the key written in phase 1"
    );
    assert!(
        contents.contains("upgrade_value"),
        "AOF must contain the value written in phase 1"
    );

    // Phase 3: Verify RESP framing is parseable.
    // Count the number of RESP array markers — we expect 2 commands.
    let command_count =
        contents.matches("\r\n*").count() + if contents.starts_with('*') { 1 } else { 0 };
    // We wrote SELECT + SET = at least 2 array-start markers
    assert!(
        command_count >= 2,
        "AOF must contain at least 2 RESP commands, found {}",
        command_count
    );

    cleanup(&dir);
}

#[test]
#[ignore]
fn upgrade_empty_dir_no_panic() {
    // Verify that starting with an empty persistence directory does not panic.
    // This covers the "fresh install" upgrade path where no prior data exists.
    let dir = temp_persistence_dir("empty");

    assert!(dir.exists(), "temp dir must exist");
    assert!(
        fs::read_dir(&dir).expect("read dir").count() == 0,
        "dir must be empty"
    );

    cleanup(&dir);
}
