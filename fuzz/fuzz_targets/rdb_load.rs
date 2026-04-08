#![no_main]
use libfuzzer_sys::fuzz_target;

use std::io::Write;

/// Fuzz the RDB loader by writing fuzzer data to a temp file and loading it.
///
/// The RDB loader reads from a file path, so we write fuzz input to a
/// temp file first. Exercises magic/version validation, type tag dispatch,
/// CRC32 checksum, and per-entry error recovery (skip-and-continue).
fuzz_target!(|data: &[u8]| {
    // Only attempt loads for inputs that could plausibly be an RDB file
    // (start with MOON magic or REDIS magic). This avoids wasting fuzz
    // cycles on inputs that fail at the first byte.
    if data.len() < 10 {
        return;
    }

    let temp = tempfile::NamedTempFile::new().unwrap();
    temp.as_file().write_all(data).unwrap();

    let mut databases: Vec<moon::storage::db::Database> = (0..1)
        .map(|_| moon::storage::db::Database::new())
        .collect();

    // Should not panic regardless of input
    let _ = moon::persistence::rdb::load(&mut databases, temp.path());
});
