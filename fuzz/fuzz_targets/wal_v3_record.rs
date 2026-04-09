#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::persistence::wal_v3::record;

/// Fuzz the WAL v3 record decoder.
///
/// Exercises CRC32C validation, record type dispatch, LZ4 decompression,
/// and truncation handling. Any panic or OOB access is a bug.
fuzz_target!(|data: &[u8]| {
    // Should return None cleanly on any malformed input
    let _ = record::read_wal_v3_record(data);
});
