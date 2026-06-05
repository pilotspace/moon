#![no_main]
use libfuzzer_sys::fuzz_target;

use std::io::Write;

// Fuzz the CSR graph segment decoders (heap + mmap).
//
// Exercises magic/version validation, version-gated section sizing
// (v1 32B NodeMeta, v2+ 48B, v3+ trailing edge_created_ms array),
// checked-arithmetic length validation, CRC32 checksum, row_offsets
// monotonicity checks, and index rebuilding. Any panic or OOB access
// is a bug — corrupted segment files must fail with CsrError, never
// crash recovery.
fuzz_target!(|data: &[u8]| {
    // Heap parser: must return Err cleanly on any malformed input.
    let _ = moon::graph::CsrSegment::from_bytes(data);

    // Mmap parser (separate unsafe pointer-math code path): only worth a
    // temp-file round trip for inputs that pass the first-byte gate.
    // Tempfile I/O failures (disk full, fd exhaustion) are environment
    // problems, not findings — skip the iteration instead of panicking.
    if data.starts_with(b"MNGR") {
        let Ok(temp) = tempfile::NamedTempFile::new() else {
            return;
        };
        if temp.as_file().write_all(data).is_err() {
            return;
        }
        let _ = moon::graph::MmapCsrSegment::from_mmap_file(temp.path());
    }
});
