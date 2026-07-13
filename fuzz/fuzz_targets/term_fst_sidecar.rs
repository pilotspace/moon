#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::text::index_persist::deserialize_term_fst_sidecar;

/// Fuzz the combined term-dict + FST sidecar decoder (kernel M4, task #50).
///
/// Exercises magic/version validation, the per-field term-count/term-len
/// framing loop, UTF-8 term validation, and FST-length truncation handling.
/// Any panic or OOB access is a bug -- malformed on-disk sidecar bytes
/// (truncated write, bit rot, downgrade/upgrade version skew) must always
/// fail closed with `Err`, never panic and never return a partially
/// populated `Vec`.
fuzz_target!(|data: &[u8]| {
    let _ = deserialize_term_fst_sidecar(data);
});
