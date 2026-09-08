#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::text::index_persist::deserialize_text_index_metas;

/// Fuzz the `text-indexes.meta` sidecar decoder: v1/v2 body framing plus the
/// trailing `TMX3` TAG/NUMERIC block. Any panic or OOB access is a bug —
/// malformed on-disk bytes (truncated write, bit rot, version skew) must
/// always fail closed with `Err`, never panic and never return a partially
/// populated `Vec`.
fuzz_target!(|data: &[u8]| {
    let _ = deserialize_text_index_metas(data);
});
