#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::replication::ws_sync::install_workspace_registry;

/// Fuzz the WS-plane PSYNC snapshot decoder (`ws_sync::install_workspace_registry`,
/// Wave B ws-plane).
///
/// `data` is untrusted bytes taken straight from a `MOON_AUX_WORKSPACE_REGISTRY`
/// RDB aux blob sent by a peer over PSYNC full-resync — a malicious or corrupted
/// "master" must never crash a replica applying it. `install_workspace_registry`
/// is documented to return `None` on any malformed input (bad version, truncated
/// count, truncated entry, truncated name) rather than panic or read out of
/// bounds; any panic/OOB here is a bug. Mirrors `graph_props_record.rs` and
/// `wal_v3_record.rs`.
fuzz_target!(|data: &[u8]| {
    let _ = install_workspace_registry(data);
});
