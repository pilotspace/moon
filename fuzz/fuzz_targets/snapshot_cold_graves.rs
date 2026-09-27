#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::persistence::snapshot::cold_graves::{decode, encode};

/// Fuzz the shard snapshot's cold-graves trailer decoder (moon#1281).
///
/// The trailer sits between the snapshot's EOF marker and its global CRC, so
/// the loader hands it bytes whose integrity the global CRC already proved —
/// but a bug in a writer, or a file edited by hand, can still put anything
/// there. The decoder must never panic, never allocate past what the input
/// can hold (every count is checked against the bytes left), and whatever it
/// accepts must survive an encode/decode round trip unchanged.
fuzz_target!(|data: &[u8]| {
    if let Ok(graves) = decode(data) {
        let files = graves.to_files();
        let again = decode(&encode(&files)).expect("an encoded trailer decodes");
        assert_eq!(again, graves, "round trip changed the graves");
    }
});
