#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::mq::wal::{
    decode_mq_ack, decode_mq_create, decode_mq_pop, decode_mq_push, decode_mq_trigger,
    peek_version,
};

/// Fuzz the MQ WAL v3 op-blob decoders (Wave B stage 2a / task #34).
///
/// `data` is fed to all five decoders directly -- exactly what
/// `src/shard/shared_databases.rs::apply_mq_wal_record` does with a raw WAL
/// record payload straight off disk (attacker/corruption-controlled: a
/// truncated write, a torn page, or a future-version payload written by a
/// newer binary that this build must never trust blindly). Every decoder is
/// documented to return `None` on ANY malformed input or unsupported
/// version byte -- NEVER panic, NEVER read out of bounds. Any panic or OOB
/// access here is a bug.
fuzz_target!(|data: &[u8]| {
    let _ = peek_version(data);
    let _ = decode_mq_create(data);
    let _ = decode_mq_ack(data);
    let _ = decode_mq_push(data);
    let _ = decode_mq_pop(data);
    let _ = decode_mq_trigger(data);
});
