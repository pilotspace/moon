#![no_main]
use libfuzzer_sys::fuzz_target;

use bytes::Bytes;
use moon::acl::keyspec::{CommandKeys, KeyPositions, command_key_positions, command_keys};
use moon::protocol::Frame;
use moon::tracking::invalidation;

/// Fuzz the shared key-position walker (moon#576).
///
/// `command_key_positions` parses attacker-controlled argv on behalf of THREE
/// consumers — ACL key-pattern checks, client-side cache invalidation, and
/// command introspection — so a single bounds bug is a remote panic in three
/// places at once. PR #571's review already found one of exactly this class: a
/// `numkeys` usize overflow that produced `&args[1..0]` (fixed in d0423747).
///
/// Beyond "never panics", the properties below are the contract the callers
/// rely on. Two of them are security-relevant, not merely tidy:
///
///   * every reported position must index `args` — the bounds property;
///   * `Unknown` and `AtPlusComputed` must reach ACL as `Indeterminate`.
///     `AtPlusComputed` means at least one key name is computed at runtime
///     (`SORT k BY w_*`), so a `~pattern` user could otherwise reach keys the
///     pattern was never meant to cover. Cache invalidation deliberately does
///     the opposite with the same value, which is why the walker reports facts
///     and each caller applies its own policy.
const MAX_ARGS: usize = 256;

/// Decode `data` into a command name and its argv.
///
/// Fields are NUL-separated: the first is the command name, the rest are the
/// arguments (which EXCLUDE the command name, matching the walker's contract).
/// A leading tag byte picks the frame type so the fuzzer can reach the
/// non-string branches — a key position holding an `Integer` is a malformed
/// invocation the walker still has to survive.
fn decode(data: &[u8]) -> Option<(Vec<u8>, Vec<Frame>)> {
    let mut fields = data.split(|&b| b == 0);
    let cmd = fields.next()?.to_vec();
    let args = fields
        .take(MAX_ARGS)
        .map(|f| match f.split_first() {
            Some((0x01, rest)) => {
                // Reach the numkeys/count walkers with values they must clamp:
                // 0, 1, usize::MAX and its neighbours all live here.
                let mut n = [0u8; 8];
                let take = rest.len().min(8);
                n[..take].copy_from_slice(&rest[..take]);
                Frame::Integer(i64::from_le_bytes(n))
            }
            Some((0x02, _)) => Frame::Null,
            Some((0x03, rest)) => Frame::SimpleString(Bytes::copy_from_slice(rest)),
            _ => Frame::BulkString(Bytes::copy_from_slice(f)),
        })
        .collect();
    Some((cmd, args))
}

fuzz_target!(|data: &[u8]| {
    let Some((cmd, args)) = decode(data) else {
        return;
    };
    let argc = args.len();

    let positions = command_key_positions(&cmd, &args);

    // Bounds: the property whose violation is a remote panic downstream.
    match &positions {
        KeyPositions::At(idx) => {
            assert!(!idx.is_empty(), "At is documented as never empty");
            for k in idx.iter() {
                let i = k.idx;
                assert!(i < argc, "At position {i} out of bounds for argc {argc}");
            }
        }
        KeyPositions::AtPlusComputed(idx) => {
            for k in idx.iter() {
                let i = k.idx;
                assert!(
                    i < argc,
                    "AtPlusComputed position {i} out of bounds for argc {argc}"
                );
            }
        }
        KeyPositions::None | KeyPositions::Unknown => {}
    }

    // The walker is a pure function of its inputs; a consumer that calls it
    // twice (ACL then tracking, on the same command) must see the same answer.
    let again = command_key_positions(&cmd, &args);
    assert_eq!(
        std::mem::discriminant(&positions),
        std::mem::discriminant(&again),
        "walker is not deterministic"
    );

    // --- consumer 1: ACL, which must fail CLOSED ---
    let acl = command_keys(&cmd, &args);
    match (&positions, &acl) {
        (KeyPositions::None, CommandKeys::None) => {}
        (KeyPositions::None, other) => {
            panic!("provably keyless command reached ACL as {other:?}")
        }
        // A named position that is not a string cannot be checked against a
        // key pattern, so `At` is allowed to degrade to Indeterminate.
        (KeyPositions::At(_), CommandKeys::Keys(k)) => {
            assert!(!k.is_empty(), "Keys is documented as never empty");
        }
        (KeyPositions::At(_), CommandKeys::Indeterminate) => {}
        (KeyPositions::At(_), CommandKeys::None) => {
            panic!("command with key positions reached ACL as keyless")
        }
        // The security property: neither of these may ever name keys to ACL.
        (KeyPositions::AtPlusComputed(_) | KeyPositions::Unknown, CommandKeys::Indeterminate) => {}
        (KeyPositions::AtPlusComputed(_) | KeyPositions::Unknown, other) => {
            panic!("unenumerable argv must deny, reached ACL as {other:?}")
        }
    }

    // --- consumer 2: cache invalidation, which must NOT fail closed ---
    let tracked = invalidation::command_keys(&cmd, &args);
    // moon#584: only the WRITE positions may be pushed to a tracking client as
    // changed. This is a strict subset of what the command names, and the
    // subset relation is the property — a `written_keys` that could name a key
    // `command_keys` does not is an invalidation of a key the argv never
    // mentioned.
    let written = invalidation::written_keys(&cmd, &args);
    assert!(
        written.len() <= tracked.len(),
        "written_keys ({}) must be a subset of command_keys ({})",
        written.len(),
        tracked.len()
    );
    for w in written.iter() {
        assert!(
            tracked.contains(w),
            "written_keys named a key the command does not"
        );
    }
    match &positions {
        KeyPositions::None | KeyPositions::Unknown => {
            assert!(
                tracked.is_empty(),
                "nothing to invalidate, got {} keys",
                tracked.len()
            );
        }
        KeyPositions::At(idx) | KeyPositions::AtPlusComputed(idx) => {
            // Non-string positions are skipped, so this is a ceiling, not an
            // equality — but inventing a key would be an over-invalidation bug.
            assert!(
                tracked.len() <= idx.len(),
                "invalidated {} keys from {} positions",
                tracked.len(),
                idx.len()
            );
        }
    }

    // --- consumer 3: COMMAND GETKEYS (moon#537) ---
    //
    // The walker's own bounds property already covers the extraction; what
    // this pins is the ANSWER SHAPE, which is what a cluster client routes on.
    // `command_has_keys` is a STATIC claim, so it must never contradict a
    // walker that just named positions: "this command has no key arguments" is
    // the exact lie moon#537 filed.
    let has_keys = moon::acl::keyspec::command_has_keys(&cmd, &args);
    if matches!(
        &positions,
        KeyPositions::At(_) | KeyPositions::AtPlusComputed(_)
    ) {
        assert!(
            has_keys,
            "walker named key positions but command_has_keys said no"
        );
    }
});
