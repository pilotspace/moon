//! moon#636 shape pin (source-grep): a privileged intercept must sit BELOW its
//! handler's ACL gate AND below its MULTI queue gate.
//!
//! Each connection handler answers a handful of commands *inline*, before the
//! generic dispatch, because they need something dispatch cannot give them —
//! `DEBUG DIGEST` needs every database and every shard, which no single-db
//! command context has. That is a legitimate reason to intercept. It is not a
//! reason to answer early.
//!
//! An intercept placed above the per-command ACL check answers before NOPERM is
//! ever considered, so a user explicitly denied the command still gets the
//! reply. This is the inline-GET ACL bypass class (v0.8.6 client-compat
//! review), and it is invisible to every functional test: the command works,
//! the digest is correct, and only a restricted user would ever notice.
//!
//! `DEBUG DIGEST` is the worst possible instance of it — a fingerprint of the
//! ENTIRE dataset, handed to a user who was denied `DEBUG`.
//!
//! The same intercept must also sit below the MULTI queue gate. Inside a
//! transaction the frame has to QUEUE, not answer: an intercept above the gate
//! replies with a 40-character digest where the protocol requires `+QUEUED`,
//! and the client's queue accounting desyncs — its `EXEC` array comes back one
//! element shorter than it queued. `handler_monoio` spells the rule out at its
//! own gate ("every non-transactional intercept MUST sit below this").
//!
//! Both bugs this pin was written against were real, and both were in
//! `handler_single`: the intercept first landed at line 1086 with the ACL gate
//! at ~1717, and after that was fixed it still sat at 1759 with the MULTI queue
//! gate at 1857.
//!
//! Source-grep in the style of `tests/cold_read_through_shape.rs` — reads `.rs`
//! files at runtime, no server, no feature flags.
//!
//! Running:  cargo test --test admin_intercept_after_acl_shape

use std::path::Path;

/// `(handler file, gate needle, privileged-intercept needle)`.
///
/// The three handlers gate differently — monoio delegates to
/// `dispatch::try_enforce_acl`, the other two call `check_command_permission`
/// inline — so the needle is per file rather than one shared string.
const ACL_GUARDED: &[(&str, &str, &str)] = &[
    (
        "src/server/conn/handler_monoio/mod.rs",
        "dispatch::try_enforce_acl(",
        "DEBUG DIGEST (moon#636)",
    ),
    (
        "src/server/conn/handler_sharded/mod.rs",
        "check_command_permission(",
        "DEBUG DIGEST (moon#636)",
    ),
    (
        "src/server/conn/handler_single.rs",
        "check_command_permission(",
        "DEBUG DIGEST (moon#636)",
    ),
];

/// The MULTI queue gate, per handler.
///
/// Matched on the gate's own banner comment rather than on `conn.in_multi`:
/// every handler reads that flag several times (SUBSCRIBE, EXEC, DISCARD, the
/// inline-write fast path), and the first textual hit is never the queue gate.
/// monoio and sharded share a banner; `handler_single` labels it differently.
const MULTI_GUARDED: &[(&str, &str, &str)] = &[
    (
        "src/server/conn/handler_monoio/mod.rs",
        "MULTI QUEUE GATE",
        "DEBUG DIGEST (moon#636)",
    ),
    (
        "src/server/conn/handler_sharded/mod.rs",
        "MULTI QUEUE GATE",
        "DEBUG DIGEST (moon#636)",
    ),
    (
        "src/server/conn/handler_single.rs",
        "--- MULTI queue mode ---",
        "DEBUG DIGEST (moon#636)",
    ),
];

fn first_line_containing(text: &str, needle: &str) -> Option<usize> {
    text.lines().position(|l| l.contains(needle)).map(|i| i + 1)
}

/// Shared body: assert `intercept` appears after `gate` in `file`.
///
/// `gate_after` skips gate matches before that line, so a table can name the
/// LAST relevant gate rather than the first textual hit.
fn assert_intercept_below(
    file: &str,
    gate: &str,
    intercept: &str,
    gate_after: usize,
    what: &str,
    consequence: &str,
) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(root.join(file))
        .unwrap_or_else(|e| panic!("{file} is unreadable: {e}"));

    // Both needles must exist, or the check is vacuously green: a renamed
    // gate or a deleted intercept would otherwise pass silently.
    let gate_line = text
        .lines()
        .enumerate()
        .find(|(i, l)| i + 1 > gate_after && l.contains(gate))
        .map(|(i, _)| i + 1)
        .unwrap_or_else(|| {
            panic!(
                "{file}: {what} marker {gate:?} not found after line \
                 {gate_after}. If the gate was renamed, update this pin — do \
                 not delete the row, or the ordering stops being checked at all."
            )
        });
    let intercept_line = first_line_containing(&text, intercept).unwrap_or_else(|| {
        panic!(
            "{file}: intercept marker {intercept:?} not found. If the \
             intercept moved to another file, move this row with it."
        )
    });

    assert!(
        intercept_line > gate_line,
        "{file}: the privileged intercept at line {intercept_line} sits ABOVE \
         the {what} at line {gate_line}.\n{consequence}\n\
         Move the intercept below the gate."
    );
}

#[test]
fn privileged_intercepts_sit_below_the_multi_queue_gate() {
    for (file, gate, intercept) in MULTI_GUARDED {
        assert_intercept_below(
            file,
            gate,
            intercept,
            0,
            "MULTI queue gate",
            "Inside a transaction the frame must QUEUE, not answer. An \
             intercept above the gate replies with a 40-character digest where \
             the protocol requires `+QUEUED`, so the client's queue accounting \
             desyncs and its EXEC array comes back one element short.",
        );
    }
}

#[test]
fn privileged_intercepts_sit_below_the_acl_gate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (file, gate, intercept) in ACL_GUARDED {
        let text = std::fs::read_to_string(root.join(file))
            .unwrap_or_else(|e| panic!("{file} is unreadable: {e}"));

        // Both needles must exist, or the check is vacuously green: a renamed
        // gate or a deleted intercept would otherwise pass silently.
        let gate_line = first_line_containing(&text, gate).unwrap_or_else(|| {
            panic!(
                "{file}: ACL-gate marker {gate:?} not found. If the gate was \
                 renamed, update this pin — do not delete the row, or the \
                 ordering stops being checked at all."
            )
        });
        let intercept_line = first_line_containing(&text, intercept).unwrap_or_else(|| {
            panic!(
                "{file}: intercept marker {intercept:?} not found. If the \
                 intercept moved to another file, move this row with it."
            )
        });

        assert!(
            intercept_line > gate_line,
            "{file}: the privileged intercept at line {intercept_line} sits \
             ABOVE the ACL gate at line {gate_line}.\n\
             It therefore answers before NOPERM is considered, so a user who \
             was explicitly denied the command still receives the reply.\n\
             `DEBUG DIGEST` returns a fingerprint of every key in every \
             database — an ACL bypass there leaks the shape of the whole \
             dataset.\n\
             Move the intercept below the gate."
        );
    }
}
