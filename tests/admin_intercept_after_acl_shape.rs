//! moon#636 shape pin (source-grep): a privileged intercept must sit BELOW its
//! handler's ACL gate.
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
//! The bug this pin was written against was real: the `handler_single`
//! intercept landed at line 1086, and that handler's ACL gate is at ~1717.
//!
//! Source-grep in the style of `tests/cold_read_through_shape.rs` — reads `.rs`
//! files at runtime, no server, no feature flags.
//!
//! Running:  cargo test --test admin_intercept_after_acl_shape

use std::path::Path;

/// `(handler file, ACL-gate needle, privileged-intercept needle)`.
///
/// The three handlers gate differently — monoio delegates to
/// `dispatch::try_enforce_acl`, the other two call `check_command_permission`
/// inline — so the needle is per file rather than one shared string.
const GUARDED: &[(&str, &str, &str)] = &[
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

fn first_line_containing(text: &str, needle: &str) -> Option<usize> {
    text.lines().position(|l| l.contains(needle)).map(|i| i + 1)
}

#[test]
fn privileged_intercepts_sit_below_the_acl_gate() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for (file, gate, intercept) in GUARDED {
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
