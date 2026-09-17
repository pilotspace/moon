//! Guard for `CommandFlags::NO_INTERCEPT`.
//!
//! `NO_INTERCEPT` lets the monoio connection handler skip its 20 unguarded
//! intercept gates — 12 of which are `async fn`s whose futures are built and
//! polled only to return `false` — for ordinary keyspace commands. Marking a
//! command that an intercept *can* claim would silently stop that intercept
//! from running, which is a correctness bug no throughput test would notice.
//!
//! The bit is therefore deliberately inverted: an unmarked command takes the
//! slow path and stays correct, so adding a command or an intercept can only
//! cost speed, never correctness. This test guards the other direction — that
//! nothing currently marked is claimed by a gate.
//!
//! # What this test can and cannot prove
//!
//! It scans the **top-level guard region** of every `try_*` gate — the prefix
//! of the function body before it starts parsing subcommands — and collects the
//! command names those guards match. That region is where a gate decides "not
//! mine, return false", so it is the right place to look; names appearing later
//! are subcommands (`CLIENT KILL`, `FT.CONFIG SET`) and must NOT count, or the
//! test would refuse to mark `SET` because `FT.CONFIG SET` exists.
//!
//! It cannot prove a gate written in some *other* shape is caught. That is why
//! the bit is inverted rather than this test being the only line of defence.

use std::collections::BTreeSet;
use std::path::PathBuf;

/// Files holding the connection handler's intercept gates. Kept in step with
/// `GATE_FILES` in `src/server/conn/shared.rs`'s tests.
///
/// `watch.rs`: `try_handle_multi_exec` delegates `WATCH`/`UNWATCH` to
/// `watch::try_handle_watch_unwatch`, whose name literals live outside the
/// handler directory — the moon#946 blind spot, for a routable command.
const GATE_FILES: [&str; 6] = [
    "src/server/conn/handler_monoio/dispatch.rs",
    "src/server/conn/handler_monoio/write.rs",
    "src/server/conn/handler_monoio/txn.rs",
    "src/server/conn/handler_monoio/pubsub.rs",
    "src/server/conn/handler_monoio/ft.rs",
    "src/server/conn/watch.rs",
];

/// Commands a gate claims through a PREDICATE rather than a literal in its own
/// body — `command::transaction::is_txn_*`, `command::temporal::is_temporal_*`,
/// `server::conn::blocking::is_blocking_command_args` — so the text scan
/// below cannot see them. The unit test
/// `no_marked_command_fires_a_delegated_gate_predicate` in `shared.rs` drives
/// the real predicates (they are crate-private); this is the integration-side
/// copy, by name, so a mark on any of them fails here too.
///
/// `XREAD`/`XREADGROUP` block only with `BLOCK`, but the gate claims them by
/// argv, and `NO_INTERCEPT` is per NAME — so neither may ever carry it.
/// `MQ`/`WS` are claimed through `mq::is_mq_command` / `workspace::is_ws_command`.
const DELEGATED_GATE_COMMANDS: [&str; 15] = [
    "TXN",
    "TEMPORAL.SNAPSHOT_AT",
    "TEMPORAL.INVALIDATE",
    "MQ",
    "WS",
    "BLPOP",
    "BRPOP",
    "BLMOVE",
    "BRPOPLPUSH",
    "BZPOPMIN",
    "BZPOPMAX",
    "BLMPOP",
    "BZMPOP",
    "XREAD",
    "XREADGROUP",
];

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR, never a hardcoded path: a literal that happens to
    // exist on one machine makes this test pass locally and vanish on CI.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every command name matched by a gate's top-level guard.
fn names_claimed_by_gates() -> BTreeSet<String> {
    let mut claimed = BTreeSet::new();
    for rel in GATE_FILES {
        let path = repo_root().join(rel);
        // Normalise line endings before scanning. The guard window below is a
        // BYTE budget, and a CRLF checkout (git's default on Windows) spends
        // one extra byte per line — enough to push the last claim of a long
        // guard past the cap. That dropped FUNCTION and GRAPH.QUERY on
        // `Check (Windows)` while every LF platform kept them, i.e. the
        // verdict depended on the checkout's line endings, not on the code
        // this audits.
        let src = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read gate file {}: {e}", path.display()))
            .replace("\r\n", "\n");

        for (idx, _) in src.match_indices("fn try_") {
            let body = &src[idx..];
            // The guard region ends where the gate stops deciding whether the
            // command is its own and starts doing work.
            let end = [
                "_inner(",
                "parse_mq_subcommand",
                "parse_ws_subcommand",
                "let sub",
            ]
            .iter()
            .filter_map(|m| body.find(m))
            .min()
            .unwrap_or(body.len().min(1200))
            .min(1200);
            let guard = &body[..end];

            for pat in ["eq_ignore_ascii_case(b\"", "starts_with(b\""] {
                let mut rest = guard;
                while let Some(p) = rest.find(pat) {
                    let after = &rest[p + pat.len()..];
                    if let Some(q) = after.find('"') {
                        claimed.insert(after[..q].to_ascii_uppercase());
                    }
                    rest = &after[1..];
                }
            }
        }
    }
    claimed
}

/// Commands marked `NO_INTERCEPT` in the static registry.
fn names_marked_no_intercept() -> BTreeSet<String> {
    moon::command::metadata::COMMAND_META
        .entries()
        .filter(|(_, m)| {
            m.flags
                .contains(moon::command::metadata::CommandFlags::NO_INTERCEPT)
        })
        .map(|(k, _)| (*k).to_string())
        .collect()
}

#[test]
fn no_marked_command_is_claimed_by_an_intercept_gate() {
    let claimed = names_claimed_by_gates();
    assert!(
        claimed.len() > 20,
        "the gate scan found only {} names — the extraction broke, and a broken \
         extraction would make this test pass vacuously. Found: {claimed:?}",
        claimed.len()
    );

    let marked = names_marked_no_intercept();
    assert!(
        !marked.is_empty(),
        "no command carries NO_INTERCEPT — either the flag was dropped or the \
         registry lost its markings; this test would then prove nothing"
    );

    let overlap: Vec<_> = marked.intersection(&claimed).cloned().collect();
    assert!(
        overlap.is_empty(),
        "these commands are marked NO_INTERCEPT but an intercept gate claims \
         them, so the gate would be silently skipped: {overlap:?}\n\
         Either drop the NO_INTERCEPT flag from them in COMMAND_META, or, if \
         the gate no longer claims them, update the gate."
    );
}

/// Dotted families are handled by prefix guards (`FT.`, `GRAPH.`, `CDC.`,
/// `TS.`), which the name scan above cannot express. Assert directly that none
/// of them is marked.
#[test]
fn no_dotted_family_command_is_marked_no_intercept() {
    let bad: Vec<_> = names_marked_no_intercept()
        .into_iter()
        .filter(|n| n.contains('.'))
        .collect();
    assert!(
        bad.is_empty(),
        "dotted commands are claimed by prefix-guarded gates (FT./GRAPH./CDC./TS.) \
         and must never be marked NO_INTERCEPT: {bad:?}"
    );
}

/// No command claimed through a delegated predicate is marked — the scan
/// cannot see those gates, so this is asserted by name (moon#946).
#[test]
fn no_delegated_gate_command_is_marked_no_intercept() {
    let marked = names_marked_no_intercept();
    let registry: BTreeSet<&str> = moon::command::metadata::COMMAND_META
        .entries()
        .map(|(k, _)| *k)
        .collect();
    for cmd in DELEGATED_GATE_COMMANDS {
        assert!(
            registry.contains(cmd),
            "{cmd} is listed as a delegated-gate command but is not in \
             COMMAND_META — the list rotted and the row would test nothing"
        );
    }
    let bad: Vec<_> = DELEGATED_GATE_COMMANDS
        .iter()
        .filter(|c| marked.contains(**c))
        .collect();
    assert!(
        bad.is_empty(),
        "claimed by a gate through a predicate the scan cannot see, yet marked \
         NO_INTERCEPT — the handler would skip that gate: {bad:?}"
    );
}

/// The flag must actually reach the commands the optimisation targets — a bit
/// nobody sets is a no-op that still looks green.
#[test]
fn the_hot_keyspace_commands_are_marked() {
    let marked = names_marked_no_intercept();
    for cmd in ["GET", "SET", "INCR", "HSET", "LPUSH", "SPOP"] {
        assert!(
            marked.contains(cmd),
            "{cmd} is not marked NO_INTERCEPT, so it still walks all 20 gates"
        );
    }
}
