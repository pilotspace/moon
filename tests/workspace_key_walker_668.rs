//! The workspace key walker must be the SHARED key walker — moon#668.
//!
//! `src/workspace/mod.rs` rewrites a command's key positions to prefix them
//! with `{ws_hex}:`. Until moon#668 it did that with a SECOND, hand-rolled
//! walker: four hardcoded name lists plus a handful of special cases, with a
//! single-key default underneath. It had drifted from
//! `acl::keyspec::command_key_positions` — the shared walker moon#582
//! consolidated the ACL layer and cache-invalidation onto — in **50** places.
//!
//! # Why a drifted walker is a tenant-isolation break, not a cosmetic gap
//!
//! README sells workspaces as multi-tenant isolation, GA since v0.6.0. A key
//! position the walker misses is written or read **unprefixed** — i.e. in the
//! global keyspace — from inside a workspace. `SORT k STORE dst` in workspace A
//! wrote a global `dst` that workspace B could read and overwrite.
//!
//! The drift ran in both directions, and both are fenced here:
//!
//!   * **missed positions** → the key escaped the workspace. `BITOP` (every
//!     position), `SORT ... STORE`, `GEORADIUS*/... STORE|STOREDIST`,
//!     `ZRANGESTORE`/`GEOSEARCHSTORE`/`LCS` (the source), `BLPOP`/`BRPOP`/
//!     `BLMOVE`/`BZPOPMIN`/`TOUCH` (everything after the first key),
//!     `EVAL`/`EVALSHA` (all of `KEYS[]`), `OBJECT`/`XINFO`/`MEMORY` (the key
//!     behind the subcommand).
//!   * **invented positions** → the single-key default prefixed `args[0]` on
//!     commands whose `args[0]` is not a key, so the command simply broke
//!     inside a workspace: the `numkeys` of the `Z*`/`S*`/`*MPOP` families,
//!     `BITOP`'s operation, `XGROUP`'s subcommand, `FCALL`'s function name,
//!     `SCAN`'s cursor.
//!
//! # The instrument
//!
//! `wkw1` sweeps the WHOLE registry rather than a list somebody remembered to
//! extend — that is what turned "five *STORE destinations" (the issue's count,
//! from reading the file) into fifty (from measuring it). A curated corpus
//! (`wkw2`) covers what a synthetic argv cannot reach: every command whose key
//! positions depend on a parsed `numkeys`.

use bytes::Bytes;
use moon::acl::keyspec::{KeyPositions, command_key_positions};
use moon::command::metadata::COMMAND_META;
use moon::protocol::Frame;
use moon::workspace::{WorkspaceId, workspace_rewrite_args};

fn ws() -> WorkspaceId {
    WorkspaceId::from_bytes([0xab; 16])
}

fn bs(s: &str) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
}

fn argv(items: &[&str]) -> Vec<Frame> {
    items.iter().map(|s| bs(s)).collect()
}

fn rewrite(cmd: &str, args: &[Frame]) -> Vec<Frame> {
    match workspace_rewrite_args(cmd.as_bytes(), args, &ws()) {
        Ok(out) => out,
        Err(err) => panic!("{cmd} was refused inside a workspace: {err:?}"),
    }
}

/// Positions whose bytes the rewrite changed. Requires `out.len() == args.len()`
/// — a rewrite that appends (only `SCAN`, see `wkw4`) is compared differently.
fn changed(args: &[Frame], out: &[Frame]) -> Vec<usize> {
    assert_eq!(
        out.len(),
        args.len(),
        "the rewrite changed the argv LENGTH; only SCAN may do that"
    );
    (0..args.len())
        .filter(|&i| match (&out[i], &args[i]) {
            (Frame::BulkString(a), Frame::BulkString(b)) => a != b,
            _ => false,
        })
        .collect()
}

/// The positions the shared walker names, or `None` when it cannot enumerate.
fn keyspec_positions(cmd: &str, args: &[Frame]) -> Option<Vec<usize>> {
    match command_key_positions(cmd.as_bytes(), args) {
        KeyPositions::None => Some(vec![]),
        KeyPositions::At(idx) | KeyPositions::AtPlusComputed(idx) => {
            Some(idx.iter().map(|k| k.idx).collect())
        }
        KeyPositions::Unknown => None,
    }
}

/// The extra positions a WORKSPACE prefixes that the ACL walker does not name.
///
/// These are policy, not drift: index/graph names live outside the keyspace ACL
/// patterns cover, and a glob is not a key — but an unscoped one enumerates
/// every other workspace. Anything NOT in this set that the walker touches is
/// an invented position.
fn policy_positions(cmd: &str) -> Vec<usize> {
    let c = cmd.to_ascii_uppercase();
    if c == "FT._LIST" || c == "GRAPH.LIST" {
        return vec![];
    }
    if c.starts_with("FT.") || c.starts_with("GRAPH.") || c == "KEYS" {
        return vec![0];
    }
    vec![]
}

// ---------------------------------------------------------------------------
// wkw1 — the registry-wide sweep
// ---------------------------------------------------------------------------

#[test]
fn wkw1_every_registry_command_prefixes_exactly_the_shared_walkers_positions() {
    let mut names: Vec<&str> = COMMAND_META.keys().copied().collect();
    names.sort_unstable();

    let mut divergences = Vec::new();
    let mut checked = 0usize;
    for name in names {
        // SCAN is the one command whose rewrite may APPEND (it injects a
        // workspace-scoped MATCH). `wkw4` owns it.
        if name.eq_ignore_ascii_case("SCAN") {
            continue;
        }
        let meta = &COMMAND_META[name];
        // Arity counts the command name. An arity-1 command never carries an
        // argv, and `workspace_rewrite_args` returns early on an empty one —
        // handing it a synthetic argument would test a shape that cannot occur.
        if meta.arity == 1 {
            continue;
        }
        let want = if meta.arity > 0 {
            (meta.arity - 1) as usize
        } else {
            ((-meta.arity) - 1) as usize
        };
        let synth: Vec<String> = (0..want.clamp(1, 8)).map(|i| format!("a{i}")).collect();
        let synth_refs: Vec<&str> = synth.iter().map(String::as_str).collect();
        let args = argv(&synth_refs);
        // A synthetic argv cannot satisfy a `numkeys`-counted layout; those
        // commands are covered by the curated corpus in `wkw2` instead.
        let Some(mut expect) = keyspec_positions(name, &args) else {
            continue;
        };
        expect.extend(policy_positions(name));
        expect.sort_unstable();
        expect.dedup();

        checked += 1;
        let got = changed(&args, &rewrite(name, &args));
        if got != expect {
            divergences.push(format!(
                "{name:<20} keyspec+policy={expect:?} walker={got:?}"
            ));
        }
    }

    // A sweep that checked almost nothing would pass silently. Before moon#668
    // this same sweep reported 50 divergences over ~230 commands.
    assert!(
        checked > 150,
        "the sweep only reached {checked} commands — the corpus builder broke, \
         not the walker"
    );
    assert!(
        divergences.is_empty(),
        "{} commands prefix positions the shared walker does not name \
         (or miss ones it does):\n  {}",
        divergences.len(),
        divergences.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// wkw2 — real argvs, including every numkeys-counted layout
// ---------------------------------------------------------------------------

#[test]
fn wkw2_real_argvs_agree_with_the_shared_walker() {
    let corpus: &[(&str, &[&str])] = &[
        // --- destinations the old walker left in the GLOBAL keyspace ---
        ("BITOP", &["AND", "dst", "s1", "s2"]),
        ("PFMERGE", &["dst", "s1", "s2"]),
        ("ZRANGESTORE", &["dst", "src", "0", "-1"]),
        (
            "GEOSEARCHSTORE",
            &[
                "dst",
                "src",
                "FROMMEMBER",
                "m",
                "BYRADIUS",
                "1",
                "km",
                "ASC",
            ],
        ),
        ("GEORADIUS", &["src", "1", "1", "1", "km", "STORE", "dst"]),
        (
            "GEORADIUSBYMEMBER",
            &["src", "m", "1", "km", "STOREDIST", "dst"],
        ),
        ("SORT", &["k", "STORE", "dst"]),
        ("LCS", &["a", "b"]),
        // --- keys after the first, dropped by the single-key default ---
        ("BLPOP", &["a", "b", "0"]),
        ("BRPOP", &["a", "b", "0"]),
        ("BLMOVE", &["a", "b", "LEFT", "RIGHT", "0"]),
        ("BZPOPMIN", &["a", "b", "0"]),
        ("BRPOPLPUSH", &["a", "b", "0"]),
        ("TOUCH", &["a", "b"]),
        ("EXISTS", &["a", "b"]),
        ("WATCH", &["a", "b"]),
        // --- numkeys-counted vectors: args[0] is a COUNT, not a key ---
        ("ZUNIONSTORE", &["dst", "2", "a", "b"]),
        ("ZINTERSTORE", &["dst", "2", "a", "b"]),
        ("ZDIFFSTORE", &["dst", "2", "a", "b"]),
        ("ZUNION", &["2", "a", "b"]),
        ("ZINTER", &["2", "a", "b"]),
        ("ZDIFF", &["2", "a", "b"]),
        ("ZINTERCARD", &["2", "a", "b"]),
        ("SINTERCARD", &["2", "a", "b"]),
        ("LMPOP", &["2", "a", "b", "LEFT"]),
        ("ZMPOP", &["2", "a", "b", "MIN"]),
        ("BLMPOP", &["0", "2", "a", "b", "LEFT"]),
        ("BZMPOP", &["0", "2", "a", "b", "MIN"]),
        // --- scripts: KEYS[] was not prefixed AT ALL ---
        ("EVAL", &["return 1", "2", "a", "b"]),
        ("EVALSHA", &["deadbeef", "2", "a", "b"]),
        ("EVAL_RO", &["return 1", "1", "a"]),
        ("EVALSHA_RO", &["deadbeef", "1", "a"]),
        ("FCALL", &["fn", "1", "a"]),
        ("FCALL_RO", &["fn", "1", "a"]),
        // --- subcommand first, key second ---
        ("OBJECT", &["ENCODING", "k"]),
        ("XINFO", &["STREAM", "k"]),
        ("MEMORY", &["USAGE", "k"]),
        ("XGROUP", &["CREATE", "k", "g", "$"]),
        // --- keys after STREAMS ---
        ("XREAD", &["COUNT", "1", "STREAMS", "a", "b", "0", "0"]),
        ("XREADGROUP", &["GROUP", "g", "c", "STREAMS", "a", "0"]),
        // --- ordinary shapes, to prove nothing regressed ---
        ("GET", &["k"]),
        ("SET", &["k", "v"]),
        ("MGET", &["a", "b"]),
        ("MSET", &["a", "1", "b", "2"]),
        ("DEL", &["a", "b"]),
        ("RENAME", &["a", "b"]),
        ("COPY", &["a", "b"]),
        ("SMOVE", &["a", "b", "m"]),
        ("LMOVE", &["a", "b", "LEFT", "RIGHT"]),
        ("SINTERSTORE", &["dst", "a", "b"]),
        ("HSET", &["k", "f", "v"]),
        ("SETEX", &["k", "1", "v"]),
        // --- no keys at all: the old default prefixed args[0] anyway ---
        ("PUBLISH", &["ch", "m"]),
        ("SUBSCRIBE", &["ch"]),
        ("SPUBLISH", &["ch", "m"]),
        ("PUBSUB", &["CHANNELS"]),
        ("SELECT", &["1"]),
        ("ECHO", &["hi"]),
    ];

    let mut divergences = Vec::new();
    for (cmd, items) in corpus {
        let args = argv(items);
        let Some(mut expect) = keyspec_positions(cmd, &args) else {
            panic!("{cmd} {items:?}: the shared walker could not enumerate a WELL-FORMED argv");
        };
        expect.extend(policy_positions(cmd));
        expect.sort_unstable();
        expect.dedup();
        let got = changed(&args, &rewrite(cmd, &args));
        if got != expect {
            divergences.push(format!(
                "{cmd} {items:?}\n     keyspec+policy: {expect:?}\n     walker        : {got:?}"
            ));
        }
    }
    assert!(
        divergences.is_empty(),
        "{} divergences:\n  {}",
        divergences.len(),
        divergences.join("\n  ")
    );
}

// ---------------------------------------------------------------------------
// wkw3 — SORT's computed keys
// ---------------------------------------------------------------------------

/// `SORT k BY w_*` dereferences `w_<element>` once per element. The names are
/// only known at run time, so the shared walker reports them as
/// `AtPlusComputed` and ACL treats them as indeterminate. A workspace does not
/// have to: the pattern is expanded by plain `*` substitution, so prefixing the
/// PATTERN puts every expansion inside the workspace.
#[test]
fn wkw3_sort_by_and_get_patterns_are_prefixed_so_computed_keys_stay_inside() {
    let prefix = format!("{{{}}}:", "ab".repeat(16));

    let args = argv(&["k", "BY", "w_*", "GET", "p_*", "GET", "#", "STORE", "dst"]);
    let out = rewrite("SORT", &args);
    let s = |i: usize| match &out[i] {
        Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected BulkString at {i}, got {other:?}"),
    };
    assert_eq!(s(0), format!("{prefix}k"), "the source key");
    assert_eq!(s(1), "BY", "the keyword itself is never touched");
    assert_eq!(
        s(2),
        format!("{prefix}w_*"),
        "moon#668 — an unprefixed BY pattern dereferences GLOBAL keys from \
         inside a workspace"
    );
    assert_eq!(s(4), format!("{prefix}p_*"), "the GET pattern, likewise");
    assert_eq!(
        s(6),
        "#",
        "`GET #` means \"the element itself\" — prefixing it changes what the \
         command DOES, not where it looks"
    );
    assert_eq!(s(8), format!("{prefix}dst"), "the STORE destination");

    // `BY nosort` is a sentinel that means "skip sorting". Prefixing it turns
    // the sentinel into a key lookup that misses for every element.
    let args = argv(&["k", "BY", "nosort"]);
    let out = rewrite("SORT", &args);
    assert_eq!(changed(&args, &out), vec![0], "only the source key moves");
}

// ---------------------------------------------------------------------------
// wkw4 — SCAN
// ---------------------------------------------------------------------------

/// `SCAN` names no key, so the shared walker correctly reports none — but an
/// unscoped `SCAN` walks the whole keyspace and would hand a workspace-bound
/// client every OTHER workspace's keys. The cursor must never move (it is a
/// number); the glob must always be scoped, and injected when absent.
///
/// Before moon#668 the single-key default prefixed `args[0]` — the CURSOR —
/// so `SCAN` did not leak, it simply did not work.
#[test]
fn wkw4_scan_scopes_its_glob_and_never_its_cursor() {
    let prefix = format!("{{{}}}:", "ab".repeat(16));
    let text = |f: &Frame| match f {
        Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected BulkString, got {other:?}"),
    };

    // No MATCH: one is injected, scoped to the workspace.
    let args = argv(&["0"]);
    let out = rewrite("SCAN", &args);
    assert_eq!(
        out.iter().map(&text).collect::<Vec<_>>(),
        vec!["0".to_string(), "MATCH".into(), format!("{prefix}*")],
        "moon#668 — an unscoped SCAN enumerates every other workspace"
    );

    // MATCH present: its glob is prefixed, everything else is left alone.
    let args = argv(&["0", "MATCH", "user:*", "COUNT", "100"]);
    let out = rewrite("SCAN", &args);
    assert_eq!(
        out.iter().map(&text).collect::<Vec<_>>(),
        vec![
            "0".to_string(),
            "MATCH".into(),
            format!("{prefix}user:*"),
            "COUNT".into(),
            "100".into()
        ]
    );

    // MATCH after another option pair is still found.
    let args = argv(&["17", "COUNT", "10", "MATCH", "*"]);
    let out = rewrite("SCAN", &args);
    assert_eq!(text(&out[0]), "17", "the cursor is not a key");
    assert_eq!(text(&out[4]), format!("{prefix}*"));

    // HSCAN/SSCAN/ZSCAN match FIELD names inside one key, not key names —
    // their MATCH must NOT be prefixed, and their key must.
    for cmd in ["HSCAN", "SSCAN", "ZSCAN"] {
        let args = argv(&["k", "0", "MATCH", "f*"]);
        let out = rewrite(cmd, &args);
        assert_eq!(
            changed(&args, &out),
            vec![0],
            "{cmd} scans FIELDS of one key; only the key is workspace-scoped"
        );
    }
}

// ---------------------------------------------------------------------------
// wkw5 / wkw6 — the two ways enumeration can fail
// ---------------------------------------------------------------------------

/// An argv whose key positions cannot be determined must be REFUSED.
///
/// Passing it through would run it against the global keyspace, which is the
/// isolation break the rewrite exists to prevent; guessing a position would
/// corrupt an argument instead. In practice this is only reachable through a
/// malformed argv the command itself would have rejected a moment later.
#[test]
fn wkw5_an_indeterminate_argv_is_refused_not_run_globally() {
    for (cmd, items) in [
        ("ZUNIONSTORE", vec!["dst", "notanumber", "a", "b"]),
        ("LMPOP", vec!["9999", "a", "LEFT"]),
        ("EVAL", vec!["return 1", "-1", "a"]),
        ("XREAD", vec!["COUNT", "1"]),
    ] {
        let args = argv(&items);
        match workspace_rewrite_args(cmd.as_bytes(), &args, &ws()) {
            Err(Frame::Error(msg)) => {
                let msg = String::from_utf8_lossy(&msg);
                assert!(
                    msg.contains("workspace") && msg.contains(cmd),
                    "the refusal must name the workspace and the command; got {msg:?}"
                );
            }
            Ok(out) => panic!(
                "moon#668 — {cmd} {items:?} has indeterminate key positions but was \
                 rewritten to {out:?} instead of refused"
            ),
            Err(other) => panic!("expected an Error frame, got {other:?}"),
        }
    }
}

/// An UNREGISTERED name is not an indeterminate argv — dispatch is about to
/// answer `unknown command`, which is a better error than anything this layer
/// could invent, so it passes through untouched.
#[test]
fn wkw6_an_unknown_command_passes_through_rather_than_being_refused() {
    let args = argv(&["a", "b"]);
    let out = rewrite("NOSUCHCOMMAND", &args);
    assert_eq!(out, args);
}
