//! `COMMAND` — the introspection surface, rendered from `COMMAND_META`.
//!
//! This module exists because the previous implementation
//! (`connection::command`, sixteen lines) answered from constants: bare
//! `COMMAND` replied `Integer(0)` and `COMMAND COUNT` replied an empty array —
//! each returning the OTHER'S RESP type. A driver that builds its command map
//! at startup does not read that as "unsupported"; it reads it as a protocol
//! violation.
//!
//! Everything here derives from `COMMAND_META`, deliberately. The registry
//! already carries name, arity, flags, key positions and ACL categories, so
//! registering a command is what makes it introspectable — there is no second
//! table to keep in sync, which is the drift this task exists to close.
//!
//! These are cold paths: a client calls them at connect time, not per
//! operation. Building result vectors with `Vec::with_capacity` here is fine
//! and is what the allocation rule permits for result building.

use bytes::Bytes;

use crate::command::metadata::{AclCategories, COMMAND_META, CommandFlags, CommandMeta};
use crate::framevec;
use crate::protocol::Frame;

/// Flag name as Redis spells it on the wire, paired with its bit.
const FLAG_NAMES: &[(CommandFlags, &str)] = &[
    (CommandFlags::WRITE, "write"),
    (CommandFlags::READONLY, "readonly"),
    (CommandFlags::FAST, "fast"),
    (CommandFlags::ADMIN, "admin"),
    (CommandFlags::PUBSUB, "pubsub"),
    (CommandFlags::NOSCRIPT, "noscript"),
    (CommandFlags::LOADING, "loading"),
    (CommandFlags::STALE, "stale"),
    (CommandFlags::SKIP_MONITOR, "skip_monitor"),
    (CommandFlags::ASKING, "asking"),
    (CommandFlags::NO_AUTH, "no-auth"),
    (CommandFlags::MAY_REPLICATE, "may_replicate"),
    (CommandFlags::SORT_FOR_SCRIPT, "sort_for_script"),
    (CommandFlags::NO_MANDATORY_KEYS, "no_mandatory_keys"),
];

/// ACL category name as Redis spells it (`@`-prefixed on the wire).
const CATEGORY_NAMES: &[(AclCategories, &str)] = &[
    (AclCategories::KEYSPACE, "@keyspace"),
    (AclCategories::READ_CAT, "@read"),
    (AclCategories::WRITE_CAT, "@write"),
    (AclCategories::SET, "@set"),
    (AclCategories::SORTEDSET, "@sortedset"),
    (AclCategories::LIST, "@list"),
    (AclCategories::HASH, "@hash"),
    (AclCategories::STRING, "@string"),
    (AclCategories::STREAM, "@stream"),
    (AclCategories::PUBSUB, "@pubsub"),
    (AclCategories::GENERIC, "@generic"),
    (AclCategories::TRANSACTIONS, "@transaction"),
    (AclCategories::SCRIPTING, "@scripting"),
    (AclCategories::CONNECTION, "@connection"),
    (AclCategories::SERVER, "@server"),
    (AclCategories::DANGEROUS, "@dangerous"),
    (AclCategories::SLOW, "@slow"),
    (AclCategories::FAST_CAT, "@fast"),
    (AclCategories::SEARCH, "@search"),
    (AclCategories::GRAPH, "@graph"),
];

/// The 10-field spec Redis 7+ emits.
///
/// The last three (tips, key_specs, subcommands) are emitted EMPTY rather than
/// omitted: that is what redis-server itself does for a command that has none,
/// and a client indexing by position needs the field to exist. Emitting six
/// fields would make every modern driver's key-spec lookup fall off the end.
fn spec_frame(meta: &CommandMeta) -> Frame {
    let mut flags = crate::protocol::FrameVec::new();
    for (bit, name) in FLAG_NAMES {
        if meta.flags.contains(*bit) {
            flags.push(Frame::SimpleString(Bytes::from_static(name.as_bytes())));
        }
    }
    let mut cats = crate::protocol::FrameVec::new();
    for (bit, name) in CATEGORY_NAMES {
        if meta.acl_categories.contains(*bit) {
            cats.push(Frame::SimpleString(Bytes::from_static(name.as_bytes())));
        }
    }
    // Five of the ten members are Sets on RESP3 — flags, acl-categories, tips,
    // key-specs, subcommands — measured on redis 8.6.1. The row itself stays
    // an Array. RESP2 clients are unaffected: the serializer downgrades a Set
    // to an Array, which is exactly what they were already receiving
    // (moon#631; item 3 of the `identity_command_info_known_and_unknown`
    // client-compat waiver).
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from(meta.name.to_ascii_lowercase())),
        Frame::Integer(meta.arity as i64),
        Frame::Set(flags),
        Frame::Integer(meta.first_key as i64),
        Frame::Integer(meta.last_key as i64),
        Frame::Integer(meta.step as i64),
        Frame::Set(cats),
        Frame::Set(framevec![]), // tips
        Frame::Set(framevec![]), // key specs
        Frame::Set(framevec![]), // subcommands
    ])
}

/// Every registered command's spec. Order is the registry's iteration order,
/// which Redis does not promise either.
fn all_specs() -> crate::protocol::FrameVec {
    let mut out = crate::protocol::FrameVec::with_capacity(COMMAND_META.len());
    for meta in COMMAND_META.values() {
        out.push(spec_frame(meta));
    }
    out
}

fn err(msg: &'static str) -> Frame {
    Frame::Error(Bytes::from_static(msg.as_bytes()))
}

/// Extract the key arguments of `argv`, where `argv[0]` is the command name.
///
/// # moon#537
///
/// This used to answer from `meta.first_key <= 0`, which is true of every
/// **movablekeys** command (`LMPOP`, `ZMPOP`, `SINTERCARD`, `XREADGROUP`,
/// `EVAL`, `ZDIFF`, `SORT ... STORE`, ...). In redis's table — which moon
/// mirrors — that means "the keys are not at a FIXED argument position", NOT
/// "there are no keys", so moon told every one of those callers
/// `ERR The command has no key arguments`. `COMMAND GETKEYS` is precisely how
/// a cluster-aware client routes a command it cannot parse itself, so the
/// answer was both wrong and self-evidently a lie.
///
/// It now delegates to the SHARED key walker ([`crate::acl::keyspec`]) — the
/// same one ACL key patterns and client-side cache invalidation consume — so
/// the three answers cannot drift apart again.
///
/// # Reply shapes
///
/// Verified against `redis-server 8.6.1`; the four error strings and their
/// ORDER are redis's, and the order is observable: `COMMAND GETKEYS SELECT`
/// reports no-keys rather than wrong-arity even though its arity is also
/// wrong.
///
/// 1. unknown command → `Invalid command specified`
/// 2. the command names no keys AT ALL → `The command has no key arguments`
/// 3. wrong arity → `Invalid number of arguments specified for command`
/// 4. this argv names no key → `Invalid arguments specified for command`,
///    except for `no-mandatory-keys` commands (`EVAL` and friends), whose
///    key COUNT is an argument and which reply with an empty array.
fn getkeys(argv: &[Frame]) -> Frame {
    let Some(name) = argv.first().and_then(extract) else {
        return err("ERR Unknown subcommand or wrong number of arguments for 'GETKEYS'");
    };
    // `lookup` uppercases internally — no need to allocate an upper copy.
    let Some(meta) = crate::command::metadata::lookup(&name) else {
        return err("ERR Invalid command specified");
    };

    // The walker's contract: `args` EXCLUDES the command name.
    let args = &argv[1..];

    // (2) A STATIC property of the command, checked before arity — see the
    // `SELECT` case above. Container commands (`MEMORY USAGE` vs
    // `MEMORY STATS`) resolve through the subcommand, which is why the argv
    // is passed.
    if !crate::acl::keyspec::command_has_keys(&name, args) {
        return err("ERR The command has no key arguments");
    }

    // (3) Arity: positive = exact, negative = minimum. argv includes the name.
    let n = argv.len() as i16;
    let arity_ok = if meta.arity >= 0 {
        n == meta.arity
    } else {
        n >= -meta.arity
    };
    if !arity_ok {
        return err("ERR Invalid number of arguments specified for command");
    }

    let unextractable = if meta
        .flags
        .contains(crate::command::metadata::CommandFlags::NO_MANDATORY_KEYS)
    {
        // `EVAL <script> 0` names no key, and redis answers an empty array
        // rather than an error — it also does so for a count this argv cannot
        // satisfy, so both walker verdicts land here.
        Frame::Array(crate::protocol::FrameVec::new())
    } else {
        err("ERR Invalid arguments specified for command")
    };

    let idx = match crate::acl::keyspec::command_key_positions(&name, args) {
        // `AtPlusComputed` is `SORT k BY w_*` (and a dangling `STORE`): some
        // key name is computed at runtime and cannot be reported, but redis
        // reports the ones that ARE named and so must moon. ACL takes the
        // opposite view of the same value and denies the argv — the walker
        // reports facts, each consumer applies its own policy.
        crate::acl::keyspec::KeyPositions::At(idx)
        | crate::acl::keyspec::KeyPositions::AtPlusComputed(idx) => idx,
        crate::acl::keyspec::KeyPositions::None | crate::acl::keyspec::KeyPositions::Unknown => {
            return unextractable;
        }
    };

    let mut keys = crate::protocol::FrameVec::with_capacity(idx.len());
    for k in idx {
        match args.get(k.idx).and_then(extract) {
            Some(b) => keys.push(Frame::BulkString(b)),
            // A key position holding a non-string is a malformed invocation.
            None => return unextractable,
        }
    }
    if keys.is_empty() {
        return unextractable;
    }
    Frame::Array(keys)
}

/// One `COMMAND DOCS` entry: the lower-cased name, and the doc Map for it.
///
/// Minimal but SHAPE-correct. Redis clients parse the shape to build
/// help/command maps; thin summary text is acceptable, a wrong shape is not.
///
/// `arity` is deliberately ABSENT. Redis carries arity in `COMMAND INFO` and
/// never in `COMMAND DOCS` — measured on 8.6.1, whose doc map holds only
/// summary, since, group, complexity and arguments. A client that builds its
/// command table from `DOCS` reads by name, so a field Redis does not define
/// there is not extra information; it is a field that means something else
/// everywhere it does exist (moon#631).
///
/// `group`, `complexity` and `arguments` are not emitted either: Moon's
/// registry does not carry them. Omitting a field it has no value for is the
/// honest gap; inventing one is not.
fn docs_for(meta: &CommandMeta) -> (Frame, Frame) {
    let arity_note = if meta.arity < 0 {
        format!("{} (variadic, minimum {})", meta.name, -meta.arity)
    } else {
        format!("{} (arity {})", meta.name, meta.arity)
    };
    (
        Frame::BulkString(Bytes::from(meta.name.to_ascii_lowercase())),
        Frame::Map(vec![
            (
                Frame::BulkString(Bytes::from_static(b"summary")),
                Frame::BulkString(Bytes::from(arity_note)),
            ),
            (
                Frame::BulkString(Bytes::from_static(b"since")),
                Frame::BulkString(Bytes::from_static(b"1.0.0")),
            ),
        ]),
    )
}

fn extract(f: &Frame) -> Option<Bytes> {
    match f {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// `COMMAND` and its subcommands, answered from the registry.
pub fn command(args: &[Frame]) -> Frame {
    // Bare COMMAND: one spec per registered command.
    let Some(sub) = args.first().and_then(extract) else {
        return Frame::Array(all_specs());
    };

    if sub.eq_ignore_ascii_case(b"COUNT") {
        if args.len() != 1 {
            return err("ERR wrong number of arguments for 'command|count' command");
        }
        return Frame::Integer(crate::command::metadata::command_count() as i64);
    }

    if sub.eq_ignore_ascii_case(b"LIST") {
        // FILTERBY is not supported; reject rather than silently ignore the
        // filter and hand back the unfiltered list as if it had been applied.
        if args.len() != 1 {
            return err("ERR Unknown subcommand or wrong number of arguments for 'LIST'");
        }
        let mut out = crate::protocol::FrameVec::with_capacity(COMMAND_META.len());
        for meta in COMMAND_META.values() {
            out.push(Frame::BulkString(Bytes::from(
                meta.name.to_ascii_lowercase(),
            )));
        }
        return Frame::Array(out);
    }

    if sub.eq_ignore_ascii_case(b"INFO") {
        // No names => every command, same as bare COMMAND.
        if args.len() == 1 {
            return Frame::Array(all_specs());
        }
        let mut out = crate::protocol::FrameVec::with_capacity(args.len() - 1);
        for a in &args[1..] {
            let spec = extract(a)
                .and_then(|n| crate::command::metadata::lookup(&n))
                .map(spec_frame)
                // An unknown name is a NULL ELEMENT inside the array, not a
                // skipped entry: the reply is positional, so dropping it would
                // silently misalign every name after it.
                .unwrap_or(Frame::Null);
            out.push(spec);
        }
        return Frame::Array(out);
    }

    if sub.eq_ignore_ascii_case(b"DOCS") {
        // A Map keyed by command name, which is what redis 8.6.1 sends on
        // RESP3; the serializer downgrades it to the flat `[name, doc, …]`
        // array RESP2 clients expect, so this is one construction for both
        // protocols (moon#631).
        let mut out: Vec<(Frame, Frame)> = Vec::new();
        if args.len() == 1 {
            out.reserve(COMMAND_META.len());
            for meta in COMMAND_META.values() {
                out.push(docs_for(meta));
            }
        } else {
            for a in &args[1..] {
                if let Some(meta) = extract(a).and_then(|n| crate::command::metadata::lookup(&n)) {
                    out.push(docs_for(meta));
                }
                // Redis omits unknown names from DOCS entirely (unlike INFO,
                // which is positional and uses a null element).
            }
        }
        return Frame::Map(out);
    }

    if sub.eq_ignore_ascii_case(b"GETKEYS") {
        if args.len() < 2 {
            return err("ERR Unknown subcommand or wrong number of arguments for 'GETKEYS'");
        }
        // Pass the FRAMES through: filtering to `Bytes` here would silently
        // drop a non-string argument and shift every key position after it.
        return getkeys(&args[1..]);
    }

    Frame::Error(Bytes::from(format!(
        "ERR Unknown subcommand '{}'. Try COMMAND HELP.",
        String::from_utf8_lossy(&sub)
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.to_string()))
    }

    #[test]
    fn count_is_an_integer_equal_to_the_registry() {
        let n = crate::command::metadata::command_count() as i64;
        assert!(matches!(command(&[bulk("COUNT")]), Frame::Integer(v) if v == n));
    }

    #[test]
    fn bare_command_yields_one_spec_per_registered_command() {
        let Frame::Array(specs) = command(&[]) else {
            panic!("bare COMMAND must be an array");
        };
        assert_eq!(specs.len(), crate::command::metadata::command_count());
    }

    #[test]
    fn every_spec_has_ten_fields() {
        let Frame::Array(specs) = command(&[]) else {
            panic!("array");
        };
        for s in &specs {
            let Frame::Array(fields) = s else {
                panic!("spec must be an array");
            };
            assert_eq!(fields.len(), 10, "spec must carry all 10 fields");
        }
    }

    #[test]
    fn info_is_positional_and_nulls_unknown_names() {
        let Frame::Array(out) = command(&[bulk("INFO"), bulk("GET"), bulk("nope"), bulk("SET")])
        else {
            panic!("array");
        };
        assert_eq!(out.len(), 3, "one element per requested name");
        assert!(
            matches!(out[1], Frame::Null),
            "unknown name is a null ELEMENT"
        );
    }

    /// The three probes from moon#469, pinned as ONE test so the surfaces
    /// cannot drift apart again. The defect was reply TYPE, not value: the old
    /// `connection::command` stub answered `*0` to `COMMAND COUNT` (where an
    /// Integer belongs) and `*0` to `COMMAND INFO`/`DOCS` of a command that
    /// dispatch handles perfectly well — a driver sizing a capability probe
    /// reads that as "zero commands", not "unimplemented".
    ///
    /// `WATCH` is the name the issue measured, chosen because `ACL CAT
    /// transaction` already listed it, so the two introspection surfaces
    /// disagreed with each other as well as with dispatch.
    #[test]
    fn issue_469_probes_are_typed_and_non_empty() {
        assert!(
            matches!(command(&[bulk("COUNT")]), Frame::Integer(n) if n > 0),
            "COMMAND COUNT must be a non-zero Integer, not an array"
        );
        let Frame::Array(info) = command(&[bulk("INFO"), bulk("WATCH")]) else {
            panic!("COMMAND INFO must be an array");
        };
        assert_eq!(info.len(), 1, "one spec per requested name");
        assert!(
            !matches!(info[0], Frame::Null),
            "WATCH is dispatchable and ACL-categorised, so its spec must exist"
        );
        // A Map keyed by command name since moon#631 — that is what redis
        // 8.6.1 sends on RESP3, and the serializer still hands RESP2 clients
        // the flat `[name, doc]` pair this used to assert on directly.
        let Frame::Map(docs) = command(&[bulk("DOCS"), bulk("WATCH")]) else {
            panic!("COMMAND DOCS must be a map keyed by command name");
        };
        assert_eq!(docs.len(), 1, "one name -> doc entry per requested command");
        assert_eq!(docs[0].0, bulk("watch"), "keyed by the lower-cased name");
        assert!(
            matches!(docs[0].1, Frame::Map(_)),
            "each doc is itself a map: {:?}",
            docs[0].1
        );
    }

    #[test]
    fn count_rejects_extra_arguments() {
        let r = command(&[bulk("COUNT"), bulk("extra")]);
        let Frame::Error(e) = r else {
            panic!("must be an error")
        };
        assert!(String::from_utf8_lossy(&e).contains("command|count"));
    }

    #[test]
    fn getkeys_uses_the_registry_key_spec() {
        let r = command(&[
            bulk("GETKEYS"),
            bulk("MSET"),
            bulk("k1"),
            bulk("v1"),
            bulk("k2"),
            bulk("v2"),
        ]);
        let Frame::Array(keys) = r else {
            panic!("array")
        };
        assert_eq!(keys.len(), 2, "MSET keys are every other argument");
    }

    #[test]
    fn getkeys_rejects_a_keyless_command() {
        let Frame::Error(e) = command(&[bulk("GETKEYS"), bulk("PING")]) else {
            panic!("keyless GETKEYS must be an error, never an empty array");
        };
        assert!(String::from_utf8_lossy(&e).contains("no key arguments"));
    }

    #[test]
    fn getkeys_rejects_an_unregistered_command() {
        let Frame::Error(e) = command(&[bulk("GETKEYS"), bulk("NOSUCHCMD"), bulk("k")]) else {
            panic!("must be an error");
        };
        assert!(String::from_utf8_lossy(&e).contains("Invalid command"));
    }

    #[test]
    fn getkeys_rejects_a_short_argv() {
        // SET has arity -3; two argv entries is one short.
        let Frame::Error(e) = command(&[bulk("GETKEYS"), bulk("SET"), bulk("k")]) else {
            panic!("must be an error");
        };
        assert!(String::from_utf8_lossy(&e).contains("Invalid number of arguments"));
    }

    // ── moon#537: COMMAND GETKEYS for movablekeys commands ────────────────

    /// Run `COMMAND GETKEYS <parts...>` and render the reply as either the key
    /// list or the error text, so a test case can be written the way the wire
    /// shows it.
    fn getkeys_of(parts: &[&str]) -> Result<Vec<String>, String> {
        let mut argv = vec![bulk("GETKEYS")];
        argv.extend(parts.iter().map(|p| bulk(p)));
        match command(&argv) {
            Frame::Array(keys) => Ok(keys
                .iter()
                .map(|f| match f {
                    Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    other => panic!("key must be a bulk string, got {other:?}"),
                })
                .collect()),
            Frame::Error(e) => Err(String::from_utf8_lossy(&e).into_owned()),
            other => panic!("GETKEYS must reply an array or an error, got {other:?}"),
        }
    }

    /// moon#537. Every command here carries `first_key: 0` in `COMMAND_META`,
    /// mirroring redis's own table, where it means "the keys are not at a
    /// FIXED argument position" — NOT "there are no keys". `getkeys()` read it
    /// as the latter and answered `ERR The command has no key arguments` to
    /// the entire movablekeys family. `COMMAND GETKEYS` is how a cluster-aware
    /// client routes a command it cannot parse itself, so it either refused to
    /// route or guessed; and the error text was itself false.
    ///
    /// Expectations are the measured replies of `redis-server 8.6.1`.
    #[test]
    fn issue_537_movablekeys_commands_report_their_keys() {
        // The exact table from the issue.
        assert_eq!(
            getkeys_of(&["LMPOP", "1", "k", "LEFT"]),
            Ok(vec!["k".into()])
        );
        assert_eq!(
            getkeys_of(&["ZMPOP", "1", "k", "MIN"]),
            Ok(vec!["k".into()])
        );
        assert_eq!(getkeys_of(&["SINTERCARD", "1", "k"]), Ok(vec!["k".into()]));
        assert_eq!(
            getkeys_of(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]),
            Ok(vec!["s".into()])
        );
        assert_eq!(
            getkeys_of(&["EVAL", "return 1", "1", "k"]),
            Ok(vec!["k".into()])
        );
        assert_eq!(getkeys_of(&["ZDIFF", "1", "k"]), Ok(vec!["k".into()]));
        // The control from the issue: fixed-position commands still work.
        assert_eq!(
            getkeys_of(&["MSET", "a", "1", "b", "2"]),
            Ok(vec!["a".into(), "b".into()])
        );
    }

    /// The rest of the movablekeys family, one case per SHAPE the walker
    /// knows, so a fix that covers only the six names in the issue cannot pass.
    #[test]
    fn getkeys_covers_every_movable_shape() {
        // numkeys vectors, count at argv[1] and argv[2]
        assert_eq!(
            getkeys_of(&["ZINTERCARD", "2", "a", "b", "LIMIT", "1"]),
            Ok(vec!["a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["ZUNION", "2", "a", "b"]),
            Ok(vec!["a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["ZINTER", "2", "a", "b"]),
            Ok(vec!["a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["BLMPOP", "0", "2", "a", "b", "LEFT"]),
            Ok(vec!["a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["BZMPOP", "0", "2", "a", "b", "MIN"]),
            Ok(vec!["a".into(), "b".into()])
        );
        // scripting
        assert_eq!(
            getkeys_of(&["EVALSHA", "sha", "1", "k"]),
            Ok(vec!["k".into()])
        );
        assert_eq!(
            getkeys_of(&["FCALL", "f", "2", "a", "b"]),
            Ok(vec!["a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["FCALL_RO", "f", "1", "k"]),
            Ok(vec!["k".into()])
        );
        // destination + numkeys vector: the destination comes FIRST
        assert_eq!(
            getkeys_of(&["ZUNIONSTORE", "d", "2", "a", "b"]),
            Ok(vec!["d".into(), "a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["ZINTERSTORE", "d", "2", "a", "b"]),
            Ok(vec!["d".into(), "a".into(), "b".into()])
        );
        // positional STORE clauses
        assert_eq!(getkeys_of(&["SORT", "k"]), Ok(vec!["k".into()]));
        assert_eq!(
            getkeys_of(&["SORT", "k", "ALPHA", "STORE", "dst"]),
            Ok(vec!["k".into(), "dst".into()])
        );
        assert_eq!(
            getkeys_of(&["GEORADIUS", "src", "1", "2", "3", "m", "STORE", "d"]),
            Ok(vec!["src".into(), "d".into()])
        );
        assert_eq!(
            getkeys_of(&["GEORADIUSBYMEMBER", "src", "m", "3", "m", "STOREDIST", "d"]),
            Ok(vec!["src".into(), "d".into()])
        );
        // the STREAMS token: N keys then N ids
        assert_eq!(
            getkeys_of(&["XREAD", "COUNT", "1", "STREAMS", "a", "b", "0", "0"]),
            Ok(vec!["a".into(), "b".into()])
        );
        // subcommand-shaped
        assert_eq!(
            getkeys_of(&["OBJECT", "ENCODING", "k"]),
            Ok(vec!["k".into()])
        );
        assert_eq!(getkeys_of(&["MEMORY", "USAGE", "k"]), Ok(vec!["k".into()]));
        assert_eq!(getkeys_of(&["XINFO", "STREAM", "k"]), Ok(vec!["k".into()]));
        assert_eq!(
            getkeys_of(&["XGROUP", "CREATE", "k", "g", "$"]),
            Ok(vec!["k".into()])
        );
        // two-key move
        assert_eq!(
            getkeys_of(&["RPOPLPUSH", "a", "b"]),
            Ok(vec!["a".into(), "b".into()])
        );
        // a `BY`/`GET` pattern names keys nobody can enumerate; redis still
        // reports the ones it CAN, and so must moon. (ACL deliberately denies
        // the same argv — the walker reports facts, consumers apply policy.)
        assert_eq!(
            getkeys_of(&["SORT", "k", "BY", "w_*"]),
            Ok(vec!["k".into()])
        );
        assert_eq!(
            getkeys_of(&["SORT_RO", "k", "BY", "w_*"]),
            Ok(vec!["k".into()])
        );
    }

    /// The four error strings and — critically — their ORDER, all measured
    /// against redis 8.6.1. The order is observable: `SELECT`'s arity is wrong
    /// AND it has no keys, and redis reports the no-keys answer, so an
    /// implementation that checks arity first is distinguishable.
    #[test]
    fn getkeys_error_strings_and_their_order_match_redis() {
        assert_eq!(
            getkeys_of(&["NOSUCHCMD", "k"]),
            Err("ERR Invalid command specified".into())
        );
        for argv in [
            &["PING"][..],
            &["PING", "x"][..],
            &["SELECT"][..], // arity ALSO wrong: no-keys must win
            &["SELECT", "0"][..],
            &["FLUSHALL"][..],
            &["KEYS", "*"][..],
            &["MULTI"][..],
            &["SUBSCRIBE", "ch"][..],
            // container commands whose SUBCOMMAND takes no key
            &["OBJECT", "HELP"][..],
            &["MEMORY", "STATS"][..],
            &["XINFO", "HELP"][..],
        ] {
            assert_eq!(
                getkeys_of(argv),
                Err("ERR The command has no key arguments".into()),
                "{argv:?}"
            );
        }
        for argv in [
            &["GET"][..],                                         // arity 2, given 1
            &["SET", "k"][..],                                    // arity -3, given 2
            &["LMPOP", "0", "LEFT"][..],                          // arity -4, given 3
            &["SINTERCARD", "0"][..],                             // arity -3, given 2
            &["XREAD", "COUNT", "1"][..],                         // arity -4, given 3
            &["XREADGROUP", "GROUP", "g", "c", "COUNT", "1"][..], // arity -7
        ] {
            assert_eq!(
                getkeys_of(argv),
                Err("ERR Invalid number of arguments specified for command".into()),
                "{argv:?}"
            );
        }
        // Right arity, but THIS argv names no key we can enumerate.
        for argv in [
            &["LMPOP", "3", "k1", "LEFT"][..],   // numkeys exceeds the argv
            &["LMPOP", "abc", "k1", "LEFT"][..], // numkeys unparsable
            &["LMPOP", "-1", "k", "LEFT"][..],
            &["ZMPOP", "abc", "k", "MIN"][..],
            &["ZDIFF", "9", "a"][..],
            &["SINTERCARD", "2", "a"][..],
            &["ZUNIONSTORE", "d", "abc", "a"][..],
            &["XREAD", "COUNT", "1", "2", "3"][..], // no STREAMS token
        ] {
            assert_eq!(
                getkeys_of(argv),
                Err("ERR Invalid arguments specified for command".into()),
                "{argv:?}"
            );
        }
    }

    /// `no-mandatory-keys`: the scripting family takes its key COUNT from an
    /// argument, so naming zero keys is a legitimate outcome and redis replies
    /// with an EMPTY ARRAY rather than an error — including for a count the
    /// argv cannot satisfy. Every other movablekeys command errors instead,
    /// which is the distinction the flag exists to draw.
    #[test]
    fn getkeys_no_mandatory_keys_family_replies_an_empty_array() {
        for argv in [
            &["EVAL", "return 1", "0"][..],
            &["EVAL", "return 1", "0", "extra"][..],
            &["EVAL", "return 1", "5", "k"][..], // count the argv cannot satisfy
            &["EVAL", "return 1", "abc", "k"][..], // unparsable count
            &["EVAL", "s", "-1", "k"][..],
            &["EVALSHA", "sha", "abc", "k"][..],
            &["FCALL", "f", "abc", "k"][..],
            &["FCALL_RO", "f", "0"][..],
        ] {
            assert_eq!(getkeys_of(argv), Ok(vec![]), "{argv:?}");
        }
        // The contrast: LMPOP is NOT no-mandatory-keys, so the same shape of
        // bad count is an error (issue text pins this pair explicitly).
        assert!(getkeys_of(&["LMPOP", "abc", "k", "LEFT"]).is_err());
    }

    /// Fixed-position multi-key commands keep working — the meta-derived walk
    /// still answers them, including `BITOP`'s `first_key: 2` and the
    /// destination-first `*STORE` family whose SOURCES this change reclassified
    /// as read-only (moon#584). `COMMAND GETKEYS` reports every key regardless
    /// of role, exactly as redis does.
    #[test]
    fn getkeys_reports_read_only_sources_too() {
        assert_eq!(
            getkeys_of(&["SINTERSTORE", "d", "a", "b"]),
            Ok(vec!["d".into(), "a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["BITOP", "AND", "d", "a", "b"]),
            Ok(vec!["d".into(), "a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["PFMERGE", "d", "a", "b"]),
            Ok(vec!["d".into(), "a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["COPY", "s", "d"]),
            Ok(vec!["s".into(), "d".into()])
        );
        assert_eq!(
            getkeys_of(&["ZRANGESTORE", "d", "s", "0", "-1"]),
            Ok(vec!["d".into(), "s".into()])
        );
        assert_eq!(
            getkeys_of(&[
                "GEOSEARCHSTORE",
                "d",
                "s",
                "FROMMEMBER",
                "m",
                "BYRADIUS",
                "1",
                "m",
                "ASC"
            ]),
            Ok(vec!["d".into(), "s".into()])
        );
        assert_eq!(
            getkeys_of(&["RENAME", "a", "b"]),
            Ok(vec!["a".into(), "b".into()])
        );
        assert_eq!(
            getkeys_of(&["EXISTS", "a", "b"]),
            Ok(vec!["a".into(), "b".into()])
        );
    }

    /// Known, deliberate divergences from redis 8.6.1 on MALFORMED argv, kept
    /// here so a future reader can tell "we decided this" from "we missed it".
    ///
    /// All three are cases where moon is stricter or more inclusive than
    /// redis, on argv that cannot execute successfully anyway.
    #[test]
    fn getkeys_documented_divergences_on_malformed_argv() {
        // (1) A dangling STORE: redis ignores the token and answers `k`. moon
        //     answers `k` too — but ACL keeps DENYING the same argv, which is
        //     why the walker routes it through `AtPlusComputed` rather than
        //     simply dropping the clause.
        assert_eq!(getkeys_of(&["SORT", "k", "STORE"]), Ok(vec!["k".into()]));
        assert_eq!(getkeys_of(&["SORT", "k", "BY"]), Ok(vec!["k".into()]));
        assert_eq!(
            getkeys_of(&["GEORADIUS", "src", "1", "2", "3", "m", "STORE"]),
            Ok(vec!["src".into()])
        );

        // (2) Repeated STORE: redis keeps only the LAST destination (`k`,`e`);
        //     moon reports both. Reporting a superset is the safe direction —
        //     ACL checks both, and the command is a syntax error regardless.
        assert_eq!(
            getkeys_of(&["SORT", "k", "STORE", "d", "STORE", "e"]),
            Ok(vec!["k".into(), "d".into(), "e".into()])
        );

        // (3) Container arity: redis resolves `OBJECT ENCODING` to the
        //     subcommand and reports its arity error; moon has no per-
        //     subcommand arity, so it answers the no-keys error instead. Both
        //     are errors; only the text differs.
        assert_eq!(
            getkeys_of(&["OBJECT", "ENCODING"]),
            Err("ERR The command has no key arguments".into())
        );
    }

    /// A key position holding a non-string cannot be reported as a key, and
    /// must not shift the positions after it either — the reason `getkeys`
    /// walks FRAMES rather than a pre-filtered `Vec<Bytes>`.
    #[test]
    fn getkeys_survives_a_non_string_in_a_key_position() {
        let r = command(&[
            bulk("GETKEYS"),
            bulk("MSET"),
            Frame::Integer(7),
            bulk("v1"),
            bulk("k2"),
            bulk("v2"),
        ]);
        // Integer is not a key name; the argv is malformed, so it must be an
        // error rather than a silently shifted key list naming "v1".
        let Frame::Error(e) = r else {
            panic!("a non-string key position must not produce a key list");
        };
        assert!(String::from_utf8_lossy(&e).contains("Invalid arguments"));
    }
}
