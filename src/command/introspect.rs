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
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from(meta.name.to_ascii_lowercase())),
        Frame::Integer(meta.arity as i64),
        Frame::Array(flags),
        Frame::Integer(meta.first_key as i64),
        Frame::Integer(meta.last_key as i64),
        Frame::Integer(meta.step as i64),
        Frame::Array(cats),
        Frame::Array(framevec![]), // tips
        Frame::Array(framevec![]), // key specs
        Frame::Array(framevec![]), // subcommands
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

/// Extract the key arguments of `argv` using the registry's key spec.
///
/// `argv[0]` is the command name. Returns an error Frame rather than an empty
/// array when the command has no keys — "no keys" and "I did not understand
/// you" must not look identical to a cluster-aware client deciding where to
/// route a command.
fn getkeys(argv: &[Bytes]) -> Frame {
    let Some(name) = argv.first() else {
        return err("ERR Unknown subcommand or wrong number of arguments for 'GETKEYS'");
    };
    // `lookup` uppercases internally — no need to allocate an upper copy.
    let Some(meta) = crate::command::metadata::lookup(name) else {
        return err("ERR Invalid command specified");
    };

    // Arity: positive = exact, negative = minimum. argv includes the name.
    let n = argv.len() as i16;
    let arity_ok = if meta.arity >= 0 {
        n == meta.arity
    } else {
        n >= -meta.arity
    };
    if !arity_ok {
        return err("ERR Invalid number of arguments specified for command");
    }

    if meta.first_key <= 0 {
        return err("ERR The command has no key arguments");
    }

    let last = if meta.last_key < 0 {
        // -1 means "through the last argument"; -2 means "through the
        // second-to-last", and so on.
        (n + meta.last_key) as usize
    } else {
        meta.last_key as usize
    };
    let step = if meta.step <= 0 {
        1
    } else {
        meta.step as usize
    };

    let mut keys = crate::protocol::FrameVec::new();
    let mut i = meta.first_key as usize;
    while i <= last && i < argv.len() {
        keys.push(Frame::BulkString(argv[i].clone()));
        i += step;
    }
    if keys.is_empty() {
        return err("ERR The command has no key arguments");
    }
    Frame::Array(keys)
}

/// Minimal but SHAPE-correct docs: name followed by a map. Redis clients parse
/// the shape to build help/command maps; thin summary text is acceptable, a
/// wrong shape is not.
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
            (
                Frame::BulkString(Bytes::from_static(b"arity")),
                Frame::Integer(meta.arity as i64),
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
        let mut out = crate::protocol::FrameVec::new();
        if args.len() == 1 {
            out.reserve(COMMAND_META.len() * 2);
            for meta in COMMAND_META.values() {
                let (n, d) = docs_for(meta);
                out.push(n);
                out.push(d);
            }
        } else {
            for a in &args[1..] {
                if let Some(meta) = extract(a).and_then(|n| crate::command::metadata::lookup(&n)) {
                    let (n, d) = docs_for(meta);
                    out.push(n);
                    out.push(d);
                }
                // Redis omits unknown names from DOCS entirely (unlike INFO,
                // which is positional and uses a null element).
            }
        }
        return Frame::Array(out);
    }

    if sub.eq_ignore_ascii_case(b"GETKEYS") {
        if args.len() < 2 {
            return err("ERR Unknown subcommand or wrong number of arguments for 'GETKEYS'");
        }
        let argv: Vec<Bytes> = args[1..].iter().filter_map(extract).collect();
        return getkeys(&argv);
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
}
