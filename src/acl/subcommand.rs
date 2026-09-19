//! Per-subcommand (`+config|get`, `-config|set`) and first-argument
//! (`+select|0`) command rules.
//!
//! Both are stored in the same `allowed` / `denied` sets as bare commands, as
//! the lowercased token `cmd|arg`. Redis evaluates them per command ID with
//! last-rule-wins, and this module reproduces that with one invariant:
//!
//! **A `cmd|arg` entry is always newer than any bare `cmd` entry.** Every rule
//! that names `cmd` bare -- `+cmd`, `-cmd`, or a category containing `cmd` --
//! drops all `cmd|*` entries from both sets ([`clear_first_arg_rules`]), so a
//! surviving `cmd|arg` entry was applied after the bare verdict and overrides
//! it ([`permits`]). `+@all` / `-@all` rebuild the sets and clear everything.
//!
//! Before this module the check only ever probed the bare name, so
//! `+@all -config|set` answered `+OK` and still let the user run
//! `CONFIG SET`.

use std::collections::HashSet;

use crate::protocol::Frame;

use super::table::CommandPermissions;

/// First arguments up to this length are matched through a stack buffer;
/// longer ones by a scan of the set. Either way nothing is allocated, so a
/// restricted user's `ECHO <512 MB>` costs no copy of the argument.
const INLINE_RULE_KEY: usize = 128;

/// `argv[1]` as bytes, when it is a string frame.
#[inline]
pub(crate) fn first_arg(args: &[Frame]) -> Option<&[u8]> {
    match args.first()? {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// Does `set` hold the rule `bare|arg`? `bare` must already be lowercase;
/// `arg` is compared ASCII-case-insensitively, as redis does for both
/// subcommand names and first-arg rules. Allocation-free.
fn contains_first_arg_rule(set: &HashSet<String>, bare: &str, arg: &[u8]) -> bool {
    let total = bare.len() + 1 + arg.len();
    if total <= INLINE_RULE_KEY {
        let mut buf = [0u8; INLINE_RULE_KEY];
        buf[..bare.len()].copy_from_slice(bare.as_bytes());
        buf[bare.len()] = b'|';
        for (dst, src) in buf[bare.len() + 1..total].iter_mut().zip(arg) {
            *dst = src.to_ascii_lowercase();
        }
        // A non-UTF-8 argument cannot equal any stored rule (they are all
        // `String`s), so failing the conversion is a correct "absent".
        return std::str::from_utf8(&buf[..total]).is_ok_and(|key| set.contains(key));
    }
    set.iter().any(|rule| {
        rule.len() == total
            && rule.as_bytes().starts_with(bare.as_bytes())
            && rule.as_bytes()[bare.len()] == b'|'
            && rule.as_bytes()[bare.len() + 1..].eq_ignore_ascii_case(arg)
    })
}

/// Drop every `cmd|*` rule, for each `cmd` in `cmds`, from `set`.
///
/// Called by every rule that names a command bare (directly or through a
/// category): that rule is now the newest word on every subcommand of `cmd`,
/// exactly as redis's per-ID bits are all rewritten by `+cmd` / `-cmd`.
/// Skipping this is fail-OPEN: `-@all +config|get -@admin` would keep the
/// stale `config|get` grant alive under the newer category revoke.
pub(super) fn clear_first_arg_rules(set: &mut HashSet<String>, cmds: &[&str]) {
    set.retain(|rule| match rule.split_once('|') {
        Some((cmd, _)) => !cmds.contains(&cmd),
        None => true,
    });
}

/// The verdict for one invocation. `bare` is the lowercased command name,
/// `first_arg` its `argv[1]`. A `bare|arg` rule, being newer than any bare
/// rule (see the module docs), decides first; then the bare rule; then the
/// base polarity.
pub(super) fn permits(perms: &CommandPermissions, bare: &str, first_arg: Option<&[u8]>) -> bool {
    match perms {
        CommandPermissions::AllAllowed => true,
        CommandPermissions::Specific {
            base_allow,
            allowed,
            denied,
        } => {
            if let Some(arg) = first_arg {
                if contains_first_arg_rule(denied, bare, arg) {
                    return false;
                }
                if contains_first_arg_rule(allowed, bare, arg) {
                    return true;
                }
            }
            if denied.contains(bare) {
                return false;
            }
            if allowed.contains(bare) {
                return true;
            }
            // Neither set names this command, so the answer is the base
            // polarity recorded when this `Specific` was created -- never
            // inferred from set emptiness (GHSA-9x86-7597-5wwj).
            *base_allow
        }
    }
}

/// The command name redis reports for an invocation, in `NOPERM` text and as
/// the `ACL LOG` object: `container|sub` when `cmd` is a container and
/// `argv[1]` one of its subcommands (`config|set`), otherwise the bare name
/// (`select`, also for `SELECT 0` under a `+select|0` rule). Lowercase.
pub fn command_log_object(cmd: &[u8], args: &[Frame]) -> String {
    let mut name = String::from_utf8_lossy(cmd).to_ascii_lowercase();
    if let Some(sub) = first_arg(args)
        && crate::command::metadata::is_known_subcommand(cmd, sub)
        && let Ok(sub) = std::str::from_utf8(sub)
    {
        name.push('|');
        name.push_str(&sub.to_ascii_lowercase());
    }
    name
}

#[cfg(test)]
mod tests {
    use crate::acl::AclTable;
    use crate::acl::io::parse_acl_line;
    use crate::protocol::Frame;
    use bytes::Bytes;

    fn args(words: &[&str]) -> Vec<Frame> {
        words
            .iter()
            .map(|w| Frame::BulkString(Bytes::copy_from_slice(w.as_bytes())))
            .collect()
    }

    fn table(rules: &[&str]) -> AclTable {
        let mut t = AclTable::new_empty();
        let mut all = vec!["on", "nopass", "~*", "&*"];
        all.extend_from_slice(rules);
        t.apply_setuser("u", &all);
        t
    }

    /// `None` = allowed; `Some(name)` = NOPERM naming `name`.
    fn verdict(t: &AclTable, argv: &[&str]) -> Option<String> {
        let cmd = argv[0].as_bytes();
        t.check_command_permission("u", cmd, &args(&argv[1..]))
            .map(|reason| {
                let prefix = "User u has no permissions to run the '";
                let rest = reason
                    .strip_prefix(prefix)
                    .unwrap_or_else(|| panic!("unexpected deny text: {reason}"));
                rest.strip_suffix("' command")
                    .unwrap_or_else(|| panic!("unexpected deny text: {reason}"))
                    .to_string()
            })
    }

    const CFG_GET: &[&str] = &["CONFIG", "GET", "maxmemory-samples"];
    const CFG_SET: &[&str] = &["CONFIG", "SET", "maxmemory-samples", "5"];
    const CFG_SET_LC: &[&str] = &["config", "set", "maxmemory-samples", "5"];
    const CFG_RESETSTAT: &[&str] = &["CONFIG", "RESETSTAT"];
    const SELECT0: &[&str] = &["SELECT", "0"];
    const SELECT1: &[&str] = &["SELECT", "1"];
    const GET_FOO: &[&str] = &["GET", "foo"];
    const GET_K: &[&str] = &["GET", "k"];
    const OBJ_ENC: &[&str] = &["OBJECT", "ENCODING", "k"];
    const OBJ_FREQ: &[&str] = &["OBJECT", "FREQ", "k"];
    const PS_NUMSUB: &[&str] = &["PUBSUB", "NUMSUB", "c"];
    const PS_CHANNELS: &[&str] = &["PUBSUB", "CHANNELS"];

    /// Every row transcribed from redis-server 8.6.1 (`AUTH u x`, then the
    /// command on a raw socket). `None` = executed, `Some(n)` =
    /// `-NOPERM User u has no permissions to run the 'n' command`.
    #[test]
    fn subcommand_rules_match_redis_8_6_1() {
        type Row = (&'static [&'static str], Option<&'static str>);
        let cases: &[(&[&str], &[Row])] = &[
            // The reported defect: an allow-base user with one subcommand
            // revoked. Redis refuses CONFIG SET in any casing.
            (
                &["+@all", "-config|set"],
                &[
                    (CFG_GET, None),
                    (CFG_SET, Some("config|set")),
                    (CFG_SET_LC, Some("config|set")),
                    (CFG_RESETSTAT, None),
                    (&["CONFIG", "sEt", "x", "y"], Some("config|set")),
                ],
            ),
            (
                &["+@all", "-CONFIG|SET"],
                &[(CFG_SET, Some("config|set")), (CFG_RESETSTAT, None)],
            ),
            // Deny base, grant one, revoke a sibling.
            (
                &["-@all", "+config|get", "-config|set"],
                &[
                    (CFG_GET, None),
                    (CFG_SET, Some("config|set")),
                    (CFG_RESETSTAT, Some("config|resetstat")),
                    (SELECT0, Some("select")),
                    (OBJ_ENC, Some("object|encoding")),
                    (PS_CHANNELS, Some("pubsub|channels")),
                ],
            ),
            // Bare revoke covers every subcommand.
            (
                &["+@all", "-config"],
                &[
                    (CFG_GET, Some("config|get")),
                    (CFG_SET, Some("config|set")),
                    (SELECT0, None),
                ],
            ),
            // Last rule wins: a later category grant re-allows the subcommand.
            (
                &["+@all", "-config|set", "+@admin"],
                &[(CFG_SET, None), (CFG_GET, None)],
            ),
            // Last rule wins: a later category revoke kills an earlier grant.
            (
                &["-@all", "+config|get", "-@admin"],
                &[(CFG_GET, Some("config|get")), (CFG_SET, Some("config|set"))],
            ),
            // A subcommand grant after a bare revoke.
            (
                &["+@all", "-config", "+config|get"],
                &[
                    (CFG_GET, None),
                    (CFG_SET, Some("config|set")),
                    (CFG_RESETSTAT, Some("config|resetstat")),
                ],
            ),
            // A bare grant after a subcommand revoke clears it.
            (
                &["+@all", "-config|set", "+config"],
                &[(CFG_SET, None), (CFG_GET, None)],
            ),
            // Re-granting the same subcommand.
            (&["+@all", "-config|set", "+config|set"], &[(CFG_SET, None)]),
            // A bare revoke after subcommand grants clears them.
            (
                &["-@all", "+config|get", "+config|set", "-config"],
                &[(CFG_GET, Some("config|get")), (CFG_SET, Some("config|set"))],
            ),
            // First-argument grants on a non-container.
            (
                &["-@all", "+select|0"],
                &[
                    (SELECT0, None),
                    (SELECT1, Some("select")),
                    (CFG_GET, Some("config|get")),
                ],
            ),
            (
                &["-@all", "+get|foo"],
                &[(GET_FOO, None), (GET_K, Some("get"))],
            ),
            // Two containers, one subcommand each.
            (
                &["-@all", "+object|encoding", "+pubsub|numsub"],
                &[
                    (OBJ_ENC, None),
                    (OBJ_FREQ, Some("object|freq")),
                    (PS_NUMSUB, None),
                    (PS_CHANNELS, Some("pubsub|channels")),
                ],
            ),
        ];
        for (rules, rows) in cases {
            let t = table(rules);
            for (argv, expected) in *rows {
                assert_eq!(
                    verdict(&t, argv).as_deref(),
                    *expected,
                    "rules {rules:?}, command {argv:?}"
                );
            }
        }
    }

    /// `ACL LIST` / `ACL SAVE` must render a line that reloads to the SAME
    /// verdicts. A subcommand revoke rendered after the bare grant that
    /// clears it would reload fail-OPEN.
    #[test]
    fn rendered_rules_reload_to_identical_verdicts() {
        let probes: &[&[&str]] = &[
            CFG_GET,
            CFG_SET,
            CFG_RESETSTAT,
            SELECT0,
            SELECT1,
            GET_FOO,
            GET_K,
            OBJ_ENC,
            OBJ_FREQ,
            PS_NUMSUB,
            PS_CHANNELS,
        ];
        let rule_sets: &[&[&str]] = &[
            &["+@all", "-config|set"],
            &["+@all", "-config", "+config", "-config|set"],
            &["+@all", "-config", "+config|get"],
            &["-@all", "+config", "-config|set"],
            &["-@all", "+config|get", "-config|set", "+select|0"],
            &["+@all", "-@admin", "+config|get"],
            &["-@all", "+@admin", "-config|set", "+get|foo"],
            &["+@all", "-object|freq", "-pubsub|channels"],
        ];
        for rules in rule_sets {
            let t = table(rules);
            let line = t.user_to_rule_string("u").expect("user exists");
            let reloaded_user = parse_acl_line(&line)
                .unwrap_or_else(|| panic!("rendered line must reload: {line}"));
            let mut reloaded = AclTable::new_empty();
            reloaded.set_user("u".to_string(), reloaded_user);
            for argv in probes {
                assert_eq!(
                    verdict(&reloaded, argv),
                    verdict(&t, argv),
                    "rules {rules:?} rendered as `{line}`, command {argv:?}"
                );
            }
        }
    }

    /// A subcommand name spelled in any case, and a first argument that is
    /// not UTF-8 or is very long, must neither panic nor slip past a revoke.
    #[test]
    fn odd_first_arguments_fail_closed() {
        let t = table(&["+@all", "-config|set", "-object|encoding"]);
        assert_eq!(
            verdict(&t, &["CONFIG", "SET"]).as_deref(),
            Some("config|set")
        );
        let long = "x".repeat(4096);
        assert_eq!(
            verdict(&t, &["CONFIG", &long]),
            None,
            "unknown sub: handler replies"
        );
        let bad = [Frame::BulkString(Bytes::from_static(b"\xff\xfe"))];
        assert!(t.check_command_permission("u", b"CONFIG", &bad).is_none());
        assert!(t.check_command_permission("u", b"CONFIG", &[]).is_none());

        let t = table(&["-@all", "+get|foo"]);
        let long_key = [Frame::BulkString(Bytes::from(vec![b'f'; 1 << 16]))];
        assert!(t.check_command_permission("u", b"GET", &long_key).is_some());
        let t = table(&["-@all", &format!("+get|{}", "f".repeat(200))]);
        assert!(
            t.check_command_permission("u", b"GET", &args(&[&"F".repeat(200)]))
                .is_none(),
            "a long first-arg grant is matched case-insensitively"
        );
        assert!(
            t.check_command_permission("u", b"GET", &args(&["f"]))
                .is_some()
        );
    }

    /// The ACL LOG object names the subcommand, as redis does.
    #[test]
    fn log_object_names_the_subcommand() {
        use super::command_log_object;
        assert_eq!(
            command_log_object(b"CONFIG", &args(&["SET", "a", "b"])),
            "config|set"
        );
        assert_eq!(
            command_log_object(b"config", &args(&["get", "a"])),
            "config|get"
        );
        assert_eq!(command_log_object(b"CONFIG", &args(&["BOGUS"])), "config");
        assert_eq!(command_log_object(b"CONFIG", &[]), "config");
        assert_eq!(command_log_object(b"SELECT", &args(&["0"])), "select");
        assert_eq!(command_log_object(b"GET", &args(&["k"])), "get");
    }
}
