//! The command rules of a restricted ACL user, in the order they were applied
//! (moon#1296).
//!
//! Redis 7.2+ renders a user's command rules the way the operator typed them
//! (`ACLDescribeSelectorCommandRules`): `-@all +set +get` is `-@all +set +get`
//! and `-@all +get +set` is `-@all +get +set`, and re-applying a rule moves it
//! to the end (`ACLUpdateCommandRules` drops the older spelling, then
//! appends). Moon kept two `HashSet`s and sorted them at render time, so the
//! text was neither the redis order nor, before the sort, stable between
//! runs: `ACL GETUSER`, `ACL LIST` and the ACL file flipped between
//! `-@all +get +set` and `-@all +set +get` across processes.
//!
//! One insertion-ordered map holds both polarities. That is lossless: a rule
//! lives in at most one set at a time (a grant removes the same rule's denial
//! and vice versa), so the two sets were only ever one map with a boolean.
//! Keeping them together is what preserves the order BETWEEN grants and
//! revocations (`+get -set +put`), which two separate ordered sets cannot.
//! `permits` also probes one map instead of two.
//!
//! Only the rule TEXT is ordered. The verdict for a command is decided by
//! [`CommandRules::verdict`] exactly as before: the rule for `cmd|arg`, then
//! the bare `cmd` rule, then the base polarity.

use indexmap::IndexMap;

/// `rule -> allowed`, in application order. Keys are lowercase: a bare
/// `cmd`, or `cmd|arg` (a subcommand or first-argument rule, see
/// [`super::subcommand`]).
#[derive(Clone, Debug, Default)]
pub struct CommandRules {
    rules: IndexMap<String, bool>,
}

impl CommandRules {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rules held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rules.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The verdict of exactly this rule (`None` when none was applied).
    #[inline]
    #[must_use]
    pub fn get(&self, rule: &str) -> Option<bool> {
        self.rules.get(rule).copied()
    }

    /// Apply `+rule` (`allow`) or `-rule`: any older entry for the same rule
    /// goes, and the new one is appended, as redis's `ACLUpdateCommandRules`
    /// does. `rule` must already be lowercase.
    pub fn apply(&mut self, rule: &str, allow: bool) {
        // `shift_remove`, not a plain insert: an insert over an existing key
        // keeps its OLD position, which would render `+get +set +get` as
        // `+get +set` where redis renders `+set +get`.
        self.rules.shift_remove(rule);
        self.rules.insert(rule.to_string(), allow);
    }

    /// Drop every `cmd|*` rule, for each `cmd` in `cmds`.
    ///
    /// Called by every rule that names a command bare (directly or through a
    /// category): that rule is now the newest word on every subcommand of
    /// `cmd`, exactly as redis's per-ID bits are all rewritten by `+cmd` /
    /// `-cmd` (see [`super::subcommand`]).
    pub fn clear_first_arg_rules(&mut self, cmds: &[&str]) {
        self.rules.retain(|rule, _| match rule.split_once('|') {
            Some((cmd, _)) => !cmds.contains(&cmd),
            None => true,
        });
    }

    /// The verdict of the `bare|arg` rule, comparing `arg` ASCII-case-
    /// insensitively as redis does for subcommand names and first-arg rules.
    /// `bare` must be lowercase. Allocation-free.
    #[must_use]
    pub fn first_arg_verdict(&self, bare: &str, arg: &[u8]) -> Option<bool> {
        /// Arguments up to this length are matched through a stack buffer;
        /// longer ones by a scan. Either way nothing is allocated, so a
        /// restricted user's `ECHO <512 MB>` costs no copy of the argument.
        const INLINE_RULE_KEY: usize = 128;
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
            return std::str::from_utf8(&buf[..total])
                .ok()
                .and_then(|key| self.get(key));
        }
        self.rules
            .iter()
            .find(|(rule, _)| {
                rule.len() == total
                    && rule.as_bytes().starts_with(bare.as_bytes())
                    && rule.as_bytes()[bare.len()] == b'|'
                    && rule.as_bytes()[bare.len() + 1..].eq_ignore_ascii_case(arg)
            })
            .map(|(_, allow)| *allow)
    }

    /// The rules as `+rule` / `-rule` tokens, in application order.
    ///
    /// A bare `cmd` token is never emitted after a `cmd|arg` one: on reload a
    /// bare `+cmd` / `-cmd` clears every `cmd|*` rule applied before it
    /// ([`Self::clear_first_arg_rules`]), so a `-config|set` written ahead of
    /// a `+config` would reload as `CONFIG SET` allowed, which is fail-open.
    /// Application order cannot produce that (a bare rule clears the older
    /// `cmd|*` ones as it lands), so the hoist below is a guard for a state
    /// that should not exist, not a reordering of a normal one.
    #[must_use]
    pub fn tokens(&self) -> Vec<String> {
        let mut out: Vec<(String, &str)> = Vec::with_capacity(self.rules.len());
        for (rule, allow) in &self.rules {
            let token = format!("{}{rule}", if *allow { '+' } else { '-' });
            if rule.contains('|') {
                out.push((token, rule));
                continue;
            }
            let first_sub = out.iter().position(|(_, r)| {
                r.split_once('|')
                    .is_some_and(|(cmd, _)| cmd == rule.as_str())
            });
            match first_sub {
                Some(at) => out.insert(at, (token, rule)),
                None => out.push((token, rule)),
            }
        }
        out.into_iter().map(|(token, _)| token).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::CommandRules;
    use crate::acl::io::{command_rules_to_string, parse_acl_line, user_to_acl_line};

    /// Rules as redis renders them: `-@all` first when the user is created
    /// denying everything, then the tokens as applied.
    fn rendered(rule_line: &str) -> String {
        let user = parse_acl_line(&format!("user u {rule_line}")).expect("rules must parse");
        command_rules_to_string(&user.allowed_commands)
    }

    /// Redis 7.2.7 (`ACL SETUSER u <rules>`, then `ACL LIST`), verbatim: the
    /// order the operator typed, a re-applied rule moved to the end, and
    /// grants interleaved with revocations. Every expectation was read off
    /// the oracle, not derived.
    #[test]
    fn rules_render_in_the_order_they_were_applied() {
        for (input, want) in [
            ("+set +get", "-@all +set +get"),
            ("+get +set", "-@all +get +set"),
            ("-@all +get +set +get", "-@all +set +get"),
            ("-@all +set +set +get", "-@all +set +get"),
            ("-@all +hset +hget +hdel", "-@all +hset +hget +hdel"),
            (
                "-@all +del +get +set +mset +mget",
                "-@all +del +get +set +mset +mget",
            ),
            ("+@all -set -get", "+@all -set -get"),
            ("+@all -get -set", "+@all -get -set"),
            ("+@all -set -get +set", "+@all -get +set"),
            ("+@all -get -set +get -del", "+@all -set +get -del"),
            ("+@all -get +get", "+@all +get"),
            ("-@all +get -set +append", "-@all +get -set +append"),
            ("-@all +get -get +get", "-@all +get"),
            ("-@all -get +get", "-@all +get"),
            ("-@all +get nocommands +set", "-@all +set"),
            // subcommand and first-argument rules
            ("-@all +config|get +config", "-@all +config"),
            ("-@all +config +config|get", "-@all +config +config|get"),
            ("-@all +config|get +get", "-@all +config|get +get"),
            ("-@all +get +config|get", "-@all +get +config|get"),
            (
                "-@all +config|set +config|get",
                "-@all +config|set +config|get",
            ),
            (
                "-@all +config|get +config|set +config|get",
                "-@all +config|set +config|get",
            ),
            (
                "+@all -config|set -config|get",
                "+@all -config|set -config|get",
            ),
            ("+@all -config|set +config", "+@all +config"),
            ("+@all -config -config|get", "+@all -config -config|get"),
            (
                "-@all +client|list +client|id",
                "-@all +client|list +client|id",
            ),
            ("-@all +select|0 +get", "-@all +select|0 +get"),
        ] {
            assert_eq!(rendered(input), want, "rules `{input}`");
        }
    }

    /// The text must not depend on the process: `HashSet` iteration order is
    /// seeded per process, which is how the moon#981 rows flipped between
    /// runs. Build the same user many times over.
    #[test]
    fn the_rendering_is_stable_across_rebuilds() {
        let first = rendered("-@all +get +set +del +mget +hset +lpush -flushall +config|get");
        for _ in 0..50 {
            assert_eq!(
                rendered("-@all +get +set +del +mget +hset +lpush -flushall +config|get"),
                first
            );
        }
        assert_eq!(
            first,
            "-@all +get +set +del +mget +hset +lpush -flushall +config|get"
        );
    }

    /// `ACL SAVE` -> `ACL LOAD` is a fixed point: re-rendering a reloaded
    /// user gives the same tokens (so an existing file does not churn again
    /// on every save).
    #[test]
    fn save_and_load_reach_a_fixed_point() {
        for input in [
            "-@all +set +get",
            "+@all -set -get +set -del",
            "-@all +config +config|get -config|set",
            "+@all -config|set -client|kill",
            "-@all +get -set +append",
        ] {
            let user = parse_acl_line(&format!("user u {input}")).expect("parse");
            let line = user_to_acl_line(&user);
            let reloaded = parse_acl_line(&line).expect("the saved line reloads");
            assert_eq!(
                command_rules_to_string(&reloaded.allowed_commands),
                command_rules_to_string(&user.allowed_commands),
                "`{input}` via `{line}`"
            );
            for cmd in ["get", "set", "del", "append", "config|get", "config|set"] {
                assert_eq!(
                    reloaded.is_command_allowed(cmd),
                    user.is_command_allowed(cmd),
                    "`{input}`: {cmd} after reload"
                );
            }
        }
    }

    /// A rule's order never decides a verdict: the same rules typed in every
    /// order give the same permissions, only a different text. (A rule for
    /// one command is unaffected by rules for another.)
    #[test]
    fn order_changes_the_text_not_the_permissions() {
        let a = parse_acl_line("user u -@all +get +set -del").expect("parse");
        let b = parse_acl_line("user u -@all -del +set +get").expect("parse");
        for cmd in ["get", "set", "del", "hset"] {
            assert_eq!(
                a.is_command_allowed(cmd),
                b.is_command_allowed(cmd),
                "{cmd}"
            );
        }
        assert_ne!(
            command_rules_to_string(&a.allowed_commands),
            command_rules_to_string(&b.allowed_commands)
        );
    }

    /// Fail-open guard: a `cmd|arg` token must never be written ahead of its
    /// bare `cmd`, or a reload's bare rule would clear it (`-config|set`
    /// before `+config` reloads as CONFIG SET allowed). Application order
    /// cannot build that state (the bare rule clears the older `cmd|*` ones
    /// as it lands); a hand-built map can, so the renderer hoists.
    #[test]
    fn a_bare_rule_is_never_written_after_its_subcommand_rules() {
        let mut rules = CommandRules::new();
        rules.apply("get", true);
        rules.apply("config|set", false);
        rules.apply("client|id", true);
        rules.apply("config", true);
        assert_eq!(
            rules.tokens(),
            ["+get", "+config", "-config|set", "+client|id"]
        );
    }

    /// The bare-before-sub invariant holds through the real rule path, and
    /// the resulting text reloads to the same permissions (fail-closed).
    #[test]
    fn a_bare_rule_after_a_subcommand_rule_clears_it() {
        let user = parse_acl_line("user u -@all +config|get -config|set +config").expect("parse");
        assert_eq!(
            command_rules_to_string(&user.allowed_commands),
            "-@all +config"
        );
        assert!(user.is_command_allowed("config|set"));
        let user = parse_acl_line("user u +@all -config|set -config +get").expect("parse");
        assert!(!user.is_command_allowed("config|set"));
    }

    #[test]
    fn first_arg_rules_match_case_insensitively_and_past_the_inline_buffer() {
        let mut rules = CommandRules::new();
        rules.apply("select|0", true);
        rules.apply("config|set", false);
        assert_eq!(rules.first_arg_verdict("select", b"0"), Some(true));
        assert_eq!(rules.first_arg_verdict("config", b"SET"), Some(false));
        assert_eq!(rules.first_arg_verdict("config", b"get"), None);
        let long = vec![b'a'; 4096];
        assert_eq!(rules.first_arg_verdict("select", &long), None);
        rules.apply(&format!("echo|{}", "b".repeat(200)), false);
        assert_eq!(
            rules.first_arg_verdict("echo", "B".repeat(200).as_bytes()),
            Some(false),
            "the scan path compares ASCII-case-insensitively"
        );
        assert_eq!(rules.first_arg_verdict("echo", &[0xff; 4]), None);
    }
}
