use std::io::BufRead;

use super::rules::apply_rule;
use super::table::{AclTable, AclUser, CommandPermissions, KeyPattern};

/// Serialize a user to an ACL file line:
/// "user <name> on|off [nopass] [#hash ...] [~pattern ...] [&channel ...] [+@all|-@all ...]"
pub fn user_to_acl_line(user: &AclUser) -> String {
    let mut parts = vec![format!("user {}", user.username)];
    parts.push(if user.enabled {
        "on".to_string()
    } else {
        "off".to_string()
    });
    if user.nopass {
        parts.push("nopass".to_string());
    }
    for hash in &user.passwords {
        parts.push(format!("#{}", hash));
    }
    parts.extend(user.key_patterns.iter().filter_map(key_pattern_to_rule));
    for cp in &user.channel_patterns {
        parts.push(format!("&{}", cp));
    }
    parts.push(command_rules_to_string(&user.allowed_commands));
    parts.join(" ")
}

/// Render one key pattern as the rule token that rebuilds it, the way redis
/// 8.6.1 does: read+write is `~pat` (so `%RW~pat` reads back as `~pat`), and
/// a one-sided grant is `%R~pat` / `%W~pat`.
///
/// The ONE key-pattern renderer behind `ACL SAVE`, `ACL LIST` and
/// `ACL GETUSER`, for the same reason [`command_rules_to_string`] is the one
/// command renderer (moon#981): a second copy drifts.
///
/// A pattern with neither flag grants nothing and renders as nothing
/// (`None`). The parser rejects such a `%` selector, so this is unreachable
/// from `ACL SETUSER`, but the fallback must fail CLOSED: GETUSER's old copy
/// rendered it as `%W~pat`, which would reload as a write grant.
pub fn key_pattern_to_rule(kp: &KeyPattern) -> Option<String> {
    match (kp.read, kp.write) {
        (true, true) => Some(format!("~{}", kp.pattern)),
        (true, false) => Some(format!("%R~{}", kp.pattern)),
        (false, true) => Some(format!("%W~{}", kp.pattern)),
        (false, false) => None,
    }
}

/// Render command permissions as the ACL rule tokens that rebuild them:
/// the base polarity (`+@all` / `-@all`) first, then the named sets.
///
/// moon#981: this is the ONE serializer behind `ACL SAVE`, `ACL LIST` and
/// `ACL GETUSER`. It used to emit `-@all` for every `Specific` value,
/// discarding `base_allow` (moon#971), so a `+@all -flushall` user was
/// written to disk as `-@all -flushall` and reloaded able to run nothing.
///
/// The rule parser is the reader. `+@all` yields `AllAllowed`, and the first
/// `-<cmd>` after it is what transitions to `Specific { base_allow: true }`,
/// so under an allow base the revocations must precede the re-grants: a
/// `+get` emitted while the user is still `AllAllowed` is a no-op, and the
/// later `-@string` expansion would then deny `get` again. Under a deny base
/// the grants precede the revocations, exactly as before, so every existing
/// file line is byte-identical. Both sets are written even where one is
/// redundant with the base, so nothing the operator granted is dropped.
pub fn command_rules_to_string(perms: &CommandPermissions) -> String {
    match perms {
        CommandPermissions::AllAllowed => "+@all".to_string(),
        CommandPermissions::Specific {
            base_allow,
            allowed,
            denied,
        } => {
            // Bare `cmd` tokens first, `cmd|arg` tokens after: on reload a
            // bare `+cmd` / `-cmd` clears every `cmd|*` rule applied before
            // it (`acl::subcommand`), so a `-config|set` written ahead of a
            // `+config` would reload as `CONFIG SET` allowed -- fail-open.
            // `cmd|arg` rules are always the newer ones in the live set, so
            // this order reproduces them exactly.
            fn sorted(set: &std::collections::HashSet<String>, sub: bool) -> Vec<&String> {
                let mut v: Vec<&String> = set.iter().filter(|r| r.contains('|') == sub).collect();
                v.sort();
                v
            }
            let mut parts: Vec<String> = Vec::with_capacity(1 + allowed.len() + denied.len());
            parts.push(if *base_allow { "+@all" } else { "-@all" }.to_string());
            for sub in [false, true] {
                let grants = sorted(allowed, sub).into_iter().map(|a| format!("+{a}"));
                let revocations = sorted(denied, sub).into_iter().map(|d| format!("-{d}"));
                if *base_allow {
                    parts.extend(revocations);
                    parts.extend(grants);
                } else {
                    parts.extend(grants);
                    parts.extend(revocations);
                }
            }
            parts.join(" ")
        }
    }
}

/// Parse a single ACL file line. Returns None for blank/comment lines.
pub fn parse_acl_line(line: &str) -> Option<AclUser> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let mut tokens = line.split_whitespace();
    if tokens.next()? != "user" {
        return None;
    }
    let username = tokens.next()?.to_string();
    let mut user = AclUser::default_deny(username.clone());
    for token in tokens {
        // #978/#979: a rule the grammar rejects (unknown category, unknown
        // token, bad hash, ...) rejects the whole
        // LINE. Applying the rest would hand back a user whose permissions are
        // not the ones the file asked for, and the failure mode this fixes was
        // exactly a silently-wrong permission set. Dropping the user is
        // fail-closed -- an absent user is denied by
        // `check_command_permission` -- but it is silent on its own, so it is
        // logged at WARN.
        if let Err(err) = apply_rule(&mut user, token) {
            tracing::warn!(
                user = %username,
                rule = %token,
                error = %err,
                "rejecting ACL file line: invalid rule; user not loaded"
            );
            return None;
        }
    }
    Some(user)
}

/// Save all users to an ACL file.
///
/// Atomic via `atomic_write_durable` (task #49, kernel M4 prep): temp
/// file, fsync, rename, parent-dir fsync, so a kill-9 mid-save can never
/// leave a torn or (post-rename, pre-dir-fsync) reverted ACL file. The
/// prior code did a bare tmp-write + rename with no fsync at all -- on
/// ext4/xfs a crash right after `rename()` returns can still lose the
/// directory-entry update, silently reverting `path` to its old contents
/// (or nothing at all, on first save).
pub fn acl_save(path: &str, table: &AclTable) -> std::io::Result<()> {
    let mut content = String::new();
    for user in table.list_users() {
        content.push_str(&user_to_acl_line(user));
        content.push('\n');
    }
    crate::persistence::atomic::atomic_write_durable(
        std::path::Path::new(path),
        content.as_bytes(),
    )?;
    Ok(())
}

/// Load an ACL table from file. Returns error if file can't be read.
pub fn acl_load(path: &str) -> std::io::Result<AclTable> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut table = AclTable::new();
    for line in reader.lines() {
        let line = line?;
        if let Some(user) = parse_acl_line(&line) {
            table.set_user(user.username.clone(), user);
        }
    }
    Ok(table)
}

/// Load AclTable from config: uses aclfile if configured, otherwise bootstraps from requirepass.
/// Non-fatal: if aclfile missing or unreadable, falls back to requirepass-based defaults.
pub fn acl_table_from_config(config: &crate::config::ServerConfig) -> AclTable {
    if let Some(ref path) = config.aclfile {
        match acl_load(path) {
            Ok(mut table) => {
                // c10k B2: the permission checks now DENY an unknown user, so
                // an ACL file that never defines `default` would lock out
                // every connection. Redis guarantees `default` exists; so do
                // we, seeded from requirepass exactly as the no-file path below.
                table.ensure_default_user(config.requirepass.as_deref());
                return table;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File doesn't exist yet -- start with defaults
            }
            Err(e) => {
                eprintln!("Warning: could not load ACL file '{}': {}", path, e);
            }
        }
    }
    // Bootstrap default user from requirepass (NOT load_or_default to avoid recursion)
    let mut table = AclTable::new();
    let default_user = match &config.requirepass {
        Some(p) if !p.is_empty() => AclUser::new_default_with_password(p),
        _ => AclUser::new_default_nopass(),
    };
    table.set_user("default".to_string(), default_user);
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::rules::hash_password;
    use crate::acl::table::KeyPattern;

    #[test]
    fn test_user_to_acl_line_default_nopass() {
        let user = AclUser::new_default_nopass();
        let line = user_to_acl_line(&user);
        assert!(line.starts_with("user default on"));
        assert!(line.contains("nopass"));
        assert!(line.contains("~*"));
        assert!(line.contains("&*"));
        assert!(line.contains("+@all"));
    }

    #[test]
    fn test_user_to_acl_line_with_password() {
        let user = AclUser::new_default_with_password("secret");
        let line = user_to_acl_line(&user);
        assert!(line.contains(&format!("#{}", hash_password("secret"))));
        assert!(!line.contains("nopass"));
    }

    #[test]
    fn test_parse_acl_line_basic() {
        let line = "user alice on nopass ~* &* +@all";
        let user = parse_acl_line(line).unwrap();
        assert_eq!(user.username, "alice");
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::AllAllowed
        ));
    }

    #[test]
    fn test_parse_acl_line_comment() {
        assert!(parse_acl_line("# this is a comment").is_none());
        assert!(parse_acl_line("").is_none());
        assert!(parse_acl_line("  ").is_none());
    }

    #[test]
    fn test_parse_acl_line_non_user() {
        assert!(parse_acl_line("notuser alice on").is_none());
    }

    #[test]
    fn test_roundtrip_user_to_acl_line_and_parse() {
        let user = AclUser::new_default_nopass();
        let line = user_to_acl_line(&user);
        let parsed = parse_acl_line(&line).unwrap();
        assert_eq!(parsed.username, user.username);
        assert_eq!(parsed.enabled, user.enabled);
        assert_eq!(parsed.nopass, user.nopass);
        assert!(matches!(
            parsed.allowed_commands,
            CommandPermissions::AllAllowed
        ));
    }

    #[test]
    fn test_acl_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.acl");
        let path_str = path.to_str().unwrap();

        let mut table = AclTable::new();
        let mut alice = AclUser::default_deny("alice".to_string());
        alice.enabled = true;
        alice.nopass = true;
        alice.key_patterns.push(KeyPattern {
            pattern: "*".to_string(),
            read: true,
            write: true,
        });
        alice.channel_patterns.push("*".to_string());
        alice.allowed_commands = CommandPermissions::AllAllowed;
        table.set_user("alice".to_string(), alice);

        let default_user = AclUser::new_default_nopass();
        table.set_user("default".to_string(), default_user);

        acl_save(path_str, &table).unwrap();

        let loaded = acl_load(path_str).unwrap();
        let alice_loaded = loaded.get_user("alice").unwrap();
        assert!(alice_loaded.enabled);
        assert!(alice_loaded.nopass);
        assert!(matches!(
            alice_loaded.allowed_commands,
            CommandPermissions::AllAllowed
        ));

        let default_loaded = loaded.get_user("default").unwrap();
        assert!(default_loaded.enabled);
        assert!(default_loaded.nopass);
    }

    /// Task #49: `acl_save` must go through `atomic_write_durable`, not a
    /// bare tmp-write+rename. Regression pin: no leftover `.tmp` file and
    /// the directory holds exactly the final `.acl` file after a
    /// successful save -- what a hand-rolled `format!("{}.tmp", path)`
    /// helper (the pre-fix code) would also achieve on the happy path, but
    /// silently skip the fsync/dir-fsync durability steps.
    #[test]
    fn test_acl_save_leaves_no_leftover_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.acl");
        let path_str = path.to_str().unwrap();

        let mut table = AclTable::new();
        table.set_user("default".to_string(), AclUser::new_default_nopass());

        acl_save(path_str, &table).unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("test.acl")]);
    }

    /// Build a user the way `ACL SETUSER` / the file loader do: through the
    /// rule parser, so the test exercises the same `CommandPermissions`
    /// transitions a live server performs.
    fn user_from_rules(rules: &[&str]) -> AclUser {
        let mut user = AclUser::default_deny("rt".to_string());
        for rule in rules {
            // moon#978 made `apply_rule` fallible. Discarding the result here
            // would let a fixture rule fail silently and build a DIFFERENT
            // user than the test names, so the helper asserts instead.
            assert!(
                apply_rule(&mut user, rule).is_ok(),
                "fixture rule {rule:?} must apply"
            );
        }
        user
    }

    /// moon#981. `CommandPermissions::Specific` carries `base_allow`
    /// (moon#971), but the serializer dropped it and wrote `-@all` for EVERY
    /// Specific user. A `+@all -flushall` user therefore came back from
    /// `ACL LOAD` (or a restart with `--aclfile`) able to run nothing at all.
    /// Redis 8.6.1 writes the line as `... +@all -flushall`, verified on the
    /// wire against a real `aclfile`.
    #[test]
    fn allow_all_base_with_one_revocation_survives_the_line_roundtrip() {
        let user = user_from_rules(&["+@all", "-flushall"]);
        assert!(
            user.is_command_allowed("get"),
            "precondition: base is allow"
        );
        assert!(
            !user.is_command_allowed("flushall"),
            "precondition: -flushall took"
        );

        let line = user_to_acl_line(&user);
        assert!(
            line.ends_with(" +@all -flushall"),
            "the written rules must carry the allow-all base, got: {line}"
        );

        let reloaded = parse_acl_line(&line).expect("a user line parses");
        assert!(
            reloaded.is_command_allowed("get"),
            "OUTAGE: the reloaded user lost every grant except the one revocation"
        );
        assert!(reloaded.is_command_allowed("hset"));
        assert!(
            !reloaded.is_command_allowed("flushall"),
            "the one revocation must survive too"
        );
    }

    /// Same defect, with a category revocation and a re-grant inside it:
    /// `+@all -@string +get`. Everything outside `@string` stays allowed,
    /// `set` stays denied, `get` is back. The re-grant is the case where the
    /// `allowed` set is non-empty under an allow-all base, so a serializer
    /// that emits `allowed` alone would still read as a deny-all user.
    #[test]
    fn allow_all_base_with_category_revocation_and_regrant_survives_the_roundtrip() {
        let user = user_from_rules(&["+@all", "-@string", "+get"]);
        let line = user_to_acl_line(&user);
        assert!(
            line.contains(" +@all "),
            "allow-all base missing from: {line}"
        );
        assert!(
            !line.contains("-@all"),
            "deny-all base written for an allow-all user: {line}"
        );

        let reloaded = parse_acl_line(&line).expect("a user line parses");
        assert!(
            reloaded.is_command_allowed("hset"),
            "outside the revoked category"
        );
        assert!(reloaded.is_command_allowed("get"), "re-granted inside it");
        assert!(!reloaded.is_command_allowed("set"), "still revoked");
        assert!(!reloaded.is_command_allowed("append"), "still revoked");
    }

    /// Control: a deny-all base was already written correctly, and must keep
    /// being written as `-@all` followed by the grants. Pins the polarity so
    /// the moon#981 fix cannot overshoot into the other direction, which would
    /// be an ESCALATION rather than an outage.
    #[test]
    fn deny_all_base_with_grants_still_writes_minus_all() {
        let user = user_from_rules(&["-@all", "+get", "+set"]);
        let line = user_to_acl_line(&user);
        assert!(line.ends_with(" -@all +get +set"), "got: {line}");

        let reloaded = parse_acl_line(&line).expect("a user line parses");
        assert!(reloaded.is_command_allowed("get"));
        assert!(reloaded.is_command_allowed("set"));
        assert!(
            !reloaded.is_command_allowed("hset"),
            "ESCALATION: base must stay deny"
        );
        assert!(
            !reloaded.is_command_allowed("flushall"),
            "ESCALATION: base must stay deny"
        );
    }

    /// moon#981 end to end through the file: `acl_save` then `acl_load`, the
    /// exact path `ACL SAVE` + restart-with-`--aclfile` takes.
    #[test]
    fn acl_save_load_preserves_the_allow_all_base_of_a_restricted_user() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.acl");
        let path_str = path.to_str().unwrap();

        let mut table = AclTable::new();
        let mut rt = user_from_rules(&["on", "+@all", "-flushall"]);
        rt.nopass = true;
        table.set_user("rt".to_string(), rt);
        table.set_user("default".to_string(), AclUser::new_default_nopass());

        acl_save(path_str, &table).unwrap();
        let loaded = acl_load(path_str).unwrap();
        let rt = loaded.get_user("rt").unwrap();

        assert!(
            rt.is_command_allowed("get"),
            "OUTAGE: grants lost across SAVE/LOAD"
        );
        assert!(rt.is_command_allowed("set"));
        assert!(
            !rt.is_command_allowed("flushall"),
            "the revocation must survive"
        );
    }

    /// A pattern with neither flag grants nothing and must render as
    /// nothing. The GETUSER copy this replaced rendered it `%W~pat`, which
    /// reloads as a WRITE grant.
    #[test]
    fn key_pattern_with_no_access_renders_as_nothing() {
        let kp = |read, write| KeyPattern {
            pattern: "p".to_string(),
            read,
            write,
        };
        assert_eq!(key_pattern_to_rule(&kp(true, true)).as_deref(), Some("~p"));
        assert_eq!(
            key_pattern_to_rule(&kp(true, false)).as_deref(),
            Some("%R~p")
        );
        assert_eq!(
            key_pattern_to_rule(&kp(false, true)).as_deref(),
            Some("%W~p")
        );
        assert_eq!(key_pattern_to_rule(&kp(false, false)), None);
    }

    /// moon#970/#979/#999/#981: every permission set this change touches
    /// survives `ACL SETUSER` -> `ACL LIST` -> `ACL SAVE` -> restart with
    /// `ACL LOAD` IDENTICALLY -- the rendered line is a fixed point, token
    /// order included, and the reloaded user enforces the same thing.
    #[test]
    fn every_touched_permission_set_is_a_save_load_fixed_point() {
        let cases: &[&[&str]] = &[
            // #981: allow base with a revocation; order matters under +@all.
            &["on", ">pw", "~*", "&*", "+@all", "-flushall"],
            &["on", ">pw", "~*", "+@all", "-@string", "+get"],
            // #970: the three all* keywords and the % selectors.
            &["on", ">pw", "allkeys", "allcommands", "allchannels"],
            &["on", ">pw", "ALLKEYS", "ALLCOMMANDS"],
            &["on", ">pw", "%RW~cache:*", "%r~r:*", "%W~w:*", "+@read"],
            &[
                "on",
                ">pw",
                "~a",
                "%R~b",
                "allkeys",
                "&c",
                "allchannels",
                "+@all",
            ],
            // #979: revocations.
            &["on", ">pw", "~*", "&*", "+@all", "nocommands"],
            &["on", ">pw", "~*", "&*", "+@all", "OFF"],
            &[
                "on",
                ">pw",
                "~*",
                "&*",
                "+@all",
                "RESETKEYS",
                "RESETCHANNELS",
            ],
            // #999: credentials.
            &["on", "nopass", "~*", "+@all", ">pw"],
            &["on", ">old", "nopass", ">new", "~*", "+@all"],
            &["on", ">pw", "nopass", "~*", "+@all"],
        ];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rt.acl");
        let path_str = path.to_str().unwrap();

        for rules in cases {
            let mut table = AclTable::new();
            table.set_user("default".to_string(), AclUser::new_default_nopass());
            table.apply_setuser("rt", rules);
            let before = user_to_acl_line(table.get_user("rt").unwrap());

            acl_save(path_str, &table).unwrap();
            let loaded = acl_load(path_str).unwrap();
            let reloaded = loaded
                .get_user("rt")
                .unwrap_or_else(|| panic!("{rules:?}: user lost across SAVE/LOAD: {before}"));
            let after = user_to_acl_line(reloaded);
            assert_eq!(after, before, "{rules:?}: SAVE/LOAD changed the line");

            let original = table.get_user("rt").unwrap();
            for cmd in ["get", "set", "flushall", "ping", "publish", "hset"] {
                assert_eq!(
                    reloaded.is_command_allowed(cmd),
                    original.is_command_allowed(cmd),
                    "{rules:?}: `{cmd}` changed across SAVE/LOAD"
                );
            }
            for pw in ["pw", "old", "new", "wrong"] {
                assert_eq!(
                    loaded.authenticate("rt", pw),
                    table.authenticate("rt", pw),
                    "{rules:?}: AUTH rt {pw} changed across SAVE/LOAD"
                );
            }
        }
    }
}
