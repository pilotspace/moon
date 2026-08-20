//! The caller's ACL identity, carried into a Lua script's `redis.call`
//! bridge (moon#569).
//!
//! # Why this exists
//!
//! `EVAL script numkeys k1 k2 ...` is ACL-checked like any other command:
//! the dispatcher runs [`AclTable::check_command_permission`] on `EVAL` and
//! [`AclTable::check_key_permission`] on the keys the invocation DECLARES.
//! But `redis.call()` inside the script reached `Database::execute_command`
//! directly, so the declaration was the *only* thing checked — and a script
//! is free not to declare anything:
//!
//! ```text
//! ACL SETUSER app on >pw ~app:* +@all
//! EVAL "return redis.call('GET', 'secret:x')" 0   -> "leaked"
//! ```
//!
//! `numkeys 0` made the outer key check a no-op (no keys, empty loop), and
//! nothing downstream looked again. Upstream Redis instead runs every
//! script-issued command under the calling user's permissions; this type is
//! how moon threads that user from the connection that issued `EVAL`/`FCALL`
//! down to each individual `redis.call`.
//!
//! # Fail-closed contract
//!
//! The default is [`ScriptAcl::deny`] — an execution path that reaches a Lua
//! VM without supplying an identity refuses every inner command rather than
//! running it unchecked. Every producer is therefore forced by the compiler
//! to make an explicit choice, and forgetting fails loudly instead of
//! silently granting `~*`.
//!
//! # Hot path
//!
//! The common case (unrestricted user, no ACL mutation since the script
//! started) costs one enum discriminant test plus one `Acquire` load of the
//! ACL version counter per `redis.call` — no lock, no allocation, no key
//! extraction. The freshness check is what makes the cached verdict safe:
//! an `ACL SETUSER` landing mid-script bumps the counter, and the very next
//! `redis.call` re-resolves under the lock. This is exactly the
//! `ConnectionState::acl_skip_allowed` pattern, for the same reason.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::table::AclTable;
use crate::protocol::Frame;

/// Prefix every script-ACL denial carries.
///
/// Two jobs: it makes the reply detectable as a permission failure by
/// clients that match on `NOPERM` (same as the dispatch-level gate), and it
/// lets the script runners recognise their own denial after `mlua` has
/// wrapped it in a `RuntimeError`, so the caller sees a clean
/// `-NOPERM ...` instead of `-ERR Error running script: runtime error: ...`.
pub const SCRIPT_ACL_DENIED_PREFIX: &str = "NOPERM ACL failure in script: ";

/// Build the wire error for a denial reason.
#[must_use]
pub fn script_acl_error(reason: &str) -> String {
    format!("{SCRIPT_ACL_DENIED_PREFIX}{reason}")
}

/// A resolved ACL identity plus the cached verdict that lets the hot path
/// skip the lock.
struct ScriptAclUser {
    table: Arc<std::sync::RwLock<AclTable>>,
    username: Box<str>,
    /// Shared ACL mutation counter (`AclTable::version_handle`). Survives
    /// `ACL LOAD`, which swaps the user set but preserves this handle.
    version: Arc<AtomicU64>,
    /// `version` as observed when this identity was resolved.
    snapshot: u64,
    /// Whether `username` was unrestricted at `snapshot`. Only trusted while
    /// `version` still equals `snapshot`.
    unrestricted: bool,
}

enum Repr {
    /// No identity available — refuse every inner command.
    Deny,
    /// Caller is trusted by construction and is not subject to user ACLs.
    Trusted,
    /// Enforce this user's ACL, re-resolved whenever the table changes.
    User(ScriptAclUser),
}

/// The identity a script's `redis.call`/`redis.pcall` run under.
///
/// Cheap to clone (`Arc` bump) and `Send`, so it can ride a
/// `ShardMessage::Execute` to the shard that owns the script's keys.
#[derive(Clone)]
pub struct ScriptAcl(Arc<Repr>);

// Hand-written: `AclTable` is not `Debug`, and printing a user's rules into a
// log line would be the wrong default anyway. Only the MODE is shown.
impl std::fmt::Debug for ScriptAcl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self.0 {
            Repr::Deny => f.write_str("ScriptAcl::Deny"),
            Repr::Trusted => f.write_str("ScriptAcl::Trusted"),
            Repr::User(u) => write!(f, "ScriptAcl::User({})", u.username),
        }
    }
}

impl Default for ScriptAcl {
    fn default() -> Self {
        Self::deny()
    }
}

impl ScriptAcl {
    /// Fail-closed identity: every inner command is refused.
    ///
    /// This is the default state of the bridge, and what non-script
    /// `ShardMessage::Execute` producers carry — if a script ever reaches a
    /// shard through one of those paths, it is refused rather than run with
    /// nobody's permissions.
    #[must_use]
    pub fn deny() -> Self {
        // One process-wide allocation, not one per message.
        static DENY: std::sync::LazyLock<Arc<Repr>> =
            std::sync::LazyLock::new(|| Arc::new(Repr::Deny));
        ScriptAcl(Arc::clone(&DENY))
    }

    /// Identity for a caller that is trusted by construction and has no ACL
    /// user behind it.
    ///
    /// The ONLY production user is the admin console gateway
    /// (`src/admin/console_gateway.rs`), which is a separate operator-
    /// authenticated plane with no ACL surface at all — it can already call
    /// anything, so making its scripts refuse would be theatre, not
    /// security. Unit tests that have no `AclTable` use it too.
    ///
    /// Never hand this to anything reachable from a client connection.
    #[must_use]
    pub fn trusted() -> Self {
        static TRUSTED: std::sync::LazyLock<Arc<Repr>> =
            std::sync::LazyLock::new(|| Arc::new(Repr::Trusted));
        ScriptAcl(Arc::clone(&TRUSTED))
    }

    /// Resolve `username` against `table` once, at script-start time.
    ///
    /// Takes the read lock exactly once per script invocation (not per
    /// `redis.call`) to snapshot the "unrestricted" verdict and the ACL
    /// version it was valid at.
    #[must_use]
    pub fn for_user(table: &Arc<std::sync::RwLock<AclTable>>, username: &str) -> Self {
        #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
        let (unrestricted, snapshot, version) = {
            let guard = table.read().unwrap();
            (
                guard.is_user_unrestricted(username),
                guard.version(),
                guard.version_handle(),
            )
        };
        ScriptAcl(Arc::new(Repr::User(ScriptAclUser {
            table: Arc::clone(table),
            username: username.into(),
            version,
            snapshot,
            unrestricted,
        })))
    }

    /// Whether this identity is subject to per-command ACL checks at all.
    /// Diagnostics/tests only — the check itself is [`Self::check`].
    #[must_use]
    pub fn is_enforcing(&self) -> bool {
        matches!(&*self.0, Repr::User(_) | Repr::Deny)
    }

    /// Authorize one script-issued command. `args` EXCLUDES the command name.
    ///
    /// Returns `None` when allowed, `Some(reason)` when denied. Mirrors the
    /// dispatch-level gate (`try_enforce_acl`) exactly: command permission
    /// first, then key patterns, with the same fail-closed key extraction.
    #[inline]
    #[must_use]
    pub fn check(&self, cmd: &[u8], args: &[Frame]) -> Option<String> {
        match &*self.0 {
            // Hot path for the admin plane: one discriminant test.
            Repr::Trusted => None,
            Repr::Deny => Some(
                "this script was executed without an ACL identity, so no command may run"
                    .to_string(),
            ),
            Repr::User(user) => {
                // Hot path for unrestricted users: one Acquire load. A stale
                // "unrestricted" verdict would be a privilege-escalation bug
                // if ACL SETUSER has since revoked the user, so the version
                // must still match — same invariant as
                // `ConnectionState::acl_skip_allowed`.
                if user.unrestricted && user.version.load(Ordering::Acquire) == user.snapshot {
                    return None;
                }
                Self::check_locked(user, cmd, args)
            }
        }
    }

    /// Cold path: re-resolve under the ACL read lock.
    #[cold]
    fn check_locked(user: &ScriptAclUser, cmd: &[u8], args: &[Frame]) -> Option<String> {
        #[allow(clippy::unwrap_used)] // std RwLock: poison = prior panic = unrecoverable
        let guard = user.table.read().unwrap();
        if let Some(reason) = guard.check_command_permission(&user.username, cmd, args) {
            return Some(reason);
        }
        // Same `is_write` source the dispatcher uses, so a command's ACL
        // read/write class cannot differ depending on whether it was issued
        // directly or from Lua.
        let is_write = crate::command::metadata::is_write(cmd);
        guard.check_key_permission(&user.username, cmd, args, is_write)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn table_with(user: &str, rules: &[&str]) -> Arc<std::sync::RwLock<AclTable>> {
        let mut t = AclTable::new();
        t.ensure_default_user(None);
        t.apply_setuser(user, rules);
        Arc::new(std::sync::RwLock::new(t))
    }

    fn arg(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn default_is_deny_not_allow() {
        // The single most important property: a path that forgets to supply
        // an identity must refuse, not fall open.
        let acl = ScriptAcl::default();
        assert!(acl.check(b"GET", &[arg("anything")]).is_some());
        assert!(acl.check(b"PING", &[]).is_some());
        assert!(ScriptAcl::deny().check(b"GET", &[arg("k")]).is_some());
    }

    #[test]
    fn trusted_allows_everything() {
        let acl = ScriptAcl::trusted();
        assert!(acl.check(b"GET", &[arg("k")]).is_none());
        assert!(acl.check(b"FLUSHALL", &[]).is_none());
    }

    #[test]
    fn restricted_user_is_gated_on_key_patterns() {
        let table = table_with("app", &["on", ">pw", "~app:*", "+@all"]);
        let acl = ScriptAcl::for_user(&table, "app");
        assert!(acl.check(b"GET", &[arg("app:ok")]).is_none());
        assert!(acl.check(b"GET", &[arg("secret:x")]).is_some());
        assert!(acl.check(b"SET", &[arg("secret:x"), arg("v")]).is_some());
    }

    #[test]
    fn restricted_user_is_gated_on_commands() {
        let table = table_with("nodel", &["on", ">pw", "~*", "+@all", "-del"]);
        let acl = ScriptAcl::for_user(&table, "nodel");
        assert!(acl.check(b"GET", &[arg("k")]).is_none());
        assert!(acl.check(b"DEL", &[arg("k")]).is_some());
    }

    #[test]
    fn movable_key_commands_are_enforced() {
        let table = table_with("app", &["on", ">pw", "~app:*", "+@all"]);
        let acl = ScriptAcl::for_user(&table, "app");
        // numkeys-counted key vector
        assert!(
            acl.check(b"LMPOP", &[arg("1"), arg("secret:l"), arg("LEFT")])
                .is_some()
        );
        // positional STORE clause
        assert!(
            acl.check(
                b"SORT",
                &[arg("app:l"), arg("ALPHA"), arg("STORE"), arg("secret:d")]
            )
            .is_some()
        );
        // runtime-computed weight keys -> indeterminate -> deny
        assert!(
            acl.check(b"SORT", &[arg("app:l"), arg("BY"), arg("secret:w_*")])
                .is_some()
        );
        // COPY destination
        assert!(
            acl.check(b"COPY", &[arg("app:a"), arg("secret:b")])
                .is_some()
        );
    }

    #[test]
    fn unknown_command_fails_closed() {
        let table = table_with("app", &["on", ">pw", "~app:*", "+@all"]);
        let acl = ScriptAcl::for_user(&table, "app");
        assert!(acl.check(b"NOTACOMMAND", &[arg("app:ok")]).is_some());
    }

    #[test]
    fn unrestricted_user_short_circuits_but_re_resolves_on_revocation() {
        let table = table_with("wide", &["on", ">pw", "~*", "&*", "+@all"]);
        let acl = ScriptAcl::for_user(&table, "wide");
        assert!(acl.check(b"GET", &[arg("secret:x")]).is_none());

        // Revoke MID-SCRIPT: the cached "unrestricted" verdict must not
        // survive the version bump.
        #[allow(clippy::unwrap_used)]
        table
            .write()
            .unwrap()
            .apply_setuser("wide", &["resetkeys", "~app:*"]);
        assert!(
            acl.check(b"GET", &[arg("secret:x")]).is_some(),
            "cached unrestricted verdict outlived the revocation"
        );
        assert!(acl.check(b"GET", &[arg("app:ok")]).is_none());
    }

    #[test]
    fn deleted_user_is_denied_not_promoted() {
        let table = table_with("app", &["on", ">pw", "~app:*", "+@all"]);
        let acl = ScriptAcl::for_user(&table, "app");
        #[allow(clippy::unwrap_used)]
        table.write().unwrap().del_user("app");
        assert!(acl.check(b"GET", &[arg("app:ok")]).is_some());
    }

    #[test]
    fn error_string_is_client_detectable() {
        assert!(script_acl_error("nope").starts_with("NOPERM"));
    }
}
