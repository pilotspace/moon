//! The dispatch-time ACL verdict for one resolved user (moon#1165).
//!
//! These are the bodies of `AclTable::check_command_permission` /
//! `check_key_permission`, moved onto [`AclUser`] so a connection can run
//! them against its own immutable snapshot of the user
//! (`ConnectionState::acl_denial`) without the table lock; the table methods
//! delegate here, so there is ONE implementation of each check.

use crate::protocol::Frame;

use super::subcommand::{command_log_object, first_arg, permits};
use super::table::AclUser;

/// Why the dispatch-time gate refused a command (moon#1165). Handlers log the
/// two differently (`ACL LOG` object), so the kind travels with the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AclDenial {
    /// The command (or its `cmd|arg` form) is not permitted.
    Command(String),
    /// A key the command touches is outside the user's key patterns.
    Key(String),
}

impl AclUser {
    /// Command-permission verdict for this user: `None` when `cmd` (with its
    /// `argv[1]`) may run, the `NOPERM` reason otherwise. `username` is the
    /// name the caller resolved this user under, used only in the reason.
    ///
    /// The body of [`AclTable::check_command_permission`], moved here so a
    /// connection can run it against its own snapshot of the user without the
    /// table lock (moon#1165). Allocation-free on the allowed path: the
    /// command name is lowercased into a stack buffer, not a `String`.
    pub fn command_denial(&self, username: &str, cmd: &[u8], args: &[Frame]) -> Option<String> {
        // Hot path: unrestricted user (default `on nopass ~* &* +@all`)
        // short-circuits before any per-command work. The unrestricted check
        // is a single bool load.
        if self.unrestricted() {
            return None;
        }
        if !self.enabled {
            return Some(format!("User {} is disabled", username));
        }
        // Lowercase `cmd` for the rule sets. A name that is not UTF-8 is
        // probed as "" (it can equal no stored rule), exactly as the old
        // `from_utf8(cmd).unwrap_or("").to_ascii_lowercase()` did; a name
        // longer than the stack buffer (no real command is) takes the old
        // allocating path.
        const NAME_BUF: usize = 64;
        let mut buf = [0u8; NAME_BUF];
        let heap;
        let cmd_lower: &str = if cmd.len() <= NAME_BUF {
            for (d, s) in buf.iter_mut().zip(cmd) {
                *d = s.to_ascii_lowercase();
            }
            std::str::from_utf8(&buf[..cmd.len()]).unwrap_or("")
        } else {
            heap = std::str::from_utf8(cmd).unwrap_or("").to_ascii_lowercase();
            &heap
        };
        // `argv[1]` takes part: `-config|set` / `+select|0` rules are keyed on
        // it. Probing the bare name alone let `+@all -config|set` run
        // `CONFIG SET` (see `acl::subcommand`).
        if !permits(&self.allowed_commands, cmd_lower, first_arg(args)) {
            return Some(format!(
                "User {} has no permissions to run the '{}' command",
                username,
                command_log_object(cmd, args)
            ));
        }
        None
    }

    /// Key-permission verdict for this user — the body of
    /// [`AclTable::check_key_permission`] (moon#1165, see
    /// [`Self::command_denial`]).
    pub fn key_denial(
        &self,
        username: &str,
        cmd: &[u8],
        args: &[Frame],
        is_write: bool,
    ) -> Option<String> {
        // Hot path: unrestricted user skips key extraction (keyspec) + the
        // O(patterns*keys) glob match loop. Profile showed ~1.2% of CPU
        // here, most of it in glob_match and Vec allocation for the
        // extracted keys.
        if self.unrestricted() {
            return None;
        }

        // NOTE (#979): there used to be an early `key_patterns.is_empty()`
        // deny here, ahead of the keyless-command check below. It made a user
        // with no key patterns unable to run PING, DBSIZE or any other command
        // that names no key -- redis gates only KEYED commands on key
        // patterns (`RESETKEYS` then `FLUSHALL` is permitted there). The loop
        // at the bottom already denies every keyed command when the pattern
        // list is empty (`any` over nothing is false), so removing the early
        // return loses no protection.
        //
        // ~* (read+write) shortcut -- fast path for users that have
        // unrestricted keys but restricted commands (so `unrestricted`
        // above was false for other reasons).
        if self
            .key_patterns
            .iter()
            .any(|kp| kp.pattern == "*" && kp.read && kp.write)
        {
            return None;
        }
        // moon#566: key extraction is derived from the command registry's key
        // specs and fails CLOSED. The old hand-maintained match returned an
        // empty vec for anything it did not name — and an empty key list does
        // not mean "check less precisely", it means the loop below never runs,
        // so every `~pattern` was silently ignored for that command.
        let keys = match super::keyspec::command_keys(cmd, args) {
            // The command provably names no key (PING, CONFIG, SUBSCRIBE...):
            // there is nothing for key patterns to gate.
            super::keyspec::CommandKeys::None => return None,
            super::keyspec::CommandKeys::Keys(keys) => keys,
            // Known (or suspected) to touch keys, but this argv could not be
            // enumerated: deny, and say so once per command name so a missing
            // key spec is visible in the log rather than silently permissive.
            super::keyspec::CommandKeys::Indeterminate => {
                super::keyspec::warn_indeterminate(cmd);
                return Some(format!(
                    "User {} has no permissions to access one of the keys used as arguments to '{}'",
                    username,
                    String::from_utf8_lossy(cmd).to_ascii_lowercase()
                ));
            }
        };
        for key in keys {
            let key_str = std::str::from_utf8(key).unwrap_or("");
            let allowed = self.key_patterns.iter().any(|kp| {
                let access_ok = if is_write { kp.write } else { kp.read };
                access_ok && crate::command::key::glob_match(kp.pattern.as_bytes(), key)
            });
            if !allowed {
                return Some(format!(
                    "User {} has no permissions to access key '{}'",
                    username, key_str
                ));
            }
        }
        None
    }

    /// The dispatch-time gate for one command: the command check, then the
    /// key check — the order every handler has always used — on this one
    /// resolved user (one lookup instead of two, moon#1165).
    pub fn denial(&self, username: &str, cmd: &[u8], args: &[Frame]) -> Option<AclDenial> {
        if let Some(reason) = self.command_denial(username, cmd, args) {
            return Some(AclDenial::Command(reason));
        }
        if self.unrestricted() {
            return None;
        }
        let is_write = crate::command::metadata::is_write(cmd);
        self.key_denial(username, cmd, args, is_write)
            .map(AclDenial::Key)
    }
}
