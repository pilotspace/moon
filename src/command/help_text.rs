//! Static `HELP` bodies for the container commands — moon#698.
//!
//! Redis answers `<CONTAINER> HELP` with an array of **simple** strings that
//! always opens with
//! `<CONTAINER> <subcommand> [<arg> [value] [opt] ...]. Subcommands are:`
//! and always closes with `HELP` / `    Print this help.` (measured across all
//! 13 containers against `redis-server 8.6.1`, 2026-08-24).
//!
//! Only the BODY lines live here — [`crate::command::helpers::help_reply`]
//! supplies the header and the footer — so a container cannot be added with a
//! divergent shape. That uniformity is the point: judged only on "an array came
//! back", `OBJECT`/`MEMORY`/`SLOWLOG` looked correct while emitting bulk
//! strings and no header line.
//!
//! # The body advertises Moon's surface, not Redis's
//!
//! Copying Redis's own help text would advertise subcommands and options Moon
//! does not dispatch — `CLIENT KILL` here supports `ID`/`ADDR`/`USER` and the
//! legacy `addr:port` only (`client_registry::parse_kill_args`), `CLIENT LIST`
//! has no `TYPE` filter, and `FUNCTION DUMP`/`RESTORE`/`STATS` are recognised
//! names that answer "not supported in this release". Advertising any of them
//! would reproduce moon#640's defect one level up: help text that points the
//! reader at something that does not work.
//!
//! `tests/container_help_698.rs::ch2` walks every name in `SUBCOMMAND_META` and
//! asserts it opens a line here, and `ch3` asserts the recognised-but-refused
//! names do NOT appear, so the two tables cannot drift apart in either
//! direction.

use crate::protocol::Frame;

/// Body lines for one container, keyed by its canonical upper-case name.
///
/// Kept as a plain slice-of-pairs rather than a `phf` map: it is walked once
/// per `HELP` — an introspection command, never a hot path — and a linear scan
/// over 13 entries keeps the table readable as one block.
const HELP_BODIES: &[(&str, &[&str])] = &[
    (
        "ACL",
        &[
            "CAT [<category>]",
            "    List all commands that belong to <category>, or all command categories",
            "    when no category is specified.",
            "DELUSER <username> [<username> ...]",
            "    Delete a list of users.",
            "GETUSER <username>",
            "    Get the user's details.",
            "GENPASS [<bits>]",
            "    Generate a secure 256-bit user password. The optional `bits` argument can",
            "    be used to specify a different size.",
            "LIST",
            "    Show users details in config file format.",
            "LOAD",
            "    Reload users from the ACL file.",
            "LOG [<count> | RESET]",
            "    Show the ACL log entries.",
            "SAVE",
            "    Save the current config to the ACL file.",
            "SETUSER <username> <attribute> [<attribute> ...]",
            "    Create or modify a user with the specified attributes.",
            "USERS",
            "    List all the registered usernames.",
            "WHOAMI",
            "    Return the current connection username.",
        ],
    ),
    (
        "CLIENT",
        &[
            "GETNAME",
            "    Return the name of the current connection.",
            "ID",
            "    Return the ID of the current connection.",
            "INFO",
            "    Return information about the current client connection.",
            "KILL <ip:port>",
            "    Kill connection made from <ip:port>.",
            "KILL <option> <value> [<option> <value> [...]]",
            "    Kill connections. Options are:",
            "    * ADDR (<ip:port>|<unixsocket>:0)",
            "      Kill connections made from the specified address",
            "    * ID <client-id>",
            "      Kill connections by client id.",
            "    * USER <username>",
            "      Kill connections authenticated by <username>.",
            "LIST",
            "    Return information about client connections.",
            "NO-EVICT (ON|OFF)",
            "    Protect current client connection from eviction.",
            "NO-TOUCH (ON|OFF)",
            "    Will not touch LRU/LFU stats when this mode is on.",
            "PAUSE <timeout> [WRITE|ALL]",
            "    Suspend all, or just write, clients for <timeout> milliseconds.",
            "SETNAME <name>",
            "    Assign the name <name> to the current connection.",
            "TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
            "         [OPTIN] [OPTOUT] [NOLOOP]",
            "    Control server assisted client side caching.",
            "UNPAUSE",
            "    Stop the current client pause, resuming traffic.",
        ],
    ),
    (
        "COMMAND",
        &[
            "(no subcommand)",
            "    Return details about all commands.",
            "COUNT",
            "    Return the total number of commands in this server.",
            "DOCS [<command-name> ...]",
            "    Return documentation details about multiple commands.",
            "    If no command names are given, documentation details for all",
            "    commands are returned.",
            "GETKEYS <full-command>",
            "    Return the keys from a full command.",
            "INFO [<command-name> ...]",
            "    Return details about multiple commands.",
            "    If no command names are given, details for all commands are returned.",
            "LIST",
            "    Return a list of all commands in this server.",
        ],
    ),
    (
        "CONFIG",
        &[
            "GET <pattern>",
            "    Return parameters matching the glob-like <pattern> and their values.",
            "RESETSTAT",
            "    Reset statistics reported by the INFO command.",
            "REWRITE",
            "    Rewrite the configuration file.",
            "SET <directive> <value>",
            "    Set the configuration <directive> to <value>.",
        ],
    ),
    (
        "DEBUG",
        &[
            "OBJECT <key>",
            "    Return low-level information about <key>.",
            "SLEEP <seconds>",
            "    Stall this shard for <seconds> (float, capped at 30).",
            "PANIC",
            "    Panic this shard thread (crash-handling test aid, as in Redis).",
            "DIGEST",
            "    SHA1 fingerprint of the whole dataset, every database and shard.",
            "    Byte-compatible with Redis, so two servers can be compared in one",
            "    round trip. Not available inside MULTI or Lua.",
        ],
    ),
    (
        "FUNCTION",
        &[
            "DELETE <library-name>",
            "    Delete the given library.",
            "FLUSH [ASYNC|SYNC]",
            "    Delete all the libraries.",
            "LIST [LIBRARYNAME <pattern>] [WITHCODE]",
            "    Return general information on all the libraries.",
            "LOAD [REPLACE] <function-code>",
            "    Create a new library with the given library name and code.",
        ],
    ),
    (
        "MEMORY",
        &[
            "DOCTOR",
            "    Return memory problems reports.",
            "STATS",
            "    Return a map of memory usage counters.",
            "USAGE <key> [SAMPLES <count>]",
            "    Estimate memory usage of the key in bytes.",
        ],
    ),
    (
        "MODULE",
        &[
            "LIST",
            "    Return a list of loaded modules. moon loads none, so this is always empty.",
            "LOAD <path> [<arg> ...]",
            "LOADEX <path> [<arg> ...]",
            "UNLOAD <name>",
            "    Not supported: moon has no module loader.",
        ],
    ),
    (
        "OBJECT",
        &[
            "ENCODING <key>",
            "    Return the encoding of the object stored at <key>.",
            "FREQ <key>",
            "    Return the access frequency of the object at <key>.",
            "IDLETIME <key>",
            "    Return the idle time in seconds of the object at <key>.",
            "REFCOUNT <key>",
            "    Return the reference count of the object at <key>.",
        ],
    ),
    (
        "PUBSUB",
        &[
            "CHANNELS [<pattern>]",
            "    Return the currently active channels matching a <pattern> (default: '*').",
            "NUMPAT",
            "    Return number of subscriptions to patterns.",
            "NUMSUB [<channel> ...]",
            "    Return the number of subscribers for the specified channels, excluding",
            "    pattern subscriptions (default: no channels).",
            "SHARDCHANNELS [<pattern>]",
            "    Return the currently active shard level channels matching a <pattern>",
            "    (default: '*').",
            "SHARDNUMSUB [<shardchannel> ...]",
            "    Return the number of subscribers for the specified shard level channel(s).",
        ],
    ),
    (
        "SCRIPT",
        &[
            "EXISTS <sha1> [<sha1> ...]",
            "    Return information about the existence of the scripts in the script cache.",
            "FLUSH [ASYNC|SYNC]",
            "    Flush the Lua scripts cache.",
            "LOAD <script>",
            "    Load a script into the scripts cache without executing it.",
        ],
    ),
    (
        "SLOWLOG",
        &[
            "GET [<count>]",
            "    Return top <count> entries from the slowlog (default 10).",
            "LEN",
            "    Return the number of entries in the slowlog.",
            "RESET",
            "    Reset the slowlog.",
        ],
    ),
    (
        "XGROUP",
        &[
            "CREATE <key> <groupname> <id|$> [option]",
            "    Create a new consumer group. Options are:",
            "    * MKSTREAM",
            "      Create the empty stream if it does not exist.",
            "    * ENTRIESREAD <entries-read>",
            "      Set the group's entries_read counter (internal use).",
            "CREATECONSUMER <key> <groupname> <consumer>",
            "    Create a new consumer in the specified group.",
            "DELCONSUMER <key> <groupname> <consumer>",
            "    Remove the specified consumer.",
            "DESTROY <key> <groupname>",
            "    Remove the specified group.",
            "SETID <key> <groupname> <id|$> [ENTRIESREAD <entries-read>]",
            "    Set the current group ID and entries_read counter.",
        ],
    ),
    (
        "XINFO",
        &[
            "CONSUMERS <key> <groupname>",
            "    Show consumers of <groupname>.",
            "GROUPS <key>",
            "    Show the stream consumer groups.",
            "STREAM <key> [FULL [COUNT <count>]]",
            "    Show information about the stream.",
        ],
    ),
];

/// The `HELP` reply for `container`, or `None` if it has no help body.
///
/// `container` must be the canonical upper-case name; every caller passes a
/// string literal, so there is no case folding to pay for here.
pub fn container_help(container: &str) -> Option<Frame> {
    HELP_BODIES
        .iter()
        .find(|(name, _)| *name == container)
        .map(|(name, body)| super::helpers::help_reply(name, body))
}

/// The `HELP` reply for a container that is known to have one.
///
/// Callers that have already decided `HELP` was asked for — the containers whose
/// `HELP` predates moon#698 and dispatches through their own match arm. A
/// missing table entry degrades to a header-and-footer-only reply rather than
/// panicking; `every_published_container_has_a_help_body` is what actually
/// catches that, at test time.
pub fn help_or_empty(container: &str) -> Frame {
    container_help(container).unwrap_or_else(|| super::helpers::help_reply(container, &[]))
}

/// The `HELP` reply for `container` when `sub` is `HELP`, otherwise `None`.
///
/// The one call each dispatch site makes. Folding the name test and the table
/// lookup together is deliberate: `HELP` is wired at TWELVE sites (three for
/// `CLIENT`, two each for `PUBSUB` and `XINFO`, one apiece for the rest), and a
/// site that hand-rolled the comparison could differ in case sensitivity from
/// the other eleven without any test noticing.
///
/// `sub` is UNTRUSTED client input; `container` is a caller-supplied literal.
pub fn help_if_requested(container: &str, sub: &[u8]) -> Option<Frame> {
    if !sub.eq_ignore_ascii_case(b"HELP") {
        return None;
    }
    container_help(container)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every container Moon publishes subcommands for must have a help body.
    ///
    /// Catches the half of the drift the integration test cannot: a NEW
    /// container added to `SUBCOMMAND_META` with no help body at all would make
    /// `container_help` return `None` and the dispatch site fall through to
    /// "unknown subcommand" — the exact defect moon#698 fixed.
    #[test]
    fn every_published_container_has_a_help_body() {
        for container in crate::command::metadata::SUBCOMMAND_META.keys() {
            // CLUSTER is deliberately absent: with cluster support disabled,
            // dispatch answers every CLUSTER subcommand — HELP included — with
            // "This instance has cluster support disabled".
            if *container == "CLUSTER" {
                continue;
            }
            assert!(
                container_help(container).is_some(),
                "{container} publishes subcommands but has no HELP body (moon#698)"
            );
        }
    }

    #[test]
    fn a_body_never_repeats_the_header_or_the_footer() {
        // The helper owns both. A container that also spelled them out would
        // emit them twice, which no assertion on "the first line" would catch.
        for (name, body) in HELP_BODIES {
            for line in body.iter() {
                assert!(
                    !line.contains("Subcommands are:"),
                    "{name}: help_reply already emits the header"
                );
                assert!(
                    *line != "HELP" && *line != "    Print this help.",
                    "{name}: help_reply already emits the HELP footer"
                );
            }
        }
    }
}
