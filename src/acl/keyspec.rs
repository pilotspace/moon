//! Key extraction for ACL `~pattern` enforcement — derived from the command
//! registry's key specs, fail-closed on anything it cannot enumerate.
//!
//! # Why this module exists (moon#566)
//!
//! `AclTable::check_key_permission` used to get a command's keys from a
//! hand-maintained `match` on the command name whose fallthrough returned an
//! empty vector. An empty key list is **not** "checked less precisely": the
//! permission loop simply never runs, so every `~pattern` was silently
//! ignored for any command the list forgot. `SMOVE` (listed, but as a
//! single-key command, leaving its DESTINATION unchecked), `COPY`,
//! `ZRANGESTORE`, `LMPOP`/`ZMPOP`/`BLMPOP`/`BZMPOP`, `SINTERCARD`,
//! `ZDIFF`/`ZINTER`/`ZUNION`, `SORT ... STORE` and `GEORADIUS ... STORE` all
//! had that shape: a `~cache:*` user could read from — and write to —
//! arbitrary keys through them.
//!
//! The list is now DERIVED from [`crate::command::metadata::COMMAND_META`]
//! (`first_key`/`last_key`/`step`, Redis argv semantics), so a command that
//! declares its keys there is enforced automatically. The hand-written arms
//! that remain cover only the layouts a fixed key spec provably cannot
//! express — `numkeys`-counted key vectors, positional `STORE` clauses, the
//! `STREAMS` token, and subcommand-shaped key positions.
//!
//! # Fail-closed contract
//!
//! [`command_keys`] answers one of three things:
//!
//! * [`CommandKeys::None`] — the command provably names NO key (`PING`,
//!   `CONFIG`, `SUBSCRIBE`, ...). The caller must not deny on key patterns.
//! * [`CommandKeys::Keys`] — the exact keys this invocation touches.
//! * [`CommandKeys::Indeterminate`] — the command is known (or suspected) to
//!   touch keys, but THIS argv could not be enumerated. The caller must DENY.
//!   A future command that ships without a key spec therefore fails safe
//!   instead of falling open.
//!
//! # Hot path
//!
//! `check_key_permission` short-circuits for unrestricted users and for `~*`
//! BEFORE calling in here, so this code runs only for key-restricted users.
//! Even so it is allocation-free for up to four keys (`SmallVec` inline
//! capacity) and borrows key bytes out of the argv rather than copying them —
//! strictly cheaper than the `Vec<&[u8]>` it replaces, which heap-allocated
//! for every single-key command.

use std::collections::HashSet;
use std::sync::LazyLock;

use parking_lot::Mutex;
use smallvec::SmallVec;

use crate::command::metadata;
use crate::protocol::Frame;

/// Borrowed key slices. Four inline slots cover every fixed-arity keyed
/// command in the registry; only variadic forms (`DEL a b c d e`) spill.
pub type KeyVec<'a> = SmallVec<[&'a [u8]; 4]>;

/// Outcome of key extraction. See the module docs for the contract.
#[derive(Debug)]
pub enum CommandKeys<'a> {
    /// The command provably names no key.
    None,
    /// The keys this invocation touches (never empty).
    Keys(KeyVec<'a>),
    /// Keys exist (or may exist) but could not be enumerated — deny.
    Indeterminate,
}

/// Commands that are dispatched but carry no entry in `COMMAND_META`, and
/// genuinely name no key. Without this list they would be `Indeterminate`
/// (fail-closed) and a key-restricted user would lose them.
///
/// Keep this in sync with the dispatch table: a NEW keyless command added
/// outside `COMMAND_META` must be listed here, and a new KEYED command must
/// get a `COMMAND_META` entry (the point of the fail-closed default is that
/// forgetting is safe).
const UNREGISTERED_KEYLESS: &[&[u8]] = &[
    b"HEALTHZ",
    b"READYZ",
    b"PUBSUB",
    b"ASKING",
    b"READONLY",
    b"READWRITE",
];

/// What an invocation does to the key at a reported position.
///
/// Redis carries this per key in its command table (`RO`/`RW`/`OW`, plus
/// `access`/`update`/`insert`/`delete`); moon collapses it to the one
/// distinction its consumers act on — "may this position be MODIFIED?".
///
/// `first_key`/`last_key`/`step` cannot express it at all, which is why
/// `ZUNIONSTORE dst 2 a b` looked to moon like three equally-written keys
/// (moon#584). Only [`Write`](KeyRole::Write) positions may be pushed to a
/// tracking client as invalidated; ACL ignores the role entirely, because a
/// `~pattern` gates reads and writes alike.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyRole {
    /// The command only READS this position (a `*STORE` source, `COPY`'s
    /// source, `SORT`'s input).
    Read,
    /// The command may MODIFY this position.
    ///
    /// Deliberately over-inclusive where a static spec cannot do better:
    /// `LMPOP 2 a b LEFT` writes only the first NON-EMPTY key, which is not
    /// knowable from the argv, so both are `Write`. Over-invalidating costs a
    /// client one refetch; under-invalidating leaves it stale forever.
    Write,
}

/// One key argument: where it is, and what the command does to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyAt {
    /// Zero-based index into `args` (the argv WITHOUT the command name).
    pub idx: usize,
    /// Whether this invocation may modify the key at `idx`.
    pub role: KeyRole,
}

impl KeyAt {
    #[inline]
    const fn new(idx: usize, role: KeyRole) -> Self {
        Self { idx, role }
    }
}

/// Zero-based positions in `args` of the keys an invocation touches.
pub type KeyIdx = SmallVec<[KeyAt; 4]>;

/// Where the keys of an argv live, without interpreting them.
///
/// This is the single walker every consumer shares (moon#582). It reports
/// FACTS; each caller applies its own POLICY, because the callers legitimately
/// disagree — see [`AtPlusComputed`](KeyPositions::AtPlusComputed).
#[derive(Debug)]
pub enum KeyPositions {
    /// The command provably names no key.
    None,
    /// The positions of the keys this invocation touches (never empty).
    At(KeyIdx),
    /// The positions we CAN name, plus at least one key whose name is computed
    /// at runtime and therefore cannot be named at all (`SORT ... BY w_*`).
    ///
    /// ACL must treat this as indeterminate: a `~pattern` user could otherwise
    /// reach arbitrary keys through the pattern. Cache invalidation must NOT —
    /// redis reports and invalidates the named keys either way, and dropping
    /// them would leave a tracking client permanently stale.
    AtPlusComputed(KeyIdx),
    /// Keys exist (or may exist) but this argv could not be enumerated.
    Unknown,
}

/// Locate the key arguments of `cmd`/`args`. `args` EXCLUDES the command name,
/// so registry key-spec index `N` maps to `args[N - 1]`.
pub fn command_key_positions(cmd: &[u8], args: &[Frame]) -> KeyPositions {
    // Layouts a fixed first/last/step spec cannot express come first: some of
    // them (SORT, OBJECT, ZUNIONSTORE) DO have a spec, but it describes only
    // part of the truth.
    if let Some(shape) = movable_shape(cmd) {
        return shape.positions(args);
    }

    let Some(meta) = metadata::lookup(cmd) else {
        // FT.* / GRAPH.* address indexes and graphs, not keyspace keys; ACL
        // key patterns do not cover that namespace at all (tracked
        // separately). Everything else unknown fails closed.
        if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.")
            || cmd.len() > 6 && cmd[..6].eq_ignore_ascii_case(b"GRAPH.")
            || UNREGISTERED_KEYLESS
                .iter()
                .any(|k| cmd.eq_ignore_ascii_case(k))
        {
            return KeyPositions::None;
        }
        return KeyPositions::Unknown;
    };

    if meta.first_key <= 0 {
        return KeyPositions::None;
    }

    let argc = args.len();
    let first = meta.first_key as usize;
    // Redis argv semantics: index 1 is the first argument after the command
    // name, so the last valid index is `argc`. A negative `last_key` counts
    // back from there (-1 = last argument, -2 = second to last).
    let last = if meta.last_key < 0 {
        let back = (-meta.last_key) as usize;
        match (argc + 1).checked_sub(back) {
            Some(idx) => idx,
            None => return KeyPositions::Unknown,
        }
    } else {
        meta.last_key as usize
    };
    // A declared key position that the argv does not reach is a malformed
    // invocation — report nothing rather than silently naming fewer keys.
    if first > last || last > argc {
        return KeyPositions::Unknown;
    }

    let step = if meta.step > 0 { meta.step as usize } else { 1 };
    // Every position of a same-role command takes the command's own verdict:
    // a write command writes all of them, a read command reads all of them.
    // The exceptions carry a `DEST` shape instead (see `dest_position`).
    let bulk = if meta.flags.contains(metadata::CommandFlags::WRITE) {
        KeyRole::Write
    } else {
        KeyRole::Read
    };
    let mut idx = KeyIdx::new();
    let mut i = first;
    while i <= last {
        idx.push(KeyAt::new(i - 1, bulk));
        i += step;
    }
    if idx.is_empty() {
        return KeyPositions::Unknown;
    }
    // A single-key invocation has nothing to split, so the overwhelmingly
    // common case never reaches the name match at all.
    if let Some(dest) = if idx.len() > 1 {
        dest_position(cmd)
    } else {
        None
    } {
        // `SINTERSTORE dst a b`: only `dst` is written, the rest are read.
        // Demote everything, then promote the one destination slot.
        for k in idx.iter_mut() {
            k.role = KeyRole::Read;
        }
        let slot = match dest {
            Dest::First => 0,
            Dest::Last => idx.len() - 1,
        };
        idx[slot].role = KeyRole::Write;
    }
    KeyPositions::At(idx)
}

/// Which key position of a fixed-spec command is the WRITE destination, for
/// the commands whose positions do not all share one role.
///
/// `first_key`/`last_key`/`step` cannot say this — redis carries it per key as
/// `OW`/`RO` flags, which is how it knows `ZUNIONSTORE`'s sources are only
/// read (moon#584). Everything absent from here is same-role, taking its
/// verdict from the command's own `WRITE` flag, which is correct for `DEL a b`,
/// `MSET k v k v`, `RENAME a b` and `SMOVE src dst` alike (those really do
/// write every position).
///
/// Membership is deliberately conservative: a command wrongly listed here
/// would MISS an invalidation, which is the direction that leaves a client
/// cache stale forever (moon#582). A command wrongly absent merely
/// over-invalidates, costing one refetch.
#[derive(Clone, Copy)]
enum Dest {
    /// `<cmd> dst src [src ...]`
    First,
    /// `<cmd> src dst`
    Last,
}

fn dest_position(cmd: &[u8]) -> Option<Dest> {
    let b0 = cmd.first().map(|b| b | 0x20)?;
    let d = match (cmd.len(), b0) {
        // dst first, read-only sources after it
        (11, b's') if cmd.eq_ignore_ascii_case(b"SINTERSTORE") => Dest::First,
        (11, b's') if cmd.eq_ignore_ascii_case(b"SUNIONSTORE") => Dest::First,
        (10, b's') if cmd.eq_ignore_ascii_case(b"SDIFFSTORE") => Dest::First,
        (7, b'p') if cmd.eq_ignore_ascii_case(b"PFMERGE") => Dest::First,
        // `BITOP <op> dst src ...` — the op is not a key, so `first_key` is 2
        // and the destination is still the FIRST reported position.
        (5, b'b') if cmd.eq_ignore_ascii_case(b"BITOP") => Dest::First,
        (11, b'z') if cmd.eq_ignore_ascii_case(b"ZRANGESTORE") => Dest::First,
        (14, b'g') if cmd.eq_ignore_ascii_case(b"GEOSEARCHSTORE") => Dest::First,
        // `COPY src dst` — the odd one out: destination LAST.
        (4, b'c') if cmd.eq_ignore_ascii_case(b"COPY") => Dest::Last,
        _ => return None,
    };
    Some(d)
}

/// Does `cmd` name key arguments AT ALL, independent of what this particular
/// argv managed to parse?
///
/// This is redis's `doesCommandHaveKeys`, and `COMMAND GETKEYS` answers
/// "The command has no key arguments" from it BEFORE checking arity — which is
/// why `COMMAND GETKEYS SELECT` reports no-keys rather than wrong-arity
/// (verified against redis 8.6.1). A movablekeys command has keys even when
/// this argv's `numkeys` is unparsable, so `EVAL <script> <garbage>` must not
/// be answered "has no key arguments"; that is the lie moon#537 was about.
///
/// `args` matters only for the CONTAINER commands, whose key-ness belongs to
/// the SUBCOMMAND: `MEMORY USAGE k` has a key, `MEMORY STATS` does not.
pub fn command_has_keys(cmd: &[u8], args: &[Frame]) -> bool {
    match movable_shape(cmd) {
        Some(Movable::Container { role }) => {
            matches!(subcommand_key(args, role), KeyPositions::At(_))
        }
        Some(_) => true,
        None => metadata::lookup(cmd).is_some_and(|m| m.first_key > 0),
    }
}

/// Extract the keys `cmd`/`args` touch, for ACL `~pattern` enforcement.
///
/// A thin, fail-closed policy over [`command_key_positions`]: anything that
/// cannot be fully enumerated — including an argv that also reaches
/// runtime-computed key names — is [`CommandKeys::Indeterminate`], so the
/// caller denies.
pub fn command_keys<'a>(cmd: &[u8], args: &'a [Frame]) -> CommandKeys<'a> {
    let idx = match command_key_positions(cmd, args) {
        KeyPositions::None => return CommandKeys::None,
        KeyPositions::At(idx) => idx,
        // Some key of this argv is unnameable, so `~pattern` cannot gate it.
        KeyPositions::AtPlusComputed(_) | KeyPositions::Unknown => {
            return CommandKeys::Indeterminate;
        }
    };
    let mut keys = KeyVec::new();
    // The ROLE is deliberately ignored: a `~pattern` gates reads and writes
    // alike, so ACL checks every named position regardless of what the command
    // does to it. (Cache invalidation is the consumer that filters on role.)
    for KeyAt { idx: i, .. } in idx {
        // A key position holding a non-string is a malformed invocation.
        match args.get(i).and_then(key_bytes) {
            Some(k) => keys.push(k),
            None => return CommandKeys::Indeterminate,
        }
    }
    if keys.is_empty() {
        return CommandKeys::Indeterminate;
    }
    CommandKeys::Keys(keys)
}

/// Log ONCE per command name that a key check failed closed, so an operator
/// can tell a real permission denial from a missing key spec.
///
/// Bounded: an attacker can otherwise mint unbounded command names. Past the
/// cap the denial still happens, only the log line is dropped.
pub(crate) fn warn_indeterminate(cmd: &[u8]) {
    const CAP: usize = 128;
    static WARNED: LazyLock<Mutex<HashSet<Box<str>>>> =
        LazyLock::new(|| Mutex::new(HashSet::new()));

    let name = String::from_utf8_lossy(cmd).to_ascii_uppercase();
    let mut seen = WARNED.lock();
    if seen.len() >= CAP || seen.contains(name.as_str()) {
        return;
    }
    seen.insert(name.clone().into_boxed_str());
    drop(seen);
    tracing::warn!(
        command = %name,
        "ACL denied a key-restricted user: this command's key arguments could not be \
         determined, so `~pattern` cannot be enforced for it (fail-closed). Add a \
         COMMAND_META key spec, or a movable-key arm in acl::keyspec, to allow it."
    );
}

#[inline]
fn key_bytes(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// A key layout that `first_key`/`last_key`/`step` cannot express, together
/// with the per-position roles the fixed spec cannot carry either.
///
/// Keeping the SHAPE separate from the walk lets two questions share one
/// table: "where are this argv's keys" ([`Movable::positions`]) and "does this
/// command name keys at all" ([`command_has_keys`]). Answering the second from
/// a second list is exactly how moon#537 shipped — `COMMAND GETKEYS` decided
/// from `first_key <= 0` and told every movablekeys client the command had no
/// keys.
#[derive(Clone, Copy)]
enum Movable {
    /// `... numkeys key [key ...] ...`, count at `nk`, with an optional write
    /// destination at `args[0]` (the `Z*STORE` family).
    NumKeys {
        nk: usize,
        dest_at_zero: bool,
        vec_role: KeyRole,
    },
    /// `key ... [STORE dest]`: one source key plus a positional destination.
    /// `by_get` additionally scans `SORT`'s `BY`/`GET` patterns.
    SourceStore { by_get: bool },
    /// `<cmd> src dst` — both written.
    TwoKeys,
    /// N keys after the `STREAMS` token, then N ids.
    Streams { role: KeyRole },
    /// `<CMD> <SUBCOMMAND> <key>` — only SOME subcommands take a key.
    Container { role: KeyRole },
}

/// Classify `cmd`'s key layout, or `None` when the meta-derived walk applies.
///
/// Matched on `(len, first byte)` first so the common single-key commands fall
/// through after one integer compare.
fn movable_shape(cmd: &[u8]) -> Option<Movable> {
    use KeyRole::{Read, Write};
    let len = cmd.len();
    if len == 0 {
        return None;
    }
    // `<cmd> numkeys key [key ...]`, count at args[0].
    const fn vec0(vec_role: KeyRole) -> Movable {
        Movable::NumKeys {
            nk: 0,
            dest_at_zero: false,
            vec_role,
        }
    }
    // `<cmd> <arg> numkeys key [key ...]`, count at args[1].
    const fn vec1(vec_role: KeyRole) -> Movable {
        Movable::NumKeys {
            nk: 1,
            dest_at_zero: false,
            vec_role,
        }
    }
    let b0 = cmd[0] | 0x20;
    let shape = match (len, b0) {
        // ---- numkeys-counted key vectors ----
        // The MPOP family pops from the first NON-EMPTY key, which no static
        // spec can identify, so every candidate counts as written.
        (5, b'l') if cmd.eq_ignore_ascii_case(b"LMPOP") => vec0(Write),
        (5, b'z') if cmd.eq_ignore_ascii_case(b"ZMPOP") => vec0(Write),
        // Set/zset combinators only read their inputs.
        (5, b'z') if cmd.eq_ignore_ascii_case(b"ZDIFF") => vec0(Read),
        (6, b'z') if cmd.eq_ignore_ascii_case(b"ZINTER") || cmd.eq_ignore_ascii_case(b"ZUNION") => {
            vec0(Read)
        }
        (10, b'z') if cmd.eq_ignore_ascii_case(b"ZINTERCARD") => vec0(Read),
        (10, b's') if cmd.eq_ignore_ascii_case(b"SINTERCARD") => vec0(Read),
        // <cmd> timeout numkeys key [key ...] <dir>
        (6, b'b') if cmd.eq_ignore_ascii_case(b"BLMPOP") || cmd.eq_ignore_ascii_case(b"BZMPOP") => {
            vec1(Write)
        }
        // <cmd> script|sha|function numkeys key [key ...] [arg ...]
        // A script may write any key it was handed; only the `_RO` forms
        // cannot.
        (4, b'e') if cmd.eq_ignore_ascii_case(b"EVAL") => vec1(Write),
        (7, b'e') if cmd.eq_ignore_ascii_case(b"EVALSHA") => vec1(Write),
        (5, b'f') if cmd.eq_ignore_ascii_case(b"FCALL") => vec1(Write),
        (8, b'f') if cmd.eq_ignore_ascii_case(b"FCALL_RO") => vec1(Read),
        // <cmd> dest numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...]
        // The registry spec names only `dest` (first_key == last_key == 1);
        // the numkeys vector after it is READ, not written (moon#584).
        (11, b'z')
            if cmd.eq_ignore_ascii_case(b"ZUNIONSTORE")
                || cmd.eq_ignore_ascii_case(b"ZINTERSTORE") =>
        {
            Movable::NumKeys {
                nk: 1,
                dest_at_zero: true,
                vec_role: Read,
            }
        }
        (10, b'z') if cmd.eq_ignore_ascii_case(b"ZDIFFSTORE") => Movable::NumKeys {
            nk: 1,
            dest_at_zero: true,
            vec_role: Read,
        },

        // ---- positional STORE clauses (source key + optional destination) ----
        (4, b's') if cmd.eq_ignore_ascii_case(b"SORT") => Movable::SourceStore { by_get: true },
        (7, b's') if cmd.eq_ignore_ascii_case(b"SORT_RO") => Movable::SourceStore { by_get: true },
        (9, b'g') if cmd.eq_ignore_ascii_case(b"GEORADIUS") => {
            Movable::SourceStore { by_get: false }
        }
        (17, b'g') if cmd.eq_ignore_ascii_case(b"GEORADIUSBYMEMBER") => {
            Movable::SourceStore { by_get: false }
        }

        // ---- two-key move ----
        // `RPOPLPUSH src dst` also carries a registry spec (1..2), which would
        // give the identical answer; this arm is kept only so the layout is
        // stated in one place with its siblings.
        (9, b'r') if cmd.eq_ignore_ascii_case(b"RPOPLPUSH") => Movable::TwoKeys,

        // ---- keys after the STREAMS token ----
        // XREADGROUP advances the group's last-delivered id, so its streams
        // are written; plain XREAD only reads.
        (5, b'x') if cmd.eq_ignore_ascii_case(b"XREAD") => Movable::Streams { role: Read },
        (10, b'x') if cmd.eq_ignore_ascii_case(b"XREADGROUP") => Movable::Streams { role: Write },

        // ---- subcommand first, key second ----
        (6, b'o') if cmd.eq_ignore_ascii_case(b"OBJECT") => Movable::Container { role: Read },
        (5, b'x') if cmd.eq_ignore_ascii_case(b"XINFO") => Movable::Container { role: Read },
        (6, b'm') if cmd.eq_ignore_ascii_case(b"MEMORY") => Movable::Container { role: Read },
        (6, b'x') if cmd.eq_ignore_ascii_case(b"XGROUP") => Movable::Container { role: Write },

        _ => return None,
    };
    Some(shape)
}

impl Movable {
    /// Walk `args` for this shape.
    fn positions(self, args: &[Frame]) -> KeyPositions {
        match self {
            Movable::NumKeys {
                nk,
                dest_at_zero,
                vec_role,
            } => numkeys_positions(args, nk, dest_at_zero, vec_role),
            Movable::SourceStore { by_get } => source_plus_store(args, by_get),
            Movable::TwoKeys => two_keys(args),
            Movable::Streams { role } => stream_keys(args, role),
            Movable::Container { role } => subcommand_key(args, role),
        }
    }
}

/// `... numkeys key [key ...]` with `numkeys` at `nk_idx`, plus an optional
/// WRITE destination key at `args[0]` (the `Z*STORE` family).
fn numkeys_positions(
    args: &[Frame],
    nk_idx: usize,
    dest_at_zero: bool,
    vec_role: KeyRole,
) -> KeyPositions {
    let Some(nk) = args
        .get(nk_idx)
        .and_then(key_bytes)
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<usize>().ok())
    else {
        return KeyPositions::Unknown;
    };
    let first = nk_idx + 1;
    // `numkeys` larger than the argv is malformed; the command errors out
    // anyway, and enumerating fewer keys than declared must not silently
    // reduce enforcement. `checked_add` is load-bearing, not defensive
    // decoration: `nk` is attacker-controlled, so `first + nk` can overflow
    // `usize` — in release (no overflow-checks) it WRAPS, the `<` guard then
    // sees a tiny sum and the range below panics. A panic here is reachable by
    // any key-restricted user = remote DoS inside ACL.
    let Some(end) = first.checked_add(nk).filter(|&e| e <= args.len()) else {
        return KeyPositions::Unknown;
    };
    let mut idx = KeyIdx::new();
    if dest_at_zero {
        if args.is_empty() {
            return KeyPositions::Unknown;
        }
        idx.push(KeyAt::new(0, KeyRole::Write));
    }
    idx.extend((first..end).map(|i| KeyAt::new(i, vec_role)));
    if idx.is_empty() {
        // `EVAL <script> 0` and friends: numkeys parsed cleanly and says the
        // invocation names no key.
        return KeyPositions::None;
    }
    KeyPositions::At(idx)
}

/// The READ source at `args[0]` plus the WRITE key after a positional
/// `STORE`/`STOREDIST` token.
///
/// With `check_by_get`, a `BY`/`GET` pattern containing `*` (SORT's weight and
/// projection lookups) yields [`KeyPositions::AtPlusComputed`]: those read keys
/// whose names are computed at runtime from the sorted elements, so no key spec
/// can name them. ACL must refuse such an argv outright; cache invalidation
/// must still act on the keys that ARE named, which is what redis does.
/// `BY nosort` / `GET #` carry no `*` and are therefore unaffected.
///
/// A DANGLING clause (`SORT k STORE`, `SORT k BY`) takes the same route: the
/// keys we can name are complete and redis reports exactly them, but the
/// unfinished clause is one more thing this walker could not account for, so
/// ACL keeps denying the argv as it did when this returned `Unknown`.
fn source_plus_store(args: &[Frame], check_by_get: bool) -> KeyPositions {
    if args.is_empty() {
        return KeyPositions::Unknown;
    }
    let mut idx = KeyIdx::new();
    idx.push(KeyAt::new(0, KeyRole::Read));
    let mut computed = false;

    let mut i = 1;
    while i < args.len() {
        let Some(tok) = key_bytes(&args[i]) else {
            i += 1;
            continue;
        };
        if tok.eq_ignore_ascii_case(b"STORE") || tok.eq_ignore_ascii_case(b"STOREDIST") {
            if args.get(i + 1).is_none() {
                computed = true;
                break;
            }
            idx.push(KeyAt::new(i + 1, KeyRole::Write));
            i += 2;
            continue;
        }
        if check_by_get && (tok.eq_ignore_ascii_case(b"BY") || tok.eq_ignore_ascii_case(b"GET")) {
            let Some(pattern) = args.get(i + 1).and_then(key_bytes) else {
                computed = true;
                break;
            };
            if pattern.contains(&b'*') {
                computed = true;
            }
            i += 2;
            continue;
        }
        i += 1;
    }
    if computed {
        KeyPositions::AtPlusComputed(idx)
    } else {
        KeyPositions::At(idx)
    }
}

/// `<cmd> source destination ...` — both are written, both must be checked.
fn two_keys(args: &[Frame]) -> KeyPositions {
    if args.len() < 2 {
        return KeyPositions::Unknown;
    }
    let mut idx = KeyIdx::new();
    idx.push(KeyAt::new(0, KeyRole::Write));
    idx.push(KeyAt::new(1, KeyRole::Write));
    KeyPositions::At(idx)
}

/// `XREAD`/`XREADGROUP`: after the `STREAMS` token come N keys then N ids.
fn stream_keys(args: &[Frame], role: KeyRole) -> KeyPositions {
    let Some(pos) = args
        .iter()
        .position(|f| key_bytes(f).is_some_and(|t| t.eq_ignore_ascii_case(b"STREAMS")))
    else {
        return KeyPositions::Unknown;
    };
    let num_keys = (args.len() - pos - 1) / 2;
    if num_keys == 0 {
        return KeyPositions::Unknown;
    }
    let mut idx = KeyIdx::new();
    idx.extend((pos + 1..pos + 1 + num_keys).map(|i| KeyAt::new(i, role)));
    KeyPositions::At(idx)
}

/// `<CMD> <SUBCOMMAND> <key> ...` (OBJECT, XINFO, MEMORY USAGE, XGROUP).
///
/// The keyless subcommands of these (`OBJECT HELP`, `XINFO HELP`,
/// `MEMORY DOCTOR|STATS|PURGE`) carry no second argument.
fn subcommand_key(args: &[Frame], role: KeyRole) -> KeyPositions {
    match args.get(1) {
        None => KeyPositions::None,
        Some(_) => {
            let mut idx = KeyIdx::new();
            idx.push(KeyAt::new(1, role));
            KeyPositions::At(idx)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn argv(parts: &[&str]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect()
    }

    fn keys_of(cmd: &[u8], parts: &[&str]) -> Vec<String> {
        let args = argv(parts);
        match command_keys(cmd, &args) {
            CommandKeys::Keys(k) => k
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect(),
            other => panic!(
                "{} expected keys, got {:?}",
                String::from_utf8_lossy(cmd),
                other
            ),
        }
    }

    fn is_none(cmd: &[u8], parts: &[&str]) -> bool {
        let args = argv(parts);
        matches!(command_keys(cmd, &args), CommandKeys::None)
    }

    fn is_indeterminate(cmd: &[u8], parts: &[&str]) -> bool {
        let args = argv(parts);
        matches!(command_keys(cmd, &args), CommandKeys::Indeterminate)
    }

    #[test]
    fn meta_derived_specs_cover_the_fixed_layouts() {
        assert_eq!(keys_of(b"GET", &["k"]), ["k"]);
        assert_eq!(keys_of(b"SET", &["k", "v"]), ["k"]);
        assert_eq!(keys_of(b"MSET", &["a", "1", "b", "2"]), ["a", "b"]);
        assert_eq!(keys_of(b"DEL", &["a", "b", "c"]), ["a", "b", "c"]);
        assert_eq!(keys_of(b"BLPOP", &["a", "b", "0"]), ["a", "b"]);
        assert_eq!(keys_of(b"SMOVE", &["src", "dst", "m"]), ["src", "dst"]);
        assert_eq!(keys_of(b"COPY", &["src", "dst"]), ["src", "dst"]);
        assert_eq!(
            keys_of(b"ZRANGESTORE", &["dst", "src", "0", "-1"]),
            ["dst", "src"]
        );
        assert_eq!(keys_of(b"BITOP", &["AND", "d", "a", "b"]), ["d", "a", "b"]);
        assert_eq!(keys_of(b"GEOSEARCHSTORE", &["d", "s", "x"]), ["d", "s"]);
        // Case-insensitive, like the wire.
        assert_eq!(keys_of(b"smove", &["src", "dst", "m"]), ["src", "dst"]);
    }

    #[test]
    fn numkeys_walkers_enumerate_every_slot() {
        assert_eq!(keys_of(b"LMPOP", &["2", "a", "b", "LEFT"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZMPOP", &["2", "a", "b", "MIN"]), ["a", "b"]);
        assert_eq!(
            keys_of(b"BLMPOP", &["0", "2", "a", "b", "LEFT"]),
            ["a", "b"]
        );
        assert_eq!(keys_of(b"BZMPOP", &["0", "2", "a", "b", "MIN"]), ["a", "b"]);
        assert_eq!(keys_of(b"SINTERCARD", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(keys_of(b"ZDIFF", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(
            keys_of(b"ZINTER", &["2", "a", "b", "WITHSCORES"]),
            ["a", "b"]
        );
        assert_eq!(keys_of(b"ZUNION", &["2", "a", "b"]), ["a", "b"]);
        assert_eq!(
            keys_of(b"ZINTERCARD", &["2", "a", "b", "LIMIT", "1"]),
            ["a", "b"]
        );
        assert_eq!(
            keys_of(b"ZUNIONSTORE", &["d", "2", "a", "b", "WEIGHTS", "1", "2"]),
            ["d", "a", "b"]
        );
        assert_eq!(keys_of(b"EVAL", &["s", "1", "a", "argv"]), ["a"]);
        assert_eq!(keys_of(b"FCALL", &["f", "2", "a", "b"]), ["a", "b"]);
        // numkeys that says "no keys" is an answer, not a failure.
        assert!(is_none(b"EVAL", &["return 1", "0"]));
        // An over-large count still enumerates every slot it claims — the
        // command errors out later, but the check never sees FEWER keys than
        // the client asked for (that is the direction that would bypass).
        assert_eq!(
            keys_of(b"LMPOP", &["3", "a", "b", "LEFT"]),
            ["a", "b", "LEFT"]
        );
        // ... and a count the argv cannot satisfy at all is a failure.
        assert!(is_indeterminate(b"LMPOP", &["4", "a", "b", "LEFT"]));
        assert!(is_indeterminate(b"LMPOP", &["notanumber", "a", "LEFT"]));
        assert!(is_indeterminate(b"LMPOP", &[]));
    }

    /// A numkeys big enough to overflow `first + nk` must fail closed, not
    /// panic. Release builds wrap the addition (no overflow-checks), so the
    /// old `args.len() < first + nk` guard saw `< 0` (false) and then sliced
    /// `&args[first..first + nk]` = `&args[1..0]` — a panic reachable by any
    /// key-restricted user, i.e. a remote DoS inside ACL enforcement.
    #[test]
    fn numkeys_overflow_fails_closed_without_panic() {
        let huge = usize::MAX.to_string();
        // nk at index 0 (LMPOP/ZMPOP/ZDIFF/ZINTER/ZUNION/SINTERCARD/…).
        assert!(is_indeterminate(b"LMPOP", &[&huge, "a", "LEFT"]));
        assert!(is_indeterminate(b"ZDIFF", &[&huge, "a"]));
        assert!(is_indeterminate(b"SINTERCARD", &[&huge, "a"]));
        // nk at index 1 (BLMPOP/BZMPOP/EVAL/EVALSHA/FCALL/Z*STORE).
        assert!(is_indeterminate(b"BLMPOP", &["0", &huge, "a", "LEFT"]));
        assert!(is_indeterminate(b"EVAL", &["script", &huge, "a"]));
        assert!(is_indeterminate(b"ZUNIONSTORE", &["dst", &huge, "a"]));
        // A near-boundary value that would overflow only when `first` is
        // added — still must not panic.
        let near = (usize::MAX - 1).to_string();
        assert!(is_indeterminate(b"LMPOP", &[&near, "a", "LEFT"]));
    }

    #[test]
    fn store_clauses_and_stream_tokens() {
        assert_eq!(keys_of(b"SORT", &["src"]), ["src"]);
        assert_eq!(keys_of(b"SORT", &["src", "STORE", "dst"]), ["src", "dst"]);
        assert_eq!(keys_of(b"SORT", &["src", "ALPHA", "GET", "#"]), ["src"]);
        assert_eq!(
            keys_of(b"GEORADIUS", &["src", "1", "2", "3", "m", "STORE", "d"]),
            ["src", "d"]
        );
        assert_eq!(
            keys_of(
                b"GEORADIUSBYMEMBER",
                &["src", "m", "3", "m", "STOREDIST", "d"]
            ),
            ["src", "d"]
        );
        assert_eq!(
            keys_of(b"XREAD", &["COUNT", "1", "STREAMS", "a", "b", "0", "0"]),
            ["a", "b"]
        );
        assert_eq!(
            keys_of(b"XREADGROUP", &["GROUP", "g", "c", "STREAMS", "a", ">"]),
            ["a"]
        );
        // BY/GET patterns read runtime-computed key names: unnameable, so denied.
        assert!(is_indeterminate(b"SORT", &["src", "BY", "w_*"]));
        assert!(is_indeterminate(b"SORT", &["src", "GET", "d_*"]));
        assert!(is_indeterminate(b"SORT", &["src", "STORE"]));
        assert!(is_indeterminate(b"XREAD", &["COUNT", "1"]));
    }

    #[test]
    fn subcommand_shaped_keys() {
        assert_eq!(keys_of(b"OBJECT", &["ENCODING", "k"]), ["k"]);
        assert_eq!(keys_of(b"XINFO", &["STREAM", "k"]), ["k"]);
        assert_eq!(keys_of(b"MEMORY", &["USAGE", "k"]), ["k"]);
        assert_eq!(keys_of(b"XGROUP", &["CREATE", "k", "g", "$"]), ["k"]);
        // RPOPLPUSH has no registry entry yet (moon#520) but both of its
        // arguments are keys and both must be enforced.
        assert_eq!(keys_of(b"RPOPLPUSH", &["src", "dst"]), ["src", "dst"]);
        assert!(is_indeterminate(b"RPOPLPUSH", &["src"]));
        assert!(is_none(b"OBJECT", &["HELP"]));
        assert!(is_none(b"MEMORY", &["STATS"]));
    }

    /// Roles, reported as `(key, is-written)` pairs so a case reads like the
    /// command's own semantics.
    fn roles_of(cmd: &[u8], parts: &[&str]) -> Vec<(String, bool)> {
        let args = argv(parts);
        let idx = match command_key_positions(cmd, &args) {
            KeyPositions::At(idx) | KeyPositions::AtPlusComputed(idx) => idx,
            other => panic!(
                "{} expected positions, got {other:?}",
                String::from_utf8_lossy(cmd)
            ),
        };
        idx.iter()
            .map(|k| {
                (
                    String::from_utf8_lossy(key_bytes(&args[k.idx]).unwrap_or_default())
                        .into_owned(),
                    k.role == KeyRole::Write,
                )
            })
            .collect()
    }

    /// moon#584. A key position's ROLE is the fact `first_key`/`last_key`/
    /// `step` cannot carry, and without it moon told a tracking client that a
    /// `*STORE` command had changed the keys it merely read.
    ///
    /// Each case pins EVERY position's role, so a fix that flips the whole
    /// command one way cannot pass.
    #[test]
    fn key_roles_separate_destinations_from_sources() {
        // numkeys vector after a destination
        assert_eq!(
            roles_of(b"ZUNIONSTORE", &["d", "2", "a", "b"]),
            [("d".into(), true), ("a".into(), false), ("b".into(), false)]
        );
        // positional STORE
        assert_eq!(
            roles_of(b"SORT", &["src", "STORE", "dst"]),
            [("src".into(), false), ("dst".into(), true)]
        );
        assert_eq!(
            roles_of(b"GEORADIUS", &["s", "1", "2", "3", "m", "STOREDIST", "d"]),
            [("s".into(), false), ("d".into(), true)]
        );
        // fixed spec, destination FIRST
        assert_eq!(
            roles_of(b"SINTERSTORE", &["d", "a", "b"]),
            [("d".into(), true), ("a".into(), false), ("b".into(), false)]
        );
        assert_eq!(
            roles_of(b"BITOP", &["AND", "d", "a", "b"]),
            [("d".into(), true), ("a".into(), false), ("b".into(), false)]
        );
        // fixed spec, destination LAST
        assert_eq!(
            roles_of(b"COPY", &["src", "dst"]),
            [("src".into(), false), ("dst".into(), true)]
        );
        // same-role commands take the command's own verdict
        assert_eq!(
            roles_of(b"MSET", &["a", "1", "b", "2"]),
            [("a".into(), true), ("b".into(), true)]
        );
        assert_eq!(
            roles_of(b"MGET", &["a", "b"]),
            [("a".into(), false), ("b".into(), false)]
        );
        assert_eq!(
            roles_of(b"SMOVE", &["s", "d", "m"]),
            [("s".into(), true), ("d".into(), true)]
        );
        // read-only combinators vs the write-flagged MPOP family
        assert_eq!(
            roles_of(b"ZDIFF", &["2", "a", "b"]),
            [("a".into(), false), ("b".into(), false)]
        );
        assert_eq!(
            roles_of(b"LMPOP", &["2", "a", "b", "LEFT"]),
            [("a".into(), true), ("b".into(), true)]
        );
        assert_eq!(
            roles_of(b"FCALL_RO", &["f", "1", "k"]),
            [("k".into(), false)]
        );
        assert_eq!(roles_of(b"FCALL", &["f", "1", "k"]), [("k".into(), true)]);
        // XREAD reads its streams; XREADGROUP advances the group's cursor
        assert_eq!(
            roles_of(b"XREAD", &["STREAMS", "s", "0"]),
            [("s".into(), false)]
        );
        assert_eq!(
            roles_of(b"XREADGROUP", &["GROUP", "g", "c", "STREAMS", "s", ">"]),
            [("s".into(), true)]
        );
        assert_eq!(
            roles_of(b"OBJECT", &["ENCODING", "k"]),
            [("k".into(), false)]
        );
        assert_eq!(
            roles_of(b"XGROUP", &["CREATE", "k", "g", "$"]),
            [("k".into(), true)]
        );
    }

    /// Every command that claims a split-role layout must actually HAVE a
    /// fixed key spec to split, and must name at least two keys in practice —
    /// otherwise `dest_position` is silently decorating a command whose
    /// positions it never reaches.
    #[test]
    fn split_role_commands_have_a_fixed_spec_to_split() {
        for cmd in [
            b"SINTERSTORE".as_ref(),
            b"SUNIONSTORE".as_ref(),
            b"SDIFFSTORE".as_ref(),
            b"PFMERGE".as_ref(),
            b"BITOP".as_ref(),
            b"ZRANGESTORE".as_ref(),
            b"GEOSEARCHSTORE".as_ref(),
            b"COPY".as_ref(),
        ] {
            let name = String::from_utf8_lossy(cmd);
            assert!(
                dest_position(cmd).is_some(),
                "{name} must be recognised by dest_position"
            );
            assert!(
                movable_shape(cmd).is_none(),
                "{name} is meta-derived; a movable arm would bypass dest_position"
            );
            let meta = metadata::lookup(cmd)
                .unwrap_or_else(|| panic!("{name} needs a COMMAND_META entry to split"));
            assert!(
                meta.first_key > 0,
                "{name} has no fixed key spec for dest_position to split"
            );
            assert!(
                meta.flags.contains(metadata::CommandFlags::WRITE),
                "{name} must be a write command for the split to mean anything"
            );
        }
    }

    /// `command_has_keys` is the STATIC claim `COMMAND GETKEYS` answers
    /// before it even looks at the argv (moon#537). It must never contradict
    /// the walker: a command whose positions we can name has keys, full stop.
    #[test]
    fn command_has_keys_agrees_with_the_walker() {
        let cases: &[(&[u8], &[&str])] = &[
            (b"GET", &["k"]),
            (b"MSET", &["a", "1"]),
            (b"LMPOP", &["1", "k", "LEFT"]),
            (b"EVAL", &["s", "1", "k"]),
            (b"XREAD", &["STREAMS", "s", "0"]),
            (b"SORT", &["k", "STORE", "d"]),
            (b"OBJECT", &["ENCODING", "k"]),
            (b"ZUNIONSTORE", &["d", "1", "a"]),
        ];
        for (cmd, parts) in cases {
            let args = argv(parts);
            assert!(
                command_has_keys(cmd, &args),
                "{} names keys",
                String::from_utf8_lossy(cmd)
            );
        }
        // A movablekeys command has keys even when THIS argv is unparsable —
        // the exact confusion moon#537 filed. `EVAL <script> <garbage>` must
        // not be answered "the command has no key arguments".
        assert!(command_has_keys(b"EVAL", &argv(&["s", "garbage", "k"])));
        assert!(command_has_keys(b"LMPOP", &argv(&["garbage", "k"])));
        assert!(command_has_keys(b"XREAD", &argv(&["COUNT", "1"])));
        // Genuinely keyless, and containers whose subcommand takes no key.
        for (cmd, parts) in [
            (b"PING".as_ref(), &[][..]),
            (b"SELECT".as_ref(), &["0"][..]),
            (b"FLUSHALL".as_ref(), &[][..]),
            (b"NOSUCHCMD".as_ref(), &["k"][..]),
            (b"MEMORY".as_ref(), &["STATS"][..]),
            (b"OBJECT".as_ref(), &["HELP"][..]),
            (b"XINFO".as_ref(), &["HELP"][..]),
        ] {
            assert!(
                !command_has_keys(cmd, &argv(parts)),
                "{} names no key",
                String::from_utf8_lossy(cmd)
            );
        }
    }

    #[test]
    fn keyless_and_unknown_commands() {
        for cmd in [
            b"PING".as_ref(),
            b"CONFIG".as_ref(),
            b"SUBSCRIBE".as_ref(),
            b"KEYS".as_ref(),
            b"FLUSHALL".as_ref(),
            b"PUBSUB".as_ref(),
            b"HEALTHZ".as_ref(),
            b"FT.SEARCH".as_ref(),
            b"FT.INVALIDATE_RANGE".as_ref(),
            b"GRAPH.QUERY".as_ref(),
        ] {
            assert!(
                is_none(cmd, &["x"]),
                "{} must report NO keys",
                String::from_utf8_lossy(cmd)
            );
        }
        // Unknown to the registry and not on the keyless allowlist: deny.
        assert!(is_indeterminate(b"NOTACOMMAND", &["k"]));
        assert!(is_indeterminate(b"", &["k"]));
        // A declared key that the argv does not carry: deny.
        assert!(is_indeterminate(b"GET", &[]));
        assert!(is_indeterminate(b"SMOVE", &["only-one"]));
    }

    /// The guard that keeps the fail-closed default honest: every command in
    /// the registry that declares NO key must be one we have actually looked
    /// at. A new command whose `first_key` is left at 0 by accident lands
    /// here and fails the suite instead of shipping unenforced.
    #[test]
    fn keyless_registry_entries_are_reviewed() {
        // Commands whose registry entry says `first_key == 0` AND that this
        // module does not special-case. Every name here has been checked to
        // name no keyspace key.
        const REVIEWED_KEYLESS: &[&str] = &[
            // connection / server / admin
            "PING",
            "ECHO",
            "QUIT",
            "SELECT",
            "INFO",
            "COMMAND",
            "AUTH",
            "HELLO",
            "RESET",
            "CLIENT",
            "ROLE",
            "WAIT",
            "BGSAVE",
            "BGREWRITEAOF",
            "SAVE",
            "LASTSAVE",
            "CONFIG",
            "ACL",
            "SLOWLOG",
            "MONITOR",
            "HOTKEYS",
            "DEBUG",
            "KILL",
            "VACUUM",
            "FLUSHDB",
            "FLUSHALL",
            "SWAPDB",
            "SHUTDOWN",
            "TIME",
            "LOLWUT",
            "DBSIZE",
            "RANDOMKEY",
            "KEYS",
            "SCAN",
            // pub/sub (channel patterns, not key patterns)
            "SUBSCRIBE",
            "UNSUBSCRIBE",
            "PSUBSCRIBE",
            "PUNSUBSCRIBE",
            "PUBLISH",
            "SPUBLISH",
            "SSUBSCRIBE",
            "SUNSUBSCRIBE",
            // scripting containers (EVAL/EVALSHA/FCALL are movable-key handled)
            "SCRIPT",
            "FUNCTION",
            // transactions
            "MULTI",
            "EXEC",
            "DISCARD",
            "TXN",
            "UNWATCH",
            // replication / cluster
            "REPLICAOF",
            "SLAVEOF",
            "REPLCONF",
            "PSYNC",
            "CLUSTER",
            "CDC.READ",
            // moon extensions addressing indexes/graphs/queues, not keys
            "WS",
            "MQ",
            "TEMPORAL.SNAPSHOT_AT",
            "TEMPORAL.INVALIDATE",
        ];

        let mut unreviewed: Vec<&str> = metadata::COMMAND_META
            .entries()
            .filter(|(_, meta)| meta.first_key == 0)
            .map(|(name, _)| *name)
            .filter(|name| {
                !REVIEWED_KEYLESS.contains(name)
                    && !name.starts_with("FT.")
                    && !name.starts_with("GRAPH.")
                    // movable-key commands: their registry spec says 0 but
                    // this module extracts their keys explicitly.
                    && movable_shape(name.as_bytes()).is_none()
            })
            .collect();
        unreviewed.sort_unstable();
        assert!(
            unreviewed.is_empty(),
            "these registry commands declare NO keys and are not reviewed as keyless — \
             if they touch keys, give them a key spec (or a movable-key arm) or the ACL \
             key check will not enforce ~patterns for them: {:?}",
            unreviewed
        );
    }

    // ── end-to-end through `AclTable::check_key_permission` ───────────────
    //
    // The tests above pin the extraction itself; these pin the ANSWER a
    // key-restricted user gets, which is the property moon#566 was about.
    // They live here rather than in `table.rs` because the defect and the
    // fix are both in this module (and `table.rs` is at its size ceiling).
    /// moon#566. `extract_command_keys` was a hand-maintained name-keyed
    /// match whose fallthrough returned an empty key list — and an empty key
    /// list does not mean "checked less precisely", it means the permission
    /// loop in `check_key_permission` never runs, so EVERY `~pattern` was
    /// silently ignored for the command.
    ///
    /// Every command below either was missing from that match entirely
    /// (COPY, ZRANGESTORE, the MPOP family, SINTERCARD, ZDIFF/ZINTER/ZUNION,
    /// SORT ... STORE, GEORADIUS ... STORE) or was listed with the WRONG
    /// key layout (SMOVE was treated as single-key, so its DESTINATION was
    /// unchecked). Each case is asserted in EVERY key position, so a fix that
    /// names only one argument position cannot pass.
    ///
    /// The in-pattern control in the same loop is what proves the fix is
    /// real enforcement rather than a blanket denial.
    #[test]
    fn test_check_key_permission_multi_key_commands_are_enforced() {
        let mut table = crate::acl::AclTable::new();
        table.apply_setuser("alice", &["on", "nopass", "~allowed:*", "+@all"]);

        fn b(s: &str) -> Frame {
            Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
        }

        // (command, is_write, allowed argv, list of argv that must be DENIED)
        let cases: &[(&[u8], bool, &[&str], &[&[&str]])] = &[
            // --- two-key forms: source AND destination ---
            (
                b"SMOVE",
                true,
                &["allowed:src", "allowed:dst", "m"],
                &[
                    &["other:src", "allowed:dst", "m"],
                    &["allowed:src", "other:dst", "m"],
                ],
            ),
            (
                b"COPY",
                true,
                &["allowed:src", "allowed:dst"],
                &[&["other:src", "allowed:dst"], &["allowed:src", "other:dst"]],
            ),
            (
                b"ZRANGESTORE",
                true,
                &["allowed:dst", "allowed:src", "0", "-1"],
                &[
                    &["other:dst", "allowed:src", "0", "-1"],
                    &["allowed:dst", "other:src", "0", "-1"],
                ],
            ),
            (
                b"BRPOPLPUSH",
                true,
                &["allowed:src", "allowed:dst", "0"],
                &[
                    &["other:src", "allowed:dst", "0"],
                    &["allowed:src", "other:dst", "0"],
                ],
            ),
            (
                b"LMOVE",
                true,
                &["allowed:src", "allowed:dst", "LEFT", "RIGHT"],
                &[
                    &["other:src", "allowed:dst", "LEFT", "RIGHT"],
                    &["allowed:src", "other:dst", "LEFT", "RIGHT"],
                ],
            ),
            // --- numkeys walkers: EVERY key slot must be checked ---
            (
                b"LMPOP",
                true,
                &["2", "allowed:a", "allowed:b", "LEFT"],
                &[
                    &["2", "other:a", "allowed:b", "LEFT"],
                    &["2", "allowed:a", "other:b", "LEFT"],
                ],
            ),
            (
                b"ZMPOP",
                true,
                &["2", "allowed:a", "allowed:b", "MIN"],
                &[
                    &["2", "other:a", "allowed:b", "MIN"],
                    &["2", "allowed:a", "other:b", "MIN"],
                ],
            ),
            (
                b"BLMPOP",
                true,
                &["0", "2", "allowed:a", "allowed:b", "LEFT"],
                &[
                    &["0", "2", "other:a", "allowed:b", "LEFT"],
                    &["0", "2", "allowed:a", "other:b", "LEFT"],
                ],
            ),
            (
                b"BZMPOP",
                true,
                &["0", "2", "allowed:a", "allowed:b", "MIN"],
                &[
                    &["0", "2", "other:a", "allowed:b", "MIN"],
                    &["0", "2", "allowed:a", "other:b", "MIN"],
                ],
            ),
            (
                b"SINTERCARD",
                false,
                &["2", "allowed:a", "allowed:b"],
                &[
                    &["2", "other:a", "allowed:b"],
                    &["2", "allowed:a", "other:b"],
                ],
            ),
            (
                b"ZDIFF",
                false,
                &["2", "allowed:a", "allowed:b"],
                &[
                    &["2", "other:a", "allowed:b"],
                    &["2", "allowed:a", "other:b"],
                ],
            ),
            (
                b"ZINTER",
                false,
                &["2", "allowed:a", "allowed:b"],
                &[
                    &["2", "other:a", "allowed:b"],
                    &["2", "allowed:a", "other:b"],
                ],
            ),
            (
                b"ZUNION",
                false,
                &["2", "allowed:a", "allowed:b"],
                &[
                    &["2", "other:a", "allowed:b"],
                    &["2", "allowed:a", "other:b"],
                ],
            ),
            (
                b"ZINTERCARD",
                false,
                &["2", "allowed:a", "allowed:b"],
                &[
                    &["2", "other:a", "allowed:b"],
                    &["2", "allowed:a", "other:b"],
                ],
            ),
            // --- dest + numkeys sources ---
            (
                b"ZUNIONSTORE",
                true,
                &["allowed:dst", "2", "allowed:a", "allowed:b"],
                &[
                    &["other:dst", "2", "allowed:a", "allowed:b"],
                    &["allowed:dst", "2", "other:a", "allowed:b"],
                    &["allowed:dst", "2", "allowed:a", "other:b"],
                ],
            ),
            (
                b"ZINTERSTORE",
                true,
                &["allowed:dst", "2", "allowed:a", "allowed:b"],
                &[&["allowed:dst", "2", "allowed:a", "other:b"]],
            ),
            // --- STORE clauses (positional, invisible to first/last/step) ---
            (
                b"SORT",
                true,
                &["allowed:src", "STORE", "allowed:dst"],
                &[
                    &["other:src", "STORE", "allowed:dst"],
                    &["allowed:src", "STORE", "other:dst"],
                ],
            ),
            (
                b"GEORADIUS",
                true,
                &["allowed:src", "1", "2", "3", "m", "STORE", "allowed:dst"],
                &[
                    &["other:src", "1", "2", "3", "m", "STORE", "allowed:dst"],
                    &["allowed:src", "1", "2", "3", "m", "STORE", "other:dst"],
                    &["allowed:src", "1", "2", "3", "m", "STOREDIST", "other:dst"],
                ],
            ),
            (
                b"GEOSEARCHSTORE",
                true,
                &[
                    "allowed:dst",
                    "allowed:src",
                    "FROMMEMBER",
                    "m",
                    "BYRADIUS",
                    "1",
                    "m",
                    "ASC",
                ],
                &[
                    &[
                        "other:dst",
                        "allowed:src",
                        "FROMMEMBER",
                        "m",
                        "BYRADIUS",
                        "1",
                        "m",
                        "ASC",
                    ],
                    &[
                        "allowed:dst",
                        "other:src",
                        "FROMMEMBER",
                        "m",
                        "BYRADIUS",
                        "1",
                        "m",
                        "ASC",
                    ],
                ],
            ),
            // --- scripting: declared keys are real key accesses ---
            (
                b"EVAL",
                true,
                &["return 1", "2", "allowed:a", "allowed:b"],
                &[
                    &["return 1", "2", "other:a", "allowed:b"],
                    &["return 1", "2", "allowed:a", "other:b"],
                ],
            ),
            (
                b"EVALSHA",
                true,
                &["deadbeef", "1", "allowed:a"],
                &[&["deadbeef", "1", "other:a"]],
            ),
            (
                b"FCALL",
                true,
                &["fn", "1", "allowed:a"],
                &[&["fn", "1", "other:a"]],
            ),
            (
                b"FCALL_RO",
                false,
                &["fn", "1", "allowed:a"],
                &[&["fn", "1", "other:a"]],
            ),
            // --- streams: keys live after the STREAMS token ---
            (
                b"XREAD",
                false,
                &["COUNT", "1", "STREAMS", "allowed:a", "allowed:b", "0", "0"],
                &[
                    &["COUNT", "1", "STREAMS", "other:a", "allowed:b", "0", "0"],
                    &["COUNT", "1", "STREAMS", "allowed:a", "other:b", "0", "0"],
                ],
            ),
            (
                b"XREADGROUP",
                true,
                &["GROUP", "g", "c", "STREAMS", "allowed:a", ">"],
                &[&["GROUP", "g", "c", "STREAMS", "other:a", ">"]],
            ),
            // --- subcommand-shaped key positions ---
            (
                b"OBJECT",
                false,
                &["ENCODING", "allowed:a"],
                &[&["ENCODING", "other:a"]],
            ),
            (
                b"XINFO",
                false,
                &["STREAM", "allowed:a"],
                &[&["STREAM", "other:a"]],
            ),
            (
                b"MEMORY",
                false,
                &["USAGE", "allowed:a"],
                &[&["USAGE", "other:a"]],
            ),
            (
                b"XGROUP",
                true,
                &["CREATE", "allowed:a", "g", "$"],
                &[&["CREATE", "other:a", "g", "$"]],
            ),
            // --- regression controls that were already enforced ---
            (b"GET", false, &["allowed:a"], &[&["other:a"]]),
            (b"SET", true, &["allowed:a", "v"], &[&["other:a", "v"]]),
            (
                b"MSET",
                true,
                &["allowed:a", "v", "allowed:b", "v"],
                &[&["allowed:a", "v", "other:b", "v"]],
            ),
            (
                b"DEL",
                true,
                &["allowed:a", "allowed:b"],
                &[&["allowed:a", "other:b"]],
            ),
            (
                b"BITOP",
                true,
                &["AND", "allowed:dst", "allowed:a"],
                &[
                    &["AND", "other:dst", "allowed:a"],
                    &["AND", "allowed:dst", "other:a"],
                ],
            ),
            (
                b"LCS",
                false,
                &["allowed:a", "allowed:b"],
                &[&["allowed:a", "other:b"]],
            ),
        ];

        for (cmd, is_write, allowed, denied_variants) in cases {
            let name = String::from_utf8_lossy(cmd);
            let ok_args: Vec<Frame> = allowed.iter().map(|s| b(s)).collect();
            assert!(
                table
                    .check_key_permission("alice", cmd, &ok_args, *is_write)
                    .is_none(),
                "{} with only in-pattern keys must be ALLOWED (over-denial): {:?}",
                name,
                allowed
            );
            for variant in *denied_variants {
                let args: Vec<Frame> = variant.iter().map(|s| b(s)).collect();
                assert!(
                    table
                        .check_key_permission("alice", cmd, &args, *is_write)
                        .is_some(),
                    "{} must be DENIED on an out-of-pattern key: {:?}",
                    name,
                    variant
                );
            }
        }
    }

    /// moon#566 fail-closed half: a command whose key arguments cannot be
    /// enumerated must be DENIED, not allowed. Both shapes below reach the
    /// permission check with no extractable key, and both used to fall
    /// through to "no keys -> nothing to check -> allowed".
    #[test]
    fn test_check_key_permission_fails_closed_when_keys_unknown() {
        let mut table = crate::acl::AclTable::new();
        table.apply_setuser("alice", &["on", "nopass", "~allowed:*", "+@all"]);

        // A command that is not in the registry at all: nothing can vouch
        // for which keys it touches, so it must not be waved through.
        let args = vec![Frame::BulkString(Bytes::from_static(b"other:a"))];
        assert!(
            table
                .check_key_permission("alice", b"NOTACOMMAND", &args, true)
                .is_some(),
            "an unregistered command must fail CLOSED for a key-restricted user"
        );

        // A registered keyed command whose declared key position is absent
        // or unusable (malformed argv) must also fail closed.
        assert!(
            table
                .check_key_permission("alice", b"GET", &[], false)
                .is_some(),
            "GET with no key argument must fail closed, not allow"
        );
        let bad = vec![Frame::Integer(7)];
        assert!(
            table
                .check_key_permission("alice", b"GET", &bad, false)
                .is_some(),
            "GET whose key argument is not a string must fail closed"
        );

        // SORT's BY/GET patterns read arbitrary keys that no key spec can
        // name, so a key-restricted user must not be able to use them.
        let by_args = vec![
            Frame::BulkString(Bytes::from_static(b"allowed:src")),
            Frame::BulkString(Bytes::from_static(b"BY")),
            Frame::BulkString(Bytes::from_static(b"secret:*->w")),
        ];
        assert!(
            table
                .check_key_permission("alice", b"SORT", &by_args, true)
                .is_some(),
            "SORT BY <pattern> must fail closed for a key-restricted user"
        );
    }

    /// The other side of fail-closed: a command that genuinely has NO keys
    /// must stay usable. If this regresses, restricted users lose PING,
    /// HELLO, SUBSCRIBE and the whole admin surface.
    #[test]
    fn test_check_key_permission_keyless_commands_unaffected() {
        let mut table = crate::acl::AclTable::new();
        table.apply_setuser("alice", &["on", "nopass", "~allowed:*", "+@all"]);

        let one = vec![Frame::BulkString(Bytes::from_static(b"anything"))];
        let keyless: &[(&[u8], &[Frame])] = &[
            (b"PING", &[]),
            (b"ECHO", &one),
            (b"HELLO", &[]),
            (b"AUTH", &one),
            (b"RESET", &[]),
            (b"SELECT", &one),
            (b"COMMAND", &[]),
            (b"CONFIG", &one),
            (b"CLIENT", &one),
            (b"INFO", &one),
            (b"ACL", &one),
            (b"DBSIZE", &[]),
            (b"KEYS", &one),
            (b"SCAN", &one),
            (b"RANDOMKEY", &[]),
            (b"FLUSHALL", &[]),
            (b"FLUSHDB", &[]),
            (b"SWAPDB", &one),
            (b"MULTI", &[]),
            (b"EXEC", &[]),
            (b"DISCARD", &[]),
            (b"UNWATCH", &[]),
            (b"SUBSCRIBE", &one),
            (b"PUBLISH", &one),
            (b"PUBSUB", &one),
            (b"HEALTHZ", &[]),
            (b"READYZ", &[]),
            (b"ASKING", &[]),
            (b"READONLY", &[]),
            (b"READWRITE", &[]),
            (b"CLUSTER", &one),
            (b"SCRIPT", &one),
            (b"FUNCTION", &one),
            (b"WAIT", &one),
            (b"TIME", &[]),
            (b"FT.SEARCH", &one),
            (b"FT.INVALIDATE_RANGE", &one),
            (b"GRAPH.QUERY", &one),
            (b"TEMPORAL.INVALIDATE", &one),
            (b"CDC.READ", &one),
            (b"WS", &one),
            (b"MQ", &one),
            (b"TXN", &one),
            (b"XINFO", &[]),
            (b"OBJECT", &[]),
            (b"MEMORY", &[]),
        ];
        for (cmd, args) in keyless {
            assert!(
                table
                    .check_key_permission("alice", cmd, args, true)
                    .is_none(),
                "{} names no key and must not be denied by the key check",
                String::from_utf8_lossy(cmd)
            );
        }
    }
}
