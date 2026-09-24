//! Keyspace notification flags: `notify-keyspace-events`.
//!
//! The flag string is not a set — it is an ordered canonical form, and clients
//! read it back. Every rule below was MEASURED against redis-server 8.6.1
//! rather than recalled, because the ordering is not what the obvious reading
//! of the letters suggests:
//!
//! ```text
//!   KEA            -> AKE      A collapses the ten class flags
//!   Kg$            -> g$K      classes first, then K/E
//!   Km             -> Km       ...but m trails K/E, unlike the other letters
//!   mn             -> nm       n is a CLASS letter, m is not
//!   An             -> A        so `A` swallows n as well
//!   Amn            -> Am       ...while m survives it
//!   g$lshzxetdmnKE -> AKEm
//! ```
//!
//! Emission order is therefore: `A` **or** the class letters
//! `g $ l s h z x e t d n`, then `K`, `E`, and finally `m`. `A` is emitted
//! whenever all ten classes are present — `n` is not required for it, but is
//! suppressed by it.

/// Which events fire, and whether they are delivered.
///
/// A plain bitset newtype rather than a `bitflags!` macro: the crate is not a
/// direct dependency of moon, and this needs six operations.
///
/// `KEYSPACE`/`KEYEVENT` are not classes — they select the two channel
/// families. With neither set nothing is delivered however many class flags
/// are on, which is why the default is genuinely zero-cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NotifyFlags(u16);

impl NotifyFlags {
    /// `K` — publish to `__keyspace@<db>__:<key>`.
    pub const KEYSPACE: NotifyFlags = NotifyFlags(1 << 0);
    /// `E` — publish to `__keyevent@<db>__:<event>`.
    pub const KEYEVENT: NotifyFlags = NotifyFlags(1 << 1);
    /// `g` — generic commands (DEL, EXPIRE, RENAME ...).
    pub const GENERIC: NotifyFlags = NotifyFlags(1 << 2);
    /// `$` — string commands.
    pub const STRING: NotifyFlags = NotifyFlags(1 << 3);
    /// `l` — list commands.
    pub const LIST: NotifyFlags = NotifyFlags(1 << 4);
    /// `s` — set commands.
    pub const SET: NotifyFlags = NotifyFlags(1 << 5);
    /// `h` — hash commands.
    pub const HASH: NotifyFlags = NotifyFlags(1 << 6);
    /// `z` — sorted set commands.
    pub const ZSET: NotifyFlags = NotifyFlags(1 << 7);
    /// `x` — expired events.
    pub const EXPIRED: NotifyFlags = NotifyFlags(1 << 8);
    /// `e` — evicted events.
    pub const EVICTED: NotifyFlags = NotifyFlags(1 << 9);
    /// `t` — stream commands.
    pub const STREAM: NotifyFlags = NotifyFlags(1 << 10);
    /// `d` — module key type events.
    pub const MODULE: NotifyFlags = NotifyFlags(1 << 11);
    /// `m` — key-miss events. Deliberately NOT part of `A`: it would put a
    /// pub/sub fan-out on the read path.
    pub const KEY_MISS: NotifyFlags = NotifyFlags(1 << 12);
    /// `n` — new-key events. Suppressed by `A`'s collapse but not required
    /// for it.
    pub const NEW_KEY: NotifyFlags = NotifyFlags(1 << 13);

    /// No flags — notifications off.
    pub const NONE: NotifyFlags = NotifyFlags(0);

    /// Union.
    #[inline]
    pub const fn union(self, other: NotifyFlags) -> NotifyFlags {
        NotifyFlags(self.0 | other.0)
    }

    /// `true` when every bit of `other` is set here.
    #[inline]
    pub const fn contains(self, other: NotifyFlags) -> bool {
        self.0 & other.0 == other.0
    }

    /// `true` when any bit of `other` is set here.
    #[inline]
    pub const fn intersects(self, other: NotifyFlags) -> bool {
        self.0 & other.0 != 0
    }

    /// `true` when no flag is set.
    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Raw bits, for storing the value in an atomic.
    #[inline]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Rebuild from raw bits read out of an atomic.
    #[inline]
    pub const fn from_bits(bits: u16) -> NotifyFlags {
        NotifyFlags(bits)
    }
}

impl std::ops::BitOrAssign for NotifyFlags {
    fn bitor_assign(&mut self, rhs: NotifyFlags) {
        self.0 |= rhs.0;
    }
}

impl NotifyFlags {
    /// The `A` class: every type/event class except `m` and `n`.
    pub const ALL_CLASSES: NotifyFlags = NotifyFlags(
        NotifyFlags::GENERIC.0
            | NotifyFlags::STRING.0
            | NotifyFlags::LIST.0
            | NotifyFlags::SET.0
            | NotifyFlags::HASH.0
            | NotifyFlags::ZSET.0
            | NotifyFlags::EXPIRED.0
            | NotifyFlags::EVICTED.0
            | NotifyFlags::STREAM.0
            | NotifyFlags::MODULE.0,
    );

    /// Every class letter, including the two `A` leaves out.
    const ANY_CLASS: NotifyFlags =
        NotifyFlags(NotifyFlags::ALL_CLASSES.0 | NotifyFlags::KEY_MISS.0 | NotifyFlags::NEW_KEY.0);

    /// `true` when at least one event could actually be delivered.
    ///
    /// Class flags with neither `K` nor `E` deliver nothing, and `K`/`E` with
    /// no class selects nothing to deliver — so the emit path must check this
    /// rather than merely "are any flags set".
    #[inline]
    pub const fn is_enabled(self) -> bool {
        self.intersects(NotifyFlags(
            NotifyFlags::KEYSPACE.0 | NotifyFlags::KEYEVENT.0,
        )) && self.intersects(NotifyFlags::ANY_CLASS)
    }
}

/// Process-global published flags.
///
/// Read on every mutation that could notify, so it must be a Relaxed atomic
/// load and nothing more — a config-lock read here would put a lock on the
/// write path. Same publish contract as `maxmemory`: every write site of the
/// config value must call [`publish_flags`], or the emit path silently
/// disagrees with `CONFIG GET`.
static PUBLISHED: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);

/// Publish the active flag set. Startup and `CONFIG SET`.
#[inline]
pub fn publish_flags(flags: NotifyFlags) {
    PUBLISHED.store(flags.bits(), std::sync::atomic::Ordering::Relaxed);
}

/// The active flag set.
#[inline]
pub fn published_flags() -> NotifyFlags {
    NotifyFlags::from_bits(PUBLISHED.load(std::sync::atomic::Ordering::Relaxed))
}

/// `true` when any event could be delivered — the one check the write path
/// pays when notifications are off (a Relaxed load and two masks).
#[inline]
pub fn notifications_enabled() -> bool {
    published_flags().is_enabled()
}

// ── moon#1214 item 2: gate event construction on a live `__key*` listener ───
//
// With `notify-keyspace-events` enabled but NOBODY subscribed to a
// `__keyspace@*`/`__keyevent@*` channel or pattern, HEAD still paid a key copy
// and ~3 allocations per mutating command. This process-global count, kept in
// step by (P)SUBSCRIBE/(P)UNSUBSCRIBE/disconnect (see `pubsub`), makes that case
// free: `notify_keyspace_event` returns before allocating when the count is 0.
//
// The count tracks the number of DISTINCT keyspace-relevant channel/pattern
// entries with at least one subscriber, summed across shard registries — a
// present↔absent transition per entry, so increments and decrements balance and
// it never leaks. Decrement is saturating as belt-and-braces (moon#1214: "counts
// never go negative across disconnects"). It is a HINT that only gates a pure
// optimisation: a late (P)SUBSCRIBE that raises it from 0 makes every SUBSEQUENT
// event flow, which is exactly Redis's own guarantee (a subscription established
// after a write does not receive that write's event).

static KEYSPACE_LISTENERS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Record that a keyspace-relevant channel/pattern gained its first subscriber.
#[inline]
pub fn keyspace_listener_added() {
    KEYSPACE_LISTENERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Record that a keyspace-relevant channel/pattern lost its last subscriber.
/// Saturating at 0.
#[inline]
pub fn keyspace_listener_removed() {
    let _ = KEYSPACE_LISTENERS.fetch_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |v| Some(v.saturating_sub(1)),
    );
}

/// `true` when at least one client is subscribed to a `__keyspace@*`/
/// `__keyevent@*` channel or pattern anywhere in the process.
#[inline]
pub fn has_keyspace_listener() -> bool {
    KEYSPACE_LISTENERS.load(std::sync::atomic::Ordering::Relaxed) > 0
}

/// The live keyspace-listener count (tests only).
#[cfg(test)]
pub fn keyspace_listener_count() -> usize {
    KEYSPACE_LISTENERS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether a SUBSCRIBE channel or PSUBSCRIBE pattern could deliver keyspace
/// notifications — i.e. could match a `__keyspace@<db>__:…` or
/// `__keyevent@<db>__:…` channel.
///
/// Conservative by construction: it compares the pattern's LITERAL prefix (the
/// bytes before the first glob metacharacter) against the two families' shared
/// `__key…` prefix, so it NEVER undercounts a real listener (an undercount
/// would silently drop that listener's notifications). An overcount only costs
/// the optimisation, never correctness. An exact channel is a pattern with no
/// metacharacters, so the same test serves both.
///
/// The metacharacters are EVERY byte `glob_match` treats specially: `*`, `?`,
/// `[` and the `\` escape. A pattern can spell a keyspace channel's bytes
/// through escapes (`__keyspace\@0__:*`, `\_\_keyevent@0__:set`); stopping the
/// prefix only at `*?[` read the backslash as a literal, found the prefix
/// inconsistent, and dropped a real sole listener's events (moon#1227 review
/// M1). Treating `\` as a metacharacter errs toward counting: an escaped
/// pattern that could never match (`\x_keyspace@*`) is counted too, which costs
/// only the optimisation.
pub fn subscription_targets_keyspace(name: &[u8]) -> bool {
    let lit_end = name
        .iter()
        .position(|&b| matches!(b, b'*' | b'?' | b'[' | b'\\'))
        .unwrap_or(name.len());
    let lit = &name[..lit_end];
    prefix_consistent(lit, b"__keyspace@") || prefix_consistent(lit, b"__keyevent@")
}

/// `true` when `a` and `b` agree on their shared-length prefix — so a literal
/// shorter than `__keyspace@` (e.g. `__key`) is still treated as a potential
/// match.
#[inline]
fn prefix_consistent(a: &[u8], b: &[u8]) -> bool {
    let n = a.len().min(b.len());
    a[..n] == b[..n]
}

/// One event waiting to be published, produced by command code and consumed
/// by whichever layer owns this shard's cross-shard mesh.
#[derive(Debug, Clone)]
pub struct PendingNotification {
    /// Logical db the key lives in — part of both channel names.
    pub db: usize,
    /// Event name, e.g. `set`, `incrby`, `rename_from`. Always a literal:
    /// event names are a closed set, so this costs no allocation.
    pub event: &'static str,
    /// The key the event is about.
    pub key: bytes::Bytes,
}

thread_local! {
    /// Per-shard-thread outbox.
    ///
    /// Command code cannot publish directly: it has no access to the pub/sub
    /// registries, and — the real constraint — a subscriber's task lives on
    /// another shard thread, where a `Waker` from this thread does not reach
    /// it (see the monoio note in CLAUDE.md). So events are queued here and
    /// drained by a layer that holds the SPSC mesh, which is the only
    /// cross-thread wake that works.
    ///
    /// Thread-local rather than a field on the shard: expiry and eviction
    /// notify from the shard timer, command dispatch notifies from three
    /// different handlers, and threading a handle through all of them would
    /// touch every signature on the write path.
    static OUTBOX: std::cell::RefCell<Vec<PendingNotification>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Queue one keyspace event, if its class is enabled.
///
/// The disabled path is a Relaxed load and two masks — no allocation, no
/// lock, no thread-local access — which is what lets this sit on the write
/// path of every mutating command.
#[inline]
pub fn notify_keyspace_event(class: NotifyFlags, event: &'static str, key: &[u8], db: usize) {
    let flags = published_flags();
    if !flags.is_enabled() || !flags.intersects(class) {
        return;
    }
    // moon#1214 item 2: with the class enabled but no `__keyspace@*`/
    // `__keyevent@*` subscriber anywhere, the event would be built, queued and
    // fanned out only to be dropped. Skip the key copy + allocations entirely —
    // one extra Relaxed load on the write path. A late (P)SUBSCRIBE flips this
    // and every subsequent event flows.
    if !has_keyspace_listener() {
        return;
    }
    let pending = PendingNotification {
        db,
        event,
        key: bytes::Bytes::copy_from_slice(key),
    };
    OUTBOX.with(|o| o.borrow_mut().push(pending));
}

/// Take everything queued on this thread, leaving the outbox empty.
///
/// Returns `None` when there is nothing pending, so the overwhelmingly common
/// case allocates nothing and the caller can skip its fan-out entirely.
#[inline]
pub fn take_outbox() -> Option<Vec<PendingNotification>> {
    OUTBOX.with(|o| {
        let mut b = o.borrow_mut();
        if b.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut *b))
        }
    })
}

/// `true` when this thread has queued events. A borrow-and-check, cheaper
/// than [`take_outbox`] for a caller that only wants to know.
#[inline]
pub fn outbox_is_empty() -> bool {
    OUTBOX.with(|o| o.borrow().is_empty())
}

/// Render the `(channel, payload)` pairs one event publishes.
///
/// The two channels are INVERTED with respect to each other, which is the
/// detail consumers get wrong: `__keyspace@<db>__:<key>` carries the EVENT,
/// while `__keyevent@<db>__:<event>` carries the KEY.
pub fn channels_for(
    n: &PendingNotification,
    flags: NotifyFlags,
) -> Vec<(bytes::Bytes, bytes::Bytes)> {
    let mut out = Vec::with_capacity(2);
    if flags.contains(NotifyFlags::KEYSPACE) {
        let mut ch = Vec::with_capacity(16 + n.key.len());
        ch.extend_from_slice(b"__keyspace@");
        ch.extend_from_slice(itoa::Buffer::new().format(n.db).as_bytes());
        ch.extend_from_slice(b"__:");
        ch.extend_from_slice(&n.key);
        out.push((
            bytes::Bytes::from(ch),
            bytes::Bytes::from_static(n.event.as_bytes()),
        ));
    }
    if flags.contains(NotifyFlags::KEYEVENT) {
        let mut ch = Vec::with_capacity(16 + n.event.len());
        ch.extend_from_slice(b"__keyevent@");
        ch.extend_from_slice(itoa::Buffer::new().format(n.db).as_bytes());
        ch.extend_from_slice(b"__:");
        ch.extend_from_slice(n.event.as_bytes());
        out.push((bytes::Bytes::from(ch), n.key.clone()));
    }
    out
}

/// The valid characters, in the order Redis names them in its error message.
pub const VALID_FLAG_CHARS: &str = "Ag$lshzxeKEtmdn";

/// Redis 8.6.1's wording, verbatim — a config-management tool surfaces this
/// string unchanged, so paraphrasing it is a compatibility break.
pub const INVALID_FLAG_ERROR: &str = "Invalid event class character. Use 'Ag$lshzxeKEtmdn'.";

/// Parse a `notify-keyspace-events` flag string.
///
/// Returns `Err` naming the offending character's class set on the FIRST
/// invalid character, and parses nothing — a partially-applied flag set would
/// silently change which events fire.
pub fn parse_flags(s: &str) -> Result<NotifyFlags, &'static str> {
    let mut flags = NotifyFlags::NONE;
    for c in s.chars() {
        flags |= match c {
            'A' => NotifyFlags::ALL_CLASSES,
            'K' => NotifyFlags::KEYSPACE,
            'E' => NotifyFlags::KEYEVENT,
            'g' => NotifyFlags::GENERIC,
            '$' => NotifyFlags::STRING,
            'l' => NotifyFlags::LIST,
            's' => NotifyFlags::SET,
            'h' => NotifyFlags::HASH,
            'z' => NotifyFlags::ZSET,
            'x' => NotifyFlags::EXPIRED,
            'e' => NotifyFlags::EVICTED,
            't' => NotifyFlags::STREAM,
            'd' => NotifyFlags::MODULE,
            'm' => NotifyFlags::KEY_MISS,
            'n' => NotifyFlags::NEW_KEY,
            _ => return Err(INVALID_FLAG_ERROR),
        };
    }
    Ok(flags)
}

/// Render flags back to their canonical string — what `CONFIG GET` returns.
///
/// Not the caller's spelling: `CONFIG SET KEA` reads back as `AKE`. See the
/// module docs for why `n` sits with the classes and `m` does not.
pub fn flags_to_string(flags: NotifyFlags) -> String {
    let mut out = String::with_capacity(8);
    if flags.contains(NotifyFlags::ALL_CLASSES) {
        out.push('A');
    } else {
        for (bit, ch) in [
            (NotifyFlags::GENERIC, 'g'),
            (NotifyFlags::STRING, '$'),
            (NotifyFlags::LIST, 'l'),
            (NotifyFlags::SET, 's'),
            (NotifyFlags::HASH, 'h'),
            (NotifyFlags::ZSET, 'z'),
            (NotifyFlags::EXPIRED, 'x'),
            (NotifyFlags::EVICTED, 'e'),
            (NotifyFlags::STREAM, 't'),
            (NotifyFlags::MODULE, 'd'),
            (NotifyFlags::NEW_KEY, 'n'),
        ] {
            if flags.contains(bit) {
                out.push(ch);
            }
        }
    }
    if flags.contains(NotifyFlags::KEYSPACE) {
        out.push('K');
    }
    if flags.contains(NotifyFlags::KEYEVENT) {
        out.push('E');
    }
    if flags.contains(NotifyFlags::KEY_MISS) {
        out.push('m');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// moon#1214 item 2: the listener classifier must flag every channel /
    /// pattern that COULD receive keyspace notifications (never undercount — an
    /// undercount silently drops a real listener's events), while leaving
    /// unrelated channels free.
    #[test]
    fn keyspace_subscription_classifier_never_undercounts() {
        // Real keyspace/keyevent targets — exact and pattern forms.
        for name in [
            &b"__keyspace@0__:foo"[..],
            b"__keyevent@0__:expired",
            b"__keyspace@*__:*",
            b"__keyevent@0__:*",
            b"__key*",      // matches both families
            b"__keyspace@", // degenerate but consistent -> conservative yes
            b"*",           // matches everything
            b"__*",         // prefix consistent with __key...
        ] {
            assert!(
                subscription_targets_keyspace(name),
                "must be treated as a keyspace listener: {:?}",
                String::from_utf8_lossy(name)
            );
        }
        // Unrelated channels — free to skip.
        for name in [
            &b"news.tech"[..],
            b"chat:*",
            b"__keyspac", // diverges before the '@' but shares "__keyspac"... still consistent
            b"foobar",
            b"_keyspace@0__:x", // missing leading underscore
        ] {
            // Only the genuinely-inconsistent ones must be false; the deliberately
            // tricky `__keyspac` shares a prefix so it is allowed to be true.
            if name.starts_with(b"__key") {
                continue;
            }
            assert!(
                !subscription_targets_keyspace(name),
                "must NOT be treated as a keyspace listener: {:?}",
                String::from_utf8_lossy(name)
            );
        }
    }

    /// moon#1227 review M1: `glob_match` honours `\x` escapes, so a pattern
    /// can spell a keyspace channel's literal bytes with backslashes
    /// (`__keyspace\@0__:*`). Every pattern that matches ANY keyspace or
    /// keyevent channel must be counted, or a sole such listener's events are
    /// dropped before they are built. Property-tested against the real
    /// matcher on patterns derived from sample channels (escapes, `?`,
    /// classes, negated classes, ranges, `*`), plus the review's patterns.
    #[test]
    fn every_pattern_that_matches_a_keyspace_channel_is_counted() {
        use crate::command::key::glob_match;

        let channels: [&[u8]; 7] = [
            b"__keyspace@0__:foo",
            b"__keyevent@0__:set",
            b"__keyspace@12__:user:1",
            b"__keyevent@3__:expired",
            b"__keyspace@0__:",
            b"__keyspace@0__:a*b?[c]\\d",
            b"__keyevent@0__:hset",
        ];
        let matches_any = |pat: &[u8]| channels.iter().any(|c| glob_match(pat, c));

        // The review's patterns: each really matches, so each must count.
        for pat in [
            &br"__keyspace\@0__:*"[..],
            br"\_\_keyspace@0__:*",
            br"__key\space@*",
            br"\__keyevent@0__:set",
        ] {
            assert!(
                matches_any(pat),
                "{:?} must match a channel",
                String::from_utf8_lossy(pat)
            );
            assert!(
                subscription_targets_keyspace(pat),
                "undercounted {:?}",
                String::from_utf8_lossy(pat)
            );
        }

        // Derived patterns: rewrite each byte of a channel into a construct
        // that still matches it — or, now and then, into one that does not,
        // so the property is also exercised on non-matching shapes.
        let mut state = 0x1227_u64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as usize
        };
        let mut matched = 0usize;
        for _ in 0..20_000 {
            let ch = channels[next() % channels.len()];
            let mut pat = Vec::with_capacity(ch.len() * 3);
            // Keep a random-length prefix, then optionally close with `*`.
            let keep = next() % (ch.len() + 1);
            for &b in &ch[..keep] {
                match next() % 10 {
                    0 | 1 => pat.extend_from_slice(&[b'\\', b]),
                    2 => pat.push(b'?'),
                    3 => pat.extend_from_slice(&[b'[', b, b'q', b']']),
                    4 => pat.extend_from_slice(if b == b'z' { b"[^y]" } else { b"[^z]" }),
                    5 => pat.extend_from_slice(&[
                        b'[',
                        b.saturating_sub(1),
                        b'-',
                        b.saturating_add(1),
                        b']',
                    ]),
                    6 if next() % 4 == 0 => pat.push(b'*'),
                    7 if next() % 16 == 0 => pat.push(b'x'), // a mismatch
                    _ => pat.push(b),
                }
            }
            if keep < ch.len() || next() % 2 == 0 {
                pat.push(b'*');
            }
            if matches_any(&pat) {
                matched += 1;
                assert!(
                    subscription_targets_keyspace(&pat),
                    "{:?} matches a keyspace channel but is not counted",
                    String::from_utf8_lossy(&pat)
                );
            }
        }
        assert!(
            matched > 10_000,
            "the generator must mostly produce matching patterns ({matched})"
        );

        // Precision is kept where escapes cannot reach a keyspace channel.
        for pat in [&br"chat\:*"[..], br"news.\*", br"x\_keyspace@*"] {
            assert!(
                !subscription_targets_keyspace(pat),
                "{:?} can never match a keyspace channel",
                String::from_utf8_lossy(pat)
            );
        }
    }

    /// The listener count is a saturating counter: balanced add/remove returns
    /// to zero, and an extra remove can never drive it negative (moon#1214:
    /// "counts never go negative across disconnects").
    #[test]
    fn listener_count_is_balanced_and_saturating() {
        let start = keyspace_listener_count();
        keyspace_listener_added();
        keyspace_listener_added();
        assert_eq!(keyspace_listener_count(), start + 2);
        keyspace_listener_removed();
        keyspace_listener_removed();
        assert_eq!(keyspace_listener_count(), start);
        // Underflow guard: an unpaired remove saturates at 0, never wraps.
        // (Only meaningful when the process count is already 0.)
        if start == 0 {
            keyspace_listener_removed();
            assert_eq!(keyspace_listener_count(), 0);
        }
    }

    /// Every pair here was captured from a running redis-server 8.6.1, not
    /// derived from the letters. `Km -> Km` and `mn -> nm` are the two that
    /// disprove the obvious "one ordered list" model.
    #[test]
    fn canonical_form_matches_measured_redis() {
        for (input, want) in [
            ("KEA", "AKE"),
            ("Kg$", "g$K"),
            ("xe", "xe"),
            ("Km", "Km"),
            ("mK", "Km"),
            ("Em", "Em"),
            ("KEm", "KEm"),
            ("mn", "nm"),
            ("nm", "nm"),
            ("KEmn", "nKEm"),
            ("Amn", "Am"),
            ("An", "A"),
            ("nA", "A"),
            ("Anm", "Am"),
            ("n", "n"),
            ("nK", "nK"),
            ("Kn", "nK"),
            ("nE", "nE"),
            ("gn", "gn"),
            ("nd", "dn"),
            ("dn", "dn"),
            ("tn", "tn"),
            ("gxE", "gxE"),
            ("g$lshzxetdn", "A"),
            ("g$lshzxetdmnKE", "AKEm"),
            ("EKdtezxhslg$", "AKE"),
            ("", ""),
            ("K", "K"),
            ("A", "A"),
        ] {
            let parsed = parse_flags(input).expect("valid flag string");
            assert_eq!(
                flags_to_string(parsed),
                want,
                "canonicalization of {input:?} diverges from redis-server 8.6.1"
            );
        }
    }

    #[test]
    fn canonical_form_is_idempotent() {
        // A client that writes back what it read must not drift the config.
        for input in ["KEA", "Kg$", "Km", "KEmn", "Amn", "gn", "g$lshzxetdmnKE"] {
            let once = flags_to_string(parse_flags(input).expect("valid"));
            let twice = flags_to_string(parse_flags(&once).expect("canonical form re-parses"));
            assert_eq!(once, twice, "canonical form of {input:?} is not a fixpoint");
        }
    }

    #[test]
    fn invalid_char_is_rejected_with_redis_wording() {
        // 'Q' is not in the class set. The message is compared verbatim by
        // config-management tooling.
        assert_eq!(parse_flags("KEQ"), Err(INVALID_FLAG_ERROR));
        assert!(INVALID_FLAG_ERROR.contains(VALID_FLAG_CHARS));
    }

    #[test]
    fn a_excludes_keymiss_and_newkey() {
        // The reason `A` is safe to enable in production: neither of the two
        // read-path classes is in it.
        let a = parse_flags("A").expect("valid");
        assert!(!a.contains(NotifyFlags::KEY_MISS), "'m' must not be in 'A'");
        assert!(!a.contains(NotifyFlags::NEW_KEY), "'n' must not be in 'A'");
    }

    #[test]
    fn classes_without_k_or_e_deliver_nothing() {
        // kn8's invariant, at the unit level: K/E select WHETHER, classes
        // select WHICH. All the classes in the world with neither is silence.
        assert!(!parse_flags("g$").expect("valid").is_enabled());
        assert!(!parse_flags("A").expect("valid").is_enabled());
        // ...and K/E with no class is equally silent.
        assert!(!parse_flags("KE").expect("valid").is_enabled());
        assert!(parse_flags("KEA").expect("valid").is_enabled());
        assert!(parse_flags("Km").expect("valid").is_enabled());
    }
}
