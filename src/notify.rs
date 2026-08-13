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
