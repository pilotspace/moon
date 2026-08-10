use bytes::Bytes;

use super::affinity::MigratedConnectionState;
use crate::protocol::Frame;

/// Extract command name (as raw byte slice reference) and args from a Frame::Array.
/// Returns the name without allocation -- callers use `eq_ignore_ascii_case` for matching.
pub(crate) fn extract_command(frame: &Frame) -> Option<(&[u8], &[Frame])> {
    match frame {
        Frame::Array(args) if !args.is_empty() => {
            let name = match &args[0] {
                Frame::BulkString(s) => s.as_ref(),
                Frame::SimpleString(s) => s.as_ref(),
                _ => return None,
            };
            Some((name, &args[1..]))
        }
        _ => None,
    }
}

/// Extract a Bytes value from a Frame argument.
pub(crate) fn extract_bytes(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// Restore connection state from migration, or use defaults for fresh connections.
///
/// Returns (protocol_version, selected_db, authenticated, current_user, client_name).
pub(crate) fn restore_migrated_state(
    migrated: Option<&MigratedConnectionState>,
    requirepass: &Option<String>,
) -> (u8, usize, bool, String, Option<Bytes>) {
    (
        migrated.map_or(2, |s| s.protocol_version),
        migrated.map_or(0, |s| s.selected_db),
        migrated.map_or(requirepass.is_none(), |s| s.authenticated),
        migrated.map_or_else(|| "default".to_string(), |s| s.current_user.clone()),
        migrated.and_then(|s| s.client_name.clone()),
    )
}

/// Classify a command's RESP3 reply shape from its name and arguments.
/// Uppercases the command name into a stack buffer for O(1) lookup.
///
/// Split out from [`apply_resp3_conversion`] because the cross-shard path must
/// classify at ENQUEUE time (where the args still exist) and apply later, when
/// the batch reply arrives carrying only the shape tag.
#[inline]
pub(crate) fn resp3_shape_for(cmd: &[u8], args: &[Frame]) -> crate::protocol::resp3::Resp3Shape {
    let mut cmd_upper_buf = [0u8; 32];
    let cmd_upper_len = cmd.len().min(32);
    cmd_upper_buf[..cmd_upper_len].copy_from_slice(&cmd[..cmd_upper_len]);
    cmd_upper_buf[..cmd_upper_len].make_ascii_uppercase();
    crate::protocol::resp3::resp3_shape_of(&cmd_upper_buf[..cmd_upper_len], args)
}

/// Apply RESP3 response type conversion for a command whose args are in scope.
///
/// `args` EXCLUDES the command name. The `proto < 3` early return happens
/// before any classification, so a RESP2 connection never pays for the lookup.
#[inline]
pub(crate) fn apply_resp3_conversion(
    cmd: &[u8],
    args: &[Frame],
    response: Frame,
    proto: u8,
) -> Frame {
    if proto < 3 {
        return response;
    }
    crate::protocol::resp3::apply_shape(resp3_shape_for(cmd, args), response, proto)
}

/// Propagate a subscription (or pattern subscription) to all remote shards' subscriber maps.
///
/// Called after subscribing locally to ensure remote shards know to forward published
/// messages to this shard. Acquires each shard's RemoteSubscriberMap write lock
/// individually (no nested locks).
pub(crate) fn propagate_subscription(
    all_remote_sub_maps: &[std::sync::Arc<
        parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    >],
    channel: &Bytes,
    shard_id: usize,
    num_shards: usize,
    is_pattern: bool,
) {
    for target in 0..num_shards {
        if target == shard_id {
            continue;
        }
        all_remote_sub_maps[target]
            .write()
            .add(channel.clone(), shard_id, is_pattern);
    }
}

/// Remove a subscription (or pattern subscription) from all remote shards' subscriber maps.
///
/// Called after unsubscribing locally. Acquires each shard's RemoteSubscriberMap write lock
/// individually (no nested locks).
pub(crate) fn unpropagate_subscription(
    all_remote_sub_maps: &[std::sync::Arc<
        parking_lot::RwLock<crate::shard::remote_subscriber_map::RemoteSubscriberMap>,
    >],
    channel: &Bytes,
    shard_id: usize,
    num_shards: usize,
    is_pattern: bool,
) {
    for target in 0..num_shards {
        if target == shard_id {
            continue;
        }
        all_remote_sub_maps[target]
            .write()
            .remove(channel, shard_id, is_pattern);
    }
}

/// Post-batch capacity governor for the per-connection batch scratch vectors
/// (c10k W1). `responses`/`frames` are cleared and reused across batches; one
/// deep pipeline grows them to the 1024-frame batch cap (~74 KB each at
/// 72 B/Frame) and `.clear()` never returns that capacity — measured as a
/// permanent ~160 KB/conn RSS ratchet until disconnect (tmp/C10K-REVIEW.md,
/// experiment E5). Shrinking only above the trigger keeps every realloc off
/// the small-batch path: a p99 batch (≤64 frames) never grows past the
/// trigger, so it never pays a shrink.
/// (Consumed by the monoio handler only — the tokio handler allocates its
/// batch vecs per batch; cfg-gated so the tokio-only build stays warning-free.)
#[cfg(any(feature = "runtime-monoio", test))]
pub(crate) const BATCH_VEC_STEADY_CAP: usize = 64;
#[cfg(any(feature = "runtime-monoio", test))]
pub(crate) const BATCH_VEC_SHRINK_TRIGGER: usize = 256;

/// Shrink an emptied batch scratch vector back to steady-state capacity.
/// Call only after `clear()` — shrinking an empty Vec moves no elements.
#[inline]
#[cfg(any(feature = "runtime-monoio", test))]
pub(crate) fn shrink_batch_vec<T>(v: &mut Vec<T>) {
    debug_assert!(v.is_empty(), "shrink_batch_vec expects a cleared vec");
    if v.capacity() > BATCH_VEC_SHRINK_TRIGGER {
        v.shrink_to(BATCH_VEC_STEADY_CAP);
    }
}

/// I/O buffer shrink floor (c10k W1): a connection that once carried a large
/// value keeps its high-water read/write/rent buffer until disconnect. The
/// old floor (64 KiB) let 16–64 KiB high-waters ratchet forever; 16 KiB
/// bounds steady-state at 2× the 8 KiB working size while still amortizing
/// growth for genuinely large frames in flight.
pub(crate) const IO_BUF_SHRINK_TRIGGER: usize = 16384;

/// Reply size at or above which a write arms the `--client-write-timeout-ms`
/// watchdog (c10k C1).
///
/// Arming a timer costs a wheel insert + removal on EVERY batch flush. At
/// pipeline depth 1 that lands once per command, on the path this project
/// spent a whole milestone winning against Redis — so it must not be paid
/// where it cannot buy anything.
///
/// A reply that fits in the socket buffer is handed to the kernel and returns
/// without ever blocking, so no timeout can fire for it. Linux's default
/// `net.core.wmem_default` is 208 KiB and autotuning only raises it; 256 KiB
/// is therefore comfortably inside "this write will not block".
///
/// Residual, stated plainly rather than hidden: a SMALL write to a socket
/// whose window is genuinely closed is still unbounded. It holds at most this
/// many bytes instead of the hundreds of megabytes C1 is about, but it does
/// keep its `maxclients` slot. That connection is now visible (`omem` > 0 in
/// CLIENT LIST) and killable (`CLIENT KILL` shuts the fd down, which makes the
/// blocked write return), which it was not before.
pub(crate) const WRITE_TIMEOUT_MIN_BYTES: usize = 256 * 1024;

/// Should this write arm the stall watchdog? `None` means "write unbounded".
#[inline]
pub(crate) fn arm_write_timeout(
    pending: usize,
    configured: Option<std::time::Duration>,
) -> Option<std::time::Duration> {
    if pending >= WRITE_TIMEOUT_MIN_BYTES {
        configured
    } else {
        None
    }
}

#[cfg(test)]
mod shrink_tests {
    use super::*;

    #[test]
    fn oversized_batch_vec_shrinks_to_steady_cap() {
        let mut v: Vec<u64> = Vec::with_capacity(1024);
        v.clear();
        shrink_batch_vec(&mut v);
        assert!(
            v.capacity() <= BATCH_VEC_SHRINK_TRIGGER,
            "capacity {} must shrink below trigger",
            v.capacity()
        );
        assert!(v.capacity() >= BATCH_VEC_STEADY_CAP);
    }

    #[test]
    fn steady_state_batch_vec_is_untouched() {
        let mut v: Vec<u64> = Vec::with_capacity(BATCH_VEC_SHRINK_TRIGGER);
        let cap_before = v.capacity();
        shrink_batch_vec(&mut v);
        assert_eq!(
            v.capacity(),
            cap_before,
            "at-trigger capacity must not shrink"
        );
    }

    #[test]
    fn small_batch_vec_is_untouched() {
        let mut v: Vec<u64> = Vec::with_capacity(BATCH_VEC_STEADY_CAP);
        let cap_before = v.capacity();
        shrink_batch_vec(&mut v);
        assert_eq!(v.capacity(), cap_before);
    }
}

/// Resolve the query-buffer ceiling that applies to a connection right now.
///
/// c10k hardening C2. The input buffer grows to whatever a frame header
/// declares — a `$536870911` bulk header plus a dribble of bytes pins half a
/// gigabyte, bounded only by the parser's 512 MiB ceiling — and that memory
/// is invisible to `used_memory`, so `maxmemory` never sees it. The auth gate
/// runs AFTER parsing, so this is reachable with no credentials at all: 20
/// connections is 10 GB.
///
/// Unauthenticated connections therefore get their own, much smaller ceiling.
/// No legitimate pre-auth command is large (AUTH, HELLO and the inline forms
/// all fit in well under a kilobyte), and the full limit applies from the
/// moment a client authenticates.
///
/// `0` means unlimited on either knob; a `0` pre-auth limit falls back to the
/// general one rather than to "unlimited".
#[inline]
pub(crate) fn query_buf_limit(authenticated: bool, limit: usize, preauth_limit: usize) -> usize {
    if authenticated {
        return limit;
    }
    match (preauth_limit, limit) {
        (0, l) => l,
        // Never let the pre-auth ceiling exceed the general one: an operator
        // who lowers `--client-query-buffer-limit` below the pre-auth default
        // means the lower number.
        (p, 0) => p,
        (p, l) => p.min(l),
    }
}

/// True when `len` has passed the resolved ceiling. Always false when the
/// resolved ceiling is `0` (unlimited).
#[inline]
pub(crate) fn query_buf_exceeded(
    len: usize,
    authenticated: bool,
    limit: usize,
    preauth_limit: usize,
) -> bool {
    let resolved = query_buf_limit(authenticated, limit, preauth_limit);
    resolved != 0 && len > resolved
}

/// The error moon sends before closing a connection that blew its query
/// buffer. Redis closes silently (and logs); we say why first, then close —
/// a silent close on a large pipeline is very hard to tell from a crash.
pub(crate) const QUERY_BUF_LIMIT_ERROR: &[u8] =
    b"-ERR Protocol error: query buffer limit reached\r\n";

#[cfg(test)]
mod query_buf_limit_tests {
    use super::*;

    #[test]
    fn preauth_ceiling_is_the_smaller_of_the_two() {
        // Defaults: 1 GiB general, 64 KiB pre-auth.
        assert_eq!(query_buf_limit(false, 1 << 30, 64 * 1024), 64 * 1024);
        assert_eq!(query_buf_limit(true, 1 << 30, 64 * 1024), 1 << 30);

        // An operator who lowers the general limit below the pre-auth default
        // means the lower number, even before auth.
        assert_eq!(query_buf_limit(false, 4096, 64 * 1024), 4096);

        // 0 pre-auth = "no separate pre-auth rule", not "unlimited".
        assert_eq!(query_buf_limit(false, 4096, 0), 4096);
        // 0 general = unlimited once authenticated, but the pre-auth rule
        // still binds before that.
        assert_eq!(query_buf_limit(true, 0, 64 * 1024), 0);
        assert_eq!(query_buf_limit(false, 0, 64 * 1024), 64 * 1024);
    }

    #[test]
    fn exceeded_is_strict_and_honours_unlimited() {
        // Exactly at the limit is fine; one byte past is not.
        assert!(!query_buf_exceeded(4096, true, 4096, 0));
        assert!(query_buf_exceeded(4097, true, 4096, 0));

        // Unlimited on both knobs never trips, at any size.
        assert!(!query_buf_exceeded(usize::MAX, true, 0, 0));
        assert!(!query_buf_exceeded(usize::MAX, false, 0, 0));

        // The pre-auth ceiling is what an unauthenticated client hits.
        assert!(query_buf_exceeded(64 * 1024 + 1, false, 1 << 30, 64 * 1024));
        assert!(!query_buf_exceeded(64 * 1024 + 1, true, 1 << 30, 64 * 1024));
    }
}

/// How long the `maxclients` rejection write may take before we give up and
/// close (c10k C3).
///
/// The reject is 36 bytes, so any peer that is reading at all completes it
/// instantly. A peer that advertises a zero window never does — and the write
/// was unbounded and untimed, so the rejected fd stayed open for as long as
/// the attacker cared to hold it. That defeats the whole point of the gate:
/// live fds climb past `maxclients` without limit, which is exactly what the
/// RLIMIT_NOFILE reconciliation is there to prevent. Two seconds is far more
/// than a healthy client needs and far less than an attacker wants.
pub(crate) const MAXCLIENTS_REJECT_WRITE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(2);

static MAXCLIENTS_WARN_LAST_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static MAXCLIENTS_WARN_SUPPRESSED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Rate-limit the "maxclients reached" warning to at most one per second,
/// returning the number of warnings suppressed since the last emit.
///
/// c10k C3. Being at `maxclients` is precisely the state in which
/// connections arrive fastest, and every rejection logged a line — so the
/// symptom (a full connection table) produced an unbounded log flood that
/// competes for the very I/O needed to diagnose it. Returning the suppressed
/// count keeps the signal: one line per second that says how many others it
/// stands for.
pub(crate) fn maxclients_warn_due(now_ms: u64) -> Option<u64> {
    let last = MAXCLIENTS_WARN_LAST_MS.load(std::sync::atomic::Ordering::Relaxed);
    if now_ms.saturating_sub(last) < 1000 {
        MAXCLIENTS_WARN_SUPPRESSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return None;
    }
    // Lost CAS = another thread is emitting this second's line; count as
    // suppressed rather than emitting twice.
    if MAXCLIENTS_WARN_LAST_MS
        .compare_exchange(
            last,
            now_ms,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        )
        .is_err()
    {
        MAXCLIENTS_WARN_SUPPRESSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return None;
    }
    Some(MAXCLIENTS_WARN_SUPPRESSED.swap(0, std::sync::atomic::Ordering::Relaxed))
}

#[cfg(test)]
mod maxclients_warn_tests {
    use super::*;

    #[test]
    fn warn_is_rate_limited_to_one_per_second_and_reports_the_gap() {
        // Serialized against the process-wide statics by running the whole
        // sequence in one test; `now_ms` is a parameter, so no sleeping.
        MAXCLIENTS_WARN_LAST_MS.store(0, std::sync::atomic::Ordering::Relaxed);
        MAXCLIENTS_WARN_SUPPRESSED.store(0, std::sync::atomic::Ordering::Relaxed);

        // First rejection of the storm emits, standing for nothing yet.
        assert_eq!(maxclients_warn_due(10_000), Some(0));
        // The next 999 ms are swallowed...
        for t in 10_001..10_999 {
            assert_eq!(maxclients_warn_due(t), None);
        }
        // ...and the next emit says how many it stands for.
        assert_eq!(maxclients_warn_due(11_000), Some(998));
        // The counter resets with each emit.
        assert_eq!(maxclients_warn_due(12_000), Some(0));
    }
}

#[cfg(test)]
mod write_timeout_gate_tests {
    use super::*;
    use std::time::Duration;

    const WT: Option<Duration> = Some(Duration::from_millis(60_000));

    #[test]
    fn small_writes_never_arm_the_timer() {
        // The p=1 hot path: a +OK, a bulk string, a small batch. None of these
        // can block on a healthy socket, so none may pay for a timer.
        for n in [0, 5, 64, 4096, WRITE_TIMEOUT_MIN_BYTES - 1] {
            assert_eq!(arm_write_timeout(n, WT), None, "{n} bytes must not arm");
        }
    }

    #[test]
    fn writes_that_can_block_do_arm() {
        for n in [
            WRITE_TIMEOUT_MIN_BYTES,
            WRITE_TIMEOUT_MIN_BYTES + 1,
            64 << 20,
        ] {
            assert_eq!(arm_write_timeout(n, WT), WT, "{n} bytes must arm");
        }
    }

    #[test]
    fn disabled_stays_disabled_regardless_of_size() {
        // `--client-write-timeout-ms 0` means wait forever, and the size gate
        // must not resurrect a timeout the operator turned off.
        assert_eq!(arm_write_timeout(1 << 30, None), None);
    }
}
