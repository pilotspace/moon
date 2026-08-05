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

/// Apply RESP3 response type conversion based on command name and protocol version.
/// Uppercases the command name into a stack buffer for O(1) lookup.
#[inline]
pub(crate) fn apply_resp3_conversion(cmd: &[u8], response: Frame, proto: u8) -> Frame {
    if proto < 3 {
        return response;
    }
    let mut cmd_upper_buf = [0u8; 32];
    let cmd_upper_len = cmd.len().min(32);
    cmd_upper_buf[..cmd_upper_len].copy_from_slice(&cmd[..cmd_upper_len]);
    cmd_upper_buf[..cmd_upper_len].make_ascii_uppercase();
    crate::protocol::resp3::maybe_convert_resp3(&cmd_upper_buf[..cmd_upper_len], response, proto)
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
