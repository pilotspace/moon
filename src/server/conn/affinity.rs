//! Connection affinity tracking for lazy FD migration.
//!
//! `AffinityTracker` samples key-to-shard routing decisions per connection,
//! detecting when a connection consistently accesses keys on a remote shard.
//! When a dominant shard is identified (>= 60% of samples), the connection
//! can be migrated to that shard to eliminate cross-shard dispatch overhead.
//!
//! `MigratedConnectionState` carries all connection-level state needed to
//! reconstruct a handler on the target shard after FD migration.

use bytes::{Bytes, BytesMut};

use crate::workspace::WorkspaceId;

/// Minimum number of key samples before making a migration decision.
const SAMPLE_SIZE: u16 = 16;

/// Threshold: a shard must own >= 10/16 (62.5%) of sampled keys to trigger migration.
/// Uses integer comparison to avoid floating-point issues.
const AFFINITY_THRESHOLD_NUM: u16 = 10;

/// After migration, if the connection sends 64 consecutive commands to a remote
/// shard (not the shard it was migrated to), re-sampling is triggered.
const REMIGRATION_TRIGGER: u16 = 64;

/// Per-connection key-to-shard histogram for affinity detection.
///
/// Tracks which shard owns the keys accessed by this connection. After
/// `SAMPLE_SIZE` samples, decides whether to recommend migration.
pub(crate) struct AffinityTracker {
    /// Per-shard hit counts during the sampling window.
    shard_counts: Vec<u16>,
    /// Total samples collected in the current window.
    total: u16,
    /// The shard this connection currently lives on.
    my_shard: usize,
    /// Whether a migration decision has been made (sampling complete).
    decided: bool,
    /// After migration decision: consecutive commands targeting a remote shard.
    /// When this reaches `REMIGRATION_TRIGGER`, sampling resets.
    consecutive_remote: u16,
}

impl AffinityTracker {
    /// Create a new tracker for a connection on `my_shard` in a `num_shards` cluster.
    pub fn new(my_shard: usize, num_shards: usize) -> Self {
        Self {
            shard_counts: vec![0u16; num_shards],
            total: 0,
            my_shard,
            decided: false,
            consecutive_remote: 0,
        }
    }

    /// Record a key access targeting `target_shard`.
    ///
    /// Returns `Some(target_shard)` when migration should occur (dominant remote
    /// shard detected after sampling window completes). Returns `None` otherwise.
    #[inline]
    pub fn record(&mut self, target_shard: usize) -> Option<usize> {
        if self.decided {
            // Post-decision: track consecutive remote commands for re-migration.
            if target_shard != self.my_shard {
                self.consecutive_remote += 1;
                if self.consecutive_remote >= REMIGRATION_TRIGGER {
                    // Reset for re-sampling
                    self.decided = false;
                    self.shard_counts.fill(0);
                    self.total = 0;
                    self.consecutive_remote = 0;
                }
            } else {
                self.consecutive_remote = 0;
            }
            return None;
        }

        // Sampling phase: accumulate histogram.
        self.shard_counts[target_shard] += 1;
        self.total += 1;

        if self.total >= SAMPLE_SIZE {
            self.decided = true;

            // Find the shard with the most hits.
            let mut max_shard = 0;
            let mut max_count = 0u16;
            for (i, &count) in self.shard_counts.iter().enumerate() {
                if count > max_count {
                    max_count = count;
                    max_shard = i;
                }
            }

            // Only migrate if dominant shard is remote and exceeds threshold.
            if max_shard != self.my_shard && max_count >= AFFINITY_THRESHOLD_NUM {
                return Some(max_shard);
            }
        }

        None
    }

    /// Returns `true` if sampling is still in progress (caller should extract
    /// the primary key for shard routing). Returns `false` after a decision
    /// has been made and no re-migration is pending.
    #[inline]
    #[allow(dead_code)] // Called from handler hot path once sampling integration lands
    pub fn should_sample(&self) -> bool {
        !self.decided
    }
}

/// Serializable connection state for cross-shard FD migration.
///
/// Carries everything needed to reconstruct a connection handler on the
/// target shard. The raw FD is transferred separately via `ShardMessage`.
#[allow(dead_code)]
pub struct MigratedConnectionState {
    /// Currently selected database index (from SELECT command).
    pub selected_db: usize,
    /// Whether the client has authenticated (AUTH command or no requirepass).
    pub authenticated: bool,
    /// Client name set via CLIENT SETNAME.
    pub client_name: Option<Bytes>,
    /// RESP protocol version (2 or 3, set via HELLO).
    pub protocol_version: u8,
    /// ACL username for the connection.
    pub current_user: String,
    /// Reserved flags for future use (e.g., READONLY, SUBSCRIBE mode).
    pub flags: u32,
    /// Unparsed bytes remaining in the read buffer at migration time.
    pub read_buf_remainder: BytesMut,
    /// Unique client ID assigned at connection time.
    pub client_id: u64,
    /// Peer address string for logging/CLIENT LIST.
    pub peer_addr: String,
    /// Workspace binding at migration time. Preserved so workspace-bound
    /// connections continue to see workspace-scoped keys after shard migration.
    pub workspace_id: Option<WorkspaceId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_tracker_is_zeroed() {
        let t = AffinityTracker::new(0, 8);
        assert_eq!(t.total, 0);
        assert!(!t.decided);
        assert!(t.should_sample());
        assert_eq!(t.consecutive_remote, 0);
        for &c in &t.shard_counts {
            assert_eq!(c, 0);
        }
    }

    #[test]
    fn record_returns_none_before_sample_size() {
        let mut t = AffinityTracker::new(0, 8);
        // First 15 samples should all return None
        for _ in 0..15 {
            assert_eq!(t.record(1), None);
        }
        assert!(t.should_sample());
    }

    #[test]
    fn record_returns_dominant_shard_at_sample_size() {
        let mut t = AffinityTracker::new(0, 8);
        // 12 hits on shard 3, 4 hits on shard 0 => shard 3 has 75% > 60%
        for _ in 0..12 {
            let _ = t.record(3);
        }
        for _ in 0..3 {
            let _ = t.record(0);
        }
        // 16th sample triggers decision
        let result = t.record(0);
        assert_eq!(result, Some(3));
        assert!(!t.should_sample());
    }

    #[test]
    fn record_returns_none_when_threshold_not_met() {
        let mut t = AffinityTracker::new(0, 8);
        // Spread evenly: 4 each across shards 1,2,3,4 => none >= 10/16
        for shard in 1..=4 {
            for _ in 0..4 {
                let _ = t.record(shard);
            }
        }
        // All 16 samples consumed, last call should have triggered decision
        assert!(!t.should_sample());
        // The 16th record() was the last one in the loop — it returned None
        // because no shard had >= 10. Verify by attempting another record.
        // After decided=true, record always returns None (post-decision tracking).
        assert_eq!(t.record(1), None);
    }

    #[test]
    fn record_returns_none_when_dominant_is_local() {
        let mut t = AffinityTracker::new(2, 8);
        // 14 hits on shard 2 (local), 2 on shard 5 => dominant is local
        for _ in 0..14 {
            let _ = t.record(2);
        }
        for _ in 0..1 {
            let _ = t.record(5);
        }
        let result = t.record(5);
        assert_eq!(result, None);
        assert!(!t.should_sample());
    }

    #[test]
    fn post_decision_tracks_consecutive_remote() {
        let mut t = AffinityTracker::new(0, 8);
        // Force a decision (dominant shard 1)
        for _ in 0..15 {
            let _ = t.record(1);
        }
        let result = t.record(1);
        assert_eq!(result, Some(1));

        // After decision, consecutive remote commands tracked
        for _ in 0..63 {
            assert_eq!(t.record(2), None);
        }
        // 64th consecutive remote triggers re-sampling reset
        assert_eq!(t.record(2), None);
        // Now sampling is active again
        assert!(t.should_sample());
    }

    #[test]
    fn post_decision_resets_consecutive_on_local() {
        let mut t = AffinityTracker::new(0, 8);
        // Force a decision
        for _ in 0..15 {
            let _ = t.record(1);
        }
        assert_eq!(t.record(1), Some(1));

        // Accumulate 60 remote, then one local resets the counter
        for _ in 0..60 {
            let _ = t.record(2);
        }
        assert_eq!(t.record(0), None); // local command resets counter
        // Now 64 more remote needed (not 4)
        for _ in 0..63 {
            assert_eq!(t.record(2), None);
        }
        assert!(!t.should_sample()); // still decided, need one more
        assert_eq!(t.record(2), None); // 64th => re-sample
        assert!(t.should_sample());
    }

    #[test]
    fn exact_threshold_boundary() {
        let mut t = AffinityTracker::new(0, 8);
        // Exactly 10 on shard 5, 6 on shard 0 => 10/16 = 62.5% >= threshold
        for _ in 0..10 {
            let _ = t.record(5);
        }
        for _ in 0..5 {
            let _ = t.record(0);
        }
        let result = t.record(0);
        assert_eq!(result, Some(5));
    }

    #[test]
    fn just_below_threshold() {
        let mut t = AffinityTracker::new(0, 8);
        // 9 on shard 5, 7 on shard 0 => 9/16 = 56.25% < threshold
        for _ in 0..9 {
            let _ = t.record(5);
        }
        for _ in 0..6 {
            let _ = t.record(0);
        }
        let result = t.record(0);
        assert_eq!(result, None);
        assert!(!t.should_sample());
    }

    #[test]
    fn migrated_connection_state_fields() {
        let state = MigratedConnectionState {
            selected_db: 3,
            authenticated: true,
            client_name: Some(Bytes::from_static(b"my-app")),
            protocol_version: 3,
            current_user: "admin".to_string(),
            flags: 0,
            read_buf_remainder: BytesMut::from(&b"partial"[..]),
            client_id: 42,
            peer_addr: "127.0.0.1:54321".to_string(),
            workspace_id: None,
        };
        assert_eq!(state.selected_db, 3);
        assert!(state.authenticated);
        assert_eq!(state.client_name.as_deref(), Some(&b"my-app"[..]));
        assert_eq!(state.protocol_version, 3);
        assert_eq!(state.current_user, "admin");
        assert_eq!(state.flags, 0);
        assert_eq!(&state.read_buf_remainder[..], b"partial");
        assert_eq!(state.client_id, 42);
        assert_eq!(state.peer_addr, "127.0.0.1:54321");
    }
}
