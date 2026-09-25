//! Tracks which remote shards have subscribers for each channel/pattern.
//! Used by PUBLISH to skip fan-out to shards with no subscribers (targeted fan-out).

use std::collections::{HashMap, HashSet};

use bytes::Bytes;

/// Per-shard map of remote subscriber presence.
///
/// When shard X receives a PubSubSubscribe message from shard Y for channel "foo",
/// it records that shard Y has subscribers for "foo". PUBLISH then only fans out
/// to shards present in this map (plus checking patterns).
#[derive(Default)]
pub struct RemoteSubscriberMap {
    /// channel_name -> set of shard IDs that have at least one subscriber
    channels: HashMap<Bytes, HashSet<usize>>,
    /// pattern -> set of shard IDs that have at least one pattern subscriber
    patterns: HashMap<Bytes, HashSet<usize>>,
    /// sharded channel -> set of shard IDs with at least one `SSUBSCRIBE`
    /// subscriber. Separate from `channels`: the two namespaces may share a
    /// channel NAME while being different destinations.
    shard_channels: HashMap<Bytes, HashSet<usize>>,
}

impl RemoteSubscriberMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `shard_id` has a subscriber for `channel` (exact or
    /// pattern). A new name is stored as an exact-size copy (moon#1160, see
    /// [`insert_name`]).
    pub fn add(&mut self, channel: Bytes, shard_id: usize, is_pattern: bool) {
        let map = if is_pattern {
            &mut self.patterns
        } else {
            &mut self.channels
        };
        insert_name(map, &channel, shard_id);
    }

    /// Remove `shard_id` from subscribers for `channel`. Cleans up empty entries.
    pub fn remove(&mut self, channel: &[u8], shard_id: usize, is_pattern: bool) {
        let map = if is_pattern {
            &mut self.patterns
        } else {
            &mut self.channels
        };
        if let Some(shards) = map.get_mut(channel) {
            shards.remove(&shard_id);
            if shards.is_empty() {
                map.remove(channel);
            }
        }
    }

    /// Return shard IDs that have exact-channel subscribers for this channel.
    pub fn shards_for_channel(&self, channel: &[u8]) -> Vec<usize> {
        self.channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Return shard IDs that have any pattern subscription matching this channel.
    /// Uses glob_match to check each registered pattern against the channel.
    pub fn shards_for_patterns(&self, channel: &[u8]) -> Vec<usize> {
        let mut result = HashSet::new();
        for (pattern, shards) in &self.patterns {
            if crate::command::key::glob_match(pattern, channel) {
                result.extend(shards.iter());
            }
        }
        result.into_iter().collect()
    }

    /// Return deduplicated set of all shard IDs that should receive a PUBLISH for `channel`.
    ///
    /// Uses a SmallVec + manual dedup to avoid HashSet heap allocation on the hot path.
    pub fn target_shards(&self, channel: &[u8]) -> Vec<usize> {
        let mut result: smallvec::SmallVec<[usize; 8]> = smallvec::SmallVec::new();
        if let Some(shards) = self.channels.get(channel) {
            result.extend(shards.iter().copied());
        }
        if !self.patterns.is_empty() {
            for (pattern, shards) in &self.patterns {
                if crate::command::key::glob_match(pattern, channel) {
                    for &s in shards {
                        if !result.contains(&s) {
                            result.push(s);
                        }
                    }
                }
            }
        }
        result.into_vec()
    }

    // ── Sharded pub/sub ─────────────────────────────────────────────────
    //
    // A third map, kept apart from `channels` for the same reason the
    // registry keeps its sharded subscribers apart: a sharded channel and a
    // plain channel may share a NAME while being different destinations. One
    // map keyed by name would fan a plain PUBLISH out to shards that only hold
    // sharded subscribers, and vice versa.

    /// Record that `shard_id` has a SHARDED subscriber for `channel` (a new
    /// name is stored as an exact-size copy, as in [`Self::add`]).
    pub fn add_shard_channel(&mut self, channel: Bytes, shard_id: usize) {
        insert_name(&mut self.shard_channels, &channel, shard_id);
    }

    /// Remove `shard_id` from sharded subscribers for `channel`.
    pub fn remove_shard_channel(&mut self, channel: &[u8], shard_id: usize) {
        if let Some(shards) = self.shard_channels.get_mut(channel) {
            shards.remove(&shard_id);
            if shards.is_empty() {
                self.shard_channels.remove(channel);
            }
        }
    }

    /// Shard IDs that should receive an `SPUBLISH` for `channel`.
    ///
    /// No pattern leg: `PSUBSCRIBE` does not match sharded channels in Redis.
    pub fn shard_target_shards(&self, channel: &[u8]) -> Vec<usize> {
        self.shard_channels
            .get(channel)
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Every stored channel, pattern and sharded-channel name (tests).
    #[cfg(test)]
    pub(crate) fn for_each_stored_name(&self, mut f: impl FnMut(&Bytes)) {
        self.channels
            .keys()
            .chain(self.patterns.keys())
            .chain(self.shard_channels.keys())
            .for_each(|k| f(k));
    }
}

/// Add `shard_id` under `name`, storing a NEW name as an exact-size copy
/// (moon#1160): the caller's `Bytes` is a slice of the subscribing
/// connection's request buffer, and this map outlives the request.
fn insert_name(map: &mut HashMap<Bytes, HashSet<usize>>, name: &Bytes, shard_id: usize) {
    match map.get_mut(name) {
        Some(shards) => {
            shards.insert(shard_id);
        }
        None => {
            map.insert(
                crate::storage::owned_bytes::detach(name),
                HashSet::from([shard_id]),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_and_lookup() {
        let mut map = RemoteSubscriberMap::new();
        map.add(Bytes::from_static(b"news"), 1, false);
        map.add(Bytes::from_static(b"news"), 2, false);
        let shards = map.shards_for_channel(b"news");
        assert!(shards.contains(&1));
        assert!(shards.contains(&2));
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn test_remove() {
        let mut map = RemoteSubscriberMap::new();
        map.add(Bytes::from_static(b"news"), 1, false);
        map.add(Bytes::from_static(b"news"), 2, false);
        map.remove(b"news", 1, false);
        let shards = map.shards_for_channel(b"news");
        assert_eq!(shards, vec![2]);
    }

    #[test]
    fn test_remove_cleans_empty() {
        let mut map = RemoteSubscriberMap::new();
        map.add(Bytes::from_static(b"news"), 1, false);
        map.remove(b"news", 1, false);
        assert!(map.shards_for_channel(b"news").is_empty());
        // Internal map should be clean (no empty HashSet entries)
        assert!(!map.channels.contains_key(&Bytes::from_static(b"news")[..]));
    }

    #[test]
    fn test_pattern_matching() {
        let mut map = RemoteSubscriberMap::new();
        map.add(Bytes::from_static(b"news.*"), 1, true);
        let shards = map.shards_for_patterns(b"news.sports");
        assert_eq!(shards, vec![1]);
        let shards = map.shards_for_patterns(b"weather");
        assert!(shards.is_empty());
    }

    #[test]
    fn test_target_shards_dedup() {
        let mut map = RemoteSubscriberMap::new();
        map.add(Bytes::from_static(b"news.sports"), 1, false);
        map.add(Bytes::from_static(b"news.*"), 1, true);
        let shards = map.target_shards(b"news.sports");
        assert_eq!(shards, vec![1]); // deduplicated
    }

    #[test]
    fn test_target_shards_combined() {
        let mut map = RemoteSubscriberMap::new();
        map.add(Bytes::from_static(b"news.sports"), 1, false);
        map.add(Bytes::from_static(b"news.*"), 2, true);
        let mut shards = map.target_shards(b"news.sports");
        shards.sort();
        assert_eq!(shards, vec![1, 2]);
    }
}
