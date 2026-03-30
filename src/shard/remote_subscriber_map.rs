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
}

impl RemoteSubscriberMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `shard_id` has a subscriber for `channel` (exact or pattern).
    pub fn add(&mut self, channel: Bytes, shard_id: usize, is_pattern: bool) {
        let map = if is_pattern {
            &mut self.patterns
        } else {
            &mut self.channels
        };
        map.entry(channel).or_default().insert(shard_id);
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
    pub fn target_shards(&self, channel: &[u8]) -> Vec<usize> {
        let mut result: HashSet<usize> = HashSet::new();
        if let Some(shards) = self.channels.get(channel) {
            result.extend(shards.iter());
        }
        for (pattern, shards) in &self.patterns {
            if crate::command::key::glob_match(pattern, channel) {
                result.extend(shards.iter());
            }
        }
        result.into_iter().collect()
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
