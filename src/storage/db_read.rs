use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use super::bptree::BPTree;
use super::intset::Intset;
use super::listpack::Listpack;

// ---------------------------------------------------------------------------
// Read-only Ref enums for immutable access to compact and full encodings
// ---------------------------------------------------------------------------

/// Read-only reference to a hash (full HashMap or compact Listpack).
pub enum HashRef<'a> {
    Map(&'a HashMap<Bytes, Bytes>),
    Listpack(&'a Listpack),
}

impl<'a> HashRef<'a> {
    /// Look up a single field. Linear scan for listpack, O(1) for HashMap.
    pub fn get_field(&self, field: &[u8]) -> Option<Bytes> {
        match self {
            HashRef::Map(map) => map.get(field).cloned(),
            HashRef::Listpack(lp) => {
                for (f, v) in lp.iter_pairs() {
                    if f.as_bytes() == field {
                        return Some(v.to_bytes());
                    }
                }
                None
            }
        }
    }

    /// Number of fields in the hash.
    pub fn len(&self) -> usize {
        match self {
            HashRef::Map(map) => map.len(),
            HashRef::Listpack(lp) => lp.len() / 2,
        }
    }

    /// Return all (field, value) pairs.
    pub fn entries(&self) -> Vec<(Bytes, Bytes)> {
        match self {
            HashRef::Map(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            HashRef::Listpack(lp) => lp
                .iter_pairs()
                .map(|(f, v)| (f.to_bytes(), v.to_bytes()))
                .collect(),
        }
    }
}

/// Read-only reference to a list (full VecDeque or compact Listpack).
pub enum ListRef<'a> {
    Deque(&'a VecDeque<Bytes>),
    Listpack(&'a Listpack),
}

impl<'a> ListRef<'a> {
    /// Number of elements.
    pub fn len(&self) -> usize {
        match self {
            ListRef::Deque(d) => d.len(),
            ListRef::Listpack(lp) => lp.len(),
        }
    }

    /// Get element at index.
    pub fn get(&self, index: usize) -> Option<Bytes> {
        match self {
            ListRef::Deque(d) => d.get(index).cloned(),
            ListRef::Listpack(lp) => lp.get_at(index).map(|e| e.to_bytes()),
        }
    }

    /// Get a range of elements [start..=end]. Caller must clamp bounds.
    pub fn range(&self, start: usize, end: usize) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => (start..=end).filter_map(|i| d.get(i).cloned()).collect(),
            ListRef::Listpack(lp) => (start..=end)
                .filter_map(|i| lp.get_at(i).map(|e| e.to_bytes()))
                .collect(),
        }
    }

    /// Iterate all elements (for LPOS).
    pub fn iter_bytes(&self) -> Vec<Bytes> {
        match self {
            ListRef::Deque(d) => d.iter().cloned().collect(),
            ListRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
        }
    }
}

/// Read-only reference to a set (full HashSet, compact Listpack, or Intset).
pub enum SetRef<'a> {
    Hash(&'a HashSet<Bytes>),
    Listpack(&'a Listpack),
    Intset(&'a Intset),
}

impl<'a> SetRef<'a> {
    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            SetRef::Hash(s) => s.len(),
            SetRef::Listpack(lp) => lp.len(),
            SetRef::Intset(is) => is.len(),
        }
    }

    /// Check if member exists.
    pub fn contains(&self, member: &[u8]) -> bool {
        match self {
            SetRef::Hash(s) => s.contains(member),
            SetRef::Listpack(lp) => lp.find(member).is_some(),
            SetRef::Intset(is) => {
                if let Ok(s) = std::str::from_utf8(member) {
                    if let Ok(v) = s.parse::<i64>() {
                        return is.contains(v);
                    }
                }
                false
            }
        }
    }

    /// Return all members as Bytes.
    pub fn members(&self) -> Vec<Bytes> {
        match self {
            SetRef::Hash(s) => s.iter().cloned().collect(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
        }
    }

    /// Convert to an owned HashSet for set-algebra operations.
    pub fn to_hash_set(&self) -> HashSet<Bytes> {
        match self {
            SetRef::Hash(s) => (*s).clone(),
            SetRef::Listpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            SetRef::Intset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
        }
    }
}

/// Read-only reference to a sorted set.
pub enum SortedSetRef<'a> {
    BPTree {
        tree: &'a BPTree,
        members: &'a HashMap<Bytes, f64>,
    },
    Listpack(&'a Listpack),
    #[allow(dead_code)]
    Legacy {
        members: &'a HashMap<Bytes, f64>,
        scores: &'a BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl<'a> SortedSetRef<'a> {
    /// Number of members.
    pub fn len(&self) -> usize {
        match self {
            SortedSetRef::BPTree { members, .. } => members.len(),
            SortedSetRef::Listpack(lp) => lp.len() / 2,
            SortedSetRef::Legacy { members, .. } => members.len(),
        }
    }

    /// Get score for a member.
    pub fn score(&self, member: &[u8]) -> Option<f64> {
        match self {
            SortedSetRef::BPTree { members, .. } => members.get(member).copied(),
            SortedSetRef::Listpack(lp) => {
                for (m, s) in lp.iter_score_member_pairs() {
                    if m.as_bytes() == member {
                        return match s {
                            super::listpack::ListpackEntry::Integer(i) => Some(i as f64),
                            super::listpack::ListpackEntry::String(ref b) => {
                                std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                            }
                        };
                    }
                }
                None
            }
            SortedSetRef::Legacy { members, .. } => members.get(member).copied(),
        }
    }

    /// Get all (member, score) pairs sorted by score then member.
    pub fn entries_sorted(&self) -> Vec<(Bytes, f64)> {
        match self {
            SortedSetRef::BPTree { tree, .. } => tree
                .iter()
                .map(|(score, member)| (member.clone(), score.0))
                .collect(),
            SortedSetRef::Listpack(lp) => {
                let mut pairs: Vec<(Bytes, f64)> = Vec::new();
                for (m, s) in lp.iter_score_member_pairs() {
                    let score = match s {
                        super::listpack::ListpackEntry::Integer(i) => i as f64,
                        super::listpack::ListpackEntry::String(ref b) => std::str::from_utf8(b)
                            .ok()
                            .and_then(|ss| ss.parse().ok())
                            .unwrap_or(0.0),
                    };
                    pairs.push((m.to_bytes(), score));
                }
                pairs.sort_by(|a, b| {
                    OrderedFloat(a.1)
                        .cmp(&OrderedFloat(b.1))
                        .then_with(|| a.0.cmp(&b.0))
                });
                pairs
            }
            SortedSetRef::Legacy { scores, .. } => {
                scores.keys().map(|(s, m)| (m.clone(), s.0)).collect()
            }
        }
    }

    /// Get members HashMap reference (for BPTree and Legacy variants).
    /// For listpack, returns None (callers should use entries_sorted or score).
    pub fn members_map(&self) -> Option<&'a HashMap<Bytes, f64>> {
        match self {
            SortedSetRef::BPTree { members, .. } => Some(members),
            SortedSetRef::Legacy { members, .. } => Some(members),
            SortedSetRef::Listpack(_) => None,
        }
    }

    /// Get BPTree reference (for BPTree variant only).
    pub fn bptree(&self) -> Option<&'a BPTree> {
        match self {
            SortedSetRef::BPTree { tree, .. } => Some(tree),
            _ => None,
        }
    }
}
