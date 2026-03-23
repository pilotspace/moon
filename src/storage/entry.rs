use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Instant;

/// The type of value stored in a Redis key.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    SortedSet {
        members: HashMap<Bytes, f64>,
        scores: BTreeMap<(OrderedFloat<f64>, Bytes), ()>,
    },
}

impl RedisValue {
    /// Return the Redis type name string for this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::Hash(_) => "hash",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::SortedSet { .. } => "zset",
        }
    }
}

/// A single entry in the database, wrapping a value with expiration metadata.
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: RedisValue,
    pub expires_at: Option<Instant>,
    pub created_at: Instant,
}

impl Entry {
    /// Create a new string entry with no expiration.
    pub fn new_string(value: Bytes) -> Entry {
        Entry {
            value: RedisValue::String(value),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    /// Create a new string entry with an expiration time.
    pub fn new_string_with_expiry(value: Bytes, expires_at: Instant) -> Entry {
        Entry {
            value: RedisValue::String(value),
            expires_at: Some(expires_at),
            created_at: Instant::now(),
        }
    }

    /// Create a new hash entry with an empty HashMap.
    pub fn new_hash() -> Entry {
        Entry {
            value: RedisValue::Hash(HashMap::new()),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    /// Create a new list entry with an empty VecDeque.
    pub fn new_list() -> Entry {
        Entry {
            value: RedisValue::List(VecDeque::new()),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    /// Create a new set entry with an empty HashSet.
    pub fn new_set() -> Entry {
        Entry {
            value: RedisValue::Set(HashSet::new()),
            expires_at: None,
            created_at: Instant::now(),
        }
    }

    /// Create a new sorted set entry with empty members and scores.
    pub fn new_sorted_set() -> Entry {
        Entry {
            value: RedisValue::SortedSet {
                members: HashMap::new(),
                scores: BTreeMap::new(),
            },
            expires_at: None,
            created_at: Instant::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_string_no_expiry() {
        let entry = Entry::new_string(Bytes::from_static(b"hello"));
        assert!(entry.expires_at.is_none());
        matches!(entry.value, RedisValue::String(_));
    }

    #[test]
    fn test_new_string_with_expiry() {
        let exp = Instant::now() + std::time::Duration::from_secs(60);
        let entry = Entry::new_string_with_expiry(Bytes::from_static(b"hello"), exp);
        assert_eq!(entry.expires_at, Some(exp));
    }

    #[test]
    fn test_new_hash() {
        let entry = Entry::new_hash();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::Hash(ref m) if m.is_empty()));
        assert_eq!(entry.value.type_name(), "hash");
    }

    #[test]
    fn test_new_list() {
        let entry = Entry::new_list();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::List(ref l) if l.is_empty()));
        assert_eq!(entry.value.type_name(), "list");
    }

    #[test]
    fn test_new_set() {
        let entry = Entry::new_set();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::Set(ref s) if s.is_empty()));
        assert_eq!(entry.value.type_name(), "set");
    }

    #[test]
    fn test_new_sorted_set() {
        let entry = Entry::new_sorted_set();
        assert!(entry.expires_at.is_none());
        assert!(matches!(entry.value, RedisValue::SortedSet { ref members, ref scores } if members.is_empty() && scores.is_empty()));
        assert_eq!(entry.value.type_name(), "zset");
    }

    #[test]
    fn test_type_name() {
        assert_eq!(RedisValue::String(Bytes::from_static(b"")).type_name(), "string");
        assert_eq!(RedisValue::Hash(HashMap::new()).type_name(), "hash");
        assert_eq!(RedisValue::List(VecDeque::new()).type_name(), "list");
        assert_eq!(RedisValue::Set(HashSet::new()).type_name(), "set");
        assert_eq!(RedisValue::SortedSet { members: HashMap::new(), scores: BTreeMap::new() }.type_name(), "zset");
    }
}
