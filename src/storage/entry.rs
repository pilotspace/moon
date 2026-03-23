use bytes::Bytes;
use std::time::Instant;

/// The type of value stored in a Redis key.
///
/// Starts with only String; other types (List, Hash, Set, etc.) are added in later phases.
#[derive(Debug, Clone)]
pub enum RedisValue {
    String(Bytes),
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
}
