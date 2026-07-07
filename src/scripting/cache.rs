use bytes::Bytes;
use std::collections::HashMap;

/// Per-shard script cache: maps hex SHA1 -> script source bytes.
pub struct ScriptCache {
    scripts: HashMap<String, Bytes>,
}

impl ScriptCache {
    pub fn new() -> Self {
        ScriptCache {
            scripts: HashMap::new(),
        }
    }

    /// Cache a script and return its hex SHA1 digest.
    pub fn load(&mut self, script: Bytes) -> String {
        let sha = sha1_smol::Sha1::from(&script[..]).hexdigest();
        self.scripts.entry(sha.clone()).or_insert(script);
        sha
    }

    pub fn get(&self, sha1_hex: &str) -> Option<&Bytes> {
        self.scripts.get(sha1_hex)
    }

    pub fn exists(&self, sha1_hex: &str) -> bool {
        self.scripts.contains_key(sha1_hex)
    }

    pub fn flush(&mut self) {
        self.scripts.clear();
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }

    /// Approximate resident bytes held by cached script bodies (C4 wave-5
    /// hygiene): the sum of each entry's hex-SHA1 key length plus its
    /// source byte length. This is an estimate (it excludes `HashMap`/
    /// `String`/`Bytes` allocator bookkeeping overhead) intended for
    /// observability only -- the cache itself remains unbounded, matching
    /// Redis semantics (`SCRIPT FLUSH` is the only eviction path).
    pub fn resident_bytes(&self) -> usize {
        self.scripts.iter().map(|(k, v)| k.len() + v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_and_get() {
        let mut cache = ScriptCache::new();
        let script = Bytes::from_static(b"return 1");
        let sha = cache.load(script.clone());
        assert_eq!(sha.len(), 40); // hex SHA1 is 40 chars
        assert_eq!(cache.get(&sha), Some(&script));
        assert!(cache.exists(&sha));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_duplicate_load() {
        let mut cache = ScriptCache::new();
        let script = Bytes::from_static(b"return 1");
        let sha1 = cache.load(script.clone());
        let sha2 = cache.load(script);
        assert_eq!(sha1, sha2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_flush() {
        let mut cache = ScriptCache::new();
        cache.load(Bytes::from_static(b"return 1"));
        cache.load(Bytes::from_static(b"return 2"));
        assert_eq!(cache.len(), 2);
        cache.flush();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_resident_bytes_empty_cache_is_zero() {
        let cache = ScriptCache::new();
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn test_resident_bytes_grows_with_entries_and_shrinks_on_flush() {
        let mut cache = ScriptCache::new();
        let sha1 = cache.load(Bytes::from_static(b"return 1"));
        let after_one = cache.resident_bytes();
        // 40-byte hex key + 8-byte body.
        assert_eq!(after_one, sha1.len() + 8);

        let sha2 = cache.load(Bytes::from_static(b"return 'a much longer script body'"));
        let after_two = cache.resident_bytes();
        assert!(after_two > after_one);
        assert_eq!(
            after_two,
            sha1.len() + 8 + sha2.len() + "return 'a much longer script body'".len()
        );

        cache.flush();
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn test_sha1_deterministic() {
        let mut cache = ScriptCache::new();
        // Known SHA1 for "return 1": e0e1f9fabfc9d4800c877a703b823ac0578ff831
        let sha = cache.load(Bytes::from_static(b"return 1"));
        assert_eq!(sha, sha1_smol::Sha1::from(b"return 1").hexdigest());
    }
}
