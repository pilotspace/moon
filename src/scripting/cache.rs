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
    fn test_sha1_deterministic() {
        let mut cache = ScriptCache::new();
        // Known SHA1 for "return 1": e0e1f9fabfc9d4800c877a703b823ac0578ff831
        let sha = cache.load(Bytes::from_static(b"return 1"));
        assert_eq!(sha, sha1_smol::Sha1::from(b"return 1").hexdigest());
    }
}
