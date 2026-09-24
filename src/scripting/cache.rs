use bytes::Bytes;
use mlua::Function;
use std::collections::HashMap;

/// Cap on the per-shard COMPILED-function map (moon#1167).
///
/// The source map (`scripts`) is unbounded — that matches Redis, where the
/// only source eviction is `SCRIPT FLUSH` and `SCRIPT EXISTS` must keep
/// reporting a loaded sha. Compiled protos, however, live in the Lua heap, so
/// an application that generates unique EVAL bodies would grow the interpreter
/// heap without limit. Bounding only the compiled map keeps `NOSCRIPT`
/// semantics exact (driven by the source map) while capping the heap cost: an
/// EVALSHA whose compiled entry was evicted simply recompiles from the retained
/// source on that one call.
const COMPILED_CACHE_CAP: usize = 1024;

/// The compiled-cache cap, for the mod-level LRU bound test (moon#1167).
#[cfg(test)]
pub(crate) fn compiled_cache_cap_for_test() -> usize {
    COMPILED_CACHE_CAP
}

/// Per-shard bounded LRU of compiled Lua chunks (moon#1167).
///
/// Keyed by the 40-byte lowercase hex sha so EVALSHA can look up without
/// allocating a `String`. Every `Function` here belongs to the shard's ONE Lua
/// VM (the cache and the VM are paired 1:1 for the shard's lifetime), so a
/// cached function is always called on the VM that compiled it. Eviction is an
/// O(n) min-scan, paid only when a NEW distinct body is inserted past the cap —
/// the cold unique-body-storm path, never a cache hit.
#[derive(Default)]
struct CompiledFnCache {
    map: HashMap<[u8; 40], CompiledEntry>,
    /// Monotonic access clock; the entry with the smallest `last_used` is the
    /// least-recently-used one.
    tick: u64,
}

struct CompiledEntry {
    func: Function,
    last_used: u64,
}

impl CompiledFnCache {
    /// Return the cached compiled function for `key`, marking it most-recently
    /// used. `Function` clone is a cheap ref-count bump.
    fn get(&mut self, key: &[u8; 40]) -> Option<Function> {
        self.tick = self.tick.wrapping_add(1);
        let tick = self.tick;
        let entry = self.map.get_mut(key)?;
        entry.last_used = tick;
        Some(entry.func.clone())
    }

    /// Insert a freshly compiled function, evicting the least-recently-used
    /// entry first when at capacity. `key` is assumed absent (callers insert
    /// only after a [`Self::get`] miss).
    fn insert(&mut self, key: [u8; 40], func: Function) {
        if self.map.len() >= COMPILED_CACHE_CAP {
            if let Some(victim) = self
                .map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| *k)
            {
                self.map.remove(&victim);
            }
        }
        self.tick = self.tick.wrapping_add(1);
        let last_used = self.tick;
        self.map.insert(key, CompiledEntry { func, last_used });
    }

    fn clear(&mut self) {
        self.map.clear();
        self.tick = 0;
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.map.len()
    }
}

/// Per-shard script cache: maps hex SHA1 -> script source bytes, plus a bounded
/// LRU of the compiled Lua functions (moon#1167).
pub struct ScriptCache {
    scripts: HashMap<String, Bytes>,
    /// Digests this shard has successfully published to every other shard
    /// (moon#515). Tracked SEPARATELY from `scripts` because the two answer
    /// different questions: `scripts` says "can I run this sha", `fanned_out`
    /// says "does the rest of the server know about it".
    ///
    /// Folding the two together — gating the fan-out on the cache insert —
    /// looks tempting and is wrong: a fan-out that fails leaves the body
    /// cached locally, so every later `EVAL` of that body sees a hit and
    /// SKIPS the retry. The divergence would then be permanent for a
    /// self-inflicted reason. Keeping the flag separate makes the next `EVAL`
    /// of the same body republish, which is the whole recovery story now that
    /// there is no repair leg.
    fanned_out: std::collections::HashSet<String>,
    /// Compiled-function LRU (moon#1167). Starts empty and fills lazily on the
    /// eval path, which is the only caller that has the shard's Lua VM — so
    /// `ScriptCache::new()` still needs no VM (it is constructed on the SPSC
    /// drain path and in tests before a VM exists). Every function it holds
    /// belongs to that one shard VM; the two are paired 1:1 for the shard's
    /// lifetime.
    compiled: CompiledFnCache,
    /// Running sum of `key.len() + body.len()` over `scripts` (moon#1167).
    ///
    /// Replaces the per-100ms O(n) walk `resident_bytes` used to do. Source
    /// bytes only — compiled protos live in the Lua heap, sampled separately
    /// via `vm_used_memory` (moon#506).
    source_bytes: usize,
}

impl ScriptCache {
    pub fn new() -> Self {
        ScriptCache {
            scripts: HashMap::new(),
            fanned_out: std::collections::HashSet::new(),
            compiled: CompiledFnCache::default(),
            source_bytes: 0,
        }
    }

    /// Cache a script and return its hex SHA1 digest.
    pub fn load(&mut self, script: Bytes) -> String {
        let sha = sha1_smol::Sha1::from(&script[..]).hexdigest();
        self.store_source(sha.clone(), script);
        sha
    }

    /// Cache a script whose digest is already known (moon#1167) — lets the EVAL
    /// path compute the sha exactly once and reuse it for the source store and
    /// the compiled-cache lookup.
    pub fn load_precomputed(&mut self, sha: String, script: Bytes) {
        self.store_source(sha, script);
    }

    /// Insert a source body under `sha`, charging `source_bytes` only on a real
    /// insert (idempotent — a duplicate is a no-op, matching Redis).
    fn store_source(&mut self, sha: String, script: Bytes) {
        if let std::collections::hash_map::Entry::Vacant(e) = self.scripts.entry(sha) {
            self.source_bytes += e.key().len() + script.len();
            e.insert(script);
        }
    }

    /// Fetch a compiled function by its 40-byte lowercase-hex sha key.
    pub fn get_compiled(&mut self, key: &[u8; 40]) -> Option<Function> {
        self.compiled.get(key)
    }

    /// Store a freshly compiled function under its sha key (bounded LRU).
    pub fn store_compiled(&mut self, key: [u8; 40], func: Function) {
        self.compiled.insert(key, func);
    }

    /// Number of compiled functions currently cached (observability / tests).
    #[cfg(test)]
    pub fn compiled_len(&self) -> usize {
        self.compiled.len()
    }

    /// Cache a script and report whether this shard still OWES the other
    /// shards a copy of it (moon#515).
    ///
    /// `EVAL` must publish its body to the other shards, or a later `EVALSHA`
    /// on a connection that landed elsewhere answers `NOSCRIPT` for a sha the
    /// server has already run. Fanning out on EVERY `EVAL` would put N-1 SPSC
    /// pushes and a cross-shard round trip on the scripting hot path, so the
    /// duty is claimed once per distinct body and cleared by
    /// [`Self::mark_fanned_out`] only when the publish actually completed.
    ///
    /// Costs one sha1 pass over the body. `handle_eval` computes the digest
    /// again in [`Self::load`], so an `EVAL` at `--shards > 1` pays two —
    /// unifying them means threading the digest through three dispatch paths
    /// and is left as a follow-up rather than folded into a correctness fix.
    #[must_use]
    pub fn claim_fanout_duty(&mut self, script: Bytes) -> (String, bool) {
        let sha = sha1_smol::Sha1::from(&script[..]).hexdigest();
        self.store_source(sha.clone(), script);
        let owed = !self.fanned_out.contains(&sha);
        (sha, owed)
    }

    /// Record that `sha1` reached every other shard, so later `EVAL`s of the
    /// same body skip the fan-out. Called ONLY on a complete publish.
    pub fn mark_fanned_out(&mut self, sha1: &str) {
        self.fanned_out.insert(sha1.to_owned());
    }

    pub fn get(&self, sha1_hex: &str) -> Option<&Bytes> {
        self.scripts.get(sha1_hex)
    }

    pub fn exists(&self, sha1_hex: &str) -> bool {
        self.scripts.contains_key(sha1_hex)
    }

    pub fn flush(&mut self) {
        self.scripts.clear();
        self.source_bytes = 0;
        // SCRIPT FLUSH drops the compiled functions too (moon#1167): a sha the
        // source map no longer knows must not stay callable, and the Lua-heap
        // protos are freed for GC. `SCRIPT FLUSH ASYNC`/`SYNC` both land here,
        // as on HEAD.
        self.compiled.clear();
        // A flushed shard owes the world nothing, and the next `EVAL` of any
        // body must republish it (this shard may have been the only holder).
        self.fanned_out.clear();
    }

    pub fn len(&self) -> usize {
        self.scripts.len()
    }

    /// Approximate resident bytes held by cached script bodies (C4 wave-5
    /// hygiene): the sum of each entry's hex-SHA1 key length plus its
    /// source byte length. This is an estimate (it excludes `HashMap`/
    /// `String`/`Bytes` allocator bookkeeping overhead) intended for
    /// observability only -- the source cache itself remains unbounded,
    /// matching Redis semantics (`SCRIPT FLUSH` is the only source eviction).
    ///
    /// O(1) (moon#1167): a running counter maintained by
    /// [`Self::store_source`]/[`Self::flush`], replacing the per-100ms walk over
    /// every cached body that the shard persistence tick used to pay.
    pub fn resident_bytes(&self) -> usize {
        self.source_bytes
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
