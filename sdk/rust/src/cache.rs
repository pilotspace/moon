use crate::error::Result;
use crate::types::{CacheSearchResult, encode_vector};
use crate::util::parse_cache_search_results;

/// Semantic cache sub-client (`FT.CACHESEARCH`).
///
/// Store and retrieve semantically-similar query results using Moon's
/// built-in semantic caching layer.
///
/// Obtain via [`MoonClient::cache`](crate::MoonClient::cache).
pub struct CacheClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl CacheClient {
    /// Check the semantic cache for a matching entry, falling back to a KNN search.
    ///
    /// Returns a [`CacheSearchResult`] with a `cache_hit` flag indicating whether
    /// results came from the cache or a live search.
    pub async fn lookup(
        &mut self,
        index: &str,
        cache_prefix: &str,
        query_vec: &[f32],
        k: usize,
        field_name: &str,
        threshold: f64,
        fallback_k: usize,
    ) -> Result<CacheSearchResult> {
        let blob = encode_vector(query_vec);
        let query = format!("*=>[KNN {k} @{field_name} $query_vec]");
        let raw: redis::Value = redis::cmd("FT.CACHESEARCH")
            .arg(index)
            .arg(cache_prefix)
            .arg(&query)
            .arg("PARAMS").arg("2").arg("query_vec").arg(&blob)
            .arg("THRESHOLD").arg(threshold)
            .arg("FALLBACK").arg("KNN").arg(fallback_k)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_cache_search_results(raw))
    }

    /// Store a result into the semantic cache.
    ///
    /// Writes `query_vec` bytes and `response_json` to a hash at `cache_prefix + cache_key`,
    /// then sets an optional TTL.
    pub async fn store(
        &mut self,
        cache_key: &str,
        query_vec: &[f32],
        response_data: &str,
        vec_field: &str,
        ttl_seconds: Option<i64>,
    ) -> Result<()> {
        let blob = encode_vector(query_vec);
        redis::cmd("HSET")
            .arg(cache_key)
            .arg(vec_field)
            .arg(&blob)
            .arg("response")
            .arg(response_data)
            .query_async::<()>(&mut self.conn)
            .await?;
        if let Some(ttl) = ttl_seconds {
            redis::cmd("EXPIRE")
                .arg(cache_key)
                .arg(ttl)
                .query_async::<()>(&mut self.conn)
                .await?;
        }
        Ok(())
    }

    /// Invalidate (delete) a cached entry by key.
    pub async fn invalidate(&mut self, cache_key: &str) -> Result<()> {
        redis::cmd("DEL")
            .arg(cache_key)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Scan for all cache entries matching a prefix pattern.
    ///
    /// Returns matching keys (not the full documents). Use `get_entry` for details.
    pub async fn scan_keys(
        &mut self,
        pattern: &str,
        count: usize,
    ) -> Result<Vec<String>> {
        let (_, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(0u64)
            .arg("MATCH").arg(pattern)
            .arg("COUNT").arg(count)
            .query_async(&mut self.conn)
            .await?;
        Ok(keys)
    }
}
