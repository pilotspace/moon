use crate::error::Result;
use crate::types::{CacheSearchResult, IndexInfo, SearchResult, VectorIndexOptions, encode_vector};
use crate::util::{parse_cache_search_results, parse_index_info, parse_search_results};

/// Vector search sub-client (`FT.*` commands).
///
/// Obtain via [`MoonClient::vector`](crate::MoonClient::vector).
///
/// # Example
/// ```no_run
/// use moon_client::{MoonClient, types::{VectorIndexOptions, DistanceMetric}};
///
/// #[tokio::main]
/// async fn main() -> moon_client::Result<()> {
///     let client = MoonClient::connect("redis://127.0.0.1:6399").await?;
///     let mut v = client.vector();
///     v.create_index("idx", VectorIndexOptions::new(384, DistanceMetric::Cosine)).await?;
///     let results = v.search("idx", &[0.1_f32; 384], 10).await?;
///     Ok(())
/// }
/// ```
pub struct VectorClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl VectorClient {
    /// Create a vector search index via `FT.CREATE`.
    pub async fn create_index(&mut self, name: &str, opts: VectorIndexOptions) -> Result<String> {
        let mut cmd = redis::cmd("FT.CREATE");
        cmd.arg(name)
            .arg("ON").arg("HASH")
            .arg("PREFIX").arg("1").arg(&opts.prefix)
            .arg("SCHEMA");

        // Extra non-vector schema fields first
        for field in &opts.extra_schema {
            for arg in field.to_args() {
                cmd.arg(arg);
            }
        }

        // Vector field — Moon only supports HNSW.
        // Base params: TYPE, DIM, DISTANCE_METRIC, M, EF_CONSTRUCTION = 5 pairs = 10 args.
        // Each optional HNSW param adds 2 more.
        let param_count: usize = 10
            + if opts.ef_runtime.is_some() { 2 } else { 0 }
            + if opts.compact_threshold.is_some() { 2 } else { 0 };

        cmd.arg(&opts.field_name)
            .arg("VECTOR")
            .arg("HNSW")
            .arg(param_count)
            .arg("TYPE").arg(&opts.dtype)
            .arg("DIM").arg(opts.dim)
            .arg("DISTANCE_METRIC").arg(opts.metric.as_str())
            .arg("M").arg(opts.m)
            .arg("EF_CONSTRUCTION").arg(opts.ef_construction);

        if let Some(ef) = opts.ef_runtime {
            cmd.arg("EF_RUNTIME").arg(ef);
        }
        if let Some(ct) = opts.compact_threshold {
            cmd.arg("COMPACT_THRESHOLD").arg(ct);
        }

        let v: String = cmd.query_async(&mut self.conn).await?;
        Ok(v)
    }

    /// Drop an index. If `delete_docs` is true, also deletes the underlying hash keys.
    pub async fn drop_index(&mut self, name: &str, delete_docs: bool) -> Result<String> {
        let mut cmd = redis::cmd("FT.DROPINDEX");
        cmd.arg(name);
        if delete_docs {
            cmd.arg("DD");
        }
        let v: String = cmd.query_async(&mut self.conn).await?;
        Ok(v)
    }

    /// Retrieve metadata about an index via `FT.INFO`.
    pub async fn index_info(&mut self, name: &str) -> Result<IndexInfo> {
        let raw: redis::Value = redis::cmd("FT.INFO").arg(name).query_async(&mut self.conn).await?;
        Ok(parse_index_info(name.to_string(), raw))
    }

    /// List all vector indexes via `FT._LIST`.
    pub async fn list_indexes(&mut self) -> Result<Vec<String>> {
        let v: Vec<String> = redis::cmd("FT._LIST").query_async(&mut self.conn).await?;
        Ok(v)
    }

    /// Trigger a compaction of the mutable segment into an immutable HNSW graph.
    pub async fn compact(&mut self, name: &str) -> Result<String> {
        let v: String = redis::cmd("FT.COMPACT").arg(name).query_async(&mut self.conn).await?;
        Ok(v)
    }

    /// KNN vector search via `FT.SEARCH`.
    ///
    /// - `index` — index name
    /// - `query_vec` — query vector as `f32` slice (auto-encoded to bytes)
    /// - `k` — number of nearest neighbours to return
    /// - `field_name` — vector field name (defaults to `"vec"`)
    /// - `return_fields` — additional hash fields to return (`None` = all)
    /// - `as_of` — optional MVCC timestamp (unix ms) for temporal queries
    pub async fn search(
        &mut self,
        index: &str,
        query_vec: &[f32],
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        self.search_opts(index, query_vec, k, "vec", None, None).await
    }

    /// Full-featured KNN search with optional `field_name`, `return_fields`, and `as_of`.
    pub async fn search_opts(
        &mut self,
        index: &str,
        query_vec: &[f32],
        k: usize,
        field_name: &str,
        return_fields: Option<&[&str]>,
        as_of: Option<i64>,
    ) -> Result<Vec<SearchResult>> {
        let blob = encode_vector(query_vec);
        let query = format!("*=>[KNN {k} @{field_name} $query_vec]");
        let mut cmd = redis::cmd("FT.SEARCH");
        cmd.arg(index).arg(&query)
            .arg("PARAMS").arg("2").arg("query_vec").arg(&blob);
        if let Some(fields) = return_fields {
            cmd.arg("RETURN").arg(fields.len());
            for f in fields {
                cmd.arg(*f);
            }
        }
        if let Some(ts) = as_of {
            cmd.arg("AS_OF").arg(ts);
        }
        cmd.arg("DIALECT").arg("2");
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(parse_search_results(raw))
    }

    /// Semantic cache search (`FT.CACHESEARCH`).
    ///
    /// Checks the semantic cache for a similar query vector. On hit, returns the cached
    /// result. On miss, performs a fallback KNN search.
    pub async fn cache_search(
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

    /// Recommendation via `FT.RECOMMEND`.
    ///
    /// Finds items similar to `positive_keys` and dissimilar to `negative_keys`.
    pub async fn recommend(
        &mut self,
        index: &str,
        positive_keys: &[&str],
        negative_keys: Option<&[&str]>,
        k: usize,
        field_name: Option<&str>,
    ) -> Result<Vec<SearchResult>> {
        let mut cmd = redis::cmd("FT.RECOMMEND");
        cmd.arg(index).arg("POSITIVE");
        for k_str in positive_keys {
            cmd.arg(*k_str);
        }
        if let Some(neg) = negative_keys {
            cmd.arg("NEGATIVE");
            for k_str in neg {
                cmd.arg(*k_str);
            }
        }
        cmd.arg("K").arg(k);
        if let Some(f) = field_name {
            cmd.arg("FIELD").arg(f);
        }
        let raw: redis::Value = cmd.query_async(&mut self.conn).await?;
        Ok(parse_search_results(raw))
    }

    /// Multi-hop knowledge navigation via `FT.NAVIGATE`.
    pub async fn navigate(
        &mut self,
        index: &str,
        query_vec: &[f32],
        k: usize,
        field_name: &str,
        hops: usize,
        hop_penalty: f64,
    ) -> Result<Vec<SearchResult>> {
        let blob = encode_vector(query_vec);
        let query = format!("*=>[KNN {k} @{field_name} $vec_param]");
        let raw: redis::Value = redis::cmd("FT.NAVIGATE")
            .arg(index)
            .arg(&query)
            .arg("HOPS").arg(hops)
            .arg("HOP_PENALTY").arg(hop_penalty)
            .arg("PARAMS").arg("2").arg("vec_param").arg(&blob)
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_search_results(raw))
    }

    /// Set per-index config via `FT.CONFIG SET`.
    pub async fn config_set(&mut self, index: &str, param: &str, value: &str) -> Result<()> {
        redis::cmd("FT.CONFIG")
            .arg("SET")
            .arg(index)
            .arg(param)
            .arg(value)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Get per-index config via `FT.CONFIG GET`.
    pub async fn config_get(&mut self, index: &str, param: &str) -> Result<redis::Value> {
        Ok(redis::cmd("FT.CONFIG")
            .arg("GET")
            .arg(index)
            .arg(param)
            .query_async(&mut self.conn)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{DistanceMetric, VectorIndexOptions};

    #[test]
    fn vector_index_options_builder() {
        let opts = VectorIndexOptions::new(128, DistanceMetric::Cosine)
            .prefix("item:")
            .field_name("embedding")
            .m(32)
            .ef_construction(400)
            .ef_runtime(100)
            .compact_threshold(5000);

        assert_eq!(opts.prefix, "item:");
        assert_eq!(opts.field_name, "embedding");
        assert_eq!(opts.dim, 128);
        assert_eq!(opts.m, 32);
        assert_eq!(opts.ef_construction, 400);
        assert_eq!(opts.ef_runtime, Some(100));
        assert_eq!(opts.compact_threshold, Some(5000));
        assert_eq!(opts.metric.as_str(), "COSINE");
    }
}
