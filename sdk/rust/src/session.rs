use crate::error::Result;
use crate::types::{SearchResult, encode_vector};
use crate::util::parse_search_results;

/// Session-aware vector search sub-client.
///
/// Deduplicate search results across queries within a user session by
/// using `FT.SEARCH … SESSION <session_key>`.
///
/// Obtain via [`MoonClient::session`](crate::MoonClient::session).
pub struct SessionClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl SessionClient {
    /// Session-scoped KNN search.
    ///
    /// Results already seen in `session_key` are filtered out. The session
    /// key is a sorted set that Moon manages for deduplication.
    pub async fn search(
        &mut self,
        index: &str,
        session_key: &str,
        query_vec: &[f32],
        k: usize,
        field_name: &str,
    ) -> Result<Vec<SearchResult>> {
        let blob = encode_vector(query_vec);
        let query = format!("*=>[KNN {k} @{field_name} $query_vec]");
        let raw: redis::Value = redis::cmd("FT.SEARCH")
            .arg(index)
            .arg(&query)
            .arg("SESSION")
            .arg(session_key)
            .arg("PARAMS").arg("2").arg("query_vec").arg(&blob)
            .arg("DIALECT").arg("2")
            .query_async(&mut self.conn)
            .await?;
        Ok(parse_search_results(raw))
    }

    /// Clear a session's deduplication history by deleting its sorted-set key.
    pub async fn clear(&mut self, session_key: &str) -> Result<()> {
        redis::cmd("DEL")
            .arg(session_key)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    /// Set a TTL on the session key (seconds).
    pub async fn expire(&mut self, session_key: &str, seconds: i64) -> Result<bool> {
        Ok(redis::cmd("EXPIRE")
            .arg(session_key)
            .arg(seconds)
            .query_async(&mut self.conn)
            .await?)
    }

    /// Retrieve the list of keys seen so far in this session, ordered by recency.
    pub async fn history(&mut self, session_key: &str, count: i64) -> Result<Vec<String>> {
        Ok(redis::cmd("ZRANGE")
            .arg(session_key)
            .arg(0)
            .arg(count - 1)
            .query_async(&mut self.conn)
            .await?)
    }
}
