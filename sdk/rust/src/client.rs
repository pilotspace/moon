use crate::cache::CacheClient;
use crate::error::Result;
use crate::graph::GraphClient;
use crate::mq::MqClient;
use crate::session::SessionClient;
use crate::temporal::TemporalClient;
use crate::text::TextClient;
use crate::vector::VectorClient;
use crate::workspace::WorkspaceClient;

// Convenience macro: build a Cmd, run it, map the redis error.
macro_rules! cmd {
    ($conn:expr, $name:expr $(, $arg:expr)* => $rv:ty) => {{
        let result: $rv = redis::cmd($name)$(.arg($arg))*.query_async(&mut $conn).await?;
        Ok(result)
    }};
}

/// Async Moon client with Redis-compatible standard commands and Moon-native extensions.
///
/// Build a connection via [`MoonClient::connect`], then access Moon-native features
/// through sub-clients: [`vector`](MoonClient::vector), [`graph`](MoonClient::graph),
/// [`text`](MoonClient::text), [`session`](MoonClient::session), [`cache`](MoonClient::cache),
/// [`workspace`](MoonClient::workspace), [`mq`](MoonClient::mq), [`temporal`](MoonClient::temporal).
///
/// Standard Redis commands are exposed directly as async methods.
///
/// # Example
/// ```no_run
/// use moondb::MoonClient;
///
/// #[tokio::main]
/// async fn main() -> moon::Result<()> {
///     let mut client = MoonClient::connect("redis://127.0.0.1:6399").await?;
///     client.set("hello", "world").await?;
///     let v: String = client.get("hello").await?;
///     println!("{v}");
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct MoonClient {
    pub(crate) conn: redis::aio::MultiplexedConnection,
}

impl MoonClient {
    /// Open an async connection to Moon (or any Redis-compatible server).
    ///
    /// URL formats:
    /// - `redis://127.0.0.1:6399` — plain TCP
    /// - `redis://:password@127.0.0.1:6399/1` — with auth and DB number
    /// - `rediss://127.0.0.1:6399` — TLS (requires `tls-rustls` or `tls-native-tls` feature)
    pub async fn connect(url: impl redis::IntoConnectionInfo) -> Result<Self> {
        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { conn })
    }

    /// Access the raw underlying multiplexed connection for operations not covered
    /// by the typed API.
    pub fn inner_mut(&mut self) -> &mut redis::aio::MultiplexedConnection {
        &mut self.conn
    }

    // ── Sub-client accessors ─────────────────────────────────────────────────

    /// Vector search sub-client (`FT.*` commands).
    pub fn vector(&self) -> VectorClient {
        VectorClient {
            conn: self.conn.clone(),
        }
    }

    /// Graph engine sub-client (`GRAPH.*` commands).
    pub fn graph(&self) -> GraphClient {
        GraphClient {
            conn: self.conn.clone(),
        }
    }

    /// Full-text search sub-client (BM25, `FT.AGGREGATE`, hybrid RRF).
    pub fn text(&self) -> TextClient {
        TextClient {
            conn: self.conn.clone(),
        }
    }

    /// Session-aware vector search sub-client.
    pub fn session(&self) -> SessionClient {
        SessionClient {
            conn: self.conn.clone(),
        }
    }

    /// Semantic cache sub-client (`FT.CACHESEARCH`).
    pub fn cache(&self) -> CacheClient {
        CacheClient {
            conn: self.conn.clone(),
        }
    }

    /// Workspace sub-client (`WS.*` commands).
    pub fn workspace(&self) -> WorkspaceClient {
        WorkspaceClient {
            conn: self.conn.clone(),
        }
    }

    /// Message queue sub-client (`MQ.*` commands).
    pub fn mq(&self) -> MqClient {
        MqClient {
            conn: self.conn.clone(),
        }
    }

    /// Temporal sub-client (`TEMPORAL.*` commands).
    pub fn temporal(&self) -> TemporalClient {
        TemporalClient {
            conn: self.conn.clone(),
        }
    }

    // ── Connection ───────────────────────────────────────────────────────────

    pub async fn ping(&mut self) -> Result<String> {
        cmd!(self.conn, "PING" => String)
    }

    pub async fn auth(&mut self, password: &str) -> Result<()> {
        redis::cmd("AUTH")
            .arg(password)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn select(&mut self, db: u8) -> Result<()> {
        redis::cmd("SELECT")
            .arg(db)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn hello(&mut self, version: u8) -> Result<redis::Value> {
        cmd!(self.conn, "HELLO", version => redis::Value)
    }

    // ── String commands ──────────────────────────────────────────────────────

    pub async fn get<K, RV>(&mut self, key: K) -> Result<RV>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("GET")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn set<K, V>(&mut self, key: K, value: V) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn set_ex<K, V>(&mut self, key: K, value: V, seconds: u64) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("EX")
            .arg(seconds)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn pset_ex<K, V>(&mut self, key: K, value: V, ms: u64) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("PX")
            .arg(ms)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn set_nx<K, V>(&mut self, key: K, value: V) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("SETNX")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn mget<K, RV>(&mut self, keys: &[K]) -> Result<Vec<Option<RV>>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        let mut cmd = redis::cmd("MGET");
        for k in keys {
            cmd.arg(k);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn mset<K, V>(&mut self, items: &[(K, V)]) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let mut cmd = redis::cmd("MSET");
        for (k, v) in items {
            cmd.arg(k).arg(v);
        }
        cmd.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn getset<K, V, RV>(&mut self, key: K, value: V) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("GETSET")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn getdel<K, RV>(&mut self, key: K) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("GETDEL")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn append<K, V>(&mut self, key: K, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("APPEND")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn strlen<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("STRLEN")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn incr<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("INCR")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn incr_by<K>(&mut self, key: K, delta: i64) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("INCRBY")
            .arg(key)
            .arg(delta)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn incr_by_float<K>(&mut self, key: K, delta: f64) -> Result<f64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        let s: String = redis::cmd("INCRBYFLOAT")
            .arg(key)
            .arg(delta)
            .query_async(&mut self.conn)
            .await?;
        Ok(s.parse().unwrap_or(0.0))
    }

    pub async fn decr<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("DECR")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn decr_by<K>(&mut self, key: K, delta: i64) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("DECRBY")
            .arg(key)
            .arg(delta)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Key commands ─────────────────────────────────────────────────────────

    pub async fn del<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("DEL")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn unlink<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("UNLINK")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn exists<K>(&mut self, key: K) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut self.conn)
            .await?;
        Ok(n > 0)
    }

    pub async fn expire<K>(&mut self, key: K, seconds: i64) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn pexpire<K>(&mut self, key: K, ms: i64) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("PEXPIRE")
            .arg(key)
            .arg(ms)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn expire_at<K>(&mut self, key: K, unix_ts: i64) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("EXPIREAT")
            .arg(key)
            .arg(unix_ts)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn persist<K>(&mut self, key: K) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("PERSIST")
            .arg(key)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn ttl<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("TTL")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn pttl<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("PTTL")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn key_type<K>(&mut self, key: K) -> Result<String>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("TYPE")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn rename<K, NK>(&mut self, key: K, new_key: NK) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        NK: redis::ToRedisArgs + Send + Sync,
    {
        redis::cmd("RENAME")
            .arg(key)
            .arg(new_key)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn rename_nx<K, NK>(&mut self, key: K, new_key: NK) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
        NK: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("RENAMENX")
            .arg(key)
            .arg(new_key)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn keys<P, RV>(&mut self, pattern: P) -> Result<Vec<RV>>
    where
        P: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("KEYS")
            .arg(pattern)
            .query_async(&mut self.conn)
            .await?)
    }

    /// SCAN with pattern and count. Returns `(next_cursor, keys)`.
    pub async fn scan_match<P, RV>(
        &mut self,
        pattern: P,
        count: usize,
        cursor: u64,
    ) -> Result<(u64, Vec<RV>)>
    where
        P: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(count)
            .query_async::<(u64, Vec<RV>)>(&mut self.conn)
            .await?)
    }

    // ── Hash commands ────────────────────────────────────────────────────────

    pub async fn hset<K, F, V>(&mut self, key: K, field: F, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hset_multiple<K, F, V>(&mut self, key: K, items: &[(F, V)]) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let mut cmd = redis::cmd("HSET");
        cmd.arg(key);
        for (f, v) in items {
            cmd.arg(f).arg(v);
        }
        cmd.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn hget<K, F, RV>(&mut self, key: K, field: F) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("HGET")
            .arg(key)
            .arg(field)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hmget<K, F, RV>(&mut self, key: K, fields: &[F]) -> Result<Vec<Option<RV>>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(key);
        for f in fields {
            cmd.arg(f);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn hgetall<K>(&mut self, key: K) -> Result<std::collections::HashMap<String, String>>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("HGETALL")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hdel<K, F>(&mut self, key: K, field: F) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("HDEL")
            .arg(key)
            .arg(field)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hexists<K, F>(&mut self, key: K, field: F) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("HEXISTS")
            .arg(key)
            .arg(field)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn hlen<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("HLEN")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hkeys<K, RV>(&mut self, key: K) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("HKEYS")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hvals<K, RV>(&mut self, key: K) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("HVALS")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hincrby<K, F>(&mut self, key: K, field: F, delta: i64) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("HINCRBY")
            .arg(key)
            .arg(field)
            .arg(delta)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn hincrbyfloat<K, F>(&mut self, key: K, field: F, delta: f64) -> Result<f64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
    {
        let s: String = redis::cmd("HINCRBYFLOAT")
            .arg(key)
            .arg(field)
            .arg(delta)
            .query_async(&mut self.conn)
            .await?;
        Ok(s.parse().unwrap_or(0.0))
    }

    pub async fn hsetnx<K, F, V>(&mut self, key: K, field: F, value: V) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("HSETNX")
            .arg(key)
            .arg(field)
            .arg(value)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    // ── List commands ────────────────────────────────────────────────────────

    pub async fn lpush<K, V>(&mut self, key: K, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("LPUSH")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn rpush<K, V>(&mut self, key: K, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("RPUSH")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn lpop<K, RV>(&mut self, key: K, count: Option<usize>) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        let mut cmd = redis::cmd("LPOP");
        cmd.arg(key);
        if let Some(c) = count {
            cmd.arg(c);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn rpop<K, RV>(&mut self, key: K, count: Option<usize>) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        let mut cmd = redis::cmd("RPOP");
        cmd.arg(key);
        if let Some(c) = count {
            cmd.arg(c);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn llen<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("LLEN")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn lrange<K, RV>(&mut self, key: K, start: i64, stop: i64) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("LRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn lindex<K, RV>(&mut self, key: K, index: i64) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("LINDEX")
            .arg(key)
            .arg(index)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn lset<K, V>(&mut self, key: K, index: i64, value: V) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        redis::cmd("LSET")
            .arg(key)
            .arg(index)
            .arg(value)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn lrem<K, V>(&mut self, key: K, count: i64, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("LREM")
            .arg(key)
            .arg(count)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn ltrim<K>(&mut self, key: K, start: i64, stop: i64) -> Result<()>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        redis::cmd("LTRIM")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn lpos<K, V>(&mut self, key: K, value: V) -> Result<Option<i64>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("LPOS")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Set commands ─────────────────────────────────────────────────────────

    pub async fn sadd<K, V>(&mut self, key: K, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("SADD")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn srem<K, V>(&mut self, key: K, value: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("SREM")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn smembers<K, RV>(&mut self, key: K) -> Result<std::collections::HashSet<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue + Eq + std::hash::Hash,
    {
        Ok(redis::cmd("SMEMBERS")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn scard<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("SCARD")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn sismember<K, V>(&mut self, key: K, value: V) -> Result<bool>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let n: i64 = redis::cmd("SISMEMBER")
            .arg(key)
            .arg(value)
            .query_async(&mut self.conn)
            .await?;
        Ok(n == 1)
    }

    pub async fn smismember<K, V>(&mut self, key: K, values: &[V]) -> Result<Vec<bool>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let mut cmd = redis::cmd("SMISMEMBER");
        cmd.arg(key);
        for v in values {
            cmd.arg(v);
        }
        let results: Vec<i64> = cmd.query_async(&mut self.conn).await?;
        Ok(results.into_iter().map(|n| n == 1).collect())
    }

    pub async fn srandmember<K, RV>(&mut self, key: K, count: i64) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("SRANDMEMBER")
            .arg(key)
            .arg(count)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn spop<K, RV>(&mut self, key: K) -> Result<Option<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("SPOP")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn sinter<K, RV>(&mut self, keys: &[K]) -> Result<std::collections::HashSet<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue + Eq + std::hash::Hash,
    {
        let mut cmd = redis::cmd("SINTER");
        for k in keys {
            cmd.arg(k);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn sunion<K, RV>(&mut self, keys: &[K]) -> Result<std::collections::HashSet<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue + Eq + std::hash::Hash,
    {
        let mut cmd = redis::cmd("SUNION");
        for k in keys {
            cmd.arg(k);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn sdiff<K, RV>(&mut self, keys: &[K]) -> Result<std::collections::HashSet<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue + Eq + std::hash::Hash,
    {
        let mut cmd = redis::cmd("SDIFF");
        for k in keys {
            cmd.arg(k);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    // ── Sorted set commands ──────────────────────────────────────────────────

    pub async fn zadd<K, V>(&mut self, key: K, score: f64, member: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("ZADD")
            .arg(key)
            .arg(score)
            .arg(member)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zrem<K, V>(&mut self, key: K, member: V) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("ZREM")
            .arg(key)
            .arg(member)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zscore<K, V>(&mut self, key: K, member: V) -> Result<Option<f64>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let raw: redis::Value = redis::cmd("ZSCORE")
            .arg(key)
            .arg(member)
            .query_async(&mut self.conn)
            .await?;
        Ok(crate::util::value_to_f64(&raw))
    }

    pub async fn zcard<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("ZCARD")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zrank<K, V>(&mut self, key: K, member: V) -> Result<Option<i64>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let raw: redis::Value = redis::cmd("ZRANK")
            .arg(key)
            .arg(member)
            .query_async(&mut self.conn)
            .await?;
        Ok(crate::util::value_to_i64(&raw))
    }

    pub async fn zrevrank<K, V>(&mut self, key: K, member: V) -> Result<Option<i64>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let raw: redis::Value = redis::cmd("ZREVRANK")
            .arg(key)
            .arg(member)
            .query_async(&mut self.conn)
            .await?;
        Ok(crate::util::value_to_i64(&raw))
    }

    pub async fn zincrby<K, V>(&mut self, key: K, delta: f64, member: V) -> Result<f64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let s: String = redis::cmd("ZINCRBY")
            .arg(key)
            .arg(delta)
            .arg(member)
            .query_async(&mut self.conn)
            .await?;
        Ok(s.parse().unwrap_or(0.0))
    }

    pub async fn zrange<K, RV>(&mut self, key: K, start: i64, stop: i64) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("ZRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zrevrange<K, RV>(&mut self, key: K, start: i64, stop: i64) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("ZREVRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zrangebyscore<K, Min, Max, RV>(
        &mut self,
        key: K,
        min: Min,
        max: Max,
    ) -> Result<Vec<RV>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        Min: redis::ToRedisArgs + Send + Sync,
        Max: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("ZRANGEBYSCORE")
            .arg(key)
            .arg(min)
            .arg(max)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zcount<K, Min, Max>(&mut self, key: K, min: Min, max: Max) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        Min: redis::ToRedisArgs + Send + Sync,
        Max: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("ZCOUNT")
            .arg(key)
            .arg(min)
            .arg(max)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zpopmin<K, RV>(&mut self, key: K, count: i64) -> Result<Vec<(RV, f64)>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("ZPOPMIN")
            .arg(key)
            .arg(count)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn zpopmax<K, RV>(&mut self, key: K, count: i64) -> Result<Vec<(RV, f64)>>
    where
        K: redis::ToRedisArgs + Send + Sync,
        RV: redis::FromRedisValue,
    {
        Ok(redis::cmd("ZPOPMAX")
            .arg(key)
            .arg(count)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Stream commands ──────────────────────────────────────────────────────

    /// Append a stream entry (`XADD key * field value ...`).
    pub async fn xadd<K, F, V>(&mut self, key: K, fields: &[(F, V)]) -> Result<String>
    where
        K: redis::ToRedisArgs + Send + Sync,
        F: redis::ToRedisArgs + Send + Sync,
        V: redis::ToRedisArgs + Send + Sync,
    {
        let mut cmd = redis::cmd("XADD");
        cmd.arg(key).arg("*");
        for (f, v) in fields {
            cmd.arg(f).arg(v);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn xlen<K>(&mut self, key: K) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("XLEN")
            .arg(key)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn xrange<K>(&mut self, key: K, start: &str, end: &str) -> Result<redis::Value>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("XRANGE")
            .arg(key)
            .arg(start)
            .arg(end)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn xrevrange<K>(&mut self, key: K, end: &str, start: &str) -> Result<redis::Value>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("XREVRANGE")
            .arg(key)
            .arg(end)
            .arg(start)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn xtrim<K>(&mut self, key: K, maxlen: usize) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("XTRIM")
            .arg(key)
            .arg("MAXLEN")
            .arg(maxlen)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn xdel<K, I>(&mut self, key: K, id: I) -> Result<i64>
    where
        K: redis::ToRedisArgs + Send + Sync,
        I: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("XDEL")
            .arg(key)
            .arg(id)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Pub/Sub ──────────────────────────────────────────────────────────────

    pub async fn publish<C, M>(&mut self, channel: C, message: M) -> Result<i64>
    where
        C: redis::ToRedisArgs + Send + Sync,
        M: redis::ToRedisArgs + Send + Sync,
    {
        Ok(redis::cmd("PUBLISH")
            .arg(channel)
            .arg(message)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Scripting ────────────────────────────────────────────────────────────

    pub async fn eval<RV>(&mut self, script: &str, keys: &[&str], args: &[&str]) -> Result<RV>
    where
        RV: redis::FromRedisValue,
    {
        let mut cmd = redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys {
            cmd.arg(*k);
        }
        for a in args {
            cmd.arg(*a);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn evalsha<RV>(&mut self, sha: &str, keys: &[&str], args: &[&str]) -> Result<RV>
    where
        RV: redis::FromRedisValue,
    {
        let mut cmd = redis::cmd("EVALSHA");
        cmd.arg(sha).arg(keys.len());
        for k in keys {
            cmd.arg(*k);
        }
        for a in args {
            cmd.arg(*a);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn script_load(&mut self, script: &str) -> Result<String> {
        Ok(redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(script)
            .query_async(&mut self.conn)
            .await?)
    }

    // ── Transactions: MULTI/EXEC ─────────────────────────────────────────────

    /// Execute a [`redis::Pipeline`] atomically (wraps in `MULTI/EXEC`).
    ///
    /// Build the pipeline with `redis::pipe().atomic()`, add commands, then pass it here.
    pub async fn exec_pipeline(&mut self, pipe: redis::Pipeline) -> Result<Vec<redis::Value>> {
        Ok(pipe.query_async(&mut self.conn).await?)
    }

    // ── Moon TXN (cross-store ACID transactions) ─────────────────────────────

    pub async fn txn_begin(&mut self) -> Result<()> {
        redis::cmd("TXN")
            .arg("BEGIN")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn txn_commit(&mut self) -> Result<()> {
        redis::cmd("TXN")
            .arg("COMMIT")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn txn_abort(&mut self) -> Result<()> {
        redis::cmd("TXN")
            .arg("ABORT")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    // ── Server / Admin ───────────────────────────────────────────────────────

    pub async fn info(&mut self, section: Option<&str>) -> Result<String> {
        let mut cmd = redis::cmd("INFO");
        if let Some(s) = section {
            cmd.arg(s);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn dbsize(&mut self) -> Result<i64> {
        Ok(redis::cmd("DBSIZE").query_async(&mut self.conn).await?)
    }

    pub async fn flushdb(&mut self) -> Result<()> {
        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn flushall(&mut self) -> Result<()> {
        redis::cmd("FLUSHALL")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn bgsave(&mut self) -> Result<()> {
        redis::cmd("BGSAVE")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn bgrewriteaof(&mut self) -> Result<()> {
        redis::cmd("BGREWRITEAOF")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn config_get(
        &mut self,
        pattern: &str,
    ) -> Result<std::collections::HashMap<String, String>> {
        let flat: Vec<String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg(pattern)
            .query_async(&mut self.conn)
            .await?;
        Ok(flat
            .chunks(2)
            .filter_map(|c| {
                if c.len() == 2 {
                    Some((c[0].clone(), c[1].clone()))
                } else {
                    None
                }
            })
            .collect())
    }

    pub async fn config_set(&mut self, param: &str, value: &str) -> Result<()> {
        redis::cmd("CONFIG")
            .arg("SET")
            .arg(param)
            .arg(value)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn slowlog_get(&mut self, count: Option<usize>) -> Result<redis::Value> {
        let mut cmd = redis::cmd("SLOWLOG");
        cmd.arg("GET");
        if let Some(n) = count {
            cmd.arg(n);
        }
        Ok(cmd.query_async(&mut self.conn).await?)
    }

    pub async fn client_info(&mut self) -> Result<String> {
        Ok(redis::cmd("CLIENT")
            .arg("INFO")
            .query_async(&mut self.conn)
            .await?)
    }

    // ── ACL ──────────────────────────────────────────────────────────────────

    pub async fn acl_whoami(&mut self) -> Result<String> {
        Ok(redis::cmd("ACL")
            .arg("WHOAMI")
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn acl_list(&mut self) -> Result<Vec<String>> {
        Ok(redis::cmd("ACL")
            .arg("LIST")
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn acl_setuser(&mut self, rules: &[&str]) -> Result<()> {
        let mut cmd = redis::cmd("ACL");
        cmd.arg("SETUSER");
        for r in rules {
            cmd.arg(*r);
        }
        cmd.query_async::<()>(&mut self.conn).await?;
        Ok(())
    }

    pub async fn acl_deluser(&mut self, username: &str) -> Result<()> {
        redis::cmd("ACL")
            .arg("DELUSER")
            .arg(username)
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn acl_getuser(&mut self, username: &str) -> Result<redis::Value> {
        Ok(redis::cmd("ACL")
            .arg("GETUSER")
            .arg(username)
            .query_async(&mut self.conn)
            .await?)
    }

    pub async fn acl_save(&mut self) -> Result<()> {
        redis::cmd("ACL")
            .arg("SAVE")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }

    pub async fn acl_load(&mut self) -> Result<()> {
        redis::cmd("ACL")
            .arg("LOAD")
            .query_async::<()>(&mut self.conn)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MoonClient>();
    }
}
