//! Redis compatibility test battery.
//!
//! Ports the most important Redis TCL test behaviors as Rust integration tests.
//! Each test connects to a running Moon instance (default: 127.0.0.1:6379).
//!
//! All tests are `#[ignore]` — they require a running server:
//!   MOON_TEST_PORT=6379 cargo test --test redis_compat -- --ignored
//!
//! Set `MOON_TEST_PORT` to override the default port.

use redis::{Commands, RedisResult};

fn port() -> u16 {
    std::env::var("MOON_TEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379)
}

fn client() -> redis::Client {
    redis::Client::open(format!("redis://127.0.0.1:{}/", port())).unwrap()
}

fn sync_conn() -> redis::Connection {
    let mut conn = client().get_connection().unwrap();
    // Flush DB for isolation
    let _: RedisResult<String> = redis::cmd("FLUSHDB").query(&mut conn);
    conn
}

// =========================================================================
// String commands
// =========================================================================

#[test]
#[ignore]
fn string_set_get() {
    let mut c = sync_conn();
    let _: () = c.set("str:k1", "hello").unwrap();
    let v: String = c.get("str:k1").unwrap();
    assert_eq!(v, "hello");
}

#[test]
#[ignore]
fn string_set_nx_xx() {
    let mut c = sync_conn();
    // NX: set only if not exists
    let ok: bool = redis::cmd("SET")
        .arg("str:nx")
        .arg("first")
        .arg("NX")
        .query(&mut c)
        .unwrap();
    assert!(ok);
    // NX again should fail
    let res: Option<String> = redis::cmd("SET")
        .arg("str:nx")
        .arg("second")
        .arg("NX")
        .query(&mut c)
        .unwrap();
    assert!(res.is_none());
    // Value should remain "first"
    let v: String = c.get("str:nx").unwrap();
    assert_eq!(v, "first");
    // XX: set only if exists
    let ok: bool = redis::cmd("SET")
        .arg("str:nx")
        .arg("updated")
        .arg("XX")
        .query(&mut c)
        .unwrap();
    assert!(ok);
    let v: String = c.get("str:nx").unwrap();
    assert_eq!(v, "updated");
}

#[test]
#[ignore]
fn string_mset_mget() {
    let mut c = sync_conn();
    let _: () = redis::cmd("MSET")
        .arg("str:a")
        .arg("1")
        .arg("str:b")
        .arg("2")
        .arg("str:c")
        .arg("3")
        .query(&mut c)
        .unwrap();
    let vals: Vec<String> = redis::cmd("MGET")
        .arg("str:a")
        .arg("str:b")
        .arg("str:c")
        .query(&mut c)
        .unwrap();
    assert_eq!(vals, vec!["1", "2", "3"]);
}

#[test]
#[ignore]
fn string_incr_decr() {
    let mut c = sync_conn();
    let _: () = c.set("str:counter", "10").unwrap();
    let v: i64 = c.incr("str:counter", 1).unwrap();
    assert_eq!(v, 11);
    let v: i64 = c.incr("str:counter", 5).unwrap();
    assert_eq!(v, 16);
    let v: i64 = c.decr("str:counter", 3).unwrap();
    assert_eq!(v, 13);
}

#[test]
#[ignore]
fn string_append_strlen() {
    let mut c = sync_conn();
    let _: () = c.set("str:app", "hello").unwrap();
    let len: i64 = c.append("str:app", " world").unwrap();
    assert_eq!(len, 11);
    let v: String = c.get("str:app").unwrap();
    assert_eq!(v, "hello world");
    let slen: i64 = redis::cmd("STRLEN").arg("str:app").query(&mut c).unwrap();
    assert_eq!(slen, 11);
}

// =========================================================================
// Hash commands
// =========================================================================

#[test]
#[ignore]
fn hash_set_get_del() {
    let mut c = sync_conn();
    let _: () = c.hset("h:1", "field1", "val1").unwrap();
    let _: () = c.hset("h:1", "field2", "val2").unwrap();
    let v: String = c.hget("h:1", "field1").unwrap();
    assert_eq!(v, "val1");
    let deleted: i64 = c.hdel("h:1", "field1").unwrap();
    assert_eq!(deleted, 1);
    let exists: bool = c.hexists("h:1", "field1").unwrap();
    assert!(!exists);
}

#[test]
#[ignore]
fn hash_len_getall_keys_vals() {
    let mut c = sync_conn();
    let _: () = c.hset("h:2", "a", "1").unwrap();
    let _: () = c.hset("h:2", "b", "2").unwrap();
    let _: () = c.hset("h:2", "c", "3").unwrap();
    let len: i64 = c.hlen("h:2").unwrap();
    assert_eq!(len, 3);

    let all: std::collections::HashMap<String, String> = c.hgetall("h:2").unwrap();
    assert_eq!(all.len(), 3);
    assert_eq!(all.get("b").map(|s| s.as_str()), Some("2"));

    let mut keys: Vec<String> = redis::cmd("HKEYS").arg("h:2").query(&mut c).unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b", "c"]);

    let mut vals: Vec<String> = redis::cmd("HVALS").arg("h:2").query(&mut c).unwrap();
    vals.sort();
    assert_eq!(vals, vec!["1", "2", "3"]);
}

// =========================================================================
// List commands
// =========================================================================

#[test]
#[ignore]
fn list_push_pop_len() {
    let mut c = sync_conn();
    let _: () = c.lpush("l:1", "a").unwrap();
    let _: () = c.lpush("l:1", "b").unwrap();
    let _: () = c.rpush("l:1", "c").unwrap();
    // List is now: [b, a, c]
    let len: i64 = c.llen("l:1").unwrap();
    assert_eq!(len, 3);
    let v: String = c.lpop("l:1", None).unwrap();
    assert_eq!(v, "b");
    let v: String = c.rpop("l:1", None).unwrap();
    assert_eq!(v, "c");
}

#[test]
#[ignore]
fn list_lrange_lindex() {
    let mut c = sync_conn();
    let _: () = c.rpush("l:2", "x").unwrap();
    let _: () = c.rpush("l:2", "y").unwrap();
    let _: () = c.rpush("l:2", "z").unwrap();
    let range: Vec<String> = c.lrange("l:2", 0, -1).unwrap();
    assert_eq!(range, vec!["x", "y", "z"]);
    let idx: String = c.lindex("l:2", 1).unwrap();
    assert_eq!(idx, "y");
}

// =========================================================================
// Set commands
// =========================================================================

#[test]
#[ignore]
fn set_add_rem_card_ismember() {
    let mut c = sync_conn();
    let _: () = c.sadd("s:1", "a").unwrap();
    let _: () = c.sadd("s:1", "b").unwrap();
    let _: () = c.sadd("s:1", "c").unwrap();
    let card: i64 = c.scard("s:1").unwrap();
    assert_eq!(card, 3);
    let is: bool = c.sismember("s:1", "b").unwrap();
    assert!(is);
    let removed: i64 = c.srem("s:1", "b").unwrap();
    assert_eq!(removed, 1);
    let is: bool = c.sismember("s:1", "b").unwrap();
    assert!(!is);
}

#[test]
#[ignore]
fn set_members_union_inter_diff() {
    let mut c = sync_conn();
    let _: () = c.sadd("s:a", vec!["1", "2", "3"]).unwrap();
    let _: () = c.sadd("s:b", vec!["2", "3", "4"]).unwrap();

    let mut members: Vec<String> = c.smembers("s:a").unwrap();
    members.sort();
    assert_eq!(members, vec!["1", "2", "3"]);

    let mut union: Vec<String> = c.sunion(vec!["s:a", "s:b"]).unwrap();
    union.sort();
    assert_eq!(union, vec!["1", "2", "3", "4"]);

    let mut inter: Vec<String> = c.sinter(vec!["s:a", "s:b"]).unwrap();
    inter.sort();
    assert_eq!(inter, vec!["2", "3"]);

    let mut diff: Vec<String> = c.sdiff(vec!["s:a", "s:b"]).unwrap();
    diff.sort();
    assert_eq!(diff, vec!["1"]);
}

// =========================================================================
// Sorted set commands
// =========================================================================

#[test]
#[ignore]
fn zset_add_rem_card_score() {
    let mut c = sync_conn();
    let _: () = c.zadd("z:1", "alice", 10.0).unwrap();
    let _: () = c.zadd("z:1", "bob", 20.0).unwrap();
    let _: () = c.zadd("z:1", "carol", 15.0).unwrap();
    let card: i64 = c.zcard("z:1").unwrap();
    assert_eq!(card, 3);
    let score: f64 = c.zscore("z:1", "bob").unwrap();
    assert!((score - 20.0).abs() < f64::EPSILON);
    let removed: i64 = c.zrem("z:1", "bob").unwrap();
    assert_eq!(removed, 1);
    let card: i64 = c.zcard("z:1").unwrap();
    assert_eq!(card, 2);
}

#[test]
#[ignore]
fn zset_range_rangebyscore_rank() {
    let mut c = sync_conn();
    let _: () = c.zadd("z:2", "a", 1.0).unwrap();
    let _: () = c.zadd("z:2", "b", 2.0).unwrap();
    let _: () = c.zadd("z:2", "c", 3.0).unwrap();
    let _: () = c.zadd("z:2", "d", 4.0).unwrap();

    // ZRANGE 0 -1 (all, ascending)
    let all: Vec<String> = c.zrange("z:2", 0, -1).unwrap();
    assert_eq!(all, vec!["a", "b", "c", "d"]);

    // ZRANGEBYSCORE 2 3
    let range: Vec<String> = c.zrangebyscore("z:2", 2.0, 3.0).unwrap();
    assert_eq!(range, vec!["b", "c"]);

    // ZRANK
    let rank: i64 = c.zrank("z:2", "c").unwrap();
    assert_eq!(rank, 2); // 0-indexed
}

// =========================================================================
// Key commands
// =========================================================================

#[test]
#[ignore]
fn key_del_exists_type() {
    let mut c = sync_conn();
    let _: () = c.set("k:str", "val").unwrap();
    let _: () = c.lpush("k:list", "item").unwrap();
    assert_eq!(c.exists::<_, i64>("k:str").unwrap(), 1);
    assert_eq!(c.exists::<_, i64>("k:missing").unwrap(), 0);

    let t: String = redis::cmd("TYPE").arg("k:str").query(&mut c).unwrap();
    assert_eq!(t, "string");
    let t: String = redis::cmd("TYPE").arg("k:list").query(&mut c).unwrap();
    assert_eq!(t, "list");

    let deleted: i64 = c.del("k:str").unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(c.exists::<_, i64>("k:str").unwrap(), 0);
}

#[test]
#[ignore]
fn key_expire_ttl() {
    let mut c = sync_conn();
    let _: () = c.set("k:ttl", "temp").unwrap();
    let ttl: i64 = redis::cmd("TTL").arg("k:ttl").query(&mut c).unwrap();
    assert_eq!(ttl, -1); // No expiry set

    let _: () = c.expire("k:ttl", 100).unwrap();
    let ttl: i64 = redis::cmd("TTL").arg("k:ttl").query(&mut c).unwrap();
    assert!(ttl > 0 && ttl <= 100);
}

#[test]
#[ignore]
fn key_rename() {
    let mut c = sync_conn();
    let _: () = c.set("k:old", "data").unwrap();
    let _: () = redis::cmd("RENAME")
        .arg("k:old")
        .arg("k:new")
        .query(&mut c)
        .unwrap();
    let v: String = c.get("k:new").unwrap();
    assert_eq!(v, "data");
    assert_eq!(c.exists::<_, i64>("k:old").unwrap(), 0);
}

#[test]
#[ignore]
fn key_keys_pattern() {
    let mut c = sync_conn();
    let _: () = c.set("kp:alpha", "1").unwrap();
    let _: () = c.set("kp:beta", "2").unwrap();
    let _: () = c.set("kp:gamma", "3").unwrap();
    let _: () = c.set("other:x", "4").unwrap();

    let mut matched: Vec<String> = redis::cmd("KEYS").arg("kp:*").query(&mut c).unwrap();
    matched.sort();
    assert_eq!(matched, vec!["kp:alpha", "kp:beta", "kp:gamma"]);
}

// =========================================================================
// Transaction commands
// =========================================================================

#[test]
#[ignore]
fn transaction_multi_exec() {
    let mut c = sync_conn();
    let _: () = redis::cmd("MULTI").query(&mut c).unwrap();
    let _: redis::Value = redis::cmd("SET")
        .arg("tx:a")
        .arg("100")
        .query(&mut c)
        .unwrap();
    let _: redis::Value = redis::cmd("SET")
        .arg("tx:b")
        .arg("200")
        .query(&mut c)
        .unwrap();
    let _: redis::Value = redis::cmd("INCR").arg("tx:a").query(&mut c).unwrap();
    let results: Vec<redis::Value> = redis::cmd("EXEC").query(&mut c).unwrap();
    // EXEC returns array of results: [OK, OK, 101]
    assert_eq!(results.len(), 3);
    let v: String = c.get("tx:a").unwrap();
    assert_eq!(v, "101");
    let v: String = c.get("tx:b").unwrap();
    assert_eq!(v, "200");
}

#[test]
#[ignore]
fn transaction_discard() {
    let mut c = sync_conn();
    let _: () = c.set("tx:d", "original").unwrap();
    let _: () = redis::cmd("MULTI").query(&mut c).unwrap();
    let _: redis::Value = redis::cmd("SET")
        .arg("tx:d")
        .arg("changed")
        .query(&mut c)
        .unwrap();
    let _: () = redis::cmd("DISCARD").query(&mut c).unwrap();
    let v: String = c.get("tx:d").unwrap();
    assert_eq!(v, "original");
}

// =========================================================================
// Pub/Sub (basic flow)
// =========================================================================

#[test]
#[ignore]
fn pubsub_subscribe_publish() {
    // Use a dedicated connection for the subscriber
    let sub_client = client();
    let mut sub_conn = sub_client.get_connection().unwrap();
    let mut pub_conn = client().get_connection().unwrap();

    // Subscribe
    let mut pubsub = sub_conn.as_pubsub();
    pubsub.subscribe("test-channel").unwrap();

    // Publish from another connection
    let receivers: i64 = pub_conn.publish("test-channel", "hello-pubsub").unwrap();
    assert!(
        receivers >= 1,
        "expected at least 1 subscriber, got {receivers}"
    );

    // Receive the message
    let msg = pubsub.get_message().unwrap();
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "hello-pubsub");
    assert_eq!(msg.get_channel_name(), "test-channel");

    pubsub.unsubscribe("test-channel").unwrap();
}

// =========================================================================
// Cross-type edge cases
// =========================================================================

#[test]
#[ignore]
fn get_nonexistent_key_returns_nil() {
    let mut c = sync_conn();
    let v: Option<String> = c.get("nonexistent:key:12345").unwrap();
    assert!(v.is_none());
}

#[test]
#[ignore]
fn del_multiple_keys() {
    let mut c = sync_conn();
    let _: () = c.set("dm:1", "a").unwrap();
    let _: () = c.set("dm:2", "b").unwrap();
    let _: () = c.set("dm:3", "c").unwrap();
    let deleted: i64 = c.del(vec!["dm:1", "dm:2", "dm:3", "dm:missing"]).unwrap();
    assert_eq!(deleted, 3);
}

#[test]
#[ignore]
fn incr_on_nonexistent_creates_key() {
    let mut c = sync_conn();
    let v: i64 = c.incr("incr:new", 1).unwrap();
    assert_eq!(v, 1);
}

#[test]
#[ignore]
fn overwrite_different_type() {
    let mut c = sync_conn();
    let _: () = c.set("ow:key", "string-val").unwrap();
    // SET should overwrite regardless of type
    let _: () = c.set("ow:key", "new-val").unwrap();
    let v: String = c.get("ow:key").unwrap();
    assert_eq!(v, "new-val");
}

// ── Streams ────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn xadd_and_xlen() {
    let mut c = sync_conn();
    let _: () = redis::cmd("DEL")
        .arg("compat:stream1")
        .query(&mut c)
        .unwrap();
    let id: String = redis::cmd("XADD")
        .arg("compat:stream1")
        .arg("*")
        .arg("field1")
        .arg("value1")
        .query(&mut c)
        .unwrap();
    assert!(!id.is_empty(), "XADD should return an entry ID");
    let len: i64 = redis::cmd("XLEN")
        .arg("compat:stream1")
        .query(&mut c)
        .unwrap();
    assert_eq!(len, 1);
}

#[test]
#[ignore]
fn xrange_basic() {
    let mut c = sync_conn();
    let _: () = redis::cmd("DEL")
        .arg("compat:stream2")
        .query(&mut c)
        .unwrap();
    for i in 0..3 {
        let _: String = redis::cmd("XADD")
            .arg("compat:stream2")
            .arg("*")
            .arg("i")
            .arg(i.to_string())
            .query(&mut c)
            .unwrap();
    }
    let entries: Vec<redis::Value> = redis::cmd("XRANGE")
        .arg("compat:stream2")
        .arg("-")
        .arg("+")
        .query(&mut c)
        .unwrap();
    assert_eq!(entries.len(), 3);
}

#[test]
#[ignore]
fn xtrim_maxlen() {
    let mut c = sync_conn();
    let _: () = redis::cmd("DEL")
        .arg("compat:stream3")
        .query(&mut c)
        .unwrap();
    for i in 0..10 {
        let _: String = redis::cmd("XADD")
            .arg("compat:stream3")
            .arg("*")
            .arg("i")
            .arg(i.to_string())
            .query(&mut c)
            .unwrap();
    }
    let trimmed: i64 = redis::cmd("XTRIM")
        .arg("compat:stream3")
        .arg("MAXLEN")
        .arg("5")
        .query(&mut c)
        .unwrap();
    assert!(trimmed >= 5, "XTRIM should remove at least 5 entries");
    let len: i64 = redis::cmd("XLEN")
        .arg("compat:stream3")
        .query(&mut c)
        .unwrap();
    assert_eq!(len, 5);
}

// ── Lua scripting ──────────────────────────────────────────────────────

#[test]
#[ignore]
fn eval_return_string() {
    let mut c = sync_conn();
    let result: String = redis::cmd("EVAL")
        .arg("return 'hello'")
        .arg(0)
        .query(&mut c)
        .unwrap();
    assert_eq!(result, "hello");
}

#[test]
#[ignore]
fn eval_keys_and_argv() {
    let mut c = sync_conn();
    let _: () = c.set("compat:lua1", "world").unwrap();
    let result: String = redis::cmd("EVAL")
        .arg("return redis.call('GET', KEYS[1])")
        .arg(1)
        .arg("compat:lua1")
        .query(&mut c)
        .unwrap();
    assert_eq!(result, "world");
}

#[test]
#[ignore]
fn evalsha_after_script_load() {
    let mut c = sync_conn();
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 42")
        .query(&mut c)
        .unwrap();
    assert_eq!(sha.len(), 40, "SHA1 should be 40 hex chars");
    let result: i64 = redis::cmd("EVALSHA")
        .arg(&sha)
        .arg(0)
        .query(&mut c)
        .unwrap();
    assert_eq!(result, 42);
}

#[test]
#[ignore]
fn script_exists_and_flush() {
    let mut c = sync_conn();
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 1")
        .query(&mut c)
        .unwrap();
    let exists: Vec<bool> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&sha)
        .query(&mut c)
        .unwrap();
    assert_eq!(exists, vec![true]);
    let _: () = redis::cmd("SCRIPT").arg("FLUSH").query(&mut c).unwrap();
    let exists2: Vec<bool> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&sha)
        .query(&mut c)
        .unwrap();
    assert_eq!(exists2, vec![false]);
}

// ── ACL ────────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn acl_whoami() {
    let mut c = sync_conn();
    let user: String = redis::cmd("ACL").arg("WHOAMI").query(&mut c).unwrap();
    assert_eq!(user, "default");
}

#[test]
#[ignore]
fn acl_list_contains_default() {
    let mut c = sync_conn();
    let users: Vec<String> = redis::cmd("ACL").arg("LIST").query(&mut c).unwrap();
    assert!(
        users.iter().any(|u| u.contains("default")),
        "ACL LIST should contain 'default' user"
    );
}
