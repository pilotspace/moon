//! ADD task `sdk-wire-form-fixes` — GUARD 2: every public helper round-trips.
//!
//! The main repo's `tests/sdk_wire_forms.rs` sweeps command NAMES. This suite
//! exists because that sweep is structurally blind to the other half of a wire
//! form: the ARGUMENTS. A helper can send a command the server knows and still
//! be dead on arrival.
//!
//! That is not hypothetical, and it is not rare — two of the five helpers
//! removed in 0.3.0 were wrong this way, and the SECOND was found by this
//! suite on its first live run, after a by-hand audit had already cleared the
//! file:
//!
//! - `snapshot_at_packed` sent `TEMPORAL.SNAPSHOT_AT <packed_hlc>`, and the
//!   server's `validate_snapshot_at` rejects ANY argument.
//! - `release_snapshot` sent a bare `TEMPORAL.INVALIDATE`, which is the 3-arg
//!   entity form.
//!
//! Both named a command Moon really has, so the name sweep saw nothing wrong;
//! both answered `ERR wrong number of arguments` on every call ever made.
//!
//! # Coverage
//!
//! EVERY `pub async fn` in `sdk/rust/src/**`. Not a sample — `swf4b` counts
//! the crate's public async surface and fails if this file drives fewer, so
//! a helper added later cannot quietly go unguarded. "The rest are ordinary
//! Redis commands, they're surely fine" is exactly the reasoning that let
//! `release_snapshot` through review.
//!
//! # What counts as failure
//!
//! A PROTOCOL-level error — unknown command, wrong arity, syntax error. Not an
//! empty result, not a Nil, not "graph not found", not "no password is set":
//! those are legitimate answers that depend on server state or configuration,
//! and asserting on them would make this suite a brittle mirror of the
//! server's data model rather than a guard on its call surface.
//!
//! Run against a live server:
//! ```bash
//! MOON_TEST_URL=redis://127.0.0.1:6399 cargo test --test round_trip -- --ignored
//! ```

use moondb::{
    DistanceMetric, EntityType, MoonClient, NeighborDirection, Reducer, VectorIndexOptions,
};

fn test_url() -> String {
    std::env::var("MOON_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into())
}

async fn connect() -> MoonClient {
    MoonClient::connect(test_url())
        .await
        .expect("failed to connect to Moon server")
}

/// Turn a human-facing check label into the `Type::fn` key the coverage
/// assertion compares against.
///
/// Labels are written for whoever reads a failure — `mq.create`,
/// `txn_begin/2`. Coverage is tracked per OWNING TYPE, not per bare name,
/// because five sub-clients declare a `search` and three declare a `create`:
/// keyed by name alone, a future `SomeNewClient::search` would count as
/// covered because `VectorClient::search` happens to be driven. That is the
/// same silent-hole failure this whole suite exists to prevent.
fn qualify(label: &str) -> String {
    // Drop a trailing `/N` repeat marker.
    let label = label.split('/').next().unwrap_or(label);
    match label.split_once('.') {
        Some((prefix, name)) => {
            let ty = match prefix {
                "mq" => "MqClient",
                "graph" => "GraphClient",
                "vector" => "VectorClient",
                "text" => "TextClient",
                "session" => "SessionClient",
                "cache" => "CacheClient",
                "temporal" => "TemporalClient",
                "workspace" => "WorkspaceClient",
                // An unrecognised prefix must not silently resolve to
                // something plausible — let it fail the coverage diff loudly.
                other => other,
            };
            format!("{ty}::{name}")
        }
        None => format!("MoonClient::{label}"),
    }
}

/// Collects the helpers that came back with a protocol-level error.
#[derive(Default)]
struct Report {
    failures: Vec<String>,
    checked: std::collections::BTreeSet<String>,
}

impl Report {
    /// Record the outcome of one helper call.
    ///
    /// `Ok` passes. `Err` passes too UNLESS the message looks like the server
    /// rejecting the call shape itself — see the module docs for why the bar is
    /// there and not at "must succeed".
    fn check<T>(&mut self, helper: &str, r: Result<T, moondb::MoonError>) {
        self.checked.insert(qualify(helper));
        if let Err(e) = r {
            let msg = e.to_string().to_ascii_lowercase();
            // Matched as two loose tokens, NOT as the literal phrases
            // "unknown command" / "unknown subcommand": Moon names the family
            // in between (`ERR unknown MQ subcommand`, `ERR unknown FT.*
            // command`), so phrase matching silently passes those. That was a
            // real hole — the mutation check (`MQ CREATE <key>` reordered to
            // `MQ <key> CREATE`) survived this predicate until it was widened.
            let unknown_verb =
                msg.contains("unknown") && (msg.contains("command") || msg.contains("subcommand"));
            let protocol_level = unknown_verb
                || msg.contains("wrong number of arguments")
                || msg.contains("syntax error");
            if protocol_level {
                self.failures.push(format!("  {helper}  ->  {e}"));
            }
        }
    }

    fn assert_clean(&self) {
        assert!(
            self.failures.is_empty(),
            "{} of {} SDK helpers were rejected by the server at the protocol \
             level — the command name is fine, the ARGUMENTS are not:\n{}",
            self.failures.len(),
            self.checked.len(),
            self.failures.join("\n")
        );
    }
}

/// Every public helper, called with plausible arguments, against a live server.
#[tokio::test]
#[ignore = "requires live server"]
async fn swf4_every_public_helper_round_trips() {
    let names = drive_everything().await;
    // Re-run the count assertion's data through the same path so a failure
    // here names the helper, not just a total.
    assert!(!names.is_empty());
}

/// Drives the whole surface and returns the set of helper names exercised.
async fn drive_everything() -> std::collections::BTreeSet<String> {
    let mut c = connect().await;
    let mut r = Report::default();
    r.checked.insert(qualify("connect"));

    // ── connection / handshake ──────────────────────────────────────────────
    r.check("ping", c.ping().await);
    // Answers "no password is set" on an unauthenticated server — a
    // configuration answer, not a wire-form rejection, so it passes.
    r.check("auth", c.auth("swf4-not-a-real-password").await);
    r.check("client_info", c.client_info().await);

    // ── strings ─────────────────────────────────────────────────────────────
    r.check("set", c.set("swf4:k", "v").await);
    r.check("get", c.get::<_, String>("swf4:k").await);
    r.check("set_ex", c.set_ex("swf4:kex", "v", 100).await);
    r.check("pset_ex", c.pset_ex("swf4:kpx", "v", 100_000).await);
    r.check("set_nx", c.set_nx("swf4:knx", "v").await);
    r.check("mset", c.mset(&[("swf4:m1", "a"), ("swf4:m2", "b")]).await);
    r.check("mget", c.mget::<_, String>(&["swf4:m1", "swf4:m2"]).await);
    r.check("getset", c.getset::<_, _, String>("swf4:k", "v2").await);
    r.check("getdel", c.getdel::<_, String>("swf4:knx").await);
    r.check("append", c.append("swf4:k", "x").await);
    r.check("strlen", c.strlen("swf4:k").await);

    // ── counters ────────────────────────────────────────────────────────────
    r.check("incr", c.incr("swf4:n").await);
    r.check("incr_by", c.incr_by("swf4:n", 2).await);
    r.check("incr_by_float", c.incr_by_float("swf4:f", 1.5).await);
    r.check("decr", c.decr("swf4:n").await);
    r.check("decr_by", c.decr_by("swf4:n", 2).await);

    // ── key lifecycle ───────────────────────────────────────────────────────
    r.check("exists", c.exists("swf4:k").await);
    r.check("key_type", c.key_type("swf4:k").await);
    r.check("expire", c.expire("swf4:k", 100).await);
    r.check("pexpire", c.pexpire("swf4:k", 100_000).await);
    r.check("expire_at", c.expire_at("swf4:k", 4_102_444_800).await);
    r.check("ttl", c.ttl("swf4:k").await);
    r.check("pttl", c.pttl("swf4:k").await);
    r.check("persist", c.persist("swf4:k").await);
    r.check("rename", c.rename("swf4:m1", "swf4:m1b").await);
    r.check("rename_nx", c.rename_nx("swf4:m1b", "swf4:m1c").await);
    r.check("keys", c.keys::<_, String>("swf4:*").await);
    r.check(
        "scan_match",
        c.scan_match::<_, String>("swf4:*", 10, 0).await,
    );
    r.check("unlink", c.unlink("swf4:m2").await);
    r.check("del", c.del("swf4:f").await);

    // ── hashes ──────────────────────────────────────────────────────────────
    r.check("hset", c.hset("swf4:h", "f", "v").await);
    r.check(
        "hset_multiple",
        c.hset_multiple("swf4:h", &[("f2", "v2"), ("f3", "v3")])
            .await,
    );
    r.check("hget", c.hget::<_, _, String>("swf4:h", "f").await);
    r.check(
        "hmget",
        c.hmget::<_, _, String>("swf4:h", &["f", "f2"]).await,
    );
    r.check("hgetall", c.hgetall("swf4:h").await);
    r.check("hexists", c.hexists("swf4:h", "f").await);
    r.check("hlen", c.hlen("swf4:h").await);
    r.check("hkeys", c.hkeys::<_, String>("swf4:h").await);
    r.check("hvals", c.hvals::<_, String>("swf4:h").await);
    r.check("hincrby", c.hincrby("swf4:h", "cnt", 1).await);
    r.check("hincrbyfloat", c.hincrbyfloat("swf4:h", "fcnt", 1.5).await);
    r.check("hsetnx", c.hsetnx("swf4:h", "f4", "v4").await);
    r.check("hdel", c.hdel("swf4:h", "f3").await);

    // ── lists ───────────────────────────────────────────────────────────────
    r.check("lpush", c.lpush("swf4:l", "a").await);
    r.check("rpush", c.rpush("swf4:l", "b").await);
    r.check("llen", c.llen("swf4:l").await);
    r.check("lrange", c.lrange::<_, String>("swf4:l", 0, -1).await);
    r.check("lindex", c.lindex::<_, String>("swf4:l", 0).await);
    r.check("lset", c.lset("swf4:l", 0, "z").await);
    r.check("lpos", c.lpos("swf4:l", "z").await);
    r.check("lrem", c.lrem("swf4:l", 1, "z").await);
    r.check("ltrim", c.ltrim("swf4:l", 0, 10).await);
    r.check("lpop", c.lpop::<_, String>("swf4:l", None).await);
    r.check("rpop", c.rpop::<_, String>("swf4:l", Some(1)).await);

    // ── sets ────────────────────────────────────────────────────────────────
    r.check("sadd", c.sadd("swf4:s", "a").await);
    r.check("sadd/2", c.sadd("swf4:s2", "a").await);
    r.check("scard", c.scard("swf4:s").await);
    r.check("sismember", c.sismember("swf4:s", "a").await);
    r.check("smismember", c.smismember("swf4:s", &["a", "b"]).await);
    r.check("smembers", c.smembers::<_, String>("swf4:s").await);
    r.check("srandmember", c.srandmember::<_, String>("swf4:s", 1).await);
    r.check(
        "sinter",
        c.sinter::<_, String>(&["swf4:s", "swf4:s2"]).await,
    );
    r.check(
        "sunion",
        c.sunion::<_, String>(&["swf4:s", "swf4:s2"]).await,
    );
    r.check("sdiff", c.sdiff::<_, String>(&["swf4:s", "swf4:s2"]).await);
    r.check("spop", c.spop::<_, String>("swf4:s2").await);
    r.check("srem", c.srem("swf4:s", "a").await);

    // ── sorted sets ─────────────────────────────────────────────────────────
    r.check("zadd", c.zadd("swf4:z", 1.0, "m1").await);
    r.check("zadd/2", c.zadd("swf4:z", 2.0, "m2").await);
    r.check("zscore", c.zscore("swf4:z", "m1").await);
    r.check("zcard", c.zcard("swf4:z").await);
    r.check("zrank", c.zrank("swf4:z", "m1").await);
    r.check("zrevrank", c.zrevrank("swf4:z", "m1").await);
    r.check("zincrby", c.zincrby("swf4:z", 1.0, "m1").await);
    r.check("zrange", c.zrange::<_, String>("swf4:z", 0, -1).await);
    r.check("zrevrange", c.zrevrange::<_, String>("swf4:z", 0, -1).await);
    r.check(
        "zrangebyscore",
        c.zrangebyscore::<_, _, _, String>("swf4:z", "-inf", "+inf")
            .await,
    );
    r.check("zcount", c.zcount("swf4:z", "-inf", "+inf").await);
    r.check("zpopmin", c.zpopmin::<_, String>("swf4:z", 1).await);
    r.check("zpopmax", c.zpopmax::<_, String>("swf4:z", 1).await);
    r.check("zrem", c.zrem("swf4:z", "m1").await);

    // ── streams ─────────────────────────────────────────────────────────────
    let xid = c.xadd("swf4:x", &[("f", "v")]).await;
    let xid_str = xid.as_ref().map(|s| s.clone()).unwrap_or_default();
    r.check("xadd", xid);
    r.check("xlen", c.xlen("swf4:x").await);
    r.check("xrange", c.xrange("swf4:x", "-", "+").await);
    r.check("xrevrange", c.xrevrange("swf4:x", "+", "-").await);
    r.check("xtrim", c.xtrim("swf4:x", 10).await);
    r.check(
        "xdel",
        c.xdel("swf4:x", if xid_str.is_empty() { "0-1" } else { &xid_str })
            .await,
    );

    // ── pub/sub, scripting, pipelines ───────────────────────────────────────
    r.check("publish", c.publish("swf4:chan", "hello").await);
    let sha = c.script_load("return 1").await;
    let sha_str = sha.as_ref().map(|s| s.clone()).unwrap_or_default();
    r.check("script_load", sha);
    r.check("eval", c.eval::<i64>("return 1", &[], &[]).await);
    if !sha_str.is_empty() {
        r.check("evalsha", c.evalsha::<i64>(&sha_str, &[], &[]).await);
    } else {
        r.checked.insert(qualify("evalsha"));
    }
    let mut pipe = redis::pipe();
    pipe.cmd("PING");
    r.check("exec_pipeline", c.exec_pipeline(pipe).await);

    // ── transactions (the trio that a wrong measurement nearly deleted) ─────
    r.check("txn_begin", c.txn_begin().await);
    r.check("txn_commit", c.txn_commit().await);
    r.check("txn_begin/2", c.txn_begin().await);
    r.check("txn_abort", c.txn_abort().await);

    // ── admin / introspection ───────────────────────────────────────────────
    r.check("info", c.info(None).await);
    r.check("dbsize", c.dbsize().await);
    r.check("config_get", c.config_get("maxmemory").await);
    r.check("config_set", c.config_set("maxmemory", "0").await);
    r.check("slowlog_get", c.slowlog_get(Some(1)).await);
    r.check("bgrewriteaof", c.bgrewriteaof().await);
    r.check("bgsave", c.bgsave().await);

    // ── ACL ─────────────────────────────────────────────────────────────────
    r.check("acl_whoami", c.acl_whoami().await);
    r.check("acl_list", c.acl_list().await);
    r.check(
        "acl_setuser",
        c.acl_setuser(&["swf4user", "on", ">pw", "~swf4:*", "+@read"])
            .await,
    );
    r.check("acl_getuser", c.acl_getuser("swf4user").await);
    r.check("acl_deluser", c.acl_deluser("swf4user").await);
    // Both answer "not configured to use an ACL file" without an aclfile —
    // a configuration answer, not a wire-form rejection.
    r.check("acl_save", c.acl_save().await);
    r.check("acl_load", c.acl_load().await);

    // ── message queue ───────────────────────────────────────────────────────
    {
        let mut mq = c.mq();
        r.check("mq.create", mq.create("swf4:q", Some(3)).await);
        let pushed = mq.push("swf4:q", b"body").await;
        let pushed_id = pushed.as_ref().map(|s| s.clone()).unwrap_or_default();
        r.check("mq.push", pushed);
        r.check("mq.pop", mq.pop("swf4:q", 1).await);
        r.check(
            "mq.ack",
            mq.ack(
                "swf4:q",
                if pushed_id.is_empty() {
                    "0-1"
                } else {
                    &pushed_id
                },
            )
            .await,
        );
        r.check("mq.dlq_len", mq.dlq_len("swf4:q").await);
        r.check(
            "mq.trigger",
            mq.trigger("swf4:q", "swf4:cb", Some(100)).await,
        );
        r.check("mq.publish_txn", mq.publish_txn("swf4:q", b"body").await);
    }

    // ── graph ───────────────────────────────────────────────────────────────
    {
        let mut g = c.graph();
        r.check("graph.create", g.create("swf4g").await);
        r.check("graph.list", g.list().await);
        r.check("graph.info", g.info("swf4g").await);
        r.check(
            "graph.add_node",
            g.add_node("swf4g", "Person", &[("name", "alice")]).await,
        );
        r.check(
            "graph.add_edge",
            g.add_edge("swf4g", 1, 2, "KNOWS", 1.0, &[("since", "2020")])
                .await,
        );
        r.check("graph.query", g.query("swf4g", "RETURN 1").await);
        r.check("graph.ro_query", g.ro_query("swf4g", "RETURN 1").await);
        r.check("graph.query_raw", g.query_raw("swf4g", "RETURN 1").await);
        r.check(
            "graph.query_with_params",
            g.query_with_params("swf4g", "RETURN $id", r#"{"id":1}"#)
                .await,
        );
        r.check("graph.explain", g.explain("swf4g", "RETURN 1").await);
        r.check("graph.profile", g.profile("swf4g", "RETURN 1").await);
        r.check("graph.query_at", g.query_at("swf4g", "RETURN 1", 1).await);
        r.check(
            "graph.neighbors",
            g.neighbors("swf4g", 1, NeighborDirection::Out).await,
        );
        r.check(
            "graph.vsearch",
            g.vsearch("swf4g", 1, 2, 1, &[0.1, 0.2, 0.3, 0.4]).await,
        );
        r.check("graph.delete", g.delete("swf4g").await);
    }

    // ── temporal ────────────────────────────────────────────────────────────
    {
        let mut t = c.temporal();
        r.check("temporal.snapshot_at", t.snapshot_at().await);
        // The 3-arg entity form. `release_snapshot` sent this SAME command with
        // zero arguments and was rejected on every call — the defect this
        // suite found on its first live run, and the reason the coverage bar
        // is "every helper", not "every command name".
        r.check(
            "temporal.invalidate",
            t.invalidate("1", EntityType::Node, "swf4g").await,
        );
    }

    // ── vector ──────────────────────────────────────────────────────────────
    {
        let mut v = c.vector();
        let opts = VectorIndexOptions::new(4, DistanceMetric::Cosine);
        r.check("vector.create_index", v.create_index("swf4idx", opts).await);
        r.check("vector.list_indexes", v.list_indexes().await);
        r.check("vector.index_info", v.index_info("swf4idx").await);
        r.check("vector.compact", v.compact("swf4idx").await);
        r.check(
            "vector.config_set",
            v.config_set("swf4idx", "EF_RUNTIME", "50").await,
        );
        r.check(
            "vector.config_get",
            v.config_get("swf4idx", "EF_RUNTIME").await,
        );
        r.check(
            "vector.search",
            v.search("swf4idx", &[0.1, 0.2, 0.3, 0.4], 1).await,
        );
        r.check(
            "vector.search_opts",
            v.search_opts(
                "swf4idx",
                &[0.1, 0.2, 0.3, 0.4],
                1,
                "vec",
                Some(&["title"]),
                None,
            )
            .await,
        );
        r.check(
            "vector.search_raw",
            v.search_raw("swf4idx", "*", b"\x00\x00\x00\x00", 1, false)
                .await,
        );
        r.check(
            "vector.cache_search",
            v.cache_search(
                "swf4idx",
                "swf4cache:",
                &[0.1, 0.2, 0.3, 0.4],
                1,
                "vec",
                0.9,
                1,
            )
            .await,
        );
        r.check(
            "vector.recommend",
            v.recommend(
                "swf4idx",
                &["swf4:doc1"],
                Some(&["swf4:doc2"]),
                1,
                Some("vec"),
            )
            .await,
        );
        r.check(
            "vector.navigate",
            v.navigate("swf4idx", &[0.1, 0.2, 0.3, 0.4], 1, "vec", 2, 0.5)
                .await,
        );
        r.check("vector.drop_index", v.drop_index("swf4idx", true).await);
    }

    // ── text ────────────────────────────────────────────────────────────────
    {
        let mut t = c.text();
        r.check("text.search", t.search("swf4txt", "hello", 10, None).await);
        r.check(
            "text.hybrid_search",
            t.hybrid_search(
                "swf4txt",
                "hello",
                &[0.1, 0.2, 0.3, 0.4],
                "vec",
                None,
                1,
                [0.5, 0.3, 0.2],
                None,
            )
            .await,
        );
        r.check(
            "text.aggregate",
            t.aggregate(
                "swf4txt",
                "*",
                "@category",
                &[Reducer::Count],
                Some(("category", true)),
                Some(10),
            )
            .await,
        );
    }

    // ── session / cache ─────────────────────────────────────────────────────
    {
        let mut s = c.session();
        r.check(
            "session.search",
            s.search("swf4idx", "swf4:sess", &[0.1, 0.2, 0.3, 0.4], 1, "vec")
                .await,
        );
        r.check("session.history", s.history("swf4:sess", 1).await);
        r.check("session.expire", s.expire("swf4:sess", 100).await);
        r.check("session.clear", s.clear("swf4:sess").await);
    }
    {
        let mut ca = c.cache();
        r.check(
            "cache.lookup",
            ca.lookup(
                "swf4idx",
                "swf4cache:",
                &[0.1, 0.2, 0.3, 0.4],
                1,
                "vec",
                0.9,
                1,
            )
            .await,
        );
        r.check(
            "cache.store",
            ca.store(
                "swf4cache:1",
                &[0.1, 0.2, 0.3, 0.4],
                "answer",
                "vec",
                Some(60),
            )
            .await,
        );
        r.check("cache.scan_keys", ca.scan_keys("swf4:*", 10).await);
        r.check("cache.invalidate", ca.invalidate("swf4cache:1").await);
    }

    // ── workspace ───────────────────────────────────────────────────────────
    {
        let mut w = c.workspace();
        let created = w.create("swf4ws").await;
        let ws_id = created.as_ref().map(|s| s.clone()).unwrap_or_default();
        let ws_ref = if ws_id.is_empty() { "swf4ws" } else { &ws_id };
        r.check("workspace.create", created);
        r.check("workspace.list", w.list().await);
        r.check("workspace.info", w.info(ws_ref).await);
        r.check("workspace.auth", w.auth(ws_ref).await);
        r.check("workspace.drop", w.drop(ws_ref).await);
    }

    // ── stateful / destructive: kept last, and off the shared connection ────
    // `select` moves the db, `hello` renegotiates the protocol, and the flush
    // pair empties the keyspace. Any of them mid-suite would change what the
    // checks above are talking to, so they run on their own connection at the
    // end. They are still CHECKED — an unguarded helper is the whole defect
    // class this suite exists for.
    {
        let mut tail = connect().await;
        r.check("select", tail.select(1).await);
        r.check("hello", tail.hello(3).await);
        r.check("flushdb", tail.flushdb().await);
        r.check("flushall", tail.flushall().await);
    }
    {
        let timeout =
            MoonClient::connect_with_timeout(test_url(), std::time::Duration::from_secs(5)).await;
        r.check("connect_with_timeout", timeout.map(|_| ()));
    }

    r.assert_clean();
    r.checked
}

/// The coverage bar itself: this suite must drive EVERY public async helper.
///
/// Without this, `swf4` degrades silently — a helper added next quarter is
/// simply never called, and the suite still reports green over a shrinking
/// fraction of the surface. That is how the two removed temporal helpers
/// survived: nothing counted what was not being exercised.
#[tokio::test]
#[ignore = "requires live server"]
async fn swf4b_round_trip_covers_every_public_helper() {
    let driven = drive_everything().await;

    let mut declared = std::collections::BTreeSet::new();
    let src_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    collect_public_async_fns(&src_root, &mut declared);

    assert!(
        !declared.is_empty(),
        "found no `pub async fn` under {} — the scraper is broken, which would \
         make this assertion vacuous",
        src_root.display()
    );

    let missing: Vec<&String> = declared.difference(&driven).collect();
    assert!(
        missing.is_empty(),
        "{} of {} public async helpers are never driven by the round trip:\n  {}\n\n\
         Add a `r.check(\"<name>\", …)` call for each. A helper this suite does \
         not call is a helper whose wire form nothing verifies — which is how \
         `snapshot_at_packed` and `release_snapshot` shipped broken.",
        missing.len(),
        declared.len(),
        missing
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\n  ")
    );
}

/// Collect every `pub async fn` declared under `dir`, as `Type::fn`.
///
/// Attribution is by the most recent enclosing `impl <Type>` line. That is a
/// line-scanner, not a parser, which is adequate here because this crate
/// declares every helper inside a plain inherent `impl` at column 0 — and if
/// that ever stops being true, the assertion in `swf4b` fails loudly with an
/// unrecognised type rather than quietly dropping the helper.
fn collect_public_async_fns(dir: &std::path::Path, out: &mut std::collections::BTreeSet<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_public_async_fns(&path, out);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let Ok(src) = std::fs::read_to_string(&path) else {
            continue;
        };
        let mut current_type = String::new();
        for line in src.lines() {
            let trimmed = line.trim_start();
            if let Some(rest) = trimmed.strip_prefix("impl ") {
                // Skip generics on the impl itself (`impl<'a> Foo`), then take
                // the type name up to a space, `<`, or `{`.
                let rest = rest.strip_prefix('<').map_or(rest, |r| {
                    r.split_once('>')
                        .map(|(_, tail)| tail.trim_start())
                        .unwrap_or(r)
                });
                let ty: String = rest
                    .chars()
                    .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
                    .collect();
                if !ty.is_empty() {
                    current_type = ty;
                }
            }
            let Some(rest) = trimmed.strip_prefix("pub async fn ") else {
                continue;
            };
            let name: String = rest
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
                .collect();
            if !name.is_empty() && !current_type.is_empty() {
                out.insert(format!("{current_type}::{name}"));
            }
        }
    }
}
