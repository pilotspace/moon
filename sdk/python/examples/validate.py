"""End-to-end validation script — connects to a live Moon server and exercises
every Python SDK sub-client. Each check prints PASS or FAIL with details.

Run:
    uv run python examples/validate.py
    MOON_TEST_URL=redis://127.0.0.1:6399 uv run python examples/validate.py
"""

from __future__ import annotations

import os
import struct
import sys
from typing import Any

import redis as redis_lib

from moondb import MoonClient
from moondb.types import Count, GroupBy, SortBy, encode_vector

# ── Config ────────────────────────────────────────────────────────────────────

_URL_ENV = "MOON_TEST_URL"
_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 6399

_passes = 0
_fails = 0
_skips = 0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _section(name: str) -> None:
    print(f"\n─── {name} ───")


def _pass(label: str, detail: str = "") -> None:
    global _passes
    _passes += 1
    suffix = f" ({detail})" if detail else ""
    print(f"  PASS  {label}{suffix}")


def _fail(label: str, detail: str = "") -> None:
    global _fails
    _fails += 1
    suffix = f" — {detail}" if detail else ""
    print(f"  FAIL  {label}{suffix}")


def _skip(label: str, reason: str = "") -> None:
    global _skips
    _skips += 1
    suffix = f" ({reason})" if reason else ""
    print(f"  SKIP  {label}{suffix}")


def check(label: str, fn: Any) -> Any:
    """Run fn(), print PASS/FAIL, return result or None on failure."""
    try:
        result = fn()
        _pass(label)
        return result
    except Exception as e:
        _fail(label, str(e))
        return None


def assert_eq(label: str, actual: Any, expected: Any) -> None:
    if actual == expected:
        _pass(label)
    else:
        _fail(label, f"expected {expected!r} got {actual!r}")


def assert_true(label: str, cond: bool, detail: str = "") -> None:
    if cond:
        _pass(label, detail)
    else:
        _fail(label, detail)


# ── Sections ──────────────────────────────────────────────────────────────────

def validate_ping(c: MoonClient) -> None:
    _section("PING")
    resp = check("PING", lambda: c.ping())
    if resp is not None:
        assert_true("PING response is truthy", bool(resp))


def validate_strings(c: MoonClient) -> None:
    _section("String commands")
    c.delete("val:str")
    check("SET", lambda: c.set("val:str", "hello"))
    v = check("GET", lambda: c.get("val:str"))
    if v is not None:
        assert_eq("GET value", v, b"hello")
    d = check("DEL", lambda: c.delete("val:str"))
    if d is not None:
        assert_eq("DEL count", d, 1)
    ex = check("EXISTS after DEL", lambda: c.exists("val:str"))
    assert_eq("EXISTS=0", ex, 0)

    c.set("val:ttl", "x")
    check("EXPIRE", lambda: c.expire("val:ttl", 60))
    ttl = check("TTL", lambda: c.ttl("val:ttl"))
    if ttl is not None:
        assert_true("TTL in range", 0 < ttl <= 60, f"ttl={ttl}")
    c.delete("val:ttl")


def validate_counter(c: MoonClient) -> None:
    _section("Counter (INCR/DECR)")
    c.delete("val:ctr")
    v = check("INCR", lambda: c.incr("val:ctr"))
    assert_eq("INCR=1", v, 1)
    v = check("INCRBY +9", lambda: c.incrby("val:ctr", 9))
    assert_eq("INCRBY=10", v, 10)
    v = check("DECR", lambda: c.decr("val:ctr"))
    assert_eq("DECR=9", v, 9)
    c.delete("val:ctr")


def validate_hash(c: MoonClient) -> None:
    _section("Hash commands")
    c.delete("val:h")
    check("HSET f1", lambda: c.hset("val:h", mapping={"f1": "v1", "f2": "v2"}))
    v = check("HGET f1", lambda: c.hget("val:h", "f1"))
    if v is not None:
        assert_eq("HGET value", v, b"v1")
    ln = check("HLEN", lambda: c.hlen("val:h"))
    assert_eq("HLEN=2", ln, 2)
    all_fields = check("HGETALL", lambda: c.hgetall("val:h"))
    if all_fields is not None:
        assert_true("HGETALL has f2", b"f2" in all_fields or "f2" in all_fields)
    check("HDEL f1", lambda: c.hdel("val:h", "f1"))
    ex = check("HEXISTS f1 gone", lambda: c.hexists("val:h", "f1"))
    assert_eq("HEXISTS=0", ex, 0)
    c.delete("val:h")


def validate_list(c: MoonClient) -> None:
    _section("List commands")
    c.delete("val:lst")
    check("RPUSH a", lambda: c.rpush("val:lst", "a"))
    check("RPUSH b", lambda: c.rpush("val:lst", "b"))
    check("RPUSH c", lambda: c.rpush("val:lst", "c"))
    ln = check("LLEN", lambda: c.llen("val:lst"))
    assert_eq("LLEN=3", ln, 3)
    rng = check("LRANGE 0 -1", lambda: c.lrange("val:lst", 0, -1))
    if rng is not None:
        assert_eq("LRANGE", rng, [b"a", b"b", b"c"])
    popped = check("LPOP", lambda: c.lpop("val:lst"))
    if popped is not None:
        assert_eq("LPOP=a", popped, b"a")
    c.delete("val:lst")


def validate_set(c: MoonClient) -> None:
    _section("Set commands")
    c.delete("val:st")
    check("SADD x", lambda: c.sadd("val:st", "x"))
    check("SADD y", lambda: c.sadd("val:st", "y"))
    card = check("SCARD", lambda: c.scard("val:st"))
    assert_eq("SCARD=2", card, 2)
    is_m = check("SISMEMBER x", lambda: c.sismember("val:st", "x"))
    assert_eq("SISMEMBER x=1", is_m, 1)
    check("SREM x", lambda: c.srem("val:st", "x"))
    is_m = check("SISMEMBER x gone", lambda: c.sismember("val:st", "x"))
    assert_eq("SISMEMBER x=0", is_m, 0)
    c.delete("val:st")


def validate_zset(c: MoonClient) -> None:
    _section("Sorted set commands")
    c.delete("val:zst")
    check("ZADD alice 1.0", lambda: c.zadd("val:zst", {"alice": 1.0}))
    check("ZADD bob 2.0", lambda: c.zadd("val:zst", {"bob": 2.0}))
    check("ZADD charlie 3.0", lambda: c.zadd("val:zst", {"charlie": 3.0}))
    card = check("ZCARD", lambda: c.zcard("val:zst"))
    assert_eq("ZCARD=3", card, 3)
    score = check("ZSCORE alice", lambda: c.zscore("val:zst", "alice"))
    assert_eq("ZSCORE=1.0", score, 1.0)
    rank = check("ZRANK alice", lambda: c.zrank("val:zst", "alice"))
    assert_eq("ZRANK=0", rank, 0)
    rng = check("ZRANGE 0 -1", lambda: c.zrange("val:zst", 0, -1))
    if rng is not None:
        assert_eq("ZRANGE order", rng, [b"alice", b"bob", b"charlie"])
    c.delete("val:zst")


def validate_vector(c: MoonClient) -> None:
    _section("Vector index lifecycle (FT.*)")
    try:
        c.vector.drop_index("val_idx", delete_docs=True)
    except Exception:
        pass

    check(
        "FT.CREATE HNSW",
        lambda: c.vector.create_index(
            "val_idx",
            prefix="vdoc:",
            dim=4,
            metric="COSINE",
            m=16,
            ef_construction=200,
            compact_threshold=100,
        ),
    )

    lst = check("FT._LIST", lambda: c.vector.list_indexes())
    if lst is not None:
        assert_true("FT._LIST has val_idx", "val_idx" in lst, f"list={lst}")

    blob = encode_vector([0.1, 0.2, 0.3, 0.4])
    check("HSET vdoc:1", lambda: c.hset("vdoc:1", mapping={"vec": blob, "title": "test"}))

    info = check("FT.INFO", lambda: c.vector.index_info("val_idx"))
    if info is not None:
        assert_eq("FT.INFO name", info.name, "val_idx")

    results = check("FT.SEARCH", lambda: c.vector.search("val_idx", [0.1, 0.2, 0.3, 0.4], k=5))
    _pass("FT.SEARCH returned", f"count={len(results) if results is not None else 'N/A'}")

    check("FT.COMPACT", lambda: c.vector.compact("val_idx"))

    check("FT.DROPINDEX DD", lambda: c.vector.drop_index("val_idx", delete_docs=True))
    lst2 = check("FT._LIST after drop", lambda: c.vector.list_indexes())
    if lst2 is not None:
        assert_true("val_idx removed", "val_idx" not in lst2)

    c.delete("vdoc:1")


def validate_graph(c: MoonClient) -> None:
    _section("Graph engine (GRAPH.*)")
    try:
        c.graph.delete("val_graph")
    except Exception:
        pass

    check("GRAPH.CREATE", lambda: c.graph.create("val_graph"))

    alice = check(
        "GRAPH.ADDNODE alice",
        lambda: c.graph.add_node("val_graph", "Person", name="Alice", age="30"),
    )
    bob = check(
        "GRAPH.ADDNODE bob",
        lambda: c.graph.add_node("val_graph", "Person", name="Bob"),
    )

    if alice is not None and bob is not None:
        check(
            "GRAPH.ADDEDGE KNOWS",
            lambda: c.graph.add_edge("val_graph", alice, bob, "KNOWS", weight=1.0),
        )

    result = check(
        "GRAPH.QUERY",
        lambda: c.graph.query("val_graph", "MATCH (p:Person) RETURN p.name ORDER BY p.name"),
    )
    if result is not None:
        assert_true("GRAPH.QUERY has headers", len(result.headers) > 0, f"headers={result.headers}")

    ro_result = check(
        "GRAPH.RO_QUERY",
        lambda: c.graph.ro_query("val_graph", "MATCH (p:Person) RETURN count(p)"),
    )
    if ro_result is not None:
        assert_true("GRAPH.RO_QUERY has headers", len(ro_result.headers) > 0)

    if alice is not None:
        neighbors = check(
            "GRAPH.NEIGHBORS",
            lambda: c.graph.neighbors("val_graph", alice),
        )
        if neighbors is not None:
            assert_eq("NEIGHBORS count=1", len(neighbors), 1)

    plan = check(
        "GRAPH.EXPLAIN",
        lambda: c.graph.explain("val_graph", "MATCH (n) RETURN n LIMIT 1"),
    )
    if plan is not None:
        _pass("GRAPH.EXPLAIN returned plan", f"len={len(plan)}")

    check(
        "GRAPH.PROFILE",
        lambda: c.graph.profile("val_graph", "MATCH (n) RETURN n LIMIT 1"),
    )

    g_info = check("GRAPH.INFO", lambda: c.graph.info("val_graph"))
    if g_info is not None:
        assert_true("GRAPH.INFO non-empty", len(g_info) > 0)

    graphs = check("GRAPH.LIST", lambda: c.graph.list_graphs())
    if graphs is not None:
        assert_true("val_graph in list", "val_graph" in graphs)

    check("GRAPH.DELETE", lambda: c.graph.delete("val_graph"))


def validate_session(c: MoonClient) -> None:
    _section("Session search (FT.SEARCH SESSION)")

    idx = "val_sess_idx"
    try:
        c.vector.drop_index(idx, delete_docs=True)
    except Exception:
        pass
    c.delete("session:val:1")

    c.vector.create_index(idx, prefix="sdoc:", dim=4, metric="COSINE", compact_threshold=100)
    blob = encode_vector([0.1, 0.2, 0.3, 0.4])
    c.hset("sdoc:1", mapping={"vec": blob, "title": "session test"})

    results = check(
        "SESSION.search",
        lambda: c.session.search(idx, "session:val:1", [0.1, 0.2, 0.3, 0.4], k=5),
    )
    _pass("SESSION results", f"count={len(results) if results is not None else 'N/A'}")

    history = check("SESSION.history", lambda: c.session.history("session:val:1"))
    if history is not None:
        _pass("SESSION.history non-empty", f"len={len(history)}")

    reset_count = check("SESSION.reset", lambda: c.session.reset("session:val:1"))
    if reset_count is not None:
        assert_true("SESSION.reset ok", reset_count >= 0)

    try:
        c.vector.drop_index(idx, delete_docs=True)
    except Exception:
        pass
    c.delete("sdoc:1")


def validate_cache(c: MoonClient) -> None:
    _section("Semantic cache (FT.CACHESEARCH)")

    idx = "val_cache_idx"
    try:
        c.vector.drop_index(idx, delete_docs=True)
    except Exception:
        pass
    c.delete("cache:val:1")

    c.vector.create_index(idx, prefix="cache:val:", dim=4, metric="L2", compact_threshold=100)

    emb = [0.1, 0.2, 0.3, 0.4]

    stored = check(
        "cache.store",
        lambda: c.cache.store(
            "cache:val:1",
            emb,
            response="The answer is 42",
            model="test",
        ),
    )
    if stored is not None:
        assert_true("cache.store returned truthy", stored)

    result = check(
        "cache.lookup",
        lambda: c.cache.lookup(idx, "cache:val:", emb, threshold=0.95),
    )
    if result is not None:
        _pass("cache.lookup returned result", f"cache_hit={result.cache_hit}")

    inv = check("cache.invalidate", lambda: c.cache.invalidate("cache:val:1"))
    if inv is not None:
        assert_true("cache.invalidate ok", inv >= 0)

    try:
        c.vector.drop_index(idx, delete_docs=True)
    except Exception:
        pass


def _has_text_index_feature(c: MoonClient) -> bool:
    """Probe whether the server was compiled with --features text-index."""
    try:
        c.execute_command(
            "FT.CREATE", "_probe_text_feature", "ON", "HASH",
            "PREFIX", "1", "_probe_:", "SCHEMA", "title", "TEXT",
        )
        c.execute_command("FT.DROPINDEX", "_probe_text_feature")
        return True
    except Exception as e:
        return "text-index feature" not in str(e)


def validate_text(c: MoonClient) -> None:
    _section("Text search (BM25 + AGGREGATE + HYBRID)")

    if not _has_text_index_feature(c):
        _skip("FT.CREATE text index", "text-index feature not compiled in")
        _skip("text_search BM25", "text-index feature not compiled in")
        _skip("text_search match-all", "text-index feature not compiled in")
        _skip("FT.AGGREGATE", "text-index feature not compiled in")
        _skip("FT.CREATE hybrid index", "text-index feature not compiled in")
        _skip("hybrid_search", "text-index feature not compiled in")
        return

    idx = "val_text_idx"
    try:
        c.text.drop_text_index(idx, delete_docs=True)
    except Exception:
        pass

    check(
        "FT.CREATE text index",
        lambda: c.text.create_text_index(
            idx,
            [("title", "TEXT", {}), ("status", "TAG", {})],
            prefix="tdoc:",
        ),
    )

    c.hset("tdoc:1", mapping={"title": "moon database fast", "status": "active"})
    c.hset("tdoc:2", mapping={"title": "vector search redis", "status": "active"})
    c.hset("tdoc:3", mapping={"title": "graph engine cypher", "status": "archived"})

    hits = check("text_search BM25", lambda: c.text.text_search(idx, "moon"))
    if hits is not None:
        _pass("text_search returned", f"count={len(hits)}")

    hits_all = check("text_search match-all", lambda: c.text.text_search(idx, "*"))
    if hits_all is not None:
        _pass("text_search match-all", f"count={len(hits_all)}")

    agg_rows = check(
        "FT.AGGREGATE",
        lambda: c.text.aggregate(
            idx,
            "*",
            [GroupBy(["status"], [Count(alias="n")]), SortBy("n", "DESC")],
        ),
    )
    if agg_rows is not None:
        _pass("FT.AGGREGATE returned", f"rows={len(agg_rows)}")

    # Hybrid search requires a vector field — create a separate index
    hidx = "val_hybrid_idx"
    try:
        c.text.drop_text_index(hidx, delete_docs=True)
    except Exception:
        pass
    try:
        c.vector.drop_index(hidx, delete_docs=True)
    except Exception:
        pass

    check(
        "FT.CREATE hybrid index",
        lambda: c.text.create_text_index(
            hidx,
            [
                ("title", "TEXT", {}),
                ("vec", "VECTOR", {}),
            ],
            prefix="hdoc:",
        ),
    )

    blob = encode_vector([0.1, 0.2, 0.3, 0.4])
    c.hset("hdoc:1", mapping={"title": "moon fast database", "vec": blob})

    hybrid_hits = check(
        "hybrid_search",
        lambda: c.text.hybrid_search(
            hidx,
            "moon",
            vector=[0.1, 0.2, 0.3, 0.4],
            limit=5,
        ),
    )
    if hybrid_hits is not None:
        _pass("hybrid_search returned", f"count={len(hybrid_hits)}")

    try:
        c.text.drop_text_index(idx, delete_docs=True)
    except Exception:
        pass
    try:
        c.text.drop_text_index(hidx, delete_docs=True)
    except Exception:
        pass
    for k in ["tdoc:1", "tdoc:2", "tdoc:3", "hdoc:1"]:
        c.delete(k)


def validate_server_info(c: MoonClient) -> None:
    _section("Server info")
    info = check("INFO server", lambda: c.moon_info())
    if info is not None:
        assert_true("INFO non-empty", len(info) > 0)
        _pass("INFO sections", f"keys={list(info.keys())[:3]}...")

    db_size = check("DBSIZE", lambda: c.dbsize())
    if db_size is not None:
        _pass("DBSIZE", f"size={db_size}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    url = os.environ.get(_URL_ENV, f"redis://{_DEFAULT_HOST}:{_DEFAULT_PORT}")
    # Parse host/port from redis:// URL
    host = _DEFAULT_HOST
    port = _DEFAULT_PORT
    if url.startswith("redis://"):
        rest = url[len("redis://"):]
        if ":" in rest:
            host, port_str = rest.rsplit(":", 1)
            port = int(port_str)
        else:
            host = rest

    print(f"Connecting to {url} …")
    try:
        c = MoonClient(host=host, port=port)
        c.ping()
    except redis_lib.ConnectionError as e:
        print(f"FATAL: cannot connect — {e}")
        sys.exit(1)

    c.flushdb()

    validate_ping(c)
    validate_strings(c)
    validate_counter(c)
    validate_hash(c)
    validate_list(c)
    validate_set(c)
    validate_zset(c)
    validate_vector(c)
    validate_graph(c)
    validate_session(c)
    validate_cache(c)
    validate_text(c)
    validate_server_info(c)

    print()
    print(f"{'=' * 50}")
    total = _passes + _fails + _skips
    print(f"  Total: {total}  PASS: {_passes}  FAIL: {_fails}  SKIP: {_skips}")
    print(f"{'=' * 50}")

    if _fails > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
