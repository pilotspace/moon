#!/usr/bin/env python3
"""Smoke-test every Moon flow the moon-agent skill documents.

    python smoke.py [port]        # default 6399

Exits 0 if every flow passes, 1 otherwise. Flows that depend on server
configuration (vector FILTER needs --shards 1) are reported as SKIP, not FAIL,
when the running instance cannot support them.
"""
import math
import random
import sys
import time

try:
    from moondb import MoonClient, encode_vector
except ImportError:
    sys.exit("moondb not installed — `pip install moondb` or `pip install -e sdk/python`")

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 6399
DIM = 8
passed, failed, skipped = [], [], []


def check(name, fn):
    try:
        print(f"  PASS {name}: {fn()}")
        passed.append(name)
    except Skip as s:
        print(f"  SKIP {name}: {s}")
        skipped.append(name)
    except Exception as e:
        print(f"  FAIL {name}: {e.__class__.__name__}: {e}")
        failed.append((name, repr(e)))


class Skip(Exception):
    pass


def vec(seed):
    random.seed(seed)
    v = [random.gauss(0, 1) for _ in range(DIM)]
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


c = MoonClient(host="127.0.0.1", port=PORT, socket_timeout=5, socket_connect_timeout=5)

print(f"\n[A] operate / health  (port {PORT})")
check("ping", lambda: c.ping())
check("is moon (not redis)", lambda: c.moon_info()["moon_version"])
check("dbsize", lambda: c.dbsize())

print("\n[B] KV + TTL")
check("set/get", lambda: (c.set("smoke:step", "planning"), c.get("smoke:step"))[1])
check("ttl", lambda: (c.set("smoke:lease", "held", ex=60), c.ttl("smoke:lease"))[1])
check("hash ctx", lambda: (c.hset("smoke:ctx", mapping={"goal": "ship"}), c.hgetall("smoke:ctx"))[1])

print("\n[C] vector search")


def mk_index():
    try:
        c.vector.drop_index("smokemem")
    except Exception:
        pass
    return c.vector.create_index("smokemem", prefix="smem:", field_name="vec",
                                 dim=DIM, metric="COSINE")


def load():
    for i in range(20):
        c.hset(f"smem:{i}", mapping={"vec": encode_vector(vec(i)),
                                     "text": f"memory {i}",
                                     "kind": "note" if i % 2 else "fact"})
    return "20 docs"


def search():
    r = c.vector.search("smokemem", vec(3), k=3, return_fields=["text"])
    assert r, "empty result set"
    assert r[0].key == "smem:3", f"self-match not rank-1, got {r[0].key}"
    return [(x.key, round(x.score, 4)) for x in r]


def filt():
    try:
        r = c.vector.search("smokemem", vec(3), k=5, filter_expr="@kind:{note}")
    except Exception as e:
        if "multi-shard" in str(e):
            raise Skip("FILTER needs --shards 1 (instance is multi-shard)")
        raise
    return [x.key for x in r]


check("create_index", mk_index)
check("load vectors", load)
check("num_docs", lambda: c.vector.index_info("smokemem").num_docs)
check("search (score ascending, self rank-1)", search)
check("filter_expr", filt)

print("\n[D] session-aware retrieval")
check("session.search", lambda: len(c.session.search("smokemem", "smoke:sess", vec(5), k=3)))
check("session.history", lambda: c.session.history("smoke:sess"))
check("session.reset", lambda: c.session.reset("smoke:sess"))

print("\n[E] semantic cache")
# NOTE: the cache key must fall under the index PREFIX ("smem:") or it is never indexed.
CACHE_KEY, CACHE_PREFIX = "smem:cache:q1", "smem:cache:"


def cache_hit_semantics():
    """An exact-match query must HIT and an unrelated one must MISS.

    Regression guard for moon#748, fixed in 0.8.8: `cache_hit` used to be
    inverted on COSINE/INNER_PRODUCT indexes (true when the nearest entry was
    FARTHER than the threshold). Both directions are asserted, because checking
    only the hit would still pass if the predicate were reversed AND the
    threshold happened to admit everything.

    This fails loudly rather than skipping. A silent skip is exactly how the
    original bug stayed invisible.
    """
    c.cache.store(CACHE_KEY, vec(7), answer="42", ttl=300)
    time.sleep(0.4)
    r = c.cache.lookup("smokemem", CACHE_PREFIX, vec(7), threshold=0.5)
    top = round(r.results[0].score, 4) if r.results else None
    if not r.cache_hit:
        raise AssertionError(
            f"exact-match query MISSED (top score={top}) -- moon#748 regression: "
            f"the threshold predicate is reversed for this index's metric")
    far = c.cache.lookup("smokemem", CACHE_PREFIX, vec(9999), threshold=0.5)
    if far.cache_hit:
        raise AssertionError(
            "unrelated query reported a cache HIT -- moon#748 regression: "
            "the threshold predicate is reversed for this index's metric")
    return f"hit, top score={top}"


check("cache.store", lambda: c.cache.store(CACHE_KEY, vec(7), answer="42", ttl=300))
check("cache.lookup hit/miss direction (moon#748)", cache_hit_semantics)
check("cache.invalidate", lambda: c.cache.invalidate(CACHE_KEY))

print("\n[F] full-text")


def mk_text():
    try:
        c.text.drop_text_index("smokedocs")
    except Exception:
        pass
    return c.text.create_text_index("smokedocs",
                                    [("body", "TEXT", {}), ("kind", "TAG", {"SORTABLE": True})],
                                    prefix="sdoc:")


def text_search():
    c.hset("sdoc:1", mapping={"body": "moon is a redis compatible server", "kind": "note"})
    c.hset("sdoc:2", mapping={"body": "vector search with hnsw graphs", "kind": "note"})
    time.sleep(0.5)                      # indexing is async
    r = c.text.text_search("smokedocs", "vector", limit=5)
    assert r, "no text hits"
    return [h.id for h in r]


check("create_text_index (tuple schema)", mk_text)
check("text_search", text_search)

print("\n[G] graph")


def mk_graph():
    try:
        c.graph.delete("smokekg")
    except Exception:
        pass
    return c.graph.create("smokekg")


def graph_rt():
    c.graph.query("smokekg", "CREATE (a:Task {name:'ship'})-[:BLOCKS]->(b:Task {name:'test'}) RETURN a.name")
    r = c.graph.query("smokekg", "MATCH (a:Task)-[:BLOCKS]->(b:Task) RETURN a.name, b.name")
    assert r.rows, "no graph rows"
    return r.rows


check("graph.create", mk_graph)
check("graph cypher roundtrip", graph_rt)

print("\n[H] pub/sub")


def pubsub():
    p = c.pubsub()
    p.subscribe("smoke:events")
    p.get_message(timeout=1)             # subscribe confirmation
    c.publish("smoke:events", "step-done")
    for _ in range(5):
        m = p.get_message(timeout=1)
        if m and m["type"] == "message":
            p.close()
            return m["data"]
    p.close()
    raise AssertionError("no message received")


check("pubsub roundtrip", pubsub)

print(f"\n===== {len(passed)} passed, {len(failed)} failed, {len(skipped)} skipped =====")
for n, e in failed:
    print(f"  FAILED {n}: {e}")
sys.exit(1 if failed else 0)
