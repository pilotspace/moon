#!/usr/bin/env python3
"""INT-03 fixture seeder: deterministic KV + vectors + graph payload.

Uses redis-py + raw HTTP for FT.CREATE/GRAPH.QUERY. Idempotent — safe to
re-run. Deterministic: numpy.random.default_rng(seed=0xC0DE).
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.request

import numpy as np
import redis


def post_command(admin_port: int, cmd: str, args: list[str]) -> dict:
    body = json.dumps({"cmd": cmd, "args": args, "db": 0}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{admin_port}/api/v1/command",
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def seed_kv(r: redis.Redis, count: int) -> None:
    pipe = r.pipeline(transaction=False)
    for i in range(count):
        ns = ["user", "session", "cache", "metric"][i % 4]
        pipe.set(f"{ns}:{i // 100}:{i}", "x" * (32 + (i % 256)))
        if i % 10 == 0:
            pipe.expire(f"{ns}:{i // 100}:{i}", 3600)
        if i % 500 == 499:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()
    print(f"[seed] KV: {count} keys inserted", flush=True)


def seed_vectors(r: redis.Redis, admin_port: int, count: int, dim: int = 128) -> None:
    # FT.CREATE is RESP — use admin REST to avoid client lib quirks.
    post_command(
        admin_port,
        "FT.CREATE",
        [
            "bench:vec",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "vec:",
            "SCHEMA",
            "embedding",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            str(dim),
            "DISTANCE_METRIC",
            "COSINE",
        ],
    )
    rng = np.random.default_rng(0xC0DE)
    vectors = rng.standard_normal((count, dim)).astype(np.float32)
    for i in range(count):
        r.hset(f"vec:{i}", mapping={"embedding": vectors[i].tobytes(), "label": f"L{i % 10}"})
        if i % 1000 == 999:
            print(f"[seed] vectors: {i + 1}/{count}", flush=True)
    print(f"[seed] vectors: {count} hashes inserted (index bench:vec)", flush=True)


def seed_graph(admin_port: int, nodes: int) -> None:
    # Batch UNWIND to cut roundtrips.
    BATCH = 500
    for start in range(0, nodes, BATCH):
        end = min(start + BATCH, nodes)
        ids = list(range(start, end))
        props = [{"id": i, "label": f"N{i % 16}"} for i in ids]
        post_command(
            admin_port,
            "GRAPH.QUERY",
            [
                "bench",
                "UNWIND $rows AS r CREATE (n:Node {id: r.id, label: r.label})",
                "params",
                json.dumps({"rows": props}),
            ],
        )
    # Sparse edges: each node connects to 3 successors.
    for start in range(0, nodes, BATCH):
        end = min(start + BATCH, nodes)
        edges = [{"a": i, "b": (i + off) % nodes} for i in range(start, end) for off in (1, 3, 7)]
        post_command(
            admin_port,
            "GRAPH.QUERY",
            [
                "bench",
                "UNWIND $rows AS r MATCH (x:Node {id:r.a}),(y:Node {id:r.b}) CREATE (x)-[:E]->(y)",
                "params",
                json.dumps({"rows": edges}),
            ],
        )
    print(f"[seed] graph: {nodes} nodes + {nodes * 3} edges", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--resp-port", type=int, default=6399)
    ap.add_argument("--admin-port", type=int, default=9100)
    ap.add_argument("--kv-count", type=int, default=2000)
    ap.add_argument("--vector-count", type=int, default=50_000)
    ap.add_argument("--graph-nodes", type=int, default=10_000)
    args = ap.parse_args()

    r = redis.Redis(host="127.0.0.1", port=args.resp_port, decode_responses=False)
    r.ping()

    seed_kv(r, args.kv_count)
    seed_vectors(r, args.admin_port, args.vector_count)
    seed_graph(args.admin_port, args.graph_nodes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
