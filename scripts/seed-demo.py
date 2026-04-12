#!/usr/bin/env python3
"""Seed Moon with demo data for all data types: vectors + graph."""
import redis
import numpy as np
import struct

r = redis.Redis(port=6399, decode_responses=False)

# ── Vector index + 200 vectors ──
try:
    r.execute_command(
        "FT.CREATE", "embeddings", "ON", "HASH", "PREFIX", "1", "doc:",
        "SCHEMA", "v", "VECTOR", "HNSW", "6", "DIM", "384", "TYPE",
        "FLOAT32", "DISTANCE_METRIC", "COSINE",
    )
    print("Vector index created")
except Exception as e:
    print(f"Index: {e}")

np.random.seed(42)
categories = ["science", "technology", "business", "health", "sports"]
titles = {
    "science": ["Quantum Computing", "CRISPR Therapy", "Mars Discovery", "Dark Matter"],
    "technology": ["AI Training", "Rust Safety", "Edge Computing", "WebAssembly"],
    "business": ["Startup Funding", "Remote Work", "Supply Chain AI", "Fintech"],
    "health": ["mRNA Vaccine", "Digital Health", "Mental Health AI", "Nutrition"],
    "sports": ["Olympic Records", "F1 Aero", "Tennis AI Coach", "Cycling Tech"],
}
for i in range(200):
    vec = np.random.randn(384).astype(np.float32)
    vec /= np.linalg.norm(vec)
    blob = struct.pack(f"384f", *vec)
    cat = categories[i % len(categories)]
    title = titles[cat][i % len(titles[cat])]
    r.hset(f"doc:{i}", mapping={
        "v": blob,
        "category": cat,
        "title": f"{title} #{i}",
        "score": str(round(np.random.uniform(0.5, 1.0), 3)),
    })
    if (i + 1) % 50 == 0:
        print(f"  vectors: {i+1}/200")

print("200 vectors seeded")

# ── Graph ──
# GRAPH.ADDNODE returns the node ID; GRAPH.ADDEDGE needs numeric IDs.
try:
    r.execute_command("GRAPH.DELETE", "social")
except Exception:
    pass
r.execute_command("GRAPH.CREATE", "social")
print("  GRAPH.CREATE social: OK")

nodes = {}
node_defs = [
    ("alice", "Person", '{"name":"Alice","age":"30","city":"SF"}'),
    ("bob", "Person", '{"name":"Bob","age":"25","city":"NYC"}'),
    ("charlie", "Person", '{"name":"Charlie","age":"35","city":"London"}'),
    ("diana", "Person", '{"name":"Diana","age":"28","city":"Tokyo"}'),
    ("eve", "Person", '{"name":"Eve","age":"32","city":"Paris"}'),
    ("frank", "Person", '{"name":"Frank","age":"40","city":"Berlin"}'),
    ("react", "Tech", '{"name":"React","type":"frontend"}'),
    ("rust_lang", "Tech", '{"name":"Rust","type":"language"}'),
    ("moon_db", "Tech", '{"name":"Moon","type":"database"}'),
]
for name, label, props in node_defs:
    result = r.execute_command("GRAPH.ADDNODE", "social", name, label, props)
    nid = int(result) if isinstance(result, (int, bytes)) else int(result)
    nodes[name] = nid
    print(f"  ADDNODE {name} -> id={nid}")

edges = [
    ("alice", "bob", "FOLLOWS", ["since", "2024"]),
    ("alice", "charlie", "FOLLOWS", ["since", "2023"]),
    ("bob", "diana", "FOLLOWS", ["since", "2024"]),
    ("charlie", "eve", "FOLLOWS", ["since", "2023"]),
    ("diana", "frank", "FOLLOWS", ["since", "2024"]),
    ("eve", "alice", "FOLLOWS", ["since", "2023"]),
    ("alice", "rust_lang", "USES", ["level", "expert"]),
    ("alice", "moon_db", "USES", ["level", "contributor"]),
    ("bob", "react", "USES", ["level", "senior"]),
    ("charlie", "rust_lang", "USES", ["level", "intermediate"]),
    ("diana", "moon_db", "USES", ["level", "user"]),
]
for src, dst, etype, props in edges:
    try:
        # Properties are key-value pairs, not JSON: GRAPH.ADDEDGE g src dst TYPE k1 v1 k2 v2
        r.execute_command("GRAPH.ADDEDGE", "social", str(nodes[src]), str(nodes[dst]), etype, *props)
        print(f"  ADDEDGE {src}->{dst} ({etype}): OK")
    except Exception as e:
        print(f"  ADDEDGE {src}->{dst} error: {e}")

print(f"\nDone. Total keys: {r.dbsize()}")
print("Open http://moon-dev.orb.local:9100/ui/ to explore!")
