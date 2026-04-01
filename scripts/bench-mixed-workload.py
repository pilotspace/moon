#!/usr/bin/env python3
"""
Moon vs Redis vs Qdrant — Mixed Insert+Search Simulation

Simulates a real production workload where inserts and searches happen
concurrently across multiple phases:

  Phase 1: Bulk load (1K vectors, no search)
  Phase 2: Steady-state (insert 10 + search 5, repeated 900 turns)
  Phase 3: Search-heavy (insert 1 + search 20, repeated 100 turns)
  Phase 4: Burst insert (500 vectors, then 50 searches)
  Phase 5: Final recall measurement (200 queries)

Total: ~10K vectors inserted, ~6K searches performed.

This tests:
  - Search quality during active ingestion (mutable segment brute-force)
  - Search quality after compaction triggers
  - Latency stability under mixed load
  - Recall progression as dataset grows
  - Compaction interference with search latency (tail latency spikes)

Usage:
  python3 scripts/bench-mixed-workload.py [--moon-port 6379] [--redis-port 6400] [--qdrant-port 6333]
"""

import argparse
import json
import os
import subprocess
import sys
import time

import numpy as np

# ── Data generation ───────────────────────────────────────────────────

def generate_minilm_data():
    """Generate MiniLM embeddings or load cached."""
    cache = "target/bench-data-minilm"
    if os.path.exists(f"{cache}/vectors.npy"):
        vectors = np.load(f"{cache}/vectors.npy")
        queries = np.load(f"{cache}/queries.npy")
        with open(f"{cache}/ground_truth.json") as f:
            gt = json.load(f)
        return vectors, queries, gt

    print("Generating MiniLM embeddings (first run)...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")

    np.random.seed(42)
    nouns = ["machine", "learning", "data", "science", "cloud", "network",
             "system", "model", "server", "database", "algorithm", "pipeline",
             "engine", "platform", "architecture", "deployment", "container",
             "cluster", "storage", "memory", "processor", "kernel", "module",
             "function", "method", "structure", "pattern", "framework",
             "protocol", "service", "interface", "driver", "object", "variable"]
    templates = [
        "The {} {} {} in the {} of {}",
        "A {} {} {} with {} and {}",
        "{} is {} than {} for {} applications",
        "How to {} {} using {} and {} framework",
        "The impact of {} on {} in {} countries",
    ]
    adjectives = ["fast", "scalable", "distributed", "efficient", "modern",
                  "secure", "robust", "flexible", "lightweight", "optimized"]

    sentences = []
    for i in range(12000):
        tmpl = templates[i % len(templates)]
        words = [np.random.choice(nouns if j % 2 == 0 else adjectives)
                 for j in range(tmpl.count("{}"))]
        sentences.append(tmpl.format(*words))

    vecs = model.encode(sentences[:10000], batch_size=256, normalize_embeddings=True)
    qvecs = model.encode(sentences[10000:], batch_size=256, normalize_embeddings=True)
    # Use first 200 as queries
    qvecs = qvecs[:200]

    gt = []
    for q in qvecs:
        dists = np.sum((vecs - q) ** 2, axis=1)
        gt.append(np.argsort(dists)[:10].tolist())

    os.makedirs(cache, exist_ok=True)
    np.save(f"{cache}/vectors.npy", vecs)
    np.save(f"{cache}/queries.npy", qvecs)
    with open(f"{cache}/ground_truth.json", "w") as f:
        json.dump(gt, f)

    return vecs, qvecs, gt


# ── System adapters ──────────────────────────────────────────────────

class MoonAdapter:
    def __init__(self, port):
        import redis as redis_lib
        self.r = redis_lib.Redis(port=port, decode_responses=False, socket_timeout=600)
        self.port = port
        self.dim = None
        self.created = False

    def name(self):
        return "Moon"

    def create_index(self, dim):
        self.dim = dim
        if not self.created:
            self.r.execute_command(
                "FT.CREATE", "idx", "ON", "HASH",
                "PREFIX", "1", "doc:",
                "SCHEMA", "vec", "VECTOR", "HNSW", "10",
                "TYPE", "FLOAT32", "DIM", str(dim),
                "DISTANCE_METRIC", "L2", "QUANTIZATION", "TQ4",
                "COMPACT_THRESHOLD", "10000",
            )
            self.created = True

    def insert(self, doc_id, vector):
        self.r.execute_command("HSET", f"doc:{doc_id}", "vec", vector.tobytes())

    def insert_batch(self, start_id, vectors):
        pipe = self.r.pipeline(transaction=False)
        for i, v in enumerate(vectors):
            pipe.execute_command("HSET", f"doc:{start_id + i}", "vec", v.tobytes())
        pipe.execute()

    def search(self, query, k=10):
        t0 = time.perf_counter()
        result = self.r.execute_command(
            "FT.SEARCH", "idx",
            f"*=>[KNN {k} @vec $query]",
            "PARAMS", "2", "query", query.tobytes(),
        )
        latency = (time.perf_counter() - t0) * 1000
        ids = []
        if isinstance(result, list) and len(result) > 1:
            for j in range(1, len(result), 2):
                try:
                    raw = result[j]
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    ids.append(int(raw.split(":")[-1]))
                except Exception:
                    pass
        return ids, latency


class RedisAdapter:
    def __init__(self, port):
        import redis as redis_lib
        self.r = redis_lib.Redis(port=port, decode_responses=False, socket_timeout=600)
        self.port = port

    def name(self):
        return "Redis"

    def create_index(self, dim):
        pass  # VADD auto-creates

    def insert(self, doc_id, vector):
        self.r.execute_command("VADD", "vecset", "FP32", vector.tobytes(), f"vec:{doc_id}")

    def insert_batch(self, start_id, vectors):
        pipe = self.r.pipeline(transaction=False)
        for i, v in enumerate(vectors):
            pipe.execute_command("VADD", "vecset", "FP32", v.tobytes(), f"vec:{start_id + i}")
        pipe.execute()

    def search(self, query, k=10):
        t0 = time.perf_counter()
        result = self.r.execute_command(
            "VSIM", "vecset", "FP32", query.tobytes(), "COUNT", str(k)
        )
        latency = (time.perf_counter() - t0) * 1000
        ids = []
        if isinstance(result, list):
            for item in result:
                try:
                    raw = item.decode() if isinstance(item, bytes) else str(item)
                    ids.append(int(raw.split(":")[-1]))
                except Exception:
                    pass
        return ids, latency


class QdrantAdapter:
    def __init__(self, port):
        import requests
        self.base = f"http://localhost:{port}"
        self.requests = requests
        self.port = port
        self.dim = None

    def name(self):
        return "Qdrant"

    def create_index(self, dim):
        self.dim = dim
        self.requests.delete(f"{self.base}/collections/test")
        time.sleep(0.5)
        self.requests.put(f"{self.base}/collections/test", json={
            "vectors": {"size": dim, "distance": "Euclid"},
            "hnsw_config": {"m": 16, "ef_construct": 200},
        })

    def insert(self, doc_id, vector):
        self.requests.put(f"{self.base}/collections/test/points", json={
            "points": [{"id": doc_id, "vector": vector.tolist()}]
        })

    def insert_batch(self, start_id, vectors):
        points = [{"id": start_id + i, "vector": v.tolist()} for i, v in enumerate(vectors)]
        batch_size = 500
        for s in range(0, len(points), batch_size):
            self.requests.put(f"{self.base}/collections/test/points",
                              json={"points": points[s:s + batch_size]})

    def search(self, query, k=10):
        t0 = time.perf_counter()
        r = self.requests.post(f"{self.base}/collections/test/points/search", json={
            "vector": query.tolist(), "limit": k, "params": {"hnsw_ef": 128}
        })
        latency = (time.perf_counter() - t0) * 1000
        ids = []
        for p in r.json().get("result", []):
            ids.append(p["id"])
        return ids, latency


# ── Simulation ───────────────────────────────────────────────────────

def compute_recall(result_ids, all_vectors_so_far, query, k=10):
    """Compute recall against brute-force over vectors inserted so far."""
    if len(all_vectors_so_far) == 0:
        return 0.0
    vecs = np.array(all_vectors_so_far)
    dists = np.sum((vecs - query) ** 2, axis=1)
    gt = set(np.argsort(dists)[:k].tolist())
    return len(set(result_ids) & gt) / k


def run_simulation(adapter, vectors, queries, gt_final):
    """Run the 5-phase mixed workload simulation."""
    n, dim = vectors.shape
    adapter.create_index(dim)

    results = {
        "system": adapter.name(),
        "phases": [],
        "all_search_latencies": [],
        "all_search_recalls": [],
        "total_inserts": 0,
        "total_searches": 0,
    }

    inserted_so_far = []
    next_id = 0
    query_idx = 0

    def do_insert_batch(count):
        nonlocal next_id
        batch = vectors[next_id:next_id + count]
        adapter.insert_batch(next_id, batch)
        for v in batch:
            inserted_so_far.append(v)
        next_id += count
        results["total_inserts"] += count

    def do_search():
        nonlocal query_idx
        q = queries[query_idx % len(queries)]
        query_idx += 1
        ids, lat = adapter.search(q)
        recall = compute_recall(ids, inserted_so_far, q)
        results["all_search_latencies"].append(lat)
        results["all_search_recalls"].append(recall)
        results["total_searches"] += 1
        return lat, recall

    # ── Phase 1: Bulk load 1000 vectors ──────────────────────────────
    print(f"  Phase 1: Bulk load 1000 vectors...")
    t0 = time.time()
    do_insert_batch(1000)
    phase1_time = time.time() - t0

    # One search to trigger compaction (Moon) / indexing
    _, _ = do_search()
    time.sleep(0.5)  # Let indexing settle

    results["phases"].append({
        "name": "Bulk Load",
        "inserts": 1000,
        "time_s": phase1_time,
        "vps": 1000 / phase1_time,
    })

    # ── Phase 2: Steady state (insert 10, search 5) × 900 turns ─────
    print(f"  Phase 2: Steady-state (insert 10 + search 5) × 900 turns...")
    t0 = time.time()
    phase2_lats = []
    phase2_recalls = []
    phase2_compact_spikes = 0

    for turn in range(900):
        do_insert_batch(10)
        for _ in range(5):
            lat, recall = do_search()
            phase2_lats.append(lat)
            phase2_recalls.append(recall)
            if lat > 50:  # >50ms = likely compaction interference
                phase2_compact_spikes += 1

        if (turn + 1) % 300 == 0:
            print(f"    Turn {turn+1}/900: {next_id} vectors, "
                  f"avg recall={np.mean(phase2_recalls[-100:]):.4f}, "
                  f"p50={np.percentile(phase2_lats[-100:], 50):.1f}ms")

    phase2_time = time.time() - t0
    results["phases"].append({
        "name": "Steady State",
        "inserts": 9000,
        "searches": 4500,
        "turns": 900,
        "time_s": phase2_time,
        "avg_recall": float(np.mean(phase2_recalls)),
        "p50_ms": float(np.percentile(phase2_lats, 50)),
        "p99_ms": float(np.percentile(phase2_lats, 99)),
        "compact_spikes": phase2_compact_spikes,
    })

    # ── Phase 3: Search-heavy (insert 1, search 20) × 100 turns ─────
    # Tests search quality after most data is loaded
    # Remaining vectors may not be enough, cap at what we have
    remaining = min(100, n - next_id)
    print(f"  Phase 3: Search-heavy (insert 1 + search 20) × {remaining} turns...")
    t0 = time.time()
    phase3_lats = []
    phase3_recalls = []

    for turn in range(remaining):
        if next_id < n:
            do_insert_batch(1)
        for _ in range(20):
            lat, recall = do_search()
            phase3_lats.append(lat)
            phase3_recalls.append(recall)

    phase3_time = time.time() - t0
    results["phases"].append({
        "name": "Search Heavy",
        "inserts": remaining,
        "searches": remaining * 20,
        "time_s": phase3_time,
        "avg_recall": float(np.mean(phase3_recalls)) if phase3_recalls else 0,
        "p50_ms": float(np.percentile(phase3_lats, 50)) if phase3_lats else 0,
        "p99_ms": float(np.percentile(phase3_lats, 99)) if phase3_lats else 0,
    })

    # ── Phase 4: Burst insert (remaining vectors, then 50 searches) ──
    burst_count = n - next_id
    if burst_count > 0:
        print(f"  Phase 4: Burst insert ({burst_count} vectors, then 50 searches)...")
        t0 = time.time()
        do_insert_batch(burst_count)
        burst_insert_time = time.time() - t0

        # Wait for indexing
        time.sleep(1)

        phase4_lats = []
        phase4_recalls = []
        for _ in range(50):
            lat, recall = do_search()
            phase4_lats.append(lat)
            phase4_recalls.append(recall)

        results["phases"].append({
            "name": "Burst Insert",
            "inserts": burst_count,
            "searches": 50,
            "insert_time_s": burst_insert_time,
            "insert_vps": burst_count / burst_insert_time if burst_insert_time > 0 else 0,
            "avg_recall": float(np.mean(phase4_recalls)),
            "p50_ms": float(np.percentile(phase4_lats, 50)),
            "p99_ms": float(np.percentile(phase4_lats, 99)),
        })

    # ── Phase 5: Final recall (200 queries against full dataset) ─────
    print(f"  Phase 5: Final recall (200 queries against full dataset)...")
    final_lats = []
    final_recalls = []

    for i, q in enumerate(queries):
        ids, lat = adapter.search(q)
        final_lats.append(lat)
        # Use pre-computed ground truth for full dataset
        recall = len(set(ids) & set(gt_final[i])) / 10
        final_recalls.append(recall)

    results["phases"].append({
        "name": "Final Recall",
        "searches": len(queries),
        "recall": float(np.mean(final_recalls)),
        "p50_ms": float(np.percentile(final_lats, 50)),
        "p99_ms": float(np.percentile(final_lats, 99)),
        "qps": 1000 / np.mean(final_lats),
    })

    return results


# ── Report ───────────────────────────────────────────────────────────

def print_report(all_results):
    systems = [r["system"] for r in all_results]
    header = f"{'Phase':<20} │ " + " │ ".join(f"{s:>20}" for s in systems)
    sep = "─" * 21 + "┼" + "┼".join("─" * 22 for _ in systems)

    print()
    print("═" * 70)
    print(" Mixed Insert+Search Simulation — Results")
    print("═" * 70)
    print()

    # Phase summary table
    print(header)
    print(sep)

    # Extract phase data by name
    phase_names = ["Bulk Load", "Steady State", "Search Heavy", "Burst Insert", "Final Recall"]
    for pname in phase_names:
        row = f"{pname:<20} │ "
        cells = []
        for r in all_results:
            phase = next((p for p in r["phases"] if p["name"] == pname), None)
            if phase is None:
                cells.append(f"{'—':>20}")
                continue
            if pname == "Bulk Load":
                cells.append(f"{phase['vps']:>15,.0f} v/s")
            elif pname in ("Steady State", "Search Heavy"):
                cells.append(f"R={phase['avg_recall']:.3f} p50={phase['p50_ms']:.1f}ms")
            elif pname == "Burst Insert":
                cells.append(f"R={phase['avg_recall']:.3f} {phase.get('insert_vps',0):,.0f}v/s")
            elif pname == "Final Recall":
                cells.append(f"R={phase['recall']:.3f} {phase['qps']:,.0f}QPS")
        print(row + " │ ".join(cells))

    print(sep)
    print()

    # Detailed metrics
    print("─── Detailed Metrics ───")
    print()
    for r in all_results:
        sys_name = r["system"]
        all_lats = r["all_search_latencies"]
        all_recalls = r["all_search_recalls"]
        print(f"  {sys_name}:")
        print(f"    Total inserts: {r['total_inserts']:,}")
        print(f"    Total searches: {r['total_searches']:,}")
        if all_lats:
            print(f"    Search latency (all): p50={np.percentile(all_lats,50):.2f}ms "
                  f"p95={np.percentile(all_lats,95):.2f}ms "
                  f"p99={np.percentile(all_lats,99):.2f}ms "
                  f"max={max(all_lats):.1f}ms")
        if all_recalls:
            print(f"    Recall (all searches): mean={np.mean(all_recalls):.4f} "
                  f"min={min(all_recalls):.4f} "
                  f"std={np.std(all_recalls):.4f}")

        # Phase 2 compaction interference
        p2 = next((p for p in r["phases"] if p["name"] == "Steady State"), None)
        if p2 and "compact_spikes" in p2:
            print(f"    Compaction spikes (>50ms): {p2['compact_spikes']}")

        # Final recall
        p5 = next((p for p in r["phases"] if p["name"] == "Final Recall"), None)
        if p5:
            print(f"    Final recall@10: {p5['recall']:.4f}")
            print(f"    Final QPS: {p5['qps']:.0f}")
        print()


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Mixed insert+search simulation")
    parser.add_argument("--moon-port", type=int, default=6379)
    parser.add_argument("--redis-port", type=int, default=6400)
    parser.add_argument("--qdrant-port", type=int, default=6333)
    parser.add_argument("--skip-redis", action="store_true")
    parser.add_argument("--skip-qdrant", action="store_true")
    parser.add_argument("--skip-moon", action="store_true")
    args = parser.parse_args()

    print("Loading MiniLM data...")
    vectors, queries, gt = generate_minilm_data()
    print(f"  {vectors.shape[0]} vectors, {vectors.shape[1]}d, {len(queries)} queries")
    print()

    all_results = []

    # Moon
    if not args.skip_moon:
        print(f"{'='*65}")
        print(f" Moon (port {args.moon_port})")
        print(f"{'='*65}")
        try:
            adapter = MoonAdapter(args.moon_port)
            adapter.r.ping()
            result = run_simulation(adapter, vectors, queries, gt)
            all_results.append(result)
        except Exception as e:
            print(f"  Moon not available: {e}")
        print()

    # Redis
    if not args.skip_redis:
        print(f"{'='*65}")
        print(f" Redis (port {args.redis_port})")
        print(f"{'='*65}")
        try:
            adapter = RedisAdapter(args.redis_port)
            adapter.r.ping()
            result = run_simulation(adapter, vectors, queries, gt)
            all_results.append(result)
        except Exception as e:
            print(f"  Redis not available: {e}")
        print()

    # Qdrant
    if not args.skip_qdrant:
        print(f"{'='*65}")
        print(f" Qdrant (port {args.qdrant_port})")
        print(f"{'='*65}")
        try:
            import requests
            requests.get(f"http://localhost:{args.qdrant_port}/collections")
            adapter = QdrantAdapter(args.qdrant_port)
            result = run_simulation(adapter, vectors, queries, gt)
            all_results.append(result)
        except Exception as e:
            print(f"  Qdrant not available: {e}")
        print()

    if all_results:
        print_report(all_results)

    # Save raw results
    os.makedirs("target/bench-results", exist_ok=True)
    with open("target/bench-results/mixed-workload.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print("Raw results saved to target/bench-results/mixed-workload.json")


if __name__ == "__main__":
    main()
