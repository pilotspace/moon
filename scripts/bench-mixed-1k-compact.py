#!/usr/bin/env python3
"""
Mixed Insert+Search with COMPACT_THRESHOLD=1000

Simulates a realistic workload where vectors arrive continuously and
searches happen between inserts. Compaction triggers every 1K vectors
in the mutable segment, creating multiple immutable HNSW segments.

Timeline (10K total):
  - Insert 100 vectors, then search 10 queries → repeat 100 times
  - Every ~1000 vectors: compaction fires on next search
  - Track: recall, latency, compaction events per 100-vector window

This exposes:
  - How recall behaves BETWEEN compaction events (mutable brute-force)
  - Compaction latency spikes and their frequency
  - Recall across multiple immutable segments (merged search)
  - Whether small segments hurt recall vs one large segment
"""

import json
import os
import sys
import time

import numpy as np


def generate_or_load_data():
    cache = "target/bench-data-minilm"
    if os.path.exists(f"{cache}/vectors.npy"):
        vectors = np.load(f"{cache}/vectors.npy")
        queries = np.load(f"{cache}/queries.npy")
        with open(f"{cache}/ground_truth.json") as f:
            gt = json.load(f)
        return vectors, queries, gt
    print("ERROR: Run bench-mixed-workload.py first to generate MiniLM data")
    sys.exit(1)


def run_moon(port, vectors, queries, gt_final, compact_threshold):
    import redis as redis_lib

    r = redis_lib.Redis(port=port, decode_responses=False, socket_timeout=600)
    r.ping()

    n, dim = vectors.shape

    # Create index with specified compact threshold
    r.execute_command(
        "FT.CREATE", "idx", "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA", "vec", "VECTOR", "HNSW", "10",
        "TYPE", "FLOAT32", "DIM", str(dim),
        "DISTANCE_METRIC", "L2", "QUANTIZATION", "TQ4",
        "COMPACT_THRESHOLD", str(compact_threshold),
    )

    # Tracking arrays
    insert_batch = 100
    search_per_batch = 10
    num_batches = n // insert_batch

    timeline = []  # per-batch metrics
    all_lats = []
    compaction_events = []
    next_id = 0
    query_idx = 0
    total_compact_time = 0.0

    print(f"  Config: {n} vectors, batch={insert_batch}, "
          f"search/batch={search_per_batch}, compact_threshold={compact_threshold}")
    print(f"  Expected compactions: ~{n // compact_threshold}")
    print()
    print(f"  {'Vectors':>7} │ {'Recall':>7} │ {'p50':>7} │ {'p99':>8} │ {'max':>8} │ Compact")
    print(f"  {'':─>7}─┼─{'':─>7}─┼─{'':─>7}─┼─{'':─>8}─┼─{'':─>8}─┼─{'':─>20}")

    for batch_idx in range(num_batches):
        # Insert batch
        pipe = r.pipeline(transaction=False)
        for i in range(insert_batch):
            vid = next_id + i
            pipe.execute_command("HSET", f"doc:{vid}", "vec", vectors[vid].tobytes())
        pipe.execute()
        next_id += insert_batch

        # Search queries and measure
        batch_lats = []
        batch_recalls = []
        batch_compact = False
        batch_compact_time = 0.0

        for _ in range(search_per_batch):
            q = queries[query_idx % len(queries)]
            query_idx += 1

            t0 = time.perf_counter()
            result = r.execute_command(
                "FT.SEARCH", "idx",
                "*=>[KNN 10 @vec $query]",
                "PARAMS", "2", "query", q.tobytes(),
            )
            lat = (time.perf_counter() - t0) * 1000
            batch_lats.append(lat)
            all_lats.append(lat)

            # Detect compaction spike
            if lat > 100:  # >100ms strongly suggests compaction
                batch_compact = True
                batch_compact_time = lat

            # Parse results
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

            # Recall vs brute-force over ALL vectors inserted so far
            dists = np.sum((vectors[:next_id] - q) ** 2, axis=1)
            local_gt = set(np.argsort(dists)[:10].tolist())
            recall = len(set(ids) & local_gt) / 10
            batch_recalls.append(recall)

        avg_recall = np.mean(batch_recalls)
        p50 = np.percentile(batch_lats, 50)
        p99 = np.percentile(batch_lats, 99)
        max_lat = max(batch_lats)

        compact_str = ""
        if batch_compact:
            compact_str = f"← {batch_compact_time:.0f}ms"
            compaction_events.append({
                "at_vectors": next_id,
                "latency_ms": batch_compact_time,
            })
            total_compact_time += batch_compact_time

        timeline.append({
            "vectors": next_id,
            "recall": float(avg_recall),
            "p50_ms": float(p50),
            "p99_ms": float(p99),
            "max_ms": float(max_lat),
            "compact": batch_compact,
        })

        # Print every 500 vectors or on compaction
        if next_id % 500 == 0 or batch_compact:
            print(f"  {next_id:>7} │ {avg_recall:>7.4f} │ {p50:>6.1f}ms │ {p99:>7.1f}ms │ {max_lat:>7.0f}ms │ {compact_str}")

    # Final recall against full ground truth
    print()
    print(f"  Final recall measurement (200 queries, full GT)...")
    final_recalls = []
    final_lats = []
    for i, q in enumerate(queries):
        t0 = time.perf_counter()
        result = r.execute_command(
            "FT.SEARCH", "idx",
            "*=>[KNN 10 @vec $query]",
            "PARAMS", "2", "query", q.tobytes(),
        )
        lat = (time.perf_counter() - t0) * 1000
        final_lats.append(lat)

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
        recall = len(set(ids) & set(gt_final[i])) / 10
        final_recalls.append(recall)

    return {
        "timeline": timeline,
        "compaction_events": compaction_events,
        "total_compact_time_ms": total_compact_time,
        "final_recall": float(np.mean(final_recalls)),
        "final_p50": float(np.percentile(final_lats, 50)),
        "final_qps": 1000 / np.mean(final_lats),
        "all_lats": all_lats,
        "steady_state_recall": float(np.mean([t["recall"] for t in timeline])),
        "num_compactions": len(compaction_events),
    }


def run_redis(port, vectors, queries, gt_final):
    import redis as redis_lib

    r = redis_lib.Redis(port=port, decode_responses=False, socket_timeout=600)
    r.ping()

    n, dim = vectors.shape
    insert_batch = 100
    search_per_batch = 10
    num_batches = n // insert_batch

    timeline = []
    all_lats = []
    next_id = 0
    query_idx = 0

    for batch_idx in range(num_batches):
        pipe = r.pipeline(transaction=False)
        for i in range(insert_batch):
            vid = next_id + i
            pipe.execute_command("VADD", "vecset", "FP32", vectors[vid].tobytes(), f"vec:{vid}")
        pipe.execute()
        next_id += insert_batch

        batch_lats = []
        batch_recalls = []
        for _ in range(search_per_batch):
            q = queries[query_idx % len(queries)]
            query_idx += 1
            t0 = time.perf_counter()
            result = r.execute_command("VSIM", "vecset", "FP32", q.tobytes(), "COUNT", "10")
            lat = (time.perf_counter() - t0) * 1000
            batch_lats.append(lat)
            all_lats.append(lat)

            ids = []
            if isinstance(result, list):
                for item in result:
                    try:
                        raw = item.decode() if isinstance(item, bytes) else str(item)
                        ids.append(int(raw.split(":")[-1]))
                    except Exception:
                        pass

            dists = np.sum((vectors[:next_id] - q) ** 2, axis=1)
            local_gt = set(np.argsort(dists)[:10].tolist())
            batch_recalls.append(len(set(ids) & local_gt) / 10)

        timeline.append({
            "vectors": next_id,
            "recall": float(np.mean(batch_recalls)),
            "p50_ms": float(np.percentile(batch_lats, 50)),
        })

    final_recalls = []
    final_lats = []
    for i, q in enumerate(queries):
        t0 = time.perf_counter()
        result = r.execute_command("VSIM", "vecset", "FP32", q.tobytes(), "COUNT", "10")
        lat = (time.perf_counter() - t0) * 1000
        final_lats.append(lat)
        ids = []
        if isinstance(result, list):
            for item in result:
                try:
                    raw = item.decode() if isinstance(item, bytes) else str(item)
                    ids.append(int(raw.split(":")[-1]))
                except Exception:
                    pass
        final_recalls.append(len(set(ids) & set(gt_final[i])) / 10)

    return {
        "timeline": timeline,
        "final_recall": float(np.mean(final_recalls)),
        "final_p50": float(np.percentile(final_lats, 50)),
        "final_qps": 1000 / np.mean(final_lats),
        "steady_state_recall": float(np.mean([t["recall"] for t in timeline])),
        "all_lats": all_lats,
    }


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--moon-port", type=int, default=6379)
    parser.add_argument("--redis-port", type=int, default=6400)
    parser.add_argument("--compact-threshold", type=int, default=1000)
    parser.add_argument("--skip-redis", action="store_true")
    args = parser.parse_args()

    vectors, queries, gt = generate_or_load_data()
    n, dim = vectors.shape
    print(f"Mixed Insert+Search (compact_threshold={args.compact_threshold})")
    print(f"Data: {n} MiniLM vectors, {dim}d, {len(queries)} queries")
    print(f"Pattern: insert 100 → search 10 → repeat {n // 100} times")
    print()

    # Moon
    print("=" * 65)
    print(f" Moon (port {args.moon_port}, compact_threshold={args.compact_threshold})")
    print("=" * 65)
    try:
        moon = run_moon(args.moon_port, vectors, queries, gt, args.compact_threshold)
    except Exception as e:
        print(f"  Moon error: {e}")
        moon = None

    # Redis
    redis_result = None
    if not args.skip_redis:
        print()
        print("=" * 65)
        print(f" Redis (port {args.redis_port})")
        print("=" * 65)
        try:
            redis_result = run_redis(args.redis_port, vectors, queries, gt)
        except Exception as e:
            print(f"  Redis error: {e}")

    # Report
    print()
    print("=" * 65)
    print(" SUMMARY")
    print("=" * 65)
    print()

    if moon:
        print(f"  Moon (compact_threshold={args.compact_threshold}):")
        print(f"    Steady-state recall (avg over all batches): {moon['steady_state_recall']:.4f}")
        print(f"    Final recall@10:    {moon['final_recall']:.4f}")
        print(f"    Final QPS:          {moon['final_qps']:.0f}")
        print(f"    Final p50:          {moon['final_p50']:.2f}ms")
        print(f"    Compaction events:  {moon['num_compactions']}")
        print(f"    Total compact time: {moon['total_compact_time_ms']:.0f}ms")
        if moon['all_lats']:
            lats = moon['all_lats']
            print(f"    Latency: p50={np.percentile(lats,50):.1f}ms "
                  f"p95={np.percentile(lats,95):.1f}ms "
                  f"p99={np.percentile(lats,99):.1f}ms "
                  f"max={max(lats):.0f}ms")
        if moon['compaction_events']:
            print(f"    Compaction details:")
            for evt in moon['compaction_events']:
                print(f"      at {evt['at_vectors']:>5} vectors: {evt['latency_ms']:.0f}ms")
        print()

    if redis_result:
        print(f"  Redis:")
        print(f"    Steady-state recall: {redis_result['steady_state_recall']:.4f}")
        print(f"    Final recall@10:     {redis_result['final_recall']:.4f}")
        print(f"    Final QPS:           {redis_result['final_qps']:.0f}")
        lats = redis_result['all_lats']
        print(f"    Latency: p50={np.percentile(lats,50):.1f}ms "
              f"p95={np.percentile(lats,95):.1f}ms "
              f"p99={np.percentile(lats,99):.1f}ms "
              f"max={max(lats):.0f}ms")
        print()

    # Save
    os.makedirs("target/bench-results", exist_ok=True)
    out = {"moon": moon, "redis": redis_result, "compact_threshold": args.compact_threshold}
    with open("target/bench-results/mixed-1k-compact.json", "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
