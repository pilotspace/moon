#!/usr/bin/env python3
"""Part 3: Warm Tier Benchmark — HOT->WARM transition + mmap search.

Tests warm tier with real MiniLM embeddings:
  1. Insert vectors, wait for compaction
  2. Force warm transition (segment-warm-after=1)
  3. Measure search QPS + recall after warm (mmap)
  4. Compare vs HOT-only baseline
"""

import argparse
import json
import os
import shutil
import struct
import subprocess
import sys
import time

import numpy as np
import redis


def wait_for_port(port, timeout=15):
    import socket
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return True
        except (ConnectionRefusedError, OSError):
            time.sleep(0.3)
    return False


def vec_to_bytes(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


def get_rss_mb(pid):
    try:
        if sys.platform == "darwin":
            out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]).decode().strip()
            return int(out) / 1024
        else:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) / 1024
    except Exception:
        return 0
    return 0


def run_search(r, queries, k, dim):
    """Run search queries and collect results."""
    latencies = []
    all_results = []
    for q in queries:
        q_bytes = vec_to_bytes(q)
        t0 = time.time()
        result = r.execute_command(
            "FT.SEARCH", "warm_idx",
            f"*=>[KNN {k} @vec $query_vec]",
            "PARAMS", "2", "query_vec", q_bytes,
            "DIALECT", "2",
        )
        latencies.append((time.time() - t0) * 1000)
        if isinstance(result, list) and len(result) > 1:
            ids = []
            i_r = 1
            while i_r < len(result):
                if isinstance(result[i_r], bytes):
                    doc_id = result[i_r].decode()
                    for prefix in ("doc:", "vec:"):
                        if doc_id.startswith(prefix):
                            try:
                                ids.append(int(doc_id[len(prefix):]))
                            except ValueError:
                                pass
                            break
                    i_r += 1
                    if i_r < len(result) and isinstance(result[i_r], list):
                        i_r += 1
                else:
                    i_r += 1
            all_results.append(ids[:k])
        else:
            all_results.append([])
    return latencies, all_results


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--moon-bin", default="target/release/moon")
    p.add_argument("--data-dir", default="target/moonstore-v2-data")
    p.add_argument("--port", type=int, default=16379)
    p.add_argument("--output", default="target/moonstore-v2-bench/warm.json")
    args = p.parse_args()

    vectors = np.load(os.path.join(args.data_dir, "vectors.npy"))
    queries = np.load(os.path.join(args.data_dir, "queries.npy"))
    with open(os.path.join(args.data_dir, "ground_truth.json")) as f:
        ground_truth = json.load(f)
    with open(os.path.join(args.data_dir, "meta.json")) as f:
        meta = json.load(f)

    dim = meta["dim"]
    k = 10
    # Use first 2000 vectors for warm test (faster)
    n_warm = min(2000, len(vectors))
    vectors_sub = vectors[:n_warm]
    results = {"n_vectors": n_warm, "dim": dim}

    # ── Phase 1: HOT-only baseline ──
    print(f"\n  [HOT baseline] {n_warm} vectors, {dim}d...")
    data_dir = f"/tmp/moon-warm-{args.port}"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen([
        args.moon_bin, "--port", str(args.port), "--shards", "1",
        "--dir", data_dir, "--appendonly", "yes",
        "--disk-offload", "enable",
        "--segment-warm-after", "86400",  # Keep hot (never warm)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wait_for_port(args.port):
        print("    Failed to start Moon")
        proc.kill()
        return

    r = redis.Redis(host="127.0.0.1", port=args.port, decode_responses=False)
    try:
        r.execute_command(
            "FT.CREATE", "warm_idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2"
        )

        for i, vec in enumerate(vectors_sub):
            r.hset(f"doc:{i}", mapping={"vec": vec_to_bytes(vec)})

        time.sleep(3)  # Wait for compaction

        hot_latencies, hot_results = run_search(r, queries, k, dim)
        hot_rss = get_rss_mb(proc.pid)

        hot_recalls = []
        for res, gt in zip(hot_results, ground_truth):
            hit = len(set(res[:k]) & set(gt[:k]))
            hot_recalls.append(hit / k)
        hot_recall = sum(hot_recalls) / len(hot_recalls) if hot_recalls else 0
        hot_qps = len(queries) / (sum(hot_latencies) / 1000)
        hot_p50 = sorted(hot_latencies)[len(hot_latencies) // 2]
        hot_p99 = sorted(hot_latencies)[int(len(hot_latencies) * 0.99)]

        results["hot"] = {
            "search_qps": round(hot_qps, 1),
            "recall_at_10": round(hot_recall, 4),
            "p50_ms": round(hot_p50, 2),
            "p99_ms": round(hot_p99, 2),
            "rss_mb": round(hot_rss, 1),
        }
        print(f"    QPS: {hot_qps:.0f} | R@10: {hot_recall:.3f} | p99: {hot_p99:.1f}ms | RSS: {hot_rss:.0f}MB")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(data_dir, ignore_errors=True)

    time.sleep(1)

    # ── Phase 2: WARM (mmap search after transition) ──
    print(f"\n  [WARM mmap] {n_warm} vectors, segment-warm-after=1...")
    data_dir = f"/tmp/moon-warm2-{args.port}"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen([
        args.moon_bin, "--port", str(args.port + 1), "--shards", "1",
        "--dir", data_dir, "--appendonly", "yes",
        "--disk-offload", "enable",
        "--segment-warm-after", "1",  # Force immediate warm
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wait_for_port(args.port + 1):
        print("    Failed to start Moon")
        proc.kill()
        return

    r2 = redis.Redis(host="127.0.0.1", port=args.port + 1, decode_responses=False)
    try:
        r2.execute_command(
            "FT.CREATE", "warm_idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2"
        )

        for i, vec in enumerate(vectors_sub):
            r2.hset(f"doc:{i}", mapping={"vec": vec_to_bytes(vec)})

        # Wait for compaction + warm transition
        print("    Waiting for HOT->WARM transition (15s)...")
        time.sleep(15)

        warm_latencies, warm_results = run_search(r2, queries, k, dim)
        warm_rss = get_rss_mb(proc.pid)

        warm_recalls = []
        for res, gt in zip(warm_results, ground_truth):
            hit = len(set(res[:k]) & set(gt[:k]))
            warm_recalls.append(hit / k)
        warm_recall = sum(warm_recalls) / len(warm_recalls) if warm_recalls else 0
        warm_qps = len(queries) / (sum(warm_latencies) / 1000)
        warm_p50 = sorted(warm_latencies)[len(warm_latencies) // 2]
        warm_p99 = sorted(warm_latencies)[int(len(warm_latencies) * 0.99)]

        # Check if .mpf files exist (warm transition happened)
        import glob
        mpf_files = glob.glob(os.path.join(data_dir, "shard-0/vectors/segment-*/*.mpf"))

        results["warm"] = {
            "search_qps": round(warm_qps, 1),
            "recall_at_10": round(warm_recall, 4),
            "p50_ms": round(warm_p50, 2),
            "p99_ms": round(warm_p99, 2),
            "rss_mb": round(warm_rss, 1),
            "mpf_files": len(mpf_files),
            "transition_happened": len(mpf_files) > 0,
        }
        print(f"    QPS: {warm_qps:.0f} | R@10: {warm_recall:.3f} | p99: {warm_p99:.1f}ms | RSS: {warm_rss:.0f}MB")
        print(f"    .mpf files on disk: {len(mpf_files)} | Transition: {'YES' if mpf_files else 'NO'}")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(data_dir, ignore_errors=True)

    # ── Summary ──
    if "hot" in results and "warm" in results:
        hot_r = results["hot"]["recall_at_10"]
        warm_r = results["warm"]["recall_at_10"]
        recall_delta = warm_r - hot_r
        rss_delta = results["warm"]["rss_mb"] - results["hot"]["rss_mb"]
        results["comparison"] = {
            "recall_delta": round(recall_delta, 4),
            "rss_delta_mb": round(rss_delta, 1),
            "warm_search_works": warm_r > 0,
        }
        print(f"\n  Recall delta (warm-hot): {recall_delta:+.4f}")
        print(f"  RSS delta: {rss_delta:+.0f}MB")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Warm results saved: {args.output}")


if __name__ == "__main__":
    main()
