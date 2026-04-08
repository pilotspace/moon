#!/usr/bin/env python3
"""Part 2: Vector Search — Moon vs Redis 8.x vs Qdrant with MiniLM embeddings.

Measures: insert QPS, search QPS, recall@10, p50/p99 latency, memory.
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


def vec_to_bytes(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


def bench_moon(vectors, queries, ground_truth, port, k, ef, moon_bin, dim):
    """Benchmark Moon vector search via redis-py."""
    data_dir = f"/tmp/moon-vec-{port}"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen([
        moon_bin, "--port", str(port), "--shards", "1",
        "--dir", data_dir, "--appendonly", "yes",
        "--disk-offload", "enable",
        "--segment-warm-after", "3600",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wait_for_port(port):
        proc.kill()
        return None

    r = redis.Redis(host="127.0.0.1", port=port, decode_responses=False)

    try:
        # Create index
        r.execute_command(
            "FT.CREATE", "bench_idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2"
        )

        # Insert
        t0 = time.time()
        pipe = r.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc:{i}", mapping={"vec": vec_to_bytes(vec)})
            if (i + 1) % 500 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()
        insert_time = time.time() - t0
        insert_qps = len(vectors) / insert_time

        time.sleep(2)  # Wait for compaction

        # Search
        latencies = []
        all_results = []
        for q in queries:
            q_bytes = vec_to_bytes(q)
            t0 = time.time()
            result = r.execute_command(
                "FT.SEARCH", "bench_idx",
                f"*=>[KNN {k} @vec $query_vec]",
                "PARAMS", "2", "query_vec", q_bytes,
                "DIALECT", "2",
            )
            latencies.append((time.time() - t0) * 1000)  # ms
            # Parse result IDs — Moon returns [count, key1, [fields], key2, [fields], ...]
            if isinstance(result, list) and len(result) > 1:
                ids = []
                i_r = 1  # skip count at index 0
                while i_r < len(result):
                    if isinstance(result[i_r], bytes):
                        doc_id = result[i_r].decode()
                        # Extract numeric ID from "doc:N" or "vec:N" prefix
                        for prefix in ("doc:", "vec:"):
                            if doc_id.startswith(prefix):
                                try:
                                    ids.append(int(doc_id[len(prefix):]))
                                except ValueError:
                                    pass
                                break
                        i_r += 1
                        # Skip field array if present
                        if i_r < len(result) and isinstance(result[i_r], list):
                            i_r += 1
                    else:
                        i_r += 1
                all_results.append(ids[:k])
            else:
                all_results.append([])

        search_qps = len(queries) / (sum(latencies) / 1000)
        p50 = sorted(latencies)[len(latencies) // 2]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]

        # Recall@10
        recalls = []
        for res, gt in zip(all_results, ground_truth):
            hit = len(set(res[:k]) & set(gt[:k]))
            recalls.append(hit / k)
        avg_recall = sum(recalls) / len(recalls) if recalls else 0

        rss = get_rss_mb(proc.pid)

        return {
            "insert_qps": round(insert_qps, 1),
            "search_qps": round(search_qps, 1),
            "recall_at_10": round(avg_recall, 4),
            "p50_ms": round(p50, 2),
            "p99_ms": round(p99, 2),
            "rss_mb": round(rss, 1),
        }
    except Exception as e:
        print(f"    Moon error: {e}")
        return None
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(data_dir, ignore_errors=True)


def bench_redis(vectors, queries, ground_truth, port, k, dim):
    """Benchmark Redis 8.x with RediSearch."""
    data_dir = f"/tmp/redis-vec-{port}"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen([
        "redis-server", "--port", str(port),
        "--dir", data_dir,
        "--loadmodule", "",  # Redis 8.x has built-in search
        "--appendonly", "yes", "--save", "",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if not wait_for_port(port):
        # Try without --loadmodule for Redis 8.x
        proc.kill()
        proc = subprocess.Popen([
            "redis-server", "--port", str(port),
            "--dir", data_dir,
            "--appendonly", "yes", "--save", "",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if not wait_for_port(port):
            proc.kill()
            return None

    r = redis.Redis(host="127.0.0.1", port=port, decode_responses=False)

    try:
        r.execute_command(
            "FT.CREATE", "bench_idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2"
        )

        t0 = time.time()
        pipe = r.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"doc:{i}", mapping={"vec": vec_to_bytes(vec)})
            if (i + 1) % 500 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()
        insert_time = time.time() - t0
        insert_qps = len(vectors) / insert_time

        time.sleep(2)

        latencies = []
        all_results = []
        for q in queries:
            q_bytes = vec_to_bytes(q)
            t0 = time.time()
            result = r.execute_command(
                "FT.SEARCH", "bench_idx",
                f"*=>[KNN {k} @vec $query_vec]",
                "PARAMS", "2", "query_vec", q_bytes,
                "DIALECT", "2",
            )
            latencies.append((time.time() - t0) * 1000)
            if isinstance(result, list) and len(result) > 1:
                ids = []
                for j in range(1, len(result), 2):
                    if isinstance(result[j], bytes):
                        doc_id = result[j].decode()
                        if doc_id.startswith("doc:"):
                            ids.append(int(doc_id[4:]))
                all_results.append(ids[:k])
            else:
                all_results.append([])

        search_qps = len(queries) / (sum(latencies) / 1000)
        p50 = sorted(latencies)[len(latencies) // 2]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]

        recalls = []
        for res, gt in zip(all_results, ground_truth):
            hit = len(set(res[:k]) & set(gt[:k]))
            recalls.append(hit / k)
        avg_recall = sum(recalls) / len(recalls) if recalls else 0

        rss = get_rss_mb(proc.pid)
        return {
            "insert_qps": round(insert_qps, 1),
            "search_qps": round(search_qps, 1),
            "recall_at_10": round(avg_recall, 4),
            "p50_ms": round(p50, 2),
            "p99_ms": round(p99, 2),
            "rss_mb": round(rss, 1),
        }
    except Exception as e:
        print(f"    Redis error: {e}")
        return None
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(data_dir, ignore_errors=True)


def bench_qdrant(vectors, queries, ground_truth, port, k, dim):
    """Benchmark Qdrant via Docker + REST API."""
    import requests

    # Start Qdrant via Docker
    subprocess.run(["docker", "rm", "-f", "qdrant-bench"], capture_output=True)
    proc = subprocess.Popen([
        "docker", "run", "--name", "qdrant-bench", "-p", f"{port}:6333",
        "--rm", "qdrant/qdrant:latest",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    base = f"http://127.0.0.1:{port}"
    if not wait_for_port(port, timeout=30):
        subprocess.run(["docker", "rm", "-f", "qdrant-bench"], capture_output=True)
        return None

    time.sleep(2)

    try:
        # Create collection
        requests.put(f"{base}/collections/bench", json={
            "vectors": {"size": dim, "distance": "Euclid"},
            "optimizers_config": {"default_segment_number": 2},
        }).raise_for_status()

        # Insert
        t0 = time.time()
        batch_size = 500
        for start in range(0, len(vectors), batch_size):
            batch = vectors[start:start + batch_size]
            points = [
                {"id": start + i, "vector": v.tolist()}
                for i, v in enumerate(batch)
            ]
            requests.put(f"{base}/collections/bench/points", json={
                "points": points,
            }).raise_for_status()
        insert_time = time.time() - t0
        insert_qps = len(vectors) / insert_time

        # Wait for indexing
        for _ in range(30):
            info = requests.get(f"{base}/collections/bench").json()
            status = info.get("result", {}).get("status", "")
            if status == "green":
                break
            time.sleep(1)

        # Search
        latencies = []
        all_results = []
        for q in queries:
            t0 = time.time()
            resp = requests.post(f"{base}/collections/bench/points/search", json={
                "vector": q.tolist(),
                "limit": k,
                "with_payload": False,
            })
            latencies.append((time.time() - t0) * 1000)
            result = resp.json().get("result", [])
            ids = [r["id"] for r in result]
            all_results.append(ids[:k])

        search_qps = len(queries) / (sum(latencies) / 1000)
        p50 = sorted(latencies)[len(latencies) // 2]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]

        recalls = []
        for res, gt in zip(all_results, ground_truth):
            hit = len(set(res[:k]) & set(gt[:k]))
            recalls.append(hit / k)
        avg_recall = sum(recalls) / len(recalls) if recalls else 0

        # Memory from docker stats
        try:
            stats = subprocess.check_output(
                ["docker", "stats", "qdrant-bench", "--no-stream", "--format", "{{.MemUsage}}"]
            ).decode().strip()
            rss_str = stats.split("/")[0].strip()
            if "GiB" in rss_str:
                rss = float(rss_str.replace("GiB", "")) * 1024
            elif "MiB" in rss_str:
                rss = float(rss_str.replace("MiB", ""))
            else:
                rss = 0
        except Exception:
            rss = 0

        return {
            "insert_qps": round(insert_qps, 1),
            "search_qps": round(search_qps, 1),
            "recall_at_10": round(avg_recall, 4),
            "p50_ms": round(p50, 2),
            "p99_ms": round(p99, 2),
            "rss_mb": round(rss, 1),
        }
    except Exception as e:
        print(f"    Qdrant error: {e}")
        return None
    finally:
        subprocess.run(["docker", "rm", "-f", "qdrant-bench"], capture_output=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--moon-bin", default="target/release/moon")
    p.add_argument("--data-dir", default="target/moonstore-v2-data")
    p.add_argument("--moon-port", type=int, default=16379)
    p.add_argument("--redis-port", type=int, default=16400)
    p.add_argument("--qdrant-port", type=int, default=16333)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--ef", type=int, default=200)
    p.add_argument("--mode", default="full")
    p.add_argument("--output", default="target/moonstore-v2-bench/vector.json")
    args = p.parse_args()

    # Load data
    vectors = np.load(os.path.join(args.data_dir, "vectors.npy"))
    queries = np.load(os.path.join(args.data_dir, "queries.npy"))
    with open(os.path.join(args.data_dir, "ground_truth.json")) as f:
        ground_truth = json.load(f)
    with open(os.path.join(args.data_dir, "meta.json")) as f:
        meta = json.load(f)

    dim = meta["dim"]
    print(f"  Loaded: {len(vectors)} vectors, {len(queries)} queries, {dim}d")

    results = {"meta": meta}

    # Moon
    print("\n  [Moon] Benchmarking...")
    results["moon"] = bench_moon(vectors, queries, ground_truth,
                                  args.moon_port, args.k, args.ef, args.moon_bin, dim)
    if results["moon"]:
        m = results["moon"]
        print(f"    Insert: {m['insert_qps']:.0f}/s | Search: {m['search_qps']:.0f}/s | "
              f"R@10: {m['recall_at_10']:.3f} | p99: {m['p99_ms']:.1f}ms | RSS: {m['rss_mb']:.0f}MB")

    # Redis
    print("\n  [Redis] Benchmarking...")
    results["redis"] = bench_redis(vectors, queries, ground_truth,
                                    args.redis_port, args.k, dim)
    if results["redis"]:
        m = results["redis"]
        print(f"    Insert: {m['insert_qps']:.0f}/s | Search: {m['search_qps']:.0f}/s | "
              f"R@10: {m['recall_at_10']:.3f} | p99: {m['p99_ms']:.1f}ms | RSS: {m['rss_mb']:.0f}MB")

    # Qdrant
    if args.mode == "full":
        print("\n  [Qdrant] Benchmarking...")
        results["qdrant"] = bench_qdrant(vectors, queries, ground_truth,
                                          args.qdrant_port, args.k, dim)
        if results["qdrant"]:
            m = results["qdrant"]
            print(f"    Insert: {m['insert_qps']:.0f}/s | Search: {m['search_qps']:.0f}/s | "
                  f"R@10: {m['recall_at_10']:.3f} | p99: {m['p99_ms']:.1f}ms | RSS: {m['rss_mb']:.0f}MB")
    else:
        print("\n  [Qdrant] Skipped (quick mode)")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Vector results saved: {args.output}")


if __name__ == "__main__":
    main()
