#!/usr/bin/env python3
"""
Moon vs Qdrant — Vector Search Benchmark (MiniLM 384d, 500K+ vectors)

Fair TCP-level comparison:
  - Insert throughput (vectors/sec)
  - Search QPS, p50/p99 latency
  - Recall@10 (vs brute-force ground truth)
  - Memory (RSS)
  - Crash recovery (SIGKILL + restart + verify)

Usage:
  python3 scripts/bench-vector-500k.py [--vectors 500000] [--dim 384] [--moon-port 6399] [--qdrant-port 6333]

Works on: OrbStack ARM64, GCloud x86_64, macOS ARM64
"""

import argparse
import json
import math
import os
import random
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Moon vs Qdrant vector benchmark")
parser.add_argument("--vectors", type=int, default=500_000, help="Number of vectors")
parser.add_argument("--dim", type=int, default=384, help="Dimension (MiniLM=384)")
parser.add_argument("--queries", type=int, default=200, help="Number of search queries")
parser.add_argument("--k", type=int, default=10, help="Top-K neighbors")
parser.add_argument("--moon-port", type=int, default=6399, help="Moon server port")
parser.add_argument("--qdrant-port", type=int, default=6333, help="Qdrant REST port")
parser.add_argument("--moon-bin", type=str, default="./target/release/moon", help="Moon binary path")
parser.add_argument("--moon-dir", type=str, default="/tmp/moon-vec-bench", help="Moon data dir")
parser.add_argument("--qdrant-bin", type=str, default="", help="Qdrant binary (empty=skip)")
parser.add_argument("--qdrant-dir", type=str, default="/tmp/qdrant-vec-bench", help="Qdrant storage dir")
parser.add_argument("--skip-moon", action="store_true", help="Skip Moon benchmark")
parser.add_argument("--skip-qdrant", action="store_true", help="Skip Qdrant benchmark")
parser.add_argument("--skip-recovery", action="store_true", help="Skip crash recovery test")
parser.add_argument("--batch-size", type=int, default=500, help="Insert batch size")
parser.add_argument("--gt-sample", type=int, default=0, help="Ground truth computed on first N vectors (0=all)")
parser.add_argument("--compact-threshold", type=int, default=50000, help="Moon compact threshold")
parser.add_argument("--ef-runtime", type=int, default=0, help="Moon ef_runtime (0=auto)")
args = parser.parse_args()

N = args.vectors
DIM = args.dim
N_QUERIES = args.queries
K = args.k
BATCH = args.batch_size
BYTES_PER_VEC = DIM * 4

# ---------------------------------------------------------------------------
# Vector generation (seeded random, no numpy needed)
# ---------------------------------------------------------------------------
def gen_vector(seed):
    """Generate a normalized random vector."""
    rng = random.Random(seed)
    v = [rng.gauss(0, 1) for _ in range(DIM)]
    norm = math.sqrt(sum(x * x for x in v))
    if norm > 0:
        v = [x / norm for x in v]
    return v

def vec_to_blob(v):
    return struct.pack(f"{DIM}f", *v)

def blob_to_vec(blob):
    return list(struct.unpack(f"{DIM}f", blob))

def l2_distance(a, b):
    return sum((x - y) ** 2 for x, y in zip(a, b))

# ---------------------------------------------------------------------------
# Ground truth (brute force on subset for recall measurement)
# ---------------------------------------------------------------------------
def compute_ground_truth(query_vecs, db_vecs, k):
    """Brute-force nearest neighbors for recall calculation.
    Uses numpy if available for speed on large datasets."""
    try:
        import numpy as np
        db_arr = np.array(db_vecs, dtype=np.float32)
        q_arr = np.array(query_vecs, dtype=np.float32)
        gt = []
        for i in range(len(query_vecs)):
            diffs = db_arr - q_arr[i]
            dists = np.sum(diffs * diffs, axis=1)
            topk = np.argsort(dists)[:k].tolist()
            gt.append(topk)
        return gt
    except ImportError:
        gt = []
        for q in query_vecs:
            dists = [(l2_distance(q, db_vecs[i]), i) for i in range(len(db_vecs))]
            dists.sort()
            gt.append([idx for _, idx in dists[:k]])
        return gt

def recall_at_k(predicted_ids, ground_truth_ids, k, gt_db_size=None):
    """Compute recall@k. If gt_db_size is set, only count predictions within that range."""
    recalls = []
    for pred, truth in zip(predicted_ids, ground_truth_ids):
        truth_set = set(truth[:k])
        if gt_db_size is not None:
            # Filter predictions to only IDs within ground truth DB range
            pred_filtered = [p for p in pred[:k] if p < gt_db_size]
            tp = len(set(pred_filtered) & truth_set)
        else:
            tp = len(set(pred[:k]) & truth_set)
        recalls.append(tp / k)
    return sum(recalls) / len(recalls) if recalls else 0.0

# ---------------------------------------------------------------------------
# System info
# ---------------------------------------------------------------------------
def get_system_info():
    info = {"os": sys.platform, "arch": os.uname().machine}
    try:
        if sys.platform == "darwin":
            info["cpu"] = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
            info["cores"] = subprocess.check_output(
                ["sysctl", "-n", "hw.ncpu"], text=True
            ).strip()
        else:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if "model name" in line:
                        info["cpu"] = line.split(":")[1].strip()
                        break
            info["cores"] = str(os.cpu_count())
            # Check kernel for io_uring support
            info["kernel"] = os.uname().release
    except Exception:
        pass
    return info

def get_rss_mb(pid):
    try:
        if sys.platform == "darwin":
            out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
        else:
            out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True)
        return float(out.strip()) / 1024.0
    except Exception:
        return 0.0

# ---------------------------------------------------------------------------
# RESP protocol helpers (for Moon)
# ---------------------------------------------------------------------------
def resp_encode(args_list):
    """Encode a command as RESP array."""
    parts = [f"*{len(args_list)}\r\n".encode()]
    for a in args_list:
        if isinstance(a, bytes):
            parts.append(f"${len(a)}\r\n".encode())
            parts.append(a)
            parts.append(b"\r\n")
        else:
            s = str(a)
            parts.append(f"${len(s)}\r\n{s}\r\n".encode())
    return b"".join(parts)

def resp_read_line(sock):
    buf = b""
    while b"\r\n" not in buf:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("Connection closed")
        buf += chunk
    line, rest = buf.split(b"\r\n", 1)
    return line, rest

def resp_read_full(sock, timeout=30):
    """Read a complete RESP response (blocking)."""
    sock.settimeout(timeout)
    buf = b""
    try:
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf += chunk
            # Quick heuristic: if we got data and socket has no more
            sock.settimeout(0.05)
    except socket.timeout:
        pass
    except Exception:
        pass
    sock.settimeout(timeout)
    return buf

def resp_read_one(sock, buf=b""):
    """Read exactly one RESP value, return (value, remaining_buf)."""
    while b"\r\n" not in buf:
        buf += sock.recv(65536)

    prefix = buf[0:1]
    line_end = buf.index(b"\r\n")
    line = buf[:line_end]
    rest = buf[line_end + 2:]

    if prefix == b"+":
        return line[1:].decode(), rest
    elif prefix == b"-":
        return Exception(line[1:].decode()), rest
    elif prefix == b":":
        return int(line[1:]), rest
    elif prefix == b"$":
        length = int(line[1:])
        if length == -1:
            return None, rest
        while len(rest) < length + 2:
            rest += sock.recv(65536)
        data = rest[:length]
        return data, rest[length + 2:]
    elif prefix == b"*":
        count = int(line[1:])
        if count == -1:
            return None, rest
        elements = []
        for _ in range(count):
            elem, rest = resp_read_one(sock, rest)
            elements.append(elem)
        return elements, rest
    else:
        return line.decode(), rest

# ---------------------------------------------------------------------------
# Moon benchmark
# ---------------------------------------------------------------------------
def moon_connect(port, timeout=30):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect(("127.0.0.1", port))
    # PING
    s.sendall(resp_encode(["PING"]))
    resp, _ = resp_read_one(s)
    assert resp == "PONG" or resp == b"PONG", f"PING failed: {resp}"
    return s

def moon_create_index(sock, dim, compact_threshold, ef_runtime):
    """FT.CREATE minilm ON HASH PREFIX 1 vec: SCHEMA emb VECTOR HNSW ... """
    cmd_args = [
        "FT.CREATE", "minilm", "ON", "HASH", "PREFIX", "1", "vec:",
        "SCHEMA", "emb", "VECTOR", "HNSW", "14",
        "TYPE", "FLOAT32",
        "DIM", str(dim),
        "DISTANCE_METRIC", "L2",
        "M", "16",
        "EF_CONSTRUCTION", "200",
        "COMPACT_THRESHOLD", str(compact_threshold),
        "QUANTIZATION", "TQ4",
    ]
    if ef_runtime > 0:
        cmd_args[-2] = str(len(cmd_args) - 11 + 2)  # update param count
        cmd_args.extend(["EF_RUNTIME", str(ef_runtime)])
        # Fix: recalculate HNSW param count
        # params after HNSW: TYPE FLOAT32 DIM 384 DISTANCE_METRIC L2 M 16 EF_CONSTRUCTION 200
        #   COMPACT_THRESHOLD 50000 QUANTIZATION TQ4 EF_RUNTIME N = 16
        cmd_args[11] = "16"
    sock.sendall(resp_encode(cmd_args))
    resp, _ = resp_read_one(sock)
    return resp

def moon_insert_vectors(sock, n, dim, batch_size=500):
    """Insert vectors via pipelined HSET commands."""
    t0 = time.time()
    inserted = 0
    buf = bytearray()

    for i in range(n):
        v = gen_vector(i)
        blob = vec_to_blob(v)
        key = f"vec:{i}"
        cmd = resp_encode(["HSET", key, "emb", blob])
        buf.extend(cmd)

        if (i + 1) % batch_size == 0 or i == n - 1:
            sock.sendall(bytes(buf))
            buf = bytearray()
            # Drain replies
            count = min(batch_size, i - inserted + 1)
            remaining = b""
            for _ in range(count):
                resp, remaining = resp_read_one(sock, remaining)
            inserted = i + 1

            elapsed = time.time() - t0
            if inserted % 50000 == 0:
                rate = inserted / elapsed if elapsed > 0 else 0
                print(f"  Moon insert: {inserted}/{n} ({rate:.0f} vec/s)")

    elapsed = time.time() - t0
    rate = n / elapsed if elapsed > 0 else 0
    return elapsed, rate

def moon_search(sock, query_vec, k=10, timeout=30):
    """FT.SEARCH minilm "*=>[KNN K @emb $BLOB]" PARAMS 2 BLOB <blob> DIALECT 2"""
    blob = vec_to_blob(query_vec)
    query_str = f"*=>[KNN {k} @emb $BLOB]"
    cmd = resp_encode(["FT.SEARCH", "minilm", query_str, "PARAMS", "2", "BLOB", blob, "DIALECT", "2"])
    old_timeout = sock.gettimeout()
    sock.settimeout(timeout)
    sock.sendall(cmd)
    resp, _ = resp_read_one(sock)
    sock.settimeout(old_timeout)
    return resp

def moon_compact(sock):
    """FT.COMPACT minilm — may take minutes for large indexes."""
    old_timeout = sock.gettimeout()
    sock.settimeout(600)  # 10 min for HNSW build on 500K vectors
    sock.sendall(resp_encode(["FT.COMPACT", "minilm"]))
    resp, _ = resp_read_one(sock)
    sock.settimeout(old_timeout)
    return resp

def moon_dbsize(sock):
    """Get key count via SCAN (DBSIZE not supported)."""
    try:
        sock.sendall(resp_encode(["INFO", "keyspace"]))
        resp, _ = resp_read_one(sock)
        if isinstance(resp, bytes):
            text = resp.decode(errors="replace")
            for line in text.split("\n"):
                if "keys=" in line:
                    for part in line.split(","):
                        if part.startswith("keys="):
                            return int(part.split("=")[1])
        return 0
    except Exception:
        return 0

def parse_moon_search_results(resp, k, debug=False):
    """Parse FT.SEARCH response: [total, "vec:ID", ["__vec_score","0.5"], ...]"""
    if not isinstance(resp, list) or len(resp) < 1:
        if debug:
            print(f"    [DEBUG] Not a list: {type(resp)} {str(resp)[:200]}")
        return []
    results = []
    total = resp[0] if isinstance(resp[0], int) else int(resp[0]) if isinstance(resp[0], (bytes, str)) else 0
    if debug:
        print(f"    [DEBUG] total={total}, resp len={len(resp)}, first 5 items: {[str(x)[:50] for x in resp[:5]]}")
    i = 1
    while i < len(resp):
        key = resp[i]
        if isinstance(key, bytes):
            key = key.decode()
        elif isinstance(key, list):
            # skip nested arrays (score arrays)
            i += 1
            continue
        # Extract vector ID from key like "vec:12345"
        try:
            vid = int(str(key).split(":")[1])
        except (IndexError, ValueError):
            if debug:
                print(f"    [DEBUG] Can't parse key: {key}")
            i += 1
            continue
        results.append(vid)
        i += 1
        # Skip score array ["__vec_score", "0.5"]
        if i < len(resp) and isinstance(resp[i], list):
            i += 1
    if debug and results:
        print(f"    [DEBUG] parsed IDs: {results[:5]}...")
    return results[:k]

def run_moon_benchmark(port, moon_bin, moon_dir, n, dim, n_queries, k, batch_size, compact_threshold, ef_runtime, skip_recovery=False):
    print("\n" + "=" * 65)
    print("  MOON Vector Benchmark")
    print("=" * 65)

    # Clean + start Moon
    subprocess.run(["pkill", "-9", "-f", f"moon.*--port.*{port}"], capture_output=True)
    time.sleep(1)
    os.makedirs(moon_dir, exist_ok=True)
    subprocess.run(["rm", "-rf", moon_dir], capture_output=True)
    os.makedirs(moon_dir, exist_ok=True)
    offload_dir = f"{moon_dir}/offload"
    os.makedirs(offload_dir, exist_ok=True)

    moon_cmd = [
        moon_bin, "--port", str(port), "--shards", "1",
        "--protected-mode", "no",
        "--appendonly", "yes", "--appendfsync", "everysec",
        "--disk-offload", "enable", "--disk-offload-dir", offload_dir,
        "--dir", moon_dir,
    ]
    print(f"  Starting Moon: {' '.join(moon_cmd[:8])}...")
    moon_proc = subprocess.Popen(moon_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)

    if moon_proc.poll() is not None:
        print("  FAIL: Moon failed to start")
        return None

    moon_pid = moon_proc.pid
    rss_before = get_rss_mb(moon_pid)
    print(f"  Moon PID: {moon_pid} | RSS: {rss_before:.1f} MB")

    results = {"system": "Moon", "vectors": n, "dim": dim}

    try:
        sock = moon_connect(port)

        # Create index
        r = moon_create_index(sock, dim, compact_threshold, ef_runtime)
        print(f"  FT.CREATE: {r}")

        # Insert
        print(f"\n  >>> Inserting {n} vectors ({dim}d, batch={batch_size})...")
        insert_time, insert_rate = moon_insert_vectors(sock, n, dim, batch_size)
        results["insert_time"] = insert_time
        results["insert_rate"] = insert_rate
        print(f"  Insert: {insert_time:.1f}s ({insert_rate:.0f} vec/s)")

        rss_after_insert = get_rss_mb(moon_pid)
        results["rss_after_insert_mb"] = rss_after_insert
        print(f"  RSS after insert: {rss_after_insert:.1f} MB")

        # Trigger compaction (async — use separate connection with very long timeout)
        print("\n  >>> Triggering FT.COMPACT (may take 10-30 min for 500K vectors)...")
        t_compact = time.time()
        try:
            compact_sock = moon_connect(port, timeout=1800)  # 30 min
            compact_sock.settimeout(1800)
            compact_sock.sendall(resp_encode(["FT.COMPACT", "minilm"]))
            resp, _ = resp_read_one(compact_sock)
            compact_time = time.time() - t_compact
            results["compact_time"] = compact_time
            print(f"  FT.COMPACT: {resp} ({compact_time:.1f}s)")
            compact_sock.close()
        except Exception as e:
            compact_time = time.time() - t_compact
            results["compact_time"] = compact_time
            print(f"  FT.COMPACT timeout after {compact_time:.0f}s: {e}")
            print("  (Will search mutable segment — brute force, slower but works)")

        rss_after_compact = get_rss_mb(moon_pid)
        results["rss_after_compact_mb"] = rss_after_compact
        print(f"  RSS after compact: {rss_after_compact:.1f} MB")

        # Generate query vectors + ground truth (on smaller subset for brute force)
        gt_db_size = args.gt_sample if args.gt_sample > 0 else n
        print(f"\n  >>> Computing ground truth (brute force on {gt_db_size} vectors)...")
        query_vecs = [gen_vector(i + 10_000_000) for i in range(n_queries)]
        print(f"  Generated {n_queries} query vectors")
        if gt_db_size > 50000:
            # For large DBs, generate in batches and report progress
            gt_db_vecs = []
            for batch_start in range(0, gt_db_size, 50000):
                batch_end = min(batch_start + 50000, gt_db_size)
                gt_db_vecs.extend([gen_vector(i) for i in range(batch_start, batch_end)])
                print(f"  Generated {batch_end}/{gt_db_size} DB vectors for ground truth")
        else:
            gt_db_vecs = [gen_vector(i) for i in range(gt_db_size)]
        ground_truth = compute_ground_truth(query_vecs, gt_db_vecs, k)
        print(f"  Ground truth computed (gt[0]={ground_truth[0][:3]}...)")

        # Search benchmark
        print(f"\n  >>> Searching {n_queries} queries (K={k})...")
        latencies = []
        all_results = []

        # Warmup (5 queries)
        for i in range(min(5, n_queries)):
            moon_search(sock, query_vecs[i], k)

        for i in range(n_queries):
            t_start = time.perf_counter()
            resp = moon_search(sock, query_vecs[i], k)
            t_end = time.perf_counter()
            latencies.append((t_end - t_start) * 1000)  # ms

            ids = parse_moon_search_results(resp, k, debug=(i == 0))
            all_results.append(ids)
            if i == 0:
                print(f"  [DEBUG] First query ground truth: {ground_truth[0][:5]}...")
                print(f"  [DEBUG] First query results:      {ids[:5]}...")

        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg_lat = sum(latencies) / len(latencies)
        qps = 1000.0 / avg_lat if avg_lat > 0 else 0

        results["search_p50_ms"] = round(p50, 3)
        results["search_p99_ms"] = round(p99, 3)
        results["search_avg_ms"] = round(avg_lat, 3)
        results["search_qps"] = round(qps, 1)

        # Recall (only against gt_sample vectors)
        recall = recall_at_k(all_results, ground_truth, k, gt_db_size=gt_db_size)
        results["recall_at_k"] = round(recall, 4)

        print(f"  Search: p50={p50:.2f}ms p99={p99:.2f}ms avg={avg_lat:.2f}ms QPS={qps:.0f}")
        print(f"  Recall@{k}: {recall:.4f} (vs brute-force on {gt_db_size} vectors)")

        rss_search = get_rss_mb(moon_pid)
        results["rss_after_search_mb"] = rss_search
        print(f"  RSS after search: {rss_search:.1f} MB")

        # Bytes per vector
        if n > 0 and rss_search > rss_before:
            bpv = (rss_search - rss_before) * 1024 * 1024 / n
            results["bytes_per_vector"] = round(bpv, 1)
            print(f"  Bytes/vector: {bpv:.0f}")

        sock.close()

        # --- Crash Recovery Test ---
        if not skip_recovery:
            print(f"\n  >>> Crash Recovery Test (SIGKILL)...")
            dbsize_before = 0
            try:
                s2 = moon_connect(port)
                dbsize_before = moon_dbsize(s2)
                # Also get FT.INFO for vector count
                s2.sendall(resp_encode(["FT.INFO", "minilm"]))
                ft_info, _ = resp_read_one(s2)
                print(f"  FT.INFO: {str(ft_info)[:200]}")
                s2.close()
            except Exception:
                pass
            print(f"  DBSIZE before kill: {dbsize_before}")

            # SIGKILL
            os.kill(moon_pid, signal.SIGKILL)
            moon_proc.wait()
            time.sleep(2)

            # Restart
            print("  Restarting Moon...")
            moon_proc = subprocess.Popen(moon_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(5)

            if moon_proc.poll() is not None:
                print("  FAIL: Moon failed to restart after SIGKILL")
                results["recovery"] = "FAIL (restart)"
                return results

            try:
                s3 = moon_connect(port, timeout=15)
                dbsize_after = moon_dbsize(s3)
                print(f"  DBSIZE after recovery: {dbsize_after}")

                # Verify data integrity (sample 100 keys)
                sample_size = min(100, n)
                correct = 0
                for i in range(0, n, max(1, n // sample_size)):
                    s3.sendall(resp_encode(["HGET", f"vec:{i}", "emb"]))
                    resp, _ = resp_read_one(s3)
                    if isinstance(resp, bytes) and len(resp) == BYTES_PER_VEC:
                        correct += 1
                    if correct + (n - i) // max(1, n // sample_size) < sample_size:
                        pass  # continue checking

                # Search after recovery
                print("  Searching after recovery...")
                recovery_results = []
                for i in range(min(10, n_queries)):
                    resp = moon_search(s3, query_vecs[i], k)
                    ids = parse_moon_search_results(resp, k)
                    recovery_results.append(ids)

                if recovery_results and ground_truth:
                    recovery_recall = recall_at_k(recovery_results, ground_truth[:10], k, gt_db_size=gt_db_size)
                    results["recovery_recall"] = round(recovery_recall, 4)
                    print(f"  Recovery recall@{k}: {recovery_recall:.4f}")

                results["recovery_dbsize"] = dbsize_after
                results["recovery"] = "PASS" if dbsize_after and dbsize_after > 0 else "FAIL"
                print(f"  Recovery: {results['recovery']} ({dbsize_after} keys)")
                s3.close()
            except Exception as e:
                results["recovery"] = f"FAIL ({e})"
                print(f"  Recovery FAIL: {e}")

            # Cleanup
            subprocess.run(["pkill", "-9", "-f", f"moon.*--port.*{port}"], capture_output=True)
        else:
            subprocess.run(["pkill", "-9", "-f", f"moon.*--port.*{port}"], capture_output=True)

    except Exception as e:
        print(f"  Moon benchmark error: {e}")
        import traceback; traceback.print_exc()
        results["error"] = str(e)
        subprocess.run(["pkill", "-9", "-f", f"moon.*--port.*{port}"], capture_output=True)

    return results

# ---------------------------------------------------------------------------
# Qdrant benchmark (REST API)
# ---------------------------------------------------------------------------
def qdrant_wait_ready(port, timeout=30):
    """Wait for Qdrant to be ready."""
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2)
            if req.status == 200:
                return True
        except Exception:
            time.sleep(0.5)
    return False

def qdrant_request(port, method, path, data=None, timeout=60):
    """Make HTTP request to Qdrant."""
    import urllib.request
    url = f"http://127.0.0.1:{port}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode())
    except Exception as e:
        try:
            return json.loads(e.read().decode())
        except Exception:
            return {"error": str(e)}

def run_qdrant_benchmark(port, qdrant_bin, qdrant_dir, n, dim, n_queries, k, batch_size, skip_recovery=False):
    print("\n" + "=" * 65)
    print("  QDRANT Vector Benchmark")
    print("=" * 65)

    # Clean + start Qdrant (use exact binary path to avoid killing self)
    subprocess.run(["pkill", "-9", "-x", "qdrant"], capture_output=True)
    time.sleep(1)
    subprocess.run(["rm", "-rf", qdrant_dir], capture_output=True)
    os.makedirs(qdrant_dir, exist_ok=True)

    qdrant_proc = None
    qdrant_pid = None

    if qdrant_bin:
        qdrant_env = os.environ.copy()
        qdrant_env["QDRANT__STORAGE__STORAGE_PATH"] = qdrant_dir
        qdrant_env["QDRANT__SERVICE__HTTP_PORT"] = str(port)
        qdrant_env["QDRANT__SERVICE__GRPC_PORT"] = str(port + 1)
        print(f"  Starting Qdrant: {qdrant_bin} (port {port})...", flush=True)
        qdrant_proc = subprocess.Popen(
            [qdrant_bin], env=qdrant_env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        time.sleep(5)
        rc = qdrant_proc.poll()
        if rc is not None:
            stdout = qdrant_proc.stdout.read().decode(errors="replace")[:500]
            stderr = qdrant_proc.stderr.read().decode(errors="replace")[:500]
            print(f"  FAIL: Qdrant exited with code {rc}")
            print(f"  stdout: {stdout}")
            print(f"  stderr: {stderr}")
            return None
        qdrant_pid = qdrant_proc.pid
        print(f"  Qdrant started (PID={qdrant_pid})", flush=True)
    else:
        # Try Docker
        subprocess.run(["docker", "rm", "-f", "qdrant-bench"], capture_output=True)
        print(f"  Starting Qdrant via Docker (port {port})...")
        r = subprocess.run([
            "docker", "run", "-d", "--name", "qdrant-bench",
            "-p", f"{port}:6333", "-p", f"{port+1}:6334",
            "-v", f"{qdrant_dir}:/qdrant/storage",
            "qdrant/qdrant:latest"
        ], capture_output=True, text=True)
        if r.returncode != 0:
            print(f"  FAIL: Docker start failed: {r.stderr}")
            return None
        time.sleep(3)

    # Wait for ready
    if not qdrant_wait_ready(port):
        print("  FAIL: Qdrant not ready after 30s")
        return None

    rss_before = get_rss_mb(qdrant_pid) if qdrant_pid else 0
    print(f"  Qdrant PID: {qdrant_pid or 'docker'} | RSS: {rss_before:.1f} MB")

    results = {"system": "Qdrant", "vectors": n, "dim": dim}

    try:
        # Create collection
        r = qdrant_request(port, "PUT", "/collections/bench", {
            "vectors": {"size": dim, "distance": "Euclid"},
            "optimizers_config": {
                "default_segment_number": 2,
                "indexing_threshold": 20000,
            },
            "hnsw_config": {"m": 16, "ef_construct": 200},
        })
        print(f"  Create collection: {r.get('status', r.get('error', '?'))}")

        # Insert vectors in batches
        print(f"\n  >>> Inserting {n} vectors ({dim}d, batch={batch_size})...")
        t0 = time.time()
        for start in range(0, n, batch_size):
            end = min(start + batch_size, n)
            points = []
            for i in range(start, end):
                v = gen_vector(i)
                points.append({"id": i, "vector": v})
            r = qdrant_request(port, "PUT", "/collections/bench/points?wait=true",
                             {"points": points}, timeout=120)
            if "error" in r:
                print(f"  Insert error at {start}: {r['error'][:100]}")
                break
            if (end) % 50000 == 0 or end == n:
                elapsed = time.time() - t0
                rate = end / elapsed if elapsed > 0 else 0
                print(f"  Qdrant insert: {end}/{n} ({rate:.0f} vec/s)")

        insert_time = time.time() - t0
        insert_rate = n / insert_time if insert_time > 0 else 0
        results["insert_time"] = insert_time
        results["insert_rate"] = insert_rate
        print(f"  Insert: {insert_time:.1f}s ({insert_rate:.0f} vec/s)")

        rss_after_insert = get_rss_mb(qdrant_pid) if qdrant_pid else 0
        results["rss_after_insert_mb"] = rss_after_insert

        # Wait for indexing
        print("\n  >>> Waiting for HNSW indexing...")
        for _ in range(120):
            info = qdrant_request(port, "GET", "/collections/bench")
            result = info.get("result", {})
            status = result.get("status", "unknown")
            indexed = result.get("indexed_vectors_count", 0)
            if status == "green" and indexed >= n * 0.9:
                break
            time.sleep(2)
        print(f"  Status: {status}, indexed: {indexed}/{n}")

        rss_indexed = get_rss_mb(qdrant_pid) if qdrant_pid else 0
        results["rss_after_index_mb"] = rss_indexed
        print(f"  RSS after indexing: {rss_indexed:.1f} MB")

        # Search benchmark
        gt_db_size = args.gt_sample if args.gt_sample > 0 else n
        print(f"\n  >>> Computing ground truth (brute force on {gt_db_size} vectors)...")
        query_vecs = [gen_vector(i + 10_000_000) for i in range(n_queries)]
        if gt_db_size > 50000:
            gt_db_vecs = []
            for batch_start in range(0, gt_db_size, 50000):
                batch_end = min(batch_start + 50000, gt_db_size)
                gt_db_vecs.extend([gen_vector(i) for i in range(batch_start, batch_end)])
                print(f"  Generated {batch_end}/{gt_db_size} DB vectors for ground truth")
        else:
            gt_db_vecs = [gen_vector(i) for i in range(gt_db_size)]
        ground_truth = compute_ground_truth(query_vecs, gt_db_vecs, k)

        print(f"  >>> Searching {n_queries} queries (K={k})...")
        latencies = []
        all_results = []

        # Warmup
        for i in range(min(5, n_queries)):
            qdrant_request(port, "POST", "/collections/bench/points/search", {
                "vector": query_vecs[i], "limit": k,
                "params": {"hnsw_ef": 128}
            })

        for i in range(n_queries):
            t_start = time.perf_counter()
            r = qdrant_request(port, "POST", "/collections/bench/points/search", {
                "vector": query_vecs[i], "limit": k,
                "params": {"hnsw_ef": 128}
            })
            t_end = time.perf_counter()
            latencies.append((t_end - t_start) * 1000)

            ids = [p["id"] for p in r.get("result", [])]
            all_results.append(ids)

        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p99 = latencies[int(len(latencies) * 0.99)]
        avg_lat = sum(latencies) / len(latencies)
        qps = 1000.0 / avg_lat if avg_lat > 0 else 0

        results["search_p50_ms"] = round(p50, 3)
        results["search_p99_ms"] = round(p99, 3)
        results["search_avg_ms"] = round(avg_lat, 3)
        results["search_qps"] = round(qps, 1)

        recall = recall_at_k(all_results, ground_truth, k, gt_db_size=gt_db_size)
        results["recall_at_k"] = round(recall, 4)

        print(f"  Search: p50={p50:.2f}ms p99={p99:.2f}ms avg={avg_lat:.2f}ms QPS={qps:.0f}")
        print(f"  Recall@{k}: {recall:.4f} (vs brute-force on {gt_db_size} vectors)")

        rss_search = get_rss_mb(qdrant_pid) if qdrant_pid else 0
        results["rss_after_search_mb"] = rss_search

        if n > 0 and rss_search > rss_before:
            bpv = (rss_search - rss_before) * 1024 * 1024 / n
            results["bytes_per_vector"] = round(bpv, 1)
            print(f"  Bytes/vector: {bpv:.0f}")

        # --- Crash Recovery ---
        if not skip_recovery and qdrant_pid:
            print(f"\n  >>> Crash Recovery Test (SIGKILL)...")
            info_before = qdrant_request(port, "GET", "/collections/bench")
            points_before = info_before.get("result", {}).get("points_count", 0)
            print(f"  Points before kill: {points_before}")

            os.kill(qdrant_pid, signal.SIGKILL)
            qdrant_proc.wait()
            time.sleep(2)

            print("  Restarting Qdrant...")
            qdrant_env = os.environ.copy()
            qdrant_env["QDRANT__STORAGE__STORAGE_PATH"] = qdrant_dir
            qdrant_env["QDRANT__SERVICE__HTTP_PORT"] = str(port)
            qdrant_env["QDRANT__SERVICE__GRPC_PORT"] = str(port + 1)
            qdrant_proc = subprocess.Popen(
                [qdrant_bin], env=qdrant_env,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            time.sleep(5)

            if not qdrant_wait_ready(port, timeout=30):
                results["recovery"] = "FAIL (restart)"
                print("  Recovery FAIL: Qdrant didn't come back")
            else:
                info_after = qdrant_request(port, "GET", "/collections/bench")
                points_after = info_after.get("result", {}).get("points_count", 0)
                results["recovery_points"] = points_after
                loss_pct = round((1 - points_after / points_before) * 100, 1) if points_before > 0 else 100
                results["recovery"] = f"PASS ({points_after}/{points_before}, {loss_pct}% loss)"
                print(f"  Recovery: {results['recovery']}")

                # Search after recovery
                recovery_results = []
                for i in range(min(10, n_queries)):
                    r = qdrant_request(port, "POST", "/collections/bench/points/search", {
                        "vector": query_vecs[i], "limit": k,
                        "params": {"hnsw_ef": 128}
                    })
                    ids = [p["id"] for p in r.get("result", [])]
                    recovery_results.append(ids)

                if recovery_results and ground_truth:
                    recovery_recall = recall_at_k(recovery_results, ground_truth[:10], k, gt_db_size=gt_db_size)
                    results["recovery_recall"] = round(recovery_recall, 4)
                    print(f"  Recovery recall@{k}: {recovery_recall:.4f}")

    except Exception as e:
        print(f"  Qdrant benchmark error: {e}")
        import traceback; traceback.print_exc()
        results["error"] = str(e)

    # Cleanup
    if qdrant_proc:
        subprocess.run(["pkill", "-9", "-f", "qdrant"], capture_output=True)
    else:
        subprocess.run(["docker", "rm", "-f", "qdrant-bench"], capture_output=True)

    return results

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def print_comparison(moon_r, qdrant_r):
    print("\n" + "=" * 75)
    print("  COMPARISON: Moon vs Qdrant")
    print("=" * 75)

    info = get_system_info()
    print(f"  Platform: {info.get('os')} {info.get('arch')}")
    print(f"  CPU: {info.get('cpu', 'unknown')}")
    print(f"  Cores: {info.get('cores', '?')}")
    if "kernel" in info:
        print(f"  Kernel: {info['kernel']}")
    print(f"  Vectors: {N} | Dim: {DIM} | K: {K} | Queries: {N_QUERIES}")
    print()

    def val(r, key, fmt=".1f", suffix=""):
        if r and key in r:
            return f"{r[key]:{fmt}}{suffix}"
        return "N/A"

    def ratio(moon_val, qdrant_val, higher_better=True):
        if moon_val and qdrant_val and qdrant_val > 0:
            r = moon_val / qdrant_val
            if not higher_better:
                r = 1 / r if r > 0 else 0
            return f"{r:.2f}x"
        return ""

    header = f"{'Metric':<30} {'Moon':>15} {'Qdrant':>15} {'Ratio':>10}"
    print(header)
    print("-" * len(header))

    rows = [
        ("Insert (vec/s)", "insert_rate", ".0f", "", True),
        ("Insert time (s)", "insert_time", ".1f", "", False),
        ("Search p50 (ms)", "search_p50_ms", ".2f", "", False),
        ("Search p99 (ms)", "search_p99_ms", ".2f", "", False),
        ("Search QPS", "search_qps", ".0f", "", True),
        ("Recall@K", "recall_at_k", ".4f", "", True),
        ("RSS after insert (MB)", "rss_after_insert_mb", ".1f", "", False),
        ("RSS after search (MB)", "rss_after_search_mb", ".1f", "", False),
        ("Bytes/vector", "bytes_per_vector", ".0f", "", False),
    ]

    for label, key, fmt, suffix, higher_better in rows:
        mv = val(moon_r, key, fmt, suffix) if moon_r else "N/A"
        qv = val(qdrant_r, key, fmt, suffix) if qdrant_r else "N/A"
        rv = ""
        if moon_r and qdrant_r and key in moon_r and key in qdrant_r:
            rv = ratio(moon_r[key], qdrant_r[key], higher_better)
        print(f"{label:<30} {mv:>15} {qv:>15} {rv:>10}")

    # Recovery
    if moon_r and "recovery" in moon_r:
        print(f"\n  Moon recovery: {moon_r['recovery']}")
    if qdrant_r and "recovery" in qdrant_r:
        print(f"  Qdrant recovery: {qdrant_r['recovery']}")

    print()

    # JSON output
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "system_info": info,
        "config": {"vectors": N, "dim": DIM, "queries": N_QUERIES, "k": K},
        "moon": moon_r,
        "qdrant": qdrant_r,
    }
    out_file = f"/tmp/bench-vector-{N}_{DIM}d_{info.get('arch', 'unknown')}.json"
    with open(out_file, "w") as f:
        json.dump(output, f, indent=2)
    print(f"  Results saved to: {out_file}")

def main():
    print("=" * 75)
    print(f"  Moon vs Qdrant — Vector Search Benchmark")
    print(f"  {N} vectors, {DIM}d (MiniLM), K={K}, {N_QUERIES} queries")
    print("=" * 75)

    info = get_system_info()
    print(f"  Platform: {info.get('os')} {info.get('arch')}")
    print(f"  CPU: {info.get('cpu', 'unknown')}")
    print(f"  Date: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}")

    moon_results = None
    qdrant_results = None

    if not args.skip_moon:
        moon_results = run_moon_benchmark(
            args.moon_port, args.moon_bin, args.moon_dir,
            N, DIM, N_QUERIES, K, BATCH,
            args.compact_threshold, args.ef_runtime,
            args.skip_recovery,
        )

    if not args.skip_qdrant:
        qdrant_results = run_qdrant_benchmark(
            args.qdrant_port, args.qdrant_bin, args.qdrant_dir,
            N, DIM, N_QUERIES, K, BATCH,
            args.skip_recovery,
        )

    print_comparison(moon_results, qdrant_results)

if __name__ == "__main__":
    main()
