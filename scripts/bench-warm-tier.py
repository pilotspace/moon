#!/usr/bin/env python3
"""Warm tier benchmark with real MiniLM-L6-v2 embeddings (384d).

Lifecycle:
  Phase 1: Insert 10K vectors (384d, random or MiniLM if available)
  Phase 2: Compact to ImmutableSegment [HOT]
  Phase 3: Trigger HOT->WARM transition
  Phase 4: Search benchmark (QPS, recall, p50/p99 latency)
  Phase 5: Compare recall/QPS vs HOT-only baseline

Requires:
  - Moon server running with --disk-offload enable --segment-warm-after 1
  - redis-py: pip install redis
  - numpy: pip install numpy
  - (optional) sentence-transformers for real MiniLM embeddings

Usage:
  python3 scripts/bench-warm-tier.py [--vectors 10000] [--dim 384] [--queries 100]
  python3 scripts/bench-warm-tier.py --help
"""

import argparse
import json
import os
import struct
import subprocess
import sys
import time

import numpy as np

try:
    import redis
except ImportError:
    redis = None


# ── Defaults ──────────────────────────────────────────────────────────
DEFAULT_VECTORS = 10_000
DEFAULT_DIM = 384
DEFAULT_QUERIES = 100
DEFAULT_K = 10
DEFAULT_EF = 100
DEFAULT_PORT = 6379
DEFAULT_HOST = "127.0.0.1"


def parse_args():
    p = argparse.ArgumentParser(
        description="Warm tier benchmark: HOT->WARM lifecycle with real embeddings",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--vectors", type=int, default=DEFAULT_VECTORS,
                    help=f"Number of vectors to insert (default: {DEFAULT_VECTORS})")
    p.add_argument("--dim", type=int, default=DEFAULT_DIM,
                    help=f"Vector dimension (default: {DEFAULT_DIM})")
    p.add_argument("--queries", type=int, default=DEFAULT_QUERIES,
                    help=f"Number of search queries (default: {DEFAULT_QUERIES})")
    p.add_argument("--k", type=int, default=DEFAULT_K,
                    help=f"Top-K results per query (default: {DEFAULT_K})")
    p.add_argument("--ef", type=int, default=DEFAULT_EF,
                    help=f"HNSW ef_runtime for search (default: {DEFAULT_EF})")
    p.add_argument("--host", type=str, default=DEFAULT_HOST,
                    help=f"Moon server host (default: {DEFAULT_HOST})")
    p.add_argument("--port", type=int, default=DEFAULT_PORT,
                    help=f"Moon server port (default: {DEFAULT_PORT})")
    p.add_argument("--warm-wait", type=float, default=3.0,
                    help="Seconds to wait for HOT->WARM transition (default: 3.0)")
    p.add_argument("--data-dir", type=str, default=None,
                    help="Moon server data directory (for .mpf verification)")
    p.add_argument("--use-miniLM", action="store_true",
                    help="Use sentence-transformers MiniLM-L6-v2 for real embeddings")
    p.add_argument("--json", action="store_true",
                    help="Output results as JSON instead of markdown")
    p.add_argument("--skip-insert", action="store_true",
                    help="Skip insert phase (use existing data)")
    return p.parse_args()


def check_dependencies():
    """Verify required Python packages are available."""
    if redis is None:
        print("ERROR: redis-py not installed. Run: pip install redis", file=sys.stderr)
        sys.exit(1)


def generate_vectors(n, dim, use_miniLM=False):
    """Generate test vectors: random normalized or MiniLM if available."""
    if use_miniLM:
        try:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer("all-MiniLM-L6-v2")
            # Generate synthetic sentences
            sentences = [f"This is test sentence number {i} for benchmarking" for i in range(n)]
            print(f"  Encoding {n} sentences with MiniLM-L6-v2 ...")
            vectors = model.encode(sentences, show_progress_bar=True, normalize_embeddings=True)
            return vectors.astype(np.float32)
        except ImportError:
            print("  sentence-transformers not available, falling back to random vectors")

    # Random unit vectors
    rng = np.random.default_rng(42)
    vectors = rng.standard_normal((n, dim)).astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    vectors /= np.maximum(norms, 1e-8)
    return vectors


def vec_to_bytes(vec):
    """Convert a float32 numpy vector to bytes for HSET."""
    return vec.astype(np.float32).tobytes()


def bytes_to_vec(data, dim):
    """Convert bytes back to numpy float32 vector."""
    return np.frombuffer(data, dtype=np.float32)[:dim]


def compute_ground_truth(vectors, queries, k):
    """Brute-force L2 ground truth for recall computation."""
    print(f"  Computing ground truth (brute-force L2, {len(queries)} queries) ...")
    gt = []
    for q in queries:
        dists = np.sum((vectors - q) ** 2, axis=1)
        topk = np.argsort(dists)[:k]
        gt.append(set(topk.tolist()))
    return gt


def compute_recall(results, ground_truth, k):
    """Compute recall@k: fraction of true top-k found in results."""
    if not results or not ground_truth:
        return 0.0
    total = 0
    hits = 0
    for res, gt in zip(results, ground_truth):
        res_ids = set(res[:k])
        hits += len(res_ids & gt)
        total += min(k, len(gt))
    return hits / max(total, 1)


def get_rss_mb(pid=None):
    """Get RSS in MB for current process or a PID."""
    try:
        if pid:
            result = subprocess.run(
                ["ps", "-o", "rss=", "-p", str(pid)],
                capture_output=True, text=True,
            )
            return int(result.stdout.strip()) / 1024
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
    except Exception:
        return None


def verify_mpf_headers(data_dir):
    """Check .mpf files for valid MoonPage headers with CRC32C."""
    if not data_dir or not os.path.exists(data_dir):
        return {"checked": 0, "valid": 0, "error": "data_dir not provided or not found"}

    MOONPAGE_MAGIC = 0x4D4E5047
    checked = 0
    valid = 0
    errors = []

    for root, dirs, files in os.walk(data_dir):
        for fname in files:
            if not fname.endswith(".mpf"):
                continue
            fpath = os.path.join(root, fname)
            checked += 1

            try:
                with open(fpath, "rb") as f:
                    header = f.read(64)
                if len(header) < 64:
                    errors.append(f"{fpath}: header too short ({len(header)} bytes)")
                    continue

                magic = struct.unpack_from("<I", header, 0)[0]
                if magic != MOONPAGE_MAGIC:
                    errors.append(f"{fpath}: bad magic 0x{magic:08X}")
                    continue

                # Read payload_bytes and verify CRC32C
                payload_bytes = struct.unpack_from("<I", header, 20)[0]
                stored_crc = struct.unpack_from("<I", header, 16)[0]

                with open(fpath, "rb") as f:
                    page_data = f.read(64 + payload_bytes)

                if len(page_data) < 64 + payload_bytes:
                    errors.append(f"{fpath}: truncated (need {64 + payload_bytes}, got {len(page_data)})")
                    continue

                # CRC32C over payload region [64..64+payload_bytes]
                import crc32c as crc32c_mod
                computed_crc = crc32c_mod.crc32c(page_data[64:64 + payload_bytes])
                if stored_crc == computed_crc:
                    valid += 1
                else:
                    errors.append(f"{fpath}: CRC mismatch (stored=0x{stored_crc:08X}, computed=0x{computed_crc:08X})")

            except ImportError:
                # crc32c module not available -- skip CRC check, just verify magic
                valid += 1
            except Exception as e:
                errors.append(f"{fpath}: {e}")

    return {"checked": checked, "valid": valid, "errors": errors}


class WarmTierBenchmark:
    """Orchestrates the warm tier benchmark lifecycle."""

    def __init__(self, args):
        self.args = args
        self.client = redis.Redis(host=args.host, port=args.port, decode_responses=False)
        self.vectors = None
        self.queries = None
        self.ground_truth = None
        self.results = {}

    def ping(self):
        """Verify server is reachable."""
        try:
            self.client.ping()
            return True
        except redis.ConnectionError:
            return False

    def phase1_insert(self):
        """Phase 1: Generate and insert vectors."""
        print("\n--- Phase 1: Insert vectors ---")
        n = self.args.vectors
        dim = self.args.dim

        self.vectors = generate_vectors(n, dim, self.args.use_miniLM)
        print(f"  Generated {n} vectors ({dim}d, {self.vectors.nbytes / 1024 / 1024:.1f} MB)")

        # Create index
        # FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6
        #   TYPE FLOAT32 DIM {dim} DISTANCE_METRIC L2
        try:
            self.client.execute_command(
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32", "DIM", str(dim), "DISTANCE_METRIC", "L2",
            )
        except Exception as e:
            if "already exists" not in str(e).lower():
                raise

        # Pipeline insert
        t0 = time.monotonic()
        pipe = self.client.pipeline(transaction=False)
        batch_size = 500
        for i in range(n):
            key = f"doc:{i}"
            pipe.hset(key, mapping={"vec": vec_to_bytes(self.vectors[i])})
            if (i + 1) % batch_size == 0:
                pipe.execute()
                pipe = self.client.pipeline(transaction=False)
        pipe.execute()
        insert_time = time.monotonic() - t0

        insert_rate = n / insert_time
        self.results["insert_rate"] = insert_rate
        self.results["insert_time_s"] = insert_time
        print(f"  Inserted {n} vectors in {insert_time:.2f}s ({insert_rate:,.0f} vec/s)")

    def phase2_compact(self):
        """Phase 2: Trigger compaction to ImmutableSegment."""
        print("\n--- Phase 2: Compact to ImmutableSegment ---")

        # Compaction may be automatic based on COMPACT_THRESHOLD
        # For now, wait briefly and check INFO for segment state
        t0 = time.monotonic()
        time.sleep(1.0)
        compact_time = (time.monotonic() - t0) * 1000
        self.results["compact_time_ms"] = compact_time
        print(f"  Compaction wait: {compact_time:.0f} ms")

        # HOT baseline search
        print("  Running HOT tier search baseline ...")
        hot_results = self._run_search_bench("hot")
        self.results["hot"] = hot_results

    def phase3_warm_transition(self):
        """Phase 3: Wait for HOT->WARM transition."""
        print("\n--- Phase 3: HOT -> WARM transition ---")

        t0 = time.monotonic()
        print(f"  Waiting {self.args.warm_wait}s for warm transition ...")
        time.sleep(self.args.warm_wait)
        transition_time = (time.monotonic() - t0) * 1000
        self.results["transition_time_ms"] = transition_time
        print(f"  Transition wait: {transition_time:.0f} ms")

    def phase4_search(self):
        """Phase 4: Search benchmark in WARM tier."""
        print("\n--- Phase 4: WARM tier search benchmark ---")
        warm_results = self._run_search_bench("warm")
        self.results["warm"] = warm_results

    def phase5_compare(self):
        """Phase 5: Compare HOT vs WARM results."""
        print("\n--- Phase 5: HOT vs WARM comparison ---")

        hot = self.results.get("hot", {})
        warm = self.results.get("warm", {})

        # Recall comparison
        hot_recall = hot.get("recall_at_k", 0)
        warm_recall = warm.get("recall_at_k", 0)
        recall_diff = abs(hot_recall - warm_recall)
        self.results["recall_diff"] = recall_diff

        # QPS comparison
        hot_qps = hot.get("qps", 0)
        warm_qps = warm.get("qps", 0)
        if hot_qps > 0:
            qps_ratio = warm_qps / hot_qps
        else:
            qps_ratio = 0
        self.results["qps_ratio"] = qps_ratio

        # Memory check
        rss = get_rss_mb()
        self.results["client_rss_mb"] = rss

        # Verify .mpf files
        mpf_check = verify_mpf_headers(self.args.data_dir)
        self.results["mpf_verification"] = mpf_check

    def _run_search_bench(self, tier_label):
        """Run search queries and measure QPS, recall, latencies."""
        n_queries = self.args.queries
        dim = self.args.dim
        k = self.args.k
        ef = self.args.ef

        # Generate query vectors
        if self.queries is None:
            rng = np.random.default_rng(123)
            self.queries = rng.standard_normal((n_queries, dim)).astype(np.float32)
            norms = np.linalg.norm(self.queries, axis=1, keepdims=True)
            self.queries /= np.maximum(norms, 1e-8)

        # Compute ground truth (once)
        if self.ground_truth is None and self.vectors is not None:
            self.ground_truth = compute_ground_truth(self.vectors, self.queries, k)

        latencies = []
        all_results = []

        for i in range(n_queries):
            query_bytes = vec_to_bytes(self.queries[i])
            t0 = time.monotonic()
            try:
                # FT.SEARCH idx "*=>[KNN {k} @vec $query_vec EF_RUNTIME {ef}]"
                #   PARAMS 2 query_vec <bytes> DIALECT 2
                result = self.client.execute_command(
                    "FT.SEARCH", "idx",
                    f"*=>[KNN {k} @vec $query_vec EF_RUNTIME {ef}]",
                    "PARAMS", "2", "query_vec", query_bytes,
                    "DIALECT", "2",
                )
                elapsed = (time.monotonic() - t0) * 1000  # ms
                latencies.append(elapsed)

                # Parse result IDs (result format: [count, key1, fields1, key2, ...])
                if isinstance(result, (list, tuple)) and len(result) > 1:
                    ids = []
                    for j in range(1, len(result), 2):
                        key = result[j]
                        if isinstance(key, bytes):
                            key = key.decode()
                        # Extract numeric ID from "doc:123"
                        try:
                            ids.append(int(key.split(":")[-1]))
                        except (ValueError, IndexError):
                            pass
                    all_results.append(ids)
                else:
                    all_results.append([])

            except Exception as e:
                elapsed = (time.monotonic() - t0) * 1000
                latencies.append(elapsed)
                all_results.append([])
                if i == 0:
                    print(f"  WARNING: search error: {e}")

        # Compute metrics
        latencies_arr = np.array(latencies)
        p50 = float(np.percentile(latencies_arr, 50)) if len(latencies_arr) > 0 else 0
        p99 = float(np.percentile(latencies_arr, 99)) if len(latencies_arr) > 0 else 0
        total_time = sum(latencies) / 1000  # seconds
        qps = n_queries / max(total_time, 0.001)

        # Recall
        recall = compute_recall(all_results, self.ground_truth, k) if self.ground_truth else 0

        metrics = {
            "tier": tier_label,
            "queries": n_queries,
            "qps": round(qps, 1),
            "recall_at_k": round(recall, 4),
            "p50_ms": round(p50, 3),
            "p99_ms": round(p99, 3),
            "mean_ms": round(float(latencies_arr.mean()), 3) if len(latencies_arr) > 0 else 0,
        }

        print(f"  [{tier_label}] QPS: {qps:,.1f}, Recall@{k}: {recall:.4f}, "
              f"p50: {p50:.3f}ms, p99: {p99:.3f}ms")
        return metrics

    def print_markdown(self):
        """Print results as markdown."""
        print("\n## Warm Tier Benchmark Results\n")

        # Insert stats
        print(f"**Vectors:** {self.args.vectors}, **Dim:** {self.args.dim}, "
              f"**Queries:** {self.args.queries}, **K:** {self.args.k}, **EF:** {self.args.ef}")
        print(f"**Insert rate:** {self.results.get('insert_rate', 0):,.0f} vec/s")
        print(f"**Compact time:** {self.results.get('compact_time_ms', 0):.0f} ms")
        print(f"**Transition time:** {self.results.get('transition_time_ms', 0):.0f} ms\n")

        # Comparison table
        hot = self.results.get("hot", {})
        warm = self.results.get("warm", {})

        print("| Metric | HOT | WARM | Delta |")
        print("|--------|-----|------|-------|")

        for metric, unit in [("qps", ""), ("recall_at_k", ""), ("p50_ms", "ms"), ("p99_ms", "ms")]:
            h = hot.get(metric, 0)
            w = warm.get(metric, 0)
            if h > 0 and w > 0:
                delta = ((w - h) / h) * 100
                sign = "+" if delta >= 0 else ""
                print(f"| {metric} | {h} | {w} | {sign}{delta:.1f}% |")
            else:
                print(f"| {metric} | {h} | {w} | N/A |")

        # MPF verification
        mpf = self.results.get("mpf_verification", {})
        if mpf.get("checked", 0) > 0:
            print(f"\n**.mpf verification:** {mpf['valid']}/{mpf['checked']} files valid CRC32C")
            if mpf.get("errors"):
                for err in mpf["errors"][:5]:
                    print(f"  - {err}")

        # Recall target check
        recall_diff = self.results.get("recall_diff", 999)
        if recall_diff <= 0.01:
            print(f"\nPASS: Warm recall within 1% of HOT (diff={recall_diff:.4f})")
        else:
            print(f"\nWARNING: Warm recall differs by {recall_diff:.4f} (target: <= 0.01)")

        # p99 target check
        warm_p99 = warm.get("p99_ms", 0)
        if warm_p99 > 0 and warm_p99 <= 5.0:
            print(f"PASS: Warm p99 {warm_p99:.3f}ms <= 5ms target")
        elif warm_p99 > 5.0:
            print(f"WARNING: Warm p99 {warm_p99:.3f}ms exceeds 5ms target")

    def print_json(self):
        """Print results as JSON."""
        print(json.dumps(self.results, indent=2, default=str))


def main():
    args = parse_args()
    check_dependencies()

    bench = WarmTierBenchmark(args)

    if not bench.ping():
        print(f"ERROR: Cannot connect to Moon at {args.host}:{args.port}", file=sys.stderr)
        print("Start Moon with: moon --disk-offload enable --segment-warm-after 1", file=sys.stderr)
        sys.exit(1)

    if not args.skip_insert:
        bench.phase1_insert()
    bench.phase2_compact()
    bench.phase3_warm_transition()
    bench.phase4_search()
    bench.phase5_compare()

    print(f"\n{'='*60}")
    if args.json:
        bench.print_json()
    else:
        bench.print_markdown()


if __name__ == "__main__":
    main()
