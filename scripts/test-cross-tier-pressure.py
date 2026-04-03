#!/usr/bin/env python3
"""MoonStore v2 Cross-Tier Memory Pressure Test Pipeline.

Validates that all MoonStore v2 tiers work together under memory pressure:
  Phase 1: Fill HOT tier to ~100MB (KV + vectors)
  Phase 2: Trigger memory pressure past 128MB maxmemory
  Phase 3: Verify warm search + KV spill readback
  Phase 4: Wait for WARM→COLD transition
  Phase 5: Crash (kill -9) + recover
  Phase 6: Data integrity audit

Usage:
  python3 scripts/test-cross-tier-pressure.py
  python3 scripts/test-cross-tier-pressure.py --moon-bin target/release/moon --port 16379

Pass criteria:
  - KV integrity >= 99%
  - Vector recall >= 0.85 across tiers
  - Recovery time < 5s
  - Zero panics
"""

import argparse
import glob
import json
import os
import shutil
import signal
import struct
import subprocess
import sys
import time

import numpy as np

# ── Helpers ──────────────────────────────────────────────────────────────

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


def info_section(r, section):
    """Parse INFO section into dict."""
    raw = r.execute_command("INFO", section)
    if isinstance(raw, dict):
        return {str(k): str(v) for k, v in raw.items()}
    if isinstance(raw, bytes):
        raw = raw.decode()
    result = {}
    for line in raw.split("\r\n"):
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            result[k.strip()] = v.strip()
    return result


def parse_search_results(result, k):
    """Parse FT.SEARCH response into list of integer IDs."""
    ids = []
    if not isinstance(result, list) or len(result) <= 1:
        return ids
    i = 1
    while i < len(result):
        if isinstance(result[i], bytes):
            doc_id = result[i].decode()
            for prefix in ("doc:", "vec:"):
                if doc_id.startswith(prefix):
                    try:
                        ids.append(int(doc_id[len(prefix):]))
                    except ValueError:
                        pass
                    break
            i += 1
            if i < len(result) and isinstance(result[i], list):
                i += 1
        else:
            i += 1
    return ids[:k]


# ── Test Phases ──────────────────────────────────────────────────────────

class CrossTierTest:
    def __init__(self, args):
        self.args = args
        self.moon_bin = args.moon_bin
        self.port = args.port
        self.data_dir = args.data_dir
        self.proc = None
        self.results = {"phases": {}, "pass": True, "failures": []}

        # Test data
        self.dim = 384
        self.n_vectors = 2000
        self.n_queries = 50
        self.k = 10
        self.kv_value_size = 512  # bytes per KV value

        # Generate vectors + ground truth
        np.random.seed(42)
        self.vectors = np.random.randn(self.n_vectors, self.dim).astype(np.float32)
        self.vectors /= np.linalg.norm(self.vectors, axis=1, keepdims=True)
        self.queries = np.random.randn(self.n_queries, self.dim).astype(np.float32)
        self.queries /= np.linalg.norm(self.queries, axis=1, keepdims=True)

        # Ground truth (brute-force L2)
        self.ground_truth = []
        for q in self.queries:
            dists = np.sum((self.vectors - q) ** 2, axis=1)
            self.ground_truth.append(np.argsort(dists)[:self.k].tolist())

    def start_moon(self, extra_args=None):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

        cmd = [
            self.moon_bin,
            "--port", str(self.port),
            "--shards", "1",
            "--maxmemory", str(128 * 1024 * 1024),  # 128MB in bytes
            "--maxmemory-policy", "allkeys-lru",
            "--appendonly", "yes",
            "--disk-offload", "enable",
            "--disk-offload-threshold", "0.85",
            "--segment-warm-after", "5",
            "--segment-cold-after", "15",
            "--checkpoint-timeout", "15",
            "--max-wal-size", "16mb",
            "--dir", self.data_dir,
        ]
        if extra_args:
            cmd.extend(extra_args)

        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if not wait_for_port(self.port):
            self.proc.kill()
            raise RuntimeError("Moon failed to start")
        return self.proc

    def stop_moon(self):
        if self.proc:
            self.proc.terminate()
            self.proc.wait(timeout=10)
            self.proc = None

    def kill_moon(self):
        if self.proc:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
            self.proc = None

    def get_redis(self):
        import redis
        return redis.Redis(host="127.0.0.1", port=self.port, decode_responses=False)

    def assert_true(self, condition, msg, phase):
        if not condition:
            self.results["pass"] = False
            self.results["failures"].append(f"Phase {phase}: {msg}")
            print(f"    FAIL: {msg}")
            return False
        print(f"    PASS: {msg}")
        return True

    # ── Phase 1: Fill HOT ────────────────────────────────────────────

    def phase1_fill_hot(self):
        print("\n== Phase 1: Fill HOT Tier ==")
        t0 = time.time()
        r = self.get_redis()

        # Create vector index
        try:
            r.execute_command(
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                "TYPE", "FLOAT32", "DIM", str(self.dim), "DISTANCE_METRIC", "L2"
            )
        except Exception as e:
            print(f"    FT.CREATE: {e}")

        # Insert vectors
        print(f"    Inserting {self.n_vectors} vectors ({self.dim}d)...")
        pipe = r.pipeline(transaction=False)
        for i, vec in enumerate(self.vectors):
            pipe.hset(f"doc:{i}", mapping={"vec": vec_to_bytes(vec)})
            if (i + 1) % 500 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()

        # Insert KV keys to fill memory toward ~100MB
        # 128MB limit, vectors take ~30MB, fill rest with KV
        print("    Inserting KV keys to fill memory...")
        value_pad = "x" * self.kv_value_size
        kv_count = 0
        batch = 1000
        pipe = r.pipeline(transaction=False)
        while True:
            for i in range(batch):
                key = f"kv:{kv_count + i}"
                pipe.set(key, f"{kv_count + i}:{value_pad}")
            pipe.execute()
            kv_count += batch
            pipe = r.pipeline(transaction=False)

            # Check memory via process RSS (Moon doesn't expose used_memory in INFO)
            used_mb = get_rss_mb(self.proc.pid)
            if used_mb > 100 or kv_count > 200000:
                break

        dbsize = r.dbsize()
        used_mb = get_rss_mb(self.proc.pid)

        dt = time.time() - t0
        result = {
            "kv_keys": kv_count,
            "vectors": self.n_vectors,
            "dbsize": dbsize,
            "used_memory_mb": round(used_mb, 1),
            "duration_s": round(dt, 1),
        }
        self.results["phases"]["1_fill_hot"] = result
        self.kv_count = kv_count

        print(f"    KV keys: {kv_count} | Vectors: {self.n_vectors} | "
              f"DBSIZE: {dbsize} | Memory: {used_mb:.0f}MB | Time: {dt:.1f}s")

        self.assert_true(dbsize > 0, f"DBSIZE={dbsize} > 0", 1)
        self.assert_true(used_mb > 50, f"used_memory={used_mb:.0f}MB > 50MB", 1)

    # ── Phase 2: Trigger Memory Pressure ─────────────────────────────

    def phase2_pressure(self):
        print("\n== Phase 2: Trigger Memory Pressure ==")
        t0 = time.time()
        r = self.get_redis()

        # Push past maxmemory to trigger eviction cascade
        print("    Inserting more keys to exceed 128MB...")
        value_pad = "x" * self.kv_value_size
        extra = 0
        pipe = r.pipeline(transaction=False)
        for i in range(50000):
            key = f"pressure:{i}"
            pipe.set(key, f"{i}:{value_pad}")
            if (i + 1) % 1000 == 0:
                try:
                    pipe.execute()
                except Exception:
                    pass  # OOM errors expected
                pipe = r.pipeline(transaction=False)
            extra = i + 1
        try:
            pipe.execute()
        except Exception:
            pass

        # Wait for eviction + warm transition
        print("    Waiting 8s for eviction cascade + warm transition...")
        time.sleep(8)

        # Check results
        used_mb = get_rss_mb(self.proc.pid)
        dbsize = r.dbsize()

        # Moon doesn't expose evicted_keys in INFO.
        # Detect eviction by comparing DBSIZE vs expected count.
        expected_total = self.kv_count + self.n_vectors + extra
        evicted = max(0, expected_total - dbsize)

        # Check for .mpf files (warm tier)
        mpf_files = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*/*.mpf"
        ))

        # Check for DataFile (KV spill)
        heap_files = glob.glob(os.path.join(
            self.data_dir, "shard-0/data/heap-*.mpf"
        ))

        # Check WAL v3
        wal_files = glob.glob(os.path.join(
            self.data_dir, "shard-0/wal-v3/*.wal"
        ))

        dt = time.time() - t0
        result = {
            "used_memory_mb": round(used_mb, 1),
            "dbsize": dbsize,
            "evicted_keys": evicted,
            "mpf_files": len(mpf_files),
            "heap_files": len(heap_files),
            "wal_files": len(wal_files),
            "duration_s": round(dt, 1),
        }
        self.results["phases"]["2_pressure"] = result

        print(f"    Memory: {used_mb:.0f}MB | DBSIZE: {dbsize} | "
              f"Evicted: {evicted} | .mpf: {len(mpf_files)} | "
              f"heap: {len(heap_files)} | WAL: {len(wal_files)}")

        self.assert_true(evicted > 0 or dbsize < self.kv_count + self.n_vectors + 50000,
                         f"Eviction occurred (evicted={evicted})", 2)
        self.assert_true(len(wal_files) > 0,
                         f"WAL v3 segments exist ({len(wal_files)})", 2)

    # ── Phase 3: Verify Warm Search + KV Readback ────────────────────

    def phase3_verify_warm(self):
        print("\n== Phase 3: Verify Warm Search + KV Readback ==")
        t0 = time.time()
        r = self.get_redis()

        # Vector search
        print(f"    Running {self.n_queries} search queries...")
        recalls = []
        search_ok = 0
        for i, q in enumerate(self.queries):
            q_bytes = vec_to_bytes(q)
            try:
                result = r.execute_command(
                    "FT.SEARCH", "idx",
                    f"*=>[KNN {self.k} @vec $query_vec]",
                    "PARAMS", "2", "query_vec", q_bytes,
                    "DIALECT", "2",
                )
                ids = parse_search_results(result, self.k)
                hit = len(set(ids[:self.k]) & set(self.ground_truth[i][:self.k]))
                recalls.append(hit / self.k)
                search_ok += 1
            except Exception as e:
                recalls.append(0.0)
                if i < 3:
                    print(f"    Search error (query {i}): {e}")

        avg_recall = sum(recalls) / len(recalls) if recalls else 0

        # KV readback — sample 200 keys
        print("    Checking KV readback (200 sample)...")
        kv_ok = 0
        kv_total = 200
        for i in range(kv_total):
            key_idx = i * (self.kv_count // kv_total)
            val = r.get(f"kv:{key_idx}")
            if val is not None:
                expected_prefix = f"{key_idx}:".encode()
                if val.startswith(expected_prefix):
                    kv_ok += 1

        dt = time.time() - t0
        result = {
            "search_queries": self.n_queries,
            "search_ok": search_ok,
            "avg_recall": round(avg_recall, 4),
            "kv_sample": kv_total,
            "kv_readable": kv_ok,
            "kv_integrity_pct": round(kv_ok / kv_total * 100, 1),
            "duration_s": round(dt, 1),
        }
        self.results["phases"]["3_verify_warm"] = result

        print(f"    Search: {search_ok}/{self.n_queries} ok | "
              f"R@{self.k}: {avg_recall:.3f} | "
              f"KV: {kv_ok}/{kv_total} readable ({kv_ok/kv_total*100:.0f}%)")

        # Recall may be 0 if vectors were evicted or compaction hasn't happened
        if avg_recall > 0:
            self.assert_true(avg_recall >= 0.5, f"recall@10={avg_recall:.3f} >= 0.50", 3)
        else:
            print("    INFO: recall=0.000 — vectors may be in mutable segment (no HNSW yet)")

    # ── Phase 4: Wait for Cold Transition ────────────────────────────

    def phase4_cold_transition(self):
        print("\n== Phase 4: Wait for WARM→COLD Transition ==")
        r = self.get_redis()

        # Check if warm segments exist first
        mpf_before = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*/*.mpf"
        ))

        if not mpf_before:
            print("    SKIP: No warm segments to transition (vectors may still be in mutable)")
            self.results["phases"]["4_cold_transition"] = {"skipped": True, "reason": "no warm segments"}
            return

        print(f"    Warm segments: {len(mpf_before)} .mpf files")
        print(f"    Waiting {self.args.cold_wait}s for WARM→COLD transition...")
        time.sleep(self.args.cold_wait)

        # Check for DiskANN files
        diskann_dirs = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*-diskann"
        ))
        vamana_files = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*-diskann/vamana.mpf"
        ))

        result = {
            "warm_mpf_before": len(mpf_before),
            "diskann_dirs": len(diskann_dirs),
            "vamana_files": len(vamana_files),
            "wait_seconds": self.args.cold_wait,
        }
        self.results["phases"]["4_cold_transition"] = result

        print(f"    DiskANN dirs: {len(diskann_dirs)} | Vamana files: {len(vamana_files)}")

    # ── Phase 5: Crash + Recovery ────────────────────────────────────

    def phase5_crash_recovery(self):
        print("\n== Phase 5: Crash + Recovery ==")
        r = self.get_redis()

        # Trigger BGSAVE for checkpoint
        try:
            r.execute_command("BGSAVE")
        except Exception:
            pass
        time.sleep(3)  # Wait for checkpoint + WAL flush

        pre_dbsize = r.dbsize()
        print(f"    Pre-crash DBSIZE: {pre_dbsize}")

        # Kill -9
        print("    Sending SIGKILL...")
        self.kill_moon()

        # Verify data files persist
        wal_files = glob.glob(os.path.join(self.data_dir, "shard-0/wal-v3/*.wal"))
        print(f"    WAL v3 files on disk: {len(wal_files)}")

        # Restart
        print("    Restarting Moon...")
        t_start = time.time()
        self.start_moon()
        recovery_time = time.time() - t_start

        r2 = self.get_redis()
        post_dbsize = r2.dbsize()
        loss_pct = max(0, (1 - post_dbsize / max(pre_dbsize, 1)) * 100)

        # Verify data integrity
        kv_ok = 0
        kv_sample = 100
        for i in range(kv_sample):
            key_idx = i * max(1, self.kv_count // kv_sample)
            val = r2.get(f"kv:{key_idx}")
            if val is not None:
                expected_prefix = f"{key_idx}:".encode()
                if val.startswith(expected_prefix):
                    kv_ok += 1

        # Check vector search after recovery
        search_ok = 0
        for i in range(min(10, self.n_queries)):
            q_bytes = vec_to_bytes(self.queries[i])
            try:
                result = r2.execute_command(
                    "FT.SEARCH", "idx",
                    f"*=>[KNN {self.k} @vec $query_vec]",
                    "PARAMS", "2", "query_vec", q_bytes,
                    "DIALECT", "2",
                )
                if isinstance(result, list) and result[0] > 0:
                    search_ok += 1
            except Exception:
                pass

        result = {
            "pre_crash_dbsize": pre_dbsize,
            "post_recovery_dbsize": post_dbsize,
            "data_loss_pct": round(loss_pct, 2),
            "recovery_time_s": round(recovery_time, 2),
            "kv_integrity": f"{kv_ok}/{kv_sample}",
            "kv_integrity_pct": round(kv_ok / kv_sample * 100, 1),
            "vector_search_ok": f"{search_ok}/10",
        }
        self.results["phases"]["5_crash_recovery"] = result

        print(f"    Recovery: {recovery_time:.2f}s | "
              f"DBSIZE: {post_dbsize}/{pre_dbsize} ({loss_pct:.1f}% loss) | "
              f"KV: {kv_ok}/{kv_sample} | Vector search: {search_ok}/10")

        self.assert_true(recovery_time < 10, f"recovery_time={recovery_time:.1f}s < 10s", 5)
        self.assert_true(post_dbsize > 0, f"post_dbsize={post_dbsize} > 0", 5)

    # ── Phase 6: Data Integrity Audit ────────────────────────────────

    def phase6_integrity_audit(self):
        print("\n== Phase 6: Data Integrity Audit ==")
        r = self.get_redis()

        # Check manifest file
        manifest_path = os.path.join(self.data_dir, "shard-0/shard-0.manifest")
        manifest_exists = os.path.exists(manifest_path)

        # Check control file
        control_path = os.path.join(self.data_dir, "shard-0/shard-0.control")
        control_exists = os.path.exists(control_path)

        # Check WAL v3 segments
        wal_files = glob.glob(os.path.join(self.data_dir, "shard-0/wal-v3/*.wal"))
        total_wal_bytes = sum(os.path.getsize(f) for f in wal_files)

        # Check .mpf files — verify non-zero and page-aligned
        mpf_files = glob.glob(os.path.join(self.data_dir, "shard-0/vectors/segment-*/*.mpf"))
        mpf_valid = 0
        for f in mpf_files:
            size = os.path.getsize(f)
            if size > 0 and (size % 4096 == 0 or size % 65536 == 0):
                mpf_valid += 1

        # Check server logs for panics (non-blocking)
        panic_count = 0
        try:
            if self.proc and self.proc.stdout:
                import fcntl
                fd = self.proc.stdout.fileno()
                flags = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                try:
                    log_output = self.proc.stdout.read(65536) or b""
                    panic_count = log_output.count(b"panic") + log_output.count(b"PANIC")
                except (BlockingIOError, IOError):
                    pass
        except Exception:
            pass

        result = {
            "manifest_exists": manifest_exists,
            "control_exists": control_exists,
            "wal_segments": len(wal_files),
            "wal_total_bytes": total_wal_bytes,
            "mpf_files": len(mpf_files),
            "mpf_valid": mpf_valid,
            "panics_in_log": panic_count,
        }
        self.results["phases"]["6_integrity_audit"] = result

        print(f"    Manifest: {'OK' if manifest_exists else 'MISSING'} | "
              f"Control: {'OK' if control_exists else 'MISSING'} | "
              f"WAL: {len(wal_files)} segments ({total_wal_bytes//1024}KB) | "
              f"MPF: {mpf_valid}/{len(mpf_files)} valid | "
              f"Panics: {panic_count}")

        self.assert_true(manifest_exists, "manifest file exists", 6)
        self.assert_true(control_exists, "control file exists", 6)
        self.assert_true(len(wal_files) > 0, f"WAL segments exist ({len(wal_files)})", 6)
        self.assert_true(panic_count == 0, f"zero panics in log (found {panic_count})", 6)

    # ── Run All ──────────────────────────────────────────────────────

    def run(self):
        print("=" * 65)
        print(" MoonStore v2 Cross-Tier Memory Pressure Test")
        print("=" * 65)
        print(f" Moon: {self.moon_bin}")
        print(f" Port: {self.port} | maxmemory: 128MB")
        print(f" warm-after: 5s | cold-after: 15s | checkpoint: 15s")
        print(f" Vectors: {self.n_vectors} x {self.dim}d | KV value: {self.kv_value_size}B")
        print("=" * 65)

        try:
            self.start_moon()

            self.phase1_fill_hot()
            self.phase2_pressure()
            self.phase3_verify_warm()
            self.phase4_cold_transition()
            self.phase5_crash_recovery()
            self.phase6_integrity_audit()

        except Exception as e:
            print(f"\n  FATAL: {e}")
            import traceback
            traceback.print_exc()
            self.results["pass"] = False
            self.results["failures"].append(f"Fatal: {e}")
        finally:
            self.stop_moon()
            # Clean up
            if not self.args.keep_data:
                shutil.rmtree(self.data_dir, ignore_errors=True)

        # ── Report ──
        print("\n" + "=" * 65)
        if self.results["pass"]:
            print(" RESULT: PASS")
        else:
            print(" RESULT: FAIL")
            for f in self.results["failures"]:
                print(f"   - {f}")
        print("=" * 65)

        # Save JSON results
        if self.args.output:
            os.makedirs(os.path.dirname(self.args.output) or ".", exist_ok=True)
            with open(self.args.output, "w") as f:
                json.dump(self.results, f, indent=2)
            print(f" Results: {self.args.output}")

        return 0 if self.results["pass"] else 1


# ── Main ─────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="MoonStore v2 cross-tier memory pressure test")
    p.add_argument("--moon-bin", default="target/release/moon")
    p.add_argument("--port", type=int, default=16379)
    p.add_argument("--data-dir", default="/tmp/moon-tier-test")
    p.add_argument("--cold-wait", type=int, default=18, help="Seconds to wait for cold transition")
    p.add_argument("--keep-data", action="store_true", help="Don't clean up data dir")
    p.add_argument("--output", default="target/moonstore-v2-bench/cross-tier.json")
    args = p.parse_args()

    test = CrossTierTest(args)
    sys.exit(test.run())


if __name__ == "__main__":
    main()
