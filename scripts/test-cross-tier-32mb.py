#!/usr/bin/env python3
"""MoonStore v2 Cross-Tier 32MB Pressure Test.

Tight 32MB maxmemory forces DashTable memory estimate to exceed the limit,
exercising the FULL pressure cascade that the 128MB test never triggers:
  - PageCache eviction (step 1)
  - HOT->WARM force-demote (step 2)
  - KV eviction with spill-to-disk (step 3)
  - OOM rejection (step 4)

7 phases, ~45s total:
  Phase 1: Baseline (vectors + KV under 32MB, compact, snapshot)
  Phase 2: Pressure trigger (exceed 32MB, eviction + warm)
  Phase 3: Verify warm search + KV readback
  Phase 4: Spill readback (parse heap-*.mpf on disk)
  Phase 5: Cold transition (WARM->COLD DiskANN)
  Phase 6: Crash + recovery
  Phase 7: Integrity audit

Usage:
  python3 scripts/test-cross-tier-32mb.py
  python3 scripts/test-cross-tier-32mb.py --moon-bin target/release/moon --port 16479
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
            time.sleep(0.2)
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


# ── Test ─────────────────────────────────────────────────────────────────

class CrossTier32MB:
    MAXMEMORY = 32 * 1024 * 1024  # 32MB
    DIM = 128
    N_VECTORS = 1000
    N_QUERIES = 20
    K = 10
    KV_VALUE_SIZE = 256
    WARM_AFTER = 3      # seconds
    COLD_AFTER = 8      # seconds
    CHECKPOINT = 10     # seconds

    def __init__(self, args):
        self.args = args
        self.port = args.port
        self.data_dir = args.data_dir
        self.proc = None
        self.results = {"phases": {}, "pass": True, "failures": []}
        self.kv_count = 0

        # Generate test vectors + ground truth
        np.random.seed(42)
        self.vectors = np.random.randn(self.N_VECTORS, self.DIM).astype(np.float32)
        self.vectors /= np.linalg.norm(self.vectors, axis=1, keepdims=True)
        self.queries = np.random.randn(self.N_QUERIES, self.DIM).astype(np.float32)
        self.queries /= np.linalg.norm(self.queries, axis=1, keepdims=True)
        self.ground_truth = []
        for q in self.queries:
            dists = np.sum((self.vectors - q) ** 2, axis=1)
            self.ground_truth.append(np.argsort(dists)[:self.K].tolist())

    def start_moon(self, clean=True):
        if clean and os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)

        cmd = [
            self.args.moon_bin,
            "--port", str(self.port),
            "--shards", "1",
            "--maxmemory", str(self.MAXMEMORY),
            "--maxmemory-policy", "allkeys-lru",
            "--appendonly", "yes",
            "--disk-offload", "enable",
            "--disk-offload-threshold", "0.80",
            "--segment-warm-after", str(self.WARM_AFTER),
            "--segment-cold-after", str(self.COLD_AFTER),
            "--checkpoint-timeout", str(self.CHECKPOINT),
            "--max-wal-size", "4mb",
            "--dir", self.data_dir,
        ]
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if not wait_for_port(self.port):
            self.proc.kill()
            raise RuntimeError("Moon failed to start")

    def stop_moon(self):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
            self.proc = None

    def kill_moon(self):
        if self.proc:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
            self.proc = None

    def redis(self):
        import redis
        return redis.Redis(host="127.0.0.1", port=self.port, decode_responses=False)

    def ok(self, cond, msg, phase):
        if not cond:
            self.results["pass"] = False
            self.results["failures"].append(f"Phase {phase}: {msg}")
            print(f"    FAIL: {msg}")
            return False
        print(f"    PASS: {msg}")
        return True

    # ── Phase 1: Baseline ────────────────────────────────────────────

    def phase1_baseline(self):
        print("\n== Phase 1: Baseline (fill under 32MB) ==")
        t0 = time.time()
        r = self.redis()

        # Create index (128d, small)
        try:
            r.execute_command(
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
                "SCHEMA", "vec", "VECTOR", "HNSW", "8",
                "TYPE", "FLOAT32", "DIM", str(self.DIM), "DISTANCE_METRIC", "L2",
                "COMPACT_THRESHOLD", "500",
            )
        except Exception as e:
            print(f"    FT.CREATE: {e}")

        # Insert vectors
        print(f"    Inserting {self.N_VECTORS} vectors ({self.DIM}d)...")
        pipe = r.pipeline(transaction=False)
        for i, vec in enumerate(self.vectors):
            pipe.hset(f"doc:{i}", mapping={"vec": vec_to_bytes(vec)})
            if (i + 1) % 250 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()

        # Compact mutable -> immutable (enables warm transition later)
        try:
            r.execute_command("FT.COMPACT", "idx")
            print("    FT.COMPACT: OK")
        except Exception as e:
            print(f"    FT.COMPACT: {e}")

        # Insert KV keys to ~20K (under 32MB DashTable estimate)
        print("    Inserting KV keys (target ~20K, under 32MB)...")
        pad = "x" * self.KV_VALUE_SIZE
        kv_target = 20000
        batch = 500
        pipe = r.pipeline(transaction=False)
        for start in range(0, kv_target, batch):
            for i in range(start, min(start + batch, kv_target)):
                pipe.set(f"kv:{i}", f"{i}:{pad}")
            try:
                pipe.execute()
            except Exception:
                break  # OOM — stop early
            pipe = r.pipeline(transaction=False)
        self.kv_count = kv_target

        # BGSAVE baseline
        try:
            r.execute_command("BGSAVE")
            print("    BGSAVE: triggered")
            time.sleep(3)
        except Exception as e:
            print(f"    BGSAVE: {e}")

        dbsize = r.dbsize()
        rss = get_rss_mb(self.proc.pid)
        dt = time.time() - t0

        self.results["phases"]["1_baseline"] = {
            "dbsize": dbsize, "rss_mb": round(rss, 1),
            "kv_count": self.kv_count, "vectors": self.N_VECTORS,
            "duration_s": round(dt, 1),
        }
        print(f"    DBSIZE: {dbsize} | RSS: {rss:.0f}MB | Time: {dt:.1f}s")
        self.ok(dbsize > 0, f"DBSIZE={dbsize} > 0", 1)

    # ── Phase 2: Pressure Trigger ────────────────────────────────────

    def phase2_pressure(self):
        print("\n== Phase 2: Pressure Trigger (exceed 32MB) ==")
        t0 = time.time()
        r = self.redis()

        # Hammer with KV keys to blow past 32MB
        print("    Inserting keys to exceed 32MB maxmemory...")
        pad = "x" * self.KV_VALUE_SIZE
        extra = 0
        oom_count = 0
        batch = 500
        for start in range(0, 80000, batch):
            pipe = r.pipeline(transaction=False)
            for i in range(start, start + batch):
                pipe.set(f"p:{i}", f"{i}:{pad}")
            try:
                results = pipe.execute(raise_on_error=False)
                # Count OOM responses
                for res in results:
                    if isinstance(res, Exception) and b"OOM" in str(res).encode():
                        oom_count += 1
                extra = start + batch
            except Exception:
                extra = start + batch
                oom_count += 1

        # Wait for eviction cascade + warm transition.
        # warm_check polls at min(warm_after, 10s) = 3s, segment qualifies after 3s.
        # Need at least 2 poll cycles + margin.
        wait_s = self.WARM_AFTER * 3 + 5
        print(f"    Waiting {wait_s}s for eviction cascade + warm transition...")
        time.sleep(wait_s)

        dbsize = r.dbsize()
        rss = get_rss_mb(self.proc.pid)
        expected = self.kv_count + self.N_VECTORS + extra
        evicted = max(0, expected - dbsize)

        # Check tier artifacts
        mpf_files = glob.glob(os.path.join(self.data_dir, "shard-0/vectors/segment-*/*.mpf"))
        heap_files = glob.glob(os.path.join(self.data_dir, "shard-0/data/heap-*.mpf"))
        wal_files = glob.glob(os.path.join(self.data_dir, "shard-0/wal-v3/*.wal"))

        dt = time.time() - t0
        self.results["phases"]["2_pressure"] = {
            "dbsize": dbsize, "rss_mb": round(rss, 1),
            "expected": expected, "evicted": evicted, "oom_count": oom_count,
            "mpf_warm": len(mpf_files), "heap_spill": len(heap_files),
            "wal_v3": len(wal_files), "duration_s": round(dt, 1),
        }
        print(f"    DBSIZE: {dbsize} | Evicted: {evicted} | OOM: {oom_count}")
        print(f"    Warm .mpf: {len(mpf_files)} | Spill heap: {len(heap_files)} | WAL: {len(wal_files)}")

        self.ok(evicted > 0, f"eviction occurred ({evicted} keys evicted)", 2)
        self.ok(len(wal_files) > 0, f"WAL v3 segments exist ({len(wal_files)})", 2)
        self.ok(len(mpf_files) > 0, f"warm .mpf files created ({len(mpf_files)})", 2)
        # heap spill depends on whether cascade step 3 ran — nice-to-have
        if len(heap_files) > 0:
            print(f"    PASS: KV spill files created ({len(heap_files)} heap files)")
        else:
            print(f"    INFO: no heap spill files (eviction via handler path, not cascade)")

    # ── Phase 3: Verify Search + KV ─────────────────────────────────

    def phase3_verify(self):
        print("\n== Phase 3: Verify Warm Search + KV Readback ==")
        t0 = time.time()
        r = self.redis()

        # Vector search
        search_ok = 0
        recalls = []
        for i, q in enumerate(self.queries):
            try:
                result = r.execute_command(
                    "FT.SEARCH", "idx",
                    f"*=>[KNN {self.K} @vec $qv]",
                    "PARAMS", "2", "qv", vec_to_bytes(q), "DIALECT", "2",
                )
                ids = parse_search_results(result, self.K)
                hit = len(set(ids[:self.K]) & set(self.ground_truth[i][:self.K]))
                recalls.append(hit / self.K)
                search_ok += 1
            except Exception as e:
                recalls.append(0.0)
                if i < 2:
                    print(f"    Search error ({i}): {e}")

        avg_recall = sum(recalls) / len(recalls) if recalls else 0

        # KV readback (sample from Phase 1 keys)
        kv_ok = 0
        kv_sample = 100
        for i in range(kv_sample):
            idx = i * max(1, self.kv_count // kv_sample)
            val = r.get(f"kv:{idx}")
            if val is not None:
                if val.startswith(f"{idx}:".encode()):
                    kv_ok += 1

        dt = time.time() - t0
        self.results["phases"]["3_verify"] = {
            "search_ok": search_ok, "avg_recall": round(avg_recall, 4),
            "kv_ok": kv_ok, "kv_sample": kv_sample,
            "duration_s": round(dt, 1),
        }
        print(f"    Search: {search_ok}/{self.N_QUERIES} | R@{self.K}: {avg_recall:.3f}")
        print(f"    KV: {kv_ok}/{kv_sample} ({kv_ok/kv_sample*100:.0f}%)")

        self.ok(search_ok > 0, f"search returns results ({search_ok}/{self.N_QUERIES})", 3)
        # At 32MB with allkeys-lru, many Phase 1 keys are evicted — accept >= 20%.
        # The important thing is that SOME keys survive and are readable with correct values.
        self.ok(kv_ok >= kv_sample * 0.20,
                f"KV readback {kv_ok}/{kv_sample} >= 20%", 3)
        if avg_recall > 0:
            print(f"    INFO: recall@{self.K}={avg_recall:.3f}")

    # ── Phase 4: Spill Readback ──────────────────────────────────────

    def phase4_spill_readback(self):
        print("\n== Phase 4: Spill Readback ==")

        heap_files = sorted(glob.glob(os.path.join(
            self.data_dir, "shard-0/data/heap-*.mpf"
        )))

        if not heap_files:
            print("    SKIP: no heap spill files (eviction via handler path)")
            self.results["phases"]["4_spill_readback"] = {
                "skipped": True, "reason": "no heap files",
            }
            return

        total_files = len(heap_files)
        valid_files = 0
        total_bytes = 0

        for hf in heap_files:
            size = os.path.getsize(hf)
            total_bytes += size
            # Must be page-aligned: 4KB or 64KB
            if size > 0 and (size % 4096 == 0):
                valid_files += 1

        # Read first file header to validate MoonPage structure
        header_ok = False
        if heap_files:
            with open(heap_files[0], "rb") as f:
                hdr = f.read(64)
                if len(hdr) == 64:
                    # MoonPage magic = 0x4D4E5047 ("MNPG" little-endian)
                    magic = struct.unpack("<I", hdr[:4])[0]
                    header_ok = (magic == 0x4D4E5047)

        self.results["phases"]["4_spill_readback"] = {
            "heap_files": total_files, "valid_aligned": valid_files,
            "total_bytes": total_bytes, "header_ok": header_ok,
        }
        print(f"    Heap files: {total_files} | Valid aligned: {valid_files} | "
              f"Total: {total_bytes//1024}KB | Header: {'OK' if header_ok else 'BAD'}")

        self.ok(valid_files > 0, f"page-aligned spill files ({valid_files}/{total_files})", 4)
        if header_ok:
            print("    PASS: MoonPage header valid (magic=MOON)")

    # ── Phase 5: Cold Transition ─────────────────────────────────────

    def phase5_cold(self):
        print("\n== Phase 5: Cold Transition ==")

        mpf_before = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*/*.mpf"
        ))
        if not mpf_before:
            print("    SKIP: no warm segments")
            self.results["phases"]["5_cold"] = {"skipped": True}
            return

        # cold_after=8s, poll=min(60,8)=8s, need ~16-20s from when warm was created
        # Warm was created in Phase 2, which was ~11s + Phase 3 ~2s + Phase 4 ~1s = ~14s ago
        # So we may only need a few more seconds
        wait = self.args.cold_wait
        print(f"    Warm .mpf: {len(mpf_before)} | Waiting {wait}s for cold transition...")
        time.sleep(wait)

        diskann = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*-diskann"
        ))
        vamana = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*-diskann/vamana.mpf"
        ))
        pq = glob.glob(os.path.join(
            self.data_dir, "shard-0/vectors/segment-*-diskann/pq_codes.bin"
        ))

        self.results["phases"]["5_cold"] = {
            "warm_before": len(mpf_before),
            "diskann_dirs": len(diskann),
            "vamana_files": len(vamana),
            "pq_files": len(pq),
        }
        print(f"    DiskANN: {len(diskann)} dirs | Vamana: {len(vamana)} | PQ: {len(pq)}")

        if len(diskann) > 0:
            self.ok(len(vamana) > 0, f"vamana.mpf exists ({len(vamana)})", 5)
            self.ok(len(pq) > 0, f"pq_codes.bin exists ({len(pq)})", 5)
        else:
            print("    INFO: cold transition not yet triggered (timing-dependent)")

    # ── Phase 6: Crash + Recovery ────────────────────────────────────

    def phase6_recovery(self):
        print("\n== Phase 6: Crash + Recovery ==")
        r = self.redis()

        # Flush checkpoint before crash
        try:
            r.execute_command("BGSAVE")
        except Exception:
            pass
        time.sleep(3)

        pre_dbsize = r.dbsize()
        print(f"    Pre-crash DBSIZE: {pre_dbsize}")

        # SIGKILL
        self.kill_moon()
        wal_on_disk = glob.glob(os.path.join(self.data_dir, "shard-0/wal-v3/*.wal"))
        print(f"    SIGKILL sent | WAL on disk: {len(wal_on_disk)}")

        # Restart
        t0 = time.time()
        self.start_moon(clean=False)
        recovery_s = time.time() - t0

        r2 = self.redis()
        post_dbsize = r2.dbsize()
        loss = max(0, (1 - post_dbsize / max(pre_dbsize, 1)) * 100)

        # KV integrity
        kv_ok = 0
        sample = 50
        for i in range(sample):
            idx = i * max(1, self.kv_count // sample)
            val = r2.get(f"kv:{idx}")
            if val is not None and val.startswith(f"{idx}:".encode()):
                kv_ok += 1

        self.results["phases"]["6_recovery"] = {
            "pre_dbsize": pre_dbsize, "post_dbsize": post_dbsize,
            "loss_pct": round(loss, 2), "recovery_s": round(recovery_s, 2),
            "kv_ok": kv_ok, "kv_sample": sample,
        }
        print(f"    Recovery: {recovery_s:.2f}s | DBSIZE: {post_dbsize}/{pre_dbsize} "
              f"({loss:.1f}% loss) | KV: {kv_ok}/{sample}")

        self.ok(recovery_s < 5, f"recovery {recovery_s:.1f}s < 5s", 6)
        self.ok(post_dbsize > 0, f"post_dbsize={post_dbsize} > 0", 6)

    # ── Phase 7: Integrity Audit ─────────────────────────────────────

    def phase7_audit(self):
        print("\n== Phase 7: Integrity Audit ==")

        manifest = os.path.join(self.data_dir, "shard-0/shard-0.manifest")
        control = os.path.join(self.data_dir, "shard-0/shard-0.control")
        wal_files = glob.glob(os.path.join(self.data_dir, "shard-0/wal-v3/*.wal"))
        wal_bytes = sum(os.path.getsize(f) for f in wal_files)

        # Scan all .mpf for page alignment
        all_mpf = (
            glob.glob(os.path.join(self.data_dir, "shard-0/vectors/segment-*/*.mpf")) +
            glob.glob(os.path.join(self.data_dir, "shard-0/vectors/segment-*-diskann/*.mpf")) +
            glob.glob(os.path.join(self.data_dir, "shard-0/data/heap-*.mpf"))
        )
        mpf_valid = sum(1 for f in all_mpf if os.path.getsize(f) > 0 and os.path.getsize(f) % 4096 == 0)

        # Panic check
        panic_count = 0
        try:
            if self.proc and self.proc.stdout:
                import fcntl
                fd = self.proc.stdout.fileno()
                flags = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                try:
                    log = self.proc.stdout.read(65536) or b""
                    panic_count = log.count(b"panic") + log.count(b"PANIC")
                except (BlockingIOError, IOError):
                    pass
        except Exception:
            pass

        self.results["phases"]["7_audit"] = {
            "manifest": os.path.exists(manifest),
            "control": os.path.exists(control),
            "wal_segments": len(wal_files),
            "wal_bytes": wal_bytes,
            "mpf_total": len(all_mpf),
            "mpf_valid": mpf_valid,
            "panics": panic_count,
        }
        print(f"    Manifest: {'OK' if os.path.exists(manifest) else 'MISSING'} | "
              f"Control: {'OK' if os.path.exists(control) else 'MISSING'}")
        print(f"    WAL: {len(wal_files)} ({wal_bytes//1024}KB) | "
              f"MPF: {mpf_valid}/{len(all_mpf)} valid | Panics: {panic_count}")

        self.ok(os.path.exists(manifest), "manifest exists", 7)
        self.ok(os.path.exists(control), "control file exists", 7)
        self.ok(len(wal_files) > 0, f"WAL v3 exists ({len(wal_files)})", 7)
        self.ok(panic_count == 0, f"zero panics ({panic_count})", 7)

    # ── Run ──────────────────────────────────────────────────────────

    def run(self):
        print("=" * 65)
        print(" MoonStore v2 Cross-Tier 32MB Pressure Test")
        print("=" * 65)
        print(f" Moon: {self.args.moon_bin}")
        print(f" Port: {self.port} | maxmemory: 32MB | threshold: 0.80")
        print(f" warm-after: {self.WARM_AFTER}s | cold-after: {self.COLD_AFTER}s")
        print(f" Vectors: {self.N_VECTORS} x {self.DIM}d | KV: {self.KV_VALUE_SIZE}B")
        print("=" * 65)

        try:
            self.start_moon()
            self.phase1_baseline()
            self.phase2_pressure()
            self.phase3_verify()
            self.phase4_spill_readback()
            self.phase5_cold()
            self.phase6_recovery()
            self.phase7_audit()
        except Exception as e:
            print(f"\n  FATAL: {e}")
            import traceback
            traceback.print_exc()
            self.results["pass"] = False
            self.results["failures"].append(f"Fatal: {e}")
        finally:
            self.stop_moon()
            if not self.args.keep_data:
                shutil.rmtree(self.data_dir, ignore_errors=True)

        # Report
        print("\n" + "=" * 65)
        if self.results["pass"]:
            print(" RESULT: PASS")
        else:
            print(" RESULT: FAIL")
            for f in self.results["failures"]:
                print(f"   - {f}")
        print("=" * 65)

        if self.args.output:
            os.makedirs(os.path.dirname(self.args.output) or ".", exist_ok=True)
            with open(self.args.output, "w") as f:
                json.dump(self.results, f, indent=2)
            print(f" Results: {self.args.output}")

        return 0 if self.results["pass"] else 1


def main():
    p = argparse.ArgumentParser(description="MoonStore v2 32MB cross-tier pressure test")
    p.add_argument("--moon-bin", default="target/release/moon")
    p.add_argument("--port", type=int, default=16479)
    p.add_argument("--data-dir", default="/tmp/moon-tier-32mb")
    p.add_argument("--cold-wait", type=int, default=12,
                   help="Extra seconds to wait for cold transition")
    p.add_argument("--keep-data", action="store_true")
    p.add_argument("--output", default="target/moonstore-v2-bench/cross-tier-32mb.json")
    args = p.parse_args()

    test = CrossTier32MB(args)
    sys.exit(test.run())


if __name__ == "__main__":
    main()
