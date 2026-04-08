#!/usr/bin/env python3
"""Part 1: KV Persistence Benchmark — WAL v3 disk-offload vs default.

Tests:
  A. Baseline: Moon without disk-offload (WAL v2, default)
  B. disk-offload=enable: Moon with WAL v3, PageCache, checkpoint
  C. Redis 8.x with appendonly yes (reference)

Metrics: SET/GET QPS, p50/p99 latency, appendfsync=always overhead.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time


def run_redis_benchmark(port, keys, pipeline, cmd="SET"):
    """Run redis-benchmark and parse JSON output."""
    args = [
        "redis-benchmark", "-p", str(port),
        "-n", str(keys), "-P", str(pipeline),
        "-t", cmd.lower(),
        "-d", "128",  # 128-byte values
        "--csv",
    ]
    result = subprocess.run(args, capture_output=True, text=True, timeout=120)
    # Parse CSV: "SET","qps","avg","min","p50","p95","p99","max"
    for line in result.stdout.strip().split("\n"):
        if cmd.upper() in line.upper():
            parts = line.replace('"', '').split(",")
            if len(parts) >= 6:
                return {
                    "qps": float(parts[1]),
                    "avg_ms": float(parts[2]) if len(parts) > 2 else 0,
                    "p50_ms": float(parts[4]) if len(parts) > 4 else 0,
                    "p99_ms": float(parts[6]) if len(parts) > 6 else 0,
                }
    return {"qps": 0, "avg_ms": 0, "p50_ms": 0, "p99_ms": 0}


def start_moon(binary, port, extra_args=None, data_dir=None):
    """Start Moon server, return (process, data_dir)."""
    if data_dir is None:
        data_dir = f"/tmp/moon-bench-{port}"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    cmd = [binary, "--port", str(port), "--shards", "1",
           "--dir", data_dir, "--appendonly", "yes"]
    if extra_args:
        cmd.extend(extra_args)
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    return proc, data_dir


def start_redis(port):
    """Start Redis server."""
    data_dir = f"/tmp/redis-bench-{port}"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen([
        "redis-server", "--port", str(port),
        "--dir", data_dir,
        "--appendonly", "yes",
        "--appendfsync", "everysec",
        "--save", "",
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    return proc, data_dir


def get_rss_mb(pid):
    """Get process RSS in MB."""
    try:
        if sys.platform == "darwin":
            out = subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)]).decode().strip()
            return int(out) / 1024  # KB -> MB
        else:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) / 1024
    except Exception:
        return 0
    return 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--moon-bin", default="target/release/moon")
    p.add_argument("--port", type=int, default=16379)
    p.add_argument("--keys", type=int, default=100000)
    p.add_argument("--pipeline", type=int, default=16)
    p.add_argument("--output", default="target/moonstore-v2-bench/kv.json")
    args = p.parse_args()

    results = {}

    # ── A. Moon baseline (no disk-offload) ──
    print("\n  [A] Moon baseline (WAL v2, no disk-offload)...")
    proc, ddir = start_moon(args.moon_bin, args.port)
    try:
        set_result = run_redis_benchmark(args.port, args.keys, args.pipeline, "SET")
        get_result = run_redis_benchmark(args.port, args.keys, args.pipeline, "GET")
        rss = get_rss_mb(proc.pid)
        results["moon_baseline"] = {
            "set": set_result, "get": get_result,
            "rss_mb": round(rss, 1),
        }
        print(f"      SET: {set_result['qps']:.0f} QPS | GET: {get_result['qps']:.0f} QPS | RSS: {rss:.0f}MB")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(ddir, ignore_errors=True)

    time.sleep(1)

    # ── B. Moon with disk-offload ──
    print("\n  [B] Moon disk-offload (WAL v3, PageCache, checkpoint)...")
    proc, ddir = start_moon(args.moon_bin, args.port + 1, [
        "--disk-offload", "enable",
        "--checkpoint-timeout", "30",
        "--max-wal-size", "16mb",
    ])
    try:
        set_result = run_redis_benchmark(args.port + 1, args.keys, args.pipeline, "SET")
        get_result = run_redis_benchmark(args.port + 1, args.keys, args.pipeline, "GET")
        rss = get_rss_mb(proc.pid)
        results["moon_disk_offload"] = {
            "set": set_result, "get": get_result,
            "rss_mb": round(rss, 1),
        }
        print(f"      SET: {set_result['qps']:.0f} QPS | GET: {get_result['qps']:.0f} QPS | RSS: {rss:.0f}MB")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(ddir, ignore_errors=True)

    time.sleep(1)

    # ── C. Moon appendfsync=always ──
    print("\n  [C] Moon appendfsync=always (zero data loss)...")
    proc, ddir = start_moon(args.moon_bin, args.port + 2, [
        "--disk-offload", "enable",
        "--appendfsync", "always",
    ])
    try:
        set_result = run_redis_benchmark(args.port + 2, args.keys, args.pipeline, "SET")
        rss = get_rss_mb(proc.pid)
        results["moon_always"] = {
            "set": set_result,
            "rss_mb": round(rss, 1),
        }
        print(f"      SET: {set_result['qps']:.0f} QPS | RSS: {rss:.0f}MB")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(ddir, ignore_errors=True)

    time.sleep(1)

    # ── D. Redis 8.x reference ──
    print("\n  [D] Redis 8.x (appendonly=yes, everysec)...")
    proc, ddir = start_redis(args.port + 3)
    try:
        set_result = run_redis_benchmark(args.port + 3, args.keys, args.pipeline, "SET")
        get_result = run_redis_benchmark(args.port + 3, args.keys, args.pipeline, "GET")
        rss = get_rss_mb(proc.pid)
        results["redis"] = {
            "set": set_result, "get": get_result,
            "rss_mb": round(rss, 1),
        }
        print(f"      SET: {set_result['qps']:.0f} QPS | GET: {get_result['qps']:.0f} QPS | RSS: {rss:.0f}MB")
    finally:
        proc.terminate()
        proc.wait()
        shutil.rmtree(ddir, ignore_errors=True)

    # Save results
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  KV results saved: {args.output}")


if __name__ == "__main__":
    main()
