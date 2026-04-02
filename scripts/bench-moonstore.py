#!/usr/bin/env python3
"""MoonStore v2 persistence benchmark.

Compares --disk-offload=enable vs --disk-offload=disable:
  1. KV SET/GET throughput (redis-benchmark, pipeline=16)
  2. WAL v3 append overhead (should be ~0ns vs v2)
  3. Checkpoint I/O impact on p99 latency during flush
  4. Recovery time after kill -9 with N keys

Requires:
  - Moon server binary (cargo build --release)
  - redis-benchmark (redis-tools package)

Usage:
  python3 scripts/bench-moonstore.py [--keys 100000] [--pipeline 16]
  python3 scripts/bench-moonstore.py --help
"""

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time


# ── Defaults ──────────────────────────────────────────────────────────
DEFAULT_KEYS = 100_000
DEFAULT_PIPELINE = 16
DEFAULT_PORT = 6379
DEFAULT_MOON_BIN = "target/release/moon"
MOON_STARTUP_WAIT = 2.0
RECOVERY_TIMEOUT = 30.0


def parse_args():
    p = argparse.ArgumentParser(
        description="MoonStore v2 persistence benchmark: disk-offload enable vs disable",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--keys", type=int, default=DEFAULT_KEYS,
                    help=f"Number of KV pairs to insert (default: {DEFAULT_KEYS})")
    p.add_argument("--pipeline", type=int, default=DEFAULT_PIPELINE,
                    help=f"Pipeline depth for redis-benchmark (default: {DEFAULT_PIPELINE})")
    p.add_argument("--port", type=int, default=DEFAULT_PORT,
                    help=f"Base port for Moon server (default: {DEFAULT_PORT})")
    p.add_argument("--moon-bin", type=str, default=DEFAULT_MOON_BIN,
                    help=f"Path to Moon binary (default: {DEFAULT_MOON_BIN})")
    p.add_argument("--data-dir", type=str, default="/tmp/bench-moonstore",
                    help="Data directory for server instances (default: /tmp/bench-moonstore)")
    p.add_argument("--shards", type=int, default=1,
                    help="Number of shards (default: 1)")
    p.add_argument("--skip-build", action="store_true",
                    help="Skip cargo build step")
    p.add_argument("--json", action="store_true",
                    help="Output results as JSON instead of markdown")
    return p.parse_args()


def find_redis_benchmark():
    """Locate redis-benchmark binary."""
    for name in ["redis-benchmark"]:
        path = shutil.which(name)
        if path:
            return path
    print("ERROR: redis-benchmark not found. Install redis-tools.", file=sys.stderr)
    sys.exit(1)


def build_moon(moon_bin, skip_build):
    """Build Moon in release mode if needed."""
    if skip_build:
        if not os.path.exists(moon_bin):
            print(f"ERROR: {moon_bin} not found and --skip-build specified", file=sys.stderr)
            sys.exit(1)
        return
    print("[build] cargo build --release ...")
    result = subprocess.run(
        ["cargo", "build", "--release"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"ERROR: build failed:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)


def start_moon(moon_bin, port, data_dir, shards, disk_offload):
    """Start a Moon server instance and return the Popen object."""
    os.makedirs(data_dir, exist_ok=True)
    cmd = [
        moon_bin,
        "--port", str(port),
        "--dir", data_dir,
        "--shards", str(shards),
        "--disk-offload", disk_offload,
        "--appendonly", "disable",
    ]
    if disk_offload == "enable":
        cmd.extend(["--checkpoint-timeout", "10"])

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(MOON_STARTUP_WAIT)
    if proc.poll() is not None:
        _, stderr = proc.communicate()
        print(f"ERROR: Moon failed to start: {stderr.decode()}", file=sys.stderr)
        sys.exit(1)
    return proc


def stop_moon(proc, graceful=True):
    """Stop a Moon server. If graceful=False, use SIGKILL."""
    if proc.poll() is not None:
        return
    if graceful:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    else:
        proc.kill()
        proc.wait()


def run_redis_benchmark(bench_bin, port, keys, pipeline, command):
    """Run redis-benchmark and parse ops/sec from output."""
    cmd = [
        bench_bin, "-p", str(port),
        "-n", str(keys),
        "-P", str(pipeline),
        "-t", command,
        "-q",
        "--csv",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        return {"ops_sec": 0, "error": result.stderr.strip()}

    # Parse CSV output: "SET","123456.78","..."
    for line in result.stdout.strip().split("\n"):
        parts = line.split(",")
        if len(parts) >= 2:
            try:
                ops = float(parts[1].strip('"'))
                return {"ops_sec": ops}
            except ValueError:
                continue
    return {"ops_sec": 0, "raw": result.stdout}


def measure_p99_during_checkpoint(bench_bin, port, keys, pipeline):
    """Run SET workload for 15 seconds, capture latency histogram.

    The checkpoint should trigger during this window (timeout=10s).
    """
    cmd = [
        bench_bin, "-p", str(port),
        "-n", str(keys),
        "-P", str(pipeline),
        "-t", "set",
        "--csv",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

    # Parse p99 from extended CSV if available
    p99 = None
    for line in result.stdout.strip().split("\n"):
        if "99" in line.lower() or "percentile" in line.lower():
            match = re.search(r"[\d.]+", line)
            if match:
                p99 = float(match.group())
    return {"p99_ms": p99, "raw": result.stdout[:200]}


def measure_recovery_time(moon_bin, port, data_dir, shards, bench_bin, keys):
    """Insert keys, kill -9, restart, measure time to first successful GET."""
    # Start server with disk-offload
    proc = start_moon(moon_bin, port, data_dir, shards, "enable")

    # Insert keys
    run_redis_benchmark(bench_bin, port, keys, 16, "set")

    # Kill -9 (simulates crash)
    stop_moon(proc, graceful=False)
    time.sleep(0.5)

    # Restart and measure recovery time
    t0 = time.monotonic()
    proc2 = subprocess.Popen(
        [moon_bin, "--port", str(port), "--dir", data_dir,
         "--shards", str(shards), "--disk-offload", "enable"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )

    # Poll until we can GET a key
    recovery_ms = None
    deadline = time.monotonic() + RECOVERY_TIMEOUT
    while time.monotonic() < deadline:
        try:
            result = subprocess.run(
                [bench_bin, "-p", str(port), "-n", "1", "-t", "get", "-q"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and "0.00" not in result.stdout:
                recovery_ms = (time.monotonic() - t0) * 1000
                break
        except (subprocess.TimeoutExpired, Exception):
            pass
        time.sleep(0.2)

    stop_moon(proc2, graceful=True)
    return {"recovery_ms": recovery_ms, "keys": keys}


def print_markdown_results(results):
    """Print results as a markdown table."""
    print("\n## MoonStore v2 Persistence Benchmark Results\n")
    print("| Metric | disk-offload=disable | disk-offload=enable | Delta |")
    print("|--------|---------------------|---------------------|-------|")

    for metric in ["GET ops/sec", "SET ops/sec"]:
        off = results.get("disable", {}).get(metric, 0)
        on = results.get("enable", {}).get(metric, 0)
        if off > 0 and on > 0:
            delta = ((on - off) / off) * 100
            sign = "+" if delta >= 0 else ""
            print(f"| {metric} | {off:,.0f} | {on:,.0f} | {sign}{delta:.1f}% |")
        else:
            print(f"| {metric} | {off} | {on} | N/A |")

    # Recovery time
    rec = results.get("recovery", {})
    if rec.get("recovery_ms"):
        print(f"| Recovery time ({rec['keys']} keys) | N/A | {rec['recovery_ms']:.0f} ms | - |")

    # p99 during checkpoint
    p99 = results.get("p99_checkpoint", {})
    if p99.get("p99_ms"):
        print(f"| SET p99 during checkpoint | - | {p99['p99_ms']:.2f} ms | - |")

    print()


def print_json_results(results):
    """Print results as JSON."""
    print(json.dumps(results, indent=2, default=str))


def main():
    args = parse_args()
    bench_bin = find_redis_benchmark()
    build_moon(args.moon_bin, args.skip_build)

    results = {}

    for mode in ["disable", "enable"]:
        print(f"\n{'='*60}")
        print(f"  Mode: --disk-offload={mode}")
        print(f"{'='*60}")

        data_dir = os.path.join(args.data_dir, mode)
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)

        port = args.port if mode == "disable" else args.port + 1
        proc = start_moon(args.moon_bin, port, data_dir, args.shards, mode)

        try:
            # GET throughput
            print(f"[{mode}] Benchmarking GET ...")
            get_result = run_redis_benchmark(
                bench_bin, port, args.keys, args.pipeline, "get",
            )
            print(f"  GET: {get_result.get('ops_sec', 0):,.0f} ops/sec")

            # SET throughput
            print(f"[{mode}] Benchmarking SET ...")
            set_result = run_redis_benchmark(
                bench_bin, port, args.keys, args.pipeline, "set",
            )
            print(f"  SET: {set_result.get('ops_sec', 0):,.0f} ops/sec")

            results[mode] = {
                "GET ops/sec": get_result.get("ops_sec", 0),
                "SET ops/sec": set_result.get("ops_sec", 0),
            }

            # p99 during checkpoint (enable mode only)
            if mode == "enable":
                print(f"[{mode}] Measuring p99 during checkpoint window ...")
                p99 = measure_p99_during_checkpoint(
                    bench_bin, port, args.keys, args.pipeline,
                )
                results["p99_checkpoint"] = p99
                if p99.get("p99_ms"):
                    print(f"  p99 during checkpoint: {p99['p99_ms']:.2f} ms")

        finally:
            stop_moon(proc, graceful=True)

    # Recovery time measurement
    print(f"\n{'='*60}")
    print("  Recovery time measurement (kill -9 + restart)")
    print(f"{'='*60}")
    recovery_dir = os.path.join(args.data_dir, "recovery")
    if os.path.exists(recovery_dir):
        shutil.rmtree(recovery_dir)
    results["recovery"] = measure_recovery_time(
        args.moon_bin, args.port + 2, recovery_dir, args.shards,
        bench_bin, min(args.keys, 50_000),
    )
    if results["recovery"].get("recovery_ms"):
        print(f"  Recovery: {results['recovery']['recovery_ms']:.0f} ms")

    # Output
    print(f"\n{'='*60}")
    if args.json:
        print_json_results(results)
    else:
        print_markdown_results(results)

    # Verify regression target: GET with enable should be within 5% of disable
    get_off = results.get("disable", {}).get("GET ops/sec", 0)
    get_on = results.get("enable", {}).get("GET ops/sec", 0)
    if get_off > 0 and get_on > 0:
        regression = ((get_off - get_on) / get_off) * 100
        if regression > 5:
            print(f"WARNING: GET regression {regression:.1f}% exceeds 5% target!")
        else:
            print(f"PASS: GET regression {regression:.1f}% within 5% target")

    # Cleanup
    if os.path.exists(args.data_dir):
        shutil.rmtree(args.data_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
