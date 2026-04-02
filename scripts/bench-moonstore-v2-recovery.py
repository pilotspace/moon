#!/usr/bin/env python3
"""Part 4: Crash Recovery Benchmark — kill -9 + measure recovery time and data integrity."""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

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


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--moon-bin", default="target/release/moon")
    p.add_argument("--port", type=int, default=16379)
    p.add_argument("--keys", type=int, default=50000)
    p.add_argument("--output", default="target/moonstore-v2-bench/recovery.json")
    args = p.parse_args()

    results = {}

    for mode_name, extra_args in [
        ("wal_v2", []),
        ("disk_offload", ["--disk-offload", "enable", "--checkpoint-timeout", "30"]),
    ]:
        print(f"\n  [{mode_name}] Insert {args.keys} keys, kill -9, recover...")
        data_dir = f"/tmp/moon-recovery-{mode_name}"
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        os.makedirs(data_dir, exist_ok=True)

        # Start and insert
        cmd = [args.moon_bin, "--port", str(args.port), "--shards", "1",
               "--dir", data_dir, "--appendonly", "yes"] + extra_args
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if not wait_for_port(args.port):
            print(f"    Failed to start Moon ({mode_name})")
            proc.kill()
            continue

        r = redis.Redis(host="127.0.0.1", port=args.port, decode_responses=True)

        # Bulk insert
        t0 = time.time()
        pipe = r.pipeline(transaction=False)
        for i in range(args.keys):
            pipe.set(f"key:{i}", f"value-{i}-{'x' * 64}")
            if (i + 1) % 1000 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
        pipe.execute()
        insert_time = time.time() - t0

        # Verify a sample before kill
        pre_kill_check = r.get(f"key:{args.keys - 1}")
        pre_kill_dbsize = r.dbsize()

        # Force persistence: BGSAVE triggers a snapshot
        try:
            r.execute_command("BGSAVE")
        except Exception:
            pass
        # Wait for snapshot + WAL flush (snapshot writes .rrdshard, WAL syncs on 1s timer)
        time.sleep(4)

        # Verify data is visible before kill
        verify_count = r.dbsize()
        print(f"    DBSIZE after persist wait: {verify_count}")

        # Kill -9 (simulate crash)
        print(f"    Inserted {pre_kill_dbsize} keys in {insert_time:.1f}s. Sending SIGKILL...")
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()

        # Restart and measure recovery
        t_recovery_start = time.time()
        proc2 = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if not wait_for_port(args.port, timeout=60):
            print(f"    Recovery FAILED (server didn't come up)")
            proc2.kill()
            results[mode_name] = {"recovery_time_s": -1, "keys_recovered": 0}
            continue

        recovery_time = time.time() - t_recovery_start

        r2 = redis.Redis(host="127.0.0.1", port=args.port, decode_responses=True)
        post_dbsize = r2.dbsize()

        # Verify data integrity — check 100 random keys
        import random
        random.seed(42)
        sample_keys = random.sample(range(args.keys), min(100, args.keys))
        correct = 0
        for idx in sample_keys:
            val = r2.get(f"key:{idx}")
            expected = f"value-{idx}-{'x' * 64}"
            if val == expected:
                correct += 1

        proc2.terminate()
        proc2.wait()
        shutil.rmtree(data_dir, ignore_errors=True)

        # With appendfsync=everysec, ~1s of data may be lost
        loss_pct = max(0, (1 - post_dbsize / args.keys) * 100)

        results[mode_name] = {
            "keys_inserted": args.keys,
            "keys_recovered": post_dbsize,
            "data_loss_pct": round(loss_pct, 2),
            "recovery_time_s": round(recovery_time, 2),
            "integrity_check": f"{correct}/{len(sample_keys)}",
            "integrity_pct": round(correct / len(sample_keys) * 100, 1),
        }
        print(f"    Recovery: {recovery_time:.2f}s | Keys: {post_dbsize}/{args.keys} "
              f"({loss_pct:.1f}% loss) | Integrity: {correct}/{len(sample_keys)}")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n  Recovery results saved: {args.output}")


if __name__ == "__main__":
    main()
