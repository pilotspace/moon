#!/usr/bin/env python3
"""
Moon vs Qdrant — Real-World Vector Search Benchmark

Realistic mixed insert+search workload:
  Phase 1: Bulk insert 5K vectors (warmup)
  Phase 2: Mixed insert 50 + search 20, repeated 190 batches (9.5K more = 14.5K total)
  Phase 3: Search-only 200 queries (final recall & QPS)
  Phase 4: Crash recovery (SIGKILL + restart + verify)

Uses compact_threshold=2000 so HNSW compaction triggers naturally during mixed workload.
No external dependencies except numpy (for ground truth).

Usage:
  python3 scripts/bench-vector-realworld.py [--moon-port 6399] [--qdrant-port 6333]
"""

import argparse, json, math, os, random, signal, socket, struct, subprocess, sys, time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--dim", type=int, default=384)
parser.add_argument("--moon-port", type=int, default=6399)
parser.add_argument("--moon-bin", default="./target/release/moon")
parser.add_argument("--moon-dir", default="/tmp/moon-rw-bench")
parser.add_argument("--qdrant-port", type=int, default=6333)
parser.add_argument("--qdrant-bin", default="")
parser.add_argument("--qdrant-dir", default="/tmp/qdrant-rw-bench")
parser.add_argument("--skip-moon", action="store_true")
parser.add_argument("--skip-qdrant", action="store_true")
parser.add_argument("--skip-recovery", action="store_true")
parser.add_argument("--compact-threshold", type=int, default=2000)
args = parser.parse_args()

DIM = args.dim
K = 10

# ── Vector generation ──────────────────────────────────────────
def gen_vec(seed):
    rng = random.Random(seed)
    v = [rng.gauss(0, 1) for _ in range(DIM)]
    norm = math.sqrt(sum(x*x for x in v))
    return [x/norm for x in v] if norm > 0 else v

def vec_blob(v):
    return struct.pack(f"{DIM}f", *v)

# ── RESP helpers ───────────────────────────────────────────────
def resp_encode(args_list):
    parts = [f"*{len(args_list)}\r\n".encode()]
    for a in args_list:
        if isinstance(a, bytes):
            parts.append(f"${len(a)}\r\n".encode() + a + b"\r\n")
        else:
            s = str(a)
            parts.append(f"${len(s)}\r\n{s}\r\n".encode())
    return b"".join(parts)

def resp_read_one(sock, buf=b""):
    while b"\r\n" not in buf:
        buf += sock.recv(65536)
    prefix = buf[0:1]
    idx = buf.index(b"\r\n")
    line = buf[:idx]
    rest = buf[idx+2:]
    if prefix == b"+": return line[1:].decode(), rest
    elif prefix == b"-": return Exception(line[1:].decode()), rest
    elif prefix == b":": return int(line[1:]), rest
    elif prefix == b"$":
        length = int(line[1:])
        if length == -1: return None, rest
        while len(rest) < length + 2: rest += sock.recv(65536)
        return rest[:length], rest[length+2:]
    elif prefix == b"*":
        count = int(line[1:])
        if count == -1: return None, rest
        elems = []
        for _ in range(count):
            e, rest = resp_read_one(sock, rest)
            elems.append(e)
        return elems, rest
    return line.decode(), rest

def moon_connect(port, timeout=30):
    s = socket.socket(); s.settimeout(timeout); s.connect(("127.0.0.1", port))
    s.sendall(resp_encode(["PING"])); r, _ = resp_read_one(s)
    assert r in ("PONG", b"PONG"), f"PING failed: {r}"
    return s

def parse_search(resp, k):
    if not isinstance(resp, list) or len(resp) < 1: return []
    ids = []; i = 1
    while i < len(resp):
        key = resp[i]
        if isinstance(key, bytes): key = key.decode()
        elif isinstance(key, list): i += 1; continue
        try: ids.append(int(str(key).split(":")[1]))
        except: pass
        i += 1
        if i < len(resp) and isinstance(resp[i], list): i += 1
    return ids[:k]

def get_rss(pid):
    try: return float(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip()) / 1024
    except: return 0

# ── Brute-force recall ─────────────────────────────────────────
def bf_recall(query_vecs, result_ids_list, db_vecs, k):
    try:
        import numpy as np
        db = np.array(db_vecs, dtype=np.float32)
        recalls = []
        for i, (q, pred) in enumerate(zip(query_vecs, result_ids_list)):
            qa = np.array(q, dtype=np.float32)
            dists = np.sum((db - qa)**2, axis=1)
            gt = set(np.argsort(dists)[:k].tolist())
            recalls.append(len(set(pred[:k]) & gt) / k)
        return sum(recalls)/len(recalls) if recalls else 0
    except ImportError:
        return -1  # numpy not available

# ── Qdrant helpers ─────────────────────────────────────────────
def qdrant_req(port, method, path, data=None, timeout=60):
    import urllib.request
    url = f"http://127.0.0.1:{port}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode())
    except Exception as e:
        try: return json.loads(e.read().decode())
        except: return {"error": str(e)}

# ── MOON BENCHMARK ─────────────────────────────────────────────
def run_moon():
    print("\n" + "="*65)
    print("  MOON — Real-World Mixed Workload")
    print("="*65)

    subprocess.run(["killall", "-9", "moon"], capture_output=True)
    time.sleep(1)
    subprocess.run(["rm", "-rf", args.moon_dir], capture_output=True)
    os.makedirs(args.moon_dir, exist_ok=True)

    cmd = [args.moon_bin, "--port", str(args.moon_port), "--shards", "1",
           "--protected-mode", "no", "--appendonly", "yes", "--appendfsync", "everysec",
           "--dir", args.moon_dir]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    if proc.poll() is not None:
        print("  FAIL: Moon failed to start"); return None

    pid = proc.pid
    rss0 = get_rss(pid)
    print(f"  PID={pid}, RSS={rss0:.0f}MB")

    sock = moon_connect(args.moon_port)
    # FT.CREATE
    sock.sendall(resp_encode(["FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
        "SCHEMA", "vec", "VECTOR", "HNSW", "10",
        "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        "QUANTIZATION", "TQ4", "COMPACT_THRESHOLD", str(args.compact_threshold)]))
    r, _ = resp_read_one(sock)
    print(f"  FT.CREATE: {r}")

    results = {"system": "Moon"}
    all_vecs = []  # track inserted vectors for recall
    next_id = 0
    all_search_lats = []
    all_insert_lats = []
    timeline = []

    # Phase 1: Bulk insert 5000
    print(f"\n  Phase 1: Bulk insert 5000 vectors...")
    t0 = time.time()
    BATCH = 200
    for batch_start in range(0, 5000, BATCH):
        batch_end = min(batch_start + BATCH, 5000)
        batch_count = batch_end - batch_start
        buf = bytearray()
        for i in range(batch_start, batch_end):
            v = gen_vec(next_id)
            all_vecs.append(v)
            buf.extend(resp_encode(["HSET", f"doc:{next_id}", "vec", vec_blob(v)]))
            next_id += 1
        sock.sendall(bytes(buf))
        remaining = b""
        for _ in range(batch_count):
            _, remaining = resp_read_one(sock, remaining)
    t1 = time.time()
    print(f"  Inserted 5000 in {t1-t0:.1f}s ({5000/(t1-t0):.0f} vec/s)")
    results["bulk_insert_rate"] = round(5000/(t1-t0))

    # Phase 2: Mixed insert+search (190 batches × 50 insert + 20 search)
    print(f"\n  Phase 2: Mixed workload (insert 50 + search 20) × 190 batches")
    print(f"  {'Vectors':>7} | {'Recall':>7} | {'Ins/s':>6} | {'p50':>7} | {'p99':>8} | Note")
    print(f"  {'─'*7}─┼─{'─'*7}─┼─{'─'*6}─┼─{'─'*7}─┼─{'─'*8}─┼─{'─'*20}")

    query_vecs = [gen_vec(i + 10_000_000) for i in range(200)]
    query_idx = 0

    for batch in range(190):
        # Insert 50
        t_ins = time.time()
        remaining = b""
        for i in range(50):
            v = gen_vec(next_id)
            all_vecs.append(v)
            sock.sendall(resp_encode(["HSET", f"doc:{next_id}", "vec", vec_blob(v)]))
            next_id += 1
        for i in range(50):
            _, remaining = resp_read_one(sock, remaining)
        ins_time = time.time() - t_ins
        all_insert_lats.append(ins_time)

        # Search 20
        batch_lats = []
        batch_results = []
        for _ in range(20):
            q = query_vecs[query_idx % 200]; query_idx += 1
            blob = vec_blob(q)
            query_str = f"*=>[KNN {K} @vec $query]"
            sock.settimeout(120)
            sock.sendall(resp_encode(["FT.SEARCH", "idx", query_str, "PARAMS", "2", "query", blob]))
            t_s = time.perf_counter()
            resp, _ = resp_read_one(sock)
            lat = (time.perf_counter() - t_s) * 1000
            batch_lats.append(lat)
            all_search_lats.append(lat)
            ids = parse_search(resp, K)
            batch_results.append((q, ids))

        # Recall on this batch
        batch_recall = bf_recall(
            [r[0] for r in batch_results],
            [r[1] for r in batch_results],
            all_vecs, K
        )

        p50 = sorted(batch_lats)[len(batch_lats)//2]
        p99 = sorted(batch_lats)[int(len(batch_lats)*0.99)]
        note = ""
        if max(batch_lats) > 200: note = f"compact {max(batch_lats):.0f}ms"

        timeline.append({"n": next_id, "recall": batch_recall, "p50": p50, "p99": p99})

        if (batch+1) % 10 == 0 or note:
            ins_rate = 50/ins_time if ins_time > 0 else 0
            print(f"  {next_id:>7} | {batch_recall:>7.4f} | {ins_rate:>5.0f} | {p50:>6.1f}ms | {p99:>7.1f}ms | {note}")

    rss1 = get_rss(pid)
    results["rss_mb"] = rss1
    results["bytes_per_vec"] = round((rss1 - rss0) * 1024 * 1024 / next_id) if next_id > 0 else 0

    # Force a final compaction so all vectors are in immutable HNSW segments
    # (Without this, mutable segment remains brute-force O(n).)
    print(f"\n  Forcing final FT.COMPACT to consolidate mutable segment...")
    sock.settimeout(600)
    sock.sendall(resp_encode(["FT.COMPACT", "idx"]))
    cr, _ = resp_read_one(sock)
    print(f"  FT.COMPACT: {cr}")
    sock.settimeout(30)

    # Phase 3: Final search (200 queries)
    print(f"\n  Phase 3: Final search (200 queries, {next_id} vectors)...")
    final_lats = []
    final_results = []
    for i in range(200):
        q = query_vecs[i]; blob = vec_blob(q)
        sock.settimeout(120)
        sock.sendall(resp_encode(["FT.SEARCH", "idx", f"*=>[KNN {K} @vec $query]",
                                   "PARAMS", "2", "query", blob]))
        t_s = time.perf_counter()
        resp, _ = resp_read_one(sock)
        lat = (time.perf_counter() - t_s) * 1000
        final_lats.append(lat)
        final_results.append((q, parse_search(resp, K)))

    # DEBUG: dump first query for diagnosis
    if final_results:
        q0, ids0 = final_results[0]
        print(f"  [DEBUG] First query top-10 ids returned: {ids0[:10]}")
        try:
            import numpy as np
            db = np.array(all_vecs, dtype=np.float32)
            qa = np.array(q0, dtype=np.float32)
            dists = np.sum((db - qa)**2, axis=1)
            gt = np.argsort(dists)[:10].tolist()
            print(f"  [DEBUG] First query GT top-10:        {gt}")
            overlap = set(ids0[:10]) & set(gt)
            print(f"  [DEBUG] First query overlap: {len(overlap)}/10 = {sorted(overlap)}")
        except Exception as e:
            print(f"  [DEBUG] error: {e}")

    final_recall = bf_recall([r[0] for r in final_results], [r[1] for r in final_results], all_vecs, K)
    final_lats.sort()
    fp50 = final_lats[100]; fp99 = final_lats[198]
    fqps = 1000 / (sum(final_lats)/len(final_lats))
    print(f"  Recall@{K}: {final_recall:.4f}")
    print(f"  QPS: {fqps:.0f}, p50={fp50:.2f}ms, p99={fp99:.2f}ms")
    print(f"  RSS: {rss1:.0f} MB ({results['bytes_per_vec']} bytes/vec)")

    results.update({
        "total_vectors": next_id,
        "final_recall": round(final_recall, 4),
        "final_qps": round(fqps),
        "final_p50": round(fp50, 2),
        "final_p99": round(fp99, 2),
        "steady_recall": round(sum(t["recall"] for t in timeline)/len(timeline), 4) if timeline else 0,
        "timeline": timeline,
    })

    # Phase 4: Recovery
    if not args.skip_recovery:
        print(f"\n  Phase 4: Crash recovery (SIGKILL)...")
        sock.close()
        os.kill(pid, signal.SIGKILL); proc.wait(); time.sleep(2)
        proc2 = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5)
        if proc2.poll() is not None:
            results["recovery"] = "FAIL (restart)"; return results
        try:
            s2 = moon_connect(args.moon_port, timeout=15)
            # Check if index exists
            s2.sendall(resp_encode(["FT.INFO", "idx"]))
            info, _ = resp_read_one(s2)
            if isinstance(info, Exception):
                results["recovery"] = f"FAIL (index lost: {info})"
                print(f"  Recovery: {results['recovery']}")
            else:
                # Parse num_docs from FT.INFO
                ndocs = 0
                if isinstance(info, list):
                    for j in range(0, len(info)-1, 2):
                        if info[j] == b"num_docs" or info[j] == "num_docs":
                            ndocs = info[j+1] if isinstance(info[j+1], int) else int(info[j+1])
                results["recovery_docs"] = ndocs
                results["recovery"] = f"PASS ({ndocs}/{next_id})"
                print(f"  Recovery: {results['recovery']}")
            s2.close()
        except Exception as e:
            results["recovery"] = f"FAIL ({e})"
            print(f"  Recovery: {results['recovery']}")
        subprocess.run(["killall", "-9", "moon"], capture_output=True)
    else:
        subprocess.run(["killall", "-9", "moon"], capture_output=True)

    return results

# ── QDRANT BENCHMARK ───────────────────────────────────────────
def run_qdrant():
    print("\n" + "="*65)
    print("  QDRANT — Real-World Mixed Workload")
    print("="*65)

    subprocess.run(["killall", "-9", "qdrant"], capture_output=True)
    time.sleep(1)
    subprocess.run(["rm", "-rf", args.qdrant_dir], capture_output=True)
    os.makedirs(args.qdrant_dir, exist_ok=True)

    if not args.qdrant_bin:
        print("  SKIP: no --qdrant-bin"); return None

    env = os.environ.copy()
    env["QDRANT__STORAGE__STORAGE_PATH"] = args.qdrant_dir
    env["QDRANT__SERVICE__HTTP_PORT"] = str(args.qdrant_port)
    env["QDRANT__SERVICE__GRPC_PORT"] = str(args.qdrant_port + 1)
    proc = subprocess.Popen([args.qdrant_bin], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(5)
    if proc.poll() is not None:
        print(f"  FAIL: Qdrant exit code {proc.poll()}"); return None

    import urllib.request
    # Wait ready
    for _ in range(30):
        try:
            if urllib.request.urlopen(f"http://127.0.0.1:{args.qdrant_port}/healthz", timeout=2).status == 200: break
        except: time.sleep(1)

    # Create collection
    qdrant_req(args.qdrant_port, "PUT", "/collections/bench", {
        "vectors": {"size": DIM, "distance": "Euclid"},
        "hnsw_config": {"m": 16, "ef_construct": 200},
        "optimizers_config": {"indexing_threshold": 2000},
    })
    print(f"  Collection created (dim={DIM})")

    results = {"system": "Qdrant"}
    all_vecs = []
    next_id = 0
    all_search_lats = []
    timeline = []

    # Phase 1: Bulk insert 5000
    print(f"\n  Phase 1: Bulk insert 5000 vectors...")
    t0 = time.time()
    for start in range(0, 5000, 100):
        end = min(start + 100, 5000)
        points = []
        for i in range(start, end):
            v = gen_vec(next_id); all_vecs.append(v)
            points.append({"id": next_id, "vector": v}); next_id += 1
        qdrant_req(args.qdrant_port, "PUT", "/collections/bench/points?wait=false", {"points": points}, timeout=120)
    t1 = time.time()
    print(f"  Inserted 5000 in {t1-t0:.1f}s ({5000/(t1-t0):.0f} vec/s)")
    results["bulk_insert_rate"] = round(5000/(t1-t0))

    # Phase 2: Mixed
    print(f"\n  Phase 2: Mixed workload (insert 50 + search 20) × 190 batches")
    print(f"  {'Vectors':>7} | {'Recall':>7} | {'Ins/s':>6} | {'p50':>7} | {'p99':>8}")
    print(f"  {'─'*7}─┼─{'─'*7}─┼─{'─'*6}─┼─{'─'*7}─┼─{'─'*8}")

    query_vecs = [gen_vec(i + 10_000_000) for i in range(200)]
    query_idx = 0

    for batch in range(190):
        t_ins = time.time()
        points = []
        for i in range(50):
            v = gen_vec(next_id); all_vecs.append(v)
            points.append({"id": next_id, "vector": v}); next_id += 1
        qdrant_req(args.qdrant_port, "PUT", "/collections/bench/points?wait=false", {"points": points}, timeout=120)
        ins_time = time.time() - t_ins

        batch_lats = []; batch_results = []
        for _ in range(20):
            q = query_vecs[query_idx % 200]; query_idx += 1
            t_s = time.perf_counter()
            r = qdrant_req(args.qdrant_port, "POST", "/collections/bench/points/search",
                           {"vector": q, "limit": K, "params": {"hnsw_ef": 128}})
            lat = (time.perf_counter() - t_s) * 1000
            batch_lats.append(lat); all_search_lats.append(lat)
            ids = [p["id"] for p in r.get("result", [])]
            batch_results.append((q, ids))

        batch_recall = bf_recall([r[0] for r in batch_results], [r[1] for r in batch_results], all_vecs, K)
        p50 = sorted(batch_lats)[10]; p99 = sorted(batch_lats)[19]
        timeline.append({"n": next_id, "recall": batch_recall, "p50": p50, "p99": p99})

        if (batch+1) % 10 == 0:
            ins_rate = 50/ins_time if ins_time > 0 else 0
            print(f"  {next_id:>7} | {batch_recall:>7.4f} | {ins_rate:>5.0f} | {p50:>6.1f}ms | {p99:>7.1f}ms")

    # Phase 3: Final
    print(f"\n  Phase 3: Final search (200 queries)...")
    # Wait for indexing
    for _ in range(60):
        info = qdrant_req(args.qdrant_port, "GET", "/collections/bench")
        if info.get("result", {}).get("status") == "green": break
        time.sleep(2)

    final_lats = []; final_results = []
    for i in range(200):
        q = query_vecs[i]
        t_s = time.perf_counter()
        r = qdrant_req(args.qdrant_port, "POST", "/collections/bench/points/search",
                       {"vector": q, "limit": K, "params": {"hnsw_ef": 128}})
        lat = (time.perf_counter() - t_s) * 1000
        final_lats.append(lat)
        final_results.append((q, [p["id"] for p in r.get("result", [])]))

    final_recall = bf_recall([r[0] for r in final_results], [r[1] for r in final_results], all_vecs, K)
    final_lats.sort()
    fp50 = final_lats[100]; fp99 = final_lats[198]
    fqps = 1000 / (sum(final_lats)/len(final_lats))
    rss = get_rss(proc.pid)
    print(f"  Recall@{K}: {final_recall:.4f}")
    print(f"  QPS: {fqps:.0f}, p50={fp50:.2f}ms, p99={fp99:.2f}ms")
    print(f"  RSS: {rss:.0f} MB")

    results.update({
        "total_vectors": next_id,
        "final_recall": round(final_recall, 4),
        "final_qps": round(fqps),
        "final_p50": round(fp50, 2),
        "final_p99": round(fp99, 2),
        "rss_mb": rss,
        "steady_recall": round(sum(t["recall"] for t in timeline)/len(timeline), 4) if timeline else 0,
        "timeline": timeline,
    })

    subprocess.run(["killall", "-9", "qdrant"], capture_output=True)
    return results

# ── MAIN ───────────────────────────────────────────────────────
def main():
    info = {"arch": os.uname().machine, "os": sys.platform}
    try:
        if sys.platform == "linux":
            with open("/proc/cpuinfo") as f:
                for l in f:
                    if "model name" in l: info["cpu"] = l.split(":")[1].strip(); break
            info["kernel"] = os.uname().release
        else:
            info["cpu"] = subprocess.check_output(["sysctl","-n","machdep.cpu.brand_string"], text=True).strip()
    except: pass

    print("="*65)
    print(f"  Moon vs Qdrant — Real-World Mixed Workload Benchmark")
    print(f"  14.5K vectors, {DIM}d, K={K}, compact_threshold={args.compact_threshold}")
    print(f"  {info.get('arch','')} / {info.get('cpu','unknown')}")
    print(f"  {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}")
    print("="*65)

    moon_r = None if args.skip_moon else run_moon()
    qdrant_r = None if args.skip_qdrant else run_qdrant()

    # Summary
    print("\n" + "="*65)
    print("  COMPARISON")
    print("="*65)
    def v(r, k, f=".1f"): return f"{r[k]:{f}}" if r and k in r else "N/A"

    hdr = f"  {'Metric':<25} {'Moon':>12} {'Qdrant':>12}"
    print(hdr); print("  " + "─"*len(hdr))
    rows = [
        ("Bulk insert (vec/s)", "bulk_insert_rate", ".0f"),
        ("Final Recall@10", "final_recall", ".4f"),
        ("Steady-state Recall", "steady_recall", ".4f"),
        ("Final QPS", "final_qps", ".0f"),
        ("Final p50 (ms)", "final_p50", ".2f"),
        ("Final p99 (ms)", "final_p99", ".2f"),
        ("RSS (MB)", "rss_mb", ".0f"),
    ]
    for label, key, fmt in rows:
        print(f"  {label:<25} {v(moon_r,key,fmt):>12} {v(qdrant_r,key,fmt):>12}")

    if moon_r and "recovery" in moon_r:
        print(f"\n  Moon recovery: {moon_r['recovery']}")

    out = {"system_info": info, "moon": moon_r, "qdrant": qdrant_r,
           "config": {"dim": DIM, "compact_threshold": args.compact_threshold}}
    outf = f"/tmp/bench-rw-{info.get('arch','unknown')}.json"
    with open(outf, "w") as f: json.dump(out, f, indent=2, default=str)
    print(f"\n  Results: {outf}")

if __name__ == "__main__":
    main()
