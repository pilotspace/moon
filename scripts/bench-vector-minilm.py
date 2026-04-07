#!/usr/bin/env python3
"""
Moon vector benchmark with REAL MiniLM embeddings (clustered semantic data).

Compared to random Gaussian (concentration of distances → ~0.73 recall floor),
real MiniLM embeddings have clustered structure that HNSW exploits → ~0.92+ recall.

Usage: python3 scripts/bench-vector-minilm.py [--n 10000] [--queries 200]
"""
import argparse, json, os, socket, struct, subprocess, time
import numpy as np

p = argparse.ArgumentParser()
p.add_argument("--n", type=int, default=10000)
p.add_argument("--queries", type=int, default=200)
p.add_argument("--port", type=int, default=6399)
p.add_argument("--moon-bin", default="./target/release/moon")
p.add_argument("--cache", default="/tmp/minilm-cache")
args = p.parse_args()

# ── Generate or load MiniLM data ───────────────────────────────
def get_minilm_data():
    cache = args.cache
    os.makedirs(cache, exist_ok=True)
    db_path = f"{cache}/db_{args.n}.npy"
    q_path = f"{cache}/queries_{args.queries}.npy"
    if os.path.exists(db_path) and os.path.exists(q_path):
        return np.load(db_path), np.load(q_path)

    print(f"Generating {args.n} MiniLM embeddings + {args.queries} queries...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")
    rng = np.random.RandomState(42)
    nouns = ["machine","learning","data","science","cloud","network","system","model",
             "server","database","algorithm","pipeline","engine","platform","architecture",
             "deployment","container","cluster","storage","memory","processor","kernel",
             "module","function","method","structure","pattern","framework","protocol",
             "service","interface","driver","object","variable","computer","program",
             "developer","language","compiler","memory","cache","latency","throughput",
             "scalability","reliability","performance","optimization","security","privacy"]
    verbs = ["uses","processes","analyzes","computes","stores","retrieves","manages",
             "scales","optimizes","handles","executes","transforms","accelerates","monitors"]
    adjs = ["fast","efficient","scalable","distributed","reliable","secure","robust",
            "modern","advanced","intelligent","automated","real-time","high-performance"]
    sentences = []
    for _ in range(args.n + args.queries):
        sentences.append(f"The {rng.choice(adjs)} {rng.choice(nouns)} {rng.choice(verbs)} "
                        f"the {rng.choice(adjs)} {rng.choice(nouns)} for {rng.choice(nouns)} "
                        f"{rng.choice(nouns)} optimization")
    print(f"  Encoding {len(sentences)} sentences...")
    embs = model.encode(sentences, batch_size=64, show_progress_bar=False, normalize_embeddings=True)
    embs = embs.astype(np.float32)
    db = embs[:args.n]
    queries = embs[args.n:]
    np.save(db_path, db)
    np.save(q_path, queries)
    return db, queries

# ── RESP protocol ──────────────────────────────────────────────
def enc(args_):
    p = [f"*{len(args_)}\r\n".encode()]
    for x in args_:
        if isinstance(x, bytes): p.append(f"${len(x)}\r\n".encode() + x + b"\r\n")
        else: s = str(x); p.append(f"${len(s)}\r\n{s}\r\n".encode())
    return b"".join(p)

def read(sk, buf=b""):
    while b"\r\n" not in buf: buf += sk.recv(65536)
    pfx = buf[0:1]; i = buf.index(b"\r\n"); line = buf[:i]; rest = buf[i+2:]
    if pfx in (b"+",b"-"): return line[1:].decode(), rest
    if pfx == b":": return int(line[1:]), rest
    if pfx == b"$":
        n = int(line[1:])
        if n == -1: return None, rest
        while len(rest) < n+2: rest += sk.recv(65536)
        return rest[:n], rest[n+2:]
    if pfx == b"*":
        n = int(line[1:]); out = []
        for _ in range(n):
            e, rest = read(sk, rest); out.append(e)
        return out, rest
    return None, rest

def parse_ids(resp):
    ids = []
    if not isinstance(resp, list): return ids
    for x in resp:
        if isinstance(x, bytes):
            try: ids.append(int(x.decode().split(":")[1]))
            except: pass
    return ids

# ── Main ───────────────────────────────────────────────────────
def main():
    db, queries = get_minilm_data()
    DIM = db.shape[1]
    print(f"Loaded {db.shape[0]} db vectors, {queries.shape[0]} queries, dim={DIM}")

    # Brute force GT
    print(f"Computing brute-force GT...")
    t0 = time.time()
    gt = []
    for q in queries:
        d = np.sum((db - q)**2, axis=1)
        gt.append(np.argsort(d)[:10].tolist())
    print(f"GT computed in {time.time()-t0:.1f}s")

    # Start Moon
    subprocess.run(["killall", "-9", "moon"], capture_output=True)
    time.sleep(1)
    subprocess.run(["rm", "-rf", "/tmp/moon-minilm"], capture_output=True)
    os.makedirs("/tmp/moon-minilm", exist_ok=True)
    proc = subprocess.Popen(
        ["taskset", "-c", "0-3", args.moon_bin, "--port", str(args.port),
         "--shards", "1", "--protected-mode", "no", "--dir", "/tmp/moon-minilm"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(2)
    if proc.poll() is not None:
        print("FAIL: Moon failed to start"); return

    s = socket.socket(); s.connect(("127.0.0.1", args.port)); s.settimeout(600)
    s.sendall(enc(["PING"])); read(s)

    # Create index — high COMPACT_THRESHOLD to defer to single final compact
    s.sendall(enc(["FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
        "SCHEMA", "vec", "VECTOR", "HNSW", "16",
        "TYPE", "FLOAT32", "DIM", str(DIM), "DISTANCE_METRIC", "L2",
        "M", "16", "EF_CONSTRUCTION", "200", "EF_RUNTIME", "200",
        "COMPACT_THRESHOLD", str(args.n + 1), "QUANTIZATION", "TQ4"]))
    r, _ = read(s); print(f"FT.CREATE: {r}")

    # Insert
    print(f"Inserting {args.n} vectors...")
    t0 = time.time()
    for batch in range(0, args.n, 500):
        buf = bytearray()
        end = min(batch + 500, args.n)
        for i in range(batch, end):
            buf.extend(enc(["HSET", f"doc:{i}", "vec", db[i].tobytes()]))
        s.sendall(bytes(buf))
        rem = b""
        for _ in range(end - batch): _, rem = read(s, rem)
    print(f"Insert: {time.time()-t0:.1f}s ({args.n/(time.time()-t0):.0f} v/s)")

    # Force compact
    print(f"Compacting...")
    t0 = time.time()
    s.sendall(enc(["FT.COMPACT", "idx"])); read(s)
    print(f"Compact: {time.time()-t0:.1f}s")

    # FT.INFO
    s.sendall(enc(["FT.INFO", "idx"])); r, _ = read(s)
    for i in range(0, len(r)-1, 2):
        k = r[i].decode() if isinstance(r[i], bytes) else r[i]
        v = r[i+1]
        if isinstance(v, bytes): v = v.decode()
        if k == "num_docs": print(f"num_docs: {v}")

    # Warmup
    for i in range(min(100, len(queries))):
        s.sendall(enc(["FT.SEARCH", "idx", "*=>[KNN 10 @vec $q]", "PARAMS", "2", "q", queries[i].tobytes()]))
        read(s)

    # Measure
    lats = []
    results = []
    for q in queries:
        ts = time.perf_counter()
        s.sendall(enc(["FT.SEARCH", "idx", "*=>[KNN 10 @vec $q]", "PARAMS", "2", "q", q.tobytes()]))
        r, _ = read(s)
        lats.append((time.perf_counter() - ts) * 1000)
        results.append(parse_ids(r))

    lats.sort()
    qps = 1000 / (sum(lats) / len(lats))

    # Recall
    recalls = []
    for pred, truth in zip(results, gt):
        recalls.append(len(set(pred[:10]) & set(truth[:10])) / 10)
    recall = sum(recalls) / len(recalls)

    print(f"\nResults:")
    print(f"  Recall@10: {recall:.4f}")
    print(f"  QPS:       {qps:.0f}")
    print(f"  p50:       {lats[len(lats)//2]:.3f}ms")
    print(f"  p99:       {lats[int(len(lats)*0.99)]:.3f}ms")

    # Sample diagnosis
    print(f"  Q[0] Moon: {results[0][:5]}")
    print(f"  Q[0] GT:   {gt[0][:5]}")

    subprocess.run(["killall", "-9", "moon"], capture_output=True)

if __name__ == "__main__":
    main()
