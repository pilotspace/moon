#!/usr/bin/env python3
"""
MiniLM-384d Vector Benchmark: Moon vs Qdrant with Recall@10
Generates synthetic MiniLM-like vectors (384d, unit-normalized),
inserts into both engines, queries, and measures recall against brute-force ground truth.
"""
import socket, struct, random, time, math, json, sys
from urllib.request import urlopen, Request
from urllib.error import URLError

DIM = 384  # MiniLM-L6-v2 dimension
N_VECTORS = 10000
N_QUERIES = 200
K = 10
MOON_PORT = 6400
QDRANT_PORT = 6333

def generate_unit_vector(dim, seed):
    """Generate a unit-normalized vector (mimics MiniLM output distribution)."""
    random.seed(seed)
    v = [random.gauss(0, 1) for _ in range(dim)]
    norm = math.sqrt(sum(x*x for x in v))
    if norm > 0:
        v = [x / norm for x in v]
    return v

def cosine_distance(a, b):
    dot = sum(x*y for x, y in zip(a, b))
    return 1.0 - dot  # cosine distance for unit vectors

def brute_force_knn(queries, database, k):
    """Compute ground-truth k-NN for each query via brute force."""
    results = []
    for q in queries:
        dists = [(i, cosine_distance(q, d)) for i, d in enumerate(database)]
        dists.sort(key=lambda x: x[1])
        results.append([idx for idx, _ in dists[:k]])
    return results

def recall_at_k(predicted, ground_truth, k):
    """Compute recall@k: fraction of true top-k neighbors found."""
    if not predicted or not ground_truth:
        return 0.0
    gt_set = set(ground_truth[:k])
    pred_set = set(predicted[:k])
    return len(gt_set & pred_set) / k

# ── HTTP helper for Qdrant ──
def qdrant_request(method, path, data=None):
    url = f"http://localhost:{QDRANT_PORT}{path}"
    body = json.dumps(data).encode() if data else None
    req = Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}

# ── RESP helpers for Moon ──
def resp_cmd(*args):
    """Build RESP protocol command."""
    parts = [f"*{len(args)}\r\n".encode()]
    for a in args:
        if isinstance(a, bytes):
            parts.append(f"${len(a)}\r\n".encode())
            parts.append(a)
            parts.append(b"\r\n")
        else:
            s = str(a)
            parts.append(f"${len(s)}\r\n{s}\r\n".encode())
    return b"".join(parts)

def recv_resp(sock):
    """Read one RESP reply (simplified)."""
    buf = b""
    sock.settimeout(10)
    while True:
        chunk = sock.recv(8192)
        if not chunk:
            break
        buf += chunk
        # Simple heuristic: if we got a complete line, return
        if b"\r\n" in buf:
            break
    return buf

def recv_all_replies(sock, count):
    """Drain count RESP replies."""
    sock.settimeout(2)
    total = b""
    try:
        while True:
            d = sock.recv(65536)
            if not d:
                break
            total += d
    except:
        pass
    return total

def main():
    print(f"=== MiniLM-384d Benchmark: {N_VECTORS} vectors, {N_QUERIES} queries, k={K} ===\n")

    # ── Generate data ──
    print("Generating vectors...")
    t0 = time.time()
    database = [generate_unit_vector(DIM, i) for i in range(N_VECTORS)]
    queries = [generate_unit_vector(DIM, i + 1000000) for i in range(N_QUERIES)]
    t_gen = time.time() - t0
    print(f"  {N_VECTORS} database + {N_QUERIES} query vectors in {t_gen:.1f}s")

    # ── Compute ground truth ──
    print("Computing brute-force ground truth...")
    t0 = time.time()
    ground_truth = brute_force_knn(queries, database, K)
    t_gt = time.time() - t0
    print(f"  Ground truth computed in {t_gt:.1f}s")

    # ════════════════════════════════════════════
    # QDRANT
    # ════════════════════════════════════════════
    print("\n--- QDRANT ---")

    # Delete old collection
    qdrant_request("DELETE", "/collections/minilm")
    time.sleep(0.5)

    # Create collection
    r = qdrant_request("PUT", "/collections/minilm", {
        "vectors": {"size": DIM, "distance": "Cosine"},
        "optimizers_config": {"indexing_threshold": 0}  # force immediate indexing
    })
    if "error" in r:
        print(f"  Qdrant create failed: {r}")
        qdrant_ok = False
    else:
        qdrant_ok = True
        print("  Collection created")

    if qdrant_ok:
        # Insert in batches of 100
        print(f"  Inserting {N_VECTORS} vectors...")
        t0 = time.time()
        batch_size = 100
        for start in range(0, N_VECTORS, batch_size):
            end = min(start + batch_size, N_VECTORS)
            points = [{"id": i, "vector": database[i]} for i in range(start, end)]
            qdrant_request("PUT", "/collections/minilm/points", {"points": points})
        t_qi = time.time() - t0
        q_ips = N_VECTORS / t_qi
        print(f"  Insert: {t_qi:.1f}s ({q_ips:.0f} vec/s)")

        # Wait for indexing
        time.sleep(2)

        # Query
        print(f"  Searching {N_QUERIES} queries (k={K})...")
        qdrant_results = []
        t0 = time.time()
        for qi, qvec in enumerate(queries):
            r = qdrant_request("POST", "/collections/minilm/points/search", {
                "vector": qvec, "limit": K
            })
            if "result" in r:
                ids = [p["id"] for p in r["result"]]
                qdrant_results.append(ids)
            else:
                qdrant_results.append([])
        t_qq = time.time() - t0
        q_qps = N_QUERIES / t_qq
        print(f"  Search: {t_qq:.1f}s ({q_qps:.1f} QPS)")

        # Recall
        recalls = [recall_at_k(pred, gt, K) for pred, gt in zip(qdrant_results, ground_truth)]
        q_recall = sum(recalls) / len(recalls)
        print(f"  Recall@{K}: {q_recall:.4f}")
    else:
        t_qi, q_ips, t_qq, q_qps, q_recall = 0, 0, 0, 0, 0

    # ════════════════════════════════════════════
    # MOON
    # ════════════════════════════════════════════
    print("\n--- MOON ---")

    sock = socket.socket()
    try:
        sock.connect(("127.0.0.1", MOON_PORT))
    except:
        print("  Moon not reachable")
        return

    # Create index: FT.CREATE minilm ON HASH PREFIX 1 ml: SCHEMA emb VECTOR HNSW 6 DIM 384 DISTANCE_METRIC COSINE TYPE FLOAT32
    create_cmd = resp_cmd(
        "FT.CREATE", "minilm", "ON", "HASH", "PREFIX", "1", "ml:",
        "SCHEMA", "emb", "VECTOR", "HNSW", "6",
        "DIM", str(DIM), "DISTANCE_METRIC", "COSINE", "TYPE", "FLOAT32"
    )
    sock.sendall(create_cmd)
    r = recv_resp(sock)
    print(f"  FT.CREATE: {r.decode(errors='replace').strip()}")

    # Insert via pipelined HSET
    print(f"  Inserting {N_VECTORS} vectors...")
    t0 = time.time()
    batch = bytearray()
    for i in range(N_VECTORS):
        blob = struct.pack(f"{DIM}f", *database[i])
        key = f"ml:{i}"
        cmd = resp_cmd("HSET", key, "emb", blob)
        batch += cmd
        if len(batch) > 65536:
            sock.sendall(bytes(batch))
            batch = bytearray()
    if batch:
        sock.sendall(bytes(batch))

    # Drain insert replies
    time.sleep(2)
    recv_all_replies(sock, N_VECTORS)
    t_mi = time.time() - t0
    m_ips = N_VECTORS / t_mi
    print(f"  Insert: {t_mi:.1f}s ({m_ips:.0f} vec/s)")

    # Search: FT.SEARCH minilm "*=>[KNN 10 @emb $BLOB AS score]" PARAMS 2 BLOB <blob> DIALECT 2
    print(f"  Searching {N_QUERIES} queries (k={K})...")
    moon_results = []
    t0 = time.time()
    query_str = f"*=>[KNN {K} @emb $BLOB AS score]"
    for qi, qvec in enumerate(queries):
        blob = struct.pack(f"{DIM}f", *qvec)
        cmd = resp_cmd("FT.SEARCH", "minilm", query_str,
                       "PARAMS", "2", "BLOB", blob, "DIALECT", "2")
        sock.sendall(cmd)
        try:
            r = recv_resp(sock)
            # Parse RESP array to extract IDs
            # Response format: *N\r\n (count) then pairs of key, fields
            text = r.decode(errors="replace")
            ids = []
            # Extract ml:NNN patterns
            import re
            for m in re.finditer(r'ml:(\d+)', text):
                ids.append(int(m.group(1)))
            moon_results.append(ids[:K])
        except:
            moon_results.append([])
    t_mq = time.time() - t0
    m_qps = N_QUERIES / t_mq if t_mq > 0 else 0
    print(f"  Search: {t_mq:.1f}s ({m_qps:.1f} QPS)")

    # Recall
    if moon_results and any(len(r) > 0 for r in moon_results):
        recalls = [recall_at_k(pred, gt, K) for pred, gt in zip(moon_results, ground_truth)]
        m_recall = sum(recalls) / len(recalls)
        valid = sum(1 for r in moon_results if len(r) > 0)
        print(f"  Recall@{K}: {m_recall:.4f} ({valid}/{N_QUERIES} queries returned results)")
    else:
        m_recall = 0
        print(f"  No search results returned (FT.SEARCH may need different syntax)")

    sock.close()

    # ════════════════════════════════════════════
    # SUMMARY
    # ════════════════════════════════════════════
    print("\n" + "=" * 65)
    print(f"  RESULTS: MiniLM-384d, {N_VECTORS} vectors, {N_QUERIES} queries, k={K}")
    print("=" * 65)
    print(f"")
    print(f"| Metric              | Moon          | Qdrant        | Moon/Qdrant |")
    print(f"|---------------------|---------------|---------------|-------------|")
    print(f"| Insert {N_VECTORS:,}       | {t_mi:.1f}s ({m_ips:.0f}/s) | {t_qi:.1f}s ({q_ips:.0f}/s) | {m_ips/q_ips:.1f}x" if q_ips > 0 else "| --          |")
    print(f"| Search QPS (k={K})   | {m_qps:.1f}         | {q_qps:.1f}         | {m_qps/q_qps:.1f}x" if q_qps > 0 else "| --          |")
    print(f"| Recall@{K}           | {m_recall:.4f}        | {q_recall:.4f}        | {'--' if m_recall == 0 else f'{m_recall/q_recall:.2f}x' if q_recall > 0 else '--'}          |")
    print()

if __name__ == "__main__":
    main()
