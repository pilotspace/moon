#!/usr/bin/env python3
"""bench-vector-vs-redisearch.py — Moon vs RediSearch (Redis Stack) vector head-to-head.

Runs ON the target machine. Both engines speak the same FT.* dialect, so one
driver benches both:
  - insert throughput (pipelined HSET, vectors auto-indexed)
  - FT.SEARCH KNN-10 QPS + p50/p99 (single connection, 10s tight loop)
  - R@10 vs numpy brute-force ground truth

Datasets (384d unit vectors):
  gaussian   — iid random Gaussian, normalized (harsh: concentration of distances)
  clustered  — 200-center Gaussian mixture, sigma 0.25 (closer to real embeddings)

Configs:
  redisearch — FLOAT32 HNSW, EF_RUNTIME default(10) and 64
  moon SQ8 / TQ4 — Moon quantized HNSW (COMPACT_THRESHOLD 4096 + FT.COMPACT)

Usage:
  python3 bench-vector-vs-redisearch.py --moon-bin PATH --redis-bin PATH \
      [--n-vectors 20000] [--out results.json]

`--redis-bin` is the redis-stack-server *redis-server* binary; the RediSearch
module is loaded via --loadmodule if REDISEARCH_SO is set, otherwise assumed
built in (redis-stack-server layout).
"""

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import numpy as np

DIM = 384
K = 10


# ── RESP client (same shape as vector-validate.py) ──
class Resp:
    def __init__(self, port, timeout=60):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

    def _read_more(self):
        chunk = self.sock.recv(1 << 16)
        if not chunk:
            raise ConnectionError("server closed connection")
        self.buf += chunk

    def _read_line(self):
        while b"\r\n" not in self.buf:
            self._read_more()
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n + 2:
            self._read_more()
        data, self.buf = self.buf[:n], self.buf[n + 2 :]
        return data

    def _parse(self):
        line = self._read_line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Exception(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n == -1 else self._read_exact(n)
        if t == b"*":
            n = int(rest)
            return None if n == -1 else [self._parse() for _ in range(n)]
        if t == b"%":
            n = int(rest)
            return [self._parse() for _ in range(2 * n)]
        raise ValueError(f"unexpected RESP type {line!r}")

    @staticmethod
    def encode(*args):
        parts = [f"*{len(args)}\r\n".encode()]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode()
            parts += [f"${len(b)}\r\n".encode(), b, b"\r\n"]
        return b"".join(parts)

    def cmd(self, *args):
        self.sock.sendall(self.encode(*args))
        return self._parse()

    def pipeline(self, cmds):
        self.sock.sendall(b"".join(self.encode(*c) for c in cmds))
        return [self._parse() for _ in cmds]


class Server:
    """Spawn either moon or redis-server(+RediSearch). Kill -9 only."""

    def __init__(self, argv, port, log_path):
        self.port = port
        self.log = open(log_path, "ab")
        self.proc = subprocess.Popen(argv, stdout=self.log, stderr=self.log)

    def wait_ready(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                c = Resp(self.port, timeout=2)
                if c.cmd("PING") == "PONG":
                    c.close()
                    return
                c.close()
            except Exception:
                pass
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited rc={self.proc.returncode}")
            time.sleep(0.2)
        raise RuntimeError(f"no PING on {self.port}")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(10)


# ── Data ──
def gen_gaussian(n, seed):
    rng = np.random.default_rng(seed)
    v = rng.standard_normal((n, DIM), dtype=np.float32)
    v /= np.linalg.norm(v, axis=1, keepdims=True)
    return v


def gen_clustered(n, seed, centers=200, sigma=0.04, center_seed=1234):
    # center_seed is FIXED so DB vectors and query vectors come from the SAME
    # mixture (in-distribution queries — like real embedding workloads).
    # sigma 0.04 at 384d puts members at ~0.8 cosine to their center (noise
    # norm sigma*sqrt(d) ~= 0.78 vs unit center) — real-embedding-like
    # neighborhoods. Larger sigmas drown the centers and degenerate to the
    # random-Gaussian regime.
    crng = np.random.default_rng(center_seed)
    c = crng.standard_normal((centers, DIM), dtype=np.float32)
    c /= np.linalg.norm(c, axis=1, keepdims=True)
    rng = np.random.default_rng(seed)
    which = rng.integers(0, centers, size=n)
    v = c[which] + sigma * rng.standard_normal((n, DIM), dtype=np.float32)
    v /= np.linalg.norm(v, axis=1, keepdims=True)
    return v.astype(np.float32)


def cosine_gt(queries, db, k):
    sims = queries @ db.T
    return np.argsort(-sims, axis=1)[:, :k]


def knn_ids(c, idx, q, k=K, ef_runtime=None):
    query = f"*=>[KNN {k} @vec $B" + (f" EF_RUNTIME {ef_runtime}" if ef_runtime else "") + "]"
    r = c.cmd(
        "FT.SEARCH", idx, query, "PARAMS", "2", "B", q.tobytes(),
        "NOCONTENT", "DIALECT", "2",
    )
    if isinstance(r, Exception):
        # Engines differ on NOCONTENT / EF_RUNTIME support — retry plain.
        r = c.cmd(
            "FT.SEARCH", idx, f"*=>[KNN {k} @vec $B]",
            "PARAMS", "2", "B", q.tobytes(), "DIALECT", "2",
        )
        if isinstance(r, Exception):
            raise RuntimeError(f"FT.SEARCH failed: {r}")
        keys = [r[i] for i in range(1, len(r), 2)]
    else:
        # NOCONTENT reply: [total, key1, key2, ...]; with-content: keys at odd idx.
        body = r[1:]
        if body and isinstance(body[0], bytes) and len(body) >= 2 and isinstance(body[1], list):
            keys = body[0::2]
        else:
            keys = body
    out = []
    for kk in keys:
        s = kk.decode() if isinstance(kk, bytes) else str(kk)
        if ":" in s:
            out.append(int(s.split(":", 1)[1]))
    return out


def ft_num_docs(c, idx):
    r = c.cmd("FT.INFO", idx)
    if isinstance(r, Exception):
        return -1
    for i in range(0, len(r) - 1, 2):
        k = r[i].decode() if isinstance(r[i], bytes) else str(r[i])
        if k == "num_docs":
            v = r[i + 1]
            return int(v.decode() if isinstance(v, bytes) else v)
    return -1


def settle_index(c, idx, n, timeout=120):
    """Wait until the index reports >= n docs (RediSearch backfill / Moon mutable)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if ft_num_docs(c, idx) >= n:
            return
        time.sleep(0.5)


def bench_engine(c, idx, vecs, queries, gt, ef_runtime=None, insert=True):
    n = len(vecs)
    res = {}
    if insert:
        t0 = time.time()
        for s in range(0, n, 500):
            cmds = [
                ("HSET", f"{idx}:{i}", "vec", vecs[i].tobytes())
                for i in range(s, min(s + 500, n))
            ]
            for r in c.pipeline(cmds):
                if isinstance(r, Exception):
                    raise RuntimeError(f"HSET failed: {r}")
        res["insert_secs"] = round(time.time() - t0, 2)
        res["insert_vps"] = round(n / max(1e-9, time.time() - t0))
        settle_index(c, idx, n)

    # warmup (drives Moon background-build installs; JITs RediSearch caches)
    for qi in range(20):
        knn_ids(c, idx, queries[qi % len(queries)], ef_runtime=ef_runtime)

    # recall + latency
    lat = []
    hits = 0
    for qi in range(len(queries)):
        t0 = time.time()
        got = knn_ids(c, idx, queries[qi], ef_runtime=ef_runtime)
        lat.append(time.time() - t0)
        hits += len(set(got[:K]) & set(gt[qi].tolist()))
    res["recall_at_10"] = round(hits / (len(queries) * K), 4)
    lat.sort()
    res["p50_ms"] = round(lat[len(lat) // 2] * 1000, 3)
    res["p99_ms"] = round(lat[int(len(lat) * 0.99)] * 1000, 3)

    t_end = time.time() + 10
    nq = 0
    while time.time() < t_end:
        knn_ids(c, idx, queries[nq % len(queries)], ef_runtime=ef_runtime)
        nq += 1
    res["qps"] = round(nq / 10.0, 1)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--moon-bin")
    ap.add_argument("--redis-bin")
    ap.add_argument("--n-vectors", type=int, default=20000)
    ap.add_argument("--port", type=int, default=6460)
    ap.add_argument("--out", default="vector-vs-redisearch.json")
    ap.add_argument("--only", choices=["moon", "redisearch"],
                    help="bench a single engine (smoke tests)")
    ap.add_argument("--moon-args", default="",
                    help="extra CLI args appended to the moon server command "
                         "(e.g. '--ft-search-workers 0' for A/B runs)")
    args = ap.parse_args()
    if args.only != "redisearch" and not args.moon_bin:
        ap.error("--moon-bin required")
    if args.only != "moon" and not args.redis_bin:
        ap.error("--redis-bin required")

    report = {"machine": os.uname().machine, "n_vectors": args.n_vectors}
    queries_per_ds = 200

    for ds_name, gen in (("gaussian", gen_gaussian), ("clustered", gen_clustered)):
        vecs = gen(args.n_vectors, seed=42)
        queries = gen(queries_per_ds, seed=7)
        gt = cosine_gt(queries, vecs, K)

        # ── RediSearch (FLOAT32 HNSW) ──
        if args.only != "moon":
            d = tempfile.mkdtemp(prefix="redisearch-")
            argv = [args.redis_bin, "--port", str(args.port), "--dir", d,
                    "--save", "", "--appendonly", "no"]
            so = os.environ.get("REDISEARCH_SO")
            if so:
                argv += ["--loadmodule", so]
            srv = Server(argv, args.port, os.path.join(d, "log"))
            try:
                srv.wait_ready()
                c = Resp(args.port)
                r = c.cmd(
                    "FT.CREATE", "r", "ON", "HASH", "PREFIX", "1", "r:",
                    "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                    "TYPE", "FLOAT32", "DIM", DIM, "DISTANCE_METRIC", "COSINE",
                )
                if isinstance(r, Exception):
                    raise RuntimeError(f"redisearch FT.CREATE: {r}")
                base = bench_engine(c, "r", vecs, queries, gt)
                report[f"{ds_name}/redisearch/ef-default"] = base
                print(f"[{ds_name}] redisearch/ef-default: {base}", flush=True)
                # Sweep EF_RUNTIME to find RediSearch's recall-matched operating
                # point — QPS is only comparable at equal recall.
                for ef in (64, 128, 256, 512):
                    hi = bench_engine(c, "r", vecs, queries, gt, ef_runtime=ef, insert=False)
                    report[f"{ds_name}/redisearch/ef-{ef}"] = hi
                    print(f"[{ds_name}] redisearch/ef-{ef}: {hi}", flush=True)
                c.close()
            finally:
                srv.stop()
                shutil.rmtree(d, ignore_errors=True)

        # ── Moon SQ8 / TQ4 ──
        for quant in ("SQ8", "TQ4") if args.only != "redisearch" else ():
            d = tempfile.mkdtemp(prefix=f"moon-{quant}-")
            argv = [args.moon_bin, "--port", str(args.port), "--shards", "1",
                    "--admin-port", "0", "--appendonly", "no", "--dir", d]
            argv += args.moon_args.split()
            srv = Server(argv, args.port, os.path.join(d, "log"))
            try:
                srv.wait_ready()
                c = Resp(args.port)
                r = c.cmd(
                    "FT.CREATE", "m", "ON", "HASH", "PREFIX", "1", "m:",
                    "SCHEMA", "vec", "VECTOR", "HNSW", "10",
                    "TYPE", "FLOAT32", "DIM", DIM, "DISTANCE_METRIC", "COSINE",
                    "QUANTIZATION", quant, "COMPACT_THRESHOLD", "4096",
                )
                if isinstance(r, Exception):
                    raise RuntimeError(f"moon FT.CREATE: {r}")
                res = bench_engine(c, "m", vecs, queries, gt)
                c.cmd("FT.COMPACT", "m")
                time.sleep(2)
                post = bench_engine(c, "m", vecs, queries, gt, insert=False)
                res.update({f"compacted_{k}": v for k, v in post.items()})
                report[f"{ds_name}/moon/{quant}"] = res
                print(f"[{ds_name}] moon/{quant}: {res}", flush=True)
                c.close()
            finally:
                srv.stop()
                shutil.rmtree(d, ignore_errors=True)

    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
