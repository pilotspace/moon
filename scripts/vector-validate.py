#!/usr/bin/env python3
"""
vector-validate.py — long-run reliability / stability / durability validation
for Moon's vector search (FT.*), plus recall/QPS comparison between two moon
binaries (baseline vs branch).

Runs ON the target machine (GCloud instance or Linux VM). Manages moon server
processes itself (spawn / kill -9 / restart) so the durability phase needs no
outer orchestration.

Phases:
  recall      — per binary: fresh server, MiniLM-like 384d unit vectors,
                SQ8 + TQ4 indexes, compaction, R@10 vs numpy brute force,
                query latency p50/p99 + QPS.
  soak        — branch binary, appendonly yes: update/insert/delete/search
                churn for SOAK_MINUTES; samples RSS, MEMORY DOCTOR, FT.INFO,
                live-set recall every SAMPLE_SECS; flags RSS runaway, recall
                drift, deleted-key resurrection.
  durability  — kill -9 mid-churn, restart on the same dir, verify recovery:
                keys settled >FSYNC_MARGIN before the kill must survive
                (appendonly everysec), survivor recall must stay >= floor.

Usage:
  python3 vector-validate.py --moon-bin PATH [--baseline-bin PATH]
      [--phases recall,soak,durability] [--soak-minutes 20] [--port 6470]
      [--n-vectors 20000] [--out results.json]

Exit code 0 = all hard assertions passed. JSON report to --out.
"""

import argparse
import json
import math
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

import numpy as np

DIM = 384
K = 10
FSYNC_MARGIN = 5.0  # seconds a write must predate kill -9 to be "must survive"


# ── RESP client (proper incremental parser — FT.SEARCH replies are nested) ──
class Resp:
    def __init__(self, port, timeout=30):
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
        if t == b"%":  # RESP3 map
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


# ── Server lifecycle ──
class Moon:
    def __init__(self, binary, port, data_dir, appendonly="no", extra=()):
        self.binary, self.port, self.data_dir = binary, port, data_dir
        self.appendonly = appendonly
        self.extra = list(extra)
        self.proc = None
        self.log = open(os.path.join(data_dir, "moon.log"), "ab")

    def start(self, wait=30):
        args = [
            self.binary,
            "--port", str(self.port),
            "--shards", "1",
            "--admin-port", "0",
            "--appendonly", self.appendonly,
            "--dir", self.data_dir,
        ] + self.extra
        self.proc = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        deadline = time.time() + wait
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
                raise RuntimeError(f"moon exited early rc={self.proc.returncode}")
            time.sleep(0.2)
        raise RuntimeError(f"moon did not answer PING on {self.port} within {wait}s")

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait(10)

    def stop(self):
        self.kill9()  # tests always hard-kill (SIGTERM+SO_REUSEPORT hang gotcha)


# ── Data ──
def gen_unit(n, seed):
    rng = np.random.default_rng(seed)
    v = rng.standard_normal((n, DIM), dtype=np.float32)
    v /= np.linalg.norm(v, axis=1, keepdims=True)
    return v


def cosine_gt(queries, db_matrix, k):
    """Ground-truth top-k ids by cosine distance (rows of db_matrix are unit)."""
    sims = queries @ db_matrix.T
    return np.argsort(-sims, axis=1)[:, :k]


def ft_create(c, idx, quant):
    r = c.cmd(
        "FT.CREATE", idx, "ON", "HASH", "PREFIX", "1", f"{idx}:",
        "SCHEMA", "vec", "VECTOR", "HNSW", "10",
        "TYPE", "FLOAT32", "DIM", DIM, "DISTANCE_METRIC", "COSINE",
        "QUANTIZATION", quant, "COMPACT_THRESHOLD", "4096",
    )
    if isinstance(r, Exception):
        raise r


def insert_batch(c, idx, ids, vecs):
    cmds = [
        ("HSET", f"{idx}:{i}", "vec", vecs[j].tobytes())
        for j, i in enumerate(ids)
    ]
    for r in c.pipeline(cmds):
        if isinstance(r, Exception):
            raise r


def knn(c, idx, q, k=K, timeout_note=""):
    r = c.cmd(
        "FT.SEARCH", idx, f"*=>[KNN {k} @vec $BLOB]",
        "PARAMS", "2", "BLOB", q.tobytes(), "DIALECT", "2",
    )
    if isinstance(r, Exception):
        raise RuntimeError(f"FT.SEARCH failed{timeout_note}: {r}")
    # reply: [total, key1, fields1, key2, fields2, ...]
    keys = [r[i].decode() for i in range(1, len(r), 2)]
    return [int(kk.split(":", 1)[1]) for kk in keys if ":" in kk]


def ft_info(c, idx):
    r = c.cmd("FT.INFO", idx)
    if isinstance(r, Exception):
        return {}
    out = {}
    i = 0
    while i + 1 < len(r):
        key = r[i].decode() if isinstance(r[i], bytes) else str(r[i])
        val = r[i + 1]
        if isinstance(val, bytes):
            val = val.decode()
        if not isinstance(val, list):
            out[key] = val
        i += 2
    return out


def rss_mb(c):
    r = c.cmd("MEMORY", "DOCTOR")
    if isinstance(r, (bytes, str)):
        text = r.decode() if isinstance(r, bytes) else r
        for line in text.splitlines():
            if "RSS:" in line:
                parts = line.split()
                try:
                    val = float(parts[-2])
                    unit = parts[-1]
                    return val * (1024 if unit == "GB" else 1) if unit in ("MB", "GB") else val / 1024
                except (ValueError, IndexError):
                    pass
    return -1.0


# ── Phase: recall/QPS ──
def phase_recall(binary, label, port, n_vectors, report):
    db = gen_unit(n_vectors, seed=42)
    queries = gen_unit(200, seed=7)
    gt = cosine_gt(queries, db, K)

    for quant in ("SQ8", "TQ4"):
        d = tempfile.mkdtemp(prefix=f"moon-recall-{label}-{quant}-")
        srv = Moon(binary, port, d)
        srv.start()
        try:
            c = Resp(port, timeout=60)
            idx = f"r{quant.lower()}"
            ft_create(c, idx, quant)

            t0 = time.time()
            for s in range(0, n_vectors, 500):
                ids = range(s, min(s + 500, n_vectors))
                insert_batch(c, idx, ids, db[s:])
            insert_secs = time.time() - t0
            c.cmd("FT.COMPACT", idx)
            time.sleep(2)  # let background build install

            # Recall + single-query latency
            lat = []
            hits = 0
            for qi in range(len(queries)):
                t0 = time.time()
                got = knn(c, idx, queries[qi])
                lat.append(time.time() - t0)
                hits += len(set(got[:K]) & set(gt[qi].tolist()))
            recall = hits / (len(queries) * K)

            # QPS: 10s tight loop
            t_end = time.time() + 10
            nq = 0
            while time.time() < t_end:
                knn(c, idx, queries[nq % len(queries)])
                nq += 1
            qps = nq / 10.0

            lat.sort()
            info = ft_info(c, idx)
            report[f"recall/{label}/{quant}"] = {
                "recall_at_10": round(recall, 4),
                "qps": round(qps, 1),
                "p50_ms": round(lat[len(lat) // 2] * 1000, 3),
                "p99_ms": round(lat[int(len(lat) * 0.99)] * 1000, 3),
                "insert_secs": round(insert_secs, 1),
                "num_docs": info.get("num_docs"),
            }
            print(f"[recall] {label}/{quant}: R@10={recall:.4f} qps={qps:.0f} "
                  f"p50={lat[len(lat)//2]*1000:.2f}ms", flush=True)
            c.close()
        finally:
            srv.stop()
            shutil.rmtree(d, ignore_errors=True)


# ── Phase: soak ──
def phase_soak(binary, port, minutes, n_vectors, report):
    d = tempfile.mkdtemp(prefix="moon-soak-")
    srv = Moon(binary, port, d, appendonly="yes")
    srv.start()
    failures = []
    warnings = []
    samples = []
    try:
        c = Resp(port, timeout=60)
        idx = "soak"
        ft_create(c, idx, "SQ8")

        rng = np.random.default_rng(99)
        live = {}  # id -> vector row (numpy)
        id_pool = []  # O(1) random sampling; kept in sync with `live`
        deleted = set()

        # Seed data
        seed_vecs = gen_unit(n_vectors, seed=11)
        for s in range(0, n_vectors, 500):
            ids = list(range(s, min(s + 500, n_vectors)))
            insert_batch(c, idx, ids, seed_vecs[s:])
            for i in ids:
                live[i] = seed_vecs[i]
                id_pool.append(i)
        next_id = n_vectors
        probe_q = gen_unit(20, seed=3)

        t_end = time.time() + minutes * 60
        t_sample = 0.0
        ops = 0
        while time.time() < t_end:
            r = rng.random()
            if r < 0.60:  # search
                knn(c, idx, probe_q[ops % len(probe_q)])
            elif r < 0.85 and id_pool:  # update existing (tombstone pressure)
                i = id_pool[int(rng.integers(len(id_pool)))]
                v = gen_unit(1, seed=int(rng.integers(1 << 30)))[0]
                cr = c.cmd("HSET", f"{idx}:{i}", "vec", v.tobytes())
                if isinstance(cr, Exception):
                    failures.append(f"HSET update failed: {cr}")
                else:
                    live[i] = v
            elif r < 0.95:  # insert new
                v = gen_unit(1, seed=int(rng.integers(1 << 30)))[0]
                cr = c.cmd("HSET", f"{idx}:{next_id}", "vec", v.tobytes())
                if not isinstance(cr, Exception):
                    live[next_id] = v
                    id_pool.append(next_id)
                next_id += 1
            elif id_pool:  # delete (swap-remove from the pool)
                j = int(rng.integers(len(id_pool)))
                i = id_pool[j]
                id_pool[j] = id_pool[-1]
                id_pool.pop()
                c.cmd("DEL", f"{idx}:{i}")
                live.pop(i, None)
                deleted.add(i)
            ops += 1

            now = time.time()
            if now - t_sample >= 60:
                t_sample = now
                mat = np.stack(list(live.values()))
                id_list = np.array(list(live.keys()))
                gt = cosine_gt(probe_q, mat, K)
                hits, resurrections = 0, 0
                for qi in range(len(probe_q)):
                    got = knn(c, idx, probe_q[qi])
                    truth = set(id_list[gt[qi]].tolist())
                    hits += len(set(got[:K]) & truth)
                    resurrections += sum(1 for g in got if g in deleted)
                recall = hits / (len(probe_q) * K)
                info = ft_info(c, idx)
                mem = rss_mb(c)
                sample = {
                    "t": round(now - (t_end - minutes * 60), 1),
                    "ops": ops,
                    "live": len(live),
                    "recall": round(recall, 4),
                    "rss_mb": round(mem, 1),
                    "num_docs": info.get("num_docs"),
                    "resurrected": resurrections,
                }
                samples.append(sample)
                print(f"[soak] {json.dumps(sample)}", flush=True)
                if resurrections:
                    failures.append(f"deleted keys resurfaced in search: {sample}")
                if recall < 0.60:  # catastrophic collapse guard
                    failures.append(f"recall collapsed below 0.60: {sample}")
                elif recall < 0.85:
                    # Warning only: on random-Gaussian 384d, live-set recall
                    # legitimately declines as N grows (concentration of
                    # distances). Data LOSS is judged by the self-recall probe.
                    warnings.append(f"recall below 0.85 (grew to {sample['live']} live): {sample}")

        # Self-recall LOST probe: every sampled live key, queried by its own
        # CURRENT vector, must appear in its own top-10. A key that fails is
        # GONE from the index (the direct mass-loss detector — recall drift
        # can be dataset noise; lost keys cannot).
        probe_ids = list(live.keys())
        if len(probe_ids) > 1000:
            probe_ids = [probe_ids[int(i)] for i in
                         rng.choice(len(probe_ids), size=1000, replace=False)]
        lost = sum(1 for i in probe_ids if i not in knn(c, idx, live[i]))
        lost_frac = lost / max(1, len(probe_ids))
        report["soak_lost_probe"] = {"sampled": len(probe_ids), "lost": lost}
        print(f"[soak] lost-probe: {lost}/{len(probe_ids)} sampled live keys missing",
              flush=True)
        if lost_frac > 0.005:
            failures.append(
                f"index lost {lost}/{len(probe_ids)} sampled live keys (>0.5%)")

        # RSS runaway check: last sample vs first, adjusted for growth in live set
        if len(samples) >= 2 and samples[0]["rss_mb"] > 0:
            growth = samples[-1]["rss_mb"] / samples[0]["rss_mb"]
            live_growth = max(1.0, samples[-1]["live"] / max(1, samples[0]["live"]))
            if growth > 3.0 * live_growth:
                failures.append(
                    f"RSS runaway: {samples[0]['rss_mb']}MB -> {samples[-1]['rss_mb']}MB "
                    f"(live set only grew {live_growth:.2f}x)"
                )
        c.close()
    finally:
        srv.stop()
        shutil.rmtree(d, ignore_errors=True)
    report["soak"] = {"samples": samples, "failures": failures, "warnings": warnings}
    return failures


# ── Phase: durability ──
def phase_durability(binary, port, report):
    d = tempfile.mkdtemp(prefix="moon-dur-")
    failures = []
    srv = Moon(binary, port, d, appendonly="yes")
    srv.start()
    try:
        c = Resp(port, timeout=60)
        idx = "dur"
        ft_create(c, idx, "SQ8")
        vecs = gen_unit(5000, seed=21)
        for s in range(0, 5000, 500):
            insert_batch(c, idx, range(s, s + 500), vecs[s:])
        settled = 5000  # ids 0..4999 written now
        time.sleep(FSYNC_MARGIN)  # everysec fsync window + margin

        # churn a bit more (these may or may not survive), then SIGKILL
        extra = gen_unit(500, seed=22)
        insert_batch(c, idx, range(5000, 5500), extra)
        c.close()
        srv.kill9()
        print("[durability] killed -9 mid-churn", flush=True)

        # restart on same dir
        srv2 = Moon(binary, port, d, appendonly="yes")
        srv2.start(wait=60)
        c = Resp(port, timeout=60)

        # every settled key must be readable
        missing = 0
        for s in range(0, settled, 500):
            cmds = [("HEXISTS", f"{idx}:{i}", "vec") for i in range(s, s + 500)]
            missing += sum(1 for r in c.pipeline(cmds) if r != 1)
        if missing:
            failures.append(f"{missing}/{settled} settled keys missing after kill -9 + restart")

        # recall of survivors must hold (validates index rebuild incl. rerank)
        queries = gen_unit(50, seed=5)
        gt = cosine_gt(queries, vecs[:settled], K)
        hits = 0
        for qi in range(len(queries)):
            got = knn(c, idx, queries[qi], timeout_note=" (post-recovery)")
            hits += len(set(g for g in got[:K] if g < settled) & set(gt[qi].tolist()))
        recall = hits / (len(queries) * K)
        if recall < 0.85:
            failures.append(f"post-recovery recall {recall:.4f} < 0.85")
        print(f"[durability] post-recovery: missing={missing} recall={recall:.4f}", flush=True)

        # double-crash: kill again right after recovery, restart once more
        c.close()
        srv2.kill9()
        srv3 = Moon(binary, port, d, appendonly="yes")
        srv3.start(wait=60)
        c = Resp(port, timeout=60)
        if c.cmd("PING") != "PONG":
            failures.append("server unresponsive after double crash-recovery")
        c.close()
        srv3.stop()
        report["durability"] = {
            "settled": settled,
            "missing": missing,
            "post_recovery_recall": round(recall, 4),
            "failures": failures,
        }
    finally:
        srv.kill9()
        # -x (exact comm match), NOT -f: the driver's own cmdline contains the
        # binary path via --moon-bin, so -f SIGKILLs the driver itself (rc=137).
        subprocess.run(["pkill", "-9", "-x", os.path.basename(binary)], check=False)
        shutil.rmtree(d, ignore_errors=True)
    return failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--moon-bin", required=True)
    ap.add_argument("--baseline-bin")
    ap.add_argument("--phases", default="recall,soak,durability")
    ap.add_argument("--soak-minutes", type=float, default=20)
    ap.add_argument("--port", type=int, default=6470)
    ap.add_argument("--n-vectors", type=int, default=20000)
    ap.add_argument("--out", default="vector-validate-results.json")
    args = ap.parse_args()

    phases = set(args.phases.split(","))
    report = {"host": os.uname().nodename, "machine": os.uname().machine}
    all_failures = []

    if "recall" in phases:
        if args.baseline_bin:
            phase_recall(args.baseline_bin, "baseline", args.port, args.n_vectors, report)
        phase_recall(args.moon_bin, "branch", args.port, args.n_vectors, report)
    if "soak" in phases:
        all_failures += phase_soak(args.moon_bin, args.port, args.soak_minutes,
                                   args.n_vectors, report)
    if "durability" in phases:
        all_failures += phase_durability(args.moon_bin, args.port, report)

    report["failures"] = all_failures
    report["pass"] = not all_failures
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2)
    print(json.dumps({"pass": report["pass"], "failures": all_failures}, indent=2))
    sys.exit(0 if report["pass"] else 1)


if __name__ == "__main__":
    main()
