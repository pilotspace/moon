#!/usr/bin/env python3
"""moon#513 A1 acceptance measurement: co-located multi-key in a pipeline.

    scripts/bench-single-owner-multikey.py <control-binary> <fix-binary>

Shape: `SET {t}:a v`, `SET {t}:b v`, `MGET {t}:a {t}:b` -- all three keys on ONE
shard, which is the `{tag}` co-location pattern CLAUDE.md tells users to adopt.

Two shapes are reported, and the difference between them is the point:

* **round-trip** -- send one group, wait for its replies, send the next. Every
  group is its own batch pass, so a control build cuts once per group. This is
  the read-modify-write shape moon#513 was filed from.
* **bulk** -- one huge write. The server does few batch passes, so a control
  build cuts only ~8 times per 8000 groups and the delta sits inside the noise.
  Reported anyway, because "no effect" on this shape is a fact about the shape,
  not about the fix.

Tags ROTATE per group. With a FIXED tag the connection-affinity tracker
migrates the connection onto the owner shard after ~16 samples (Linux only),
every key becomes local, and the cuts stop by themselves -- a fixed-tag leg
measures ~8 deferrals per 6000 groups and hides the effect entirely.

Three refusals guard the number, because a benchmark that cannot fail is not
evidence:

1. control and fix must be DIFFERENT binaries (sha256). Two checkouts of the
   same crate version sharing one CARGO_TARGET_DIR silently produce one binary,
   and the resulting ~0% delta reads exactly like "the fix does nothing".
2. the control must actually DEFER in the pre-flight, or the harness is not
   measuring what moon#513 A1 changes.
3. the drain is byte-exact and refuses on any error reply, so a run that lost
   or errored replies cannot be reported as a fast one.

Run on Linux (CLAUDE.md: production numbers never come from macOS). Legs
alternate control/fix/control/fix so host drift cannot masquerade as an effect.
"""
import socket, subprocess, sys, time, os, statistics, tempfile, shutil

SHARDS = "4"
REPS = 15
BATCHES = 8000

def enc(*parts):
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        b = p.encode() if isinstance(p, str) else p
        out += b"$%d\r\n%s\r\n" % (len(b), b)
    return out

def spawn(binary, port, dirpath):
    p = subprocess.Popen([binary, "--port", str(port), "--shards", SHARDS,
                          "--appendonly", "no", "--disk-free-min-pct", "0",
                          "--dir", dirpath],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(300):
        try:
            s = socket.create_connection(("127.0.0.1", port), 0.2); s.close(); return p
        except OSError:
            time.sleep(0.1)
    p.kill(); raise SystemExit(f"{binary} never accepted on {port}")

def defers(port):
    s = socket.create_connection(("127.0.0.1", port)); s.sendall(enc("INFO", "stats"))
    time.sleep(0.25); data = s.recv(1 << 20).decode(errors="replace"); s.close()
    for line in data.split("\r\n"):
        if line.startswith("total_pipeline_remote_defer:"):
            return int(line.split(":")[1])
    raise SystemExit("no total_pipeline_remote_defer in INFO stats")

def run_leg(port, tag, batches):
    """Returns (seconds, deferrals) for `batches` co-located SET,SET,MGET groups."""
    s = socket.create_connection(("127.0.0.1", port)); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    before = defers(port)
    payload = b""
    for i in range(batches):
        a, b = f"{{{tag}}}:a{i}", f"{{{tag}}}:b{i}"
        payload += enc("SET", a, "v") + enc("SET", b, "v") + enc("MGET", a, b)
    # Drain by BYTE COUNT, not by re-scanning a growing buffer: the obvious
    # "buf += chunk; buf.count(...)" is O(n^2) and dominated both the time and
    # the variance of the first version of this harness.
    #   SET reply            "+OK\r\n"                      =  5
    #   MGET reply (2 x "v") "*2\r\n$1\r\nv\r\n$1\r\nv\r\n"      = 18
    per_batch = 5 + 5 + 18
    need = batches * per_batch
    t0 = time.perf_counter()
    s.sendall(payload)
    got, saw_err = 0, False
    while got < need:
        chunk = s.recv(1 << 20)
        if not chunk: raise SystemExit("connection closed mid-drain")
        if b"-ERR" in chunk or b"-MOONERR" in chunk: saw_err = True
        got += len(chunk)
    el = time.perf_counter() - t0
    if saw_err or got != need:
        raise SystemExit(f"drain mismatch: got {got} want {need} err={saw_err} — "
                         f"the harness is not measuring the intended shape")
    s.close()
    return el, defers(port) - before

def bench(binary, port, tagbase):
    d = tempfile.mkdtemp(prefix="moon513-", dir="/tmp")
    p = spawn(binary, port, d)
    try:
        run_leg(port, tagbase + "warm", 500)           # warm
        times, defs = [], []
        for r in range(REPS):
            el, dd = run_leg(port, f"{tagbase}{r}", BATCHES)
            # First 3 reps per server are page-fault / allocator warm-up.
            if r >= 3:
                times.append(BATCHES * 3 / el); defs.append(dd)
        return times, defs
    finally:
        p.kill(); p.wait(); shutil.rmtree(d, ignore_errors=True)



# --- round-trip mode: the shape moon#513 was filed from ---------------------
# A read-modify-write loop pipelines a SMALL group and waits for its replies
# before sending the next one. Every group is then its own batch pass, so the
# control cuts ONCE PER GROUP — which is what makes the cut expensive. The
# bulk mode above sends one huge write, so the server does few batch passes and
# only ~8 cuts per 8000 groups; that is why its delta sits inside the noise and
# must not be reported as "no effect".
RTT_GROUPS = 6000
ROTATE = True

def run_rtt(port, tag, groups):
    s = socket.create_connection(("127.0.0.1", port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    before = defers(port)
    per_group = 5 + 5 + 18
    t0 = time.perf_counter()
    for i in range(groups):
        # ROTATING tag: each group's keys are co-located with each other but on
        # a different shard from the last group's, so the connection-affinity
        # tracker has no single migration target. A fixed tag migrates the
        # connection onto the owner shard after ~16 samples (Linux only), every
        # key goes local, and the cuts stop by themselves — which is why the
        # fixed-tag leg measured only 8 deferrals per 6000 groups.
        t = f"{tag}{i % 64}" if ROTATE else tag
        a, b = f"{{{t}}}:a{i}", f"{{{t}}}:b{i}"
        s.sendall(enc("SET", a, "v") + enc("SET", b, "v") + enc("MGET", a, b))
        got = 0
        while got < per_group:
            chunk = s.recv(4096)
            if not chunk: raise SystemExit("closed mid-drain")
            got += len(chunk)
        if got != per_group:
            raise SystemExit(f"group {i}: got {got} want {per_group}")
    el = time.perf_counter() - t0
    s.close()
    return groups * 3 / el, defers(port) - before

def bench_rtt(binary, port, tagbase):
    d = tempfile.mkdtemp(prefix="moon513r-", dir="/tmp")
    p = spawn(binary, port, d)
    try:
        run_rtt(port, tagbase + "warm", 500)
        times, defs = [], []
        for r in range(7):
            t, dd = run_rtt(port, f"{tagbase}{r}", RTT_GROUPS)
            times.append(t); defs.append(dd)
        return times, defs
    finally:
        p.kill(); p.wait(); shutil.rmtree(d, ignore_errors=True)

if __name__ == "__main__":
    ctl, fix = sys.argv[1], sys.argv[2]
    # --- pre-flight 0: the two sides must actually BE two binaries ---
    # A shared CARGO_TARGET_DIR across two checkouts of the same crate
    # name+version makes the second build a no-op, so "control" comes out
    # byte-identical to the subject and the benchmark prints ~0% — the most
    # believable wrong answer available. Observed 2026-08-26.
    import hashlib
    def sha(p):
        with open(p, "rb") as f: return hashlib.sha256(f.read()).hexdigest()
    hc, hf = sha(ctl), sha(fix)
    if hc == hf:
        raise SystemExit(f"REFUSING TO REPORT: control and fix are the same binary ({hc[:12]}…) "
                         f"— rebuild them in SEPARATE target dirs")
    print(f"binaries differ: control {hc[:12]}…  fix {hf[:12]}…")
    # --- pre-flight: the control MUST reproduce the deferral ---
    # Rotating tags, like the subject legs: a SINGLE tag lands on the
    # connection's own shard about 1 run in 4 and defers zero, which would
    # trip the refusal below for a reason that has nothing to do with the code.
    d = tempfile.mkdtemp(prefix="moon513pf-", dir="/tmp"); p = spawn(ctl, 7311, d)
    try:
        _, pf = run_rtt(7311, "preflight", 128)
    finally:
        p.kill(); p.wait(); shutil.rmtree(d, ignore_errors=True)
    print(f"pre-flight: control deferred {pf} times over 128 co-located round-trip groups")
    if pf == 0:
        raise SystemExit("REFUSING TO REPORT: the control binary never deferred, so this "
                         "harness is not measuring the thing moon#513 A1 changes")
    # --- interleaved A/B ---
    ct, cd, ft, fd = [], [], [], []
    for r in range(3):
        t, dd = bench(ctl, 7312, f"c{r}"); ct += t; cd += dd
        t, dd = bench(fix, 7313, f"f{r}"); ft += t; fd += dd
    def q(v, p):
        v = sorted(v); return v[int(p * (len(v) - 1))]
    med_c, med_f = statistics.median(ct), statistics.median(ft)
    for name, t, d in (("control", ct, cd), ("fix", ft, fd)):
        print(f"{name:8}: median {statistics.median(t):10,.0f} ops/s  "
              f"IQR {q(t,.25):,.0f}..{q(t,.75):,.0f}  "
              f"min..max {min(t):,.0f}..{max(t):,.0f}  n={len(t)}  "
              f"deferrals/leg {statistics.median(d):,.0f}")
    delta = 100 * (med_f - med_c) / med_c
    noise = max((q(ct,.75)-q(ct,.25))/med_c, (q(ft,.75)-q(ft,.25))/med_f) * 100
    print(f"delta   : {delta:+.1f}% median")
    print(f"noise   : {noise:.1f}% (widest IQR as % of its own median)")
    print("VERDICT : " + ("throughput delta is INSIDE the noise floor — report the "
                          "deferral count, not the percentage"
                          if abs(delta) < noise else
                          "throughput delta exceeds the noise floor"))

    # --- round-trip shape ---
    print()
    print("== round-trip shape (one batch pass per group — the filed shape) ==")
    rct, rcd, rft, rfd = [], [], [], []
    for r in range(3):
        t, dd = bench_rtt(ctl, 7314, f"rc{r}"); rct += t; rcd += dd
        t, dd = bench_rtt(fix, 7315, f"rf{r}"); rft += t; rfd += dd
    rmc, rmf = statistics.median(rct), statistics.median(rft)
    for name, t, d in (("control", rct, rcd), ("fix", rft, rfd)):
        print(f"{name:8}: median {statistics.median(t):10,.0f} ops/s  "
              f"IQR {q(t,.25):,.0f}..{q(t,.75):,.0f}  n={len(t)}  "
              f"deferrals/leg {statistics.median(d):,.0f}")
    rdelta = 100 * (rmf - rmc) / rmc
    rnoise = max((q(rct,.75)-q(rct,.25))/rmc, (q(rft,.75)-q(rft,.25))/rmf) * 100
    print(f"delta   : {rdelta:+.1f}% median")
    print(f"noise   : {rnoise:.1f}%")
    print("VERDICT : " + ("INSIDE the noise floor" if abs(rdelta) < rnoise
                          else "exceeds the noise floor"))
