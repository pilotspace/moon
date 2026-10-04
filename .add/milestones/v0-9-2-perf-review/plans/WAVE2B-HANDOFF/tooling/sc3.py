"""Reply-before-write(2) / reply-before-fsync detector, R2b round-3 review.

Each checked command is sent as [ECHO pre] + cmds + [ECHO post]; its reply
time is the entry of the socket write carrying the first byte after `pre`'s
reply (strict: the earliest the command's own reply can have left).
Keyed kinds: every listed key's first AOF write (after the sentinel) must
complete before that reply (and, under `always`, an fsync of that file must
complete in between). Counted kinds (flush/swapdb): on EVERY incr AOF file the
i-th record of that verb must be written (+fsynced) before the i-th reply.
"""
import os, re, signal, subprocess, sys, tempfile, time, threading
sys.path.insert(0, os.path.dirname(__file__))
from resp import Conn, enc, wait_up

LINE = re.compile(r'^(\d+)\s+(\d+\.\d+)\s+(.*)$')
HERE = os.path.dirname(os.path.abspath(__file__))


def parse(path):
    pend = {}
    out = []
    with open(path, errors="replace") as f:
        for raw in f:
            m = LINE.match(raw.rstrip("\n"))
            if not m:
                continue
            pid, t, rest = int(m.group(1)), float(m.group(2)), m.group(3)
            if rest.endswith("<unfinished ...>"):
                pend[pid] = (t, rest)
                continue
            if rest.startswith("<..."):
                if pid not in pend:
                    continue
                t0, head = pend.pop(pid)
                rest = head[:-len("<unfinished ...>")] + rest.split(">", 1)[1] if ">" in rest else head
                t_entry = t0
            else:
                t_entry = t
            dm = re.search(r'<(\d+\.\d+)>\s*$', rest)
            dur = float(dm.group(1)) if dm else 0.0
            fm = re.match(r'(\w+)\((\d+)(?:<([^>]*)>)?', rest)
            if not fm:
                continue
            sc = fm.group(1)
            rm = re.search(r'\)\s*=\s*(-?\d+)', rest)
            if rm and int(rm.group(1)) < 0:
                continue
            p = fm.group(3) or ("fd%s" % fm.group(2))
            out.append((t_entry, t_entry + dur, sc, p, rest))
    out.sort(key=lambda w: w[0])
    return out


def is_aof(p):
    return ".aof" in p or "appendonly" in p


def esc(s):
    return s.replace("\r\n", "\\r\\n")


def start(binary, port, d, shards, env_extra, trace):
    env = dict(os.environ, MOON_DISK_FREE_MIN_PCT="0", **env_extra)
    cmd = ["strace", "-f", "-y", "-ttt", "-T", "-s", "200000", "-e",
           "trace=write,writev,sendto,sendmsg,pwrite64,pwritev,fsync,fdatasync", "-o", trace,
           binary, "--port", str(port), "--shards", str(shards), "--appendonly", "yes",
           "--appendfsync", "everysec", "--auto-aof-rewrite-percentage", "0",
           "--disk-free-min-pct", "0", "--dir", d]
    return subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL,
                            stderr=open(os.path.join(d, "stderr.log"), "w"))


def build(kind, j, n):
    """List of (cmds, keys, setup_cmds). keys=None -> counted kind."""
    items = []
    for i in range(n):
        tag = "{t%d}" % j
        u = lambda s: "QK%s%s_%d_%dZ" % (kind, s, j, i)
        v = 7000000 + j * 10000 + i
        if kind == "mset":
            items.append(([("MSET", u("A"), v, u("B"), v, u("C"), v)], [u("A"), u("B"), u("C")], []))
        elif kind == "msetnx":
            items.append(([("MSETNX", u("A"), v, u("B"), v)], [u("A"), u("B")], []))
        elif kind == "del":
            items.append(([("DEL", u("A"), u("B"), u("C"))], [u("A"), u("B"), u("C")],
                          [("SET", u("A"), 1), ("SET", u("B"), 1), ("SET", u("C"), 1)]))
        elif kind == "unlink":
            items.append(([("UNLINK", u("A"), u("B"))], [u("A"), u("B")],
                          [("SET", u("A"), 1), ("SET", u("B"), 1)]))
        elif kind == "bitop":
            items.append(([("BITOP", "OR", u("D"), u("S"))], [u("D")], [("SET", u("S"), "abc%d" % v)]))
        elif kind == "copy":
            items.append(([("COPY", u("S"), u("D"))], [u("D")], [("SET", u("S"), v)]))
        elif kind == "set":  # inline-eligible SET (untagged: some keys local)
            items.append(([("SET", u("K"), v)], [u("K")], []))
        elif kind == "incr":
            items.append(([("INCRBY", u("K"), v)], [u("K")], []))
        elif kind == "eval":  # routed script (key owner varies)
            items.append(([("EVAL", "redis.call('SET', KEYS[1], ARGV[1]); return 1", 1, u("K"), v)], [u("K")], []))
        elif kind == "multi":  # routed MULTI: all keys on one (tagged) owner
            k = "QKmulti%s_%d_%dZ" % (tag, j, i)
            items.append(([("MULTI",), ("INCRBY", k, v), ("SET", k + "b", v), ("EXEC",)], [k, k + "b"], []))
        elif kind == "txn":
            k = "QKtxn%s_%d_%dZ" % (tag, j, i)
            items.append(([("TXN", "BEGIN"), ("INCRBY", k, v), ("TXN", "COMMIT")], [k], []))
    return items


COUNTED = {
    "flushall": ([("FLUSHALL",)], "FLUSHALL"),
    "flushdb": ([("FLUSHDB",)], "FLUSHDB"),
    "mflush": ([("MULTI",), ("FLUSHALL",), ("EXEC",)], "FLUSHALL"),
    "eflush": ([("EVAL", "redis.call('FLUSHALL'); return 1", 0)], "FLUSHALL"),
    "erflush": ([("EVAL", "redis.call('SET', KEYS[1], '1'); redis.call('FLUSHALL'); return 1", 1, "QKerf{rr}")], "FLUSHALL"),
    "mrflush": ([("MULTI",), ("SET", "QKmrf{rr}", 1), ("FLUSHALL",), ("EXEC",)], "FLUSHALL"),
    "swapdb": ([("SWAPDB", 0, 1)], "SWAPDB"),
}


def reply_time(writes, pre):
    """Entry time of the socket write carrying the first byte after pre's reply."""
    needle = esc("%s\r\n" % pre)
    for idx, (te, tx, sc, p, text) in enumerate(writes):
        if is_aof(p) or sc in ("fsync", "fdatasync"):
            continue
        pos = text.find(needle)
        if pos < 0:
            continue
        after = text[pos + len(needle):pos + len(needle) + 1]
        if after and after != '"':
            return te
        for (te2, tx2, sc2, p2, text2) in writes[idx + 1:]:
            if p2 == p and sc2 not in ("fsync", "fdatasync"):
                return te2
        return None
    return None


def synced_between(writes, path, t_from, t_to):
    for (te, tx, sc, p, text) in writes:
        if p == path and sc in ("fsync", "fdatasync") and te >= t_from - 1e-6 and tx <= t_to:
            return True
    return False


def check_keyed(writes, checks, t0, need_sync):
    viol = []
    checked = 0
    for (pre, keys) in checks:
        t_rep = reply_time(writes, pre)
        if t_rep is None:
            continue
        checked += 1
        for k in keys:
            hit = None
            for (te, tx, sc, p, text) in writes:
                if te >= t0 and is_aof(p) and sc not in ("fsync", "fdatasync") and k in text:
                    hit = (tx, p)
                    break
            if hit is None or t_rep < hit[0]:
                viol.append(("NOWRITE", k, pre, hit and hit[0], t_rep))
            elif need_sync and not synced_between(writes, hit[1], hit[0], t_rep):
                viol.append(("NOSYNC", k, pre, hit[0], t_rep))
    return checked, viol


def check_counted(writes, checks, verb, t0, need_sync):
    files = sorted({p for (te, tx, sc, p, text) in writes
                    if te >= t0 and is_aof(p) and "incr" in p and sc not in ("fsync", "fdatasync")})
    viol = []
    checked = 0
    for i, pre in enumerate(checks):
        t_rep = reply_time(writes, pre)
        if t_rep is None:
            continue
        checked += 1
        for f in files:
            cnt = 0
            when = None
            for (te, tx, sc, p, text) in writes:
                if p == f and te >= t0 and sc not in ("fsync", "fdatasync"):
                    cnt += text.count(verb)
                    if cnt >= i + 1:
                        when = tx
                        break
            if when is None or t_rep < when:
                viol.append(("NOWRITE", f[-40:], pre, when, t_rep))
            elif need_sync and not synced_between(writes, f, when, t_rep):
                viol.append(("NOSYNC", f[-40:], pre, when, t_rep))
    return checked, viol, files


def scenario(binary, port, shards, kind, conns, n, env_extra, mode, reps):
    total_viol, total_checked = [], 0
    for rep in range(reps):
        d = tempfile.mkdtemp(prefix="sc-", dir=HERE)
        trace = os.path.join(d, "trace.txt")
        env = dict(env_extra)
        if mode == "boot":
            env["MOON_TEST_AOF_WRITER_START_DELAY_MS"] = env.get("MOON_TEST_AOF_WRITER_START_DELAY_MS", "1500")
        p = start(binary, port, d, shards, env, trace)
        errors = 0
        try:
            c = wait_up(port, 60)
            counted = kind in COUNTED
            nconn = 1 if counted else conns
            streams = [Conn(port) for _ in range(nconn)]
            for s in streams:
                s.send("PING")
            plans, checks = [], []
            if counted:
                cmds, verb = COUNTED[kind]
                setup = [("SET", "QKprime%d" % x, x) for x in range(64)]
                if kind == "swapdb":
                    setup += [("SELECT", 1), ("SET", "QKdb1", 1), ("SELECT", 0)]
            else:
                setup = []
                for j in range(nconn):
                    for (cm, keys, su) in build(kind, j, n):
                        setup += su
            if mode != "boot" and setup:
                c.pipeline(setup)
            if mode == "always":
                assert "OK" in c.send("CONFIG", "SET", "appendfsync", "always")
            if mode == "after_always":
                assert "OK" in c.send("CONFIG", "SET", "appendfsync", "always")
                c.pipeline([("SET", "warm%d" % i, i) for i in range(32)])
                assert "OK" in c.send("CONFIG", "SET", "appendfsync", "everysec")
            if mode == "boot" and setup:
                # setup must not block: only counted kinds' priming (it blocks
                # until the writers start, which ends the window) -> skip.
                pass
            t_mark = time.time()
            if mode != "boot":
                c.send("SET", "QKSENTINEL", 1)
            if counted:
                for i in range(n):
                    pre = "PRE%s_%dX" % (kind, i)
                    rs = streams[0].pipeline([("ECHO", pre)] + cmds + [("ECHO", "POST%s_%dX" % (kind, i))])
                    if any(isinstance(r, str) and r.startswith("-") for r in rs[1:-1]) or \
                       any(isinstance(r, list) and any(isinstance(x, str) and x.startswith("-") for x in r) for r in rs[1:-1]):
                        errors += 1
                        print("   ERR reply", rs[1:-1], flush=True) if errors < 3 else None
                        continue
                    checks.append(pre)
            else:
                res = [[] for _ in range(nconn)]
                for j in range(nconn):
                    plan, its = [], []
                    for i, (cm, keys, su) in enumerate(build(kind, j, n)):
                        pre = "PRE%s_%d_%dX" % (kind, j, i)
                        plan += [("ECHO", pre)] + cm + [("ECHO", "POST%s_%d_%dX" % (kind, j, i))]
                        its.append((pre, keys, len(cm)))
                    plans.append((plan, its))

                def run(j):
                    res[j] = streams[j].pipeline(plans[j][0])
                ths = [threading.Thread(target=run, args=(j,)) for j in range(nconn)]
                for t in ths:
                    t.start()
                for t in ths:
                    t.join()
                for j in range(nconn):
                    pos = 0
                    for (pre, keys, ncm) in plans[j][1]:
                        rs = res[j][pos + 1:pos + 1 + ncm]
                        pos += ncm + 2
                        bad = any(isinstance(r, str) and r.startswith("-") for r in rs) or \
                            any(isinstance(r, list) and any(isinstance(x, str) and str(x).startswith("-") for x in r) for r in rs)
                        if bad:
                            errors += 1
                            if errors < 3:
                                print("   ERR reply", rs, flush=True)
                            continue
                        checks.append((pre, keys))
            time.sleep(0.3)
        finally:
            subprocess.run(["pkill", "-9", "-f", "--", "dir " + d], stderr=subprocess.DEVNULL)
            try:
                p.wait(timeout=20)
            except Exception:
                p.kill()
                p.wait()
            time.sleep(0.2)
        writes = parse(trace)
        t0 = 0.0
        for (te, tx, sc, pth, text) in writes:
            if is_aof(pth) and "QKSENTINEL" in text:
                t0 = te
                break
        need_sync = mode == "always"
        if kind in COUNTED:
            checked, viol, files = check_counted(writes, checks, COUNTED[kind][1], t0, need_sync)
            extra = "files=%d" % len(files)
        else:
            checked, viol = check_keyed(writes, checks, t0, need_sync)
            extra = ""
        total_checked += checked
        total_viol += viol
        print("rep %d kind=%s mode=%s shards=%d checked=%d errors=%d violations=%d %s" % (
            rep, kind, mode, shards, checked, errors, len(viol), extra), flush=True)
        for v in viol[:4]:
            print("   VIOL", v, flush=True)
        if checked == 0:
            print("   NO CHECKS; stderr:", open(os.path.join(d, "stderr.log")).read()[-400:], flush=True)
        if not viol and checked:
            subprocess.run(["rm", "-rf", d])
        else:
            print("   kept", d, flush=True)
    return total_checked, total_viol


if __name__ == "__main__":
    binary, port, shards, kind = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
    mode = sys.argv[5] if len(sys.argv) > 5 else "after_always"
    reps = int(sys.argv[6]) if len(sys.argv) > 6 else 1
    env = {}
    for kv in os.environ.get("EXTRA_ENV", "").split():
        a, b = kv.split("=", 1)
        env[a] = b
    ch, v = scenario(binary, port, shards, kind, int(os.environ.get("CONNS", "8")),
                     int(os.environ.get("PIPE", "40")), env, mode, reps)
    print("TOTAL %s %s s%d checked=%d violations=%d" % (kind, mode, shards, ch, len(v)), flush=True)
