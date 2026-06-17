#!/usr/bin/env bash
# GCloud 4-feature benchmark: KV, Vector, Graph, Full-Text Search.
# Moon vs Redis-family where a baseline exists:
#   KV     -> Redis (apt)
#   Vector -> RediSearch (redis-stack-server, Docker)
#   FTS    -> RediSearch (redis-stack-server, Docker)
#   Graph  -> FalkorDB (Docker)
# Robust: a failing feature/competitor never kills the rest. Parseable output: KV| VEC| FTS| GRAPH|
set -uo pipefail
ARCH="$(uname -m)"; BR="${MOON_BRANCH:-main}"
MOON=6399; REDIS=6379; STACK=6380; FALKOR=6381
echo "================ 4FEATURE BENCH START · arch=$ARCH · $(date -u) ================"

# ---------- deps ----------
command -v cargo >/dev/null 2>&1 || { curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.94.1 >/dev/null 2>&1; }
source "$HOME/.cargo/env"
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq >/dev/null 2>&1
sudo apt-get install -y -qq build-essential pkg-config libssl-dev git redis-server redis-tools \
     python3 python3-pip python3-redis python3-numpy docker.io >/dev/null 2>&1
sudo systemctl start docker >/dev/null 2>&1 || true
sudo usermod -aG docker "$USER" >/dev/null 2>&1 || true
DOCKER="sudo docker"

# ---------- competitor containers (best-effort) ----------
if [ "${SKIP_COMPETITORS:-0}" = 1 ]; then
  echo "[setup] SKIP_COMPETITORS=1 — Moon-only run"
else
  $DOCKER rm -f rstack falkor >/dev/null 2>&1 || true
  echo "[setup] pulling competitor images (redis-stack, falkordb)…"
  $DOCKER run -d --name rstack -p ${STACK}:6379 redis/redis-stack-server:latest >/dev/null 2>&1 && echo "[setup] redis-stack up" || echo "[setup] redis-stack FAILED (vector/fts competitor skipped)"
  $DOCKER run -d --name falkor -p ${FALKOR}:6379 falkordb/falkordb:latest >/dev/null 2>&1 && echo "[setup] falkordb up" || echo "[setup] falkordb FAILED (graph competitor skipped)"
fi

# ---------- build Moon (or reuse MOONBIN override for local smoke test) ----------
if [ -n "${MOONBIN:-}" ] && [ -x "${MOONBIN:-}" ]; then
  echo "HEAD: (using MOONBIN override $MOONBIN)"
  cd "$HOME/moon" 2>/dev/null || cd /Volumes/Games/tindang-repo/moon 2>/dev/null || true
else
  rm -rf "$HOME/moon"; git clone --depth 1 --branch "$BR" https://github.com/pilotspace/moon.git "$HOME/moon" >/dev/null 2>&1
  cd "$HOME/moon" || { echo "CLONE FAILED"; echo "4FEATURE BENCH DONE · $ARCH"; exit 1; }
  echo "HEAD: $(git log --oneline -1)"
  RUSTFLAGS="-C target-cpu=native" cargo build --release 2>&1 | tail -1
  MOONBIN="$HOME/moon/target/release/moon"
fi
[ -x "$MOONBIN" ] || { echo "BUILD FAILED"; echo "4FEATURE BENCH DONE · $ARCH"; exit 1; }
echo "CPU: $(lscpu | awk -F: '/Model name/{print $2}' | xargs) · cores=$(nproc)"
IKH=""; "$MOONBIN" --help 2>&1 | grep -q initial-keyspace-hint && IKH="--initial-keyspace-hint 1000000"

wp(){ for _ in $(seq 1 60); do redis-cli -p "$1" PING 2>/dev/null | grep -q PONG && return 0; sleep 0.5; done; return 1; }
kill_moon(){ pkill -9 -x moon 2>/dev/null; sleep 1; }
start_moon(){ rm -rf /tmp/md; mkdir -p /tmp/md; "$MOONBIN" --port $MOON --shards "${1:-1}" --protected-mode no --appendonly no --disk-offload disable $IKH --dir /tmp/md >/dev/null 2>&1 & wp $MOON; }

# wait competitors (best-effort, up to ~40s for module load)
STACK_OK=0; FALKOR_OK=0
if [ "${SKIP_COMPETITORS:-0}" != 1 ]; then
  wp $STACK && redis-cli -p $STACK FT._LIST >/dev/null 2>&1 && STACK_OK=1 || true
  wp $FALKOR && FALKOR_OK=1 || true
fi
echo "[setup] competitor ready: redis-stack=$STACK_OK falkordb=$FALKOR_OK"

#############################################################################
# 1) KV — Moon-fair vs Redis (loose + strict, best-of-3)
#############################################################################
echo "---- [1/4] KV: Moon-fair vs Redis ----"
NREQ=${KV_NREQ:-400000}
rps(){ local port=$1 op=$2 p=$3 s=$4 b=0 r f=""; [ "$s" = 1 ] && f="-r 1000000"
  for _ in 1 2 3; do r=$(redis-benchmark -p "$port" -c 50 -n "$NREQ" -P "$p" $f -t "$op" -d 64 --csv -q 2>/dev/null|tr -d '"'|awk -F, -v O="${op^^}" '$1==O{print $2;exit}'); r=${r%.*}; [ -n "$r" ]&&[ "$r" -gt "$b" ] 2>/dev/null && b=$r; done; echo "$b"; }
pkill -9 -f 'redis-server --port' 2>/dev/null; sleep 1
redis-server --port $REDIS --save "" --appendonly no --protected-mode no --daemonize yes --loglevel warning --dir /tmp >/dev/null 2>&1; wp $REDIS
for METH in loose strict; do S=0; [ "$METH" = strict ] && S=1
  kill_moon; start_moon 1
  for P in 1 16 64; do for OP in get set; do
    echo "KV|$ARCH|$METH|$OP|P$P|moon=$(rps $MOON $OP $P $S)|redis=$(rps $REDIS $OP $P $S)"
  done; done
done
kill_moon

#############################################################################
# 2/3/4) Vector + FTS + Graph via python (redis-py)
#############################################################################
start_moon 1
python3 - "$ARCH" "$MOON" "$STACK" "$FALKOR" "$STACK_OK" "$FALKOR_OK" <<'PY' 2>&1
import sys, os, time, struct, random, math, threading
import numpy as np
import redis
arch, MOON, STACK, FALKOR = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
STACK_OK, FALKOR_OK = sys.argv[5]=="1", sys.argv[6]=="1"
def envi(k,d): return int(os.environ.get(k,d))
def envf(k,d): return float(os.environ.get(k,d))
random.seed(7); np.random.seed(7)
def conn(p): return redis.Redis(host='127.0.0.1', port=p, decode_responses=False)
def emit(*a): print("|".join(str(x) for x in a)); sys.stdout.flush()

# concurrent QPS+latency: M threads issue qfn(c) for DURATION s; returns (qps,p50ms,p99ms,n)
DUR=envf("BENCH_DUR",6.0)
def load(make_conn, qfn, DURATION=None, THREADS=8):
    if DURATION is None: DURATION=DUR
    stop=time.time()+DURATION; lat=[]; cnt=[0]; lk=threading.Lock()
    def worker():
        c=make_conn(); L=[]; n=0
        while time.time()<stop:
            t=time.perf_counter()
            try: qfn(c)
            except Exception: pass
            L.append((time.perf_counter()-t)*1000); n+=1
        with lk: lat.extend(L); cnt[0]+=n
    ts=[threading.Thread(target=worker) for _ in range(THREADS)]
    t0=time.time(); [t.start() for t in ts]; [t.join() for t in ts]; el=time.time()-t0
    lat.sort(); n=len(lat)
    p50=lat[int(n*.50)] if n else 0; p99=lat[int(n*.99)] if n else 0
    return cnt[0]/el, p50, p99, n

############################# VECTOR #############################
emit("VEC", arch, "=== Vector: 50K x 384d clustered, COSINE, KNN10 ===")
DIM=384; NUM=envi("VEC_NUM",50000); NQ=envi("VEC_NQ",500); K=10; NCLUST=200
centers=np.random.randn(NCLUST,DIM).astype('float32')
lab=np.random.randint(0,NCLUST,NUM)
vecs=(centers[lab]+0.35*np.random.randn(NUM,DIM).astype('float32'))
vecs/=np.linalg.norm(vecs,axis=1,keepdims=True)+1e-9
qi=np.random.choice(NUM,NQ,replace=False); queries=vecs[qi]
# exact ground truth (cosine == dot on unit vectors)
sims=queries@vecs.T
gt=np.argpartition(-sims,K,axis=1)[:,:K]
gt=[set(int(j) for j in np.argsort(-sims[r])[:K]) for r in range(NQ)]
def vec_bench(port, name):
    r=conn(port)
    try: r.execute_command('FT.DROPINDEX','vidx','DD')
    except Exception: pass
    try:
        r.execute_command('FT.CREATE','vidx','ON','HASH','PREFIX','1','v:','SCHEMA','vec','VECTOR','HNSW','6','TYPE','FLOAT32','DIM',str(DIM),'DISTANCE_METRIC','COSINE')
    except Exception as e:
        emit("VEC",arch,name,f"create_err={str(e)[:60]}"); return
    t0=time.time(); pipe=r.pipeline(transaction=False)
    for i in range(NUM):
        pipe.hset(f'v:{i}',mapping={'vec':vecs[i].tobytes()})
        if (i+1)%1000==0: pipe.execute(); pipe=r.pipeline(transaction=False)
    pipe.execute(); ins=NUM/(time.time()-t0)
    emit("VEC",arch,name,f"insert_rate={ins:.0f}/s")
    qb=[queries[i].tobytes() for i in range(NQ)]
    def parse_ids(res):
        ids=set()
        if isinstance(res,list):
            for el in res[1:]:
                if isinstance(el,bytes) and el.startswith(b'v:'):
                    try: ids.add(int(el.split(b':')[1]))
                    except Exception: pass
        return ids
    def measure(stage):
        # recall (single-conn) + concurrent QPS
        rec=0; got=0; ci=[0]
        for i in range(NQ):
            try:
                res=r.execute_command('FT.SEARCH','vidx',f'*=>[KNN {K} @vec $q]','PARAMS','2','q',qb[i],'RETURN','0','DIALECT','2')
                ids=parse_ids(res)
                if ids: got+=1; rec+=len(ids & gt[i])/K
            except Exception: pass
        recall=rec/NQ if NQ else 0
        def qfn(c, _ct=[0]):
            b=qb[_ct[0]%NQ]; _ct[0]+=1
            c.execute_command('FT.SEARCH','vidx',f'*=>[KNN {K} @vec $q]','PARAMS','2','q',b,'RETURN','0','DIALECT','2')
        qps,p50,p99,n=load(lambda:conn(port),qfn,DURATION=6.0,THREADS=8)
        emit("VEC",arch,name,f"{stage}|recall@10={recall:.4f}|qps={qps:.0f}|p50ms={p50:.2f}|p99ms={p99:.2f}|ret={got}/{NQ}")
    measure("brute")
    try:
        r.execute_command('FT.COMPACT','vidx'); time.sleep(6)
        measure("hnsw")
    except Exception as e: emit("VEC",arch,name,f"compact_err={str(e)[:60]}")
vec_bench(MOON,"moon")
if STACK_OK: vec_bench(STACK,"redisearch")
else: emit("VEC",arch,"redisearch","SKIPPED (no redis-stack)")

############################# FTS #############################
emit("FTS", arch, "=== Full-Text Search: 100K docs, BM25, Zipf vocab ===")
NDOC=envi("FTS_NDOC",100000); VOCAB=8000; NCAT=20
# Zipf vocabulary
zw=[f"term{i:05d}" for i in range(VOCAB)]
zp=np.array([1.0/(i+1) for i in range(VOCAB)]); zp/=zp.sum()
def make_doc():
    n=random.randint(20,80)
    idx=np.random.choice(VOCAB,n,p=zp)
    return " ".join(zw[j] for j in idx)
docs=[make_doc() for _ in range(NDOC)]
cats=[f"cat{random.randint(0,NCAT-1):02d}" for _ in range(NDOC)]
prices=[round(random.uniform(1,1000),2) for _ in range(NDOC)]
# query terms by document-frequency tier. NB: rank-0..~10 Zipf terms match ~ALL docs and hit the
# O(N) TF-lookup path (store.rs:504) -> O(M^2) at 100K docs -> minutes/query. We characterize bounded-DF
# tiers (hi=rank100 ~few-k docs, mid=rank500 ~hundreds, rare=rank6000 ~tens) to keep the run tractable;
# the rank-0 high-DF pathology is recorded separately from the smoke test + code review.
hi=[zw[i] for i in range(100,110)]; mid=[zw[i] for i in range(500,510)]; rare=[zw[i] for i in range(6000,6010)]
def fts_bench(port, name):
    r=conn(port)
    try: r.execute_command('FT.DROPINDEX','tidx','DD')
    except Exception: pass
    try:
        r.execute_command('FT.CREATE','tidx','ON','HASH','PREFIX','1','d:','SCHEMA','body','TEXT','category','TAG','price','NUMERIC')
    except Exception as e:
        emit("FTS",arch,name,f"create_err={str(e)[:60]}"); return
    t0=time.time(); pipe=r.pipeline(transaction=False)
    for i in range(NDOC):
        pipe.hset(f'd:{i}',mapping={'body':docs[i],'category':cats[i],'price':prices[i]})
        if (i+1)%2000==0: pipe.execute(); pipe=r.pipeline(transaction=False)
    pipe.execute(); ins=NDOC/(time.time()-t0)
    emit("FTS",arch,name,f"index_rate={ins:.0f}docs/s")
    def docc(res):
        return res[0] if isinstance(res,list) and res else 0
    suite=[
        ("term_hi",     lambda c,t=hi[0]:   c.execute_command('FT.SEARCH','tidx',t,'NOCONTENT','LIMIT','0','10')),
        ("term_mid",    lambda c,t=mid[0]:  c.execute_command('FT.SEARCH','tidx',t,'NOCONTENT','LIMIT','0','10')),
        ("term_rare",   lambda c,t=rare[0]: c.execute_command('FT.SEARCH','tidx',t,'NOCONTENT','LIMIT','0','10')),
        ("and2",        lambda c,a=mid[0],b=mid[1]: c.execute_command('FT.SEARCH','tidx',f'{a} {b}','NOCONTENT','LIMIT','0','10')),
        ("or2",         lambda c,a=mid[0],b=mid[1]: c.execute_command('FT.SEARCH','tidx',f'{a}|{b}','NOCONTENT','LIMIT','0','10')),
        ("tag",         lambda c: c.execute_command('FT.SEARCH','tidx','@category:{cat03}','NOCONTENT','LIMIT','0','10')),
        ("numeric",     lambda c: c.execute_command('FT.SEARCH','tidx','@price:[100 200]','NOCONTENT','LIMIT','0','10')),
        ("text_tag",    lambda c,t=hi[0]:   c.execute_command('FT.SEARCH','tidx',f'{t} @category:{{cat03}}','NOCONTENT','LIMIT','0','10')),
    ]
    # sanity doc counts (single shot)
    for nm,fn in suite:
        try: dc=docc(fn(r))
        except Exception: dc=-1
        qps,p50,p99,n=load(lambda:conn(port),fn,DURATION=5.0,THREADS=8)
        emit("FTS",arch,name,f"{nm}|qps={qps:.0f}|p50ms={p50:.2f}|p99ms={p99:.2f}|hits={dc}")
    # FT.AGGREGATE groupby category
    try:
        def agg(c): c.execute_command('FT.AGGREGATE','tidx','*','GROUPBY','1','@category','REDUCE','COUNT','0','AS','n')
        agg(r)
        qps,p50,p99,n=load(lambda:conn(port),agg,DURATION=4.0,THREADS=4)
        emit("FTS",arch,name,f"aggregate_groupby|qps={qps:.0f}|p50ms={p50:.2f}|p99ms={p99:.2f}")
    except Exception as e: emit("FTS",arch,name,f"aggregate_err={str(e)[:60]}")
fts_bench(MOON,"moon")
if STACK_OK: fts_bench(STACK,"redisearch")
else: emit("FTS",arch,"redisearch","SKIPPED (no redis-stack)")

############################# GRAPH #############################
emit("GRAPH", arch, "=== Graph: 5K nodes, 15K edges, 1-hop/2-hop ===")
NN=envi("GRAPH_NN",5000); NE=envi("GRAPH_NE",15000)
edges=[(random.randint(0,NN-1),random.randint(0,NN-1)) for _ in range(NE)]
def parse_nid(res):
    if isinstance(res,int): return res
    if isinstance(res,(bytes,str)):
        try: return int(res)
        except Exception: return None
    if isinstance(res,list) and res: return parse_nid(res[0])
    return None
# Moon uses native GRAPH.CREATE + GRAPH.ADDNODE/ADDEDGE; FalkorDB uses Cypher CREATE (auto-creates graph).
# Reads compared via the same Cypher MATCH on both; Moon additionally measured via its native GRAPH.NEIGHBORS.
def graph_bench(port, name, gname, engine):
    r=conn(port)
    try: r.execute_command('GRAPH.DELETE',gname)
    except Exception: pass
    t0=time.time(); nid=[]
    if engine=="moon":
        try: r.execute_command('GRAPH.CREATE',gname)
        except Exception: pass
        for i in range(NN):
            try: pid=parse_nid(r.execute_command('GRAPH.ADDNODE',gname,'N','id',str(i)))
            except Exception as e: emit("GRAPH",arch,name,f"addnode_err={str(e)[:60]}"); return
            nid.append(pid if pid is not None else i)
        for (a,b) in edges:
            try: r.execute_command('GRAPH.ADDEDGE',gname,nid[a],nid[b],'E')
            except Exception: pass
    else:
        try:
            B=500
            for s in range(0,NN,B):
                params="["+",".join(f"{{id:{i}}}" for i in range(s,min(s+B,NN)))+"]"
                r.execute_command('GRAPH.QUERY',gname,f"UNWIND {params} AS r CREATE (:N {{id:r.id}})")
            for s in range(0,NE,B):
                chunk=edges[s:s+B]
                params="["+",".join(f"[{a},{b}]" for a,b in chunk)+"]"
                r.execute_command('GRAPH.QUERY',gname,f"UNWIND {params} AS e MATCH (a:N {{id:e[0]}}),(b:N {{id:e[1]}}) CREATE (a)-[:E]->(b)")
        except Exception as e: emit("GRAPH",arch,name,f"build_err={str(e)[:70]}"); return
        nid=list(range(NN))
    emit("GRAPH",arch,name,f"build_rate={(NN+NE)/(time.time()-t0):.0f}ops/s")
    # Inline filter MUST be the node's `id` PROPERTY value (the loop index i, 0..NN-1), NOT nid[i]
    # (the GRAPH.ADDNODE internal handle / FFI key). Moon stores id=str(i); filtering on the handle
    # matches no node -> 0 rows once the v3-2 inline filter is active. FalkorDB's nid=range(N) so
    # i==property there anyway. (Fixed 2026-06-17 after the v3-2 re-baseline surfaced the bug.)
    def q1(c,_ct=[0]):
        i=_ct[0]%NN; _ct[0]+=1
        c.execute_command('GRAPH.QUERY',gname,f"MATCH (a:N {{id:{i}}})-[:E]->(b) RETURN b.id")
    def q2(c,_ct=[0]):
        i=_ct[0]%NN; _ct[0]+=1
        c.execute_command('GRAPH.QUERY',gname,f"MATCH (a:N {{id:{i}}})-[:E]->()-[:E]->(c) RETURN c.id")
    try:
        s=r.execute_command('GRAPH.QUERY',gname,f"MATCH (a:N {{id:0}})-[:E]->(b) RETURN b.id")
        rows=len(s[1]) if isinstance(s,list) and len(s)>1 and isinstance(s[1],list) else -1
    except Exception: rows=-1
    for nm,fn in [("cypher_1hop",q1),("cypher_2hop",q2)]:
        qps,p50,p99,n=load(lambda:conn(port),fn,DURATION=DUR,THREADS=8)
        emit("GRAPH",arch,name,f"{nm}|qps={qps:.0f}|p50ms={p50:.2f}|p99ms={p99:.2f}")
    emit("GRAPH",arch,name,f"cypher_match_rows={rows}")
    if engine=="moon":
        def qn(c,_ct=[0]):
            i=nid[_ct[0]%NN]; _ct[0]+=1
            c.execute_command('GRAPH.NEIGHBORS',gname,i)
        try:
            qps,p50,p99,n=load(lambda:conn(port),qn,DURATION=DUR,THREADS=8)
            emit("GRAPH",arch,name,f"native_neighbors_1hop|qps={qps:.0f}|p50ms={p50:.2f}|p99ms={p99:.2f}")
        except Exception as e: emit("GRAPH",arch,name,f"native_err={str(e)[:60]}")
graph_bench(MOON,"moon","g","moon")
if FALKOR_OK: graph_bench(FALKOR,"falkordb","gf","falkordb")
else: emit("GRAPH",arch,"falkordb","SKIPPED (no falkordb)")
PY

kill_moon
$DOCKER rm -f rstack falkor >/dev/null 2>&1 || true
echo "================ 4FEATURE BENCH DONE · arch=$ARCH · $(date -u) ================"
