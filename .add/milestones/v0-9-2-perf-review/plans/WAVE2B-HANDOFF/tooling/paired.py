#!/usr/bin/env python3
# Paired per-rep ratios: within each (file, rep, cell), arm/baseline.
import csv, sys, statistics as st
from collections import defaultdict
groups = defaultdict(dict)
for f in sys.argv[1:]:
    for r in csv.DictReader(open(f)):
        if not r.get('rps'): continue
        groups[(f, r['rep'], r['shards'], r['P'], r['c'])][r['arm']] = r
cells = defaultdict(lambda: defaultdict(list))
for (f, rep, s, P, c), arms in groups.items():
    base = next((a for a in arms if a.startswith('w2a')), None)
    off = next((a for a in arms if a.endswith('@0')), None)
    on = next((a for a in arms if a.startswith('i2b') and '@' not in a), None)
    if not (base and off and on): continue
    for name, a, b in (('1A/base', on, base), ('1A/off', on, off), ('off/base', off, base)):
        cells[(s, P, c)][name].append(float(arms[a]['rps']) / float(arms[b]['rps']))
        cells[(s, P, c)][name + ' p99'].append(float(arms[a]['p99_ms']) / float(arms[b]['p99_ms']))
        cells[(s, P, c)][name + ' shardcpu'].append(float(arms[a].get('shard_us_per_op') or 'nan') / float(arms[b].get('shard_us_per_op') or 'nan') if arms[a].get('shard_us_per_op') else float('nan'))
        cells[(s, P, c)][name + ' cpu'].append(float(arms[a]['server_core_us_per_op']) / float(arms[b]['server_core_us_per_op']))
for key in sorted(cells, key=lambda k: (int(k[0]), -int(k[1]), -int(k[2]))):
    print(f"s{key[0]} P{key[1]} c{key[2]}")
    for name in ('1A/base', '1A/off', 'off/base'):
        r = cells[key][name]; p = cells[key][name + ' p99']; c = cells[key][name + ' cpu']
        sc = [x for x in cells[key][name + ' shardcpu'] if x == x]
        print(f"  {name:9s} n={len(r):2d} rps median {(st.median(r)-1)*100:+6.1f}%  [{(min(r)-1)*100:+.0f}% .. {(max(r)-1)*100:+.0f}%]  p99 median {(st.median(p)-1)*100:+5.0f}%  cpu/op {(st.median(c)-1)*100:+5.1f}%" + (f"  shard-cpu/op {(st.median(sc)-1)*100:+5.1f}% (n={len(sc)})" if sc else ""))
