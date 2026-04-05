#!/usr/bin/env bash
set -uo pipefail
cd /Users/tindang/workspaces/tind-repo/moon
OUT=/tmp/bench-nopersist.txt
: > "$OUT"

ps aux | grep -E "moon |redis-bench" | grep -v grep | awk '{print $2}' | xargs kill -9 2>/dev/null
sleep 2

redis-server --port 7000 --bind 127.0.0.1 --protected-mode no --save "" --appendonly no --daemonize yes --loglevel warning
sleep 1

cat >> "$OUT" <<HDR
# Moon (no persistence) vs Redis (no persistence)
# Pure RAM, no AOF, no WAL, no disk-offload
# Date: $(date)
# Platform: $(uname -srm)

HDR

extract_moon() {
    local cmd=$1 C=$2 N=$3 P=$4
    killall -9 moon redis-benchmark 2>/dev/null
    sleep 1
    ./target/release/moon --port 7001 --shards 1 &>/dev/null &
    sleep 1
    local raw=$(timeout 8 redis-benchmark -p 7001 -c $C -n $N -t $cmd -P $P 2>&1)
    echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "rps=0.0" | grep -v "nan" | head -1 | awk -F'overall: ' '{print $2}' | awk -F')' '{print $1}'
}

redis_csv() {
    redis-benchmark -p 7000 -c $1 -n $2 -t $3 -P $4 --csv 2>&1 | grep -v '^"test"' | head -1 | cut -d'"' -f4
}

for sect in "p=1|1|50|50000" "p=16|16|50|200000" "p=64|64|100|500000"; do
    IFS='|' read -r title P C N <<< "$sect"
    [ "$P" = "64" ] && cmds="get set" || cmds="get set incr lpush rpush lpop rpop sadd spop hset zadd"

    echo "## $title (c=$C, n=$N)" >> "$OUT"
    echo "| Command | Redis | Moon | Ratio |" >> "$OUT"
    echo "|---------|------:|-----:|------:|" >> "$OUT"

    for cmd in $cmds; do
        R=$(redis_csv "$C" "$N" "$cmd" "$P")
        M=$(extract_moon "$cmd" "$C" "$N" "$P")
        RATIO="--"
        if [ -n "$R" ] && [ -n "$M" ]; then
            RATIO=$(python3 -c "print(f'{$M/$R:.2f}')" 2>/dev/null || echo "--")
        fi
        CMD=$(echo $cmd | tr a-z A-Z)
        printf "| %-7s | %s | %s | %sx |\n" "$CMD" "${R:---}" "${M:---}" "$RATIO" >> "$OUT"
    done
    echo "" >> "$OUT"
done

echo "## CLOSE_WAIT" >> "$OUT"
echo "Moon: $(ss -tnp 2>/dev/null | grep 7001 | grep -c CLOSE_WAIT || echo 0)" >> "$OUT"
echo "Redis: $(ss -tnp 2>/dev/null | grep 7000 | grep -c CLOSE_WAIT || echo 0)" >> "$OUT"

redis-cli -p 7000 SHUTDOWN NOSAVE 2>/dev/null
killall -9 moon 2>/dev/null
echo "DONE" >> "$OUT"
