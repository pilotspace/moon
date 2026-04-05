#!/usr/bin/env bash
set -uo pipefail
cd /Users/tindang/workspaces/tind-repo/moon
OUT=/tmp/bench-clean.txt
: > "$OUT"

restart_moon() {
    pkill -9 moon 2>/dev/null || true
    pkill -9 redis-benchmark 2>/dev/null || true
    sleep 1
    ./target/release/moon --port 6400 --shards 1 &>/dev/null &
    sleep 2
}

bench() {
    local label="$1" port="$2" cmd="$3" pipeline="$4" clients="$5" n="$6"
    if [ "$port" = "6400" ]; then
        # Moon: use timeout + extract first burst
        local raw=$(timeout 6 redis-benchmark -p 6400 -c "$clients" -n "$n" -t "$cmd" -P "$pipeline" 2>&1)
        local rps=$(echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "rps=0.0" | grep -v "nan" | head -1 | grep -oP 'overall: \K[0-9.]+')
        local lat=$(echo "$raw" | tr '\r' '\n' | grep "rps=" | grep -v "nan" | grep -v "rps=0.0" | head -1 | grep -oP 'avg_msec=\K[0-9.]+' | tail -1)
        echo "${rps:---}|${lat:---}"
    else
        # Redis: CSV mode works cleanly
        local raw=$(redis-benchmark -p 6399 -c "$clients" -n "$n" -t "$cmd" -P "$pipeline" --csv 2>&1 | grep -v '^"test"' | head -1)
        local rps=$(echo "$raw" | cut -d'"' -f4)
        local lat=$(echo "$raw" | cut -d'"' -f10)  # p50
        echo "${rps:---}|${lat:---}"
    fi
}

echo "# Moon vs Redis 8.0.2 — All Commands ($(date))" >> "$OUT"
echo "" >> "$OUT"

for section in "p=1|1|50|100000" "p=16|16|50|200000" "p=64|64|100|1000000"; do
    IFS='|' read -r title pipeline clients n <<< "$section"

    if [ "$pipeline" = "64" ]; then
        cmds="get set"
    else
        cmds="get set incr lpush rpush lpop rpop sadd spop hset zadd"
    fi

    echo "## $title (c=$clients, n=$n)" >> "$OUT"
    echo "" >> "$OUT"
    echo "| Command | Redis | Moon | Ratio | Redis p50 | Moon avg |" >> "$OUT"
    echo "|---------|------:|-----:|------:|----------:|---------:|" >> "$OUT"

    for cmd in $cmds; do
        # Redis first (doesn't hang)
        r=$(bench "Redis" 6399 "$cmd" "$pipeline" "$clients" "$n")
        r_rps=$(echo "$r" | cut -d'|' -f1)
        r_lat=$(echo "$r" | cut -d'|' -f2)

        # Restart Moon fresh for each command to avoid connection pool issues
        restart_moon

        m=$(bench "Moon" 6400 "$cmd" "$pipeline" "$clients" "$n")
        m_rps=$(echo "$m" | cut -d'|' -f1)
        m_lat=$(echo "$m" | cut -d'|' -f2)

        ratio="--"
        if [ "$m_rps" != "--" ] && [ "$r_rps" != "--" ]; then
            ratio=$(echo "scale=2; $m_rps / $r_rps" | bc 2>/dev/null || echo "--")
        fi

        CMD_UP=$(echo "$cmd" | tr 'a-z' 'A-Z')
        printf "| %-7s | %s | %s | %sx | %sms | %sms |\n" \
            "$CMD_UP" "$r_rps" "$m_rps" "$ratio" "$r_lat" "$m_lat" >> "$OUT"
    done
    echo "" >> "$OUT"
done

# Cleanup
pkill -9 moon 2>/dev/null || true
echo "=== DONE ===" >> "$OUT"
cat "$OUT"
