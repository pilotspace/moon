# shellcheck shell=bash
###############################################################################
# harness-guard.sh -- shared by scripts/test-consistency.sh and
# scripts/test-commands.sh (moon#1276). Sourced, never executed.
#
# Three things both oracle harnesses got wrong, fixed once here:
#
# 1. `redis-cli -t <secs>` exists only in redis-cli 7.2+. On 7.0.x it prints
#    "Unrecognized option or bad number of args for: '-t'" and exits 1, and
#    under `set -euo pipefail` a `x=$(redis-cli -t 3 ...)` then ended the run
#    SILENTLY mid-section. `cli_bounded <secs> ...` probes once and falls back to
#    `timeout`/`gtimeout` (a whole-command bound -- strictly stronger than
#    `-t`), or to no bound at all with a loud warning.
# 2. An auxiliary moon (eviction, SLOWLOG, cross-shard, SHUTDOWN, NUMERIC-07
#    legs) was stopped only at the end of its own leg, and the EXIT trap knew
#    only the main pair. A death mid-leg leaked it; moon binds with
#    SO_REUSEPORT, so the NEXT run's server on the same port bound
#    successfully and the kernel split connections between the two
#    (`evicted_keys=2391 + DBSIZE=0 != 6000`). `aux_start` records every PID
#    for `aux_kill_all` (call it from the EXIT trap) and refuses a port that
#    already accepts connections.
# 3. A `set -e` death printed nothing. `harness_install_err_trap` names the
#    line, the exit status and the command, so a truncated run is never
#    mistaken for a finished one, and turns SIGINT/SIGTERM/SIGHUP into an
#    `exit` so the EXIT trap's cleanup runs for those too.
# 4. The main-server sweep `pkill -f "moon.*<port>"` matched the harness's
#    own command line (`.../moon/scripts/test-consistency.sh --port-rust N`)
#    and the shell that launched it, and missed a MOON_BIN not named `moon`.
#    `kill_port_servers <binary> <port>` matches `^<binary> --port <port>`.
#
# The EXIT trap must start with `trap - ERR` (the run is over; its own
# non-zero return is not a death) and call `aux_kill_all`.
#
# Bash 3.2 compatible (macOS /bin/bash): no associative arrays, no `mapfile`,
# and empty arrays are expanded with the `${a[@]+"${a[@]}"}` idiom (bash < 4.4
# treats `"${a[@]}"` of an empty array as unbound under `set -u`).
###############################################################################

# native | timeout | none -- how `cli_bounded` bounds a redis-cli call.
REDIS_CLI_TIMEOUT_MODE=""
# `timeout` or `gtimeout` (and whether it takes `-k`) when the mode is timeout.
REDIS_CLI_TIMEOUT_BIN=""
REDIS_CLI_TIMEOUT_KILL=false

# Probe redis-cli for `-t` BY BEHAVIOUR, not by version string (distro
# builds backport options): ask for `-t 1` against a port nothing listens on.
# A redis-cli that knows the flag fails to CONNECT; one that does not rejects
# the ARGUMENT. Neither touches a server.
harness_probe_redis_cli() {
    local out bin
    out=$(redis-cli -t 1 -p 1 PING 2>&1 || true)
    case "$out" in
        *[Uu]nrecognized\ option*)
            ;;
        *)
            REDIS_CLI_TIMEOUT_MODE=native
            return 0
            ;;
    esac
    for bin in timeout gtimeout; do
        if command -v "$bin" >/dev/null 2>&1 && "$bin" 5 true >/dev/null 2>&1; then
            REDIS_CLI_TIMEOUT_MODE=timeout
            REDIS_CLI_TIMEOUT_BIN="$bin"
            if "$bin" -k 1 5 true >/dev/null 2>&1; then
                REDIS_CLI_TIMEOUT_KILL=true
            fi
            echo "NOTE: $(redis-cli --version 2>/dev/null) has no '-t' (added in 7.2);" \
                "bounding redis-cli calls with '$bin' instead." >&2
            return 0
        fi
    done
    REDIS_CLI_TIMEOUT_MODE=none
    {
        echo "WARNING: ****************************************************************"
        echo "WARNING: $(redis-cli --version 2>/dev/null) has no '-t' (added in 7.2), and neither"
        echo "WARNING: 'timeout' (GNU coreutils) nor 'gtimeout' (brew install coreutils) is on"
        echo "WARNING: PATH. redis-cli calls run UNBOUNDED: a server that stops answering (the"
        echo "WARNING: liveness rows exist to catch exactly that) HANGS this run instead of"
        echo "WARNING: failing it. Install redis 7.2+ or coreutils."
        echo "WARNING: ****************************************************************"
    } >&2
}

# cli_bounded <seconds> <redis-cli args...> -- redis-cli with a bound on how long it
# may take. Exit status is redis-cli's (124 when `timeout` fired). Callers
# that capture output add `|| true`, so a hung server records a FAIL row
# instead of ending the run.
cli_bounded() {
    local secs="$1"
    shift
    if [[ -z "$REDIS_CLI_TIMEOUT_MODE" ]]; then
        # Fallback only: callers probe ONCE at startup (to see its notes). A
        # lazy probe here may run inside `$(... 2>&1)`, where its note would
        # land in the captured reply, so it is silent.
        harness_probe_redis_cli >/dev/null 2>&1
    fi
    case "$REDIS_CLI_TIMEOUT_MODE" in
        native) redis-cli -t "$secs" "$@" ;;
        timeout)
            if [[ "$REDIS_CLI_TIMEOUT_KILL" == true ]]; then
                "$REDIS_CLI_TIMEOUT_BIN" -k 2 "$secs" redis-cli "$@"
            else
                "$REDIS_CLI_TIMEOUT_BIN" "$secs" redis-cli "$@"
            fi
            ;;
        *) redis-cli "$@" ;;
    esac
}

# Print the oracle's version and, below 7.2, the known expected-value diffs
# (moon follows redis 7.2+/8.x). Informational: the run continues.
harness_note_oracle_version() {
    local v major minor
    v=$(redis-server --version 2>/dev/null | sed -n 's/.*v=\([0-9][0-9.]*\).*/\1/p')
    echo "Oracle: redis-server ${v:-<unknown>}, $(redis-cli --version 2>/dev/null || echo 'redis-cli <missing>')" >&2
    major=${v%%.*}
    minor=${v#*.}
    minor=${minor%%.*}
    if [[ "$major" =~ ^[0-9]+$ ]] && [[ "$minor" =~ ^[0-9]+$ ]] &&
        { ((major < 7)) || { ((major == 7)) && ((minor < 2)); }; }; then
        {
            echo "WARNING: the expected values in this script assume a redis 7.2+/8.x oracle."
            echo "WARNING: Against redis $v about 80 rows differ for version reasons, not moon"
            echo "WARNING: bugs: listpack set/list encodings, ZRANK ... WITHSCORE, the NOPERM"
            echo "WARNING: 'User <name> has no permissions' text, the COMMAND GETKEYS keyless"
            echo "WARNING: text, hash-field-expiry tracking, DEBUG DIGEST values, and the flaky"
            echo "WARNING: moon#981 '-@all +get +set' ACL GETUSER row (7.0 renders per-command"
            echo "WARNING: rules in randomized dict order). See the header of this script."
        } >&2
    fi
}

# ---- auxiliary servers ------------------------------------------------------

AUX_PIDS=()
AUX_LAST_PID=""

# True when something already accepts connections on 127.0.0.1:<port>. A
# plain connect, not PING: a HUNG leaked server accepts (the kernel backlog
# does) but never answers, and it poisons a SO_REUSEPORT bind all the same.
aux_port_busy() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

# aux_start <port> <logfile> <command...>
# Start an auxiliary server in the background, remember its PID in AUX_PIDS
# (for `aux_kill_all`) and in AUX_LAST_PID, and wait up to 5 s for PONG.
# Returns 1 WITHOUT starting anything when the port is already taken, and 2
# when the server never answered (it is still tracked and gets killed).
aux_start() {
    local port="$1" logfile="$2"
    shift 2
    if aux_port_busy "$port"; then
        echo "  ERROR: port $port already accepts connections -- a server leaked by an" \
            "earlier run? ('pgrep -af -- \"--port $port\"'). Not starting a second one:" \
            "moon binds with SO_REUSEPORT, so both would bind and the kernel would split" \
            "this leg's connections between them." >&2
        AUX_LAST_PID=""
        return 1
    fi
    "$@" >"$logfile" 2>&1 &
    AUX_LAST_PID=$!
    AUX_PIDS+=("$AUX_LAST_PID")
    local _i reply
    for _i in $(seq 1 50); do
        reply=$(cli_bounded 1 -p "$port" PING 2>/dev/null || true)
        if [[ "$reply" == *PONG* ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "  ERROR: auxiliary server on port $port (pid $AUX_LAST_PID) never answered PING" >&2
    return 2
}

# aux_stop <pid> -- stop one auxiliary server and forget it.
aux_stop() {
    local pid="$1"
    [[ -n "$pid" ]] || return 0
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    aux_forget "$pid"
}

# aux_forget <pid> -- drop a PID the caller already reaped itself (a server
# that exited on SHUTDOWN): the trap must never signal a recycled PID.
aux_forget() {
    local pid="$1" p
    local keep=()
    for p in ${AUX_PIDS[@]+"${AUX_PIDS[@]}"}; do
        [[ "$p" == "$pid" ]] || keep+=("$p")
    done
    AUX_PIDS=(${keep[@]+"${keep[@]}"})
}

# aux_kill_all -- the EXIT trap's half: TERM every tracked auxiliary server,
# give it a moment, then KILL what is left. By exact PID, never by name:
# `MOON_BIN` can be any file name, and a name pattern also matches servers
# this run does not own.
aux_kill_all() {
    local pid alive=()
    for pid in ${AUX_PIDS[@]+"${AUX_PIDS[@]}"}; do
        if kill "$pid" 2>/dev/null; then
            alive+=("$pid")
        fi
    done
    if ((${#alive[@]} > 0)); then
        local _i
        for _i in 1 2 3 4 5 6 7 8 9 10; do
            local still=false
            for pid in "${alive[@]}"; do
                kill -0 "$pid" 2>/dev/null && still=true
            done
            [[ "$still" == true ]] || break
            sleep 0.2
        done
        for pid in "${alive[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                echo "  cleanup: auxiliary server pid $pid ignored SIGTERM; sending SIGKILL" >&2
                kill -9 "$pid" 2>/dev/null || true
            fi
            wait "$pid" 2>/dev/null || true
        done
        echo "  cleanup: stopped ${#alive[@]} auxiliary server(s) left running by an interrupted leg" >&2
    fi
    AUX_PIDS=()
}

# kill_port_servers <binary> <port> -- best-effort sweep for a server of THIS
# run, started as `<binary> --port <port> ...`, whose PID a restart lost. The
# match is anchored on that exact argv prefix. The old `pkill -f "moon.*<port>"`
# matched ANY command line that merely mentions "moon" and the port -- the
# harness itself when run as `.../moon/scripts/test-consistency.sh --port-rust
# 6400`, or the shell that launched it with `PORT_RUST=6400` -- and killed it
# mid-run, with no summary (moon#1276).
kill_port_servers() {
    local re
    re=$(printf '%s' "$1" | sed 's/[][\.^$*+?(){}|]/\\&/g')
    pkill -f -- "^${re} --port $2( |\$)" 2>/dev/null || true
}

# ---- loud death -------------------------------------------------------------

# harness_install_err_trap <script name>
# `set -E` so the trap also fires inside functions (every leg is one). Inside
# a command substitution the failure is reported by the PARENT, at the line
# of the assignment, so the subshell stays quiet.
#
# Also turns SIGINT/SIGTERM/SIGHUP (Ctrl-C, a CI job timeout, a closed
# terminal) into an ordinary `exit`, so the EXIT trap -- and with it
# `aux_kill_all` -- runs then too: bash skips the EXIT trap when a signal's
# default action kills it.
harness_install_err_trap() {
    HARNESS_NAME="$1"
    set -E
    trap 'harness_on_err "$?" "$LINENO" "$BASH_COMMAND"' ERR
    trap 'echo "" >&2; echo "ABORTED: $HARNESS_NAME got SIGINT" >&2; exit 130' INT
    trap 'echo "" >&2; echo "ABORTED: $HARNESS_NAME got SIGTERM" >&2; exit 143' TERM
    trap 'exit 129' HUP
}

harness_on_err() {
    local rc="$1" line="$2" cmd="$3"
    if [[ "${BASH_SUBSHELL:-0}" -gt 0 ]]; then
        return 0
    fi
    {
        echo ""
        echo "FATAL: $HARNESS_NAME died at line $line (exit $rc) under 'set -e' while running:"
        echo "FATAL:   $cmd"
        echo "FATAL: every section after this point was SKIPPED -- this is not a finished run"
        echo "FATAL: (PASS=${PASS:-?} FAIL=${FAIL:-?} so far)."
    } >&2
}
