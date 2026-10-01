#!/usr/bin/env bash
# Bench phase (minimal runtime image): end-to-end A/B of the previous vs the
# current engine under the same 2 CPU / 2 GiB cgroup, with no network.
#
# Both variants run the SAME driver and bench servers (staged from the current
# tree by the verify phase); only packages/beamsocket (SDK + musl addon) is
# swapped. Rounds alternate order (A,B then B,A) to cancel drift. A reference
# library (default `ws`) runs once per round as a control: its round-to-round
# spread is this environment's noise floor, reported next to every delta.
set -Euo pipefail

OUT=${RESULTS_DIR:-/results}
ROUNDS=${ROUNDS:-6}
REFERENCE_LIB=${REFERENCE_LIB:-ws}
DRIVER_ARGS=${DRIVER_ARGS:-}
RUN_TIMEOUT=${RUN_TIMEOUT:-900}
WORK=/tmp/bench

for v in baseline current; do
  if [ ! -f "$OUT/build/$v/beamsocket/native/beamsocket.node" ]; then
    echo "missing $v build in $OUT/build — run the verify phase first" >&2
    exit 2
  fi
done
[ -f "$OUT/build/bench/benchmarks/driver.mjs" ] || { echo "missing staged benchmarks" >&2; exit 2; }

rm -rf "$OUT/bench" && mkdir -p "$OUT/bench/raw" "$OUT/bench/logs"
rm -rf "$WORK" && mkdir -p "$WORK/packages"
cp -r "$OUT/build/bench/benchmarks" "$WORK/benchmarks"
ulimit -n 65536 2>/dev/null || true

{
  echo "phase=bench"
  echo "alpine=$(cat /etc/alpine-release)"
  echo "node=$(node --version)"
  echo "rounds=$ROUNDS reference_lib=${REFERENCE_LIB:-none} driver_args=${DRIVER_ARGS:-default}"
  echo "cpus_visible=$(nproc)"
  echo "cpuset=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || echo n/a)"
  echo "cpu_quota=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo n/a)"
  echo "memory_max=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo n/a)"
  echo "swap_max=$(cat /sys/fs/cgroup/memory.swap.max 2>/dev/null || echo n/a)"
  echo "nofile=$(ulimit -n)"
  echo "port_range=$(cat /proc/sys/net/ipv4/ip_local_port_range | tr '\t' ' ')"
  echo "tcp_tw_reuse=$(cat /proc/sys/net/ipv4/tcp_tw_reuse)"
} | tee "$OUT/bench/environment.txt"

failures=0
# run_one <name> <lib>: one full driver run (throughput, 16 KiB, latency,
# fan-out, idle memory). For beamsocket, <name> selects the staged engine.
run_one() {
  local name=$1 lib=$2 round=$3
  if [ "$lib" = beamsocket ]; then
    ln -sfn "$OUT/build/$name/beamsocket" "$WORK/packages/beamsocket"
  fi
  local tag="$name-r$round"
  local start=$SECONDS
  # shellcheck disable=SC2086 # DRIVER_ARGS is intentionally word-split
  if (cd "$WORK/benchmarks" && timeout "$RUN_TIMEOUT" node driver.mjs --lib "$lib" $DRIVER_ARGS \
        --out "$OUT/bench/raw/$tag.json" >/dev/null 2>"$OUT/bench/logs/$tag.err"); then
    echo "round $round  $name: ok ($((SECONDS - start))s)"
  else
    echo "round $round  $name: FAILED ($((SECONDS - start))s) — $(tail -n 3 "$OUT/bench/logs/$tag.err" | tr '\n' ' ')"
    rm -f "$OUT/bench/raw/$tag.json"
    failures=$((failures + 1))
  fi
  sleep 2 # let the previous server's sockets drain before the next run
}

for round in $(seq 1 "$ROUNDS"); do
  if [ $((round % 2)) -eq 1 ]; then order="baseline current"; else order="current baseline"; fi
  for v in $order; do run_one "$v" beamsocket "$round"; done
  if [ -n "$REFERENCE_LIB" ]; then run_one "reference" "$REFERENCE_LIB" "$round"; fi
done

{
  echo "memory_peak=$(cat /sys/fs/cgroup/memory.peak 2>/dev/null || echo n/a)"
  grep -E '^(oom|oom_kill|max) ' /sys/fs/cgroup/memory.events 2>/dev/null | tr '\n' ' '
  echo
  echo "failed_runs=$failures"
} | tee -a "$OUT/bench/environment.txt"

node /usr/local/lib/beamsocket/evaluate.mjs bench "$OUT"
status=$?

if [ -n "${HOST_UID:-}" ]; then chown -R "$HOST_UID:${HOST_GID:-$HOST_UID}" "$OUT"; fi
[ "$failures" -eq 0 ] || status=1
exit "$status"
