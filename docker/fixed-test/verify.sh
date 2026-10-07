#!/usr/bin/env bash
# Verify phase (toolchain image): run the correctness matrix for the previous
# and the current tree under the same 2 CPU / 2 GiB cgroup, then stage what
# the bench phase needs. Every step is recorded (status, seconds, log) and the
# run continues past failures so the evaluation shows the whole matrix; the
# exit status is non-zero only if the CURRENT tree fails a step.
set -Euo pipefail

ROOT=/workspace
OUT=${RESULTS_DIR:-/results}
BASELINE_REF=${BASELINE_REF:-d961332}
BASELINE_DIR=/tmp/beamsocket-baseline
# Optional: compare two commits instead of <ref> vs the working tree.
CURRENT_REF=${CURRENT_REF:-}
CURRENT_DIR=$ROOT
CACHE=${CACHE_DIR:-/cache}
STATUS="$OUT/verify/status.tsv"

# The public runner supplies the checkout revision used for docker build.
# Reject a stale or mistagged image before testing or replacing any results.
if [ -n "${EXPECTED_BUILD_REV:-}" ]; then
  image_revision=$(cat /image-build-revision 2>/dev/null || true)
  checkout_revision=$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)
  if [ "$image_revision" != "$EXPECTED_BUILD_REV" ] || [ "$checkout_revision" != "$EXPECTED_BUILD_REV" ]; then
    echo "source revision mismatch: expected=$EXPECTED_BUILD_REV image=${image_revision:-missing} checkout=${checkout_revision:-missing}" >&2
    exit 2
  fi
fi

mkdir -p "$OUT/verify/logs" "$OUT/build" "$OUT/micro" "$CACHE"
printf 'tree\tstep\tstatus\tseconds\tlog\n' > "$STATUS"

record_environment() {
  {
    echo "phase=verify"
    echo "image_build_revision=$(cat /image-build-revision 2>/dev/null || echo unknown)"
    echo "image_rust=${RUST_VERSION:-unknown}"
    echo "alpine=$(cat /etc/alpine-release)"
    echo "baseline_ref=$BASELINE_REF ($(git -C "$ROOT" rev-parse --short "$BASELINE_REF^{commit}" 2>/dev/null || echo missing))"
    if [ -n "$CURRENT_REF" ]; then
      echo "current_ref=$CURRENT_REF ($(git -C "$ROOT" rev-parse --short "$CURRENT_REF^{commit}" 2>/dev/null || echo missing))"
    else
      echo "current_ref=working tree at $(git -C "$ROOT" rev-parse --short HEAD)$(git -C "$ROOT" diff --quiet HEAD -- . 2>/dev/null || echo '+uncommitted')"
    fi
    echo "node=$(node --version)"
    echo "npm=$(npm --version)"
    echo "rust=$(rustc --version)"
    echo "cpus_visible=$(nproc)"
    echo "cpuset=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || echo n/a)"
    echo "cpu_quota=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo n/a)"
    echo "memory_max=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo n/a)"
    echo "swap_max=$(cat /sys/fs/cgroup/memory.swap.max 2>/dev/null || echo n/a)"
  } | tee "$OUT/verify/environment.txt"
}

current_failed=0
# step <tree> <name> <command...>: run in the tree's directory, log, record.
step() {
  local tree=$1 name=$2
  shift 2
  local log="$OUT/verify/logs/$tree-$name.log"
  local start=$SECONDS status
  printf '\n[%s] %s: %s\n' "$tree" "$name" "$*"
  "$@" >"$log" 2>&1
  status=$?
  local secs=$((SECONDS - start))
  if [ "$status" -eq 0 ]; then
    printf '[%s] %s: pass (%ss)\n' "$tree" "$name" "$secs"
    printf '%s\t%s\tpass\t%s\t%s\n' "$tree" "$name" "$secs" "logs/$tree-$name.log" >> "$STATUS"
  else
    printf '[%s] %s: FAIL (exit %s, %ss) — tail of %s:\n' "$tree" "$name" "$status" "$secs" "$log"
    tail -n 25 "$log"
    printf '%s\t%s\tfail\t%s\t%s\n' "$tree" "$name" "$secs" "logs/$tree-$name.log" >> "$STATUS"
    [ "$tree" = current ] && current_failed=1
  fi
  return "$status"
}

run_suite() {
  local tree=$1 dir=$2 cache_key=$3
  # Separate target dirs per tree: one tree's artifacts must never satisfy
  # (or hide a failure in) the other. Kept in the cache volume across runs.
  export CARGO_TARGET_DIR="$CACHE/target-$cache_key"
  export npm_config_cache="$CACHE/npm"
  cd "$dir" || return 1
  echo "===== $tree ($dir) ====="

  step "$tree" fmt cargo fmt --all --check
  step "$tree" clippy cargo clippy --workspace --all-targets -- -D warnings
  step "$tree" clippy-napi cargo clippy -p beamsocket-node --features napi --all-targets -- -D warnings
  step "$tree" cargo-test cargo test --workspace --no-fail-fast
  if [ -f spike/Cargo.toml ]; then
    step "$tree" spike-test cargo test --manifest-path spike/Cargo.toml --workspace
  fi
  step "$tree" npm-ci npm ci --workspaces --include-workspace-root || return 0
  step "$tree" build-native npm run build:native -w beamsocket || return 0
  step "$tree" build-sdk npm run build -w beamsocket || return 0
  step "$tree" js-test npm test -w beamsocket

  # Stage this tree's SDK + musl addon for the bench phase.
  local stage="$OUT/build/$tree/beamsocket"
  rm -rf "$stage" && mkdir -p "$stage"
  cp -r packages/beamsocket/package.json packages/beamsocket/dist packages/beamsocket/native "$stage/"
}

record_environment

if ! git -C "$ROOT" cat-file -e "$BASELINE_REF^{commit}" 2>/dev/null; then
  echo "baseline ref is unavailable: $BASELINE_REF (use a full clone)" >&2
  exit 2
fi
rm -rf "$BASELINE_DIR" && mkdir -p "$BASELINE_DIR"
git -C "$ROOT" archive "$BASELINE_REF" | tar -x -C "$BASELINE_DIR"

# The previous tree's cache is keyed by commit SHA. `git archive` stamps every
# file with the commit time, which is older than any cached artifact, so a
# shared directory would make Cargo's mtime check reuse ANOTHER commit's build
# (observed: a cc1d1ba run silently linked d961332's core). Per-SHA reuse is
# safe because the sources are identical. The current tree keeps one directory:
# its files carry real edit times.
baseline_sha=$(git -C "$ROOT" rev-parse "$BASELINE_REF^{commit}")
run_suite baseline "$BASELINE_DIR" "ref-${baseline_sha:0:12}"
if [ -n "$CURRENT_REF" ]; then
  git -C "$ROOT" cat-file -e "$CURRENT_REF^{commit}" 2>/dev/null ||
    { echo "current ref is unavailable: $CURRENT_REF" >&2; exit 2; }
  current_sha=$(git -C "$ROOT" rev-parse "$CURRENT_REF^{commit}")
  CURRENT_DIR=/tmp/beamsocket-current
  rm -rf "$CURRENT_DIR" && mkdir -p "$CURRENT_DIR"
  git -C "$ROOT" archive "$CURRENT_REF" | tar -x -C "$CURRENT_DIR"
  run_suite current "$CURRENT_DIR" "ref-${current_sha:0:12}"
else
  run_suite current "$ROOT" worktree
fi

# The benchmark harness always comes from the CURRENT tree for both variants:
# an engine A/B must not also change the driver (the previous driver's echo
# accounting and bench-server send budget were invalid; see benchmarks/README).
cd "$ROOT"
export npm_config_cache="$CACHE/npm"
step current bench-deps npm ci --prefix benchmarks
rm -rf "$OUT/build/bench" && mkdir -p "$OUT/build/bench/benchmarks"
cp -r benchmarks/driver.mjs benchmarks/servers benchmarks/package.json benchmarks/node_modules \
  "$OUT/build/bench/benchmarks/"

# Core micro A/B: the current tree's bench embeds the frozen previous fan-out
# implementation and runs both in one process (same registries, same cgroup).
# Skipped when the current tree predates the bench.
cd "$CURRENT_DIR"
if [ -f crates/core/benches/fanout_exclusions.rs ]; then
  if [ -n "$CURRENT_REF" ]; then export CARGO_TARGET_DIR="$CACHE/target-ref-${current_sha:0:12}"
  else export CARGO_TARGET_DIR="$CACHE/target-worktree"; fi
  step current micro-bench sh -c \
    "cargo bench -p beamsocket-core --bench fanout_exclusions > '$OUT/micro/fanout-exclusions.csv'"
fi

node /usr/local/lib/beamsocket/evaluate.mjs verify "$OUT" || current_failed=1

# Results are written as root inside the container; hand them back.
if [ -n "${HOST_UID:-}" ]; then chown -R "$HOST_UID:${HOST_GID:-$HOST_UID}" "$OUT"; fi
exit "$current_failed"
