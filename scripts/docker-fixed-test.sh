#!/usr/bin/env bash
# Reproducible previous-vs-current validation in Alpine containers with a fixed
# resource budget: 2 CPUs (pinned cores) and 2 GiB RAM with swap disabled.
#
#   ./scripts/docker-fixed-test.sh                 # verify + bench
#   PHASES=verify ./scripts/docker-fixed-test.sh   # correctness + micro A/B only
#   PHASES=bench OUTPUT_DIR=.artifacts/<run> ./scripts/docker-fixed-test.sh
#
# Environment (all optional):
#   BASELINE_REF   previous version to compare against      (default d961332)
#   CURRENT_REF    compare this commit instead of the working tree (optional)
#   CPUSET         host CPUs to pin to, exactly 2            (default: auto)
#   ROUNDS         end-to-end A/B rounds                     (default 6)
#   REFERENCE_LIB  control library run each round, or ""     (default ws)
#   DRIVER_ARGS    extra benchmarks/driver.mjs arguments      (default none)
#   OUTPUT_DIR     results directory           (default .artifacts/fixed-test-<utc>)
#   NO_CACHE=1     don't reuse the cargo/npm cache volume between runs
set -Eeuo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
image=${IMAGE:-beamsocket-fixed-test}
baseline_ref=${BASELINE_REF:-d961332}
phases=${PHASES:-verify bench}
output_dir=${OUTPUT_DIR:-"$repo_root/.artifacts/fixed-test-$(date -u +%Y%m%dT%H%M%SZ)"}
cache_volume=${CACHE_VOLUME:-beamsocket-fixed-test-cache}

# The fixed budget. Changing these changes what the numbers mean; keep them
# identical between any two runs you intend to compare.
CPUS=2
MEMORY=2g

die() { echo "error: $*" >&2; exit 2; }
command -v docker >/dev/null 2>&1 || die "docker is required"
docker info >/dev/null 2>&1 || die "the Docker daemon is unavailable"
git -C "$repo_root" cat-file -e "$baseline_ref^{commit}" 2>/dev/null ||
  die "baseline ref $baseline_ref is not in this checkout (use a full clone or set BASELINE_REF)"
if [ -n "${CURRENT_REF:-}" ]; then
  git -C "$repo_root" cat-file -e "$CURRENT_REF^{commit}" 2>/dev/null || die "current ref $CURRENT_REF is not in this checkout"
fi

# Pin to two whole logical CPUs rather than only a CFS quota: a quota lets the
# work smear across every host core and get throttled in bursts, which is
# exactly the run-to-run variance this pipeline exists to remove. Default:
# skip CPU 0 (most interrupt handling) and take the next two CPUs that sit on
# different physical cores when the host has enough of them.
if [ -z "${CPUSET:-}" ]; then
  host_cpus=$(nproc --all)
  if [ "$host_cpus" -ge 4 ]; then CPUSET="2,3"; else CPUSET="0,1"; fi
fi
[ "$(tr ',' '\n' <<<"$CPUSET" | wc -l)" -eq "$CPUS" ] || die "CPUSET must name exactly $CPUS CPUs (got $CPUSET)"

mkdir -p "$output_dir"
output_dir=$(cd "$output_dir" && pwd)

limits=(
  --cpus="$CPUS" --cpuset-cpus="$CPUSET"
  --memory="$MEMORY" --memory-swap="$MEMORY"   # equal values = swap disabled
  --pids-limit=1024
  --ulimit nofile=65536:65536
  # The idle-memory step opens 10k loopback connections per run; widen the
  # container's port range and reuse TIME_WAIT so repeated runs never exhaust it.
  --sysctl net.ipv4.ip_local_port_range="1024 65000"
  --sysctl net.ipv4.tcp_tw_reuse=1
  --sysctl net.core.somaxconn=4096
)
common_env=(--env RESULTS_DIR=/results --env HOST_UID="$(id -u)" --env HOST_GID="$(id -g)")
cache_mount=()
if [ -z "${NO_CACHE:-}" ]; then
  cache_mount=(--mount "type=volume,src=$cache_volume,dst=/cache"
               --mount "type=volume,src=$cache_volume-cargo,dst=/usr/local/cargo/registry")
fi

build_args=()
[ -n "${RUST_IMAGE:-}" ] && build_args+=(--build-arg "RUST_IMAGE=$RUST_IMAGE")
[ -n "${RUNTIME_IMAGE:-}" ] && build_args+=(--build-arg "RUNTIME_IMAGE=$RUNTIME_IMAGE")
build_rev=$(git -C "$repo_root" rev-parse HEAD)
build_args+=(--build-arg "BUILD_REV=$build_rev")

# A failing phase does not stop later phases: a current-tree test failure
# should still produce the end-to-end evaluation. The exit status reports it.
status=0
for phase in $phases; do
  case "$phase" in
    verify)
      echo "==> building $image:toolchain"
      docker build "${build_args[@]}" --target toolchain --tag "$image:toolchain" \
        --file "$repo_root/docker/fixed-test/Dockerfile" "$repo_root"
      echo "==> verify: previous=$baseline_ref vs current, cpus=$CPUS cpuset=$CPUSET memory=$MEMORY"
      docker run --rm "${limits[@]}" "${common_env[@]}" "${cache_mount[@]}" \
        --env BASELINE_REF="$baseline_ref" --env CURRENT_REF="${CURRENT_REF:-}" \
        --env EXPECTED_BUILD_REV="$build_rev" \
        --mount "type=bind,src=$output_dir,dst=/results" \
        "$image:toolchain" || status=1
      ;;
    bench)
      echo "==> building $image:bench"
      docker build "${build_args[@]}" --target bench --tag "$image:bench" \
        --file "$repo_root/docker/fixed-test/Dockerfile" "$repo_root"
      echo "==> bench: ${ROUNDS:-6} rounds, cpus=$CPUS cpuset=$CPUSET memory=$MEMORY, no network"
      docker run --rm "${limits[@]}" "${common_env[@]}" --network none \
        --env ROUNDS="${ROUNDS:-6}" --env REFERENCE_LIB="${REFERENCE_LIB-ws}" \
        --env DRIVER_ARGS="${DRIVER_ARGS:-}" \
        --mount "type=bind,src=$output_dir,dst=/results" \
        "$image:bench" || status=1
      ;;
    *) die "unknown phase: $phase (want verify and/or bench)" ;;
  esac
done

echo "==> results: $output_dir (summary: $output_dir/evaluation.md)"
[ "$status" -eq 0 ] || echo "==> FAILED: see the correctness matrix and gates in evaluation.md" >&2
exit "$status"
