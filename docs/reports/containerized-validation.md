# Fixed-resource containerized validation

## Purpose

Results on the shared development laptop drift with whatever else the machine
is doing: the same build measured ±10–15% apart run to run (see
`docs/reports/0.2.1-hardening.md`). This pipeline runs the previous and the
current tree with an identical, fixed budget so that differences come from the
code rather than the host:

- **2 CPUs, pinned:** `--cpuset-cpus` names two host CPUs (default `2,3`: not
  CPU 0, two different physical cores) plus `--cpus=2`. A quota alone lets the
  work spread across every host core and get throttled in bursts.
- **2 GiB memory, no swap:** `--memory=2g --memory-swap=2g`.
- **Bounded processes and files:** `--pids-limit=1024`, `nofile=65536`.
- **Network:** the bench phase runs with `--network none` (all traffic is
  loopback inside the container). Its network namespace widens the ephemeral
  port range and enables `tcp_tw_reuse`, so repeated 10k-connection runs
  cannot exhaust ports.
- **Same toolchain for both trees:** Rust 1.97.1 and Node 24 on Alpine 3.24 (musl).
  `RUSTUP_TOOLCHAIN` pins the image's compiler. Otherwise
  `rust-toolchain.toml` (`stable`) would download whatever stable is current.

## Images (both Alpine)

| Target | Base | Size | Used for |
|---|---|---:|---|
| `toolchain` | `rust:1.97.1-alpine3.24` + nodejs/npm/build-base | ~1.7 GB | correctness matrix, building each tree's musl addon + SDK |
| `bench` | `alpine:3.24` + nodejs | ~116 MB | end-to-end A/B measurement (no compiler, no sources) |

The toolchain image is large because it contains the Rust compiler, which no
base image can avoid. Measurements run in the small runtime image.

## Run

```sh
./scripts/docker-fixed-test.sh                       # verify + bench, previous = d961332
BASELINE_REF=v0.2.0 ROUNDS=10 ./scripts/docker-fixed-test.sh
PHASES=verify ./scripts/docker-fixed-test.sh         # correctness + core micro A/B only
PHASES=bench OUTPUT_DIR=.artifacts/<run> ./scripts/docker-fixed-test.sh   # re-measure existing builds
BASELINE_REF=d961332 CURRENT_REF=cc1d1ba ./scripts/docker-fixed-test.sh   # any two commits (bisecting)
```

| Variable | Default | Meaning |
|---|---|---|
| `BASELINE_REF` | `d961332` | previous version (`main` before the 0.2.1 hardening branch) |
| `CURRENT_REF` | working tree | compare this commit instead of the working tree |
| `CPUSET` | `2,3` (`0,1` on < 4 CPUs) | the two host CPUs to pin |
| `ROUNDS` | `6` | end-to-end A/B rounds |
| `REFERENCE_LIB` | `ws` | control library run each round (`""` disables) |
| `DRIVER_ARGS` | — | extra `benchmarks/driver.mjs` arguments |
| `OUTPUT_DIR` | `.artifacts/fixed-test-<utc>` | results (git-ignored) |
| `NO_CACHE` | unset | `1` disables the cargo/npm cache volumes |

The cache volumes hold only the crate registry, the npm cache and `target/`
directories. Commits extracted with `git archive` are cached **per commit
SHA** (`target-ref-<sha>`), and the working tree uses `target-worktree`. This
matters: `git archive` stamps every file with the commit time, which is older
than any cached artifact. A shared directory would make Cargo's mtime check
silently reuse another commit's build. That happened once while building
this pipeline: a `cc1d1ba` run linked `d961332`'s core. Prune old
`target-ref-*` directories, or set `NO_CACHE=1`, when disk is tight.

CI: `.github/workflows/fixed-resources.yml` runs the same script on
`ubuntu-latest`. It writes `evaluation.md` to the job summary and uploads the
results, excluding staged builds. Hosted runners are shared VMs, so treat
their numbers as indicative and compare runs on the same host.

## Phases

**verify** (toolchain image, 2 CPU / 2 GiB). For the previous tree (`git archive
$BASELINE_REF`) and the current working tree, it runs fmt, clippy (workspace +
napi), `cargo test --workspace`, `npm ci`, the native build, the SDK build and
`npm test`. Every step is recorded with its status, time and log. The run
continues past failures; it fails only when the **current** tree fails. It
then runs the core fan-out micro A/B (`cargo bench --bench fanout_exclusions`,
which embeds the frozen previous implementation).

**bench** (runtime image, 2 CPU / 2 GiB, no network). `ROUNDS` paired rounds of
the full `driver.mjs` run against the previous and the current engine. The
order alternates each round. Both variants use the **current** driver and bench
servers; only the SDK + addon is swapped. The previous driver's accounting was
invalid, and an engine A/B must not also change the measuring tool. The
control library runs once per round. Its spread is reported as the
environment's noise floor.

## Evaluation (`evaluation.md`, `verify.json`, `bench.json`)

For each end-to-end metric, the paired per-round change is oriented so that
positive means the current tree is better. The verdict rules:

- **improved / slower:** at least 80% of rounds agree in direction *and* the
  median exceeds both 2% and the noise floor;
- **no clear change:** anything else;
- **unstable metric:** one variant's own rounds spread more than 1.5×
  (multi-modal or heavy-tailed). The verdict is not judged and never gates;
- **REGRESSION (gate):** the median is worse than the gate *and* at least 80%
  of rounds are worse. Gates: throughput −10%, latency and fan-out +15%, idle
  memory +10%. A breach fails the run.

`DRIVER_ARGS="--only fanout --fanout 5000"` (also `throughput`,
`throughput16k`, `latency`, `memory`) runs one phase only. That is the cheap
way to get 20+ rounds on a single suspicious metric.

The micro A/B gate (≥ 2× for 256+ exclusions, no cell > 10% slower) is
reported but not enforced. One run cannot show a *repeatable* tiny-cell
regression.

## Results: 0.2.1 hardening vs previous (2026-10-01)

All runs: Alpine 3.24, Rust 1.97.1, Node 24.18.1, CPUs 2–3 of an i5-1135G7,
2 GiB with no swap. Peak container memory in full runs was 0.53–0.64 GiB, with no OOM
events and no failed runs. Raw data is under `.artifacts/` (local, not
committed).

**Correctness (final run, `d961332` → working tree):** every step passes on
both trees. Rust: 170 → **176** passed, 4 ignored. JS: 50 → **51** passed.
Earlier runs exposed a flaky mesh test. `saturation.rs` failed 5/20 in the
container because loopback kernel buffers (`tcp_rmem` max 32 MiB) absorbed
its fixed 4 MB blast. It now saturates by observation and passed 30/30.

**Core fan-out micro A/B (musl, 2 CPUs, four runs):** large-exclusion
speedups are up to 7.8×. The smallest qualifying cell (1,024 recipients / 256
exclusions) ranges 1.75–2.42× across targets and distributions, so the
"≥ 2×" gate is not met in every cell here. On the glibc laptop it was
2.07–2.28×. Small indexed cells (1,024/32: 0.85–1.20×; 64/32: 0.91–1.13×)
spread no more than cells that run identical code in both versions (1,024/0:
0.91–1.15×; 1,024/1: 0.90–1.09×). That is single-cell noise under 2 CPUs,
not a musl cost.

**End-to-end, final run (8 paired rounds):**

| Metric | Previous | Current | Paired Δ | Rounds better | Verdict |
|---|---:|---:|---:|---:|---|
| Idle memory / connection | 12,652 B | 9,201 B | **+27.4%** | 8/8 | improved |
| Echo throughput 64 B | 51,421/s | 50,698/s | −1.1% | 2/8 | no clear change |
| Echo throughput 16 KiB | 14,808/s | 14,329/s | −3.2% | 2/8 | no clear change |
| Echo latency p50 / p99 | — | — | — | — | unstable (see below) |
| Fan-out 1,000 / 3,000 | — | — | −0.9% / −1.3% | 4/8, 4/8 | no clear change |
| Fan-out 5,000 | 51.8 ms | 56.3 ms | −8.7% | 1/8 | slower (gate −15%: pass) |

Attribution (bisect runs with the same pipeline):

| Comparison | Memory | Fan-out 5,000 |
|---|---|---|
| `cc1d1ba` → working tree (read buffer only) | **+26.2%, 6/6** | −2.3%, 2/6 |
| `d961332` → `cc1d1ba` (exclusions, push helper, empty frames) | −0.3%, 3/8 | −4.8%, 2/8 |
| Fan-out 5,000 alone, `--only fanout`, 20 rounds | — | **+1.0%, 12/20** |

The memory win comes entirely from the 1 KiB read buffer. Fan-out to 5,000
is slower only when it runs after the throughput and latency phases in the
same process. In isolation it is unchanged. So the effect depends on state
built up by earlier activity, and no code path explains it: the core enqueue
is neutral in the micro A/B. It stays within the gate and is an open item.

**Engine finding unrelated to this branch:** BeamSocket's echo p50 is
bimodal under 2 CPUs in *both* trees. Runs land near 0.7 ms or near 2.7 ms,
while `ws` stays at about 0.45 ms. The evaluator marks such metrics as
unstable instead of judging them. That is the main reason this pipeline
never reported a latency gate breach that would not reproduce.

## Limitations

- Pinning removes cross-core migration, but not activity on the same cores'
  hyper-thread siblings (CPUs 6 and 7 on the 4-core laptop) or host interrupts.
  For release claims, use a dedicated host, record the image digests, and keep
  the same `CPUSET`.
- The benchmark client and server share the 2-CPU budget, as they do in
  `driver.mjs` everywhere. This is fair for an A/B comparison, but it is not
  an absolute server capacity figure.
- Alpine/musl numbers are not comparable with the glibc files under
  `benchmarks/results/`. Compare runs from this pipeline only with each other.
- uWebSockets.js ships glibc-only binaries, so it cannot run in these images.
