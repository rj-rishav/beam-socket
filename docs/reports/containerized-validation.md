# Fixed-resource containerized validation

## Purpose

Results on the shared development laptop drift with whatever else the machine
is doing: the same build measured ±10–15% apart run to run (see
`docs/reports/0.2.1-hardening.md`). This pipeline runs the previous and the
current tree with an identical, fixed budget to reduce resource-related drift.
Host scheduling, interrupts and clock-frequency changes can still affect timing:

- **2 logical CPUs, pinned:** `--cpuset-cpus` names two host CPUs (default `2,3`)
  plus `--cpus=2`. They are on different physical cores on this laptop; set
  `CPUSET` appropriately for another host's topology. A quota alone lets the
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

The wrapper passes the current Git revision as `BUILD_REV` to Docker and as
`EXPECTED_BUILD_REV` to verification. The source layer is keyed by that revision,
and verification rejects an image if its recorded revision or embedded checkout
does not match the runner's checkout. The actual image revision is recorded in
`verify/environment.txt`. Uncommitted source changes are still included by
Docker's `COPY` instruction.

CI: `.github/workflows/fixed-resources.yml` runs the same script on
`ubuntu-latest`. It writes `evaluation.md` to the job summary and uploads the
results, including the hidden `.artifacts` directory and excluding staged builds.
Hosted runners are shared VMs, so treat
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

## Version-control scope

This validation was prepared on branch `perf/0.2.1-hardening`:

- `main` / `d961332` is the previous-version reference;
- `aee8323` is the current implementation and pipeline commit used for the
  comparison;
- the follow-up changes in this working branch harden image provenance and
  documentation only; they do not change the Rust or TypeScript implementation
  being evaluated.

Use `CURRENT_REF=<commit>` when comparing two committed trees, or omit it to
test the working tree. The runner uses a full checkout because both
`git archive` and the embedded source-revision check require the referenced
objects to be available locally.

## Results: fixed-resource retest (`d961332` → `aee8323`, 2026-10-06)

This run used the actual committed previous/current refs inside the container,
not the host working tree. Verify used Alpine 3.24.1, bench used Alpine 3.24.2,
Rust 1.97.1, Node 24.18.1, CPUs 2–3, 2 GiB with no swap. The bench phase
reported a 612,794,368-byte peak and zero OOM events. Raw output is in
`.artifacts/fixed-test-current/` locally; it is intentionally ignored by Git.

**Correctness:** the current tree is green: **176 Rust tests passed, 0 failed,
4 ignored; 51 JS tests passed**. The previous tree has **169 Rust tests passed,
1 failed, 4 ignored; 50 JS tests passed**. The one previous failure is the old
`crates/mesh/tests/saturation.rs` test: its fixed blast did not fill the
sender pressure gauge under this 2-CPU loopback environment. The current tree's
observation-based saturation test passes. This is recorded as a previous-version
failure, not hidden or counted as a current regression.

**Core fan-out micro A/B:** all 108 cells completed in one process, comparing
the current implementation to its frozen pre-optimization implementation.
Large-exclusion speedups were **1.99×–7.66×**. The minimum is one
1,024-recipient / 256-exclusion cell at 1.99×, which misses the 2× target in
this run. The micro gate is reported separately from correctness and does not
fail the correctness phase. The worst all-cells result was a
1,024-recipient / one-duplicate-exclusion cell at 0.94×.

**End-to-end, six paired rounds, alternating order:**

| Metric | Previous | Current | Paired Δ | Rounds better | Verdict |
|---|---:|---:|---:|---:|---|
| Idle memory / connection | 12,635 B | 9,279 B | **+26.1%** | 6/6 | improved |
| Echo throughput 64 B | 51,386/s | 51,012/s | −0.5% | 3/6 | no clear change |
| Echo throughput 16 KiB | 16,331/s | 16,151/s | −2.1% | 3/6 | no clear change |
| Echo latency p50 | 2.516 ms | 1.860 ms | +14.9% | 4/6 | unstable, 3.5× spread |
| Echo latency p99 | 4.739 ms | 4.756 ms | +5.8% | 4/6 | unstable, 1.6× spread |
| Room fan-out 1,000 | 13.23 ms | 13.64 ms | −13.2% | 2/6 | unstable, 1.9× spread |
| Room fan-out 3,000 | 28.505 ms | 29.200 ms | −2.9% | 3/6 | no clear change |
| Room fan-out 5,000 | 51.715 ms | 49.910 ms | **+3.5%** | 5/6 | no clear change |

The evaluator's end-to-end gate passed. The memory reduction is the measured
effect of the 4 KiB → 1 KiB codec read-buffer change in this branch. The
exclusion-index change is intentionally evaluated separately by the core micro
A/B because the normal room benchmark sends to every member without a large
`except` list. Throughput remains within the 10% gate; unstable latency and
1,000-member fan-out are reported rather than converted into false claims.

### Image-provenance follow-up

The public wrapper was run again with `PHASES=verify`, `CURRENT_REF=aee8323`,
and no cache volumes after the revision check was added. It rebuilt the image
with `image_build_revision=aee8323` and the check accepted the matching embedded
checkout. Both previous and current correctness matrices passed in this
follow-up: 170 Rust tests versus 176 Rust tests, 4 ignored in each, and 50
versus 51 JavaScript tests. The micro benchmark completed all 108 cells; its
large-exclusion range was 1.75×–6.95× and, as expected for a single noisy run,
the micro gate was reported as not fully met. This follow-up was a provenance
and correctness check only; it did not replace the six-round end-to-end result
above.

If `EXPECTED_BUILD_REV` does not match either `/image-build-revision` or the
embedded checkout's `HEAD`, `verify.sh` exits with status 2 before creating
test results. This prevents an old Docker image from being mistaken for a
current comparison.

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
