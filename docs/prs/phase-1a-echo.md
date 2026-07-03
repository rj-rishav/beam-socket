# PR: Phase 1A — Echo server, end-to-end

**Phase:** 1A only (ENGINEERING.md §5). No rooms, users, or presence anywhere
in this diff. Branch: `phase-1a-echo`.

One real WebSocket client connects, sends a message, gets it echoed back —
through the graduated RFC 0001 design-C bridge. TS SDK → NAPI → engine →
Tokio → socket and back.

## What's in it

- **crates/core** — `Engine::start` boots a multi-threaded Tokio runtime on
  its own threads (`Engine::shutdown` stops it; the Node loop is never
  blocked, including during shutdown, which tears down in the background).
  `transport/websocket.rs`: accept + handshake via tokio-tungstenite behind
  the `Transport` trait (`FrameSource`/`FrameSink`), codec swap contained.
  `connection/`: per-connection task — read loop, writer task, bounded
  mailbox, Rust-only ping/pong keepalive, close handshake both directions,
  panic contained per connection **with cleanup** (`catch_unwind` → registry
  removal + Closed event still fire). `connection/registry.rs`: sharded slab
  (16 shards), `ConnectionId` = shard(8) | generation(24) | key(32); the
  generation bump on slot recycling makes a stale ID from JS miss instead of
  addressing the wrong connection. `events.rs`: bounded engine→bridge channel.
- **crates/node** — napi/cdylib turned ON behind an off-by-default `napi`
  cargo feature (`crate-type = ["cdylib","rlib"]`, deps optional, build.rs
  gated), so CI's `cargo test --workspace` never links Node symbols and stays
  green; the addon builds with `cargo build -p beamsocket-node --release
  --features napi`. The graduated flat wire format gained an event-kind byte
  (0 text / 1 binary / 2 open / 3 close`[u16 code][reason]`) — same layout
  and size, open/close ride the same batched stream. TSFN drain loop wired in
  the spike's design-C shape: dedicated thread, own current-thread runtime,
  batch 256 / 1 ms, Blocking calls, TSFN queue bound 4, external flush
  buffers per the 16 KB crossover (`buffers::should_externalize`).
- **packages/beamsocket** — real `new BeamSocket(config)`, `io.listen(port)`,
  `io.on('connection')`, `socket.on('message'|'close')`, `socket.send()`,
  `socket.id`; cursor-reader demux with **zero-copy subarray** payload views;
  native loader. `io.close()` exists with Phase 1A semantics (stop accepting,
  sweep-close 1001, background stop) — drain/`timeoutMs` semantics stay
  Phase 1D. Config gained `keepalive { pingIntervalMs, pongTimeoutMs }`
  (defaults 30 s / 10 s), mirrored in `crates/core/src/config.rs`.

## Exit gates (ENGINEERING.md §5)

| Gate | Status |
|---|---|
| Echo from stock `ws` client, text + binary | ✅ `__tests__/echo.integration.test.mjs` |
| Clean close both directions | ✅ same test: client-initiated 1000 + server-initiated 4001 w/ reason, close events observed on both sides |
| Connection-task panic contained (test proves it) | ✅ `crates/core/tests/phase1a.rs::panicking_connection_task_is_contained` — Closed(1011) emitted, runtime + sibling connection keep working |
| No unbounded queue (`grep -r unbounded`) | ✅ zero matches in crates/ + packages/ (incl. tests/benches) |
| 10k-idle-connection RSS | ✅ **15.04 KB/conn** (146.9 MB over 10k; baseline 49.5 MB; `node benchmarks/idle-rss.mjs`) — under the <20 KB target context, JS `Socket` proxies included |
| Autobahn green (deflate excluded) | ⚠️ **CI job wired, not run locally** — this sandbox has no Docker daemon. `.github/workflows/ci.yml` `autobahn` job runs the fuzzingclient against `examples/echo/server.mjs` (12.\*/13.\* excluded) and fails on any non-OK case. Treat first green CI run (or the pinned box) as the gate. |
| JS→Rust send microbench (0001 follow-up) | ✅ measured (`benchmarks/send-microbench.mjs`): stale-id FFI+registry floor **1.77 µs/call**, live 64 B binary **2.18 µs**, live text **1.27 µs** on the shared sandbox. Above the RFC's sub-µs hypothesis — shared-box numbers with the usual ~2× contention variance; ≥0.5 M sends/s per thread still cannot bottleneck vs the batched callback path. Re-measure on the pinned box with the RFC 0001 constants re-confirmation (already a 1D release blocker). |

## Rule 4 — per-connection memory cost

Measured: **15.04 KB/conn** at 10k idle (RSS delta, includes kernel TCP
buffers, tungstenite state, both task futures, registry entry, and the JS
`Socket` proxy). Itemized (approx., heap):
registry slab entry ≈ 32 B + 4 B generation side-table; mailbox
(mutex+deque+notify) ≈ 150 B; control channel (cap 4) ≈ 250 B; close watch
≈ 200 B; reader+writer task futures ≈ 1–2 KB; the remainder is codec read
buffer growth and kernel socket buffers.

## Rule 5 — queue inventory (all bounded, all with policy + metric)

| Queue | Bound | Overflow policy | Metric |
|---|---|---|---|
| engine→bridge mpsc | 8192 (`ENGINE_BRIDGE_QUEUE_CAPACITY`, cited to 0001-results.md) | `Message`: drop-newest; open/close: lossless awaited send (blocks that connection only — dropping a close would desync SDK state forever) | `bridge_dropped` |
| per-conn send mailbox | `highWaterMark` bytes (default 64 KB; one oversized frame allowed through an empty queue, real cap is `maxPayloadBytes`) | configured `BackpressurePolicy` (disconnect → close 1013 / drop-newest / drop-oldest) | `backpressure_drops` |
| per-conn control channel | 4 | benign `try_send` skip: a skipped ping retries next tick; every close is mirrored in the un-losable close watch latch | n/a (no loss possible) |
| TSFN delivery queue | 4, Blocking mode | back-pressures the drain thread → bounded mpsc sheds visibly (RFC 0001 survival behavior) | via `bridge_dropped` |

## Rule 1 audit

Ping/pong: tungstenite answers pings inside the codec; keepalive
ping/timeout runs in the reader task. Close bookkeeping: watch latch +
writer close frame + handshake, all Rust. JS is called only with the batched
event stream for subscribed events (open/message/close). No per-message JS
anywhere else; `grep`-able by the absence of TSFN calls outside the one
flush path.

## Deviations / follow-ups (not blockers)

- Autobahn: local sandbox can't run Docker; the CI job is the gate (above).
- Send microbench exceeded the sub-µs hypothesis on the shared box — pinned
  box re-measure rides the existing 1D constants re-confirmation blocker.
- `Socket.send(string)` with invalid UTF-8 can't happen from JS (strings are
  UTF-16→UTF-8 converted by napi); the Rust writer still guards and falls
  back to a binary frame rather than poisoning the connection.
- Non-goal reminder: `send()` resolving means "accepted into the send
  queue" — frame delivery, not message delivery (ARCHITECTURE.md §4).

## PR checklist (§11)

- [x] One phase per PR — Phase 1A only
- [x] Rule 1 audit above
- [x] Rule 4 memory cost above (measured, not estimated)
- [x] Rule 5: every new queue in the inventory above
- [ ] Rule 3 (proxy/IP) — n/a, no feature in this PR reads client IPs
- [x] §5 required tests green: `cargo test --workspace`, `cargo clippy
      --workspace -- -D warnings` (+ `--features napi` clippy), `cargo fmt
      --check`, `tsc --noEmit`, `npm test -w beamsocket` (API surface + ws
      echo integration), spike workspace tests untouched and green
