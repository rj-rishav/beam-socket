# RFC 0002 attach spike — THROWAWAY

**Throwaway-grade, Linux + plaintext only.** Proves the single Node-specific
unknown behind [RFC 0002](../../docs/rfcs/0002-http-attach.md) design A (fd
handoff): a socket that Node's `http.Server` accepted can be `dup()`'d, detached
from Node without closing the connection, adopted as a Tokio `TcpStream`, have
its WebSocket 101 completed by Rust, and its `head` bytes replayed so a first
frame coalesced with the upgrade is not lost.

Its own workspace on purpose — **not** in `spike/Cargo.toml`, **not** built in
CI. Nothing here graduates; only the *design decision it validates* does.

## Run

```bash
# build the addon (Linux):
cargo build --release            # → target/release/libattach_spike.so
cp <target>/release/libattach_spike.so attach.node

node run.mjs        # T1 ws-client echo + T2 coalesced-head replay
node churn.mjs 300  # fd-hygiene: fd count stays flat across N connections
```

## What it does

- `src/lib.rs` — a napi addon: `adoptAndEcho(fd, wsKey, head)` `dup()`s the fd,
  adopts it on a Tokio runtime, writes the 101 (`derive_accept_key`), replays
  `head` via a `PrefixedStream` (head-then-wire read, writes straight to the
  socket), and echoes.
- `run.mjs` — an `http.Server` that hands each `/ws` upgrade's `socket._handle.fd`
  to Rust, then `socket.destroy()`s the Node side (the dup keeps the connection
  alive). Two clients: a stock `ws` client (T1) and a raw client that writes its
  first WS frame **coalesced with the upgrade request** in one TCP write (T2 —
  the frame lands in `head` and must still echo).
- `churn.mjs` — 300 connect/echo/close cycles; asserts the process fd count is
  flat (no per-connection fd leak).

## Results (Linux, Node v18.19.1, plaintext)

```
T1 ws-client echo:            PASS
T2 coalesced-head replay:     PASS   (first frame in `head` echoed — no loss)
churn (300 conns):            PASS   (fd count Δ 0 — no leak)
```

Stable across repeated runs. **Conclusion:** design A's mechanic holds on Linux
— `socket._handle.fd` is accessible, `dup()` + `socket.destroy()` detaches
without tearing down the connection, Tokio adoption + a Rust-written 101 works,
and head replay delivers coalesced first frames. macOS uses the identical POSIX
`dup`/`from_raw_fd` path (expected to hold; not run here). This folds into RFC
0002 §12–§13; it removes the pre-implementation "prove the detach sequence" gate.

## Deliberately NOT covered (per RFC 0002 scope)

TLS, Windows (`SOCKET` handles need `WSADuplicateSocket`), benchmarks, error-path
RAII (the RFC specifies `FdHandoffGuard`), rooms/identity/authorize — all either
proven elsewhere or explicitly deferred by the RFC.
