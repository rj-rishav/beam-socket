# RFC 0002 — HTTP Server Attach

**Status:** FROZEN — accepted; the spec for the Phase 1.1 implementation
(`phase-1.1-attach`). Two review riders folded in on acceptance: (1) §8.1 gains
the stranded-bytes drain step; (2) §14's macOS row requires CI proof before any
release doc claims macOS support.
**Phase:** 1.1 — Attach to an Existing HTTP Server (ENGINEERING.md §9)
**Depends on:** Phase 1D runtime (`phase-1d-runtime`) — the connection lifecycle,
the admission `Gate`, `authorize`, and the graceful `close()` drain this RFC
designs against, and changes nothing in.
**Author:** Runtime
**Companion spike:** `spike/attach/` (throwaway; Linux plaintext only) — see §12.

> Phase 1 owns its port. The single most-requested adoption blocker is *"can I
> run BeamSocket on my existing Express/Fastify server — one port, one TLS
> setup, one load balancer?"* (ARCHITECTURE §4). This RFC decides **how a
> connection that Node's `http.Server` accepted becomes a connection the Rust
> engine owns** — and, just as importantly, which parts of that we ship in 1.1
> and which we honestly defer.

---

## 1. Why this needs an RFC (the critical unknown)

Every other Phase-1 feature operated on a TCP stream the **Rust engine itself
accepted** (`engine.listen(port)` → Tokio `TcpListener::accept` →
`transport::accept`). Attach inverts ownership: Node's `http.Server` has already
`accept()`ed the socket, parsed the HTTP request, and emitted:

```js
httpServer.on('upgrade', (req, socket, head) => { /* … */ })
```

- `req` — a fully parsed `http.IncomingMessage` (method, url, **headers**).
- `socket` — a `net.Socket` wrapping a **libuv-owned** handle (an fd on Unix, a
  `SOCKET` on Windows), possibly already registered with libuv's event loop.
- `head` — a `Buffer` of bytes Node **already read past the request headers**.

The engine wants to own the raw byte stream on its Tokio runtime and run the
1A–1D data plane over it unchanged. Three properties make this the highest-risk
component of the phase, exactly as the bridge was for RFC 0001:

1. **It cannot be validated in pure Rust.** The uncertain step is Node-specific
   — does libuv cleanly *release* a socket mid-life without stealing bytes or
   double-closing the fd? That only exists with Node + libuv + napi in the loop.
2. **Its correctness is silent when wrong.** A dropped `head` byte loses the
   client's first frame with no error. A double-closed fd corrupts an unrelated
   connection that recycled the number. These fail quietly, in production.
3. **Its platform/TLS matrix is API surface.** "Does `{ server }` work on
   Windows / with `https.Server`?" is a support commitment we must state, not
   discover. An honest "not yet" beats a broken "yes."

## 2. The one rule that constrains every option

**The data plane stays in Rust (Rule 1 / ARCHITECTURE §1).** Whatever the
acquisition path, once a connection is live, per-message bytes must NOT traverse
the Node event loop. This immediately grades the candidates: any design that
leaves Node in the byte path is at best a *documented degraded tier*, never a
default, and this RFC does not ship one.

## 3. Candidate designs

| | Design | Mechanism | Verdict (argued below) |
|---|---|---|---|
| **A** | **fd handoff** | `dup()` the fd out of the Node socket, detach Node WITHOUT closing the connection, rebuild a Tokio `TcpStream` via `from_raw_fd`, and let Rust complete the 101 + own every byte thereafter. | **Recommended default** (Linux + macOS, plaintext). Preserves Rule 1 exactly — post-handoff a connection is indistinguishable from an own-port one. |
| **B** | **stream proxy** | Socket stays in Node; a JS `'data'`/`write` pump copies every byte across the bridge to/from Rust. | **Rejected as a default; not shipped.** Puts the Node loop in the data path — violates Rule 1, re-imports the per-socket Node memory we exist to avoid, doubles the data-path CPU. Kept only as a *named future option* if a real need (e.g. a platform with no fd handoff AND a hard single-port requirement) ever forces a degraded tier — and even then it would be opt-in and loudly documented. |
| **C** | **hybrid** | A where the platform supports it; a **standalone-port fallback** (run the engine's own `listen()` alongside the app, behind the same LB) where it does not; an explicit support matrix. | **This is what 1.1 actually ships** — A is the mechanism, C is the packaging. The fallback is *standalone port*, deliberately **not** B, so Rule 1 holds everywhere. |

The decision, stated once: **ship A, package it as C (A + standalone-port
fallback), never ship B.** The rest of the RFC defends this against the four
hard problems and pins the platform/TLS matrix.

## 4. Data flow (upgrade → handoff → first frame)

```
  ┌── Node (JS) ─────────────────────────────┐        ┌── Rust (engine runtime) ──────────┐
  │                                           │        │                                   │
  │  http.Server 'upgrade' (req, socket, head)│        │                                   │
  │      │                                    │        │                                   │
  │      │ 1. path match?  no → ignore (defer │        │                                   │
  │      │      to other listeners / catch-all)        │                                   │
  │      │ yes ↓                              │        │                                   │
  │  2. socket.pause()  (stop libuv reads)    │        │                                   │
  │  3. fd = socket._handle.fd                │        │                                   │
  │  4. attach(fd, remoteAddr, method, url,   │  napi  │  5. dup(fd) → owned dup'd fd      │
  │       headersFlat, head)  ───────────────────────► │     wrapped in FdHandoffGuard (§8.4)│
  │      │                                    │        │  6. Gate.admit(peer, headers)     │
  │      │  returns Accepted / Rejected(code) │◄────── │       (trustProxy IP + per-IP +   │
  │  7a. Accepted → socket.destroy() WITHOUT  │        │        draining/503)  →  §8.1 close│
  │       ending the connection (dup keeps it)│        │  6b. authorize round-trip (§7)    │
  │  7b. Rejected → Rust already wrote the     │        │  7. from_raw_fd → Tokio TcpStream │
  │       HTTP error on the dup'd fd; JS just  │        │  8. write 101 Switching Protocols │
  │       destroys the Node socket.           │        │       (Sec-WebSocket-Accept)      │
  │                                           │        │  9. PREPEND `head` to the read    │
  │                                           │        │       stream, then normal codec   │
  │                                           │        │  10. setup_connection (1C/1D path)│
  └───────────────────────────────────────────┘        └───────────────────────────────────┘
                                    the client's first WS frame — which may be
                                    inside `head` — is delivered at step 9.
```

Steps 5–10 are the engine's `adopt` seam (§6); steps 6–10 are **identical** to
the own-port path (`Gate.admit` → `authorize` → `setup_connection`) — attach
only replaces *how the stream and the parsed request arrive*, never what happens
after.

## 5. The engine seam (design against the existing code, change nothing here)

Phase 1C/1D gave us exactly the seam attach needs. The own-port path is:

```
accept_loop → T::accept(io, peer, &config, &gate) → Accepted { sink, source, upgrade }
            → setup_connection: authorize → registry.insert → run_connection
```

`transport::accept` today reads the HTTP request off the socket
(`accept_hdr_async_with_config`) and runs the `Gate` inside the handshake
callback. **Attach cannot reuse `accept` verbatim**, because Node already
consumed the request line + headers — there is nothing left on the wire to
parse. So the implementation phase (not this RFC) adds a sibling entry point:

```
// Proposed for the implementation phase — NOT written here.
Transport::adopt(io: TcpStream, peer: IpAddr, request: ParsedUpgrade,
                 head: Bytes, config: &Config, gate: &Gate)
    -> Result<Accepted<Sink, Source>, AcceptError>
```

- `ParsedUpgrade` carries the Node-parsed `{ method, url, headers }`.
- `adopt` runs the SAME `Gate.admit(peer, headers, url)` the callback runs today
  (so `trustProxy`, `maxConnectionsPerIp`, and the 1D `draining`/503 check are
  unchanged), computes `Sec-WebSocket-Accept`, writes the 101, and builds the
  `WebSocketStream` in `Role::Server` over a **head-prefixed** reader (§8.3).
- On success it returns the identical `Accepted` the own-port path returns, so
  `setup_connection` — `authorize`, identity bind, `run_connection`, the 1D
  cleanup — is reused byte-for-byte.

Two converging producers (`accept`, `adopt`), one consumer (`setup_connection`).
The `Transport` trait already abstracts frames + connection IDs, so rooms,
identity, presence, broadcast, metrics, and `close()` all work on an adopted
connection with zero change (§11).

Rejections use the same currency as own-port, but on the correct layer: a gate
reject before the 101 is an **HTTP error status** written to the dup'd fd (Node
never sees it), mirroring how `accept`'s callback returns `Err(status)` today
(429 for per-IP, 503 for draining); an `authorize` reject *after* the 101 is a
**WebSocket close** (1008/app code), exactly like `reject_after_upgrade` in 1D.

## 6. Hard problem 1 — TLS (decision made, not deferred)

**The trap:** with `https.Server`, Node terminates TLS and owns the *decrypted*
byte stream. The underlying fd carries **encrypted** bytes plus mid-session TLS
state (keys, sequence numbers, buffered records) that lives inside Node's
OpenSSL, not on the fd. `dup()`-ing the fd hands Rust ciphertext it cannot read
and TLS state it cannot reconstruct. **Naive fd handoff of an `https.Server`
socket is silently broken** — the handshake would appear to work and then
decode garbage.

Options evaluated:

1. **Engine-side TLS (rustls in the engine).** Sound in general, but it does
   **not** help `https.Server` attach: Node already did the TLS handshake, so
   there is no plaintext-from-the-start for rustls to take over. Engine-side
   rustls only applies to the engine's **own** listener (`listen(443)` with a
   cert) — a different feature, and a different RFC.
2. **Plaintext-attach-only; TLS terminates upstream.** Attach is supported for
   `http.Server` only. TLS is terminated at the load balancer / ingress — which
   is **the deployment reality Companion Rule 2 already assumes** (`Client → CDN
   → LB → Ingress → BeamSocket`), the same topology `trustProxy` exists to serve.
   The blessed production shape is *TLS at the LB → plaintext `http.Server` on
   the app box → BeamSocket attaches*.
3. **Proxy fallback for TLS (design B for `https.Server` only).** Technically
   possible (Node hands decrypted bytes, Rust pumps them) but it drags the data
   plane back onto the Node loop for exactly the TLS users — a Rule 1 violation
   we refuse to ship as a default.

**Decision — 1.1 ships (2):** `{ server }` attaches to `http.Server` (plaintext)
only. Passing an `https.Server` throws a clear, actionable error at attach time:

> `BeamSocket cannot attach to an https.Server (Node owns the decrypted stream;
> the raw fd is ciphertext). Terminate TLS at your load balancer and attach to a
> plaintext http.Server, or run BeamSocket on its own TLS port (engine-side TLS
> is RFC 0003).`

**Deferred (justified, not by omission):** engine-side rustls termination
(`listen(443, { cert })`) is a real feature but orthogonal to attach — it is
**RFC 0003**, not 1.1. We do *not* ship the proxy-fallback TLS tier.

## 7. Hard problem 2 — Windows (honest "not yet")

On Windows, libuv sockets are `SOCKET` handles, not Unix fds; `dup()` /
`from_raw_fd` do not apply. Duplicating a socket across the Node↔Rust boundary
needs `WSADuplicateSocket` → `WSAPROTOCOL_INFO` → `WSASocket` in the adopting
process, and libuv's ownership/detach semantics for an in-use socket differ from
Unix. This is a genuinely different mechanism with its own correctness proof.

**Decision — 1.1 does NOT ship Windows fd handoff.** Windows users get the
**documented standalone-port fallback** (design C's fallback leg): run BeamSocket
on its own port via `listen()` alongside the Express app, behind the same LB /
reverse proxy. This preserves Rule 1 (the engine owns its own sockets) and every
feature, at the cost of a second port. `{ server }` on Windows throws (verbatim):

> `BeamSocket cannot attach to an HTTP server on Windows yet (fd handoff needs
> WSADuplicateSocket, not shipped in 1.1). Run BeamSocket on its own port with
> listen() alongside your HTTP server, behind the same load balancer.`

Windows fd handoff (via `WSADuplicateSocket`) is a named follow-up, gated on its
own spike. An honest "not yet" beats a broken "yes."

## 8. Hard problem 3 — head-byte replay (+ the detach sequence)

### 8.1 The detach sequence (the Node-specific risk)

The uncertain, must-prove mechanic. Ordering matters and every step has a
failure mode:

1. **`socket.pause()`** BEFORE reading the fd — stop libuv from reading more
   bytes into Node (which would strand them where Rust can't reach). Register no
   `'data'` handler.
1a. **(Rider 1) Drain already-buffered bytes into `head`.** `socket.pause()`
   stops *future* libuv reads, but bytes libuv **already** delivered into the
   socket's readable buffer past the request are neither in `head` nor on the
   wire — they would be stranded. So, after pausing, synchronously pull them:
   `for (let c; (c = socket.read()) !== null; ) head = Buffer.concat([head, c])`.
   The combined `head` is what `attach()` replays (§8.3). Named test:
   `attach_drains_stranded_prepause_bytes` (§8.4).
2. **Read `fd = socket._handle.fd`** (Unix). This is a private field; §10 covers
   the stability risk and detection.
3. **`attach(fd, …)` → Rust `dup(fd)`** — Rust now holds an **independent** fd
   referencing the same open file description. Closing either fd alone does NOT
   tear down the TCP connection (POSIX dup semantics); the connection lives until
   *all* fds close. The dup is wrapped in an RAII guard (§8.4) immediately.
4. **On `Accepted`: `socket.destroy()`** — closes Node's *original* fd and
   deregisters it from libuv's epoll. The connection survives on Rust's dup.
   Rust registers its dup in Tokio's (mio) epoll — a clean handoff between two
   independent epoll instances, no shared registration.

The bytes-stealing hazard is why step 1 precedes step 3, and why we never attach
a `'data'` listener. The spike (§12) exists primarily to prove steps 1–4 hold on
real Node.

### 8.2 Where the first frame hides

`head` is whatever Node read past the request headers. Under Nagle/coalescing a
fast client can send its **first WebSocket frame in the same TCP segment as the
upgrade request** — those bytes are in `head`, already off the wire. If Rust
starts reading from the fd without replaying `head` first, that frame is
**silently lost**. This is the classic, quiet attach bug.

### 8.3 The replay path

Rust must feed `head` to the codec *before* the socket's own bytes. Design: a
small `AsyncRead + AsyncWrite` adapter — call it `PrefixedStream` — that yields
the `head` bytes first, then transparently reads from the `TcpStream`; writes go
straight to the `TcpStream`:

```
reads:  [ head bytes … ][ TcpStream bytes … ]   (Cursor<Bytes>.chain(stream) semantics)
writes: ───────────────► TcpStream               (head never affects the write half)
```

The 101 response is written to the raw stream first (before framing starts),
then `WebSocketStream::from_raw_socket(PrefixedStream, Role::Server, ws_cfg)`
runs the normal codec over head-then-wire. Because the split `sink`/`source` the
engine consumes are unchanged, nothing downstream knows a replay happened.

`Sec-WebSocket-Accept` is computed by Rust from the Node-supplied
`Sec-WebSocket-Key` (`base64(SHA1(key + magic-GUID))`) — the same computation
tungstenite does internally; the implementation may either drive tungstenite's
`ServerHandshake` with the pre-parsed request or write the 101 manually and call
`from_raw_socket`. That is an implementation choice, flagged here, decided in the
implementation PR.

### 8.4 Head-replay tests (name them now)

- **`attach_replays_coalesced_first_frame`** — a client whose first WS data
  frame arrives coalesced with the upgrade request (forced via `TCP_NODELAY` +
  writing key+frame together) must be **echoed**; asserts zero head-byte loss.
  This is the test that proves §8.2/§8.3. In the spike it is the Express-echo
  success criterion; in the implementation it becomes an integration test.
- **`attach_empty_head_normal_first_frame`** — the common case (`head.length ===
  0`, first frame arrives after) still echoes — proves the prefix adapter is a
  no-op when empty.
- **`attach_drains_stranded_prepause_bytes`** (Rider 1) — bytes buffered by
  libuv past the request but NOT in `head` are drained via `socket.read()`
  (§8.1 step 1a) and echoed; asserts the pre-pause drain closes the gap the raw
  `head` leaves.

## 9. Hard problem 4 — error-path fd hygiene (RAII, mirroring 1C)

Every failure between `dup` and successful Tokio adoption must **neither leak
nor double-close** the dup'd fd. We reuse the exact pattern 1C used for the
per-IP slot (`IpAdmitGuard`): an RAII guard owns the resource and releases it on
every exit path unless explicitly disarmed on success.

```
// Proposed for the implementation phase — NOT written here.
struct FdHandoffGuard { fd: RawFd, armed: bool }
impl Drop for FdHandoffGuard { fn drop(&mut self) { if self.armed { libc::close(self.fd); } } }
```

- Created the instant `dup` returns; owns the dup'd fd.
- **Gate reject** (per-IP 429 / draining 503) → Rust writes the HTTP error to the
  fd, guard drops → `close(fd)` once. Node's original fd is a *separate* fd
  destroyed by JS; no double close.
- **`authorize` reject** (post-101) → the fd has already become a `TcpStream`
  (guard disarmed at adoption); the WebSocket close + stream drop own teardown.
- **Engine draining / `close()` mid-handoff** → §8.1 step 6 sees `draining`,
  gate returns 503, guard drops. If handoff already produced a `TcpStream`, that
  connection joins the 1D drain like any other.
- **`dup` itself fails** (EMFILE) → no guard created; Rust returns `Rejected`,
  JS destroys the Node socket normally.
- **Success** → guard is disarmed exactly when `from_raw_fd` takes ownership
  (`into_raw_fd` semantics); from that point the `TcpStream`'s `Drop` is the sole
  owner. The dup and the Node original are never closed twice because they are
  different fds with disjoint owners (Rust ↔ Node).

Test: **`attach_fd_hygiene_no_leak_no_double_close`** — churn N attach attempts
that each fail at a different stage (gate reject, authorize reject, drain,
forced `dup` failure) and assert the process fd count returns to baseline (no
leak) and no `EBADF`/corruption (no double close). Mirrors 1C's churn/leak test.

## 10. API surface

### 10.1 `{ server }` and its interplay with `listen()`

```ts
import express from 'express';
import { BeamSocket } from 'beamsocket';

const app = express();
const httpServer = app.listen(8080);            // Node owns the port + TLS-less HTTP
const io = new BeamSocket({ server: httpServer, path: '/ws' });

io.on('connection', (socket) => { /* … 1B–1D API unchanged … */ });
```

- `{ server }` registers BeamSocket's `'upgrade'` listener on `httpServer`
  immediately (whether or not it is already `listen()`-ing). No `io.listen()`.
- **Mutually exclusive with `io.listen(port)`.** Calling `listen()` on a
  server-attached instance throws
  `Error('listen() is invalid when constructed with { server } — the HTTP server owns the port')`;
  constructing with BOTH `{ server }` and later calling `listen()` is the same
  error. (Direction chosen so the failure names the conflict, not a generic
  "already listening".)
- `https.Server` / Windows → throw at attach with the §6/§7 guidance.

### 10.2 `path` filtering and coexisting upgrade listeners

`{ server, path: '/ws' }`. Node dispatches `'upgrade'` to **all** registered
listeners; libraries coexist (Socket.IO at `/socket.io`, BeamSocket at `/ws`).
Rules:

- BeamSocket **claims** an upgrade only when the request path matches `path`
  (exact match on the path component by default; a future `path` predicate
  function is a compatible extension). On a match it handles the handoff.
- On a **non-match**, BeamSocket does **nothing** — it neither responds nor
  destroys the socket — deferring to other `'upgrade'` listeners or the app's
  own handler. This is required for coexistence: destroying a non-matching
  socket would break a peer library.
- **Unclaimed-by-everyone upgrades leak** per Node semantics (Node does not
  auto-close an upgrade no listener answered). This is the *app's*
  responsibility, documented loudly: add a catch-all
  `httpServer.on('upgrade', (req, sock) => { if (!claimed) sock.destroy() })`
  registered LAST, or accept the leak. BeamSocket cannot safely be that catch-all
  because it cannot know whether another listener will still claim the socket.
- If `path` is omitted, BeamSocket claims **all** upgrades on that server (the
  single-app common case) and *may* then safely 400 malformed upgrades, since it
  is the sole handler by the app's declaration.

### 10.3 authorize + trustProxy (Rule 1 and Rule 3 unaffected)

The request headers are already parsed by Node, so they cross the bridge
**once**, at upgrade, into the same `authorize` round-trip (§7 of RFC 0001-era
design; the 1C `Authorizer`). Rule 1 is unaffected — still one connection-time
JS hop, never per message. `trustProxy` resolves the client IP from the **same
Node-parsed request** (`X-Forwarded-For` header + `socket.remoteAddress` as the
peer), through the identical `Gate`/`ClientIpResolver` used own-port — so the
per-IP limit and `AuthorizeRequest.ip` behave the same behind the same LB
(Rule 3 holds). No new IP-resolution path, no new authorize path.

### 10.4 `io.close()` ↔ `httpServer.close()` lifecycle (both directions)

Two owners, two sockets-worth of lifecycle: BeamSocket owns the **upgraded WS
connections** (post-handoff, Rust); the app's `httpServer` owns the **HTTP
listener + un-upgraded HTTP connections**.

- **`io.close({ timeoutMs })`** (attached mode): (1) **remove BeamSocket's
  `'upgrade'` listener** so no new WS are claimed; a matching-path upgrade that
  races the removal is answered **503** (the 1D `draining` gate, on the HTTP
  layer, before any 101). (2) Drain the Rust-owned WS connections exactly as 1D
  does (1001 sweep → wait `timeoutMs` → force-close → release the bridge). It
  does **not** touch `httpServer` (it doesn't own it).
- **`httpServer.close()`** (app): stops accepting new HTTP connections and ends
  idle HTTP keep-alives, but by Node semantics does **not** close sockets that
  were already upgraded-and-detached — those are Rust's now. So `httpServer.close()`
  **does not drain BeamSocket**; the app must also call `io.close()`.
- **Recommended ordering (documented):** `await io.close()` **then**
  `httpServer.close()` — drain WS first (clients get 1001), then stop the HTTP
  side. The reverse order is safe but leaves WS alive until `io.close()`, so a
  SIGTERM handler should call both, WS first:

  ```ts
  process.on('SIGTERM', async () => {
    await io.close({ timeoutMs: 30_000 }); // drain Rust-owned WS
    httpServer.close();                     // then the HTTP listener
  });
  ```

## 11. Memory / CPU implications per design

| Design | Per-connection memory | Per-message CPU / loop | Rule 1 |
|---|---|---|---|
| **A (fd handoff)** | After handoff, **identical to own-port** — the 1D memory table applies (~11.6 KB/conn, all Rust). Node holds nothing per connection (its socket was destroyed). One-time upgrade cost: a `dup`, a 101 write, a `head` copy (≤ one TCP segment). | Zero Node-loop involvement post-handoff; the data plane is 100% Rust. | ✅ preserved exactly |
| **B (stream proxy)** | **Worst of both:** Node keeps its `net.Socket` (+ its multi-KB per-socket buffers + a JS object) AND Rust keeps a mirror — the per-socket Node cost we exist to eliminate, re-added. | Every byte, both directions, crosses the bridge and touches the event loop — the data path we spent RFC 0001 keeping off it. | ❌ violated |
| **C (A + standalone-port fallback)** | Fallback leg = own-port = the 1D table; A leg = A above. No proxy tier, so no B row in practice. | Same as A on both legs. | ✅ preserved |

This table is the quantitative core of "never ship B": B is strictly worse on
memory *and* CPU *and* the one rule, for the sole benefit of a single port —
which the standalone-port fallback also solves (one LB, two upstream ports)
without touching the data plane.

## 12. De-risking spike — `spike/attach/` (throwaway)

Scope, deliberately minimal (mirrors RFC 0001 §8's throwaway ethos): prove the
**Node-specific** unknowns only — fd `dup` → detach → Tokio adoption → **head
replay** → **echo through an attached Express server**, **Linux + plaintext
only**. No benchmarks, no TLS, no Windows, no rooms/identity — those are proven
already or out of scope. Success criterion: a `ws` client connects through an
Express `http.Server`, and a first frame **coalesced with the upgrade** is
echoed back (proves §8.1 detach + §8.3 replay together).

**Spike status: RUN — PASS (Linux, Node v18.19.1, plaintext).** Results
(`spike/attach/`, throwaway-grade, its own workspace, not in CI):

| Check | Result | Proves |
|---|---|---|
| `run.mjs` T1 — stock `ws` client echo through an attached `http.Server` | **PASS** | §8.1 dup → `socket.destroy()` detach (connection survives) → Tokio adoption → Rust-written 101 → echo |
| `run.mjs` T2 — first WS frame **coalesced with the upgrade** (one TCP write) | **PASS** | §8.2/§8.3 — the frame lands in `head` and is still echoed; the `PrefixedStream` replay loses nothing |
| `churn.mjs` — 300 connect/echo/close cycles | **PASS**, process fd count **Δ 0** | §9 — dup'd fds are released on close; no per-connection fd leak |

Stable across repeated runs. `socket._handle.fd` was accessible; `dup()` +
`socket.destroy()` detached Node without tearing down the TCP connection;
head-coalesced first frames echoed. macOS uses the **identical** POSIX
`dup`/`from_raw_fd` mechanism (expected to hold; not run in this sandbox). This
**removes the pre-implementation "prove the detach sequence" gate** — the
mechanic is validated, not assumed.

> The spike is throwaway by construction (§12 scope): no TLS, no Windows, no
> RAII guard (the RFC specifies `FdHandoffGuard`), no gate/authorize. It exists
> only to de-risk the Node-specific unknown, which it did.

## 13. Decision mapping (if the spike shows X → ship Y)

- **Spike proved** `dup`→detach→adopt + coalesced-head echo on Linux (and, by
  the same POSIX mechanism, macOS) — see §12 → **ship A on Linux + macOS,
  plaintext**, packaged as C; `https.Server` and Windows throw with guidance
  (§6/§7); B is not shipped. **This is the active branch** (spike PASS). Exit
  gate for the *implementation* phase stays as ENGINEERING §9: a runnable
  `examples/express-attach`.
- **Spike shows Node detach is unreliable** (libuv re-reads after `pause()`,
  double-frees on `destroy()`, or `_handle.fd` is unavailable) → **1.1's attach
  story becomes standalone-port-only** (design C with *no* A leg yet), and fd
  handoff is deferred to a follow-up that coordinates with Node/libuv semantics.
  Honest, and still ships a one-LB adoption path.
- **`_handle.fd` access breaks on a future Node** (§10 stability risk) →
  detect at attach and throw early with the standalone-port fallback; the
  handoff never silently degrades.
- **A real single-port + no-fd-handoff need appears later** (e.g. a platform
  that has neither fd handoff nor a spare port) → *only then* revisit B, as an
  opt-in, loudly-documented degraded tier — never a default, and never in 1.1.

## 14. Support matrix + what 1.1 ships vs defers

**Support matrix (platform × transport):**

| Platform | Plaintext `http.Server` attach | `https.Server` attach | Fallback when unsupported |
|---|---|---|---|
| **Linux** | ✅ **fd handoff (design A)** — spike-proven (§12) | ❌ throw (§6) | standalone `listen()` port |
| **macOS** | ✅ fd handoff (same POSIX path) — **(Rider 2) CI-proven required before any release doc claims macOS support** | ❌ throw (§6) | standalone `listen()` port |
| **Windows** | ❌ throw (§7) — fd handoff deferred (`WSADuplicateSocket`) | ❌ throw (§6/§7) | standalone `listen()` port |

The single blessed TLS topology for all platforms: **terminate TLS at the LB /
ingress, attach to a plaintext `http.Server`** (the `trustProxy` deployment
reality, Companion Rule 2). Engine-side TLS (`listen(443, { cert })`, rustls) is
RFC 0003, orthogonal to attach. `❌ throw` always means a clear error naming the
fallback — never a silent degrade.

**Ships (1.1):**
- `new BeamSocket({ server: httpServer })` plaintext attach via **fd handoff**
  on **Linux + macOS**.
- `path` filtering + coexistence with other `'upgrade'` listeners (§10.2).
- `authorize` + `trustProxy` from the Node-parsed request (§10.3).
- `io.close()` ↔ `httpServer.close()` lifecycle + docs (§10.4).
- Windows + `https.Server`: **clear throw** pointing at the standalone-port /
  TLS-at-LB fallbacks (§6/§7).
- `examples/express-attach` (the ENGINEERING §9 exit gate).
- `FdHandoffGuard` + the §8.4 / §9 tests.

**Defers (named, not omitted):**
- Engine-side TLS termination (`listen(443, { cert })`, rustls) → **RFC 0003**.
- Windows fd handoff (`WSADuplicateSocket`) → follow-up, own spike.
- Stream-proxy tier (design B) → only if a concrete future need forces it.
- Fastify/`uWS`-style servers beyond the generic `http.Server` `'upgrade'`
  contract → they already expose `http.Server`; validated in the impl phase.

## 15. Future extensibility — does the handoff seam serve QUIC/HTTP-3?

**Neither serves nor fights it — they are different acquisition paths, and that
is fine.** fd handoff is intrinsically about adopting a **TCP** stream Node
accepted. QUIC/HTTP-3 is UDP-multiplexed; there is no "one connection = one fd"
to hand off, and Node does not own QUIC connections the way it owns TCP upgrades.
QUIC therefore arrives the way ARCHITECTURE §8 already says — the engine's own
`quinn` listener on the Tokio runtime — **not** through attach.

What *does* generalize is the seam this RFC deliberately reuses: the `Transport`
trait operates on **frames + connection IDs**, and `adopt` converges on the same
`Accepted → setup_connection` path as `accept`. So however a connection is
acquired (own-port TCP, attached TCP, future QUIC), rooms/identity/presence/
broadcast/metrics/`close()` work unchanged. The extensibility lives in the
*post-acquisition* seam, not in fd handoff — which is exactly why keeping fd
handoff a thin, self-contained adapter (not a new data path) matters.

## 16. Risks and tradeoffs (summary)

| Risk | Severity | Mitigation |
|---|---|---|
| `socket._handle.fd` is a private Node API | High (correctness) | Detect availability at attach; throw early → standalone-port fallback; never silently degrade. Track Node's public fd-detach discussions. |
| Head-byte loss (silent) | High | §8.3 `PrefixedStream` replay + the coalesced-first-frame test as a hard gate (§8.4). |
| fd leak / double-close | High (corruption) | `FdHandoffGuard` RAII (§9), disjoint Rust/Node fd ownership, churn/leak test. |
| TLS users expect `https.Server` attach | Medium (adoption) | Clear throw + blessed TLS-at-LB topology; engine-side TLS as RFC 0003. |
| Windows users expect `{ server }` | Medium (adoption) | Clear throw + standalone-port fallback; Windows fd handoff as a follow-up. |
| Unclaimed upgrades leak sockets | Medium | Documented app-owned catch-all; sole-handler mode may 400 (§10.2). |
| `httpServer.close()` doesn't drain WS | Medium (ops surprise) | Documented lifecycle ordering + SIGTERM recipe (§10.4). |

**Tradeoffs accepted:** attach reaches into a private Node field (mitigated by
early detection); TLS and Windows get honest fallbacks rather than fragile
support in 1.1; we ship a mechanism (A) whose fallback (standalone port) costs a
second upstream port rather than shipping a data-plane-violating proxy to avoid
that port.

---

**Reviewer asks:** (1) Is plaintext-only attach + TLS-at-LB an acceptable 1.1
TLS story, with engine-side rustls deferred to RFC 0003? (2) Is Windows
standalone-port-fallback an acceptable "not yet"? (3) Does the `adopt` seam (a
second producer into `setup_connection`) match the intended
transport/engine boundary? On acceptance, this RFC freezes and the
implementation work order follows.
