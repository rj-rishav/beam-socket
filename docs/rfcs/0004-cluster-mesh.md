# RFC 0004 — Cluster Mesh (Phase 3)

**Status:** FROZEN — architect review complete (three §4.4 hardening fixes
folded in: sender-suppression rule, pinned/direction-bound handshake
transcript, UDP probe-only with frozen format). **Conditional on the
real-hardware 30-minute loaded soak** (named in `0004-results.md`), same
precedent as 1.1's macOS CI-gated row: implementation may proceed; no release
claims cluster support until the soak is green.
**Gate scope:** Phase 3 implementation follows the 3A–3D work orders
(ENGINEERING §13); sub-phase gates close sequentially as always.
**Depends on:** ARCHITECTURE.md §6 (the three seams), the 1C delivery-semantics
note (ARCHITECTURE §4), ENGINEERING §1 + §12 rules (they apply across nodes),
RFC 0001/0002 discipline (pre-registered predictions, hard gates, honest
decision mapping).

> The problem, precisely: **N BeamSocket nodes behind one load balancer must
> behave like one runtime for the targeting verbs** — `toRoom`, `toUser`,
> `broadcast`, and `toSocket` reach members on every node — with **zero SDK API
> change**. `cluster: { listen, seeds, secret }` in config is the only new
> surface. Phase 3 is mesh + membership + routing + inter-node messaging.
> Distributed presence/identity **state** is Phase 4; this RFC leaves its seam
> named and otherwise does not design it.

---

## 1. Why this is the critical unknown

Phase 3 is the biggest design since the bridge, and it is risky for the same
structural reason RFC 0001 was: the interesting failures cannot be unit-tested.
Membership convergence, false-positive eviction under CPU load, relay-hop
latency, and partition/heal dynamics only exist with real processes, real
sockets, and real timers in the loop. Three properties make it the
highest-risk component since the bridge:

1. **Its failure modes are emergent.** A single node's correctness is provable
   with `cargo test`; a mesh's liveness under load, loss, and partial failure
   is not. The spike exists to observe these dynamics before any production
   line is written.
2. **Its semantics are API semantics.** What a partition does to `toRoom` is
   observable app behavior we must document and commit to — exactly like the
   bridge's saturation policy in RFC 0001 §1. Building routing before
   membership semantics are fixed means building on delivery semantics we
   haven't validated (again).
3. **The wire protocol is forever.** A single node deploys atomically; a
   cluster does not. The first shipped frame layout must already carry the
   version-negotiation story for every future layout, or rolling deploys
   corrupt state. This is where most real-world clustering pain lives (§5).

## 2. What Phase 3 is NOT (scope fence, stated first)

- **Not distributed presence/identity state.** `io.presence(room).list()`
  cluster-wide, cross-node identity metadata, CRDTs — Phase 4. This RFC only
  names the seam Phase 4 plugs into (§4.6, §12).
- **Not consensus.** No Raft, no etcd/ZooKeeper, no external coordinator —
  the infrastructure complexity this project exists to eliminate. Membership
  needs eventual agreement, not linearizability (§4.2).
- **Not stronger delivery semantics.** The 1C note stands verbatim: *frame
  delivery to a live socket, not message delivery — no acknowledgements, no
  retries, no persistence, no cross-node ordering guarantees.* Every cross-node
  statement in this RFC is equal to or weaker than single-node semantics,
  never stronger — not even accidentally (§9).
- **Not hub/routed/hierarchical topologies** — Phase-5-era problems (§4.1).
- **Not engine-side TLS.** mTLS on mesh links rides RFC 0003; the seam is
  named in §4.7.

## 3. The three §6 seams, exercised precisely

ARCHITECTURE §6 planted three seams for exactly this phase. The RFC must be
honest about what each seam looks like in the code *today* versus the
architecture doc's shorthand:

| Seam (§6) | In the code today | How Phase 3 exercises it |
|---|---|---|
| 1. "Trait-shaped registries" | Only `PresenceStore` is literally a trait (`presence.rs`). `RoomRegistry`, `IdentityRegistry`, and `broadcast()` are concrete — but private to the `Engine` facade; nothing outside `engine.rs` touches them | **The registries stay local and untouched.** Clustering does NOT swap in a distributed `RoomRegistry` — local membership remains the local node's truth (that discipline is what makes partitions survivable, §4.8). The mesh is *additive*: an interest table + relay component beside the registries, integrated at the `Engine` facade (the same seam 2A/2B used). `PresenceStore`'s trait is the named **Phase 4** seam and is deliberately not implemented here |
| 2. "Opaque, prefix-able IDs" | `ConnectionId` = `u64` `[shard:8][gen:24][key:32]` (`ids.rs`); public form is an opaque string minted by the SDK's private codec (`ids.ts`, documented "the encoding changes in Phase 3") | Cashes in: IDs gain a **node prefix** (§4.5). `toSocket(id)` routes to the owning node. Zero SDK breakage because apps only ever saw opaque strings — this is the payoff for that 1A decision |
| 3. "Message-passing internals" | Components talk via typed events and bounded channels (engine→bridge events, control commands); no shared method calls across components | The mesh is a peer of the bridge: engine↔mesh communication is bounded channels carrying typed events (membership deltas → interest publisher; inbound relay frames → local fan-out). A channel crossing a process boundary is an implementation detail — which is precisely what the relay link is (§4.3, §4.6) |

Rules that carry across nodes unchanged (ENGINEERING §1, §12): no per-message
JS (relay is Rust→Rust; a relayed broadcast enters JS only where a local
broadcast would); no global lock on any hot path (the interest table is
sharded); every queue bounded with counted overflow (§4.6); every new
per-connection/per-room/per-peer cost stated (§8); diagnostics free when
unused, bounded when used (§7).

---

## 4. The eight decisions

### 4.1 Topology — full TCP mesh, N ≤ 50 (hard envelope)

**Decision (prior confirmed): full mesh.** Every node holds one TCP link to
every other live node: N−1 links, N·(N−1)/2 total. **The design envelope is
N ≤ 50 nodes, stated as a hard assumption** — at 50 nodes that is 49 links and
~1,225 TCP connections cluster-wide, trivial for machines that each hold 100k+
WebSockets. Real deployments scale *up* per node (that is the whole product
thesis: 500k connections per process); a 500-node BeamSocket cluster is a
deployment smell, not a target. Hub/routed/spanning-tree topologies are
Phase-5-era problems and are not designed here.

Full mesh buys: exactly **one relay hop** for every targeted message (§4.3's
latency budget depends on this), no routing tables beyond "which peer hosts
interest", no transit nodes to fail, and a trivially correct flood fallback.

Link establishment: the lower node-id dials, the higher accepts (one link per
pair, no simultaneous-connect glare). Reconnection with jittered exponential
backoff (250 ms → 15 s cap) while the peer is alive-or-suspect in membership;
evicted peers get no reconnect attempts until membership re-admits them.

### 4.2 Membership — SWIM-style, no consensus

**Decision (prior confirmed): SWIM-style probe/ack + piggybacked gossip
dissemination + indirect probes + suspicion before eviction. No consensus
protocol.** Membership needs eventual agreement — every node eventually knows
who is in the mesh — not linearizability. Nothing in Phase 3 needs a total
order on membership events: routing degrades to "some node briefly missed" and
recovers by re-gossip, which is exactly the guarantee class the delivery
semantics already promise (§9). Raft/etcd here would be premature complexity
(master vision §1) and would add the one thing this design refuses: a
coordinator whose unavailability stops the data plane.

Mechanics (standard SWIM, stated so the spike can measure it):

- **Probe cycle:** every protocol period `T`, each node probes one random
  member (round-robin shuffled). No ack within `probe_timeout` → ask `k`
  random peers for an **indirect probe**. Still nothing → mark **suspect**.
- **Suspicion:** a suspect is announced (piggybacked), not evicted. The
  accused node, on hearing its own suspicion, **refutes with a bumped
  incarnation number** — the anti-false-positive valve. No refutation within
  `suspicion_timeout` → **dead** (evicted), announced.
- **Dissemination:** membership updates (join/alive/suspect/dead) piggyback on
  probe/ack traffic, each update relayed up to `λ·log₂(N+1)` times — no
  separate broadcast channel, cost is bounded by the probe cycle.
- **Join:** a joining node contacts any seed and does a **push-pull
  full-state sync** (the anti-entropy shortcut that makes cold-start
  convergence fast), then enters the probe cycle. `seeds` is a static list in
  config — a bootstrap hint, not a coordinator; any live member works. A
  low-rate re-seed contact continues forever (it is the partition-heal path,
  §4.8). **The push half is load-bearing, not an optimization** (spike
  finding #2, `0004-results.md`): the contacted node must see the joiner's
  claims about *it* — including "you are dead" — to trigger its own
  refutation; a pull-only join heals one side of a partition and leaves the
  other permanently stuck behind equal-incarnation Dead-beats-Alive
  precedence.
- **Rejoin/flapping:** a node declared dead that was merely partitioned
  re-enters by re-contacting seeds with a higher incarnation; `alive` with a
  newer incarnation revives a dead entry (heal path, §4.8). Every state entry
  carries the incarnation, so stale gossip cannot resurrect stale state.

**Parameters are the tuning surface, pre-registered as the risk (P1):**

| Parameter | Literature default (memberlist-ish) | Tuned prior for Tokio nodes |
|---|---|---|
| Protocol period `T` | 1 s | 500 ms |
| Probe timeout | 500 ms | 250 ms |
| Indirect probes `k` | 3 | 3 |
| Suspicion timeout | 4·T·log(N) ≈ 4–6 s @ N=5 | 2·T·log(N) ≈ 2–3 s @ N=5 |

P1's claim: Tokio nodes don't GC-pause, so the literature's generous timeouts
(sized for GC-pausing runtimes) can tighten substantially before false
positives appear — the spike measures kill-detection latency AND
false-positive rate under CPU load at both rows. If the spike shows false
positives at the tuned row, the lever is a Lifeguard-style local-health
multiplier (slow my own timeouts when I'm the unhealthy one), not a return to
consensus.

### 4.3 Routing — interest-advertised relay; flood is the lever, not the design

**Decision (prior confirmed): interest-advertised relay.** Each node
advertises which **rooms** and **users** it currently hosts (≥1 local member /
≥1 local device); each node holds an **interest table**: `room → peer set`,
`user → peer set`. A targeted message relays only to interested peers.

- **`toRoom(r)`:** local fan-out (existing 1B path, untouched) + one relay
  frame to each peer in `interest[r]`. Receiving peer does local fan-out only
  — **relays never re-relay** (full mesh = one hop, no loops, no TTL needed).
- **`toUser(u)`:** identical shape over the user interest set.
- **`broadcast()`:** relay to all live peers (interest is definitionally
  "everyone"), local fan-out on each.
- **`toSocket(id)`:** no interest table involved — the ID itself names the
  owning node (§4.5); route directly to that peer or fan out locally.

**Interest maintenance is edge-triggered and bounded:** only the 0→1 and 1→0
transitions of "do I host room r / user u" publish an update — join #2..#n of
the same room on the same node are invisible to the mesh (this is what keeps
interest chatter independent of per-room membership churn). Updates ride
small control frames (§4.4) with sequence numbers per (origin, kind); a
periodic **anti-entropy digest** (hash of the sorted interest set, compared
lazily; full resync on mismatch) repairs anything a dropped control frame or
partition left stale. On peer eviction, its interest entries are swept; on
link re-establishment, full interest exchange (same shape as the membership
push-pull).

**The 1B one-allocation invariant survives the hop, stated precisely:** a
cross-node `toRoom` serializes the payload **once** into one refcounted
`Bytes`; every peer link's bounded queue holds a refcount clone of that one
allocation (same discipline as local mailboxes); the receiving node
deserializes into one `Bytes` and its local fan-out refcount-clones as today.
Cost per cross-node broadcast: **one serialization + one wire write per
interested peer + one deserialization per receiving node** — never
per-recipient serialization.

**Full-flood is the fallback lever, not the design:** relay-to-all-peers
(ignore the interest table) remains available as a config/debug switch and as
the automatic degraded mode while a link's interest state is resyncing. P3
pre-registers the claim that interest beats flood by >5× on inter-node bytes
at the spike's cell; if the spike refutes it at realistic membership spreads,
the decision mapping (§14) says what ships.

### 4.4 Wire protocol — length-prefixed binary, versioned handshake

This section gets §6-of-RFC-0002 seriousness because a mixed-version cluster
during a rolling deploy must **interoperate or refuse loudly — never corrupt.**

**Framing:** every frame is `[len: u32 LE][kind: u8][flags: u8][body]`, `len`
covering kind+flags+body. Max frame size is a handshake-declared constant
(default 16 MB, must exceed `maxPayloadBytes` + envelope). A reader that sees
`len > negotiated max` closes the link (protocol error) — no resync
heuristics on a corrupted stream.

**Frame catalog (initial):**

| kind | name | plane | body (sketch) |
|---|---|---|---|
| 0x01 | HELLO | control | magic `BSMH`, protocol_version u16, node_id u16, cluster_name, incarnation, max_frame, feature bits (reserved u32) |
| 0x02 | CHALLENGE | control | 32-byte nonce |
| 0x03 | AUTH | control | HMAC-SHA256(secret, nonce ‖ transcript) |
| 0x04 | MEMBERSHIP | control | piggybacked SWIM updates (TCP links ONLY — UDP is probe-only, see the frozen-format rule below) |
| 0x05 | INTEREST | control | edge-triggered add/remove entries + per-origin seq |
| 0x06 | INTEREST_DIGEST | control | anti-entropy hash |
| 0x07 | RELAY_ROOM | data | room, origin node, payload (text/binary flag in `flags`) |
| 0x08 | RELAY_USER | data | user, origin node, payload |
| 0x09 | RELAY_ALL | data | payload |
| 0x0A | RELAY_SOCKET | data | target ConnectionId (wire form, §4.5), payload |
| 0x0B | PING/PONG | control | liveness on idle TCP links (distinct from SWIM UDP probes) |

**Version negotiation (the P4 section):**

- One `protocol_version: u16` in HELLO, bumped **only** for incompatible
  changes. Additive changes ride **feature bits**.
- **Compatibility promise: version N interoperates with N−1** (one-step
  rolling deploys are the supported path; skipping versions in one deploy is
  not). The link speaks `min(local, remote)`; a node seeing a version outside
  `{N, N−1}` **refuses the link with an explicit LOGGED error** naming both
  versions — visible in metrics as a distinct link-state, never a silent
  retry loop.
- **Sender-suppression rule (review hit 1 — the load-bearing rule):** a node
  **never emits** a kind or feature the peer did not advertise. Feature bits
  are an **intersection**: usable only when BOTH sides set them, and a feature
  bit may gate *which frames exist*, never *how an existing frame parses*.
  Corollary: **new data-plane kinds (`RELAY_*`) are never additive** — they
  are feature-gated or version-bumped, no third option. A skipped relay frame
  is a silently lost message; we do not design message loss into deploys.
- **Unknown-kind rule (demoted to defense-in-depth):** frames are
  self-delimiting, so an unknown `kind` is skipped and counted
  (`unknownFrames` metric). Under sender suppression this counter should read
  **zero**; a nonzero value is a bug detector, not a compatibility mechanism.
- **Body evolution rule:** existing frame bodies are append-only within a
  protocol version; readers must tolerate longer-than-known bodies (trailing
  bytes ignored). Any change that can't obey that rule bumps the version.
  (HELLO itself is append-only, which is also how the feature-bit space
  extends past the initial u32.)
- The cluster name in HELLO partitions accidental cross-cluster joins
  (staging node dialing prod refuses at HELLO, before auth).
- **UDP is probe-only and its format is frozen (review hit 3):** the N/N−1
  promise is negotiated on TCP links, so SWIM UDP packets carry **no
  negotiated context**. Therefore: UDP carries PING/ACK/PING-REQ probes
  **only** — membership *dissemination* (MEMBERSHIP frames) flows exclusively
  over negotiated TCP links. The UDP probe packet format is version-stamped,
  append-only, and **frozen forever** at ship; any probe evolution that can't
  be append-only moves probing onto TCP rather than breaking the frozen
  format.

### 4.5 IDs — the opaque-ID seam cashes in

Today: `ConnectionId` is `u64` `[shard:8][gen:24][key:32]`, fully packed — the
node prefix does **not** fit in spare bits. The public form was always an
opaque string minted by the SDK's private codec (`ids.ts`), documented as
"the encoding changes in Phase 3." That is the payoff: **the internal encoding
changes; no app breaks.**

**Decision: cluster-scoped IDs are `(node_id: u16, local: u64)`.**

- **Wire form:** 10 bytes, `node_id` + the existing u64.
- **String form (SDK codec):** `<node36>-<hi36>-<lo36>` — three base-36
  segments where today there are two. `decodeSocketId` (already `@internal`,
  already returns `null` for foreign shapes) learns the third segment;
  `socket.id` remains an opaque string to apps. Single-node mode keeps the
  two-segment form — IDs stay byte-identical for non-clustered users, and the
  codec accepts both (a two-segment ID is "this node").
- **Local fast path unchanged:** the u64 half is the existing slab id; a
  local send does exactly what it does today (the node check is one compare).
- **`toSocket(id)` cross-node:** decode → `node_id == mine` → local path;
  else RELAY_SOCKET to that peer (one hop). Unknown/evicted node or stale
  generation on the owning node → the send **misses silently** — the exact
  single-node semantics for a stale id today (Rule: never stronger, §9).
- **FFI note (production-phase detail, not spike):** the binding's two-u32
  convention gains the u16 node id as a third primitive where needed; the
  bridge's flat encoding grows the origin field behind a feature bit. No
  BigInt, no string parsing on the hot path — same rationale as 1A.
- **`node_id` assignment:** operator-assigned in config is the default
  (`cluster.nodeId`), unique within the mesh, refused at HELLO on collision
  (two nodes claiming one id is a config error, loudly fatal — not
  auto-resolved). Auto-assignment is deliberately NOT designed (it wants
  consensus; see §2). 16 bits ≫ the N ≤ 50 envelope; the width is for
  id-reuse hygiene across node replacements, not for cluster size.

### 4.6 Inter-node backpressure — Rule 5 across the wire

**Every peer link is a bounded queue with drop-and-count overflow and a
per-peer pressure gauge. A slow peer NEVER blocks local delivery** — the RFC
0001 philosophy, now per-link.

- Each peer link owns a bounded outbound queue (data plane), default sized in
  bytes (HWM semantics like 1A mailboxes, not frame counts — a 64 KB frame
  must not count as one 64 B frame). Overflow policy: **drop-newest + count**
  (`relayDrops` per peer). The local fan-out for the same message proceeds
  regardless — a relayed broadcast is local-first, relay-best-effort.
- **The link writer coalesces: one write per wakeup, draining everything
  queued (capped), never one write per frame.** Required, not an optimization
  (spike finding #1, `0004-results.md`): per-frame writes measured 3.8 ms p99
  at 100k msgs/s; coalescing cut it 5.5× to 680 µs. The RFC 0001 lesson one
  layer down — the syscall, not the byte, is the expensive unit.
- **Control/data separation (the events.rs lesson):** MEMBERSHIP, INTEREST,
  and auth frames go through a small **lossless control queue** that
  back-pressures the *mesh's own control tasks* (exactly like
  `events.control()` awaits), never the data path and never the JS thread.
  Data frames shed; control frames wait. A link whose control queue is
  persistently full is a dying link and feeds suspicion.
- **Per-peer gauge in `stats()`:** `cluster.peers[]` with
  `{ nodeId, state, pressure (0..1), relayDrops, bytesIn/Out, msgsIn/Out }` —
  the 2A discipline (bounded output: N ≤ 50 peers is naturally bounded).
  Aggregates (`relayBytesOut`, `relayDrops` totals) join `metricsText()`.
- Slow-peer containment mirrors slow-socket containment: the peer's queue
  fills, its drops count, its pressure gauge rises, everyone else — including
  every local socket — is untouched. The spike's relay cell runs with one
  artificially slowed peer to demonstrate exactly this.

### 4.7 Security — cluster secret, HMAC challenge; mTLS is RFC 0003's seam

**An unauthenticated mesh port is an open relay — not shippable even in
alpha.** Phase 3 ships secret-based mutual authentication:

- `cluster.secret` (config; same value on every node). Mesh join runs a
  **mutual HMAC-SHA256 challenge-response**: HELLO → CHALLENGE(nonce) →
  AUTH(HMAC(secret, nonce ‖ handshake transcript)) in **both directions**
  before any other frame is accepted. The secret itself never crosses the wire.
- **Transcript pinned + direction bound (review hit 2):** "transcript" means
  **both HELLO frame bodies, bit-exact as received** — covering
  protocol_version, feature bits, node ids, cluster name, and max_frame — so
  any MITM tampering with negotiation (version/feature downgrade) breaks the
  MAC. Each direction's MAC additionally includes a **distinct role label**
  (`"bsmh-initiator"` / `"bsmh-responder"`) and the responder's fresh nonce,
  so an attacker cannot reflect a node's own AUTH back at it. Both properties
  get their own tests: a downgrade-tamper test and a reflection test, in the
  implementation phase's required list.
- SWIM UDP packets carry an HMAC tag over their body (cheap, per-packet) —
  membership is an attack surface too (a forged `dead` is a remote kick).
  Replay hardening: tag covers (incarnation, seq), stale seq ignored.
- Failed auth: link closed, counted (`authFailures`), logged with peer addr;
  repeated failures get backoff, not retry storms.
- **What this does NOT provide, stated plainly:** confidentiality and
  transport integrity. Mesh traffic is cleartext TCP/UDP — payloads included.
  Deployments must treat the mesh network as trusted (private VPC/VLAN) until
  **mTLS on mesh links, which rides RFC 0003** (engine-side TLS) behind the
  same seam the WebSocket transport uses: the link IO is a `FrameSink/Source`
  pair over a pluggable stream, so TLS wraps the stream without touching
  framing, auth, or routing. That seam is named here; RFC 0003 designs it.

### 4.8 Partitions — islands, honest guarantees, heal by re-gossip

No consensus → a partition does not stop the world and does not elect anyone:
**each side becomes an island that operates independently** (local sockets are
served; intra-island relay works; the other side's nodes get suspected, then
evicted; their interest entries sweep). **Heal is re-gossip**, not
reconciliation: links re-establish, rejoined nodes refute with bumped
incarnations, push-pull resyncs membership, full interest exchange resyncs
routing. There is no cross-island state to merge **because Phase 3 keeps no
distributed state** — membership and interest are both regenerable from each
node's local truth (this is the §4.2/§3 discipline paying off; Phase 4's
distributed presence is exactly the point where heal stops being free, which
is why it is a separate phase).

**Guarantees, in the 1C currency** (each equal to or weaker than single-node;
none stronger — the fence in §2):

| Property | Single node (1C, verbatim class) | Cross-node (Phase 3) |
|---|---|---|
| Delivery unit | Frame delivery to a live socket | Same — relay then local frame delivery |
| Multiplicity | At-most-once (drops counted) | **At-most-once** (one hop, no retry, no re-relay; drops counted at whichever queue shed) |
| Ordering | Per-socket FIFO from one sender; nothing else promised | **No cross-node ordering** — two messages relayed via different links may interleave arbitrarily. Per-(origin-link, socket) order follows from TCP + FIFO queues but is NOT promised, so it cannot harden into API |
| During partition | n/a | **No delivery to unreachable nodes.** Frames for evicted/unreachable peers are dropped and counted (`relayDrops`); `send()` still means "accepted into a queue", nothing more |
| On heal | n/a | **No replay.** Missed frames stay missed (at-most-once). Membership + interest converge; future frames flow |
| Failure visibility | Metrics (Rule 5) | Same: per-peer drops, membership state transitions, island size all observable in `stats()` |

Split-brain honesty: during a partition, `toRoom('r')` on island A reaches
A's members only, and A cannot know B's members exist. That is the documented
behavior, in exactly these terms — apps needing stronger guarantees need a
different product layer (acks/persistence), which remains an explicit
non-goal.

---

## 5. Config surface (the only SDK change)

```ts
new BeamSocket({
  cluster: {
    listen: '0.0.0.0:7946',        // mesh bind (TCP link + UDP swim, same port number)
    seeds: ['10.0.0.11:7946'],     // any live member(s); bootstrap hint, not a coordinator
    secret: process.env.MESH_SECRET, // required — no secret, no cluster (§4.7)
    nodeId: 3,                     // operator-assigned u16, unique (§4.5)
    // advanced (defaults from the spike): probe/suspicion tuning, queue HWMs,
    // floodFallback: boolean
  },
});
```

Zero new verbs, zero changed signatures, zero changed events. `stats()` gains
a `cluster` section (absent when not clustered). Everything else — including
`socket.id`'s string shape in single-node mode — is byte-identical.

## 6. Pre-registered predictions (the results doc must confront each)

| # | Prediction |
|---|---|
| P1 | SWIM false-positive evictions under CPU load will be the tuning sink, not throughput — Tokio nodes don't GC-pause, so literature defaults will prove **too aggressive** (tightenable), not too lax |
| P2 | A Rust→Rust relay hop adds **< 1 ms p99 at 100k msgs/s** — inter-node throughput will NOT be the bottleneck; the local bridge stays the narrowest point |
| P3 | Interest-advertised routing beats full-flood by **> 5×** on inter-node bytes at 50 rooms/node with 10% cross-node membership |
| P4 | The version-negotiation design (§4.4) will be the section reviewers change most — it always is |

## 7. De-risking spike — `spike/mesh/` (throwaway)

Standalone mesh library spike: 3–5 local nodes as separate OS processes on
loopback, a coordinator process driving scenarios and writing JSON to
`spike/mesh/results/`. **NO integration with the real engine, SDK, or bridge**
— pure mesh dynamics, so every number is the mesh's number (the RFC 0001
isolation rule). Fault injection is socket-level (a per-node deny-set filter
on the UDP receive path + forced-close/refuse on TCP links), since the sandbox
has no iptables/netns.

Scenarios (each maps to a gate):

1. **Converge:** 5-node cold start (staggered spawn) → time until every node's
   membership view is exactly the full set.
2. **Kill:** steady 5-node mesh → `kill -9` one node → time from kill to every
   survivor marking it dead. Run at literature-default AND tuned SWIM rows
   (§4.2 table).
3. **Soak (P1):** 5 nodes, sustained CPU load on every node + relay traffic,
   N minutes → count false-positive evictions (any eviction of a live
   process; refutation events counted separately as near-misses). Gate
   duration note: the RFC demands 30 min; this sandbox caps command runtimes,
   so the soak runs as accumulated foreground chunks with the harness
   supporting the full duration on real hardware (`--soak-seconds 1800`) —
   the RFC 0001 gate-duration precedent, stated honestly in results.
4. **Relay cell (P2):** one hop A→B at 100k msgs/s, 64 B and 512 B payloads →
   added latency p50/p99 (hop timestamp delta), sustained throughput, plus a
   slow-peer cell proving drop-and-count containment (§4.6).
5. **Flood vs interest (P3):** 50 rooms/node, 10% cross-node membership,
   fixed message script → inter-node bytes under interest routing vs flood.
6. **Partition/heal:** deny-set split 2/3 → assert two stable islands
   (eviction on both sides, no reconnect storms) → heal → time to full
   re-convergence, assert **zero stuck entries** (no permanent suspect/dead,
   interest tables identical to a fresh boot's).

## 8. Memory / CPU cost statement (Rule 4, per §12 discipline)

Per peer: one TCP link, bounded queues (HWM default 1 MB data + 64 KB
control), membership entry (~64 B), reconnect state — **O(N) with N ≤ 50**.
Per room/user with local interest: one interest-table entry on each interested
peer (~room-name + 16 B) — bounded by (rooms hosted) × (peers), swept on
eviction and on 1→0 transitions. Per connection: **zero new bytes** (the node
prefix lives in the ID value, not in state). Per message: zero new JS
crossings; one serialize + refcounted clones (§4.3).

## 9. Hard gates (freeze blockers)

- [ ] Convergence: 5-node cold start **< 2 s**; kill detection **< 5 s**; false-positive rate **zero** across a 30-min loaded soak (sandbox: accumulated chunks + full-duration flag, per §7.3)
- [ ] Relay: single hop adds **< 1 ms p99** at the spike's measured cell
- [ ] Partition heals with **zero stuck membership entries**
- [ ] Every §4 decision answered above — none deferred-by-omission (§2 lists what is deferred *explicitly*, with its owning phase/RFC)
- [ ] Predictions P1–P4 confronted in `0004-results.md`

## 10. Decision mapping (if the spike shows X → ship Y)

- Tuned SWIM row shows false positives under load → ship literature row as
  default + Lifeguard-style local-health multiplier as the follow-up knob;
  gate stays (zero FPs at whatever row ships).
- Kill detection > 5 s at the shipped row → tighten suspicion only with a
  measured FP margin; if FP-vs-latency cannot meet both gates simultaneously,
  **the RFC does not freeze** — the membership section is redesigned (e.g.
  TCP-RST link hints feeding suspicion) and re-spiked.
- Relay hop ≥ 1 ms p99 → link writer redesign (batch/coalesce frames per
  flush, the bridge lesson) and re-measure before freeze; if still ≥ 1 ms,
  the envelope constraint was wrong and the RFC re-opens P2 honestly.
- Interest < 5× vs flood at the P3 cell → interest ships only if it still wins
  at ≥ 2× with sub-linear table cost; otherwise flood becomes the Phase 3
  default (N ≤ 50 makes flood viable) and interest moves to the Phase 5
  topology RFC. The SDK surface is identical either way.
- Partition leaves stuck entries → incarnation/sweep logic is wrong; fix and
  re-run scenario 6 — this gate has no soft-pass.

## 11. Risks and tradeoffs

| Risk | Severity | Mitigation |
|---|---|---|
| False-positive eviction under load flaps membership and sweeps interest (thundering resync) | High | Suspicion-before-eviction + refutation (§4.2); soak gate with FP=0; resync is per-link full exchange, bounded by interest size |
| Version negotiation designed wrong (the P4 risk) | High | N/N−1 promise + feature bits + unknown-kind rule + append-only bodies (§4.4); reviewers explicitly invited to attack this section |
| Slow peer starves the mesh | Medium | Per-link bounded queues, drop-and-count, control/data separation (§4.6); slow-peer spike cell |
| Cleartext mesh traffic mis-deployed on untrusted networks | Medium | Loud docs (§4.7); HMAC auth ships in 3; mTLS seam named for RFC 0003 |
| Interest table staleness routes to a peer with zero members | Low | Harmless (receiving fan-out finds nobody — same as local empty room); anti-entropy digest repairs; at-most-once unaffected |
| `node_id` collision via config error | Medium | Refused loudly at HELLO (§4.5); never auto-resolved |
| UDP swim traffic dropped by environment (some fabrics) | Medium | SWIM-over-TCP fallback is a feature bit reserved in HELLO; not built until observed |

## 12. What this deliberately ignores

Distributed presence/identity state and CRDTs (Phase 4 — seam: `PresenceStore`
trait + reserved mesh frame kinds), cross-node ordering or delivery upgrades
(explicit non-goal, §9), hub/routed topologies and N > 50 (Phase 5), engine
TLS/mTLS (RFC 0003), sticky-session/LB strategies (deployment docs),
auto-scaling integration, and any external coordination service (forever).
