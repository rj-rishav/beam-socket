# I shipped a Rust networking runtime to npm — solo, with AI as my co-architect. Here's the honest version.

`npm install beamsocket@alpha`

BeamSocket is a high-performance networking runtime for Node.js: a Rust engine that owns every socket, with a plain-JavaScript API on top. You write JS; connections, rooms, broadcasts, presence, backpressure, and a clustering mesh all run in Rust, off the event loop. It's an alpha. It's real, it's on npm, and this post is the honest account of building and shipping it — including the parts that didn't go the way I expected and the benchmark where it loses.

## The one bet everything rests on

There's a single rule the whole design serves: **per-message work never crosses the Rust↔JavaScript boundary unless your app subscribed to that event.** Broadcasts, room fan-out, keepalive, backpressure — all of it happens in Rust. JavaScript runs only when your business logic needs to.

That sounds obvious. It has a sharp consequence: it means paying a fixed cost — an FFI hop and a bit of message batching — on the messages that *do* reach your handlers, in exchange for keeping the event loop free during large fan-outs and packing connections far more tightly than a pure-JS server. It's a trade, and the interesting question is whether the trade pays off. Which brings me to the part I'm proudest of.

## We wrote our predictions down before we measured

The riskiest component in the whole system is the bridge — the thing that carries events from Rust into JavaScript. It's the one piece you can't benchmark in pure Rust; it only exists with the full Node + V8 + native stack in the loop. So before writing it, I wrote an RFC that named four candidate designs and, crucially, **pre-registered a prediction for each one** — on paper, before any code ran.

The predicted winner was a simple batched-objects design. We built it. We measured it.

It lost. By about 4x.

A different design — encoding events into one flat buffer per flush, decoded by a cursor reader with zero per-message allocation — won decisively and became what ships. If I'd trusted my gut instead of the gate, the entire runtime would sit on a foundation the data had already rejected. Writing the prediction down first is what made it impossible to quietly rationalize the loss away. I'd recommend the practice to anyone: it turns "I was wrong" from an ego event into a logged result.

## The benchmark — including where it loses

I benchmarked BeamSocket against the incumbents — `ws`, Socket.IO, and uWebSockets.js — on one box, one Node version for all four, same driver, deflate off, two runs each. Here's the honest scorecard at the scale I could actually test (a few thousand connections on a shared 4-core box):

- **Echo latency (low concurrency):** uWS 0.88ms, ws 0.95ms, **BeamSocket 2.32ms**, Socket.IO 3.11ms. BeamSocket is ~2.4x slower than the raw transports here — and it's the design's own fault, by design. At 50 connections there's nothing to batch, so a message mostly waits out the flush timer. The batching that costs latency here is the same batching that survives overload at scale.
- **Echo throughput (low concurrency):** ws 176k/s, uWS 168k/s, **BeamSocket 75k/s**, Socket.IO 48k/s. Same cause — the per-message FFI hop dominates when there's no fan-out to amortize it over.
- **Memory per connection:** uWS ~0 (below measurement granularity — it's untouchable here), **BeamSocket ~550 bytes**, ws ~2.3KB, Socket.IO ~9.4KB. BeamSocket is second, ~4x denser than ws and ~17x denser than Socket.IO.
- **vs Socket.IO specifically** — its closest peer in features (rooms, presence, identity): **BeamSocket wins every metric.**

What this proves: BeamSocket beats the batteries-included competitor across the board and is memory-competitive with a raw C++ transport. What it does *not* prove: the headline density and off-loop-fan-out claims that justify the whole architecture — those need a scale a 4-core box can't run, and they stay explicitly unproven pending a proper benchmark. The report keeps measured facts and projected claims strictly apart, and publishes the losses in bold next to the wins. A benchmark you can't lose is one nobody should believe.

## Shipping is its own boss fight

The code was done for days before it was *shipped*. Then the release pipeline — a GitHub Actions workflow that had, it turned out, never actually run — failed at every single layer, in sequence:

1. **Cross-compilation** — the Linux ARM/musl targets couldn't link. Fix: build natively on native runners instead.
2. **A Windows-only compile error** — a `#[cfg(unix)]` method inside a shared napi block left dangling registration glue on Windows. Fix: move it into its own fully-gated impl block.
3. **An empty artifact** — a script's "am I the main module?" check used a path format that's wrong on Windows, so staging silently did nothing. Fix: normalize with `pathToFileURL`.
4. **Auth, then 2FA** — the token worked but wasn't an *Automation* token, so npm demanded an OTP CI can't type.
5. **An npm path quirk** — `npm publish packages/beamsocket` (no trailing slash) got parsed as a GitHub `owner/repo` shorthand and tried to `git ls-remote` a repo that doesn't exist. Fix: an explicit `./` path, plus making the publish idempotent so re-runs don't choke on the packages that already went up.

Every one of these was real, none of them was the actual engine, and each was a clean one-line-ish fix once I saw the log. But that's five walls between "the code works" and "a stranger can install it" — and that last mile is where a lot of good projects quietly die. If you take one practical thing from this post: the pipeline is part of the product, budget for it, and read the actual logs instead of guessing.

## What "AI as co-architect" actually meant

Not autocomplete. The workflow was: argue the design in RFCs, pre-register predictions, gate every phase behind measurable exit criteria, and publish losses honestly — with an AI collaborator that could hold the whole architecture in view, push back, write the Rust, and diagnose a Windows napi macro failure from a CI log. It let one curious person go deep into Rust, distributed systems, and networking internals I'd otherwise never have touched — on the condition, always, of staying honest about what I actually knew versus what I was still guessing.

That condition is the whole thing. AI makes it easy to generate confident-sounding claims; the discipline that made this project real was refusing to ship a number I hadn't measured.

## Honest status

It's an alpha. Single-node today (the clustering mesh is built and tested in Rust but not yet wired through to JavaScript). Three platforms — linux-x64, macOS arm64, Windows x64 — with Alpine/musl, Intel-Mac, and ARM-Linux on the list. The headline performance claims are projections until they're run on real hardware. But it installs cleanly from npm, boots a server, echoes a client, and reports metrics — I verified that end-to-end from the published package before writing this.

I learned more building this than in the year before it. Still learning, still building.

`npm install beamsocket@alpha`
