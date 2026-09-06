# I built a networking runtime for Node.js. This is the whole journey.

*My first post. It's about the hardest, most rewarding thing I've built — and everything the process taught me along the way.*

---

I've never written one of these before. I kept waiting until I had something worth writing about. A few weeks ago I finally did: a project called **BeamSocket**, now live on npm as an alpha. But the package isn't really the story. The story is how it started as a vague itch and slowly turned into something with a spine — and how much I changed while building it.

So this is that. The beginning, the middle, the parts that went sideways, and what I'd tell myself if I could start over.

## Where it started

It started with a frustration a lot of Node.js developers quietly carry: JavaScript is a joy to write, but when you need *a lot* of persistent connections — tens of thousands, hundreds of thousands — the runtime starts to sweat. You reach for tricks, or another language, or more servers. The developer experience you loved gets traded away exactly when the problem gets interesting.

I kept turning over a question: *what if you didn't have to trade it away?* What if the network engine underneath was Rust-fast and memory-lean, but everything you actually wrote stayed plain JavaScript? Not a wrapper around a WebSocket library — an actual runtime. Connections, rooms, broadcasts, presence, backpressure, and eventually clustering, all handled beneath a familiar API.

I'd also been quietly obsessed with the ideas behind the BEAM — the virtual machine that powers Erlang and Elixir — where millions of lightweight, isolated processes pass messages and fail without taking each other down. I didn't want to rebuild the BEAM. I wanted to borrow its *shape*: isolated connections, message passing, backpressure, fault isolation, and design everything so distribution was possible later, not bolted on.

That was the whole idea at the start. Ambitious, a little naive, and — I'd learn — the easy part.

## The first real decision wasn't code

Here's the thing that surprised me most: the decision that shaped the entire project wasn't technical. It was choosing to **write things down before building them.**

Every significant piece started as an RFC — a short design document arguing what to build and why, with the tradeoffs named out loud. Only after the argument held up did any code get written. It felt slow. It was the opposite of slow. It meant I almost never built the wrong thing twice.

And I added one rule to those RFCs that changed how I think: **write your prediction down before you measure.** Not after. Before. So you can't quietly move the goalposts when the result embarrasses you.

I got to test that rule almost immediately, on the scariest part of the whole system.

## The moment the project earned my trust

The riskiest component in BeamSocket is the bridge — the thing that carries events from the Rust engine up into JavaScript. It's the one piece you genuinely cannot measure in isolation; it only exists with the full native stack running. Everything else in the architecture rests on it being fast.

I designed four candidate versions and, per my own rule, wrote down which one I expected to win. My money was on a clean, obvious design that collected events and handed them to JavaScript as an array of objects.

Then I built it and measured it.

It lost. By roughly four times.

A different design — one that packs everything into a single flat buffer per flush, with zero per-message allocation — won decisively and became what ships today. If I'd trusted my instinct instead of the benchmark, I'd have poured the entire rest of the project into a foundation the data had already rejected.

That was the moment the discipline stopped feeling like overhead and started feeling like the point. Being wrong on paper, early, cheaply, is a gift. The prediction-before-measurement habit is the only reason I caught it.

## How it took shape, phase by phase

From there it grew in deliberate stages, each one gated behind tests that had to pass before the next could start. Nothing was "done" until it was proven.

First an echo server — one connection, one message, round-trip through the whole stack — validated against the industry's WebSocket conformance suite. Then rooms and broadcasts, where a message fans out to thousands of members entirely in Rust, never touching the JavaScript event loop. Then identity: making "a user" a real, first-class thing so you could message every one of someone's devices at once. Then presence, metrics, graceful shutdown. Then the ability to attach to an existing Express or Fastify server, which turned out to have its own nest of platform-specific gremlins.

And then the big one — the part the whole architecture had been quietly designed for from day one: **clustering.** Making many BeamSocket nodes behave like one. This is where a decision I'd made phases earlier — keeping connection IDs opaque, leaving certain seams in the code — suddenly paid off. The distributed layer slotted in without tearing up what came before. Membership gossip, failure detection, routing that only sends messages to nodes that actually care — built as its own thing, and when I flipped it on, the single-node path was provably unchanged. Clustering you don't use costs you nothing.

That's the moment a project stops being a pile of features and starts feeling like a *system*.

## The honesty that made it real

Somewhere along the way, honesty became the theme.

When I finally benchmarked BeamSocket against the established players, I made a rule for myself: **publish the losses as loudly as the wins.** And there are losses. On small, low-concurrency, message-heavy workloads, raw transports beat it — because BeamSocket pays a fixed cost per message that only pays off at scale. That's in the report, in bold, next to the places it wins. A benchmark you can't lose is one nobody should believe.

It would have been so easy to cherry-pick. The whole project would have been weaker for it. The version of me that started this would have been tempted; the version that finished wasn't.

## Shipping is its own boss fight

Here's what nobody tells you: the code being done is not the same as the thing being shipped.

The release pipeline — the automation that compiles the Rust for every operating system and publishes to npm — had never actually run. When I finally triggered it, it failed at *every single layer*, one after another: cross-compilation, a compile error that only appeared on Windows, a script that silently produced an empty file because of how Windows formats paths, authentication, two-factor authentication, and a bizarre npm quirk where publishing a folder got mistaken for a request to clone a GitHub repo.

Five walls. Each real. None of them the actual engine. Each one a small, findable, fixable thing once I stopped guessing and read the actual logs.

That last mile — from "it works on my machine" to "a stranger can install it" — is where I think a lot of good projects quietly die. Not because the idea was bad, but because the unglamorous final stretch is exhausting and invisible. If I learned one practical thing, it's this: the pipeline is part of the product. Budget for it. Push through it.

Then one morning, `npm install beamsocket@alpha` worked from a clean machine, booted a server, and echoed a message. It's a small thing. It did not feel small.

## A note on how I built it

I built this with AI as a genuine collaborator — not autocomplete, but a co-architect that could hold the whole design in view, argue tradeoffs, write Rust, and diagnose a cryptic Windows build failure from a log. It let one curious person go far deeper into Rust, distributed systems, and networking internals than I could have alone.

But the tool didn't make the project good. The discipline did. AI makes it effortless to generate confident-sounding claims; the thing that made this real was refusing to ship a single number I hadn't measured. That's a line I want to keep, whatever I build next.

## What I'm taking with me

BeamSocket is an alpha. It's single-node in practice today, runs on three platforms, and its biggest performance claims are still projections until I run them on real hardware — and I'll tell you that plainly, because that's the whole ethos.

But it exists. It installs, it runs, and I understand things now that were a fog a month ago. I learned more building this than in a long time — about networking, about Rust, and mostly about the difference between having an idea and finishing something.

If you're sitting on an idea that feels too big for you: that feeling is not a stop sign. Write the first thing down. Predict, then measure. Be honest about what breaks. And push through the boring last mile, because that's where most people turn back — and it's the only part that actually ships.

This was my first post. It won't be my last.

`npm install beamsocket@alpha`

---

*BeamSocket is open source and still early. If you build with it, break it, or just want to argue about the design, I'd genuinely love to hear it.*
