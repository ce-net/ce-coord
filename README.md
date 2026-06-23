# ce-coord

**Self-organizing coordination primitives on the CE mesh — typed streams and replicated
collections.** This is the coordination layer that CE-the-node deliberately does *not* ship (see
`ce/docs/primitives.md`): it lives at the SDK tier, the same place `swarm` and `rdev` live, and it
talks to a local CE node through `ce-rs`. It is the open answer to what Flashbots' `mosaik` runtime
bundles into a node — but built as a **library over a trustless substrate** instead of baked in.

```rust
use ce_coord::Coord;

let coord = Coord::connect().await?;             // wraps the local node; one background pump

// writer node
let map = coord.map_writer::<String, i64>("balances").await?;
let v = map.insert("alice".into(), 100).await?;  // -> Version

// reader node (anywhere on the mesh, knows the writer's NodeId)
let map = coord.map_reader::<String, i64>("balances", writer_id).await?;
map.await_version(v).await;                       // converged
assert_eq!(map.get(&"alice".into()), Some(100));
```

## What it gives you

| Primitive | What it is | mosaik analogue |
|---|---|---|
| `Stream<T>` | Typed pub/sub channel. `publish(&T)` / `next().await`. | `streams().produce/consume` |
| `RMap<K,V>` | Replicated map: one writer, N readers, versioned convergence. | `Collections::Map` |
| `Replicated<S>` | The engine. Replicate any `StateMachine` you define. | the RSM under Collections |

`RVec` / `RSet` / `RCell` are the same three lines as `RMap` (state struct + op enum + `apply`); the
reference impl ships `RMap` and shows the pattern in `src/collections.rs`.

## Do you need CE *and* this app?

**Yes — `ce-coord` is a client of a running CE node; it is not standalone.** The split is the point:

- **CE node** (`ce start`) provides the three primitives this leans on — app **pub/sub**, directed
  **request/reply**, and the **inbox** — plus the thing that makes it trustworthy: it authenticates
  the *sender* of every message (`AppMessage.from` is a verified NodeId). `ce-coord` ships none of
  that and never opens a socket to a peer itself.
- **`ce-coord`** is a library you link into *your* program (and a `coord` demo binary). It turns
  those raw primitives into typed streams and replicated state.

So the dependency is one-directional and total: `your app → ce-coord → ce-rs → local CE node → mesh`.
No node, no coordination. That is exactly why it can stay a library: every node already exposes
everything it needs, so there is nothing to add to the node.

## Is it a plugin?

**Not in the load-into-the-node sense — and that's deliberate.** It does not get compiled into
`ce-node`, register an RPC, or run with the node's privileges. It is an ordinary process next to the
node, the same as `swarm` or `rdev`. "Plugin" in the useful sense — *compose new behavior without
forking the substrate* — yes: you add coordination to the network purely by running more client
programs. The extension point is "another app on the mesh," not "another module in the binary."

This matters because the node is the **trustless / Byzantine** layer. Raft and replicated state
assume *cooperating* members; bolting that into every node would put an honest-majority assumption
inside the one component that must not have one. Keeping it at the SDK tier means a Raft group is
just "a set of nodes that chose to trust each other" — scoped, optional, and (next version)
gated by a `ce-cap` capability — over a substrate that itself trusts no one.

## How it works (wire protocol)

A collection named `name` with writer `W` uses two topics:

- `ce-coord/log/<W>/<name>` — pub/sub. The writer broadcasts each operation as
  `Entry { version, op }`, version monotonic from 1.
- `ce-coord/catchup/<W>/<name>` — request/reply. A reader asks `{ from }`, the writer replies with
  every log entry `>= from`.

**Ordering is the reader's job, not the transport's** (CE delivery is best-effort):

- `version == applied + 1` → apply, then drain any buffered contiguous entries.
- `version <= applied` → already have it, ignore (idempotent).
- `version > applied + 1` → gap: buffer, and ask the writer for everything from `applied + 1`.

A reader fires one catch-up on startup, so it converges to the writer's current state immediately
instead of waiting for the next write. **Integrity** is free: the reader applies an entry only if
`msg.from == W`, and CE guarantees that field.

A `Stream<T>` is just the `ce-coord/stream/<name>` pub/sub topic with `serde` on both ends — no log,
no ordering, best-effort **at-least-once** delivery (a value may be dropped when the node's inbox
ring overflows, and may be redelivered once it falls outside the pump's bounded de-dup window, so
handlers must tolerate both gaps and duplicates). Use streams for events/telemetry, collections for
state that must converge.

## How it scales

- **Reads scale flat.** Every reader serves queries from its own local replica — zero writer load,
  zero network on the read path. Add readers freely.
- **Writes are one publish.** A write is a single gossip broadcast; cost is independent of reader
  count. Throughput is bounded by the writer's publish rate and gossip fan-out, not by N.
- **Catch-up is O(missed), not O(log).** Steady-state readers apply the live broadcast and never ask
  for catch-up. Only a reader that fell behind (just joined, or dropped messages) pulls a tail, and
  only the part it is missing.
- **Snapshot / bootstrap (shipped).** A state machine that implements [`Snapshot`](src/snapshot.rs)
  can be checkpointed: the writer serializes its state to a content-addressed object via `ce-rs`
  (`put_object`), records the `(base_version, cid)` checkpoint, and **compacts** its log (drops every
  op the snapshot already covers). A fresh `Replicated::snapshot_reader` fetches the snapshot by CID,
  loads it, and tails only newer ops — instead of replaying the writer's whole log from version 1.
  This bounds log growth for long-lived Notes spaces and Drive trees. The keystone guarantee
  (*snapshot + tail == full replay*) is property-tested.
- **Multi-writer merged-log (shipped).** [`Merged<M>`](src/merged.rs) runs **one writer-log per
  device** and folds the **key-ordered set union** of every device's ops into a [`MergeMachine`].
  Because the fold is over a sorted set, every replica converges to the same state regardless of
  delivery order — **leaderless, no Raft**. This is deliberate: Raft would stall offline edits (no
  quorum while a device is offline), which is exactly the local-first case CE targets. CE Drive's
  `DriveTree` move-CRDT and CE Notes' `MergeLog` are the intended consumers — true multi-device
  concurrent editing with deterministic conflict resolution.
- **The remaining honest limits of v0**, and where they go next:
  - *Polling pump* (250 ms) → latency floor and a bounded inbox ring. **Next:** the node's SSE
    message stream (`GET /mesh/messages/stream`) for push delivery; the protocol is unchanged.
  - *Open membership* → anyone can read. **Next:** gate the op/catch-up topics behind a `ce-cap`
    grant, so a collection is shared only with capability-holders.

## Run the demo

Needs a local CE node (`ce start`) on each machine. Then, across two nodes:

```sh
# node A — writer; prints its NodeId
coord map-writer balances
> set alice 100
> set bob 42

# node B — reader; prints the map each time it converges
coord map-reader balances <node-A-NodeId>
```

Typed stream:

```sh
coord stream-sub events     # node A
coord stream-pub events     # node B — type lines; they appear on A
```

## Boundary

`ce-coord` owns coordination *policy and ergonomics*. CE owns the *mechanism* (authenticated
transport, capabilities, economy). New device-to-device coordination belongs here as an app, not as
a node RPC — see `ce/docs/primitives.md` and the CE-vs-app boundary.
