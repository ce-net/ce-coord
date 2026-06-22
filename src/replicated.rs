//! The replication engine: a **single-writer, log-replicated state machine** over CE app messaging.
//!
//! One node is the **writer**. It owns an append-only log of operations, each stamped with a
//! monotonic [`Version`]. Every operation is broadcast to a pub/sub topic; **readers** apply them in
//! order to their own copy of the state machine. The writer's NodeId is part of the topic name *and*
//! checked on every entry, so a reader only ever applies operations authored by the writer it chose
//! to follow — that authentication is provided by CE for free.
//!
//! Delivery is best-effort, so ordering is enforced by the reader, not the transport:
//!
//! * entry `applied + 1` → apply it, then drain any buffered consecutive entries,
//! * entry `<= applied` → already have it, ignore (idempotent),
//! * entry `> applied + 1` → a gap: buffer it and ask the writer for everything from `applied + 1`.
//!
//! Catch-up is a directed [`request`](ce_rs::CeClient::request) to the writer, who replies with the
//! missing tail of its log. A reader also fires one catch-up on startup, so it converges to the
//! writer's current state immediately rather than waiting for the next write.
//!
//! This is exactly the model `mosaik` collections expose ("a writer that can mutate and readers
//! that track it"). Multi-writer groups with Raft leader election are the next layer up and slot in
//! here without changing collection call sites — see `README.md`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::snapshot::{Checkpoint, Snapshot};
use crate::Coord;

/// A monotonic operation index. The writer assigns it; readers converge to it.
pub type Version = u64;

/// One logged operation: a version plus the serialized [`StateMachine::Op`].
#[derive(Clone, Serialize, Deserialize)]
struct Entry {
    version: Version,
    op: Vec<u8>,
}

/// A reader's request: "send me every entry from `from` onward." The writer answers from its log.
#[derive(Serialize, Deserialize)]
struct CatchUp {
    from: Version,
}

/// The writer's catch-up reply: the requested tail of the log, plus — if the writer has compacted
/// past the requested floor — the [`Checkpoint`] the reader should bootstrap from instead. A reader
/// that asks for entries the writer has dropped (because they were folded into a snapshot) gets
/// `redirect = Some(cp)` and an empty/partial `tail`; it loads the snapshot, then re-tails from
/// `cp.base + 1`. Readers on the pre-snapshot path simply see `redirect = None`.
#[derive(Serialize, Deserialize)]
struct CatchUpReply {
    tail: Vec<Entry>,
    /// Set when the request floor predates the writer's compaction floor — bootstrap from here.
    redirect: Option<Checkpoint>,
}

/// The application-defined state being replicated. Implement this for anything: a map, a set, a
/// counter, a CRDT. The engine never inspects your state — it only orders and ships your `Op`s.
pub trait StateMachine: Default + Send + 'static {
    /// The mutation type. Must round-trip through JSON so it can travel the mesh.
    type Op: Serialize + DeserializeOwned + Send;

    /// Apply one operation. Called in version order on every replica, so given the same op sequence
    /// every replica reaches the same state. Keep it deterministic.
    fn apply(&mut self, op: Self::Op);
}

struct Core<S: StateMachine> {
    sm: S,
    applied: Version,
    /// Out-of-order entries held until their predecessors arrive (readers only).
    pending: BTreeMap<Version, Vec<u8>>,
}

/// The outcome of feeding one entry into the ordering core: did we advance, and (if a gap was
/// observed) which version do we still need? Extracted as a pure value so the gap-repair algorithm
/// can be unit-tested without a [`Coord`]/node. Returned by [`Core::feed`].
#[derive(Debug, PartialEq, Eq)]
enum Ingested {
    /// Entry was a duplicate (`version <= applied`); nothing changed.
    Duplicate,
    /// Entry sat past the contiguous frontier: buffered. We still need version `need`.
    Gap { need: Version },
    /// Entry applied (possibly draining buffered successors); new frontier is `applied`.
    Applied { applied: Version },
}

impl<S: StateMachine> Core<S> {
    fn apply_one(&mut self, bytes: &[u8]) {
        // A malformed op from an authenticated writer should never happen; if it does, skip it
        // rather than poison the replica. (We still advance `applied` to keep the log contiguous.)
        if let Ok(op) = serde_json::from_slice::<S::Op>(bytes) {
            self.sm.apply(op);
        }
    }

    /// Drain buffered entries that are now contiguous with `applied`. Pure; no I/O.
    fn drain_contiguous(&mut self) {
        loop {
            let next_version = self.applied + 1;
            match self.pending.remove(&next_version) {
                Some(next) => {
                    self.apply_one(&next);
                    self.applied = next_version;
                }
                None => break,
            }
        }
    }

    /// Feed one received entry into the ordering core. This is the *entire* single-writer ordering
    /// policy, isolated for testing: duplicates are ignored, gaps are buffered (and the missing
    /// version reported), and a contiguous entry is applied then followed by any buffered run.
    fn feed(&mut self, version: Version, op: Vec<u8>) -> Ingested {
        if version <= self.applied {
            return Ingested::Duplicate;
        }
        if version != self.applied + 1 {
            self.pending.insert(version, op);
            return Ingested::Gap { need: self.applied + 1 };
        }
        self.apply_one(&op);
        self.applied = version;
        self.drain_contiguous();
        Ingested::Applied { applied: self.applied }
    }

    /// Install a checkpoint-loaded state at `base`, discarding buffered entries the snapshot already
    /// covers, then draining any strictly-newer contiguous run on top. Pure; mirrors what
    /// [`Inner::load_checkpoint`] does under the lock.
    fn install_checkpoint(&mut self, sm: S, base: Version) {
        self.sm = sm;
        self.applied = base;
        self.pending.retain(|v, _| *v > base);
        self.drain_contiguous();
    }
}

/// A boxed, owned future — avoids pulling in `futures` just for `BoxFuture`.
type BoxFut<T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send>>;

/// How a reader loads a redirect snapshot: fetch the object by CID, deserialize it into a fresh `S`,
/// and return it. Installed only by the snapshot-aware reader constructor; absent on the legacy path
/// (where a redirect is simply ignored and full replay continues). Async because the fetch hits the
/// blob store via `ce-rs`.
type SnapLoader<S> = Arc<dyn Fn(String) -> BoxFut<Result<S>> + Send + Sync>;

struct Inner<S: StateMachine> {
    core: Mutex<Core<S>>,
    /// The writer's authoritative log, served to readers on catch-up. Empty on readers. After a
    /// compaction it holds only entries with `version > checkpoint.base`.
    log: Mutex<Vec<Entry>>,
    /// The writer's latest checkpoint, if it has taken one. `None` until the first `checkpoint()`.
    /// Served to readers that bootstrap or that ask for entries below the compaction floor.
    checkpoint: Mutex<Option<Checkpoint>>,
    ver_tx: watch::Sender<Version>,
    is_writer: bool,
    op_topic: String,
    coord: Coord,
    /// Reader-only: how to materialize a redirect snapshot. `None` on the legacy (full-replay) path
    /// and on writers.
    snap_loader: Mutex<Option<SnapLoader<S>>>,
}

impl<S: StateMachine> Inner<S> {
    /// Apply (or buffer) one received entry, repairing gaps via `trigger`. Idempotent and
    /// safe to call from both the pump's op-handler and the catch-up task. The ordering decision
    /// itself lives in [`Core::feed`] (unit-tested without a node); this method only adapts it to
    /// the lock, the gap-repair trigger, and the version watch.
    fn ingest(&self, version: Version, op: Vec<u8>, trigger: &mpsc::UnboundedSender<Version>) {
        let outcome = self.core.lock().unwrap().feed(version, op);
        match outcome {
            Ingested::Duplicate => {}
            Ingested::Gap { need } => {
                let _ = trigger.send(need);
            }
            Ingested::Applied { applied } => {
                let _ = self.ver_tx.send(applied);
            }
        }
    }

    /// Bootstrap from a checkpoint: fetch + deserialize the snapshot via the installed loader, then
    /// atomically replace the state machine and set `applied = cp.base`. Older entries (`<= base`)
    /// are then idempotently ignored by [`ingest`]; the tail applies on top contiguously.
    ///
    /// No-op (beyond re-triggering catch-up) if no loader is installed — the legacy reader path
    /// cannot materialize a snapshot, so it keeps asking for the full log instead. Idempotent: a
    /// checkpoint at or below the current `applied` is skipped.
    async fn load_checkpoint(&self, cp: &Checkpoint, trigger: &mpsc::UnboundedSender<Version>) {
        // Skip if we are already at/ahead of this checkpoint.
        if self.core.lock().unwrap().applied >= cp.base {
            return;
        }
        let loader = self.snap_loader.lock().unwrap().clone();
        let Some(loader) = loader else {
            // Legacy path: ask again from version 1 so a writer that has *not* compacted can still
            // serve us (the redirect only arrives when compaction happened, but be defensive).
            let _ = trigger.send(1);
            return;
        };
        let sm = match loader(cp.cid.clone()).await {
            Ok(sm) => sm,
            Err(e) => {
                tracing::warn!(cid = %cp.cid, base = cp.base, "snapshot load failed: {e:#}");
                return;
            }
        };
        let newly_applied;
        {
            let mut core = self.core.lock().unwrap();
            // Re-check under the lock: another path may have advanced us past the checkpoint.
            if core.applied >= cp.base {
                return;
            }
            core.install_checkpoint(sm, cp.base);
            newly_applied = core.applied;
        }
        let _ = self.ver_tx.send(newly_applied);
    }
}

/// A replicated state machine. Construct one with [`Replicated::writer`] or [`Replicated::reader`]
/// (or the typed wrappers like [`RMap`](crate::RMap)). Reads are local and synchronous; the single
/// writer's mutations go through [`propose`](Self::propose).
pub struct Replicated<S: StateMachine> {
    inner: Arc<Inner<S>>,
}

impl<S: StateMachine> Replicated<S> {
    /// Open this node as the **writer** for `name`. Only one writer should exist per `(node, name)`;
    /// the topic is namespaced by the writer's NodeId so distinct writers never collide.
    pub async fn writer(coord: Coord, name: &str) -> Result<Self> {
        Self::open(coord, name, None, None).await
    }

    /// Open this node as a **read replica** following `writer` (its NodeId hex) for `name`.
    pub async fn reader(coord: Coord, name: &str, writer: &str) -> Result<Self> {
        Self::open(coord, name, Some(writer.to_string()), None).await
    }

    async fn open(
        coord: Coord,
        name: &str,
        writer: Option<String>,
        loader: Option<SnapLoader<S>>,
    ) -> Result<Self> {
        let is_writer = writer.is_none();
        let writer_id = writer.unwrap_or_else(|| coord.node_id().to_string());
        let op_topic = format!("ce-coord/log/{writer_id}/{name}");
        let catchup_topic = format!("ce-coord/catchup/{writer_id}/{name}");
        let (ver_tx, _) = watch::channel(0u64);

        let inner = Arc::new(Inner {
            core: Mutex::new(Core { sm: S::default(), applied: 0, pending: BTreeMap::new() }),
            log: Mutex::new(Vec::new()),
            checkpoint: Mutex::new(None),
            ver_tx,
            is_writer,
            op_topic: op_topic.clone(),
            coord: coord.clone(),
            snap_loader: Mutex::new(loader),
        });

        if is_writer {
            // Serve catch-up: hand readers the log tail at or after the version they ask for. If the
            // request floor predates our compaction floor (the entries were folded into a snapshot),
            // attach the current checkpoint so the reader bootstraps from it instead of demanding
            // entries we no longer hold.
            let served = inner.clone();
            coord.register(&catchup_topic, move |msg| {
                let bytes = hex::decode(&msg.payload_hex).ok()?;
                let req: CatchUp = serde_json::from_slice(&bytes).ok()?;
                let cp = served.checkpoint.lock().unwrap().clone();
                let log = served.log.lock().unwrap();
                let tail: Vec<Entry> =
                    log.iter().filter(|e| e.version >= req.from).cloned().collect();
                // Redirect only when the reader is asking below what the snapshot already covers.
                let redirect = match &cp {
                    Some(c) if req.from <= c.base => Some(c.clone()),
                    _ => None,
                };
                serde_json::to_vec(&CatchUpReply { tail, redirect }).ok()
            });
        } else {
            // `trigger` carries "I need entries from version N" from the op-handler to the task
            // that actually performs the (async) catch-up request.
            let (trig_tx, mut trig_rx) = mpsc::unbounded_channel::<Version>();

            // Apply entries the writer broadcasts; on a gap, ask for the missing prefix.
            let app_inner = inner.clone();
            let expect_writer = writer_id.clone();
            let app_trig = trig_tx.clone();
            coord.register(&op_topic, move |msg| {
                if msg.from != expect_writer {
                    return None; // only the followed writer may mutate this collection
                }
                let bytes = hex::decode(&msg.payload_hex).ok()?;
                let entry: Entry = serde_json::from_slice(&bytes).ok()?;
                app_inner.ingest(entry.version, entry.op, &app_trig);
                None
            });
            coord.client().subscribe(&op_topic).await?;

            // Catch-up task: turn triggers into directed requests to the writer. Handles snapshot
            // redirects (bootstrap from a checkpoint) when a loader is installed.
            let task_inner = inner.clone();
            let task_coord = coord.clone();
            let task_writer = writer_id.clone();
            let task_trig = trig_tx.clone();
            tokio::spawn(async move {
                while let Some(from) = trig_rx.recv().await {
                    let payload = match serde_json::to_vec(&CatchUp { from }) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let Ok(reply) = task_coord
                        .client()
                        .request(&task_writer, &catchup_topic, &payload, 5_000)
                        .await
                    else {
                        continue;
                    };
                    let Ok(reply) = serde_json::from_slice::<CatchUpReply>(&reply) else {
                        continue;
                    };
                    // If the writer compacted past our floor, load its snapshot first (only if a
                    // loader is installed — the snapshot-aware reader path). This resets the state
                    // machine to `cp.base`, after which the tail (cp.base+1..) applies contiguously.
                    if let Some(cp) = reply.redirect {
                        task_inner.load_checkpoint(&cp, &task_trig).await;
                    }
                    for e in reply.tail {
                        task_inner.ingest(e.version, e.op, &task_trig);
                    }
                }
            });

            // Bootstrap: pull the writer's whole log now, don't wait for the next write.
            let _ = trig_tx.send(1);
        }

        Ok(Replicated { inner })
    }

    /// Propose a mutation (writer only). Applies it locally, appends to the log, bumps the version,
    /// and broadcasts it to readers. Returns the assigned [`Version`].
    pub async fn propose(&self, op: S::Op) -> Result<Version> {
        if !self.inner.is_writer {
            bail!("propose() on a read replica — only the writer may mutate");
        }
        let bytes = serde_json::to_vec(&op)?;
        let version = {
            let mut core = self.inner.core.lock().unwrap();
            core.applied += 1;
            core.sm.apply(op);
            core.applied
        };
        self.inner.log.lock().unwrap().push(Entry { version, op: bytes.clone() });
        let _ = self.inner.ver_tx.send(version);
        let wire = serde_json::to_vec(&Entry { version, op: bytes })?;
        self.inner.coord.client().publish(&self.inner.op_topic, &wire).await?;
        Ok(version)
    }

    /// The highest version applied to this replica.
    pub fn version(&self) -> Version {
        self.inner.core.lock().unwrap().applied
    }

    /// A watch receiver that fires every time this replica applies more entries — use it to react to
    /// convergence without polling.
    pub fn version_watch(&self) -> watch::Receiver<Version> {
        self.inner.ver_tx.subscribe()
    }

    /// Read the current state under the lock. Keep the closure cheap (it blocks writes).
    pub fn read<R>(&self, f: impl FnOnce(&S) -> R) -> R {
        f(&self.inner.core.lock().unwrap().sm)
    }

    /// Resolve once this replica has applied at least version `v` — i.e. it has caught up to the
    /// point a writer's [`propose`](Self::propose) returned.
    pub async fn await_version(&self, v: Version) {
        let mut rx = self.inner.ver_tx.subscribe();
        while *rx.borrow() < v {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

// ===========================================================================================
// Snapshot / bootstrap (additive; available only when the state machine implements `Snapshot`).
// ===========================================================================================

impl<S: Snapshot> Replicated<S> {
    /// Open a **read replica** that bootstraps from the writer's latest snapshot (if any) instead of
    /// replaying the whole log from version 1, then tails only newer ops. Equivalent to
    /// [`reader`](Self::reader) for a writer that has never checkpointed (it just full-replays), so
    /// this is always safe to use for a `Snapshot` state machine.
    ///
    /// The reader proves the keystone property: bootstrapping here reaches byte-for-byte the same
    /// state a full replay would — the snapshot is the deterministic fold of every op `<= base`.
    pub async fn snapshot_reader(coord: Coord, name: &str, writer: &str) -> Result<Self> {
        let loader = Self::make_loader(&coord);
        Self::open(coord, name, Some(writer.to_string()), Some(loader)).await
    }

    /// Build the snapshot loader closure: fetch the object by CID via `ce-rs` and [`Snapshot::load`].
    fn make_loader(coord: &Coord) -> SnapLoader<S> {
        let coord = coord.clone();
        Arc::new(move |cid: String| {
            let coord = coord.clone();
            Box::pin(async move {
                let bytes = coord.client().get_object(&cid).await?;
                S::load(&bytes)
            }) as BoxFut<Result<S>>
        })
    }

    /// Take a checkpoint (writer only): serialize the current state, store it as a content-addressed
    /// object via `ce-rs`, record the checkpoint at the current applied version, and **compact** the
    /// log by dropping every entry at or below that version. Returns the [`Checkpoint`].
    ///
    /// Safe to call repeatedly; each call advances the compaction floor. Readers that bootstrap or
    /// that ask for entries below the floor are redirected to the latest checkpoint. Readers already
    /// caught up are unaffected (they keep tailing the live ops above the floor).
    pub async fn checkpoint(&self) -> Result<Checkpoint> {
        if !self.inner.is_writer {
            bail!("checkpoint() on a read replica — only the writer may checkpoint");
        }
        // Snapshot the state + version atomically so the bytes match `base` exactly.
        let (bytes, base) = {
            let core = self.inner.core.lock().unwrap();
            (core.sm.save()?, core.applied)
        };
        let cid = self.inner.coord.client().put_object(&bytes).await?;
        let cp = Checkpoint { base, cid };
        // Record the checkpoint, then compact: drop entries the snapshot now covers.
        *self.inner.checkpoint.lock().unwrap() = Some(cp.clone());
        self.inner.log.lock().unwrap().retain(|e| e.version > base);
        Ok(cp)
    }

    /// The writer's current checkpoint, if it has taken one. Useful for tests and status displays.
    pub fn current_checkpoint(&self) -> Option<Checkpoint> {
        self.inner.checkpoint.lock().unwrap().clone()
    }

    /// The compaction floor: the highest version no longer retained in the live log (0 if never
    /// compacted). Entries with `version <= compaction_floor()` live only in the snapshot.
    pub fn compaction_floor(&self) -> Version {
        self.inner.checkpoint.lock().unwrap().as_ref().map(|c| c.base).unwrap_or(0)
    }

    /// Serialize this replica's current state to bytes (the same encoding [`checkpoint`] stores).
    /// Exposed so callers can snapshot without compacting, or persist locally.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        self.inner.core.lock().unwrap().sm.save()
    }
}

// ===========================================================================================
// In-crate unit + property tests for the ordering core (no node / mesh required). The reader's
// gap-repair / dedup / drain / checkpoint-install policy is the heart of single-writer replication;
// here it is exercised in isolation through `Core::feed` / `Core::install_checkpoint`, which the
// production `ingest` / `load_checkpoint` paths delegate to verbatim.
// ===========================================================================================
#[cfg(test)]
mod core_tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // A KV state machine whose applied state == the BTreeMap fold of its ops. Deterministic and
    // order-sensitive (a later Set for the same key clobbers an earlier one), so it detects any
    // mis-ordering in the core.
    #[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
    struct Kv {
        map: std::collections::BTreeMap<String, i64>,
    }
    #[derive(Clone, Serialize, Deserialize)]
    enum KvOp {
        Set(String, i64),
        Del(String),
    }
    impl StateMachine for Kv {
        type Op = KvOp;
        fn apply(&mut self, op: KvOp) {
            match op {
                KvOp::Set(k, v) => {
                    self.map.insert(k, v);
                }
                KvOp::Del(k) => {
                    self.map.remove(&k);
                }
            }
        }
    }
    impl crate::snapshot::Snapshot for Kv {
        crate::json_snapshot!();
    }

    fn enc(op: &KvOp) -> Vec<u8> {
        serde_json::to_vec(op).unwrap()
    }

    fn fresh() -> Core<Kv> {
        Core { sm: Kv::default(), applied: 0, pending: BTreeMap::new() }
    }

    /// In-order delivery applies each entry and advances the frontier by one.
    #[test]
    fn feed_in_order_applies_sequentially() {
        let mut c = fresh();
        assert_eq!(c.feed(1, enc(&KvOp::Set("a".into(), 1))), Ingested::Applied { applied: 1 });
        assert_eq!(c.feed(2, enc(&KvOp::Set("b".into(), 2))), Ingested::Applied { applied: 2 });
        assert_eq!(c.applied, 2);
        assert_eq!(c.sm.map["a"], 1);
        assert_eq!(c.sm.map["b"], 2);
    }

    /// A duplicate (version <= applied) is reported and changes nothing.
    #[test]
    fn feed_duplicate_is_ignored() {
        let mut c = fresh();
        c.feed(1, enc(&KvOp::Set("a".into(), 1)));
        assert_eq!(c.feed(1, enc(&KvOp::Set("a".into(), 999))), Ingested::Duplicate);
        assert_eq!(c.sm.map["a"], 1, "duplicate must not re-apply");
        assert_eq!(c.applied, 1);
    }

    /// A gap buffers the entry and reports the missing version; arrival of the gap fill drains the
    /// whole buffered run in one step.
    #[test]
    fn feed_gap_buffers_then_drains() {
        let mut c = fresh();
        // Deliver 3, 4, 2 out of order before 1 arrives.
        assert_eq!(c.feed(3, enc(&KvOp::Set("c".into(), 3))), Ingested::Gap { need: 1 });
        assert_eq!(c.feed(4, enc(&KvOp::Set("d".into(), 4))), Ingested::Gap { need: 1 });
        assert_eq!(c.feed(2, enc(&KvOp::Set("b".into(), 2))), Ingested::Gap { need: 1 });
        assert_eq!(c.applied, 0, "nothing applies while v1 is missing");
        // v1 arrives: applies 1, then drains 2,3,4 contiguously.
        assert_eq!(c.feed(1, enc(&KvOp::Set("a".into(), 1))), Ingested::Applied { applied: 4 });
        assert_eq!(c.applied, 4);
        assert_eq!(c.sm.map.len(), 4);
        assert!(c.pending.is_empty(), "buffer drained");
    }

    /// Partial drain: a gap fill that only bridges part of the buffer advances to the first hole.
    #[test]
    fn feed_partial_drain_stops_at_next_hole() {
        let mut c = fresh();
        c.feed(2, enc(&KvOp::Set("b".into(), 2)));
        c.feed(4, enc(&KvOp::Set("d".into(), 4))); // 3 is missing
        assert_eq!(c.feed(1, enc(&KvOp::Set("a".into(), 1))), Ingested::Applied { applied: 2 });
        assert_eq!(c.applied, 2, "stops before the v3 hole");
        assert!(c.pending.contains_key(&4));
        // 3 arrives -> drains 3 and 4.
        assert_eq!(c.feed(3, enc(&KvOp::Set("c".into(), 3))), Ingested::Applied { applied: 4 });
        assert_eq!(c.applied, 4);
    }

    /// A malformed op (not decodable into `S::Op`) does NOT poison the replica: the frontier still
    /// advances (so the log stays contiguous) and later ops keep applying.
    #[test]
    fn feed_malformed_op_skips_without_poisoning() {
        let mut c = fresh();
        assert_eq!(c.feed(1, b"not json at all".to_vec()), Ingested::Applied { applied: 1 });
        assert_eq!(c.applied, 1);
        assert!(c.sm.map.is_empty(), "garbage op produced no state change");
        // The next valid op still applies on top.
        assert_eq!(c.feed(2, enc(&KvOp::Set("a".into(), 7))), Ingested::Applied { applied: 2 });
        assert_eq!(c.sm.map["a"], 7);
    }

    /// install_checkpoint resets state to `base`, discards covered buffered entries, keeps newer
    /// ones, and drains them on top.
    #[test]
    fn install_checkpoint_resets_and_drains_tail() {
        let mut c = fresh();
        // Buffer some out-of-order entries spanning the checkpoint base.
        c.feed(2, enc(&KvOp::Set("b".into(), 2)));
        c.feed(6, enc(&KvOp::Set("f".into(), 6)));
        c.feed(7, enc(&KvOp::Set("g".into(), 7)));
        // Snapshot state captured everything up to base=5 (here we hand it a concrete state).
        let mut snap = Kv::default();
        snap.map.insert("snap".into(), 100);
        c.install_checkpoint(snap, 5);
        assert_eq!(c.applied, 7, "drains buffered 6,7 on top of the base=5 snapshot");
        assert_eq!(c.sm.map["snap"], 100);
        assert_eq!(c.sm.map["f"], 6);
        assert_eq!(c.sm.map["g"], 7);
        assert!(!c.sm.map.contains_key("b"), "v2 was below base and discarded from the buffer");
    }

    /// install_checkpoint with no contiguous tail leaves applied exactly at base.
    #[test]
    fn install_checkpoint_without_tail_stays_at_base() {
        let mut c = fresh();
        c.feed(9, enc(&KvOp::Set("z".into(), 9))); // far gap, buffered
        let snap = Kv::default();
        c.install_checkpoint(snap, 3);
        assert_eq!(c.applied, 3);
        assert!(c.pending.contains_key(&9), "strictly-newer buffered entry retained");
    }

    use proptest::prelude::*;

    // Reference fold: apply ops 1..=n in version order to a fresh state machine.
    fn reference(ops: &[KvOp]) -> Kv {
        let mut s = Kv::default();
        for op in ops {
            s.apply(op.clone());
        }
        s
    }

    proptest! {
        // KEYSTONE convergence: feed the SAME versioned log to the core in an ARBITRARY delivery
        // permutation (with duplicates) and it must converge to the exact in-version-order fold.
        // This is the core's whole contract: order-independent, duplicate-tolerant convergence.
        #[test]
        fn prop_arbitrary_delivery_order_converges(
            vals in proptest::collection::vec(-1000i64..1000, 1..40),
            // a permutation seed + duplicate factor
            seed in any::<u64>(),
        ) {
            // Build a deterministic log: version i (1-based) sets key "k{i%5}" = vals[i].
            let ops: Vec<KvOp> = vals.iter().enumerate().map(|(i, v)| {
                if i % 7 == 6 { KvOp::Del(format!("k{}", i % 5)) }
                else { KvOp::Set(format!("k{}", i % 5), *v) }
            }).collect();
            let n = ops.len() as u64;
            let want = reference(&ops);

            // Deterministic shuffle of versions 1..=n, with every entry delivered twice (dup test).
            let mut order: Vec<u64> = (1..=n).chain(1..=n).collect();
            // simple LCG shuffle keyed by seed
            let mut s = seed | 1;
            for i in (1..order.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (s >> 33) as usize % (i + 1);
                order.swap(i, j);
            }

            let mut c = fresh();
            for v in order {
                let op = enc(&ops[(v - 1) as usize]);
                c.feed(v, op);
            }
            // Even with arbitrary order + duplicates, if everything was delivered the frontier is n
            // and the state equals the in-order fold.
            prop_assert_eq!(c.applied, n);
            prop_assert_eq!(c.sm, want);
            prop_assert!(c.pending.is_empty());
        }

        // Idempotence: feeding the full log a second time changes nothing.
        #[test]
        fn prop_replay_is_idempotent(vals in proptest::collection::vec(-100i64..100, 1..25)) {
            let ops: Vec<KvOp> = vals.iter().enumerate()
                .map(|(i, v)| KvOp::Set(format!("k{}", i % 4), *v)).collect();
            let n = ops.len() as u64;
            let mut c = fresh();
            for v in 1..=n { c.feed(v, enc(&ops[(v-1) as usize])); }
            let after_first = c.sm.clone();
            for v in 1..=n { c.feed(v, enc(&ops[(v-1) as usize])); }
            prop_assert_eq!(c.sm, after_first);
            prop_assert_eq!(c.applied, n);
        }

        // Checkpoint equivalence at every cut: install a snapshot of the first `base` ops, then
        // deliver the tail in arbitrary order -> equals the full in-order fold. This is the
        // snapshot-bootstrap keystone, proven through the real core (not a hand-rolled replay).
        #[test]
        fn prop_snapshot_install_plus_tail_equals_full(
            vals in proptest::collection::vec(-500i64..500, 1..30),
            base_frac in 0u64..100,
        ) {
            let ops: Vec<KvOp> = vals.iter().enumerate()
                .map(|(i, v)| KvOp::Set(format!("k{}", i % 6), *v)).collect();
            let n = ops.len() as u64;
            let base = base_frac * n / 100; // 0..=n
            let want = reference(&ops);

            // The snapshot is the deterministic fold of ops 1..=base.
            let snap = reference(&ops[..base as usize]);
            let mut c = fresh();
            c.install_checkpoint(snap, base);
            // Deliver the tail (base+1..=n) reversed, to prove order-independence post-bootstrap.
            for v in ((base + 1)..=n).rev() {
                c.feed(v, enc(&ops[(v - 1) as usize]));
            }
            prop_assert_eq!(c.applied, n);
            prop_assert_eq!(c.sm, want);
        }
    }
}
