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
    fn apply_bytes(core: &mut Core<S>, bytes: &[u8]) {
        // A malformed op from an authenticated writer should never happen; if it does, skip it
        // rather than poison the replica. (We still advance `applied` to keep the log contiguous.)
        if let Ok(op) = serde_json::from_slice::<S::Op>(bytes) {
            core.sm.apply(op);
        }
    }

    /// Apply (or buffer) one received entry, repairing gaps via `trigger`. Idempotent and
    /// safe to call from both the pump's op-handler and the catch-up task.
    fn ingest(&self, version: Version, op: Vec<u8>, trigger: &mpsc::UnboundedSender<Version>) {
        let newly_applied;
        {
            let mut core = self.core.lock().unwrap();
            if version <= core.applied {
                return; // already have it
            }
            if version != core.applied + 1 {
                core.pending.insert(version, op); // gap: buffer and ask for the missing prefix
                let need = core.applied + 1;
                drop(core);
                let _ = trigger.send(need);
                return;
            }
            Self::apply_bytes(&mut core, &op);
            core.applied = version;
            // Drain any buffered entries that are now contiguous. Read the next version into a
            // local first — `core` is a MutexGuard, so an inline `core.applied` read inside the
            // `remove` call would alias the mutable borrow.
            loop {
                let next_version = core.applied + 1;
                match core.pending.remove(&next_version) {
                    Some(next) => {
                        Self::apply_bytes(&mut core, &next);
                        core.applied = next_version;
                    }
                    None => break,
                }
            }
            newly_applied = core.applied;
        }
        let _ = self.ver_tx.send(newly_applied);
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
            core.sm = sm;
            core.applied = cp.base;
            // Drop buffered entries already covered by the snapshot; keep the strictly-newer ones.
            core.pending.retain(|v, _| *v > cp.base);
            // Drain any now-contiguous buffered entries on top of the snapshot.
            loop {
                let next_version = core.applied + 1;
                match core.pending.remove(&next_version) {
                    Some(next) => {
                        Self::apply_bytes(&mut core, &next);
                        core.applied = next_version;
                    }
                    None => break,
                }
            }
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
