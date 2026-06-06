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

struct Inner<S: StateMachine> {
    core: Mutex<Core<S>>,
    /// The writer's authoritative log, served to readers on catch-up. Empty on readers.
    log: Mutex<Vec<Entry>>,
    ver_tx: watch::Sender<Version>,
    is_writer: bool,
    op_topic: String,
    coord: Coord,
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
        Self::open(coord, name, None).await
    }

    /// Open this node as a **read replica** following `writer` (its NodeId hex) for `name`.
    pub async fn reader(coord: Coord, name: &str, writer: &str) -> Result<Self> {
        Self::open(coord, name, Some(writer.to_string())).await
    }

    async fn open(coord: Coord, name: &str, writer: Option<String>) -> Result<Self> {
        let is_writer = writer.is_none();
        let writer_id = writer.unwrap_or_else(|| coord.node_id().to_string());
        let op_topic = format!("ce-coord/log/{writer_id}/{name}");
        let catchup_topic = format!("ce-coord/catchup/{writer_id}/{name}");
        let (ver_tx, _) = watch::channel(0u64);

        let inner = Arc::new(Inner {
            core: Mutex::new(Core { sm: S::default(), applied: 0, pending: BTreeMap::new() }),
            log: Mutex::new(Vec::new()),
            ver_tx,
            is_writer,
            op_topic: op_topic.clone(),
            coord: coord.clone(),
        });

        if is_writer {
            // Serve catch-up: hand readers every log entry at or after the version they ask for.
            let served = inner.clone();
            coord.register(&catchup_topic, move |msg| {
                let bytes = hex::decode(&msg.payload_hex).ok()?;
                let req: CatchUp = serde_json::from_slice(&bytes).ok()?;
                let log = served.log.lock().unwrap();
                let tail: Vec<Entry> =
                    log.iter().filter(|e| e.version >= req.from).cloned().collect();
                serde_json::to_vec(&tail).ok()
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

            // Catch-up task: turn triggers into directed requests to the writer.
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
                    if let Ok(reply) =
                        task_coord.client().request(&task_writer, &catchup_topic, &payload, 5_000).await
                    {
                        if let Ok(entries) = serde_json::from_slice::<Vec<Entry>>(&reply) {
                            for e in entries {
                                task_inner.ingest(e.version, e.op, &task_trig);
                            }
                        }
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
