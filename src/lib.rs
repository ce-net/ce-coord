//! # ce-coord — coordination primitives on the CE mesh
//!
//! `ce-coord` is the **coordination layer CE itself deliberately does not have**: typed pub/sub
//! streams and replicated collections, the way the Flashbots `mosaik` runtime exposes them — but
//! built as a *library over CE primitives*, not baked into the node. It talks to a **local CE
//! node** through `ce-rs` and uses three node-provided primitives only:
//!
//! * **app pub/sub** (`publish` / `subscribe`) — fan a signed message out to every subscriber,
//! * **directed request/reply** (`request` / `reply`) — ask one peer and get an answer,
//! * **the inbox** (`messages`) — receive everything addressed to this node.
//!
//! Nothing here is privileged. The node authenticates the *sender* of every message for free
//! (`AppMessage.from` is a verified NodeId), which is the whole trust story for a single-writer
//! replica: a reader only applies log entries signed by the one writer it was told to follow.
//! Because the substrate underneath is trustless, this works **between parties who don't fully
//! trust each other** — the thing `mosaik` (honest-fleet, not BFT) can't offer.
//!
//! ## What you get
//!
//! * [`Stream<T>`] — a typed channel. `publish(&T)` on one node, `next().await` on others.
//! * [`RMap<K, V>`](collections::RMap) — a replicated map with one **writer** and any number of
//!   **readers**. Mutations return a [`Version`](replicated::Version); readers
//!   [`await_version`](collections::RMap::await_version) to confirm convergence.
//! * [`Replicated<S>`](replicated::Replicated) — the engine: replicate *any* state machine you
//!   define (a Vec, a Set, a counter — see [`StateMachine`](replicated::StateMachine)).
//!
//! ## Shape
//!
//! ```no_run
//! use ce_coord::Coord;
//! # async fn demo() -> anyhow::Result<()> {
//! let coord = Coord::connect().await?;            // wraps the local node; starts one background pump
//!
//! // --- writer node ---
//! let map = coord.map_writer::<String, i64>("balances").await?;
//! let v = map.insert("alice".into(), 100).await?; // -> Version
//!
//! // --- reader node (knows the writer's NodeId) ---
//! let map = coord.map_reader::<String, i64>("balances", "writer_node_id_hex").await?;
//! map.await_version(v).await;                      // block until this replica has caught up
//! assert_eq!(map.get(&"alice".into()), Some(100));
//! # Ok(()) }
//! ```
//!
//! See `README.md` for the wire protocol, failure model, and how it scales.

pub mod collections;
pub mod merged;
pub mod replicated;
pub mod snapshot;
pub mod stream;

pub use collections::{RCell, RCounter, RMap, RSet, RVec};
pub use merged::{MergeKey, MergeMachine, Merged, WriterLog};
pub use replicated::{Replicated, StateMachine, Version};
pub use snapshot::{Checkpoint, Snapshot};
pub use stream::Stream;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use ce_rs::{AppMessage, CeClient};

/// A handler reacts to one inbound [`AppMessage`] on a topic it registered for. Returning
/// `Some(bytes)` means "reply with these bytes" — the pump routes them back to the requester via
/// the message's `reply_token` (used to serve catch-up requests). Handlers are synchronous and must
/// not block: state lives behind fast in-memory mutexes, never an await.
type Handler = Arc<dyn Fn(&AppMessage) -> Option<Vec<u8>> + Send + Sync>;

struct CoordInner {
    ce: CeClient,
    node_id: String,
    /// topic -> handler. Exact-match dispatch; every stream/replica registers its own topic.
    handlers: Mutex<HashMap<String, Handler>>,
}

/// Handle to the local node's coordination layer. Cheap to clone (everything is behind an `Arc`);
/// one [`Coord`] drives a single background pump that fans the node's inbox out to every registered
/// stream and replica.
#[derive(Clone)]
pub struct Coord {
    inner: Arc<CoordInner>,
}

impl Coord {
    /// Connect to the local CE node on the default port and start the inbox pump.
    pub async fn connect() -> Result<Coord> {
        Self::with_client(CeClient::local()).await
    }

    /// Connect using a pre-built [`CeClient`] (custom URL/token) and start the inbox pump.
    pub async fn with_client(ce: CeClient) -> Result<Coord> {
        let node_id = ce.status().await?.node_id;
        let coord = Coord {
            inner: Arc::new(CoordInner { ce, node_id, handlers: Mutex::new(HashMap::new()) }),
        };
        coord.spawn_pump();
        Ok(coord)
    }

    /// This node's NodeId (hex). Readers need a writer's NodeId to follow its collection.
    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    pub(crate) fn client(&self) -> &CeClient {
        &self.inner.ce
    }

    /// Register `f` to receive messages on `topic`. Replaces any prior handler for that topic.
    pub(crate) fn register<F>(&self, topic: &str, f: F)
    where
        F: Fn(&AppMessage) -> Option<Vec<u8>> + Send + Sync + 'static,
    {
        self.inner.handlers.lock().unwrap().insert(topic.to_string(), Arc::new(f));
    }

    /// The single inbox pump: poll the node's message ring, de-dup, and dispatch each message to
    /// the handler registered for its topic. One pump serves every stream and replica on this node.
    ///
    /// Delivery is best-effort (the ring is capped) — which is why the replicated log carries
    /// version numbers and repairs gaps itself, rather than trusting the pump to see every message.
    fn spawn_pump(&self) {
        let coord = self.clone();
        tokio::spawn(async move {
            // Bounded de-dup so a message seen across two polls isn't dispatched twice.
            let mut seen_order: VecDeque<u64> = VecDeque::new();
            let mut seen: HashSet<u64> = HashSet::new();
            loop {
                if let Ok(msgs) = coord.inner.ce.messages().await {
                    for msg in msgs {
                        let fp = fingerprint(&msg);
                        if !seen.insert(fp) {
                            continue;
                        }
                        seen_order.push_back(fp);
                        if seen_order.len() > 8192 {
                            if let Some(old) = seen_order.pop_front() {
                                seen.remove(&old);
                            }
                        }
                        let handler = coord.inner.handlers.lock().unwrap().get(&msg.topic).cloned();
                        if let Some(h) = handler {
                            if let Some(reply) = h(&msg) {
                                if let Some(token) = msg.reply_token {
                                    let _ = coord.inner.ce.reply(token, &reply).await;
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }
}

/// Stable fingerprint of a message for de-dup across polls.
fn fingerprint(m: &AppMessage) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    m.from.hash(&mut h);
    m.topic.hash(&mut h);
    m.payload_hex.hash(&mut h);
    m.reply_token.hash(&mut h);
    m.received_at.hash(&mut h);
    h.finish()
}
