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
//!   Delivery is best-effort, **at-least-once** (values may be dropped under load and may be
//!   redelivered); handlers must tolerate gaps and duplicates. See [`stream`] for details.
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

#[doc(hidden)]
pub mod testkit;

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
use futures_util::StreamExt;

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

    /// The single inbox pump: receive every inbound message, de-dup, and dispatch each one to the
    /// handler registered for its topic. One pump serves every stream and replica on this node.
    ///
    /// **Real-time first, polling as fallback.** The pump prefers the node's Server-Sent-Events
    /// push stream (`GET /mesh/messages/stream`, via [`CeClient::messages_stream`]): the node pushes
    /// each message the instant it arrives, so collections and streams update with no added latency.
    /// Polling [`messages`](CeClient::messages) every 250ms — the old behaviour — could add up to
    /// 250ms of delay per hop; SSE removes it.
    ///
    /// If the stream can't be opened (e.g. a node build without the endpoint) or it errors /
    /// disconnects, the pump falls back to a polling sweep and then retries the stream with capped
    /// exponential backoff. Either way the **same** de-dup-and-dispatch path runs, so behaviour is
    /// identical except for latency, and a redelivery seen across a stream-then-poll boundary (or a
    /// reconnect) is still recognised as a duplicate by the shared fingerprint window.
    ///
    /// Delivery is best-effort (the node's ring is capped, and a reconnect can miss messages sent
    /// while disconnected) — which is why the replicated log carries version numbers and repairs
    /// gaps itself, rather than trusting the pump to see every message.
    ///
    /// De-dup is **best-effort, at-least-once**, not exactly-once. The window keeps the last 8192
    /// fingerprints, so a message redelivered after more than 8192 newer ones can be dispatched
    /// again. Handlers must therefore be idempotent: the replicated log ignores entries it has
    /// already applied (by version), and [`Stream<T>`] is documented as at-least-once. The window is
    /// keyed on the stable [`fingerprint`] (content/identity, not `received_at`), so a redelivery
    /// that only changed its local timestamp is still recognised as a duplicate while in-window.
    fn spawn_pump(&self) {
        let coord = self.clone();
        tokio::spawn(async move {
            // Bounded de-dup shared across the stream and polling paths, so a message seen across a
            // stream/poll boundary or a reconnect isn't dispatched twice.
            let mut dedup = Dedup::new(8192);
            // Capped exponential backoff between stream-connect attempts; reset on a clean connect.
            let mut backoff = Duration::from_millis(250);
            const MAX_BACKOFF: Duration = Duration::from_secs(5);

            loop {
                match coord.inner.ce.messages_stream().await {
                    Ok(stream) => {
                        // Connected: reset backoff and consume pushed messages in real time.
                        backoff = Duration::from_millis(250);
                        tokio::pin!(stream);
                        loop {
                            match stream.next().await {
                                Some(Ok(msg)) => coord.inner.dispatch(&mut dedup, msg).await,
                                // A single malformed frame: skip it, keep the stream open.
                                Some(Err(_)) => continue,
                                // Stream ended/closed: drop out to the reconnect path.
                                None => break,
                            }
                        }
                    }
                    Err(_) => {
                        // The stream couldn't be opened (endpoint missing, transport error). Fall
                        // back to a single polling sweep so delivery still works, then back off.
                        if let Ok(msgs) = coord.inner.ce.messages().await {
                            for msg in msgs {
                                coord.inner.dispatch(&mut dedup, msg).await;
                            }
                        }
                    }
                }
                // Either the stream dropped or we polled once; wait before reconnecting.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        });
    }
}

impl CoordInner {
    /// De-dup one inbound message and, if fresh, dispatch it to its topic handler (routing any
    /// reply back via the message's `reply_token`). Shared by the SSE and polling pump paths.
    async fn dispatch(&self, dedup: &mut Dedup, msg: AppMessage) {
        if !dedup.insert(fingerprint(&msg)) {
            return;
        }
        let handler = self.handlers.lock().unwrap().get(&msg.topic).cloned();
        if let Some(h) = handler {
            if let Some(reply) = h(&msg) {
                if let Some(token) = msg.reply_token {
                    let _ = self.ce.reply(token, &reply).await;
                }
            }
        }
    }
}

/// A bounded sliding window of recently-seen fingerprints for at-least-once de-dup. Keeps the most
/// recent `cap` fingerprints; older ones fall out (so a very-late redelivery may be re-dispatched).
struct Dedup {
    cap: usize,
    order: VecDeque<u64>,
    seen: HashSet<u64>,
}

impl Dedup {
    fn new(cap: usize) -> Self {
        Self { cap, order: VecDeque::new(), seen: HashSet::new() }
    }

    /// Record `fp`. Returns `true` if it was newly seen (caller should dispatch), `false` if it is
    /// a duplicate currently in the window.
    fn insert(&mut self, fp: u64) -> bool {
        if !self.seen.insert(fp) {
            return false;
        }
        self.order.push_back(fp);
        if self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        true
    }
}

/// Stable fingerprint of a message for de-dup across polls.
///
/// Deliberately hashes only the stable content/identity of the message — sender, topic, payload,
/// and `reply_token` — and **not** `received_at`. `received_at` is a *local* timestamp the node
/// stamps on arrival; the same logical message redelivered (e.g. after a poll boundary, a retried
/// publish, or a node restart) carries a fresh `received_at`, so including it would defeat de-dup
/// exactly when de-dup is needed. The remaining fields uniquely identify a logical message: the same
/// sender will not re-use a `reply_token`, and two genuinely distinct messages from one sender on one
/// topic differ in their payload.
fn fingerprint(m: &AppMessage) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    m.from.hash(&mut h);
    m.topic.hash(&mut h);
    m.payload_hex.hash(&mut h);
    m.reply_token.hash(&mut h);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(from: &str, topic: &str, payload_hex: &str, received_at: u64, reply_token: Option<u64>) -> AppMessage {
        AppMessage {
            from: from.to_string(),
            topic: topic.to_string(),
            payload_hex: payload_hex.to_string(),
            received_at,
            reply_token,
        }
    }

    /// The fingerprint must ignore `received_at`: a redelivery that only re-stamps its local arrival
    /// time is the same logical message and must collide, so the pump de-dups it.
    #[test]
    fn fingerprint_ignores_received_at() {
        let a = msg("alice", "ce-coord/stream/x", "68656c6c6f", 1000, None);
        let b = msg("alice", "ce-coord/stream/x", "68656c6c6f", 9999, None);
        assert_eq!(
            fingerprint(&a),
            fingerprint(&b),
            "messages differing only in received_at must share a fingerprint"
        );
    }

    /// Genuinely distinct messages (different payload, sender, topic, or reply_token) must NOT
    /// collide — otherwise de-dup would swallow real traffic.
    #[test]
    fn fingerprint_distinguishes_content() {
        let base = msg("alice", "ce-coord/stream/x", "68656c6c6f", 1000, None);
        assert_ne!(fingerprint(&base), fingerprint(&msg("bob", "ce-coord/stream/x", "68656c6c6f", 1000, None)));
        assert_ne!(fingerprint(&base), fingerprint(&msg("alice", "ce-coord/stream/y", "68656c6c6f", 1000, None)));
        assert_ne!(fingerprint(&base), fingerprint(&msg("alice", "ce-coord/stream/x", "776f726c64", 1000, None)));
        assert_ne!(fingerprint(&base), fingerprint(&msg("alice", "ce-coord/stream/x", "68656c6c6f", 1000, Some(7))));
    }

    /// Drive the exact de-dup step the pump runs (now `Dedup::insert(fingerprint(&msg))`): a message
    /// redelivered with a fresh `received_at` is recognised as a duplicate and is NOT dispatched a
    /// second time, while a different message still gets through.
    #[test]
    fn redelivery_with_new_received_at_is_not_redispatched() {
        let mut dedup = Dedup::new(8192);
        let mut dispatched: Vec<String> = Vec::new();

        // Simulate the pump's filter-then-dispatch for a stream of inbound messages.
        let mut feed = |m: &AppMessage, dispatched: &mut Vec<String>| {
            if dedup.insert(fingerprint(m)) {
                dispatched.push(m.payload_hex.clone());
            }
        };

        let first = msg("alice", "ce-coord/stream/x", "68656c6c6f", 1000, None);
        // Same logical message, redelivered later with a different local timestamp.
        let redelivered = msg("alice", "ce-coord/stream/x", "68656c6c6f", 4242, None);
        // A genuinely new message.
        let other = msg("alice", "ce-coord/stream/x", "776f726c64", 5000, None);

        feed(&first, &mut dispatched);
        feed(&redelivered, &mut dispatched);
        feed(&other, &mut dispatched);

        assert_eq!(
            dispatched,
            vec!["68656c6c6f".to_string(), "776f726c64".to_string()],
            "redelivery with a new received_at must not be dispatched again; new content must be"
        );
    }

    /// The streaming pump and the polling fallback share one [`Dedup`] window. A message that the
    /// node first pushes over SSE and then re-delivers in a fallback polling sweep (the realistic
    /// stream-drop-then-reconnect-via-poll boundary) must be dispatched exactly once.
    #[test]
    fn dedup_collapses_stream_then_poll_redelivery() {
        let mut dedup = Dedup::new(8192);

        let pushed = msg("alice", "ce-coord/stream/x", "68656c6c6f", 1000, None);
        // After a reconnect the node re-serves the same logical message with a fresh local
        // timestamp; the shared window must recognise it.
        let repolled = msg("alice", "ce-coord/stream/x", "68656c6c6f", 2000, None);
        let fresh = msg("alice", "ce-coord/stream/x", "776f726c64", 3000, None);

        assert!(dedup.insert(fingerprint(&pushed)), "first delivery dispatches");
        assert!(!dedup.insert(fingerprint(&repolled)), "stream→poll redelivery must be de-duped");
        assert!(dedup.insert(fingerprint(&fresh)), "genuinely new content still dispatches");
    }

    /// The window is bounded: once more than `cap` distinct fingerprints have passed through, the
    /// oldest falls out and a very-late redelivery of it can be dispatched again (the documented
    /// at-least-once limit). A duplicate that is still in-window stays de-duped.
    #[test]
    fn dedup_window_evicts_oldest_beyond_cap() {
        let mut dedup = Dedup::new(4);

        assert!(dedup.insert(1));
        assert!(dedup.insert(2));
        assert!(!dedup.insert(2), "in-window duplicate is de-duped");
        // Fill past the cap (1,2,3,4,5) so fingerprint 1 is evicted.
        assert!(dedup.insert(3));
        assert!(dedup.insert(4));
        assert!(dedup.insert(5));
        assert!(dedup.insert(1), "evicted fingerprint may be dispatched again");
        // ...but 5 is still in-window.
        assert!(!dedup.insert(5), "recent fingerprint is still de-duped");
    }
}
