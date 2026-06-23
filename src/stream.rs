//! Typed pub/sub streams — the thin, ergonomic face of CE app pub/sub.
//!
//! A [`Stream<T>`] is a named, typed channel: every node that opens the same name with the same `T`
//! is wired together. `publish(&T)` fans a value out to all subscribers; `next().await` yields the
//! next value others published. Any `serde` type works. This is `mosaik`'s `produce::<T>()` /
//! `consume::<T>()` with the same one-line ergonomics, riding CE's signed gossip — so subscribers
//! get the authenticated sender for free if they want it.
//!
//! **Delivery is best-effort with at-least-once semantics, never exactly-once.** A value can be
//! *lost* (the node's inbox ring is bounded, so under load gossip is dropped) and a value can be
//! *redelivered* (the pump's de-dup window is bounded — see [`Coord`](crate::Coord) — so a message
//! that resurfaces after the window has rolled past it is handed to your consumer again). A stream
//! is therefore the right tool for telemetry, presence, and events whose handlers tolerate both gaps
//! and duplicates — not for state you can't lose or that must be applied exactly once. For
//! replicated state that must converge, use [`RMap`](crate::RMap) /
//! [`Replicated`](crate::Replicated): those carry version numbers, ignore already-applied entries,
//! and repair gaps. Note that gossip does not echo your own publishes back to you — publisher and
//! subscriber are different nodes.

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::Coord;

/// A typed pub/sub channel named `name`. Open it on every participating node.
pub struct Stream<T> {
    coord: Coord,
    topic: String,
    rx: mpsc::UnboundedReceiver<T>,
}

impl<T: Serialize + DeserializeOwned + Send + 'static> Stream<T> {
    pub(crate) async fn open(coord: Coord, name: &str) -> Result<Self> {
        let topic = format!("ce-coord/stream/{name}");
        let (tx, rx) = mpsc::unbounded_channel::<T>();

        // Decode each inbound message into `T` and hand it to the consumer.
        coord.register(&topic, move |msg| {
            if let Ok(bytes) = hex::decode(&msg.payload_hex) {
                if let Ok(item) = serde_json::from_slice::<T>(&bytes) {
                    let _ = tx.send(item);
                }
            }
            None
        });
        coord.client().subscribe(&topic).await?;

        Ok(Stream { coord, topic, rx })
    }

    /// Publish a value to every subscriber of this stream.
    pub async fn publish(&self, item: &T) -> Result<()> {
        let bytes = serde_json::to_vec(item)?;
        self.coord.client().publish(&self.topic, &bytes).await
    }

    /// Await the next value published by another node, or `None` if the stream is shutting down.
    pub async fn next(&mut self) -> Option<T> {
        self.rx.recv().await
    }
}

impl Coord {
    /// Open a typed pub/sub [`Stream<T>`] named `name`.
    pub async fn stream<T: Serialize + DeserializeOwned + Send + 'static>(
        &self,
        name: &str,
    ) -> Result<Stream<T>> {
        Stream::open(self.clone(), name).await
    }
}
