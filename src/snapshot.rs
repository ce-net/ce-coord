//! Snapshot / bootstrap: content-addressed checkpoints for [`Replicated`](crate::Replicated).
//!
//! The single-writer log in [`replicated`](crate::replicated) is correct but *unbounded*: a writer
//! keeps every op it ever proposed so a fresh reader can replay from version 1. For a long-lived
//! Notes space or a large Drive tree that is ruinous — the log dwarfs the live state and every new
//! device pays to replay all of history.
//!
//! A **snapshot** fixes this. A state machine that implements [`Snapshot`] can serialize its whole
//! current state to bytes. The writer:
//!
//! 1. serializes the state at the version it has applied (`base`),
//! 2. stores those bytes as a **content-addressed object** via `ce-rs` (returns a `cid`),
//! 3. records the checkpoint `(base, cid)` and **compacts** its log — dropping every entry at or
//!    below `base`, since a reader can reach `base` from the snapshot instead of replaying.
//!
//! A fresh **reader** then bootstraps the cheap way:
//!
//! 1. ask the writer for its latest checkpoint (`(base, cid)`),
//! 2. fetch the snapshot object by `cid`, [`Snapshot::load`] it, set `applied = base`,
//! 3. tail only ops **newer than** `base` (the normal catch-up path, just from a higher floor).
//!
//! The guarantee proven in the tests: *snapshot + tail == full replay*. A reader that bootstraps
//! from a checkpoint reaches byte-for-byte the same state as one that replayed the entire log,
//! because the snapshot is the deterministic fold of every op at or below `base`.
//!
//! Snapshotting is **opt-in and additive**: a state machine that does not implement [`Snapshot`]
//! behaves exactly as before (full-log replay). `RMap`/`RSet`/`RVec`/`RCell`/`RCounter` are
//! unaffected unless a caller adds the impl.

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::replicated::{StateMachine, Version};

/// A [`StateMachine`] that can serialize its full current state to bytes and reconstruct from them.
///
/// This is what makes log compaction and cheap reader bootstrap possible. Implement it for any
/// state whose value is fully captured by some `serde`-encodable form — which is almost everything,
/// since `apply` is already a deterministic fold over ops.
///
/// The round-trip must be **faithful**: `S::load(&s.save()?)?` must equal `s` (same observable
/// state). The engine relies on this — a reader that loads a snapshot at version `base` must be
/// indistinguishable from one that replayed every op up to `base`.
pub trait Snapshot: StateMachine {
    /// Serialize the full current state to bytes (stored as a content-addressed blob).
    fn save(&self) -> Result<Vec<u8>>;

    /// Reconstruct a state machine from bytes produced by [`save`](Snapshot::save).
    fn load(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// A checkpoint marker: the snapshot bytes live at `cid`, and replaying every op with
/// `version > base` on top of that snapshot yields the live state. Travels writer -> reader as the
/// answer to a checkpoint request, and is the unit the writer persists when it compacts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// The highest [`Version`] folded into the snapshot. The reader resumes tailing from `base + 1`.
    pub base: Version,
    /// Content id (hex sha256, the `ce-rs` object CID) of the serialized snapshot bytes.
    pub cid: String,
}

impl Checkpoint {
    /// True if this checkpoint is strictly newer (covers more ops) than `other`.
    pub fn is_newer_than(&self, other: &Checkpoint) -> bool {
        self.base > other.base
    }
}

/// Convenience: a blanket [`Snapshot`] for any state machine that is itself `serde` + `Clone`,
/// using the same JSON encoding the op log uses. Collections can opt in with one line:
///
/// ```ignore
/// impl Snapshot for MyState { json_snapshot!(); }
/// ```
///
/// (Provided as a macro rather than a blanket impl so it stays opt-in and never collides with a
/// hand-written, more compact encoding.)
#[macro_export]
macro_rules! json_snapshot {
    () => {
        fn save(&self) -> ::anyhow::Result<::std::vec::Vec<u8>> {
            ::serde_json::to_vec(self).map_err(::core::convert::Into::into)
        }
        fn load(bytes: &[u8]) -> ::anyhow::Result<Self> {
            ::serde_json::from_slice(bytes).map_err(::core::convert::Into::into)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // A concrete snapshotable state machine: a key/value map. The fold of its ops is exactly its
    // state, so it is the cleanest vehicle for proving snapshot+tail == full replay.
    #[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
    struct KvState {
        map: std::collections::BTreeMap<String, i64>,
    }

    #[derive(Clone, Serialize, Deserialize)]
    enum KvOp {
        Set(String, i64),
        Del(String),
    }

    impl StateMachine for KvState {
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

    impl Snapshot for KvState {
        json_snapshot!();
    }

    // A deterministic op stream: writes, overwrites, and deletes that interleave so the snapshot at
    // any cut point is non-trivial (keys created and removed before the cut).
    fn op_stream() -> Vec<KvOp> {
        let mut ops = Vec::new();
        for i in 0..50i64 {
            ops.push(KvOp::Set(format!("k{}", i % 12), i));
            if i % 5 == 0 {
                ops.push(KvOp::Del(format!("k{}", i % 12)));
            }
            if i % 7 == 0 {
                ops.push(KvOp::Set("hot".into(), i * 2));
            }
        }
        ops
    }

    fn full_replay(ops: &[KvOp]) -> KvState {
        let mut s = KvState::default();
        for op in ops {
            s.apply(op.clone());
        }
        s
    }

    /// The keystone property at the state-machine level: for every cut point `base`, taking a
    /// snapshot after `base` ops, reloading it, then applying the tail (`base..`) yields exactly the
    /// same state as replaying the whole stream. This is what the reader's checkpoint-bootstrap
    /// path relies on; here we prove the underlying equivalence without the mesh.
    #[test]
    fn snapshot_plus_tail_equals_full_replay() {
        let ops = op_stream();
        let reference = full_replay(&ops);

        for base in 0..=ops.len() {
            // Snapshot the state after applying the first `base` ops.
            let snapshotted = full_replay(&ops[..base]);
            let bytes = snapshotted.save().unwrap();

            // Fresh reader: load the snapshot, then tail only the newer ops.
            let mut reader = KvState::load(&bytes).unwrap();
            for op in &ops[base..] {
                reader.apply(op.clone());
            }

            assert_eq!(reader, reference, "snapshot+tail diverged at base={base}");
        }
    }

    /// Snapshot round-trip is faithful: load(save(s)) == s for many distinct states.
    #[test]
    fn snapshot_roundtrip_is_faithful() {
        let ops = op_stream();
        for cut in 0..=ops.len() {
            let s = full_replay(&ops[..cut]);
            let back = KvState::load(&s.save().unwrap()).unwrap();
            assert_eq!(s, back, "round-trip changed state at cut={cut}");
        }
    }

    /// Compaction preserves state: dropping the snapshotted prefix from the op log and keeping only
    /// the tail (on top of the snapshot) reaches the same state as keeping the whole log. Mirrors
    /// what `Replicated::checkpoint` does when it `retain`s `version > base`.
    #[test]
    fn compaction_preserves_state() {
        let ops = op_stream();
        let reference = full_replay(&ops);

        let base = 23; // arbitrary interior cut
        let snap = full_replay(&ops[..base]);
        let snap_bytes = snap.save().unwrap();
        // The "compacted log" keeps only ops strictly after the checkpoint base.
        let tail: Vec<KvOp> = ops[base..].to_vec();

        let mut rebuilt = KvState::load(&snap_bytes).unwrap();
        for op in &tail {
            rebuilt.apply(op.clone());
        }
        assert_eq!(rebuilt, reference, "compacted log lost state");
    }

    #[test]
    fn checkpoint_newer_than_orders_by_base() {
        let a = Checkpoint { base: 10, cid: "a".into() };
        let b = Checkpoint { base: 20, cid: "b".into() };
        assert!(b.is_newer_than(&a));
        assert!(!a.is_newer_than(&b));
    }
}
