//! Multi-writer **merged log**: leaderless, Raft-free convergence for N concurrent writers.
//!
//! The single-writer [`Replicated`](crate::Replicated) is the right primitive when one device owns a
//! collection. It is the *wrong* one when **any** device must edit offline — a single writer's
//! outage stalls everyone, and Raft (the usual fix) can't make progress while a quorum is offline,
//! which is exactly the local-first / mobile case CE targets. CE Notes and CE Drive both need the
//! same thing instead: every device appends to *its own* log, and readers take the **set union**
//! across all logs, **ordered by an app-supplied total-order key**. The fold of a sorted set is
//! independent of the order entries arrive in — so every replica converges to the same state
//! without any leader, quorum, or coordination round.
//!
//! ## What you implement
//!
//! A [`MergeMachine`]: a deterministic state machine whose op carries (or yields) a strict
//! total-order [`MergeMachine::key`]. Two ops never share a key (an app pairs a Lamport clock with
//! the writer id — see CE Drive's `Timestamp`). [`apply`](MergeMachine::apply) is called on a fresh
//! state in ascending key order, so the result is a pure function of the op *set*.
//!
//! ## What you get
//!
//! A [`Merged<M>`]: open it on every device with that device's id and the ids of its peers.
//! * [`propose`](Merged::propose) appends one op to *this* device's writer log (and broadcasts it).
//! * [`read`](Merged::read) folds the whole merged, key-ordered op set into `M` and hands it to you.
//! * [`watch`](Merged::watch) fires whenever the merged set grows (local write or any peer's op).
//! * [`add_writer`](Merged::add_writer) starts following a newly-added device.
//!
//! Each per-writer log still rides ce-coord's exactly-once, in-order, gap-repaired, snapshot-capable
//! machinery (it *is* a [`Replicated`]); the merge layer sits on top and only ever takes a union.
//! This is the substrate CE Notes' `MergeLog`/`MergeSet` and CE Drive's `DriveTree` move-CRDT want:
//! true multi-device concurrent editing with deterministic conflict resolution and no leader.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::watch;

use crate::replicated::{Replicated, StateMachine, Version};
use crate::Coord;

/// A strict total-order key over ops. An app pairs a Lamport clock with the writer id so no two ops
/// ever collide; ordering is the natural `Ord` of the key. CE Drive's `Timestamp { lamport, replica }`
/// is the canonical example.
pub trait MergeKey: Ord + Clone + Send + Sync + 'static {}
impl<T: Ord + Clone + Send + Sync + 'static> MergeKey for T {}

/// A deterministic state machine folded from a key-ordered **set** of ops. Unlike [`StateMachine`]
/// (which is folded in arrival/version order from a single writer), a `MergeMachine` is always
/// folded from `M::default()` over the *union* of every writer's ops, sorted by [`key`](Self::key).
/// That makes the result a pure function of the op set — identical on every replica, regardless of
/// delivery order or which writer produced what.
///
/// Two distinct ops MUST yield distinct keys (use a Lamport clock + writer id). Duplicate keys are
/// treated as the same op and deduplicated.
pub trait MergeMachine: Default + Send + 'static {
    /// The op type. Must round-trip through JSON to ride the mesh.
    type Op: Serialize + DeserializeOwned + Clone + Send + 'static;

    /// The strict total-order key. Distinct ops must have distinct keys.
    type Key: MergeKey;

    /// Extract the ordering key for an op. Must be a pure function of the op.
    fn key(op: &Self::Op) -> Self::Key;

    /// Apply one op. Called on a fresh `Self::default()` in ascending key order during a fold, so
    /// the only ordering an implementation ever sees is the canonical one. Keep it deterministic.
    fn apply(&mut self, op: Self::Op);
}

/// The per-writer state machine the merge layer replicates: an append-only list of one writer's
/// ops, in the order that writer proposed them. It is a plain [`StateMachine`] (single writer), so
/// it inherits ce-coord's ordering, gap repair, catch-up, and — when the op is `Snapshot`-able via
/// the wrapper — checkpointing. The merge layer reads the accumulated ops out and unions them.
pub struct WriterLog<Op> {
    ops: Vec<Op>,
}

impl<Op> Default for WriterLog<Op> {
    fn default() -> Self {
        WriterLog { ops: Vec::new() }
    }
}

impl<Op: Serialize + DeserializeOwned + Clone + Send + 'static> StateMachine for WriterLog<Op> {
    type Op = Op;
    fn apply(&mut self, op: Op) {
        self.ops.push(op);
    }
}

impl<Op: Clone> WriterLog<Op> {
    /// The ops at index `from..` (versions `from+1..`). Cheap clone of the tail.
    fn ops_from(&self, from: usize) -> Vec<Op> {
        if from >= self.ops.len() { Vec::new() } else { self.ops[from..].to_vec() }
    }
}

/// Shared, mergeable state across one device's writer log and all peer reader logs.
struct Shared<M: MergeMachine> {
    /// The deduped, key-ordered union of every op seen, from any writer. The fold of this *is* the
    /// converged state. Keyed by [`MergeMachine::Key`] so insertion is idempotent and ordered.
    union: BTreeMap<M::Key, M::Op>,
}

impl<M: MergeMachine> Shared<M> {
    fn fold(&self) -> M {
        let mut m = M::default();
        for op in self.union.values() {
            m.apply(op.clone());
        }
        m
    }
}

/// One device's multi-writer merged log for a collection `name`. Holds this device's writer log plus
/// one reader log per peer device; exposes the key-ordered union of all of them folded into `M`.
///
/// Convergence is leaderless: every device appends only to its own log, so an offline device keeps
/// editing; when logs reconnect the union (and thus the fold) is identical everywhere. No Raft, no
/// quorum, no elected writer.
pub struct Merged<M: MergeMachine> {
    /// This device's own writer log — its outbound ops.
    writer: Replicated<WriterLog<M::Op>>,
    /// `device_id -> (reader log, ops already pulled into the union)`. One per *other* member.
    readers: Mutex<BTreeMap<String, ReaderState<M>>>,
    /// This device's id, and how many of our own ops we've folded into the union.
    self_id: String,
    self_drained: Mutex<usize>,
    /// The deduped, key-ordered union + a change signal.
    shared: Arc<Mutex<Shared<M>>>,
    ver_tx: watch::Sender<u64>,
    coord: Coord,
    name: String,
}

struct ReaderState<M: MergeMachine> {
    log: Replicated<WriterLog<M::Op>>,
    drained: usize,
}

impl<M: MergeMachine> Merged<M> {
    /// Open the merged log for `name` on this device. `self_id` is this device's NodeId hex;
    /// `peer_ids` are the *other* member devices whose logs to follow (this device's own id is
    /// ignored if present). New members can be added later with [`add_writer`](Self::add_writer).
    pub async fn open(
        coord: &Coord,
        name: &str,
        self_id: &str,
        peer_ids: &[String],
    ) -> Result<Merged<M>> {
        let writer = Replicated::<WriterLog<M::Op>>::writer(coord.clone(), name).await?;
        let mut readers = BTreeMap::new();
        for peer in peer_ids {
            if peer == self_id || readers.contains_key(peer) {
                continue;
            }
            let log = Replicated::<WriterLog<M::Op>>::reader(coord.clone(), name, peer).await?;
            readers.insert(peer.clone(), ReaderState { log, drained: 0 });
        }
        let (ver_tx, _) = watch::channel(0u64);
        Ok(Merged {
            writer,
            readers: Mutex::new(readers),
            self_id: self_id.to_string(),
            self_drained: Mutex::new(0),
            shared: Arc::new(Mutex::new(Shared { union: BTreeMap::new() })),
            ver_tx,
            coord: coord.clone(),
            name: name.to_string(),
        })
    }

    /// Start following a peer device's writer log (e.g. a newly added member). Idempotent; never
    /// follows this device's own log.
    pub async fn add_writer(&self, peer_id: &str) -> Result<()> {
        if peer_id == self.self_id {
            return Ok(());
        }
        {
            if self.readers.lock().unwrap().contains_key(peer_id) {
                return Ok(());
            }
        }
        let log =
            Replicated::<WriterLog<M::Op>>::reader(self.coord.clone(), &self.name, peer_id).await?;
        self.readers
            .lock()
            .unwrap()
            .entry(peer_id.to_string())
            .or_insert(ReaderState { log, drained: 0 });
        Ok(())
    }

    /// Append one op to this device's writer log and broadcast it. Returns the [`Version`] in this
    /// device's own log. The op is also folded into the local union immediately (so local reads see
    /// it before any peer does).
    pub async fn propose(&self, op: M::Op) -> Result<Version> {
        let v = self.writer.propose(op).await?;
        self.pull(); // fold our own fresh op (and anything else newly arrived) into the union
        Ok(v)
    }

    /// Pull every op not yet folded — from this device's writer log and all peer reader logs — into
    /// the key-ordered union. Idempotent (dedup by key). Fires the change watch if the union grew.
    /// Called automatically by [`propose`](Self::propose) and [`read`](Self::read); call it directly
    /// to force a refresh after peers have delivered ops in the background.
    pub fn pull(&self) {
        let mut grew = false;
        let mut shared = self.shared.lock().unwrap();

        // Our own writer log first.
        {
            let mut drained = self.self_drained.lock().unwrap();
            let fresh = self.writer.read(|log| log.ops_from(*drained));
            if !fresh.is_empty() {
                *drained += fresh.len();
                for op in fresh {
                    if shared.union.insert(M::key(&op), op).is_none() {
                        grew = true;
                    }
                }
            }
        }

        // Then each peer reader log.
        {
            let mut readers = self.readers.lock().unwrap();
            for state in readers.values_mut() {
                let fresh = state.log.read(|log| log.ops_from(state.drained));
                if !fresh.is_empty() {
                    state.drained += fresh.len();
                    for op in fresh {
                        if shared.union.insert(M::key(&op), op).is_none() {
                            grew = true;
                        }
                    }
                }
            }
        }

        if grew {
            let n = shared.union.len() as u64;
            drop(shared);
            let _ = self.ver_tx.send(n);
        }
    }

    /// Fold the current merged, key-ordered op set into a fresh `M` and hand it to `f`. Pulls newly
    /// arrived ops first, so the state reflects everything delivered so far. The fold is O(n); for
    /// hot read paths, cache the result and refresh on [`watch`](Self::watch).
    pub fn read<R>(&self, f: impl FnOnce(&M) -> R) -> R {
        self.pull();
        let folded = self.shared.lock().unwrap().fold();
        f(&folded)
    }

    /// The number of distinct ops in the merged union (across all writers). Cheap; pulls first.
    pub fn op_count(&self) -> usize {
        self.pull();
        self.shared.lock().unwrap().union.len()
    }

    /// The merged, key-ordered op set as a snapshot (for debugging / re-broadcast). Pulls first.
    pub fn merged_ops(&self) -> Vec<M::Op> {
        self.pull();
        self.shared.lock().unwrap().union.values().cloned().collect()
    }

    /// A watch receiver that fires (with the new union size) whenever the merged set grows — from a
    /// local [`propose`](Self::propose) or any peer's op arriving and being pulled.
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.ver_tx.subscribe()
    }

    /// Per-writer applied versions for a sync-status display: `(device_id, applied_version)`. The
    /// first entry is this device's own writer log.
    pub fn sync_status(&self) -> Vec<(String, Version)> {
        let mut v = vec![(self.self_id.clone(), self.writer.version())];
        for (peer, state) in self.readers.lock().unwrap().iter() {
            v.push((peer.clone(), state.log.version()));
        }
        v
    }

    /// This device's own writer log (escape hatch for checkpointing it — see
    /// [`Replicated::checkpoint`](crate::Replicated)). The op type must be `Snapshot`-able for that.
    pub fn writer_log(&self) -> &Replicated<WriterLog<M::Op>> {
        &self.writer
    }
}

impl<Op> crate::snapshot::Snapshot for WriterLog<Op>
where
    Op: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    fn save(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.ops).map_err(Into::into)
    }
    fn load(bytes: &[u8]) -> Result<Self> {
        Ok(WriterLog { ops: serde_json::from_slice(bytes)? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    // A merge machine modelling a last-writer-wins register *per key*, where "last" is decided by the
    // total-order key (lamport, replica) — NOT delivery order. This is a non-commutative apply (a
    // later Set clobbers an earlier one), so it only converges because the fold is over the sorted
    // union. That makes it a strict test of order-independence.
    #[derive(Default, Clone, PartialEq, Eq, Debug)]
    struct Lww {
        // key -> (winning value, the ts that set it)
        map: std::collections::BTreeMap<String, (i64, Ts)>,
    }

    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
    struct Ts {
        lamport: u64,
        replica: String,
    }

    #[derive(Clone, Serialize, Deserialize)]
    struct LwwOp {
        ts: Ts,
        key: String,
        val: i64,
    }

    impl MergeMachine for Lww {
        type Op = LwwOp;
        type Key = Ts;
        fn key(op: &LwwOp) -> Ts {
            op.ts.clone()
        }
        fn apply(&mut self, op: LwwOp) {
            // Applied in ascending ts order, so the last write for a key wins naturally.
            self.map.insert(op.key, (op.val, op.ts));
        }
    }

    // Fold a set of ops the way Merged does: dedup into a key-ordered BTreeMap, then fold.
    fn fold_union(ops: &[LwwOp]) -> Lww {
        let mut union: BTreeMap<Ts, LwwOp> = BTreeMap::new();
        for op in ops {
            union.insert(op.ts.clone(), op.clone());
        }
        let mut m = Lww::default();
        for op in union.values() {
            m.apply(op.clone());
        }
        m
    }

    fn op(lamport: u64, replica: &str, key: &str, val: i64) -> LwwOp {
        LwwOp { ts: Ts { lamport, replica: replica.into() }, key: key.into(), val }
    }

    /// Three writers, each producing ops concurrently; the merged fold is identical no matter what
    /// order ops are delivered/merged in. This is the leaderless-convergence guarantee.
    #[test]
    fn concurrent_multiwriter_merge_converges_regardless_of_order() {
        let a = vec![op(1, "a", "x", 10), op(4, "a", "y", 11), op(7, "a", "x", 12)];
        let b = vec![op(2, "b", "x", 20), op(5, "b", "z", 21), op(8, "b", "y", 22)];
        let c = vec![op(3, "c", "z", 30), op(6, "c", "x", 31), op(9, "c", "y", 32)];

        // Reference: all ops in one canonical order.
        let mut all: Vec<LwwOp> = Vec::new();
        all.extend(a.iter().cloned());
        all.extend(b.iter().cloned());
        all.extend(c.iter().cloned());
        let reference = fold_union(&all);

        // Many delivery permutations (interleavings of the three writers' streams). Each is a valid
        // out-of-order delivery; all must converge to `reference`.
        let perms: Vec<Vec<&LwwOp>> = vec![
            // forward interleave
            a.iter().chain(b.iter()).chain(c.iter()).collect(),
            // reverse-ish
            c.iter().rev().chain(b.iter().rev()).chain(a.iter().rev()).collect(),
            // round-robin
            (0..3)
                .flat_map(|i| [a.get(i), b.get(i), c.get(i)])
                .flatten()
                .collect(),
            // b first, then c, then a
            b.iter().chain(c.iter()).chain(a.iter()).collect(),
            // fully shuffled fixed order
            vec![
                &c[2], &a[0], &b[1], &c[0], &a[2], &b[0], &c[1], &a[1], &b[2],
            ],
        ];

        for (i, perm) in perms.iter().enumerate() {
            let owned: Vec<LwwOp> = perm.iter().map(|o| (*o).clone()).collect();
            let got = fold_union(&owned);
            assert_eq!(got, reference, "permutation {i} diverged");
        }

        // And the winning value for "x" is the highest-ts op (lamport 7, replica a -> 12).
        assert_eq!(reference.map["x"].0, 12);
        assert_eq!(reference.map["y"].0, 32); // lamport 9 c
        assert_eq!(reference.map["z"].0, 21); // lamport 5 b beats lamport 3 c
    }

    /// Duplicate delivery of the same op (same key) is idempotent — the union dedups by key.
    #[test]
    fn duplicate_delivery_is_idempotent() {
        let ops = vec![op(1, "a", "x", 1), op(2, "b", "x", 2)];
        let once = fold_union(&ops);
        // Deliver everything twice, plus an extra copy out of order.
        let mut twice = ops.clone();
        twice.extend(ops.iter().cloned());
        twice.insert(0, op(2, "b", "x", 2));
        assert_eq!(fold_union(&twice), once, "duplicates changed the fold");
    }

    /// WriterLog accumulates in order and tails correctly (the per-writer substrate the merge layer
    /// drains).
    #[test]
    fn writerlog_accumulates_and_tails() {
        let mut log = WriterLog::<i64>::default();
        log.apply(1);
        log.apply(2);
        log.apply(3);
        assert_eq!(log.ops_from(0), vec![1, 2, 3]);
        assert_eq!(log.ops_from(1), vec![2, 3]);
        assert_eq!(log.ops_from(3), Vec::<i64>::new());
        assert_eq!(log.ops_from(9), Vec::<i64>::new());
    }

    /// WriterLog snapshot round-trips (so a multi-writer log can be checkpointed/compacted too).
    #[test]
    fn writerlog_snapshot_roundtrips() {
        use crate::snapshot::Snapshot;
        let mut log = WriterLog::<String>::default();
        log.apply("a".into());
        log.apply("b".into());
        let bytes = log.save().unwrap();
        let back = WriterLog::<String>::load(&bytes).unwrap();
        assert_eq!(back.ops_from(0), vec!["a".to_string(), "b".to_string()]);
    }

    /// A commutative CRDT op (grow-only set add) converges trivially — included to mirror CE Notes'
    /// commutative payloads, where key order is irrelevant but the union+fold still holds.
    #[test]
    fn commutative_ops_converge() {
        #[derive(Default, Clone, PartialEq, Eq, Debug)]
        struct GSet {
            items: std::collections::BTreeSet<String>,
        }
        #[derive(Clone, Serialize, Deserialize)]
        struct AddOp {
            seq: u64,
            replica: String,
            item: String,
        }
        impl MergeMachine for GSet {
            type Op = AddOp;
            type Key = (u64, String);
            fn key(op: &AddOp) -> (u64, String) {
                (op.seq, op.replica.clone())
            }
            fn apply(&mut self, op: AddOp) {
                self.items.insert(op.item);
            }
        }
        let mk = |seq, r: &str, item: &str| AddOp { seq, replica: r.into(), item: item.into() };
        let ops = vec![mk(1, "a", "x"), mk(1, "b", "y"), mk(2, "a", "z")];

        let fold = |order: &[usize]| {
            let mut union: BTreeMap<(u64, String), AddOp> = BTreeMap::new();
            for &i in order {
                union.insert(GSet::key(&ops[i]), ops[i].clone());
            }
            let mut g = GSet::default();
            for o in union.values() {
                g.apply(o.clone());
            }
            g
        };
        let r = fold(&[0, 1, 2]);
        assert_eq!(fold(&[2, 1, 0]), r);
        assert_eq!(fold(&[1, 0, 2]), r);
        assert_eq!(r.items.len(), 3);
    }

    // -------------------------------------------------------------------------------------------
    // Property tests: heavy proptest on multi-writer convergence. The merge layer's contract is
    // that the key-ordered union fold is a pure function of the op *set*, independent of:
    //   * how many writers produced ops,
    //   * the interleaving/delivery order across writers,
    //   * duplicate delivery,
    //   * when a writer is "added" (its ops fold in identically whenever they arrive).
    // -------------------------------------------------------------------------------------------
    use proptest::prelude::*;

    // The canonical Merged fold, mirroring `Shared::fold` exactly: dedup into a key-ordered BTreeMap,
    // then fold from default in ascending key order.
    fn merged_fold(ops: &[LwwOp]) -> Lww {
        let mut union: BTreeMap<Ts, LwwOp> = BTreeMap::new();
        for op in ops {
            union.insert(Lww::key(op), op.clone());
        }
        let mut m = Lww::default();
        for op in union.values() {
            m.apply(op.clone());
        }
        m
    }

    // Build N writers' op streams from a flat list of (writer, _lam, key, val). The MergeKey
    // contract REQUIRES distinct ops to have distinct keys (a Lamport clock + writer id), so we
    // assign each op a per-writer monotonic lamport derived from its position — guaranteeing the
    // key `(lamport, replica)` is globally unique. (Feeding colliding keys would violate the trait's
    // precondition and is meaningless to test.)
    fn writers_from(raw: &[(u8, u64, u8, i64)]) -> Vec<LwwOp> {
        let mut per_writer: std::collections::HashMap<u8, u64> = std::collections::HashMap::new();
        raw.iter()
            .map(|(w, _lam, k, v)| {
                let lam = per_writer.entry(*w).or_insert(0);
                *lam += 1;
                op(*lam, &format!("w{w}"), &format!("k{}", k % 5), *v)
            })
            .collect()
    }

    proptest! {
        // N concurrent writers; ALL delivery orders (including a fully reversed and a deterministic
        // shuffle) converge to the same fold, which equals the canonical in-order fold.
        #[test]
        fn prop_multiwriter_all_orders_converge(
            raw in proptest::collection::vec(
                (0u8..4, 0u64..50, 0u8..8, -1000i64..1000), 1..40),
            seed in any::<u64>(),
        ) {
            let ops = writers_from(&raw);
            let reference = merged_fold(&ops);

            // forward, reversed, and an LCG shuffle — all over the SAME op set.
            let mut shuffled = ops.clone();
            let mut s = seed | 1;
            for i in (1..shuffled.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (s >> 33) as usize % (i + 1);
                shuffled.swap(i, j);
            }
            let mut reversed = ops.clone();
            reversed.reverse();

            prop_assert_eq!(merged_fold(&reversed), reference.clone());
            prop_assert_eq!(merged_fold(&shuffled), reference.clone());
        }

        // Idempotent + commutative: delivering the op set twice (and concatenated with a shuffled
        // copy) yields the identical fold. Duplicates are deduped by key.
        #[test]
        fn prop_multiwriter_idempotent_commutative(
            raw in proptest::collection::vec(
                (0u8..4, 0u64..50, 0u8..8, -1000i64..1000), 1..40),
        ) {
            let ops = writers_from(&raw);
            let once = merged_fold(&ops);
            let mut doubled = ops.clone();
            doubled.extend(ops.iter().rev().cloned()); // duplicates, reversed
            prop_assert_eq!(merged_fold(&doubled), once);
        }

        // add_writer mid-stream: a writer whose ops arrive late folds in identically. Modelled by
        // folding a "partial" union (some writers), then later unioning the rest -> same as folding
        // all at once. This is exactly what `Merged::pull` does incrementally.
        #[test]
        fn prop_add_writer_midstream_equivalent(
            raw in proptest::collection::vec(
                (0u8..5, 0u64..60, 0u8..8, -500i64..500), 2..50),
            split in 1usize..49,
        ) {
            let ops = writers_from(&raw);
            let full = merged_fold(&ops);

            // Incremental: build the union in two passes (early members, then a "newly added"
            // writer's backlog), exactly as Merged accumulates into one shared BTreeMap.
            let cut = split.min(ops.len().saturating_sub(1)).max(1);
            let mut union: BTreeMap<Ts, LwwOp> = BTreeMap::new();
            for o in &ops[..cut] { union.insert(Lww::key(o), o.clone()); }
            // pull again on the same union (idempotent), then add the rest.
            for o in &ops[..cut] { union.insert(Lww::key(o), o.clone()); }
            for o in &ops[cut..] { union.insert(Lww::key(o), o.clone()); }
            let mut m = Lww::default();
            for o in union.values() { m.apply(o.clone()); }
            prop_assert_eq!(m, full);
        }
    }
}
