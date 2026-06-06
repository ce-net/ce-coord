//! Replicated collections — the ergonomic layer over [`Replicated`]. Each is one writer + N readers,
//! mirroring `mosaik`'s `Map`/`Vec`/`Set`/`Cell`. [`RMap`] is implemented here; the others are the
//! same three lines (a state struct + an op enum + a `StateMachine` impl) and noted at the bottom.

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::replicated::{Replicated, StateMachine, Version};
use crate::Coord;

/// Bounds a replicated key must satisfy: serializable (to ride the mesh), hashable, clonable.
pub trait Key: Serialize + DeserializeOwned + Eq + Hash + Clone + Send + 'static {}
impl<T: Serialize + DeserializeOwned + Eq + Hash + Clone + Send + 'static> Key for T {}

/// Bounds a replicated value must satisfy.
pub trait Val: Serialize + DeserializeOwned + Clone + Send + 'static {}
impl<T: Serialize + DeserializeOwned + Clone + Send + 'static> Val for T {}

/// The replicated state behind an [`RMap`].
struct MapState<K, V> {
    map: HashMap<K, V>,
}

// Manual `Default` so we don't force `K: Default, V: Default` the way `derive` would.
impl<K, V> Default for MapState<K, V> {
    fn default() -> Self {
        MapState { map: HashMap::new() }
    }
}

/// A mutation on an [`RMap`].
#[derive(Serialize, Deserialize)]
enum MapOp<K, V> {
    Insert(K, V),
    Remove(K),
    Clear,
}

impl<K: Key, V: Val> StateMachine for MapState<K, V> {
    type Op = MapOp<K, V>;
    fn apply(&mut self, op: MapOp<K, V>) {
        match op {
            MapOp::Insert(k, v) => {
                self.map.insert(k, v);
            }
            MapOp::Remove(k) => {
                self.map.remove(&k);
            }
            MapOp::Clear => self.map.clear(),
        }
    }
}

/// A replicated map: one **writer** mutates it, any number of **readers** track it. Writes return a
/// [`Version`]; readers [`await_version`](Self::await_version) to confirm they've converged.
pub struct RMap<K: Key, V: Val> {
    inner: Replicated<MapState<K, V>>,
}

impl<K: Key, V: Val> RMap<K, V> {
    // ----- writer mutations (error on a read replica) -----

    /// Insert or overwrite a key. Returns the version at which the change is visible.
    pub async fn insert(&self, k: K, v: V) -> Result<Version> {
        self.inner.propose(MapOp::Insert(k, v)).await
    }

    /// Remove a key.
    pub async fn remove(&self, k: K) -> Result<Version> {
        self.inner.propose(MapOp::Remove(k)).await
    }

    /// Clear all entries.
    pub async fn clear(&self) -> Result<Version> {
        self.inner.propose(MapOp::Clear).await
    }

    // ----- local reads (writer or reader) -----

    /// Current value for `k`, if present on this replica.
    pub fn get(&self, k: &K) -> Option<V> {
        self.inner.read(|s| s.map.get(k).cloned())
    }

    /// Number of entries on this replica.
    pub fn len(&self) -> usize {
        self.inner.read(|s| s.map.len())
    }

    /// True if this replica holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of all entries on this replica.
    pub fn entries(&self) -> Vec<(K, V)> {
        self.inner.read(|s| s.map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    // ----- convergence -----

    /// Highest version applied here.
    pub fn version(&self) -> Version {
        self.inner.version()
    }

    /// Watch receiver that fires whenever this replica advances.
    pub fn version_watch(&self) -> watch::Receiver<Version> {
        self.inner.version_watch()
    }

    /// Resolve once this replica has applied at least version `v`.
    pub async fn await_version(&self, v: Version) {
        self.inner.await_version(v).await
    }
}

impl Coord {
    /// Open a replicated map this node **writes** to (`name`).
    pub async fn map_writer<K: Key, V: Val>(&self, name: &str) -> Result<RMap<K, V>> {
        Ok(RMap { inner: Replicated::writer(self.clone(), name).await? })
    }

    /// Open a **read replica** of the map `name` published by `writer` (its NodeId hex).
    pub async fn map_reader<K: Key, V: Val>(
        &self,
        name: &str,
        writer: &str,
    ) -> Result<RMap<K, V>> {
        Ok(RMap { inner: Replicated::reader(self.clone(), name, writer).await? })
    }
}

// ===========================================================================================
// The remaining collections are the same three-part pattern as `RMap`: a state struct, an op
// enum, and a `StateMachine` impl — then a thin typed wrapper and the `Coord` constructors.
// ===========================================================================================

// ----- RSet<T> --------------------------------------------------------------------------------

struct SetState<T> {
    set: HashSet<T>,
}
impl<T> Default for SetState<T> {
    fn default() -> Self {
        SetState { set: HashSet::new() }
    }
}

#[derive(Serialize, Deserialize)]
enum SetOp<T> {
    Add(T),
    Remove(T),
    Clear,
}

impl<T: Key> StateMachine for SetState<T> {
    type Op = SetOp<T>;
    fn apply(&mut self, op: SetOp<T>) {
        match op {
            SetOp::Add(t) => {
                self.set.insert(t);
            }
            SetOp::Remove(t) => {
                self.set.remove(&t);
            }
            SetOp::Clear => self.set.clear(),
        }
    }
}

/// A replicated set: one writer, N readers.
pub struct RSet<T: Key> {
    inner: Replicated<SetState<T>>,
}

impl<T: Key> RSet<T> {
    pub async fn add(&self, t: T) -> Result<Version> {
        self.inner.propose(SetOp::Add(t)).await
    }
    pub async fn remove(&self, t: T) -> Result<Version> {
        self.inner.propose(SetOp::Remove(t)).await
    }
    pub async fn clear(&self) -> Result<Version> {
        self.inner.propose(SetOp::Clear).await
    }
    pub fn contains(&self, t: &T) -> bool {
        self.inner.read(|s| s.set.contains(t))
    }
    pub fn len(&self) -> usize {
        self.inner.read(|s| s.set.len())
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn entries(&self) -> Vec<T> {
        self.inner.read(|s| s.set.iter().cloned().collect())
    }
    pub fn version(&self) -> Version {
        self.inner.version()
    }
    pub fn version_watch(&self) -> watch::Receiver<Version> {
        self.inner.version_watch()
    }
    pub async fn await_version(&self, v: Version) {
        self.inner.await_version(v).await
    }
}

// ----- RVec<T> --------------------------------------------------------------------------------

struct VecState<T> {
    vec: Vec<T>,
}
impl<T> Default for VecState<T> {
    fn default() -> Self {
        VecState { vec: Vec::new() }
    }
}

#[derive(Serialize, Deserialize)]
enum VecOp<T> {
    Push(T),
    Set(u64, T),
    Truncate(u64),
    Clear,
}

impl<T: Val> StateMachine for VecState<T> {
    type Op = VecOp<T>;
    fn apply(&mut self, op: VecOp<T>) {
        match op {
            VecOp::Push(t) => self.vec.push(t),
            VecOp::Set(i, t) => {
                if let Some(slot) = self.vec.get_mut(i as usize) {
                    *slot = t;
                }
            }
            VecOp::Truncate(n) => self.vec.truncate(n as usize),
            VecOp::Clear => self.vec.clear(),
        }
    }
}

/// A replicated append-mostly vector: one writer, N readers.
pub struct RVec<T: Val> {
    inner: Replicated<VecState<T>>,
}

impl<T: Val> RVec<T> {
    pub async fn push(&self, t: T) -> Result<Version> {
        self.inner.propose(VecOp::Push(t)).await
    }
    pub async fn set(&self, index: u64, t: T) -> Result<Version> {
        self.inner.propose(VecOp::Set(index, t)).await
    }
    pub async fn truncate(&self, len: u64) -> Result<Version> {
        self.inner.propose(VecOp::Truncate(len)).await
    }
    pub async fn clear(&self) -> Result<Version> {
        self.inner.propose(VecOp::Clear).await
    }
    pub fn get(&self, index: usize) -> Option<T> {
        self.inner.read(|s| s.vec.get(index).cloned())
    }
    pub fn len(&self) -> usize {
        self.inner.read(|s| s.vec.len())
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn entries(&self) -> Vec<T> {
        self.inner.read(|s| s.vec.clone())
    }
    pub fn version(&self) -> Version {
        self.inner.version()
    }
    pub fn version_watch(&self) -> watch::Receiver<Version> {
        self.inner.version_watch()
    }
    pub async fn await_version(&self, v: Version) {
        self.inner.await_version(v).await
    }
}

// ----- RCell<V> -------------------------------------------------------------------------------

struct CellState<V> {
    val: Option<V>,
}
impl<V> Default for CellState<V> {
    fn default() -> Self {
        CellState { val: None }
    }
}

#[derive(Serialize, Deserialize)]
enum CellOp<V> {
    Set(V),
    Clear,
}

impl<V: Val> StateMachine for CellState<V> {
    type Op = CellOp<V>;
    fn apply(&mut self, op: CellOp<V>) {
        match op {
            CellOp::Set(v) => self.val = Some(v),
            CellOp::Clear => self.val = None,
        }
    }
}

/// A replicated single value (last-writer-wins cell): one writer, N readers.
pub struct RCell<V: Val> {
    inner: Replicated<CellState<V>>,
}

impl<V: Val> RCell<V> {
    pub async fn set(&self, v: V) -> Result<Version> {
        self.inner.propose(CellOp::Set(v)).await
    }
    pub async fn clear(&self) -> Result<Version> {
        self.inner.propose(CellOp::Clear).await
    }
    pub fn get(&self) -> Option<V> {
        self.inner.read(|s| s.val.clone())
    }
    pub fn version(&self) -> Version {
        self.inner.version()
    }
    pub fn version_watch(&self) -> watch::Receiver<Version> {
        self.inner.version_watch()
    }
    pub async fn await_version(&self, v: Version) {
        self.inner.await_version(v).await
    }
}

// ----- RCounter -------------------------------------------------------------------------------

#[derive(Default)]
struct CounterState {
    n: i64,
}

#[derive(Serialize, Deserialize)]
enum CounterOp {
    Add(i64),
}

impl StateMachine for CounterState {
    type Op = CounterOp;
    fn apply(&mut self, op: CounterOp) {
        match op {
            CounterOp::Add(d) => self.n += d,
        }
    }
}

/// A replicated integer counter: one writer, N readers.
pub struct RCounter {
    inner: Replicated<CounterState>,
}

impl RCounter {
    pub async fn add(&self, delta: i64) -> Result<Version> {
        self.inner.propose(CounterOp::Add(delta)).await
    }
    pub async fn incr(&self) -> Result<Version> {
        self.add(1).await
    }
    pub fn get(&self) -> i64 {
        self.inner.read(|s| s.n)
    }
    pub fn version(&self) -> Version {
        self.inner.version()
    }
    pub fn version_watch(&self) -> watch::Receiver<Version> {
        self.inner.version_watch()
    }
    pub async fn await_version(&self, v: Version) {
        self.inner.await_version(v).await
    }
}

// ----- Coord constructors for every collection ------------------------------------------------

impl Coord {
    /// Open a replicated set this node **writes** to.
    pub async fn set_writer<T: Key>(&self, name: &str) -> Result<RSet<T>> {
        Ok(RSet { inner: Replicated::writer(self.clone(), name).await? })
    }
    /// Open a **read replica** of the set `name` published by `writer`.
    pub async fn set_reader<T: Key>(&self, name: &str, writer: &str) -> Result<RSet<T>> {
        Ok(RSet { inner: Replicated::reader(self.clone(), name, writer).await? })
    }

    /// Open a replicated vector this node **writes** to.
    pub async fn vec_writer<T: Val>(&self, name: &str) -> Result<RVec<T>> {
        Ok(RVec { inner: Replicated::writer(self.clone(), name).await? })
    }
    /// Open a **read replica** of the vector `name` published by `writer`.
    pub async fn vec_reader<T: Val>(&self, name: &str, writer: &str) -> Result<RVec<T>> {
        Ok(RVec { inner: Replicated::reader(self.clone(), name, writer).await? })
    }

    /// Open a replicated cell this node **writes** to.
    pub async fn cell_writer<V: Val>(&self, name: &str) -> Result<RCell<V>> {
        Ok(RCell { inner: Replicated::writer(self.clone(), name).await? })
    }
    /// Open a **read replica** of the cell `name` published by `writer`.
    pub async fn cell_reader<V: Val>(&self, name: &str, writer: &str) -> Result<RCell<V>> {
        Ok(RCell { inner: Replicated::reader(self.clone(), name, writer).await? })
    }

    /// Open a replicated counter this node **writes** to.
    pub async fn counter_writer(&self, name: &str) -> Result<RCounter> {
        Ok(RCounter { inner: Replicated::writer(self.clone(), name).await? })
    }
    /// Open a **read replica** of the counter `name` published by `writer`.
    pub async fn counter_reader(&self, name: &str, writer: &str) -> Result<RCounter> {
        Ok(RCounter { inner: Replicated::reader(self.clone(), name, writer).await? })
    }
}
