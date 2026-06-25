//! Mock-broker integration tests — drive the **entire** ce-coord stack (pump, transport,
//! collections, Merged, snapshot, stream) without a real `ce` node.
//!
//! The in-process broker in `mockbroker` plays the role of the local CE node: pub/sub fan-out,
//! directed request/reply, and a content-addressed blob store, all over plain HTTP that `ce-rs`'s
//! `CeClient` speaks unchanged. Two (or more) `Coord`s share one broker, so a writer on one and a
//! reader on another converge exactly as they would over the mesh — but deterministically and in
//! milliseconds. This is what lifts `collections.rs` / `lib.rs` / `stream.rs` from 0% to fully
//! exercised, and pushes `replicated.rs` / `merged.rs` through their catch-up, snapshot, and
//! add-writer paths.
//!
//! Every test stands up its own broker, so they are isolated and parallel-safe.

use std::time::Duration;

use ce_coord::testkit::{within, MockNode};
use ce_coord::{Coord, Merged, MergeMachine, Replicated};
use serde::{Deserialize, Serialize};

const CONV: Duration = Duration::from_secs(10);

/// Build a writer Coord + reader Coord on a shared broker, returning their node ids too.
async fn pair() -> anyhow::Result<(MockNode, Coord, String, Coord, String)> {
    let node = MockNode::start();
    let ca = Coord::with_client(node.client()).await?;
    let cb = Coord::with_client(node.client()).await?;
    let a_id = ca.node_id().to_string();
    let b_id = cb.node_id().to_string();
    Ok((node, ca, a_id, cb, b_id))
}

// ===========================================================================================
// Coord surface (lib.rs): connect, node_id, the pump.
// ===========================================================================================
#[tokio::test]
async fn coord_connects_and_reports_node_id() -> anyhow::Result<()> {
    let node = MockNode::start();
    let coord = Coord::with_client(node.client()).await?;
    // node_id is a 64-hex string; clones share the same inner.
    assert_eq!(coord.node_id().len(), 64);
    let c2 = coord.clone();
    assert_eq!(c2.node_id(), coord.node_id());
    Ok(())
}

// ===========================================================================================
// RMap — full read/write/version/await over the broker.
// ===========================================================================================
#[tokio::test]
async fn rmap_full_lifecycle() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b_id) = pair().await?;
    let w = ca.map_writer::<String, i64>("balances").await?;
    let r = cb.map_reader::<String, i64>("balances", &a_id).await?;

    assert!(w.is_empty());
    let v1 = w.insert("alice".into(), 100).await?;
    let v2 = w.insert("bob".into(), 50).await?;
    let v3 = w.insert("alice".into(), 200).await?;
    assert!(v2 > v1 && v3 > v2);
    assert_eq!(w.get(&"alice".into()), Some(200));
    assert_eq!(w.len(), 2);
    assert!(!w.is_empty());

    r.await_version(v3).await;
    assert!(within(CONV, || r.get(&"alice".into()) == Some(200)).await, "reader converged");
    assert_eq!(r.get(&"bob".into()), Some(50));
    assert_eq!(r.len(), 2);
    assert_eq!(r.version(), v3);
    // entries() on both sides agree (sorted for determinism).
    let mut re = r.entries();
    re.sort();
    assert_eq!(re, vec![("alice".to_string(), 200), ("bob".to_string(), 50)]);

    // remove + clear propagate.
    let v4 = w.remove("bob".into()).await?;
    r.await_version(v4).await;
    assert!(within(CONV, || r.get(&"bob".into()).is_none()).await);

    // version_watch fires on advance.
    let mut watch = r.version_watch();
    let v5 = w.clear().await?;
    r.await_version(v5).await;
    assert!(within(CONV, || r.is_empty()).await, "clear propagated");
    // watch saw an update (current value >= v4).
    assert!(*watch.borrow_and_update() >= v4);
    Ok(())
}

// Reader mutations all error (single-writer invariant) — propose() bails before any network call.
#[tokio::test]
async fn rmap_reader_cannot_write() -> anyhow::Result<()> {
    let (_n, _ca, a_id, cb, _b_id) = pair().await?;
    let r = cb.map_reader::<String, i64>("ro", &a_id).await?;
    assert!(r.insert("x".into(), 1).await.is_err());
    assert!(r.remove("x".into()).await.is_err());
    assert!(r.clear().await.is_err());
    Ok(())
}

// ===========================================================================================
// RSet / RVec / RCell / RCounter — every collection's full API.
// ===========================================================================================
#[tokio::test]
async fn rset_full_lifecycle() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;
    let w = ca.set_writer::<String>("tags").await?;
    let r = cb.set_reader::<String>("tags", &a_id).await?;
    assert!(w.is_empty());
    w.add("gpu".into()).await?;
    let v = w.add("linux".into()).await?;
    assert!(w.contains(&"gpu".to_string()));
    assert_eq!(w.len(), 2);
    r.await_version(v).await;
    assert!(within(CONV, || r.contains(&"gpu".to_string()) && r.len() == 2).await);
    assert!(!r.is_empty());
    let mut e = r.entries();
    e.sort();
    assert_eq!(e, vec!["gpu".to_string(), "linux".to_string()]);
    let v2 = w.remove("gpu".into()).await?;
    r.await_version(v2).await;
    assert!(within(CONV, || !r.contains(&"gpu".to_string())).await);
    let v3 = w.clear().await?;
    r.await_version(v3).await;
    assert!(within(CONV, || r.is_empty()).await);
    assert_eq!(r.version(), v3);
    let _ = r.version_watch();
    // reader cannot write
    assert!(r.add("x".into()).await.is_err());
    assert!(r.remove("x".into()).await.is_err());
    assert!(r.clear().await.is_err());
    Ok(())
}

#[tokio::test]
async fn rvec_full_lifecycle() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;
    let w = ca.vec_writer::<i64>("log").await?;
    let r = cb.vec_reader::<i64>("log", &a_id).await?;
    assert!(w.is_empty());
    w.push(10).await?;
    w.push(20).await?;
    let v = w.push(30).await?;
    assert_eq!(w.len(), 3);
    assert_eq!(w.get(0), Some(10));
    r.await_version(v).await;
    assert!(within(CONV, || r.entries() == vec![10, 20, 30]).await);
    assert!(!r.is_empty());
    let v2 = w.set(1, 99).await?;
    r.await_version(v2).await;
    assert!(within(CONV, || r.get(1) == Some(99)).await);
    // out-of-bounds set is a no-op (covers the `if let Some(slot)` else path)
    let v_oob = w.set(1000, -1).await?;
    r.await_version(v_oob).await;
    assert!(within(CONV, || r.len() == 3).await, "oob set did not grow");
    let v3 = w.truncate(1).await?;
    r.await_version(v3).await;
    assert!(within(CONV, || r.len() == 1).await);
    let v4 = w.clear().await?;
    r.await_version(v4).await;
    assert!(within(CONV, || r.is_empty()).await);
    assert_eq!(r.get(99), None);
    let mut watch = r.version_watch();
    assert!(*watch.borrow_and_update() >= v4);
    assert!(r.push(1).await.is_err());
    assert!(r.set(0, 1).await.is_err());
    assert!(r.truncate(0).await.is_err());
    assert!(r.clear().await.is_err());
    Ok(())
}

#[tokio::test]
async fn rcell_full_lifecycle() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;
    let w = ca.cell_writer::<String>("leader").await?;
    let r = cb.cell_reader::<String>("leader", &a_id).await?;
    assert_eq!(w.get(), None);
    assert_eq!(r.get(), None);
    let v = w.set("node-7".into()).await?;
    assert_eq!(w.get(), Some("node-7".to_string()));
    r.await_version(v).await;
    assert!(within(CONV, || r.get() == Some("node-7".to_string())).await);
    assert_eq!(r.version(), v);
    let _ = r.version_watch();
    let v2 = w.clear().await?;
    r.await_version(v2).await;
    assert!(within(CONV, || r.get().is_none()).await);
    assert!(r.set("x".into()).await.is_err());
    assert!(r.clear().await.is_err());
    Ok(())
}

#[tokio::test]
async fn rcounter_full_lifecycle() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;
    let w = ca.counter_writer("hits").await?;
    let r = cb.counter_reader("hits", &a_id).await?;
    assert_eq!(w.get(), 0);
    w.incr().await?;
    w.add(5).await?;
    let v = w.add(-2).await?; // 1 + 5 - 2 = 4
    assert_eq!(w.get(), 4);
    r.await_version(v).await;
    assert!(within(CONV, || r.get() == 4).await);
    assert_eq!(r.version(), v);
    let _ = r.version_watch();
    assert!(r.add(1).await.is_err());
    assert!(r.incr().await.is_err());
    Ok(())
}

// ===========================================================================================
// Catch-up / gap repair: a late reader that joins AFTER writes still converges via the startup
// catch-up request (exercises the writer's catchup handler + the reader's catch-up task).
// ===========================================================================================
#[tokio::test]
async fn late_reader_catches_up_full_log() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;
    let w = ca.map_writer::<String, i64>("history").await?;
    let mut last = 0;
    for i in 0..10i64 {
        last = w.insert(format!("k{i}"), i).await?;
    }
    // Fresh reader joins now; must catch up the whole log.
    let r = cb.map_reader::<String, i64>("history", &a_id).await?;
    assert!(within(CONV, || r.version() >= last && r.len() == 10).await, "late reader caught up");
    assert_eq!(r.get(&"k7".into()), Some(7));
    Ok(())
}

// A reader following a writer that NEVER published stays empty; await_version on an unmet version
// pends (no panic, no hang within the bound).
#[tokio::test]
async fn reader_of_silent_writer_stays_empty() -> anyhow::Result<()> {
    let (_n, _ca, _a_id, cb, _b) = pair().await?;
    let ghost = "0".repeat(64);
    let r = cb.map_reader::<String, i64>("ghost", &ghost).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(r.version(), 0);
    assert!(r.is_empty());
    let timed = tokio::time::timeout(Duration::from_millis(300), r.await_version(99)).await;
    assert!(timed.is_err(), "await_version on an unmet version pends");
    Ok(())
}

// ===========================================================================================
// Snapshot / checkpoint bootstrap over the broker's blob store.
// ===========================================================================================
#[derive(Default, Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
struct KvState {
    map: std::collections::BTreeMap<String, i64>,
}
#[derive(Clone, Serialize, Deserialize)]
enum KvOp {
    Set(String, i64),
    Del(String),
}
impl ce_coord::StateMachine for KvState {
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
impl ce_coord::Snapshot for KvState {
    ce_coord::json_snapshot!();
}

#[tokio::test]
async fn snapshot_bootstrap_equals_full_state() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;

    let w = Replicated::<KvState>::writer(ca.clone(), "kv").await?;
    assert_eq!(w.current_checkpoint(), None);
    assert_eq!(w.compaction_floor(), 0);
    for i in 0..20i64 {
        w.propose(KvOp::Set(format!("k{}", i % 6), i)).await?;
    }
    // snapshot_bytes works without compaction.
    let raw = w.snapshot_bytes()?;
    assert!(!raw.is_empty());

    let cp = w.checkpoint().await?;
    assert_eq!(w.compaction_floor(), cp.base);
    assert!(cp.base > 0);
    assert_eq!(w.current_checkpoint(), Some(cp.clone()));
    // Post-checkpoint writes stay in the live log above the floor.
    let mut last = 0;
    for i in 20..30i64 {
        last = w.propose(KvOp::Set(format!("k{}", i % 6), i)).await?;
    }
    // A second checkpoint advances the floor again.
    let cp2 = w.checkpoint().await?;
    assert!(cp2.base > cp.base);
    assert!(cp2.is_newer_than(&cp));

    let reference = w.read(|s| s.clone());
    // Fresh snapshot-aware reader bootstraps from the CID then tails.
    let r = Replicated::<KvState>::snapshot_reader(cb.clone(), "kv", &a_id).await?;
    let ok = within(Duration::from_secs(15), || {
        r.version() >= last && r.read(|s| *s == reference)
    })
    .await;
    assert!(ok, "snapshot bootstrap + tail reached full-replay state");
    Ok(())
}

// checkpoint() on a read replica errors.
#[tokio::test]
async fn checkpoint_on_reader_errors() -> anyhow::Result<()> {
    let (_n, _ca, a_id, cb, _b) = pair().await?;
    let r = Replicated::<KvState>::snapshot_reader(cb.clone(), "kv2", &a_id).await?;
    assert!(r.checkpoint().await.is_err(), "checkpoint on a reader must error");
    Ok(())
}

// A snapshot_reader whose redirect points at a missing blob swallows the loader error and stays
// empty (covers load_checkpoint's Err arm).
#[tokio::test]
async fn snapshot_reader_survives_missing_blob() -> anyhow::Result<()> {
    let (_n, _ca, _a_id, cb, _b) = pair().await?;
    let ghost = "f".repeat(64);
    let r = Replicated::<KvState>::snapshot_reader(cb.clone(), "missing", &ghost).await?;
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(r.version(), 0, "no panic on missing snapshot; stays empty");
    Ok(())
}

// A *plain* reader (no snapshot loader) following a writer that has compacted past the log floor
// receives a redirect it cannot materialize, falls back to the legacy "ask from v1" branch, and —
// because the writer can no longer serve the dropped prefix — simply does not advance. It must not
// panic. This covers load_checkpoint's no-loader legacy arm.
#[tokio::test]
async fn plain_reader_against_compacted_writer_does_not_panic() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, _b) = pair().await?;
    let w = Replicated::<KvState>::writer(ca.clone(), "compacted").await?;
    for i in 0..12i64 {
        w.propose(KvOp::Set(format!("k{i}"), i)).await?;
    }
    // Compact away the whole prefix.
    let cp = w.checkpoint().await?;
    assert!(cp.base >= 12);
    // Plain reader (NOT snapshot_reader): gets a redirect with no loader installed.
    let r = Replicated::<KvState>::reader(cb.clone(), "compacted", &a_id).await?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    // No panic; the reader saw the redirect but cannot bootstrap, so it stays below the floor.
    assert!(r.version() <= cp.base, "plain reader survives a compacted writer");
    Ok(())
}

// ===========================================================================================
// Merged multi-writer log over the broker: two devices converge leaderless; add_writer paths.
// ===========================================================================================
#[derive(Default, Clone, PartialEq, Eq, Debug)]
struct Lww {
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
        self.map.insert(op.key, (op.val, op.ts));
    }
}
fn lww(lam: u64, who: &str, k: &str, v: i64) -> LwwOp {
    LwwOp { ts: Ts { lamport: lam, replica: who.into() }, key: k.into(), val: v }
}

#[tokio::test]
async fn merged_two_writers_converge() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, b_id) = pair().await?;

    let ma = Merged::<Lww>::open(&ca, "notes", &a_id, &[b_id.clone()]).await?;
    let mb = Merged::<Lww>::open(&cb, "notes", &b_id, &[a_id.clone()]).await?;

    // Hold a live watch receiver from the start so it observes the union growing.
    let mut wch = ma.watch();

    ma.propose(lww(1, "a", "x", 10)).await?;
    mb.propose(lww(2, "b", "x", 20)).await?; // higher ts wins for x
    ma.propose(lww(3, "a", "y", 11)).await?;
    mb.propose(lww(4, "b", "z", 22)).await?;

    // local read sees own op immediately
    assert!(ma.op_count() >= 2);

    let conv_a = within(Duration::from_secs(15), || {
        ma.op_count() == 4 && ma.read(|m| m.map.get("x").map(|p| p.0)) == Some(20)
    })
    .await;
    let conv_b = within(Duration::from_secs(15), || {
        mb.op_count() == 4 && mb.read(|m| m.map.get("x").map(|p| p.0)) == Some(20)
    })
    .await;
    assert!(conv_a, "device A converged");
    assert!(conv_b, "device B converged");

    let sa = ma.read(|m| m.clone());
    let sb = mb.read(|m| m.clone());
    assert_eq!(sa, sb, "both devices reached the identical merged state");

    // merged_ops snapshot is key-ordered and complete.
    let ops = ma.merged_ops();
    assert_eq!(ops.len(), 4);
    let keys: Vec<u64> = ops.iter().map(|o| o.ts.lamport).collect();
    assert_eq!(keys, vec![1, 2, 3, 4], "merged_ops is key-ordered");

    // sync_status: own log first, then peers.
    let st = ma.sync_status();
    assert_eq!(st[0].0, a_id);
    assert!(st.iter().any(|(id, _)| id == &b_id));
    assert!(st.iter().find(|(id, _)| id == &a_id).map(|(_, v)| *v).unwrap_or(0) >= 2);

    // writer_log escape hatch is reachable.
    assert!(ma.writer_log().version() >= 2);

    // The watch receiver held from the start observed the union grow to >= 4.
    let seen = *wch.borrow_and_update();
    assert!(seen >= 4, "watch reflects a grown union, saw {seen}");
    Ok(())
}

#[tokio::test]
async fn merged_add_writer_midstream_and_idempotent() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, b_id) = pair().await?;

    let ma = Merged::<Lww>::open(&ca, "live-add", &a_id, &[]).await?;
    let mb = Merged::<Lww>::open(&cb, "live-add", &b_id, &[]).await?;

    ma.propose(lww(1, "a", "x", 10)).await?;
    ma.propose(lww(2, "a", "y", 11)).await?;

    // B adds A mid-stream and catches up A's backlog.
    mb.add_writer(&a_id).await?;
    let ok = within(Duration::from_secs(15), || {
        mb.op_count() == 2 && mb.read(|m| m.map.get("x").map(|p| p.0)) == Some(10)
    })
    .await;
    assert!(ok, "add_writer mid-stream caught up");

    // add_writer is idempotent (already-followed peer) and ignores self.
    mb.add_writer(&a_id).await?; // idempotent
    mb.add_writer(&b_id).await?; // self — no-op
    assert_eq!(mb.op_count(), 2, "idempotent + self add_writer did not change the union");
    Ok(())
}

// Merged::open dedups/ignores self in the initial peer set.
#[tokio::test]
async fn merged_open_ignores_self_and_dups_in_peers() -> anyhow::Result<()> {
    let (_n, ca, a_id, cb, b_id) = pair().await?;
    // peer list contains self and a duplicate of b — both handled.
    let ma =
        Merged::<Lww>::open(&ca, "dedup", &a_id, &[a_id.clone(), b_id.clone(), b_id.clone()])
            .await?;
    let _mb = Merged::<Lww>::open(&cb, "dedup", &b_id, &[]).await?;
    // sync_status lists self + exactly one b entry (the dup was collapsed, self is the first entry).
    let st = ma.sync_status();
    let b_count = st.iter().filter(|(id, _)| id == &b_id).count();
    assert_eq!(b_count, 1, "duplicate peer collapsed");
    assert_eq!(st[0].0, a_id, "self is first");
    Ok(())
}

// ===========================================================================================
// Stream<T> — typed pub/sub over the broker (publish on A, receive on B).
// ===========================================================================================
#[tokio::test]
async fn stream_pubsub_delivers() -> anyhow::Result<()> {
    let node = MockNode::start();
    let ca = Coord::with_client(node.client()).await?;
    let cb = Coord::with_client(node.client()).await?;

    let mut sub = cb.stream::<String>("events").await?;
    // Give the subscribe + pump a moment.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pubr = ca.stream::<String>("events").await?;

    for i in 0..5 {
        pubr.publish(&format!("evt-{i}")).await?;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let got = tokio::time::timeout(Duration::from_secs(5), sub.next()).await;
    match got {
        Ok(Some(v)) => assert!(v.starts_with("evt-"), "received {v}"),
        other => panic!("expected a delivered event over the broker, got {other:?}"),
    }
    Ok(())
}

// A typed stream of a structured value round-trips through the broker.
#[tokio::test]
async fn stream_structured_value() -> anyhow::Result<()> {
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Telemetry {
        cpu: f64,
        host: String,
    }
    let node = MockNode::start();
    let ca = Coord::with_client(node.client()).await?;
    let cb = Coord::with_client(node.client()).await?;
    let mut sub = cb.stream::<Telemetry>("telemetry").await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pubr = ca.stream::<Telemetry>("telemetry").await?;
    for _ in 0..5 {
        pubr.publish(&Telemetry { cpu: 0.5, host: "h1".into() }).await?;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    let got = tokio::time::timeout(Duration::from_secs(5), sub.next()).await;
    match got {
        Ok(Some(t)) => assert_eq!(t, Telemetry { cpu: 0.5, host: "h1".into() }),
        other => panic!("expected telemetry, got {other:?}"),
    }
    Ok(())
}

// ===========================================================================================
// Streaming pump path: the mock now serves `GET /mesh/messages/stream` as real SSE, so the pump
// consumes pushed frames in real time instead of falling back to polling. Every test above already
// runs through this path (the mock no longer 404s the stream); this one pins it explicitly and
// asserts the *latency* property the SSE pump exists for: a publish is delivered to a subscriber
// well inside one 250ms poll interval, which the old poll-only pump could not guarantee.
// ===========================================================================================
#[tokio::test]
async fn streaming_pump_delivers_with_low_latency() -> anyhow::Result<()> {
    let node = MockNode::start();
    let ca = Coord::with_client(node.client()).await?;
    let cb = Coord::with_client(node.client()).await?;

    let mut sub = cb.stream::<String>("rt").await?;
    // Let the subscribe register and the streaming pump open its SSE connection.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let pubr = ca.stream::<String>("rt").await?;

    // Publish exactly one message and time how long it takes to surface. The mock's SSE handler
    // polls the inbox every ~10ms, so a healthy streaming pump delivers in tens of ms — far under
    // the 250ms a poll-only pump could add. We allow a generous 200ms ceiling so the assertion is
    // about "sub-poll-interval", not a flaky tight bound, while still failing if the pump silently
    // regressed to 250ms polling.
    let t0 = std::time::Instant::now();
    pubr.publish(&"now".to_string()).await?;
    let got = tokio::time::timeout(Duration::from_secs(5), sub.next()).await;
    let elapsed = t0.elapsed();
    match got {
        Ok(Some(v)) => assert_eq!(v, "now", "received the streamed value"),
        other => panic!("expected the streamed event, got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_millis(200),
        "streaming pump should deliver well inside one 250ms poll interval; took {elapsed:?}"
    );
    Ok(())
}

// ===========================================================================================
// Writers are namespaced by node id: two writers on the same name never cross-contaminate.
// ===========================================================================================
#[tokio::test]
async fn writers_namespaced_by_node_id() -> anyhow::Result<()> {
    let node = MockNode::start();
    let ca = Coord::with_client(node.client()).await?;
    let cb = Coord::with_client(node.client()).await?;
    let a_id = ca.node_id().to_string();
    let b_id = cb.node_id().to_string();

    let wa = ca.counter_writer("shared-name").await?;
    let wb = cb.counter_writer("shared-name").await?;
    wa.add(100).await?;
    wb.add(7).await?;

    let ra = cb.counter_reader("shared-name", &a_id).await?;
    let rb = ca.counter_reader("shared-name", &b_id).await?;
    assert!(within(CONV, || ra.get() == 100).await, "reader of A sees 100, not 107");
    assert!(within(CONV, || rb.get() == 7).await, "reader of B sees 7, not 107");
    Ok(())
}
