//! Serialization round-trip + wire-format property tests for ce-coord.
//!
//! Every op the engine ships travels as `serde_json::to_vec(op)` and is read back with
//! `serde_json::from_slice` (see `Replicated::propose` / `Inner::ingest`), and snapshots travel as
//! the `Checkpoint { base, cid }` struct. These tests prove that path is faithful for:
//!   * money-sized values **above 2^53** (where naive JSON-number handling silently corrupts),
//!   * the public `Checkpoint` marker,
//!   * arbitrary string keys/values (the `RMap<String,String>` the demo uses),
//!   * the `Entry`-shaped envelope (version + opaque op bytes) the wire actually carries.
//!
//! `encode == decode` is asserted for thousands of proptest-generated inputs.

use ce_coord::Checkpoint;
use proptest::prelude::*;
use serde::{Deserialize, Serialize};

// Mirror of the engine's on-wire op enum shape for an RMap<String, i128> — the exact JSON path
// `propose` uses. We test the round-trip independently of the (private) real enum so we can drive
// money-sized values through it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum MapOpWire {
    Insert(String, i128),
    Remove(String),
    Clear,
}

// Mirror of the engine's `Entry` envelope: a version plus opaque op bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EntryWire {
    version: u64,
    op: Vec<u8>,
}

fn roundtrip<T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug>(v: &T) -> T {
    let bytes = serde_json::to_vec(v).expect("encode");
    serde_json::from_slice(&bytes).expect("decode")
}

#[test]
fn money_above_2_53_survives_json_roundtrip() {
    // 2^53 is the float-safe integer ceiling; money base units (10^18/credit) blow past it.
    let big: i128 = 21_000_000_000i128 * 1_000_000_000_000_000_000i128; // ~ SUPPLY_CAP base units
    assert!(big as f64 as i128 != big, "value is genuinely beyond f64 integer precision");
    let op = MapOpWire::Insert("treasury".into(), big);
    assert_eq!(roundtrip(&op), op, "money value corrupted through the JSON wire path");

    // Negative balances (i128 balances can be negative in CE's model) too.
    let neg = MapOpWire::Insert("debt".into(), -big);
    assert_eq!(roundtrip(&neg), neg);

    // i128 extremes.
    for v in [i128::MAX, i128::MIN, i128::MAX - 1, i128::MIN + 1] {
        let op = MapOpWire::Insert("x".into(), v);
        assert_eq!(roundtrip(&op), op, "i128 extreme {v} did not round-trip");
    }
}

#[test]
fn checkpoint_roundtrips() {
    let cp = Checkpoint { base: 9_007_199_254_740_993, cid: "abc123def".into() }; // base > 2^53
    let back: Checkpoint = roundtrip(&cp);
    assert_eq!(back, cp);
    assert_eq!(back.base, 9_007_199_254_740_993);
}

proptest! {
    // Arbitrary string-keyed i128-valued ops round-trip (encode == decode).
    #[test]
    fn prop_mapop_roundtrips(key in ".*", val in any::<i128>()) {
        let op = MapOpWire::Insert(key.clone(), val);
        prop_assert_eq!(roundtrip(&op), op);
        let rem = MapOpWire::Remove(key);
        prop_assert_eq!(roundtrip(&rem), rem);
    }

    // The `Entry` envelope round-trips for arbitrary version + opaque op bytes (incl. empty and
    // non-UTF8 payloads — the engine treats op bytes as opaque).
    #[test]
    fn prop_entry_envelope_roundtrips(
        version in any::<u64>(),
        op in proptest::collection::vec(any::<u8>(), 0..256),
    ) {
        let e = EntryWire { version, op };
        prop_assert_eq!(roundtrip(&e), e);
    }

    // Checkpoint round-trips for any base (incl. u64::MAX) and any cid string.
    #[test]
    fn prop_checkpoint_roundtrips(base in any::<u64>(), cid in "[0-9a-f]{0,64}") {
        let cp = Checkpoint { base, cid };
        let back: Checkpoint = roundtrip(&cp);
        prop_assert_eq!(back.base, cp.base);
        prop_assert_eq!(back.cid, cp.cid);
    }

    // u128 (unsigned money base units, e.g. on-chain amounts) survive too.
    #[test]
    fn prop_u128_money_roundtrips(v in any::<u128>()) {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct M { amount: u128 }
        let m = M { amount: v };
        prop_assert_eq!(roundtrip(&m), m);
    }
}

#[test]
fn checkpoint_is_newer_than_total_order() {
    // is_newer_than is a strict order on `base`; equal bases are not "newer".
    let a = Checkpoint { base: 5, cid: "a".into() };
    let b = Checkpoint { base: 5, cid: "b".into() };
    let c = Checkpoint { base: 6, cid: "c".into() };
    assert!(!a.is_newer_than(&b));
    assert!(!b.is_newer_than(&a));
    assert!(c.is_newer_than(&a));
    assert!(!a.is_newer_than(&c));
}
