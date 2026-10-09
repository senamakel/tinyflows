use super::*;
use crate::catalog::test_support::catalog;
use serde_json::json;

#[tokio::test]
async fn engine_state_is_what_the_catalog_reads() {
    let catalog = catalog();
    let state = FlowStateDocuments::new(catalog.clone(), "flow-1");
    assert_eq!(state.namespace(), "flow-1");
    state.store("seen", json!(["a"])).await.unwrap();
    assert_eq!(state.load("seen").await.unwrap(), Some(json!(["a"])));
    assert_eq!(
        catalog.kv_get("flow-1", "seen").await.unwrap(),
        Some(json!(["a"]))
    );
    assert!(catalog.kv_get("flow-2", "seen").await.unwrap().is_none());
}

/// `DedupKv` runs from inside a current-thread runtime, where `block_on`
/// or `block_in_place` would panic.
#[tokio::test(flavor = "current_thread")]
async fn dedup_settlement_works_inside_a_current_thread_runtime() {
    let catalog = catalog();
    let state = FlowStateDocuments::new(catalog.clone(), "flow-1");
    state.store("tentative", json!(["k1"])).await.unwrap();
    assert_eq!(
        DedupKv::kv_get(&state, "tentative").unwrap(),
        Some(json!(["k1"]))
    );
    DedupKv::kv_set(&state, "committed", &json!(["k1"])).unwrap();
    DedupKv::kv_delete(&state, "tentative").unwrap();
    assert!(state.load("tentative").await.unwrap().is_none());
    assert_eq!(
        catalog.kv_get("flow-1", "committed").await.unwrap(),
        Some(json!(["k1"]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_host_bridge_is_used_when_given() {
    let bridge = Arc::new(Blocking::new().unwrap());
    let state = FlowStateDocuments::with_bridge(catalog(), "ns", bridge);
    DedupKv::kv_set(&state, "k", &json!(1)).unwrap();
    assert_eq!(DedupKv::kv_get(&state, "k").unwrap(), Some(json!(1)));
}

#[test]
fn dedup_settlement_works_without_any_runtime() {
    let state = FlowStateDocuments::new(catalog(), "ns");
    DedupKv::kv_set(&state, "k", &json!("v")).unwrap();
    assert_eq!(DedupKv::kv_get(&state, "k").unwrap(), Some(json!("v")));
}
