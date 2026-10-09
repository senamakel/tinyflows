use super::*;
use serde_json::json;
use tinystoragedrivers_core::Precondition;

use crate::catalog::test_support::{catalog, trigger_graph};
use crate::catalog::{FlowCatalogDocuments, RUNS};

#[test]
fn a_dependent_belongs_only_to_its_own_incarnation() {
    let flow = json!({ "incarnation": "a" });
    assert!(belongs(&json!({ "flow_incarnation": "a" }), Some(&flow)));
    assert!(!belongs(&json!({ "flow_incarnation": "b" }), Some(&flow)));
    assert!(!belongs(&json!({}), Some(&flow)), "a pre-fence run of a new flow");
    assert!(!belongs(&json!({ "flow_incarnation": "a" }), None), "the flow is gone");
    assert!(belongs(&json!({}), Some(&json!({}))), "pre-fence documents match");
}

/// What an insert that passed its first check, lost the race to
/// `remove_flow`, and was cancelled before undoing itself leaves behind.
async fn orphan_after_removal(store: &FlowCatalogDocuments) -> (String, String) {
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let docs = store.docs().await.unwrap();
    let incarnation = docs
        .get(DEFINITIONS, &flow.id)
        .await
        .unwrap()
        .and_then(|stored| text(&stored.doc, INCARNATION).map(str::to_string))
        .unwrap();
    store.remove_flow(&flow.id).await.unwrap();
    docs.put(
        RUNS,
        "orphan",
        json!({
            "flow_id": flow.id,
            "flow_incarnation": incarnation,
            "thread_id": "t",
            "status": "pending_approval",
            "started_at": "2020-01-01T00:00:00Z",
            // Newest of all, so a list must page past it.
            "started_ns": i64::MAX,
            "steps_json": "[]",
            "pending_approvals_json": "[]",
        }),
        Precondition::Absent,
    )
    .await
    .unwrap();
    (flow.id, "orphan".to_string())
}

#[tokio::test]
async fn a_run_left_by_a_cancelled_insert_is_never_visible() {
    let store = catalog();
    let (flow_id, run_id) = orphan_after_removal(&store).await;
    assert!(store.list_all_flow_runs(10).await.unwrap().is_empty());
    assert!(store.list_flow_runs(&flow_id, 10).await.unwrap().is_empty());
    assert!(
        store
            .expire_parked_runs("2030-01-01T00:00:00Z", "now", "expired")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(store.get_flow_run(&run_id).await.unwrap().is_none());
    // The readers reclaimed it.
    let docs = store.docs().await.unwrap();
    assert!(docs.get(RUNS, &run_id).await.unwrap().is_none());
}

#[tokio::test]
async fn a_recreated_flow_does_not_inherit_the_old_runs() {
    let store = catalog();
    let (flow_id, run_id) = orphan_after_removal(&store).await;
    let mut again = store
        .create_flow("again".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    again.id = flow_id.clone();
    store.upsert_flow(&again).await.unwrap();
    assert!(store.list_flow_runs(&flow_id, 10).await.unwrap().is_empty());
    assert!(store.get_flow_run(&run_id).await.unwrap().is_none());
    assert!(
        store
            .list_running_run_ids("2030-01-01T00:00:00Z")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_list_pages_past_orphans_to_fill_its_limit() {
    let store = catalog();
    let (_, _) = orphan_after_removal(&store).await;
    let flow = store
        .create_flow("live".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .insert_flow_run("live-run", &flow.id, "t", "2019-01-01T00:00:00Z")
        .await
        .unwrap();
    let runs = store.list_all_flow_runs(1).await.unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].id, "live-run");
}
