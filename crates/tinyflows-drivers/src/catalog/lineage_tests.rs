use super::*;
use serde_json::json;
use std::collections::HashMap;
use tinystoragedrivers_core::Precondition;

use crate::catalog::test_support::{catalog, trigger_graph};
use crate::catalog::{FlowCatalogDocuments, RUNS};

#[test]
fn a_dependent_belongs_only_to_its_own_incarnation() {
    let flow = json!({ "incarnation": "a" });
    assert!(belongs(&json!({ "flow_incarnation": "a" }), Some(&flow)));
    assert!(!belongs(&json!({ "flow_incarnation": "b" }), Some(&flow)));
    assert!(
        belongs(&json!({}), Some(&flow)),
        "an unfenced writer's run is never hidden (rolling upgrade)"
    );
    assert!(!belongs(&json!({}), None), "the flow is gone");
    assert!(
        !belongs(&json!({ "flow_incarnation": "a" }), Some(&json!({}))),
        "a fenced run of a removed incarnation, under a pre-fence definition"
    );
    assert!(
        !belongs(&json!({ "flow_incarnation": "a" }), None),
        "the flow is gone"
    );
    assert!(
        belongs(&json!({}), Some(&json!({}))),
        "pre-fence documents match"
    );
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

#[test]
fn the_live_flow_map_follows_the_same_rule() {
    let mut flows = HashMap::new();
    flows.insert("f".to_string(), Some("a".to_string()));
    assert!(belongs_in(
        &json!({ "flow_id": "f", "flow_incarnation": "a" }),
        &flows
    ));
    assert!(belongs_in(&json!({ "flow_id": "f" }), &flows));
    assert!(!belongs_in(
        &json!({ "flow_id": "f", "flow_incarnation": "b" }),
        &flows
    ));
    assert!(!belongs_in(&json!({ "flow_id": "g" }), &flows));
}

#[tokio::test]
async fn an_unfenced_writers_run_stays_visible_and_is_never_reclaimed() {
    let store = catalog();
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let docs = store.docs().await.unwrap();
    // An older process during a rolling upgrade writes no flow_incarnation.
    docs.put(
        RUNS,
        "old-writer",
        json!({
            "flow_id": flow.id,
            "thread_id": "t",
            "status": "running",
            "started_at": "2020-01-01T00:00:00Z",
            "started_ns": 1,
            "steps_json": "[]",
            "pending_approvals_json": "[]",
        }),
        Precondition::Absent,
    )
    .await
    .unwrap();
    assert_eq!(store.list_flow_runs(&flow.id, 10).await.unwrap().len(), 1);
    assert_eq!(store.list_all_flow_runs(10).await.unwrap().len(), 1);
    assert!(store.get_flow_run("old-writer").await.unwrap().is_some());
    assert!(docs.get(RUNS, "old-writer").await.unwrap().is_some());
}

#[tokio::test]
async fn a_stale_classification_never_reclaims_the_recreated_flows_records() {
    let store = catalog();
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let docs = store.docs().await.unwrap();
    // A reader snapshots incarnation A, then the flow is removed and created
    // again (B), and B gets a run before the reader reclaims.
    store.remove_flow(&flow.id).await.unwrap();
    let mut again = flow.clone();
    again.name = "again".into();
    store.upsert_flow(&again).await.unwrap();
    store
        .insert_flow_run("b-run", &flow.id, "t", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    let b_run = docs.get(RUNS, "b-run").await.unwrap().unwrap();
    reclaim(docs, RUNS, std::slice::from_ref(&b_run)).await;
    assert!(
        docs.get(RUNS, "b-run").await.unwrap().is_some(),
        "re-checked against the current definition, so kept"
    );
    assert_eq!(store.list_flow_runs(&flow.id, 10).await.unwrap().len(), 1);
}

/// Races real catalog operations against `remove_flow` many times; whatever
/// the interleaving, nothing of the removed flow may ever be visible, and a
/// flow created again under the same id starts clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_writes_against_removal_never_leave_a_visible_orphan() {
    let store = catalog();
    for round in 0..40 {
        let flow = store
            .create_flow(format!("f{round}"), trigger_graph(), false, true)
            .await
            .unwrap();
        let (updater, inserter, remover) = (store.clone(), store.clone(), store.clone());
        let (id_u, id_i, id_r) = (flow.id.clone(), flow.id.clone(), flow.id.clone());
        let update = tokio::spawn(async move {
            let _ = updater
                .update_flow_graph(
                    &id_u,
                    "v2".into(),
                    trigger_graph(),
                    false,
                    None,
                    false,
                    None,
                )
                .await;
        });
        let insert = tokio::spawn(async move {
            let _ = inserter
                .insert_flow_run(&format!("run-{round}"), &id_i, "t", "2026-01-01T00:00:00Z")
                .await;
        });
        let remove = tokio::spawn(async move { remover.remove_flow(&id_r).await });
        update.await.unwrap();
        insert.await.unwrap();
        remove.await.unwrap().unwrap();

        assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
        assert!(store.list_flow_runs(&flow.id, 10).await.unwrap().is_empty());
        assert!(
            store
                .get_flow_run(&format!("run-{round}"))
                .await
                .unwrap()
                .is_none()
        );
        let mut again = flow.clone();
        again.name = "again".into();
        store.upsert_flow(&again).await.unwrap();
        assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
        assert!(store.list_flow_runs(&flow.id, 10).await.unwrap().is_empty());
        store.remove_flow(&flow.id).await.unwrap();
    }
    assert!(store.list_all_flow_runs(100).await.unwrap().is_empty());
}
