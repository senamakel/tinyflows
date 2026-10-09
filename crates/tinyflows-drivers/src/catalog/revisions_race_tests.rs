//! Revisions racing a flow's removal or a prune.

use super::*;
use crate::catalog::test_support::{catalog, trigger_graph};

#[tokio::test]
async fn a_revision_written_during_the_removal_never_shows() {
    let store = catalog();
    let flow = store
        .create_flow("v1".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .update_flow_graph(
            &flow.id,
            "v2".into(),
            trigger_graph(),
            false,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    let docs = store.docs().await.unwrap();
    // A graph update that read the flow before the removal and writes its
    // revision after the final sweep: the late write the sweeps cannot see.
    let incarnation = docs
        .get(DEFINITIONS, &flow.id)
        .await
        .unwrap()
        .and_then(|stored| text(&stored.doc, "incarnation").map(str::to_string))
        .unwrap();
    store.remove_flow(&flow.id).await.unwrap();
    docs.put(
        REVISIONS,
        "late",
        json!({
            "flow_id": flow.id,
            "flow_incarnation": incarnation,
            "graph_json": "{}",
            "name": "late",
            "created_at": "2026-01-01T00:00:00Z",
            "created_ns": 1,
        }),
        Precondition::Absent,
    )
    .await
    .unwrap();
    assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
    assert!(
        store
            .revision_by_id(&flow.id, "late")
            .await
            .unwrap()
            .is_none()
    );

    // Not even once a flow with the same id exists again.
    let mut again = flow.clone();
    again.name = "again".into();
    store.upsert_flow(&again).await.unwrap();
    assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
    assert!(
        store
            .revision_by_id(&flow.id, "late")
            .await
            .unwrap()
            .is_none()
    );
    // The next update's prune reclaims it.
    store
        .update_flow_graph(
            &flow.id,
            "v3".into(),
            trigger_graph(),
            false,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(docs.get(REVISIONS, "late").await.unwrap().is_none());
    assert_eq!(store.list_revisions(&flow.id, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_revision_pruned_in_flight_is_restored_after_the_swap() {
    let store = catalog();
    let docs = store.docs().await.unwrap();
    let revision = json!({ "flow_id": "f", "graph_json": "{}", "name": "n", "created_at": "x" });
    confirm_or_restore(docs, "gone", &revision).await.unwrap();
    let restored = docs.get(REVISIONS, "gone").await.unwrap().unwrap();
    assert!(!flag(&restored.doc, "pending"));
    assert_eq!(restored.doc["name"], json!("n"));
    // An existing one is only confirmed.
    let mut pending = revision.clone();
    pending["pending"] = json!(true);
    docs.put(REVISIONS, "here", pending, Precondition::Absent)
        .await
        .unwrap();
    confirm_or_restore(docs, "here", &revision).await.unwrap();
    assert!(!flag(
        &docs.get(REVISIONS, "here").await.unwrap().unwrap().doc,
        "pending"
    ));
}

#[tokio::test]
async fn pruning_leaves_a_revision_that_changed_since_it_was_read() {
    let store = catalog();
    let docs = store.docs().await.unwrap();
    docs.put(
        REVISIONS,
        "r",
        json!({ "pending": true }),
        Precondition::Absent,
    )
    .await
    .unwrap();
    let read = docs.get(REVISIONS, "r").await.unwrap().unwrap();
    compare_and_swap(docs, REVISIONS, "r", |_| Some(json!({ "confirmed": true })))
        .await
        .unwrap();
    delete_unchanged(docs, &read).await.unwrap();
    assert!(docs.get(REVISIONS, "r").await.unwrap().is_some());
    let fresh = docs.get(REVISIONS, "r").await.unwrap().unwrap();
    delete_unchanged(docs, &fresh).await.unwrap();
    assert!(docs.get(REVISIONS, "r").await.unwrap().is_none());
}
