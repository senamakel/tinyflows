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
    let late = || {
        json!({
            "flow_id": flow.id,
            "flow_incarnation": incarnation,
            "graph_json": "{}",
            "name": "late",
            "created_at": "2026-01-01T00:00:00Z",
            "created_ns": 1,
        })
    };
    // Hidden, and reclaimed by whichever reader meets it.
    assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
    assert!(
        docs.get(REVISIONS, "late").await.unwrap().is_none(),
        "the list reclaimed it"
    );
    docs.put(REVISIONS, "late", late(), Precondition::Absent)
        .await
        .unwrap();
    assert!(
        store
            .revision_by_id(&flow.id, "late")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        docs.get(REVISIONS, "late").await.unwrap().is_none(),
        "the lookup reclaimed it"
    );

    // Not even once a flow with the same id exists again.
    docs.put(REVISIONS, "late", late(), Precondition::Absent)
        .await
        .unwrap();
    let mut again = flow.clone();
    again.name = "again".into();
    store.upsert_flow(&again).await.unwrap();
    assert!(
        store
            .revision_by_id(&flow.id, "late")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
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
    assert_eq!(store.list_revisions(&flow.id, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn the_committed_revision_is_written_whatever_a_prune_did() {
    let store = catalog();
    let docs = store.docs().await.unwrap();
    let revision = json!({ "flow_id": "f", "graph_json": "{}", "name": "n", "created_at": "x" });
    let mut pending = revision.clone();
    pending["pending"] = json!(true);
    docs.put(REVISIONS, "r", pending, Precondition::Absent)
        .await
        .unwrap();
    // A prune reads the pending revision as abandoned...
    let prune_read = docs.get(REVISIONS, "r").await.unwrap().unwrap();
    // ...and deletes it before the committed update confirms it.
    delete_unchanged(docs, &prune_read).await.unwrap();
    assert!(docs.get(REVISIONS, "r").await.unwrap().is_none());
    confirm_or_restore(docs, "r", &revision).await.unwrap();
    let restored = docs.get(REVISIONS, "r").await.unwrap().unwrap();
    assert!(!flag(&restored.doc, "pending"));
    assert_eq!(restored.doc["name"], json!("n"));

    // The other order: confirmed first, then a prune holding the old pending
    // version cannot delete it.
    let mut pending = revision.clone();
    pending["pending"] = json!(true);
    docs.put(REVISIONS, "s", pending, Precondition::Absent)
        .await
        .unwrap();
    let prune_read = docs.get(REVISIONS, "s").await.unwrap().unwrap();
    confirm_or_restore(docs, "s", &revision).await.unwrap();
    delete_unchanged(docs, &prune_read).await.unwrap();
    assert!(docs.get(REVISIONS, "s").await.unwrap().is_some());
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

#[tokio::test]
async fn confirmation_is_decided_by_the_swap() {
    let store = catalog();
    let docs = store.docs().await.unwrap();
    docs.put(
        REVISIONS,
        "p",
        json!({ "pending": true }),
        Precondition::Absent,
    )
    .await
    .unwrap();
    assert!(confirm(docs, "p").await.unwrap(), "pending, then confirmed");
    assert!(
        confirm(docs, "p").await.unwrap(),
        "already confirmed still exists"
    );
    assert!(!confirm(docs, "missing").await.unwrap(), "gone");
}
