use super::*;
use std::sync::Arc;
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Precondition};

use crate::catalog::test_support::{Interposed, docs_in, trigger_graph};

async fn update(store: &FlowCatalogDocuments, id: &str, name: &str) -> Flow {
    store
        .update_flow_graph(id, name.into(), trigger_graph(), false, None, false, None)
        .await
        .unwrap()
}

/// The revision a flow names is gone (its post-swap write was lost and it
/// was removed). Its snapshot cannot be rebuilt, so the next update logs the
/// gap and goes on rather than leaving the flow uneditable, and history
/// continues from the new revision.
#[tokio::test]
async fn an_update_moves_past_a_missing_named_revision() {
    let storage = MemoryStorage::new();
    let inner = docs_in(&storage, "local");
    let store = FlowCatalogDocuments::new(Arc::clone(&inner));
    let flow = store
        .create_flow("v0".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    update(&store, &flow.id, "v1").await;
    let named = inner
        .get(DEFINITIONS, &flow.id)
        .await
        .unwrap()
        .and_then(|stored| text(&stored.doc, "last_revision_id").map(str::to_string))
        .unwrap();
    inner
        .delete(REVISIONS, &named, Precondition::None)
        .await
        .unwrap();

    let updated = update(&store, &flow.id, "v2").await;
    assert_eq!(updated.name, "v2");
    let revisions = store.list_revisions(&flow.id, 10).await.unwrap();
    assert_eq!(revisions.len(), 1, "{revisions:?}");
    assert_eq!(
        revisions[0].name, "v1",
        "the snapshot taken by the v2 update"
    );
    assert_ne!(revisions[0].id, named);
}

/// An update that names an abandoned-looking pending revision after prune
/// partitioned the revisions keeps it: prune re-reads the definition before
/// deleting.
#[tokio::test]
async fn prune_spares_a_revision_named_after_it_partitioned() {
    let storage = MemoryStorage::new();
    let inner = docs_in(&storage, "local");
    let setup = FlowCatalogDocuments::new(Arc::clone(&inner));
    let flow = setup
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let definition = inner.get(DEFINITIONS, &flow.id).await.unwrap().unwrap();
    let mut stale = json!({
        "flow_id": flow.id,
        "graph_json": "{}",
        "name": "f",
        "require_approval": false,
        "created_at": "2020-01-01T00:00:00Z",
        "created_ns": 0,
        "pending": true,
    });
    if let Some(incarnation) = incarnation(&definition.doc) {
        stale[FLOW_INCARNATION] = json!(incarnation);
    }
    inner
        .put(REVISIONS, "stale", stale, Precondition::Absent)
        .await
        .unwrap();

    // The flow names it between prune's partition and its delete.
    let flow_id = flow.id.clone();
    let store = FlowCatalogDocuments::new(Interposed::wrap(
        Arc::clone(&inner),
        ("get", REVISIONS),
        Box::new(move |docs: Arc<dyn DocumentStore>| {
            Box::pin(async move {
                let mut current = docs.get(DEFINITIONS, &flow_id).await.unwrap().unwrap();
                current.doc["last_revision_id"] = json!("stale");
                docs.put(DEFINITIONS, &flow_id, current.doc, Precondition::None)
                    .await
                    .unwrap();
            })
        }),
    ));
    store.prune_revisions(&flow.id).await.unwrap();
    assert!(
        inner.get(REVISIONS, "stale").await.unwrap().is_some(),
        "a named revision is never pruned"
    );
}
