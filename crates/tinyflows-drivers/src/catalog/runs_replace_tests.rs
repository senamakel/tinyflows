use super::*;
use std::sync::Arc;
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Precondition};

use crate::catalog::test_support::{Interposed, docs_in, trigger_graph};

/// A flow removed and created again (a new incarnation) between the
/// definition read and the run query must not list the old incarnation's
/// run as its history; the run is reclaimed instead.
#[tokio::test]
async fn a_replaced_flow_does_not_list_the_old_incarnations_runs() {
    let storage = MemoryStorage::new();
    let inner = docs_in(&storage, "local");
    let setup = FlowCatalogDocuments::new(Arc::clone(&inner));
    let flow = setup
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    setup
        .insert_flow_run("old-run", &flow.id, "t", "2020-01-01T00:00:00Z")
        .await
        .unwrap();

    // Remove-and-recreate lands right before the run query, before any sweep
    // of the old incarnation's runs.
    let replaced = flow.clone();
    let store = FlowCatalogDocuments::new(Interposed::new(
        Arc::clone(&inner),
        ("query", RUNS),
        Box::new(move |docs: Arc<dyn DocumentStore>| {
            Box::pin(async move {
                docs.delete(DEFINITIONS, &replaced.id, Precondition::None)
                    .await
                    .unwrap();
                FlowCatalogDocuments::new(docs)
                    .upsert_flow(&replaced)
                    .await
                    .unwrap();
            })
        }),
    ));

    let listed = store.list_flow_runs(&flow.id, 10).await.unwrap();
    assert!(
        listed.is_empty(),
        "old incarnation's run listed: {listed:?}"
    );
    assert!(
        inner.get(RUNS, "old-run").await.unwrap().is_none(),
        "the orphan is reclaimed"
    );
}

/// A flow that is gone has no history, and its leftover runs are reclaimed
/// rather than kept until some other reader meets them.
#[tokio::test]
async fn listing_an_absent_flow_reclaims_its_leftover_runs() {
    let storage = MemoryStorage::new();
    let inner = docs_in(&storage, "local");
    let store = FlowCatalogDocuments::new(Arc::clone(&inner));
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .insert_flow_run("left", &flow.id, "t", "2020-01-01T00:00:00Z")
        .await
        .unwrap();
    // The definition went away without its sweep (a crash mid-removal).
    inner
        .delete(DEFINITIONS, &flow.id, Precondition::None)
        .await
        .unwrap();

    assert!(store.list_flow_runs(&flow.id, 10).await.unwrap().is_empty());
    assert!(inner.get(RUNS, "left").await.unwrap().is_none());
}

/// A stable flow still lists its own runs after the second definition read.
#[tokio::test]
async fn an_unchanged_flow_lists_its_runs() {
    let storage = MemoryStorage::new();
    let store = FlowCatalogDocuments::new(docs_in(&storage, "local"));
    let flow = store
        .create_flow("f".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .insert_flow_run("r", &flow.id, "t", "2020-01-01T00:00:00Z")
        .await
        .unwrap();
    let listed = store.list_flow_runs(&flow.id, 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, "r");
}
