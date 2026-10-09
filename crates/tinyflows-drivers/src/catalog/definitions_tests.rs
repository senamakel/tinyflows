use super::*;
use crate::catalog::test_support::{catalog, catalog_in, trigger_graph};
use tinystoragedrivers_core::MemoryStorage;

#[tokio::test]
async fn create_get_list_delete_roundtrip() {
    let store = catalog();
    let flow = store
        .create_flow("demo".into(), trigger_graph(), true, true)
        .await
        .unwrap();
    let loaded = store.get_flow(&flow.id).await.unwrap().expect("stored");
    assert_eq!(loaded.name, "demo");
    assert!(loaded.require_approval && loaded.enabled);
    assert_eq!(loaded.graph.nodes.len(), 1);
    assert_eq!(store.list_flows().await.unwrap().0.len(), 1);
    store.remove_flow(&flow.id).await.unwrap();
    assert!(store.get_flow(&flow.id).await.unwrap().is_none());
    let error = store.remove_flow(&flow.id).await.unwrap_err();
    assert!(error.to_string().contains("not found"));
}

#[tokio::test]
async fn upsert_keeps_created_at_and_replaces_the_rest() {
    let store = catalog();
    let mut flow = store
        .create_flow("a".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let created = flow.created_at.clone();
    flow.created_at = "2000-01-01T00:00:00Z".into();
    flow.name = "renamed".into();
    flow.last_status = Some("completed".into());
    store.upsert_flow(&flow).await.unwrap();
    let loaded = store.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(loaded.created_at, created);
    assert_eq!(loaded.name, "renamed");
    assert_eq!(loaded.last_status.as_deref(), Some("completed"));
    flow.last_status = None;
    store.upsert_flow(&flow).await.unwrap();
    assert!(
        store
            .get_flow(&flow.id)
            .await
            .unwrap()
            .unwrap()
            .last_status
            .is_none()
    );
}

#[tokio::test]
async fn enable_record_and_duplicate() {
    let store = catalog();
    let flow = store
        .create_flow("a".into(), trigger_graph(), true, true)
        .await
        .unwrap();
    assert!(!store.set_enabled(&flow.id, false).await.unwrap().enabled);
    assert!(store.set_enabled("missing", true).await.is_err());
    store.record_run(&flow.id, "failed").await.unwrap();
    let loaded = store.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(loaded.last_status.as_deref(), Some("failed"));
    assert!(loaded.last_run_at.is_some());
    assert!(store.record_run("missing", "x").await.is_err());

    let copy = store
        .insert_duplicate_flow(&loaded, "copy".into())
        .await
        .unwrap();
    assert_ne!(copy.id, flow.id);
    assert!(!copy.enabled && copy.require_approval);
    assert!(copy.last_run_at.is_none());
    store.set_enabled(&flow.id, true).await.unwrap();
    let (enabled, skipped) = store.list_enabled_flows().await.unwrap();
    assert_eq!((enabled.len(), skipped), (1, 0));
    assert_eq!(enabled[0].id, flow.id);
}

#[tokio::test]
async fn flows_list_oldest_first() {
    let store = catalog();
    let mut ids = Vec::new();
    for name in ["one", "two", "three"] {
        ids.push(
            store
                .create_flow(name.into(), trigger_graph(), false, true)
                .await
                .unwrap()
                .id,
        );
    }
    let listed: Vec<String> = store
        .list_flows()
        .await
        .unwrap()
        .0
        .into_iter()
        .map(|f| f.id)
        .collect();
    assert_eq!(listed, ids);
}

#[tokio::test]
async fn a_legacy_graph_is_migrated_on_read() {
    let store = catalog();
    let flow = store
        .create_flow("legacy".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let legacy = json!({
        "name": "legacy",
        "nodes": [{ "id": "t", "kind": "trigger", "name": "Trigger" }],
        "edges": []
    })
    .to_string();
    store
        .force_corrupt_graph_json_for_test(&flow.id, &legacy)
        .await
        .unwrap();
    let loaded = store.get_flow(&flow.id).await.unwrap().unwrap();
    assert_eq!(
        loaded.graph.schema_version,
        tinyflows::model::CURRENT_SCHEMA_VERSION
    );
}

#[tokio::test]
async fn corrupt_and_too_new_graphs_are_skipped_and_counted() {
    let store = catalog();
    let good = store
        .create_flow("good".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let corrupt = store
        .create_flow("bad".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let future = store
        .create_flow("future".into(), trigger_graph(), false, false)
        .await
        .unwrap();
    store
        .force_corrupt_graph_json_for_test(&corrupt.id, "{not json")
        .await
        .unwrap();
    let too_new =
        json!({ "schema_version": 999, "name": "x", "nodes": [], "edges": [] }).to_string();
    store
        .force_corrupt_graph_json_for_test(&future.id, &too_new)
        .await
        .unwrap();
    let (flows, skipped) = store.list_flows().await.unwrap();
    assert_eq!((flows.len(), skipped), (1, 2));
    assert_eq!(flows[0].id, good.id);
    let (enabled, skipped) = store.list_enabled_flows().await.unwrap();
    assert_eq!(
        (enabled.len(), skipped),
        (1, 1),
        "a disabled corrupt flow is not counted"
    );
    assert!(store.get_flow(&corrupt.id).await.is_err());
    assert!(
        store
            .force_corrupt_graph_json_for_test("missing", "x")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn removing_a_flow_removes_its_history() {
    let store = catalog();
    let flow = store
        .create_flow("a".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    let other = store
        .create_flow("b".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    store
        .insert_flow_run("r1", &flow.id, "r1", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    store
        .insert_flow_run("r2", &other.id, "r2", "2026-01-01T00:00:00Z")
        .await
        .unwrap();
    let step = tinyflows_catalog::FlowRunStep {
        node_id: "t".into(),
        ..Default::default()
    };
    store.upsert_flow_run_step("r1", &step).await.unwrap();
    store.upsert_flow_run_step("r2", &step).await.unwrap();
    store
        .update_flow_graph(
            &flow.id,
            "a2".into(),
            trigger_graph(),
            false,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    store.remove_flow(&flow.id).await.unwrap();
    assert!(store.get_flow_run("r1").await.unwrap().is_none());
    assert!(store.list_revisions(&flow.id, 10).await.unwrap().is_empty());
    assert_eq!(
        store.get_flow_run("r2").await.unwrap().unwrap().steps.len(),
        1
    );
}

#[tokio::test]
async fn scopes_keep_catalogs_apart() {
    let storage = MemoryStorage::new();
    let alice = catalog_in(&storage, "alice");
    let bob = catalog_in(&storage, "bob");
    let flow = alice
        .create_flow("a".into(), trigger_graph(), false, true)
        .await
        .unwrap();
    assert!(bob.get_flow(&flow.id).await.unwrap().is_none());
    assert!(bob.list_flows().await.unwrap().0.is_empty());
    assert!(bob.remove_flow(&flow.id).await.is_err());
}
