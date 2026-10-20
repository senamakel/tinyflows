use super::*;
use serde_json::json;
use tinyflows_storage::{Kv,  Scope, StorageBackend};

fn scoped() -> Storage {
    MemoryStorage::new().for_scope(&Scope::local()).unwrap()
}

async fn seed(storage: &Storage, collection: &str, id: &str, body: Value) {
    storage
        .documents()
        .put(collection, id, body, Precondition::None)
        .await
        .unwrap();
}

#[tokio::test]
async fn both_v1_collections_move_into_tsd_kv_with_values_unwrapped() {
    let storage = scoped();
    // v1 ids were `<len>:<part>/<len>:<part>`; the bodies are what matters.
    seed(
        &storage,
        "flows_kv",
        "2:ns/1:k",
        json!({"namespace": "ns", "key": "k", "value_json": "{\"n\":1}"}),
    )
    .await;
    seed(
        &storage,
        "flows_kv",
        "bad",
        json!({"namespace": "ns", "key": "k2", "value_json": "{"}),
    )
    .await;
    seed(
        &storage,
        "flows_state",
        "run/1",
        json!({"key": "run/1", "value": [1, 2]}),
    )
    .await;

    assert_eq!(import_legacy_kv(&storage).await.unwrap(), 2);

    let catalog = Kv::ns(storage.documents(), "flows", "kv:ns").unwrap();
    assert_eq!(
        catalog.get::<Value>("k").await.unwrap(),
        Some(json!({"n": 1}))
    );
    assert_eq!(
        catalog.get::<Value>("k2").await.unwrap(),
        None,
        "malformed rows are dropped"
    );
    let state = Kv::ns(storage.documents(), "flows", "state").unwrap();
    assert_eq!(
        state.get::<Value>("run/1").await.unwrap(),
        Some(json!([1, 2]))
    );
    for legacy in ["flows_kv", "flows_state"] {
        let left = storage
            .documents()
            .count(legacy, &Filter::default())
            .await
            .unwrap();
        assert_eq!(left, 0, "{legacy} is removed once copied");
    }
}

#[tokio::test]
async fn the_import_runs_once_and_keeps_newer_v2_values() {
    let storage = scoped();
    seed(
        &storage,
        "flows_state",
        "k",
        json!({"key": "k", "value": "old"}),
    )
    .await;
    let state = Kv::ns(storage.documents(), "flows", "state").unwrap();
    state.put("k", &"new").await.unwrap();

    assert_eq!(import_legacy_kv(&storage).await.unwrap(), 0);
    assert_eq!(
        state.get::<String>("k").await.unwrap().as_deref(),
        Some("new")
    );
    assert!(
        Migrations::new(&storage)
            .done("flows", "kv_import")
            .await
            .unwrap()
    );

    seed(
        &storage,
        "flows_state",
        "later",
        json!({"key": "later", "value": 1}),
    )
    .await;
    assert_eq!(
        import_legacy_kv(&storage).await.unwrap(),
        0,
        "a finished import is not repeated"
    );
}
