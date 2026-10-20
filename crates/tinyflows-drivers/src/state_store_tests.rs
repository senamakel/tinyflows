use super::*;
use serde_json::json;
use tinyflows_storage::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

fn docs(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

#[tokio::test]
async fn state_round_trips_and_overwrites() {
    let store = DriverStateStore::new(docs(&MemoryStorage::new(), "local"));
    assert_eq!(store.load("k").await.unwrap(), None);
    store.store("k", json!({ "n": 1 })).await.unwrap();
    assert_eq!(store.load("k").await.unwrap(), Some(json!({ "n": 1 })));
    store.store("k", json!(null)).await.unwrap();
    assert_eq!(
        store.load("k").await.unwrap(),
        Some(Value::Null),
        "a stored null is a value, not a missing key"
    );
}

#[tokio::test]
async fn scopes_and_collections_keep_state_apart() {
    let storage = MemoryStorage::new();
    let alice = DriverStateStore::new(docs(&storage, "alice"));
    let bob = DriverStateStore::new(docs(&storage, "bob"));
    let other = DriverStateStore::with_namespace(docs(&storage, "alice"), "other_state");
    alice.store("run/1", json!("alice")).await.unwrap();
    assert_eq!(bob.load("run/1").await.unwrap(), None);
    assert_eq!(other.load("run/1").await.unwrap(), None);
}

#[tokio::test]
async fn an_unstorable_namespace_is_a_capability_error() {
    let error = DriverStateStore::with_namespace(docs(&MemoryStorage::new(), "local"), "")
        .load("k")
        .await
        .unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)), "{error:?}");
    assert!(error.to_string().contains("state store"), "{error}");
}

#[tokio::test]
async fn state_is_a_tsd_kv_entry_of_owner_flows() {
    let backing = docs(&MemoryStorage::new(), "local");
    let store = DriverStateStore::new(Arc::clone(&backing));
    store.store("a/b", json!({ "n": 1 })).await.unwrap();
    let row = backing
        .get("tsd.kv", "flows/state/a%2Fb")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.doc["owner"], "flows");
    assert_eq!(row.doc["ns"], "state");
    assert_eq!(row.doc["key"], "a/b");
    assert_eq!(row.doc["value"], json!({ "n": 1 }));
}
