use super::*;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

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
    let other = DriverStateStore::with_collection(docs(&storage, "alice"), "other_state");
    alice.store("run/1", json!("alice")).await.unwrap();
    assert_eq!(bob.load("run/1").await.unwrap(), None);
    assert_eq!(other.load("run/1").await.unwrap(), None);
}

#[tokio::test]
async fn an_unstorable_key_is_a_capability_error() {
    let store = DriverStateStore::new(docs(&MemoryStorage::new(), "local"));
    let error = store.store("", json!(1)).await.unwrap_err();
    assert!(matches!(error, EngineError::Capability(_)), "{error:?}");
    let error = DriverStateStore::with_collection(docs(&MemoryStorage::new(), "local"), "bad name")
        .load("k")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("state store"), "{error}");
}
