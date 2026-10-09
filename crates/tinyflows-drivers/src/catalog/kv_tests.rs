use super::*;
use crate::catalog::test_support::{catalog, catalog_in};
use tinystoragedrivers_core::MemoryStorage;

#[tokio::test]
async fn values_round_trip_per_namespace() {
    let store = catalog();
    assert!(store.kv_get("ns", "k").await.unwrap().is_none());
    store.kv_set("ns", "k", &json!({ "n": 1 })).await.unwrap();
    store.kv_set("other", "k", &json!(2)).await.unwrap();
    assert_eq!(
        store.kv_get("ns", "k").await.unwrap(),
        Some(json!({ "n": 1 }))
    );
    assert_eq!(store.kv_get("other", "k").await.unwrap(), Some(json!(2)));
    store.kv_set("ns", "k", &Value::Null).await.unwrap();
    assert_eq!(
        store.kv_get("ns", "k").await.unwrap(),
        Some(Value::Null),
        "null is a value"
    );
    store.kv_delete("ns", "k").await.unwrap();
    store.kv_delete("ns", "k").await.unwrap();
    assert!(store.kv_get("ns", "k").await.unwrap().is_none());
    store.kv_set("a/b", "c", &json!(1)).await.unwrap();
    assert!(
        store.kv_get("a", "b/c").await.unwrap().is_none(),
        "no id collision"
    );
}

#[tokio::test]
async fn scopes_keep_state_apart() {
    let storage = MemoryStorage::new();
    catalog_in(&storage, "alice")
        .kv_set("ns", "k", &json!(1))
        .await
        .unwrap();
    assert!(
        catalog_in(&storage, "bob")
            .kv_get("ns", "k")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_corrupt_value_is_an_error() {
    let store = catalog();
    store.kv_set("ns", "k", &json!(1)).await.unwrap();
    let docs = store.docs().await.unwrap();
    docs.put(
        KV,
        &kv_id("ns", "k"),
        json!({ "value_json": "{" }),
        Precondition::None,
    )
    .await
    .unwrap();
    assert!(store.kv_get("ns", "k").await.is_err());
}
