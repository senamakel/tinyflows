use super::*;

use crate::StorageBackend;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn key_values_are_isolated_by_namespace_and_list_in_key_order() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("scope"))
        .expect("backend");
    let docs = storage.documents();
    docs.ensure_collection(&tinystoragedrivers_types::CollectionSpec::new(KV).index(
        tinystoragedrivers_types::IndexSpec::new("kv_by_key", ["owner", "ns", "key"]),
    ))
    .await
    .expect("collection");
    let kv = Kv::ns(docs, "flows", "catalog").expect("namespace");
    let other = Kv::ns(docs, "flows", "state").expect("other namespace");
    kv.put("b", &serde_json::json!({"value": 2}))
        .await
        .expect("write b");
    kv.put("a", &serde_json::json!({"value": 1}))
        .await
        .expect("write a");
    other
        .put("a", &serde_json::json!({"value": 3}))
        .await
        .expect("write isolated");
    let listed = kv.list::<serde_json::Value>("").await.expect("list");
    assert_eq!(
        listed
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(
        kv.get::<serde_json::Value>("a")
            .await
            .expect("read")
            .expect("present")["value"],
        1
    );
    assert!(kv.delete("a").await.expect("delete"));
    assert!(!kv.delete("a").await.expect("delete missing"));
    assert_eq!(
        other
            .get::<serde_json::Value>("a")
            .await
            .expect("read other")
            .expect("present")["value"],
        3
    );
}
