use super::*;
use crate::catalog::test_support::catalog;
use serde_json::json;
use tinyflows_storage::{
    Capabilities, CollectionSpec, DocumentStore, Filter, Page, Precondition, Query,
    Result as StorageResult, Sort, StorageError, Version, Versioned,
};

#[derive(Debug)]
struct DeniedStore;

impl DeniedStore {
    fn denied<T>() -> StorageResult<T> {
        Err(StorageError::unavailable("storage denied"))
    }
}

#[tinyflows_storage::async_trait]
impl DocumentStore for DeniedStore {
    fn capabilities(&self) -> Capabilities {
        Capabilities::none()
    }

    async fn ensure_collection(&self, _: &CollectionSpec) -> StorageResult<()> {
        Self::denied()
    }

    async fn get(&self, _: &str, _: &str) -> StorageResult<Option<Versioned<serde_json::Value>>> {
        Self::denied()
    }

    async fn put(
        &self,
        _: &str,
        _: &str,
        _: serde_json::Value,
        _: Precondition,
    ) -> StorageResult<Version> {
        Self::denied()
    }

    async fn delete(&self, _: &str, _: &str, _: Precondition) -> StorageResult<bool> {
        Self::denied()
    }

    async fn query(&self, _: &str, _: &Query) -> StorageResult<Page<Versioned<serde_json::Value>>> {
        Self::denied()
    }

    async fn count(&self, _: &str, _: &Filter) -> StorageResult<u64> {
        Self::denied()
    }

    async fn delete_where(&self, _: &str, _: &Filter) -> StorageResult<u64> {
        Self::denied()
    }

    async fn claim(
        &self,
        _: &str,
        _: &Filter,
        _: &[Sort],
        _: &serde_json::Value,
    ) -> StorageResult<Option<Versioned<serde_json::Value>>> {
        Self::denied()
    }

    async fn drop_collection(&self, _: &str) -> StorageResult<()> {
        Self::denied()
    }
}

#[tokio::test]
async fn engine_state_is_what_the_catalog_reads() {
    let catalog = catalog();
    let state = FlowStateDocuments::new(catalog.clone(), "flow-1");
    assert_eq!(state.namespace(), "flow-1");
    state.store("seen", json!(["a"])).await.unwrap();
    assert_eq!(state.load("seen").await.unwrap(), Some(json!(["a"])));
    assert_eq!(
        catalog.kv_get("flow-1", "seen").await.unwrap(),
        Some(json!(["a"]))
    );
    assert!(catalog.kv_get("flow-2", "seen").await.unwrap().is_none());
}

#[tokio::test]
async fn opening_flow_state_ensures_storage_before_returning_a_handle() {
    let catalog = catalog();
    let state = FlowStateDocuments::open(catalog.clone(), "flow-open")
        .await
        .unwrap();
    assert_eq!(state.namespace(), "flow-open");
    state.store("key", json!("value")).await.unwrap();
    assert_eq!(
        catalog.kv_get("flow-open", "key").await.unwrap(),
        Some(json!("value"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn denied_storage_fails_open_and_every_state_and_dedup_operation_closed() {
    let catalog = FlowCatalogDocuments::new(Arc::new(DeniedStore));
    assert!(
        FlowStateDocuments::open(catalog.clone(), "denied")
            .await
            .is_err()
    );

    let state = FlowStateDocuments::new(catalog, "denied");
    assert!(state.load("key").await.is_err());
    assert!(state.store("key", json!("value")).await.is_err());
    assert!(DedupKv::kv_get(&state, "key").is_err());
    assert!(DedupKv::kv_set(&state, "key", &json!("value")).is_err());
    assert!(DedupKv::kv_delete(&state, "key").is_err());
}

/// `DedupKv` runs from inside a current-thread runtime, where `block_on`
/// or `block_in_place` would panic.
#[tokio::test(flavor = "current_thread")]
async fn dedup_settlement_works_inside_a_current_thread_runtime() {
    let catalog = catalog();
    let state = FlowStateDocuments::new(catalog.clone(), "flow-1");
    state.store("tentative", json!(["k1"])).await.unwrap();
    assert_eq!(
        DedupKv::kv_get(&state, "tentative").unwrap(),
        Some(json!(["k1"]))
    );
    DedupKv::kv_set(&state, "committed", &json!(["k1"])).unwrap();
    DedupKv::kv_delete(&state, "tentative").unwrap();
    assert!(state.load("tentative").await.unwrap().is_none());
    assert_eq!(
        catalog.kv_get("flow-1", "committed").await.unwrap(),
        Some(json!(["k1"]))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_host_bridge_is_used_when_given() {
    let bridge = Arc::new(Blocking::new().unwrap());
    let state = FlowStateDocuments::with_bridge(catalog(), "ns", bridge);
    DedupKv::kv_set(&state, "k", &json!(1)).unwrap();
    assert_eq!(DedupKv::kv_get(&state, "k").unwrap(), Some(json!(1)));
}

#[test]
fn dedup_settlement_works_without_any_runtime() {
    let state = FlowStateDocuments::new(catalog(), "ns");
    DedupKv::kv_set(&state, "k", &json!("v")).unwrap();
    assert_eq!(DedupKv::kv_get(&state, "k").unwrap(), Some(json!("v")));
}
