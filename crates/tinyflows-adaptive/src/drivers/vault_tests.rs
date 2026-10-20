use super::*;
use crate::workflows::conformance;
use serde_json::json;
use tinyflows_storage::DocumentStoreExt;
use tinyflows_storage::{ Scope, StorageBackend};

fn memory_docs() -> Arc<dyn DocumentStore> {
    Arc::clone(
        MemoryStorage::new()
            .for_scope(&Scope::local())
            .unwrap()
            .documents(),
    )
}

#[tokio::test]
async fn passes_the_conformance_suite() {
    conformance::run_all(&DriverVault::new(memory_docs())).await;
}

#[tokio::test]
async fn passes_the_tenant_isolation_suite() {
    let vault = DriverVault::new(memory_docs());
    conformance::run_tenants(&vault, &vault.for_tenant("a"), &vault.for_tenant("b")).await;
}

#[tokio::test]
async fn an_unparseable_record_is_an_error() {
    let docs = memory_docs();
    let vault = DriverVault::new(Arc::clone(&docs));
    vault.put(&conformance::record("wf")).await.unwrap();
    docs.put(
        WORKFLOWS,
        &key(&["", "wf"]).unwrap(),
        json!({ "scope_key": "", "workflow_id": "wf", "name": 7, "graph": "nope" }),
        Precondition::None,
    )
    .await
    .unwrap();
    let error = vault.load().await.unwrap_err();
    assert!(error.to_string().contains("no longer parses"), "{error}");
    assert!(format!("{vault:?}").contains("DriverVault"));
}

#[tokio::test]
async fn storage_scopes_both_halves_together() {
    use crate::ledger::Ledger;
    let storage = crate::storage::Storage::from_documents(memory_docs());
    let tenant = storage.for_tenant("t");
    assert_eq!(tenant.ledger().scope(), Some("t"));
    assert_eq!(tenant.vault().scope(), Some("t"));
    tenant
        .vault()
        .put(&conformance::record("wf"))
        .await
        .unwrap();
    assert!(storage.vault().load().await.unwrap().is_empty());
}

#[tokio::test]
async fn records_are_stored_as_bare_objects_keyed_by_scope() {
    let docs = memory_docs();
    let vault = DriverVault::new(Arc::clone(&docs)).for_tenant("t");
    vault.put(&conformance::record("wf")).await.unwrap();
    let stored = docs.query_all(WORKFLOWS, &Query::all()).await.unwrap();
    assert_eq!(stored[0].id, key(&["t", "wf"]).unwrap());
    assert_eq!(stored[0].doc["scope_key"], "t");
    assert_eq!(stored[0].doc["workflow_id"], "wf");
    assert!(stored[0].doc["graph"].is_object() && stored[0].doc.get("record").is_none());
    assert_eq!(stored[0].meta.schema, 2);
    assert_eq!(vault.load().await.unwrap()[0].id, "wf");
}
