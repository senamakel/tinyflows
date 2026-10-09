use super::*;
use crate::workflows::conformance;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

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
        &key(&["", "wf"]),
        json!({ "scope_key": "", "workflow_id": "wf", "record": 7 }),
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
