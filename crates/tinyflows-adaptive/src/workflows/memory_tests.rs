use super::*;
use crate::workflows::conformance;

#[tokio::test]
async fn passes_the_conformance_suite() {
    conformance::run_all(&MemoryVault::new()).await;
}

#[tokio::test]
async fn passes_the_tenant_isolation_suite() {
    let vault = MemoryVault::new();
    conformance::run_tenants(&vault, &vault.for_tenant("a"), &vault.for_tenant("b")).await;
}
