use super::*;
use crate::workflows::conformance;

/// Needs a real server, so it is `#[ignore]` and visible in the run summary
/// rather than silently skipped — the same posture as the mongo ledger.
#[tokio::test]
#[ignore = "needs a MongoDB server; set ADAPTIVE_MONGO_URI"]
async fn passes_the_conformance_suite() {
    let uri = std::env::var("ADAPTIVE_MONGO_URI").expect("ADAPTIVE_MONGO_URI");
    let name = format!("adaptive_vault_{}", std::process::id());
    let vault = MongoVault::connect(&uri, &name).await.expect("connect");
    conformance::run_all(&vault).await;
    conformance::run_tenants(&vault, &vault.for_tenant("a"), &vault.for_tenant("b")).await;
    vault.db.drop().await.expect("drop the throwaway database");
}
