use super::*;
use crate::ledger::conformance;

/// Runs the same suite the sqlite backend passes, against a real server.
///
/// Ignored by default: it needs one. Point `ADAPTIVE_MONGO_URI` at a
/// throwaway database and run with `--ignored`. Skipping silently when the
/// variable is absent would let this rot unnoticed, so the case is
/// `#[ignore]` and visible in the run summary instead.
#[tokio::test]
#[ignore = "needs a MongoDB server; set ADAPTIVE_MONGO_URI"]
async fn passes_the_conformance_suite() {
    let uri = std::env::var("ADAPTIVE_MONGO_URI").expect("ADAPTIVE_MONGO_URI");
    let name = format!("adaptive_conformance_{}", std::process::id());
    let store = MongoLedger::connect(&uri, &name).await.expect("connect");
    conformance::run_all(&store).await;
    conformance::run_tenants(
        &store,
        &store.for_tenant("user-a"),
        &store.for_tenant("user-b"),
    )
    .await;
    store.db.drop().await.expect("drop the throwaway database");
}
