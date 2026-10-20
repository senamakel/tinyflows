use super::*;

use crate::StorageBackend;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::{CollectionSpec, Scope};

#[tokio::test]
async fn lease_epochs_fence_stale_holders_after_expiry_and_release() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("scope"))
        .expect("backend");
    let docs = storage.documents();
    docs.ensure_collection(&CollectionSpec::new(LEASES))
        .await
        .expect("collection");
    let leases = Leases::new(docs).with_clock(Arc::new(|| 100));
    let first = leases
        .acquire("flows", "run", "worker-a", 10)
        .await
        .expect("acquire")
        .expect("first holder wins");
    let epoch = first.epoch;
    assert!(
        leases
            .renew("flows", "run", "worker-b", epoch, 10)
            .await
            .expect("wrong holder cannot renew")
            .is_none()
    );
    assert!(
        leases
            .release("flows", "run", "worker-a", epoch)
            .await
            .expect("release")
    );
    let second = leases
        .acquire("flows", "run", "worker-b", 10)
        .await
        .expect("reacquire")
        .expect("released key is available");
    assert!(second.epoch > epoch);
}
