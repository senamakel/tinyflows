use super::*;

use crate::StorageBackend;
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

#[tokio::test]
async fn counters_advance_monotonically_and_raise_imported_values() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("scope"))
        .expect("backend");
    let docs = storage.documents();
    docs.ensure_collection(&tinystoragedrivers_types::CollectionSpec::new(
        "tsd.counters",
    ))
    .await
    .expect("collection");
    let counters = Counters::new(docs);
    assert_eq!(counters.peek("flows", "rows").await.expect("peek"), 0);
    assert_eq!(counters.next("flows", "rows").await.expect("first"), 1);
    assert_eq!(
        counters
            .set_at_least("flows", "rows", 8)
            .await
            .expect("import"),
        8
    );
    assert_eq!(
        counters
            .set_at_least("flows", "rows", 4)
            .await
            .expect("do not lower"),
        8
    );
    assert_eq!(counters.next("flows", "rows").await.expect("next"), 9);
}
