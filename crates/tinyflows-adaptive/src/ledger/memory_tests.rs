use super::*;
use crate::ledger::conformance;

#[tokio::test]
async fn passes_the_conformance_suite() {
    // The same cases both durable backends pass. That the trait is
    // implementable in std alone is the point: a host writing a third
    // backend has a complete example checked by the cases theirs will be.
    conformance::run_all(&MemoryLedger::new()).await;
}

#[tokio::test]
async fn passes_the_tenant_isolation_suite() {
    let store = MemoryLedger::new();
    let a = store.for_tenant("user-a");
    let b = store.for_tenant("user-b");
    conformance::run_tenants(&store, &a, &b).await;
}

#[tokio::test]
async fn a_scoped_handle_shares_the_store_rather_than_copying_it() {
    // Two handles for the SAME tenant must see each other's writes — that
    // is what "shares" means. Probing it across scopes would now fail by
    // design, because rows carry the bucket that wrote them.
    let store = MemoryLedger::new();
    let one = store.for_tenant("user-a");
    let two = store.for_tenant("user-a");
    one.append(&conformance::row("ep-shared", 1, "authored"))
        .await
        .expect("append");
    assert_eq!(two.rows("ep-shared").await.expect("rows").len(), 1);
    assert!(
        store.rows("ep-shared").await.expect("rows").is_empty(),
        "and the global bucket is its own, not a union"
    );
}

#[tokio::test]
async fn it_forgets_which_is_the_whole_point_of_the_name() {
    // Not a limitation being tested around — the behaviour, pinned, so the
    // difference from a durable backend is visible in the test names.
    let first = MemoryLedger::new();
    first
        .append(&conformance::row("ep-gone", 1, "authored"))
        .await
        .expect("append");
    assert_eq!(first.rows("ep-gone").await.expect("rows").len(), 1);

    let second = MemoryLedger::new();
    assert!(
        second.rows("ep-gone").await.expect("rows").is_empty(),
        "a new ledger is a new memory; nothing crosses between them"
    );
}
