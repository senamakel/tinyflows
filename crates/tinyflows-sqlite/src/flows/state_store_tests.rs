use super::*;
use crate::flows::test_support::*;
use serde_json::json;
use tempfile::TempDir;
use tinyflows::nodes::control_flow::dedup_settle::{CommitOutcome, Settlement, settle};

#[tokio::test]
async fn load_store_round_trip_and_namespaces_are_isolated() {
    let tmp = TempDir::new().unwrap();
    let dir = test_dir(&tmp);
    let a = SqliteStateStore::new(&dir, "flow:a");
    let b = SqliteStateStore::new(&dir, "flow:b");

    assert_eq!(a.load("k").await.unwrap(), None);
    a.store("k", json!({"n": 1})).await.unwrap();
    assert_eq!(a.load("k").await.unwrap(), Some(json!({"n": 1})));
    assert_eq!(b.load("k").await.unwrap(), None);
}

#[tokio::test]
async fn settle_runs_against_the_same_namespace_the_engine_wrote() {
    let tmp = TempDir::new().unwrap();
    let dir = test_dir(&tmp);
    let store = SqliteStateStore::new(&dir, "flow:f1");
    store
        .store("dedup:n1:tentative", json!(["x", "y"]))
        .await
        .unwrap();

    let out = settle(&store, "n1", true);
    assert!(matches!(
        out,
        Settlement::Commit(CommitOutcome::Committed {
            added: 2,
            committed_len: 2,
            clear_error: None
        })
    ));
    assert_eq!(
        store.load("dedup:n1:committed").await.unwrap(),
        Some(json!(["x", "y"]))
    );
    assert_eq!(store.load("dedup:n1:tentative").await.unwrap(), None);
}
