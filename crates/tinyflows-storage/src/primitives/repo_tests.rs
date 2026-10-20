use super::*;

use crate::StorageBackend;
use serde::{Deserialize, Serialize};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::{CollectionSpec, Scope};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Row {
    value: u32,
}

#[tokio::test]
async fn repo_update_is_atomic_and_conditional_delete_preserves_unmatched_rows() {
    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("local").expect("scope"))
        .expect("backend");
    let docs = storage.documents();
    docs.ensure_collection(&CollectionSpec::new("flows.rows"))
        .await
        .expect("collection");
    let repo = Repo::<Row>::new(docs, "flows.rows")
        .expect("repo")
        .with_retry(RetryPolicy::immediate());
    let (row, _) = repo
        .update("one", |old| {
            Ok(Some(Row {
                value: old.map_or(1, |row| row.value + 1),
            }))
        })
        .await
        .expect("first update")
        .expect("write accepted");
    assert_eq!(row.value, 1);
    let (row, _) = repo
        .update("one", |old| {
            Ok(Some(Row {
                value: old.map_or(1, |row| row.value + 1),
            }))
        })
        .await
        .expect("second update")
        .expect("write accepted");
    assert_eq!(row.value, 2);
    assert!(
        !repo
            .delete_if("one", |row| row.value == 1)
            .await
            .expect("conditional delete")
    );
    assert!(
        repo.delete_if("one", |row| row.value == 2)
            .await
            .expect("matching delete")
    );
}
