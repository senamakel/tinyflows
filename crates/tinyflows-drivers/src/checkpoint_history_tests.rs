//! Deletion and history-walk behaviour of the storage-driver checkpointer.

use super::tests::{checkpoint, store};
use super::*;
use serde_json::json;

fn addressed(thread: &str, id: &str) -> CheckpointConfig {
    CheckpointConfig {
        thread_id: thread.to_string(),
        checkpoint_id: Some(id.to_string()),
        namespace: Vec::new(),
    }
}

#[tokio::test]
async fn deleting_a_thread_drops_its_pending_writes() {
    let saver = store();
    saver
        .put(checkpoint("doomed", "a", None, 1, json!(1)))
        .await
        .unwrap();
    let write = PendingWrite::data("n", "task", 0, "out", json!("x"));
    saver
        .put_writes(&addressed("doomed", "a"), &[write])
        .await
        .unwrap();
    saver.delete_thread("doomed").await.unwrap();
    assert!(
        saver
            .get_writes(&addressed("doomed", "a"))
            .await
            .unwrap()
            .is_empty()
    );
    // A thread recreated under the same name starts clean.
    saver
        .put(checkpoint("doomed", "a", None, 1, json!(2)))
        .await
        .unwrap();
    assert!(
        saver
            .get_writes(&addressed("doomed", "a"))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_checkpoints_removes_only_the_named_ones_and_their_writes() {
    let saver = store();
    assert_eq!(saver.delete_checkpoints("t", &[]).await.unwrap(), 0);
    for (thread, id) in [("t", "a"), ("t", "b"), ("t", "c"), ("other", "a")] {
        saver
            .put(checkpoint(thread, id, None, 1, json!(id)))
            .await
            .unwrap();
        saver
            .put_writes(
                &addressed(thread, id),
                &[PendingWrite::data("n", "task", 0, "out", json!(id))],
            )
            .await
            .unwrap();
    }
    let removed = saver
        .delete_checkpoints("t", &["a".to_string(), "c".to_string()])
        .await
        .unwrap();
    assert_eq!(removed, 2);
    let left: Vec<String> = saver
        .list("t")
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.checkpoint_id)
        .collect();
    assert_eq!(left, vec!["b".to_string()]);
    assert!(
        saver
            .get_writes(&addressed("t", "a"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        saver.get_writes(&addressed("t", "b")).await.unwrap().len(),
        1
    );
    assert_eq!(
        saver.list("other").await.unwrap().len(),
        1,
        "other threads keep theirs"
    );
    assert_eq!(
        saver
            .get_writes(&addressed("other", "a"))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn state_history_walks_the_lineage_newest_first_in_one_read() {
    let saver = store();
    saver
        .put(checkpoint("t", "a", None, 1, json!(1)))
        .await
        .unwrap();
    saver
        .put(checkpoint("t", "b", Some("a"), 2, json!(2)))
        .await
        .unwrap();
    // An off-lineage sibling of `b` is not part of `c`'s history.
    saver
        .put(checkpoint("t", "x", Some("a"), 2, json!(9)))
        .await
        .unwrap();
    saver
        .put(checkpoint("t", "c", Some("b"), 3, json!(3)))
        .await
        .unwrap();
    let ledger = PendingWrite::data("n", "task", 0, "out", json!("ledger"));
    saver
        .put_writes(&addressed("t", "b"), std::slice::from_ref(&ledger))
        .await
        .unwrap();
    // Another namespace never leaks into the root's history.
    let mut nested = checkpoint("t", "z", Some("c"), 4, json!(0));
    nested.namespace = vec!["sub".to_string()];
    saver.put(nested).await.unwrap();

    let history = saver.state_history("t", &[], None).await.unwrap();
    let ids: Vec<&str> = history
        .iter()
        .map(|tuple| tuple.checkpoint.checkpoint_id.as_str())
        .collect();
    assert_eq!(ids, vec!["c", "b", "a"]);
    assert_eq!(
        history[1].pending_writes,
        vec![ledger],
        "the write ledger wins"
    );
    assert_eq!(
        history[0]
            .parent_config
            .as_ref()
            .and_then(|p| p.checkpoint_id.as_deref()),
        Some("b")
    );
    assert!(history[2].parent_config.is_none());

    let capped = saver.state_history("t", &[], Some(2)).await.unwrap();
    assert_eq!(capped.len(), 2);
    assert!(
        saver
            .state_history("none", &[], None)
            .await
            .unwrap()
            .is_empty()
    );

    // Same answer as the trait default walks it hop by hop.
    let mut default_walk = Vec::new();
    let mut cursor = None;
    while let Some(tuple) = saver
        .get_tuple(CheckpointConfig {
            thread_id: "t".to_string(),
            checkpoint_id: cursor.clone(),
            namespace: Vec::new(),
        })
        .await
        .unwrap()
    {
        default_walk.push(tuple.checkpoint.checkpoint_id.clone());
        cursor = tuple.checkpoint.parent_checkpoint_id.clone();
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(default_walk, vec!["c", "b", "a"]);
}

#[tokio::test]
async fn state_history_stops_at_a_lineage_cycle() {
    let saver = store();
    saver
        .put(checkpoint("t", "a", Some("b"), 1, json!(1)))
        .await
        .unwrap();
    saver
        .put(checkpoint("t", "b", Some("a"), 2, json!(2)))
        .await
        .unwrap();
    let history = saver.state_history("t", &[], None).await.unwrap();
    assert_eq!(history.len(), 2, "each checkpoint is visited once");
}
