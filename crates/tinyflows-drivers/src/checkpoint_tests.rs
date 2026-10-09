//! Behaviour of the storage-driver checkpointer: the same checks the
//! host-owned SQLite checkpointer (`tinyflows-sqlite`) is held to, run against
//! a driver document store, plus the isolation and key-encoding guarantees
//! the driver layout adds.

use super::*;
use serde_json::json;
use tinyflows::graph::ids::NodeId;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};

fn docs(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

fn store() -> DriverCheckpointer<serde_json::Value> {
    DriverCheckpointer::new(docs(&MemoryStorage::new(), "local"))
}

/// One checkpoint in `thread`, at `step`, chained to `parent`.
fn checkpoint(
    thread: &str,
    id: &str,
    parent: Option<&str>,
    step: u64,
    state: serde_json::Value,
) -> Checkpoint<serde_json::Value> {
    Checkpoint {
        thread_id: thread.to_string(),
        checkpoint_id: id.to_string(),
        run_id: Some("run-1".to_string()),
        parent_checkpoint_id: parent.map(str::to_string),
        namespace: Vec::new(),
        state,
        next_nodes: vec![NodeId::new("next")],
        completed_tasks: vec![NodeId::new("done")],
        pending_writes: Vec::new(),
        interrupts: Vec::new(),
        pending_activations: None,
        barrier_arrivals: Vec::new(),
        metadata: json!({ "source": "loop", "step": step }),
    }
}

/// `get(None)` is what resume calls, and "latest" has to mean latest by
/// insertion, not by id — the ids are opaque and nothing orders them.
#[tokio::test]
async fn get_without_an_id_returns_the_most_recently_written_checkpoint() {
    let store = store();
    store
        .put(checkpoint("t1", "cp-1", None, 1, json!({ "n": 1 })))
        .await
        .unwrap();
    store
        .put(checkpoint("t1", "cp-2", Some("cp-1"), 2, json!({ "n": 2 })))
        .await
        .unwrap();

    let latest = store.get("t1", None).await.unwrap().expect("latest");
    assert_eq!(latest.checkpoint_id, "cp-2");
    assert_eq!(latest.state, json!({ "n": 2 }));

    let addressed = store.get("t1", Some("cp-1")).await.unwrap().expect("cp-1");
    assert_eq!(addressed.state, json!({ "n": 1 }));

    assert!(store.get("t1", Some("nope")).await.unwrap().is_none());
    assert!(store.get("other-thread", None).await.unwrap().is_none());
}

/// Threads must not see each other: a flow run addresses its own thread id,
/// and one flow's checkpoints leaking into another's resume would replay the
/// wrong graph.
#[tokio::test]
async fn threads_are_isolated_and_listed_in_insertion_order() {
    let store = store();
    store
        .put(checkpoint("t1", "a", None, 1, json!(1)))
        .await
        .unwrap();
    store
        .put(checkpoint("t2", "b", None, 1, json!(2)))
        .await
        .unwrap();
    store
        .put(checkpoint("t1", "c", Some("a"), 2, json!(3)))
        .await
        .unwrap();

    let listed: Vec<String> = store
        .list("t1")
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.checkpoint_id)
        .collect();
    assert_eq!(listed, vec!["a".to_string(), "c".to_string()]);

    let mut threads = store.list_threads().await.unwrap();
    threads.sort();
    assert_eq!(threads, vec!["t1".to_string(), "t2".to_string()]);
}

/// A namespaced read is how a parent run and the sub-workflows it embeds stay
/// out of each other's checkpoints — they share a thread id and differ only
/// here.
#[tokio::test]
async fn scoped_reads_see_only_their_own_namespace() {
    let store = store();
    let mut root = checkpoint("t1", "root-1", None, 1, json!("root"));
    root.namespace = Vec::new();
    let mut child = checkpoint("t1", "child-1", None, 1, json!("child"));
    child.namespace = vec!["sub".to_string()];
    store.put(root).await.unwrap();
    store.put(child).await.unwrap();

    let scoped = store
        .get_scoped("t1", None, &["sub".to_string()])
        .await
        .unwrap()
        .expect("child checkpoint");
    assert_eq!(scoped.state, json!("child"));

    let at_root = store
        .get_scoped("t1", None, &[])
        .await
        .unwrap()
        .expect("root checkpoint");
    assert_eq!(at_root.state, json!("root"));
}

/// The partial-failure ledger, and its replace-vs-ignore rule — the thing in
/// this file most likely to be got subtly wrong, because both halves look like
/// "write it again".
///
/// A **data** write (`idx >= 0`) is append-once: a superstep that is retried
/// after a partial failure re-emits what it already emitted, and taking the
/// second copy would fold the same emission twice on resume. A
/// **control-plane** write (`idx < 0`, e.g. a resume value) legitimately
/// changes on a retry and must upsert. Both are pushed into SQL as two
/// conflict clauses rather than a read-then-write, so they stay correct under
/// concurrent writers.
#[tokio::test]
async fn data_writes_are_append_once_and_control_plane_writes_upsert() {
    let store = store();
    store
        .put(checkpoint("t1", "cp-1", None, 1, json!({})))
        .await
        .unwrap();
    let config = CheckpointConfig {
        thread_id: "t1".to_string(),
        checkpoint_id: Some("cp-1".to_string()),
        namespace: Vec::new(),
    };

    // Data write, then the same (task_id, idx) again: the first value stands.
    store
        .put_writes(
            &config,
            &[PendingWrite::data("n1", "task-a", 0, "out", json!("first"))],
        )
        .await
        .unwrap();
    store
        .put_writes(
            &config,
            &[PendingWrite::data(
                "n1",
                "task-a",
                0,
                "out",
                json!("second"),
            )],
        )
        .await
        .unwrap();

    let writes = store.get_writes(&config).await.unwrap();
    assert_eq!(
        writes.len(),
        1,
        "a re-run task must not duplicate its write"
    );
    assert_eq!(
        writes[0].payload,
        json!("first"),
        "a data write is append-once — a retry must not overwrite what already landed"
    );

    // Control-plane write at the reserved resume index: the newest value wins.
    let resume = |payload| {
        PendingWrite::data(
            "n1",
            "task-a",
            tinyflows::graph::checkpoint::WRITES_IDX_RESUME,
            "__resume__",
            payload,
        )
    };
    store
        .put_writes(&config, &[resume(json!("old"))])
        .await
        .unwrap();
    store
        .put_writes(&config, &[resume(json!("new"))])
        .await
        .unwrap();

    let writes = store.get_writes(&config).await.unwrap();
    let control: Vec<_> = writes.iter().filter(|w| w.is_control_plane()).collect();
    assert_eq!(
        control.len(),
        1,
        "control-plane writes are keyed, not appended"
    );
    assert_eq!(
        control[0].payload,
        json!("new"),
        "a control-plane write must upsert — a resume value changes on a retry"
    );
}

/// Deleting a thread is how a flow's history is dropped when the flow is
/// deleted; it must take that thread's writes with it and leave every other
/// thread alone.
#[tokio::test]
async fn deleting_a_thread_removes_its_checkpoints_and_leaves_others() {
    let store = store();
    store
        .put(checkpoint("doomed", "a", None, 1, json!(1)))
        .await
        .unwrap();
    store
        .put(checkpoint("kept", "b", None, 1, json!(2)))
        .await
        .unwrap();

    store.delete_thread("doomed").await.unwrap();

    assert!(store.get("doomed", None).await.unwrap().is_none());
    assert!(store.list("doomed").await.unwrap().is_empty());
    assert!(store.get("kept", None).await.unwrap().is_some());
}

/// Pruning bounds a long-running flow's history. It keeps the newest N, which
/// is the end resume reads from.
#[tokio::test]
async fn prune_keeps_the_newest_checkpoints() {
    let store = store();
    for i in 1..=5 {
        store
            .put(checkpoint("t1", &format!("cp-{i}"), None, i, json!(i)))
            .await
            .unwrap();
    }

    let removed = store.prune("t1", 2).await.unwrap();
    assert_eq!(removed, 3);

    let remaining: Vec<String> = store
        .list("t1")
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.checkpoint_id)
        .collect();
    assert_eq!(remaining, vec!["cp-4".to_string(), "cp-5".to_string()]);
}

/// An in-memory database lives on its connection, so clones have to share one
/// — a clone that silently got its own empty database would be a resume that
/// finds nothing.

#[tokio::test]
async fn scopes_and_prefixes_keep_threads_apart() {
    let storage = MemoryStorage::new();
    let alice = DriverCheckpointer::<serde_json::Value>::new(docs(&storage, "alice"));
    let bob = DriverCheckpointer::<serde_json::Value>::new(docs(&storage, "bob"));
    let other =
        DriverCheckpointer::<serde_json::Value>::with_prefix(docs(&storage, "alice"), "other");
    alice
        .put(checkpoint("t", "c1", None, 1, json!(1)))
        .await
        .unwrap();
    assert!(bob.get("t", None).await.unwrap().is_none());
    assert!(other.get("t", None).await.unwrap().is_none());
    assert_eq!(alice.list_threads().await.unwrap(), vec!["t".to_owned()]);
    assert!(bob.list_threads().await.unwrap().is_empty());
}

#[tokio::test]
async fn scoped_reads_stay_in_their_namespace_when_ids_repeat() {
    let saver = store();
    let child = vec!["sub".to_string()];
    saver
        .put(checkpoint("t", "same", None, 1, json!(1)))
        .await
        .unwrap();
    let mut nested = checkpoint("t", "same", None, 2, json!(2));
    nested.namespace = child.clone();
    saver.put(nested).await.unwrap();
    let root = saver
        .get_scoped("t", Some("same"), &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(root.state, json!(1));
    let sub = saver
        .get_scoped("t", Some("same"), &child)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sub.state, json!(2));
    assert!(
        saver
            .get_scoped("t", None, &["other".to_string()])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn pending_writes_never_merge_across_lookalike_namespaces() {
    let saver = store();
    saver
        .put(checkpoint("t", "c", None, 1, json!(1)))
        .await
        .unwrap();
    let config = |namespace: Vec<String>| CheckpointConfig {
        thread_id: "t".to_string(),
        checkpoint_id: Some("c".to_string()),
        namespace,
    };
    let split = config(vec!["a".to_string(), "b".to_string()]);
    let joined = config(vec!["a\u{1f}b".to_string()]);
    let write = PendingWrite::data("n", "task", 0, "out", json!("split"));
    saver.put_writes(&split, &[write]).await.unwrap();
    assert!(saver.get_writes(&joined).await.unwrap().is_empty());
    assert_eq!(saver.get_writes(&split).await.unwrap().len(), 1);
}

#[test]
fn keys_are_unambiguous_and_long_ones_hashed() {
    assert_ne!(key(&["a/b", "c"]), key(&["a", "b/c"]));
    let hashed = key(&[&"t".repeat(500)]);
    assert!(hashed.starts_with("h:") && hashed.len() == 66, "{hashed}");
    assert_ne!(hashed, key(&[&"u".repeat(500)]));
    let ns = |parts: &[&str]| parts.iter().map(|p| (*p).to_string()).collect::<Vec<_>>();
    assert_ne!(
        namespace_key(&ns(&["a", "b"])),
        namespace_key(&ns(&["a/b"]))
    );
    assert_ne!(namespace_key(&ns(&["ab"])), namespace_key(&ns(&["a", "b"])));
}

#[tokio::test]
async fn a_corrupt_record_is_a_checkpoint_error() {
    let storage = MemoryStorage::new();
    let docs = docs(&storage, "local");
    let saver = DriverCheckpointer::<serde_json::Value>::new(Arc::clone(&docs));
    saver
        .put(checkpoint("t", "c", None, 1, json!(1)))
        .await
        .unwrap();
    let stored = docs
        .query_all(&saver.checkpoints, &Query::all())
        .await
        .unwrap()
        .remove(0);
    let mut doc = stored.doc;
    doc["record"] = json!("not a checkpoint");
    docs.put(&saver.checkpoints, &stored.id, doc, Precondition::None)
        .await
        .unwrap();
    let error = saver.get("t", None).await.unwrap_err();
    assert!(matches!(error, GraphError::Checkpoint(_)), "{error:?}");
    assert!(
        map_error(StorageError::unavailable("busy"))
            .to_string()
            .contains("busy")
    );
}

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
