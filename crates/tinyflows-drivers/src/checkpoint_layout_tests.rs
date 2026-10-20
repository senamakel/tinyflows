use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
//! Pins the checkpointer's documents to the layout `tinyagents-graph` stores
//! for namespace `flows`. The fixtures are copied from tinyagents' own tests
//! (`checkpoint/drivers_tests.rs`, `storage/mod_tests.rs` on its epic branch)
//! and from the document shape its `checkpoints_flows_v2` step produces, so a
//! drift on either side fails here.

use crate::checkpoint::DriverCheckpointer;
use serde_json::json;
use std::sync::Arc;
use tinyflows::graph::{Checkpoint, CheckpointConfig, Checkpointer, PendingWrite};
use tinyflows_storage::{DocumentStore,  Precondition, Scope, StorageBackend};

fn docs() -> Arc<dyn DocumentStore> {
    Arc::clone(
        MemoryStorage::new()
            .for_scope(&Scope::local())
            .unwrap()
            .documents(),
    )
}

/// A tinyflows checkpoint record as the retired `flows_graph_checkpoints`
/// stored it in `record`, and as tinyagents' step carries it into `checkpoint`.
fn record(thread: &str, id: &str, namespace: &[&str]) -> serde_json::Value {
    json!({
        "thread_id": thread, "checkpoint_id": id, "run_id": null,
        "parent_checkpoint_id": null, "namespace": namespace, "state": 7,
        "next_nodes": [], "completed_tasks": [], "pending_writes": [],
        "interrupts": [], "pending_activations": null, "barrier_arrivals": [],
        "metadata": {}
    })
}

fn sample(state: i32, thread: &str, id: &str, namespace: &[&str]) -> Checkpoint<i32> {
    serde_json::from_value(record(thread, id, namespace).clone())
        .map(|mut checkpoint: Checkpoint<i32>| {
            checkpoint.state = state;
            checkpoint
        })
        .unwrap()
}

#[tokio::test]
async fn documents_migrated_by_tinyagents_step_are_read_back() {
    let docs = docs();
    // What `checkpoints_flows_v2` writes for a v1 `flows_graph_checkpoints` row.
    docs.put_with(
        "graph.checkpoints",
        "flows/t/00000000000000000000",
        json!({"ns": "flows", "thread": "t", "subgraph": "", "seq": 0,
               "checkpoint_id": "c1", "checkpoint": record("t", "c1", &[])}),
        Precondition::Absent,
        tinyflows_storage::WriteOptions::new().schema(4),
    )
    .await
    .unwrap();
    docs.put_with(
        "graph.checkpoints",
        "flows/t/00000000000000000001",
        json!({"ns": "flows", "thread": "t", "subgraph": "n/sub", "seq": 1,
               "checkpoint_id": "c1", "checkpoint": record("t", "c1", &["sub"])}),
        Precondition::Absent,
        tinyflows_storage::WriteOptions::new().schema(4),
    )
    .await
    .unwrap();
    // The migrated thread counter and pending writes.
    docs.put(
        "graph.threads",
        "flows/t",
        json!({"ns": "flows", "thread": "t", "next": 2}),
        Precondition::Absent,
    )
    .await
    .unwrap();
    docs.put(
        "graph.writes",
        "flows/t/n%2Fsub/c1",
        json!({"ns": "flows", "thread": "t", "subgraph": "n/sub", "checkpoint_id": "c1",
               "writes": [{"node": "n", "task_id": "task", "idx": 0, "channel": "out", "payload": "x"}]}),
        Precondition::Absent,
    )
    .await
    .unwrap();

    let cp = DriverCheckpointer::<i32>::new(Arc::clone(&docs));
    assert_eq!(cp.list_threads().await.unwrap(), ["t"]);
    let root = cp.get_scoped("t", Some("c1"), &[]).await.unwrap().unwrap();
    assert!(root.namespace.is_empty());
    let sub = cp
        .get_scoped("t", Some("c1"), &["sub".to_owned()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sub.namespace, ["sub"]);
    let config = CheckpointConfig {
        thread_id: "t".to_owned(),
        checkpoint_id: Some("c1".to_owned()),
        namespace: vec!["sub".to_owned()],
    };
    let writes = cp.get_writes(&config).await.unwrap();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, json!("x"));
    // The counter continues after the migrated rows.
    cp.put(sample(9, "t", "c2", &[])).await.unwrap();
    assert!(
        docs.get("graph.checkpoints", "flows/t/00000000000000000002")
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn written_documents_have_exactly_the_tinyagents_fields_and_ids() {
    let docs = docs();
    let cp = DriverCheckpointer::<i32>::with_namespace(Arc::clone(&docs), "flows");
    cp.put(sample(1, "a/b", "c1", &["sub"])).await.unwrap();
    let config = CheckpointConfig {
        thread_id: "a/b".to_owned(),
        checkpoint_id: Some("c1".to_owned()),
        namespace: vec!["sub".to_owned()],
    };
    cp.put_writes(
        &config,
        &[PendingWrite::data("n", "task", 0, "out", json!("w"))],
    )
    .await
    .unwrap();

    let row = docs
        .get("graph.checkpoints", "flows/a%2Fb/00000000000000000000")
        .await
        .unwrap()
        .unwrap();
    let mut fields: Vec<&str> = row
        .doc
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort_unstable();
    assert_eq!(
        fields,
        [
            "checkpoint",
            "checkpoint_id",
            "ns",
            "seq",
            "subgraph",
            "thread"
        ]
    );
    assert_eq!(row.doc["subgraph"], "n/sub");
    assert_eq!(row.meta.schema, 4);
    let counter = docs
        .get("graph.threads", "flows/a%2Fb")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        counter.doc,
        json!({"ns": "flows", "thread": "a/b", "next": 1})
    );
    let writes = docs
        .get("graph.writes", "flows/a%2Fb/n%2Fsub/c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(writes.doc["subgraph"], "n/sub");
    assert_eq!(writes.doc["checkpoint_id"], "c1");
    assert_eq!(writes.doc["ns"], "flows");
}
