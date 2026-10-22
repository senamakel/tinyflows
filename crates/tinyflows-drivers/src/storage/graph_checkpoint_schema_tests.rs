use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{
    DocumentStoreExt, MemoryStorage, MigrateOptions, Precondition, Query, Registry, Scope,
    ScopeClass, ScopedStorage, StorageBackend,
};

use crate::checkpoint::DriverCheckpointer;
use tinyflows::graph::{Checkpoint, Checkpointer};

fn scoped() -> ScopedStorage {
    MemoryStorage::new()
        .for_scope(&Scope::local())
        .expect("local scope")
}

#[tokio::test]
async fn schema_migrates_flow_checkpoints_and_supports_resume_writes() {
    let storage = scoped();
    let mut nested = serde_json::to_value(checkpoint("thread-1", "checkpoint-1", 7)).unwrap();
    nested["namespace"] = json!(["nested"]);
    nested["created_at"] = json!(1234);
    storage
        .documents()
        .put(
            "flows_graph_checkpoints",
            "8:thread-1/20:00000000000000000000",
            json!({
                "thread": "thread-1",
                "namespace": [],
                "seq": 0,
                "checkpoint_id": "checkpoint-1",
                "record": nested,
            }),
            Precondition::None,
        )
        .await
        .unwrap();
    storage
        .documents()
        .put(
            "flows_graph_checkpoints",
            "8:thread-1/20:00000000000000000001",
            json!({
                "thread": "thread-1",
                "seq": 1,
                "checkpoint_id": "checkpoint-2",
                "record": checkpoint("thread-1", "checkpoint-2", 8),
            }),
            Precondition::None,
        )
        .await
        .unwrap();
    let fallback_created_ms = storage
        .documents()
        .get(
            "flows_graph_checkpoints",
            "8:thread-1/20:00000000000000000001",
        )
        .await
        .unwrap()
        .expect("legacy root source")
        .meta
        .created_ms;
    storage
        .documents()
        .put(
            "flows_graph_threads",
            "8:thread-1",
            json!({"thread": "thread-1", "next": 2}),
            Precondition::None,
        )
        .await
        .unwrap();

    let schema = super::graph_checkpoint_schema();
    assert_eq!(schema.owner.as_str(), "graph");
    assert!(
        schema
            .items
            .iter()
            .all(|item| item.scope() == ScopeClass::Agent)
    );
    let registry = Registry::compose([schema]).unwrap();
    for item in super::graph_checkpoint_schema().items {
        if let tinystoragedrivers_core::Item::Collection { spec, .. } = item {
            storage.documents().ensure_collection(&spec).await.unwrap();
        }
    }
    registry
        .migrate(&storage, &MigrateOptions::new())
        .await
        .unwrap();

    let checkpointer =
        DriverCheckpointer::<i32>::with_namespace(Arc::clone(storage.documents()), "flows");
    let resumed = checkpointer
        .get_scoped("thread-1", Some("checkpoint-1"), &["nested".to_owned()])
        .await
        .unwrap()
        .expect("migrated nested flow checkpoint");
    assert_eq!(resumed.state, 7);
    let root = checkpointer.get("thread-1", None).await.unwrap().unwrap();
    assert_eq!(root.state, 8);

    let nested_record = storage
        .documents()
        .query_all("graph.checkpoints", &Query::all())
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.doc["checkpoint_id"] == "checkpoint-1")
        .expect("migrated nested record");
    assert_eq!(nested_record.doc["subgraph"], "n/nested");
    assert_eq!(nested_record.meta.created_ms, 1234);
    let root_record = storage
        .documents()
        .query_all("graph.checkpoints", &Query::all())
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.doc["checkpoint_id"] == "checkpoint-2")
        .unwrap();
    assert_eq!(root_record.meta.created_ms, fallback_created_ms);

    let next = checkpoint("thread-1", "checkpoint-2", 8);
    checkpointer.put(next).await.unwrap();
    assert_eq!(checkpointer.get_thread("thread-1").await.unwrap().len(), 3);
}

fn checkpoint(thread: &str, id: &str, state: i32) -> Checkpoint<i32> {
    Checkpoint {
        thread_id: thread.to_owned(),
        checkpoint_id: id.to_owned(),
        run_id: None,
        parent_checkpoint_id: None,
        namespace: Vec::new(),
        state,
        next_nodes: Vec::new(),
        completed_tasks: Vec::new(),
        pending_writes: Vec::new(),
        interrupts: Vec::new(),
        pending_activations: None,
        barrier_arrivals: Vec::new(),
        metadata: json!({"source": "loop"}),
    }
}
