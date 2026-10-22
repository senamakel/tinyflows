use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{
    MemoryStorage, MigrateOptions, Precondition, Registry, Scope, ScopeClass, ScopedStorage,
    StorageBackend,
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
    let legacy = checkpoint("thread-1", "checkpoint-1", 7);
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
                "record": serde_json::to_value(&legacy).unwrap(),
            }),
            Precondition::None,
        )
        .await
        .unwrap();
    storage
        .documents()
        .put(
            "flows_graph_threads",
            "8:thread-1",
            json!({"thread": "thread-1", "next": 1}),
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
        .get("thread-1", None)
        .await
        .unwrap()
        .expect("migrated flow checkpoint");
    assert_eq!(resumed.state, 7);

    let next = checkpoint("thread-1", "checkpoint-2", 8);
    checkpointer.put(next).await.unwrap();
    assert_eq!(checkpointer.get_thread("thread-1").await.unwrap().len(), 2);
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
