use std::sync::Arc;

use serde_json::json;
use tinystoragedrivers_core::{
    DocumentStoreExt, MemoryStorage, MigrateOptions, Precondition, Registry, Scope, ScopeClass,
    ScopedStorage, StorageBackend,
};

use crate::checkpoint::{Checkpoint, Checkpointer, DriverCheckpointer};

fn scoped() -> ScopedStorage {
    MemoryStorage::new()
        .for_scope(&Scope::local())
        .expect("local scope")
}

#[tokio::test]
async fn schema_migrates_flow_checkpoints_and_supports_resume_writes() {
    let storage = scoped();
    let legacy = Checkpoint::new(7_i32, Vec::new())
        .with_thread_id("thread-1".to_owned())
        .with_checkpoint_id("checkpoint-1".to_owned());
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
    assert!(schema.items.iter().all(|item| item.scope() == ScopeClass::Agent));
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

    let checkpointer = DriverCheckpointer::<i32>::with_namespace(
        Arc::clone(storage.documents()),
        "flows",
    );
    let resumed = checkpointer
        .get("thread-1", None)
        .await
        .unwrap()
        .expect("migrated flow checkpoint");
    assert_eq!(resumed.state, 7);

    let next = Checkpoint::new(8_i32, Vec::new())
        .with_thread_id("thread-1".to_owned())
        .with_checkpoint_id("checkpoint-2".to_owned());
    checkpointer.put(next).await.unwrap();
    assert_eq!(checkpointer.get_thread("thread-1").await.unwrap().len(), 2);
}
