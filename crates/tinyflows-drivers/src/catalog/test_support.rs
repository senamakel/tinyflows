//! Fixtures shared by the catalog tests.

use std::sync::Arc;

use tinyflows::model::{Node, NodeKind, WorkflowGraph};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

use super::FlowCatalogDocuments;

/// The catalog under `scope` of `storage`.
pub(crate) fn catalog_in(storage: &MemoryStorage, scope: &str) -> FlowCatalogDocuments {
    FlowCatalogDocuments::new(docs_in(storage, scope))
}

/// A fresh catalog on its own memory backend.
pub(crate) fn catalog() -> FlowCatalogDocuments {
    catalog_in(&MemoryStorage::new(), "local")
}

/// The raw document handle under `scope`.
pub(crate) fn docs_in(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

fn trigger(config: serde_json::Value) -> WorkflowGraph {
    WorkflowGraph {
        nodes: vec![Node {
            id: "t".to_string(),
            kind: NodeKind::Trigger,
            type_version: 1,
            name: "Trigger".to_string(),
            config,
            ports: Vec::new(),
            position: None,
        }],
        ..Default::default()
    }
}

/// A manual-trigger graph.
pub(crate) fn trigger_graph() -> WorkflowGraph {
    trigger(serde_json::Value::Null)
}

/// An automatic (`schedule`) trigger graph.
pub(crate) fn automatic_schedule_graph() -> WorkflowGraph {
    trigger(serde_json::json!({ "trigger_kind": "schedule", "schedule": "0 9 * * *" }))
}
