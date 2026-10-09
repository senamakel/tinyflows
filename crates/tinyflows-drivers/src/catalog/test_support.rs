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

/// What [`Interposed`] runs, once, against the store it wraps.
pub(crate) type Interjection = Box<
    dyn FnOnce(
            Arc<dyn DocumentStore>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send,
>;

/// A store that runs one scripted write against the store it wraps the
/// first time a chosen read happens, so a test can land another process's
/// change in an exact gap between two steps of an operation.
#[derive(Debug)]
pub(crate) struct Interposed {
    inner: Arc<dyn DocumentStore>,
    /// `("query" | "get", collection)` that triggers the interjection.
    trigger: (&'static str, &'static str),
    interjection: std::sync::Mutex<Option<InterjectionCell>>,
}

/// [`Interjection`] in a `Debug` wrapper.
pub(crate) struct InterjectionCell(Interjection);

impl std::fmt::Debug for InterjectionCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Interjection(..)")
    }
}

impl Interposed {
    /// Wraps `inner`; `interjection` runs before the first `trigger` read.
    pub(crate) fn wrap(
        inner: Arc<dyn DocumentStore>,
        trigger: (&'static str, &'static str),
        interjection: Interjection,
    ) -> Arc<dyn DocumentStore> {
        Arc::new(Self {
            inner,
            trigger,
            interjection: std::sync::Mutex::new(Some(InterjectionCell(interjection))),
        })
    }

    async fn maybe_interject(&self, op: &str, collection: &str) {
        if self.trigger != (op, collection) {
            return;
        }
        let taken = self.interjection.lock().unwrap().take();
        if let Some(InterjectionCell(interjection)) = taken {
            interjection(Arc::clone(&self.inner)).await;
        }
    }
}

#[tinystoragedrivers_core::async_trait]
impl DocumentStore for Interposed {
    fn capabilities(&self) -> tinystoragedrivers_core::Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(
        &self,
        spec: &tinystoragedrivers_core::CollectionSpec,
    ) -> tinystoragedrivers_core::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_core::Result<
        Option<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.maybe_interject("get", collection).await;
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: serde_json::Value,
        precondition: tinystoragedrivers_core::Precondition,
    ) -> tinystoragedrivers_core::Result<tinystoragedrivers_core::Version> {
        self.inner.put(collection, id, doc, precondition).await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: tinystoragedrivers_core::Precondition,
    ) -> tinystoragedrivers_core::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &tinystoragedrivers_core::Query,
    ) -> tinystoragedrivers_core::Result<
        tinystoragedrivers_core::Page<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.maybe_interject("query", collection).await;
        self.inner.query(collection, query).await
    }

    async fn count(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
    ) -> tinystoragedrivers_core::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_core::Filter,
        sort: &[tinystoragedrivers_core::Sort],
        patch: &serde_json::Value,
    ) -> tinystoragedrivers_core::Result<
        Option<tinystoragedrivers_core::Versioned<serde_json::Value>>,
    > {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_core::Result<()> {
        self.inner.drop_collection(collection).await
    }
}
