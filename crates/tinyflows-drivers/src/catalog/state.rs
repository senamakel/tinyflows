//! [`FlowStateDocuments`]: one flow's namespaced state, as the engine's
//! [`StateStore`] and the host's synchronous [`DedupKv`].

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use serde_json::Value;
use tinyflows::caps::StateStore;
use tinyflows::error::{EngineError, Result};
use tinyflows::nodes::control_flow::dedup_settle::DedupKv;
use tinystoragedrivers_core::Blocking;

use super::FlowCatalogDocuments;

/// The document-port counterpart of `tinyflows_sqlite::flows::SqliteStateStore`:
/// state scoped to one namespace (a host passes one per flow), over the same
/// `flows_kv` documents as [`FlowCatalogDocuments::kv_get`] /
/// [`FlowCatalogDocuments::kv_set`] / [`FlowCatalogDocuments::kv_delete`].
///
/// [`DedupKv`] is synchronous and hosts call it from async code, so it runs
/// each call on a [`Blocking`] bridge: a dedicated runtime thread, safe from
/// inside any runtime (including a current-thread one, where `block_on` or
/// `block_in_place` would panic). [`Self::new`] shares one bridge per process,
/// started on first use; [`Self::with_bridge`] supplies a host's own.
///
/// `tinyflows_drivers::DriverStateStore` stays as it was: un-namespaced, one
/// document per key in its own collection, for a host that gives each run a
/// collection of its own. This type is for hosts that bind state per flow.
#[derive(Debug, Clone)]
pub struct FlowStateDocuments {
    catalog: FlowCatalogDocuments,
    namespace: String,
    bridge: Option<Arc<Blocking>>,
}

/// The bridge [`FlowStateDocuments::new`] shares across the process.
fn shared_bridge() -> std::result::Result<&'static Blocking, String> {
    static BRIDGE: OnceLock<std::result::Result<Blocking, String>> = OnceLock::new();
    BRIDGE
        .get_or_init(|| Blocking::new().map_err(|error| error.to_string()))
        .as_ref()
        .map_err(Clone::clone)
}

impl FlowStateDocuments {
    /// State in `namespace` of `catalog`, its synchronous calls on the
    /// process-wide bridge.
    pub fn new(catalog: FlowCatalogDocuments, namespace: impl Into<String>) -> Self {
        Self {
            catalog,
            namespace: namespace.into(),
            bridge: None,
        }
    }

    /// Like [`Self::new`], with the host's own bridge.
    pub fn with_bridge(
        catalog: FlowCatalogDocuments,
        namespace: impl Into<String>,
        bridge: Arc<Blocking>,
    ) -> Self {
        Self {
            catalog,
            namespace: namespace.into(),
            bridge: Some(bridge),
        }
    }

    /// The namespace every key is scoped to.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Runs `op` against the catalog from synchronous code.
    fn blocking<T, F, Fut>(&self, op: F) -> std::result::Result<T, String>
    where
        F: FnOnce(FlowCatalogDocuments, String) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        let future = op(self.catalog.clone(), self.namespace.clone());
        let ran = match &self.bridge {
            Some(bridge) => bridge.run(future),
            None => shared_bridge()?.run(future),
        };
        ran.map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())
    }
}

fn capability(error: anyhow::Error) -> EngineError {
    EngineError::Capability(format!("flow state: {error}"))
}

#[async_trait]
impl StateStore for FlowStateDocuments {
    async fn load(&self, key: &str) -> Result<Option<Value>> {
        self.catalog
            .kv_get(&self.namespace, key)
            .await
            .map_err(capability)
    }

    async fn store(&self, key: &str, value: Value) -> Result<()> {
        self.catalog
            .kv_set(&self.namespace, key, &value)
            .await
            .map_err(capability)
    }
}

impl DedupKv for FlowStateDocuments {
    fn kv_get(&self, key: &str) -> std::result::Result<Option<Value>, String> {
        let key = key.to_string();
        self.blocking(|catalog, namespace| async move { catalog.kv_get(&namespace, &key).await })
    }

    fn kv_set(&self, key: &str, value: &Value) -> std::result::Result<(), String> {
        let (key, value) = (key.to_string(), value.clone());
        self.blocking(
            |catalog, namespace| async move { catalog.kv_set(&namespace, &key, &value).await },
        )
    }

    fn kv_delete(&self, key: &str) -> std::result::Result<(), String> {
        let key = key.to_string();
        self.blocking(|catalog, namespace| async move { catalog.kv_delete(&namespace, &key).await })
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
