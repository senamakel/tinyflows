//! [`StateStore`] over a `tinystoragedrivers` [`DocumentStore`].
//!
//! A host that has opened a storage backend hands the engine a scoped
//! document handle and wraps it here; every key is one `tsd.kv` entry of owner
//! [`KV_OWNER`] (`flows`) in one namespace (default
//! [`DriverStateStore::DEFAULT_NAMESPACE`]). The handle is already bound to a
//! tenant scope, so two tenants' runs never read each other's state.
//!
//! The retired `flows_state` collection moves here with
//! [`import_legacy_kv`](crate::import_legacy_kv).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tinyflows_storage::{DocumentStore, Kv, StorageError, primitives};
use tokio::sync::OnceCell;

use tinyflows::caps::StateStore;
use tinyflows::error::{EngineError, Result};

use crate::storage::{KV_OWNER, STATE_NAMESPACE};

/// A [`StateStore`] keeping each key in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverStateStore {
    docs: Arc<dyn DocumentStore>,
    namespace: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverStateStore {
    /// The `tsd.kv` namespace [`Self::new`] uses.
    pub const DEFAULT_NAMESPACE: &'static str = STATE_NAMESPACE;

    /// State in namespace [`Self::DEFAULT_NAMESPACE`] of `docs`.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_namespace(docs, Self::DEFAULT_NAMESPACE)
    }

    /// State in `namespace` of `docs`, so independent stores can share one
    /// backend.
    pub fn with_namespace(docs: Arc<dyn DocumentStore>, namespace: impl Into<String>) -> Self {
        Self {
            docs,
            namespace: namespace.into(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    async fn kv(&self) -> Result<Kv> {
        self.declared
            .get_or_try_init(|| async {
                for spec in primitives::collection_specs() {
                    self.docs.ensure_collection(&spec).await?;
                }
                Ok::<(), StorageError>(())
            })
            .await
            .map_err(map_error)?;
        Kv::ns(&self.docs, KV_OWNER, self.namespace.as_str()).map_err(map_error)
    }
}

/// A storage failure as the engine's capability error.
fn map_error(error: StorageError) -> EngineError {
    EngineError::Capability(format!("state store: {error}"))
}

#[async_trait]
impl StateStore for DriverStateStore {
    async fn load(&self, key: &str) -> Result<Option<Value>> {
        self.kv().await?.get(key).await.map_err(map_error)
    }

    async fn store(&self, key: &str, value: Value) -> Result<()> {
        self.kv()
            .await?
            .put(key, &value)
            .await
            .map(|_| ())
            .map_err(map_error)
    }
}

#[cfg(test)]
#[path = "state_store_tests.rs"]
mod tests;
