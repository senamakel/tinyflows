//! [`StateStore`] over a `tinystoragedrivers` [`DocumentStore`].
//!
//! A host that has opened a storage backend hands the engine a scoped
//! document handle and wraps it here; every key is one document
//! `{ "key": <key>, "value": <value> }` in one collection (default
//! [`DriverStateStore::DEFAULT_COLLECTION`]). The handle is already bound to a
//! tenant scope, so two tenants' runs never read each other's state.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, ErrorKind, Precondition, StorageError,
};
use tokio::sync::OnceCell;

use super::StateStore;
use crate::error::{Result, TinyFlowsError};

/// A [`StateStore`] keeping each key in a driver [`DocumentStore`].
#[derive(Debug, Clone)]
pub struct DriverStateStore {
    docs: Arc<dyn DocumentStore>,
    collection: String,
    declared: Arc<OnceCell<()>>,
}

impl DriverStateStore {
    /// The collection [`Self::new`] uses.
    pub const DEFAULT_COLLECTION: &'static str = "flows_state";

    /// State in [`Self::DEFAULT_COLLECTION`] of `docs`.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self::with_collection(docs, Self::DEFAULT_COLLECTION)
    }

    /// State in `collection` of `docs`, so independent stores can share one
    /// backend.
    pub fn with_collection(docs: Arc<dyn DocumentStore>, collection: impl Into<String>) -> Self {
        Self {
            docs,
            collection: collection.into(),
            declared: Arc::new(OnceCell::new()),
        }
    }

    async fn declared(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                self.docs
                    .ensure_collection(&CollectionSpec::new(&self.collection))
                    .await
                    .map_err(map_error)
            })
            .await
            .map(|_| ())
    }
}

/// A state key as a document id: keys longer than the driver's id limit are
/// refused rather than truncated or hashed, so two keys never share a slot.
fn map_error(error: StorageError) -> TinyFlowsError {
    match error.kind() {
        ErrorKind::InvalidInput => TinyFlowsError::Validation(format!("state store: {error}")),
        _ => TinyFlowsError::Capability(format!("state store: {error}")),
    }
}

#[async_trait]
impl StateStore for DriverStateStore {
    async fn load(&self, key: &str) -> Result<Option<Value>> {
        self.declared().await?;
        let found = self
            .docs
            .get(&self.collection, key)
            .await
            .map_err(map_error)?;
        Ok(found.and_then(|mut stored| stored.doc.get_mut("value").map(Value::take)))
    }

    async fn store(&self, key: &str, value: Value) -> Result<()> {
        self.declared().await?;
        self.docs
            .put(
                &self.collection,
                key,
                json!({ "key": key, "value": value }),
                Precondition::None,
            )
            .await
            .map(|_| ())
            .map_err(map_error)
    }
}

#[cfg(test)]
#[path = "drivers_tests.rs"]
mod tests;
