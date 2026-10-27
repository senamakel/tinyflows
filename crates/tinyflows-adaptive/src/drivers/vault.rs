//! [`Vault`] over a driver [`DocumentStore`]: one document per
//! `(bucket, workflow)`, read back as this bucket plus global with this
//! bucket's record winning — the Mongo vault's rule.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tinyflows::store::types::{WorkflowError, WorkflowRecord};
use tinyflows_storage::{
    DocumentStore, DocumentStoreExt, Filter, Precondition, Query, Sort, StorageError, WriteOptions,
    primitives,
};
use tokio::sync::OnceCell;

use super::convert::{from_object, object, workflow_body, workflow_from};
use super::{SCHEMA_VERSION, WORKFLOWS, collections, key};
use crate::workflows::Vault;

fn backend(error: StorageError) -> WorkflowError {
    WorkflowError::Engine(format!("vault: {error}"))
}

/// A vault stored in a driver document store.
#[derive(Clone)]
pub struct DriverVault {
    docs: Arc<dyn DocumentStore>,
    scope: Option<String>,
    declared: Arc<OnceCell<()>>,
}

impl std::fmt::Debug for DriverVault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverVault")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl DriverVault {
    /// A vault in `docs`, reading and writing the global bucket.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs,
            scope: None,
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// A handle onto the same store, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: impl Into<String>) -> Self {
        Self {
            docs: Arc::clone(&self.docs),
            scope: Some(scope.into()),
            declared: Arc::clone(&self.declared),
        }
    }

    fn bucket(&self) -> &str {
        self.scope.as_deref().unwrap_or_default()
    }

    async fn declared(&self) -> Result<(), WorkflowError> {
        self.declared
            .get_or_try_init(|| async {
                for spec in collections()
                    .into_iter()
                    .chain(primitives::collection_specs())
                {
                    self.docs.ensure_collection(&spec).await.map_err(backend)?;
                }
                Ok::<(), WorkflowError>(())
            })
            .await
            .map(|_| ())
    }
}

#[async_trait]
impl Vault for DriverVault {
    fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        self.declared().await?;
        // Global (`""`) sorts first, so this bucket's record for the same id
        // is inserted last and wins.
        let query = Query::filter(Filter::one_of("scope_key", [self.bucket(), ""]))
            .sort(Sort::asc("scope_key"))
            .sort(Sort::asc("workflow_id"));
        let mut chosen: BTreeMap<String, WorkflowRecord> = BTreeMap::new();
        for stored in self
            .docs
            .query_all(WORKFLOWS, &query)
            .await
            .map_err(backend)?
        {
            let record: WorkflowRecord =
                workflow_from(&stored.doc)
                    .and_then(from_object)
                    .map_err(|e| {
                        WorkflowError::Engine(format!("stored workflow no longer parses: {e}"))
                    })?;
            chosen.insert(record.id.clone(), record);
        }
        Ok(chosen.into_values().collect())
    }

    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        self.declared().await?;
        let fields = object(record)
            .map_err(|e| WorkflowError::Engine(format!("workflow will not serialize: {e}")))?;
        let doc = workflow_body(fields, self.bucket());
        let id = key(&[self.bucket(), &record.id]).map_err(backend)?;
        self.docs
            .put_with(
                WORKFLOWS,
                &id,
                doc,
                Precondition::None,
                WriteOptions::new().schema(SCHEMA_VERSION),
            )
            .await
            .map(|_| ())
            .map_err(backend)
    }

    async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
        self.declared().await?;
        let key = key(&[self.bucket(), id]).map_err(backend)?;
        self.docs
            .delete(WORKFLOWS, &key, Precondition::None)
            .await
            .map(|_| ())
            .map_err(backend)
    }
}

#[cfg(test)]
#[path = "vault_tests.rs"]
mod tests;
