//! Namespaced key/value state a host binds to the engine's `StateStore`
//! (through [`super::FlowStateDocuments`]) and settles `dedup` nodes against.
//!
//! The values live in `tsd.kv` under owner `flows`, one `tsd.kv` namespace per
//! catalog namespace (`kv:<namespace>`, see
//! [`catalog_namespace`](crate::storage::catalog_namespace)); the retired
//! `flows_kv` collection moves there with
//! [`import_legacy_kv`](crate::storage::import_legacy_kv).

use anyhow::{Context, Result};
use serde_json::Value;
use tinyflows_storage::Kv;

use super::FlowCatalogDocuments;
use crate::storage::{KV_OWNER, catalog_namespace};

impl FlowCatalogDocuments {
    /// The `tsd.kv` handle of `namespace`, (`tsd.kv` is declared once by [`FlowCatalogDocuments::ensure`]).
    async fn kv(&self, namespace: &str) -> Result<Kv> {
        let docs = self.docs().await?;
        Ok(Kv::ns(docs, KV_OWNER, catalog_namespace(namespace))?)
    }

    /// The value under `key` in `namespace`, or `None`.
    pub async fn kv_get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        self.kv(namespace)
            .await?
            .get(key)
            .await
            .with_context(|| format!("flow state {namespace}/{key} is corrupt"))
    }

    /// Stores `value` under `key` in `namespace`, replacing any earlier value.
    pub async fn kv_set(&self, namespace: &str, key: &str, value: &Value) -> Result<()> {
        self.kv(namespace)
            .await?
            .put(key, value)
            .await
            .context("Failed to store flow state value")?;
        Ok(())
    }

    /// Deletes `key` from `namespace`; a missing key is not an error.
    pub async fn kv_delete(&self, namespace: &str, key: &str) -> Result<()> {
        self.kv(namespace)
            .await?
            .delete(key)
            .await
            .context("Failed to delete flow state value")?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;
