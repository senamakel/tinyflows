//! `flows_kv`: the namespaced key/value state a host binds to the engine's
//! `StateStore` (through [`super::FlowStateDocuments`]) and settles `dedup`
//! nodes against.
//!
//! One document per `(namespace, key)`, id length-prefixed so no two pairs
//! share a slot, the value kept as a JSON string. These are the documents
//! [`super::FlowStateDocuments`] reads and writes, so a value a run stored is
//! what [`FlowCatalogDocuments::kv_get`] returns.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tinystoragedrivers_core::Precondition;

use super::{FlowCatalogDocuments, KV, composite_id, required};

fn kv_id(namespace: &str, key: &str) -> String {
    composite_id(&[namespace, key])
}

impl FlowCatalogDocuments {
    /// The value under `key` in `namespace`, or `None`.
    pub async fn kv_get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        let docs = self.docs().await?;
        let Some(stored) = docs.get(KV, &kv_id(namespace, key)).await? else {
            return Ok(None);
        };
        let value = serde_json::from_str(required(&stored, "value_json")?)
            .with_context(|| format!("flow state {namespace}/{key} is corrupt"))?;
        Ok(Some(value))
    }

    /// Stores `value` under `key` in `namespace`, replacing any earlier value.
    pub async fn kv_set(&self, namespace: &str, key: &str, value: &Value) -> Result<()> {
        let raw = serde_json::to_string(value).context("Failed to serialize flow state value")?;
        let docs = self.docs().await?;
        docs.put(
            KV,
            &kv_id(namespace, key),
            json!({ "namespace": namespace, "key": key, "value_json": raw }),
            Precondition::None,
        )
        .await
        .context("Failed to store flow state value")?;
        Ok(())
    }

    /// Deletes `key` from `namespace`; a missing key is not an error.
    pub async fn kv_delete(&self, namespace: &str, key: &str) -> Result<()> {
        let docs = self.docs().await?;
        docs.delete(KV, &kv_id(namespace, key), Precondition::None)
            .await
            .context("Failed to delete flow state value")?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "kv_tests.rs"]
mod tests;
