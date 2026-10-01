//! [`SqliteStateStore`]: the engine's `StateStore` capability over the
//! `flow_state` KV table, scoped to one namespace (a host passes one per flow
//! so two flows' state cannot collide). It also implements
//! [`DedupKv`] so a host can settle `dedup` nodes against the very namespace
//! the node wrote to during the run.

use std::path::PathBuf;

use async_trait::async_trait;
use serde_json::Value;
use tinyflows::caps::StateStore;
use tinyflows::error::{EngineError, Result};
use tinyflows::nodes::control_flow::dedup_settle::DedupKv;

use super::{kv_delete, kv_get, kv_set};

/// `StateStore` backed by the catalog directory's `flow_state` table.
#[derive(Debug, Clone)]
pub struct SqliteStateStore {
    /// The catalog directory holding `flows.db`.
    pub dir: PathBuf,
    /// The KV namespace every key is scoped to.
    pub namespace: String,
}

impl SqliteStateStore {
    /// A store over `dir` scoped to `namespace`.
    pub fn new(dir: impl Into<PathBuf>, namespace: impl Into<String>) -> Self {
        Self {
            dir: dir.into(),
            namespace: namespace.into(),
        }
    }
}

#[async_trait]
impl StateStore for SqliteStateStore {
    async fn load(&self, key: &str) -> Result<Option<Value>> {
        let (dir, namespace, key) = (self.dir.clone(), self.namespace.clone(), key.to_string());
        tokio::task::spawn_blocking(move || kv_get(&dir, &namespace, &key))
            .await
            .map_err(|e| EngineError::Capability(format!("flow state load task failed: {e}")))?
            .map_err(|e| EngineError::Capability(e.to_string()))
    }

    async fn store(&self, key: &str, value: Value) -> Result<()> {
        let (dir, namespace, key) = (self.dir.clone(), self.namespace.clone(), key.to_string());
        tokio::task::spawn_blocking(move || kv_set(&dir, &namespace, &key, &value))
            .await
            .map_err(|e| EngineError::Capability(format!("flow state store task failed: {e}")))?
            .map_err(|e| EngineError::Capability(e.to_string()))
    }
}

impl DedupKv for SqliteStateStore {
    fn kv_get(&self, key: &str) -> std::result::Result<Option<Value>, String> {
        kv_get(&self.dir, &self.namespace, key).map_err(|e| e.to_string())
    }
    fn kv_set(&self, key: &str, value: &Value) -> std::result::Result<(), String> {
        kv_set(&self.dir, &self.namespace, key, value).map_err(|e| e.to_string())
    }
    fn kv_delete(&self, key: &str) -> std::result::Result<(), String> {
        kv_delete(&self.dir, &self.namespace, key).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
#[path = "state_store_tests.rs"]
mod tests;
