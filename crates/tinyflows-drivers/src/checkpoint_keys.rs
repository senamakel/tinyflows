//! Document-id and error helpers shared by the checkpointer.

use tinyflows::graph::CheckpointConfig;
use tinyflows::graph::error::{GraphError, Result};
use tinystoragedrivers_core::StorageError;

/// Map a storage driver failure onto the graph's checkpoint error.
pub(crate) fn map_error(error: StorageError) -> GraphError {
    GraphError::Checkpoint(format!("storage driver: {error}"))
}

/// Longest id stored as is; longer ones are hashed.
const MAX_KEY_LEN: usize = 400;

/// A document id for `parts`: length-prefixed so no two tuples collide, and
/// replaced by its SHA-256 when it would exceed the driver's id limit.
pub(crate) fn key(parts: &[&str]) -> String {
    let joined: String = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<Vec<_>>()
        .join("/");
    if joined.len() <= MAX_KEY_LEN {
        joined
    } else {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(joined.as_bytes());
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        format!("h:{hex}")
    }
}

/// `namespace` as one string, injectively: each component is
/// length-prefixed, so `["a", "b"]` and `["a/b"]` never meet.
pub(crate) fn namespace_key(namespace: &[String]) -> String {
    namespace
        .iter()
        .map(|part| format!("{}:{part};", part.len()))
        .collect()
}

/// The checkpoint id a write must name; a write against "the latest
/// checkpoint" has no meaning, so it is refused.
pub(crate) fn require_checkpoint_id(config: &CheckpointConfig) -> Result<String> {
    config.checkpoint_id.clone().ok_or_else(|| {
        GraphError::Checkpoint(format!(
            "put_writes requires an explicit checkpoint_id (thread `{}`)",
            config.thread_id
        ))
    })
}
