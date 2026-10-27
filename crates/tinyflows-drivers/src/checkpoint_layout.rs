//! The stored layout of the graph checkpointer (storage schema v2).
//!
//! tinyflows keeps its own checkpointer code, but the documents it reads and
//! writes are exactly the ones `tinyagents-graph` stores for namespace
//! [`NAMESPACE`] (`flows`): the owner `graph` declares the three collections
//! and migrates the retired `flows_graph_*` ones, so this repository declares
//! none of them. The compatibility is pinned by `checkpoint_layout_tests.rs`
//! against fixtures copied from tinyagents.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyflows::graph::CheckpointConfig;
use tinyflows::graph::PendingWrite;
use tinyflows::graph::error::{GraphError, Result};
use tinyflows_storage::{CollectionSpec, IndexSpec, Key, StorageError};

/// The body version of every `graph.*` document (tinyagents' `SCHEMA_VERSION`).
pub(crate) const SCHEMA_VERSION: u32 = 4;

/// Stored checkpoints, one document per checkpoint.
pub(crate) const CHECKPOINTS: &str = "graph.checkpoints";

/// The per-thread insertion counters.
pub(crate) const THREADS: &str = "graph.threads";

/// The merged pending writes of a checkpoint.
pub(crate) const WRITES: &str = "graph.writes";

/// The checkpointer namespace workflows use (`ns` of every `graph.*` document).
pub const NAMESPACE: &str = "flows";

/// The body of one `graph.checkpoints` document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CheckpointBody {
    pub(crate) ns: String,
    pub(crate) thread: String,
    pub(crate) subgraph: String,
    pub(crate) seq: u64,
    pub(crate) checkpoint_id: String,
    pub(crate) checkpoint: Value,
}

/// The body of one `graph.threads` document: the next insertion sequence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ThreadBody {
    pub(crate) ns: String,
    pub(crate) thread: String,
    pub(crate) next: u64,
}

/// The body of one `graph.writes` document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct WritesBody {
    pub(crate) ns: String,
    pub(crate) thread: String,
    pub(crate) subgraph: String,
    pub(crate) checkpoint_id: String,
    pub(crate) writes: Vec<PendingWrite>,
    /// Negative-index identities last supplied by an importer. A live control
    /// write claims its identity and removes it; old documents default empty.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub(crate) imported_control_writes: BTreeSet<(String, i64)>,
}

/// A subgraph namespace as one string, injectively: the root is empty and any
/// other namespace is `Key::of(["n", parts...])`, so `["a", "b"]`, `["a/b"]`
/// and `[""]` never meet.
pub(crate) fn subgraph_key(namespace: &[String]) -> String {
    if namespace.is_empty() {
        return String::new();
    }
    Key::of(std::iter::once("n").chain(namespace.iter().map(String::as_str)))
        .map(|key| key.as_str().to_owned())
        .unwrap_or_default()
}

/// The id of the checkpoint inserted at `seq` for `thread` in `ns`.
pub(crate) fn checkpoint_id_key(ns: &str, thread: &str, seq: u64) -> String {
    key(&[ns, thread, &format!("{seq:020}")])
}

/// The id of `thread`'s counter document in `ns`.
pub(crate) fn thread_id_key(ns: &str, thread: &str) -> String {
    key(&[ns, thread])
}

/// The id of the pending writes of `checkpoint_id` in `subgraph`.
pub(crate) fn writes_id_key(ns: &str, thread: &str, subgraph: &str, checkpoint_id: &str) -> String {
    key(&[ns, thread, subgraph, checkpoint_id])
}

/// `Key::of(parts)` as text; the parts are never empty, so it cannot fail.
fn key(parts: &[&str]) -> String {
    Key::of(parts)
        .map(|key| key.as_str().to_owned())
        .unwrap_or_default()
}

/// `graph.checkpoints`: every read filters on the namespace and thread.
pub(crate) fn checkpoints_spec() -> CollectionSpec {
    CollectionSpec::new(CHECKPOINTS)
        .index(IndexSpec::new("by_thread_seq", ["ns", "thread", "seq"]))
        .index(IndexSpec::new(
            "by_thread_id",
            ["ns", "thread", "checkpoint_id"],
        ))
        .index(IndexSpec::new(
            "by_thread_subgraph",
            ["ns", "thread", "subgraph", "seq"],
        ))
}

/// `graph.threads`.
pub(crate) fn threads_spec() -> CollectionSpec {
    CollectionSpec::new(THREADS).index(IndexSpec::new("by_ns", ["ns"]))
}

/// `graph.writes`.
pub(crate) fn writes_spec() -> CollectionSpec {
    CollectionSpec::new(WRITES).index(IndexSpec::new("by_thread", ["ns", "thread"]))
}

/// Map a storage driver failure onto the graph's checkpoint error.
pub(crate) fn map_error(error: StorageError) -> GraphError {
    GraphError::Checkpoint(format!("storage driver: {error}"))
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

#[cfg(test)]
#[path = "checkpoint_layout_tests.rs"]
mod tests;
