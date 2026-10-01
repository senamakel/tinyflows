//! The host half of the `dedup` node's commit-on-success contract, as pure
//! logic over a synchronous key/value seam.
//!
//! The [`DedupNode`](super::DedupNode) writes each unseen item's key into a
//! *tentative* set during a run. When the run finishes, the host settles every
//! `dedup` node in the flow through [`settle`]:
//!
//! - **success** — union `tentative` into `committed`, then clear `tentative`
//!   ([`commit`]);
//! - **anything else** — clear `tentative` only ([`release`]), so the keys are
//!   exactly as unseen as before the run and the next run reprocesses them.
//!
//! The host keeps everything with a lifecycle: the run-finished subscriber, the
//! per-flow lock that serializes concurrent settlements (this is a
//! read-modify-write, not a compare-and-swap), and which flow's graph names the
//! `dedup` nodes. This module only knows the key layout and the set algebra.

use std::collections::HashSet;

use serde_json::Value;

use super::dedup::{committed_key, tentative_key};

/// Synchronous key/value store scoped to one flow's state namespace: the same
/// namespace the engine's `StateStore` hands the `dedup` node during the run.
pub trait DedupKv {
    /// Loads the value under `key`; `Ok(None)` when absent.
    fn kv_get(&self, key: &str) -> Result<Option<Value>, String>;
    /// Stores `value` under `key`.
    fn kv_set(&self, key: &str, value: &Value) -> Result<(), String>;
    /// Deletes `key`; a missing key is not an error.
    fn kv_delete(&self, key: &str) -> Result<(), String>;
}

/// How [`commit`] ended for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    /// There were no tentative keys; nothing was written.
    NothingTentative,
    /// A stored set could not be read, or the committed set could not be
    /// written; both sets were left in place so the next successful run
    /// retries the commit.
    CommitFailed(String),
    /// Tentative keys were unioned into `committed`.
    Committed {
        /// Keys newly added (tentative keys not already committed).
        added: usize,
        /// Size of the committed set after the union.
        committed_len: usize,
        /// `Some(error)` when clearing `tentative` failed afterwards. Harmless:
        /// the committed set is idempotent under re-union.
        clear_error: Option<String>,
    },
}

/// What [`settle`] did for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settlement {
    /// The run succeeded; see the inner [`CommitOutcome`].
    Commit(CommitOutcome),
    /// The run did not succeed; `Err` carries a failed `tentative` delete
    /// (harmless: `committed` is untouched either way).
    Release(Result<(), String>),
}

/// Whether a finished run's status counts as success for settlement.
/// `completed_with_warnings` does; `failed`, `cancelled`, `interrupted` and any
/// unrecognized status do not ("retry an already-done item" is always safe,
/// "mark an uncertain outcome done" is not).
#[must_use]
pub fn is_success_status(status: &str) -> bool {
    matches!(status, "completed" | "completed_with_warnings")
}

/// Settles one `dedup` node: [`commit`] when `success`, else [`release`].
pub fn settle(kv: &dyn DedupKv, node_id: &str, success: bool) -> Settlement {
    if success {
        Settlement::Commit(commit(kv, node_id))
    } else {
        Settlement::Release(release(kv, node_id))
    }
}

/// Success path: union the node's `tentative` set into `committed`, then clear
/// `tentative`.
pub fn commit(kv: &dyn DedupKv, node_id: &str) -> CommitOutcome {
    let tentative_key = tentative_key(node_id);
    let committed_key = committed_key(node_id);

    let tentative = match load_key_set(kv, &tentative_key) {
        Ok(set) => set,
        Err(e) => return CommitOutcome::CommitFailed(e),
    };
    if tentative.is_empty() {
        return CommitOutcome::NothingTentative;
    }

    // A failed committed read must not be treated as an empty set: writing
    // only the tentative keys back would forget every previously committed
    // key and let those items through the dedup again.
    let mut committed = match load_key_set(kv, &committed_key) {
        Ok(set) => set,
        Err(e) => return CommitOutcome::CommitFailed(e),
    };
    let added = tentative
        .iter()
        .filter(|k| committed.insert((*k).clone()))
        .count();

    if let Err(e) = store_key_set(kv, &committed_key, &committed) {
        return CommitOutcome::CommitFailed(e);
    }
    CommitOutcome::Committed {
        added,
        committed_len: committed.len(),
        clear_error: kv.kv_delete(&tentative_key).err(),
    }
}

/// Failure path: clear `tentative` only, leaving `committed` untouched. Does
/// not read first: `kv_delete` already no-ops on a missing key.
pub fn release(kv: &dyn DedupKv, node_id: &str) -> Result<(), String> {
    kv.kv_delete(&tentative_key(node_id))
}

/// Loads a key set (a JSON array of strings). A missing key, non-array value,
/// or non-string elements degrade to an empty set: a first run against a fresh
/// store has nothing recorded, which is not a fault. A store error is
/// propagated, since treating it as empty would let [`commit`] overwrite the
/// real committed set.
fn load_key_set(kv: &dyn DedupKv, key: &str) -> Result<HashSet<String>, String> {
    Ok(match kv.kv_get(key) {
        Ok(Some(value)) => value
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        Ok(None) => HashSet::new(),
        Err(e) => {
            tracing::warn!(key, error = %e, "[dedup-commit] failed to load key set — aborting commit");
            return Err(e);
        }
    })
}

/// Persists `set` as a sorted JSON array of strings (stable, diffable).
fn store_key_set(kv: &dyn DedupKv, key: &str, set: &HashSet<String>) -> Result<(), String> {
    let mut keys: Vec<String> = set.iter().cloned().collect();
    keys.sort_unstable();
    let value = Value::Array(keys.into_iter().map(Value::String).collect());
    kv.kv_set(key, &value)
}

#[cfg(test)]
#[path = "dedup_settle_tests.rs"]
mod tests;
