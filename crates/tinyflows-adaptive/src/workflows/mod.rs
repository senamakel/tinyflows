//! Workflows in whatever the host configured, behind the engine's own trait.
//!
//! The ledger has three backends chosen at boot. Workflows had one — a
//! directory of JSON files — which is wrong for a hosted service and wrong for
//! the symmetry: a deployment picks Mongo for one half of its durable state and
//! gets a filesystem for the other.
//!
//! # Why this is a snapshot and not a store
//!
//! [`tinyflows::store::WorkflowStore`] is **synchronous** — ten required
//! methods, none of them `async`. A Mongo driver is not. The obvious fixes are
//! both bad: `block_on` inside a sync method deadlocks a current-thread
//! runtime, and async-ifying the trait upstream means rewriting the file store
//! and the authoring module and then contending with that rewrite on every
//! merge from upstream. The fork stays mergeable or it stops being a fork.
//!
//! So the async half is ours ([`Vault`]) and the sync half is a snapshot over
//! it: load once, serve every read from memory, buffer writes, flush after.
//! That fits how the loop actually uses a store — a handful of reads while
//! deciding, at most one or two writes when closing — and it makes the reads
//! free rather than a round trip each.
//!
//! # Two things fall out of it
//!
//! **Workflows become tenant-scoped**, which they were not. The engine's store
//! has no scope, so a repaired variant of one tenant's workflow appeared in
//! every tenant's catalogue. A `Vault` is scoped like a `Ledger`, so this
//! closes that as a side effect rather than as a separate feature.
//!
//! **Concurrent flushes are safe by construction**, because every id this crate
//! writes is content-derived — [`crate::reuse::shape_id`] for a learned graph, a
//! digest of the edits for a variant. Two episodes that arrive at the same
//! procedure write the same id with byte-identical content, so last-write-wins
//! is not a lost update. A snapshot only flushes what it actually changed, so a
//! human editing a workflow the loop never touched is never clobbered.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tinyflows::store::types::{
    RunRecord, WorkflowError, WorkflowRecord, WorkflowRevision, WorkflowSummary,
};
use tinyflows::store::{HostPolicy, WorkflowStore};

pub mod memory;
#[cfg(feature = "mongo")]
pub mod mongo;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub mod compat;
pub mod conformance;

/// Durable workflow storage, in whatever the host configured.
///
/// Deliberately narrower than [`WorkflowStore`]: load everything, write one,
/// delete one. Run records, revisions, notes and proposals are the engine's
/// authoring surface and this crate neither reads nor writes them — a `Vault`
/// that had to implement them would be ten methods of `unimplemented!` in every
/// backend.
#[async_trait]
pub trait Vault: Send + Sync {
    /// Whose workflows this handle sees. `None` is the global bucket, and the
    /// rule is the ledger's: writes go to this bucket, reads return this bucket
    /// plus global.
    fn scope(&self) -> Option<&str> {
        None
    }

    /// Every workflow in scope, **at most one record per id**: when the same
    /// id exists in this handle's bucket and in global, the handle's own wins.
    ///
    /// The precedence is each backend's obligation rather than the caller's,
    /// because the caller dedupes by id in arrival order — leaving it to
    /// storage iteration order would make "whose record wins" an
    /// implementation accident that differs across backends.
    ///
    /// The whole catalogue in one call, because a snapshot loads once and a
    /// tenant's procedures number in the tens, not the millions. A host that
    /// outgrows that wants a different seam, not a paged version of this one.
    ///
    /// # Errors
    /// When the backend is unreachable or holds a record that no longer parses.
    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError>;

    /// Write one, replacing any with the same id.
    ///
    /// # Errors
    /// When the backend refuses the write.
    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError>;

    /// Remove one. Removing what is not there is not an error.
    ///
    /// # Errors
    /// When the backend refuses.
    async fn remove(&self, id: &str) -> Result<(), WorkflowError>;
}

/// The engine's synchronous store, served from memory.
///
/// Cheap to clone — clones share the same buffer, so a `Snapshot` handed to the
/// loop as `Arc<dyn WorkflowStore>` and the one you flush are the same state.
#[derive(Clone)]
pub struct Snapshot {
    records: Arc<Mutex<BTreeMap<String, WorkflowRecord>>>,
    /// Ids written or deleted through the sync surface. Only these are flushed,
    /// so a concurrent editor of an untouched workflow is never clobbered.
    dirty: Arc<Mutex<BTreeMap<String, Option<WorkflowRecord>>>>,
    policy: Arc<dyn HostPolicy>,
}

impl Snapshot {
    /// Read a vault into memory.
    ///
    /// # Errors
    /// When the vault cannot be read.
    pub async fn load(
        vault: &dyn Vault,
        policy: Arc<dyn HostPolicy>,
    ) -> Result<Self, WorkflowError> {
        let records = vault
            .load()
            .await?
            .into_iter()
            .map(|record| (record.id.clone(), record))
            .collect();
        Ok(Self {
            records: Arc::new(Mutex::new(records)),
            dirty: Arc::new(Mutex::new(BTreeMap::new())),
            policy,
        })
    }

    /// An empty snapshot, for a caller with nothing stored yet.
    #[must_use]
    pub fn empty(policy: Arc<dyn HostPolicy>) -> Self {
        Self {
            records: Arc::new(Mutex::new(BTreeMap::new())),
            dirty: Arc::new(Mutex::new(BTreeMap::new())),
            policy,
        }
    }

    /// Push everything written since the load back to the vault.
    ///
    /// Only what changed: a workflow the loop read and did not touch is not
    /// rewritten, so this cannot undo an edit someone else made in the
    /// meantime.
    ///
    /// Clears the dirty set on success, so flushing twice is not two writes.
    ///
    /// # Errors
    /// On the first write the vault refuses. Earlier writes stand — this is not
    /// a transaction, and pretending otherwise across three backends with
    /// different guarantees would be a lie.
    pub async fn flush(&self, vault: &dyn Vault) -> Result<usize, WorkflowError> {
        let pending: Vec<(String, Option<WorkflowRecord>)> = {
            let dirty = self.guard_dirty();
            dirty.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let mut written = 0;
        for (id, record) in &pending {
            match record {
                Some(record) => vault.put(record).await?,
                None => vault.remove(id).await?,
            }
            written += 1;
        }
        // Remove only what was flushed, and only if it has not changed since
        // the snapshot of `pending` was taken. Clearing the whole map would
        // drop a save that landed *during* the awaits above — the record would
        // exist only in memory and be gone after a restart, silently.
        let mut dirty = self.guard_dirty();
        for (id, record) in &pending {
            if dirty.get(id) == Some(record) {
                dirty.remove(id);
            }
        }
        Ok(written)
    }

    /// How many writes are waiting. Zero after a `flush`.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.guard_dirty().len()
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, WorkflowRecord>> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn guard_dirty(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Option<WorkflowRecord>>> {
        self.dirty
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl WorkflowStore for Snapshot {
    fn policy(&self) -> &dyn HostPolicy {
        self.policy.as_ref()
    }

    fn list(&self) -> Result<Vec<WorkflowSummary>, WorkflowError> {
        Ok(self
            .guard()
            .values()
            .map(|record| WorkflowSummary {
                id: record.id.clone(),
                name: record.name.clone(),
                description: record.description.clone(),
                enabled: record.enabled,
                node_count: record.graph.nodes.len(),
                inputs: record.graph.inputs.clone(),
                // What the summary carries instead of a path: the one node kind
                // a lister filters on.
                trigger_kind: record
                    .graph
                    .nodes
                    .iter()
                    .find(|n| n.kind == tinyflows::model::NodeKind::Trigger)
                    .and_then(|n| n.config.get("trigger_kind"))
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string),
            })
            .collect())
    }

    fn get(&self, id: &str) -> Result<Option<WorkflowRecord>, WorkflowError> {
        Ok(self.guard().get(id).cloned())
    }

    fn save(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        self.guard().insert(record.id.clone(), record.clone());
        self.guard_dirty()
            .insert(record.id.clone(), Some(record.clone()));
        Ok(())
    }

    fn delete(&self, id: &str) -> Result<(), WorkflowError> {
        self.guard().remove(id);
        self.guard_dirty().insert(id.to_string(), None);
        Ok(())
    }

    // The engine's authoring surface. This crate does not use it, and a
    // snapshot that pretended to would give a caller a run history that
    // vanishes on the next load rather than an honest refusal.
    fn record_run(&self, _run: &RunRecord) -> Result<(), WorkflowError> {
        Err(unsupported("run records"))
    }

    fn get_run(&self, _run_id: &str) -> Result<Option<RunRecord>, WorkflowError> {
        Ok(None)
    }

    fn list_runs(&self, _workflow_id: &str) -> Result<Vec<RunRecord>, WorkflowError> {
        Ok(Vec::new())
    }

    fn list_revisions(&self, _workflow_id: &str) -> Result<Vec<WorkflowRevision>, WorkflowError> {
        Ok(Vec::new())
    }

    fn revision(
        &self,
        _workflow_id: &str,
        _revision_id: &str,
    ) -> Result<Option<WorkflowRevision>, WorkflowError> {
        Ok(None)
    }
}

/// A refusal that names what is missing rather than panicking.
fn unsupported(what: &str) -> WorkflowError {
    WorkflowError::Engine(format!(
        "this store keeps workflows only; {what} are the engine's authoring \
         surface and a snapshot does not carry them"
    ))
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
