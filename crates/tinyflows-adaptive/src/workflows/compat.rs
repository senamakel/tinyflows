//! Reaching workflows that already exist somewhere else.
//!
//! The loop's own procedures live in a [`Vault`]. Everyone else's live in a
//! [`WorkflowStore`] — the engine's file store, a host's own implementation, a
//! device's local catalogue. Those are the same records; only the way in
//! differs, and without a way in the loop can only ever select what it wrote
//! itself.
//!
//! Two adapters, and between them the loop reads any catalogue that exists.
//!
//! [`StoreVault`] makes any `WorkflowStore` a `Vault`, so nothing has to be
//! rewritten or migrated to be selectable.
//!
//! [`Layered`] reads several and writes one. That is the shape that solves the
//! problem importing otherwise creates: a device's catalogue is **read-only**,
//! so a workflow of theirs can be selected, judged and scored, and when it
//! falls short the repaired variant lands in *our* writable layer with its own
//! id. Their copy is never touched, so there is no second master and no
//! question of whose version is current.

use std::sync::Arc;

use async_trait::async_trait;
use tinyflows::store::WorkflowStore;
use tinyflows::store::types::{WorkflowError, WorkflowRecord};

use super::Vault;

/// Any [`WorkflowStore`] as a [`Vault`].
///
/// Unscoped, and it cannot be otherwise: the engine's store has no tenant
/// concept to filter on. So scoping here is **by construction** — build one
/// per tenant over that tenant's own store. An unscoped vault's records read as
/// global, which is right for a shared catalogue and wrong for a device's, so
/// this is worth getting right at the call site.
pub struct StoreVault {
    inner: Arc<dyn WorkflowStore>,
}

impl StoreVault {
    /// Wrap a store.
    #[must_use]
    pub fn new(inner: Arc<dyn WorkflowStore>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Vault for StoreVault {
    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        // `list` gives summaries, so each record is a second call. One pass per
        // episode over a catalogue of tens, against a store that is already
        // synchronous and therefore local.
        let mut out = Vec::new();
        for summary in self.inner.list()? {
            if let Some(record) = self.inner.get(&summary.id)? {
                out.push(record);
            }
        }
        Ok(out)
    }

    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        self.inner.save(record)
    }

    async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
        self.inner.delete(id)
    }
}

/// Told which read-only layer could not be read, and why.
pub type OnUnavailable = Arc<dyn Fn(&str, &WorkflowError) + Send + Sync>;

/// Several catalogues to read, one to write.
///
/// Reads are the union, and **later layers shadow earlier ones** by id — so
/// order the writable layer last and a copy we have taken ownership of wins
/// over the original it came from.
///
/// Writes go only to the writable layer, which is the whole point. A variant of
/// somebody else's workflow is ours; their record is evidence, not something to
/// edit.
///
/// # When a layer cannot be read
///
/// [`new`](Self::new) is **strict**: any failure fails the load, and therefore
/// the episode. That is right when every layer is a database you own.
///
/// It is wrong the moment a layer is a device. Fetching a device's catalogue
/// per episode is cheap and keeps it current, but a device is sometimes asleep,
/// and a machine being asleep must not stop a tenant's goals — their own
/// procedures are in another layer and perfectly readable.
///
/// [`degrading`](Self::degrading) skips a read-only layer that errors. It
/// **requires a handler**, and that is deliberate: a catalogue that quietly
/// vanishes is this crate's worst failure shape — the loop runs, authors from
/// scratch, and looks like it is working. You cannot have the degradation
/// without being told each time it happens.
///
/// The writable layer is fatal either way. It is your own store, and a loop
/// that cannot read its own procedures should stop rather than relearn them.
pub struct Layered {
    /// Consulted in order, each shadowing the last. Named so a report can say
    /// which one was missing.
    read_only: Vec<(String, Arc<dyn Vault>)>,
    /// Read last, and the only one written to.
    writable: Arc<dyn Vault>,
    /// Set by [`degrading`](Self::degrading). `None` means strict.
    on_unavailable: Option<OnUnavailable>,
}

impl Layered {
    /// Read `read_only` in order, then `writable`; write only `writable`.
    ///
    /// Strict: an unreadable layer fails the load.
    #[must_use]
    pub fn new(read_only: Vec<(String, Arc<dyn Vault>)>, writable: Arc<dyn Vault>) -> Self {
        Self {
            read_only,
            writable,
            on_unavailable: None,
        }
    }

    /// Skip a read-only layer that cannot be read, telling `on_unavailable`.
    ///
    /// For layers that are somebody else's machine. See the type note on why
    /// the handler is required rather than optional.
    #[must_use]
    pub fn degrading(mut self, on_unavailable: OnUnavailable) -> Self {
        self.on_unavailable = Some(on_unavailable);
        self
    }
}

#[async_trait]
impl Vault for Layered {
    fn scope(&self) -> Option<&str> {
        // The scope that matters is the one writes land in. A read-only layer
        // may be unscoped — a device store has no tenant concept — and
        // reporting *that* would understate who this handle belongs to.
        self.writable.scope()
    }

    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        let mut merged: std::collections::BTreeMap<String, WorkflowRecord> =
            std::collections::BTreeMap::new();
        for (name, layer) in &self.read_only {
            let records = match (layer.load().await, self.on_unavailable.as_ref()) {
                (Ok(records), _) => records,
                // Skipped, and reported. A device asleep is a catalogue we do
                // not have this episode, not a tenant who cannot run anything.
                (Err(why), Some(tell)) => {
                    tell(name, &why);
                    continue;
                }
                (Err(why), None) => return Err(why),
            };
            for record in records {
                merged.insert(record.id.clone(), record);
            }
        }
        // Last, so ours wins an id collision.
        for record in self.writable.load().await? {
            merged.insert(record.id.clone(), record);
        }
        Ok(merged.into_values().collect())
    }

    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        self.writable.put(record).await
    }

    async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
        // Only ever ours. Removing from a read-only layer would delete
        // something on a machine that never asked.
        self.writable.remove(id).await
    }
}

#[cfg(test)]
#[path = "compat_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "compat_degradation_tests.rs"]
mod degradation_tests;
