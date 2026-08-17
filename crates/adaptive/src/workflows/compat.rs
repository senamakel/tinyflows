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

/// Several catalogues to read, one to write.
///
/// Reads are the union, and **later layers shadow earlier ones** by id — so
/// order the writable layer last and a copy we have taken ownership of wins
/// over the original it came from.
///
/// Writes go only to the writable layer, which is the whole point. A variant of
/// somebody else's workflow is ours; their record is evidence, not something to
/// edit.
pub struct Layered {
    /// Consulted in order, each shadowing the last.
    read_only: Vec<Arc<dyn Vault>>,
    /// Read last, and the only one written to.
    writable: Arc<dyn Vault>,
}

impl Layered {
    /// Read `read_only` in order, then `writable`; write only `writable`.
    #[must_use]
    pub fn new(read_only: Vec<Arc<dyn Vault>>, writable: Arc<dyn Vault>) -> Self {
        Self {
            read_only,
            writable,
        }
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
        for layer in &self.read_only {
            for record in layer.load().await? {
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
mod tests {
    use super::*;
    use crate::workflows::conformance::record;
    use crate::workflows::memory::MemoryVault;

    async fn layer(ids: &[&str]) -> Arc<MemoryVault> {
        let vault = Arc::new(MemoryVault::new());
        for id in ids {
            vault.put(&record(id)).await.expect("put");
        }
        vault
    }

    #[tokio::test]
    async fn a_plain_store_becomes_selectable_without_being_migrated() {
        let dir = std::env::temp_dir().join(format!("adaptive-compat-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("workflows")).expect("temp dir");
        let store: Arc<dyn WorkflowStore> = Arc::new(tinyflows::store::FileWorkflowStore::new(
            vec![dir.join("workflows")],
            dir.join("runs"),
        ));
        store.save(&record("theirs")).expect("save");

        let vault = StoreVault::new(store);
        let loaded = vault.load().await.expect("load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].description, "does the theirs thing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reads_are_the_union_of_every_layer() {
        let theirs = layer(&["device-a", "device-b"]).await;
        let ours = layer(&["learned-1"]).await;
        let stack = Layered::new(vec![theirs], ours);

        let mut ids: Vec<String> = stack
            .load()
            .await
            .expect("load")
            .into_iter()
            .map(|r| r.id)
            .collect();
        ids.sort();
        assert_eq!(ids, ["device-a", "device-b", "learned-1"]);
    }

    #[tokio::test]
    async fn what_we_wrote_shadows_what_we_read() {
        let theirs = Arc::new(MemoryVault::new());
        let mut original = record("shared-id");
        original.description = "the device's version".into();
        theirs.put(&original).await.expect("put");

        let ours = Arc::new(MemoryVault::new());
        let mut taken = record("shared-id");
        taken.description = "the copy we took ownership of".into();
        ours.put(&taken).await.expect("put");

        let stack = Layered::new(vec![theirs], ours);
        let loaded = stack.load().await.expect("load");
        assert_eq!(loaded.len(), 1, "one id, one record");
        assert_eq!(loaded[0].description, "the copy we took ownership of");
    }

    #[tokio::test]
    async fn a_variant_of_their_workflow_lands_in_our_layer_not_theirs() {
        // The reason this is layered rather than merged. Their catalogue is
        // evidence; the repair is ours, and their machine never changes.
        let theirs = Arc::new(MemoryVault::new());
        theirs.put(&record("device-weekly")).await.expect("put");
        let ours = Arc::new(MemoryVault::new());

        let stack = Layered::new(vec![theirs.clone()], ours.clone());
        stack
            .put(&record("device-weekly-fix-a1b2c3d"))
            .await
            .expect("put");

        assert_eq!(
            theirs.load().await.expect("load").len(),
            1,
            "their catalogue is untouched"
        );
        assert_eq!(ours.load().await.expect("load").len(), 1, "ours gained it");
    }

    #[tokio::test]
    async fn a_delete_never_reaches_a_read_only_layer() {
        // Otherwise the loop could remove a workflow from a machine that never
        // asked it to.
        let theirs = Arc::new(MemoryVault::new());
        theirs.put(&record("device-weekly")).await.expect("put");
        let stack = Layered::new(vec![theirs.clone()], Arc::new(MemoryVault::new()));

        stack.remove("device-weekly").await.expect("remove");
        assert_eq!(
            theirs.load().await.expect("load").len(),
            1,
            "still theirs, still there"
        );
    }

    #[tokio::test]
    async fn the_scope_reported_is_the_one_writes_land_in() {
        // A device store has no tenant concept, so a read-only layer over it is
        // unscoped. Reporting that would understate who the handle belongs to.
        let unscoped_device = Arc::new(MemoryVault::new());
        let ours = Arc::new(MemoryVault::new().for_tenant("user-7"));
        let stack = Layered::new(vec![unscoped_device], ours);
        assert_eq!(stack.scope(), Some("user-7"));
    }
}
