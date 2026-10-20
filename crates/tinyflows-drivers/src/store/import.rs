//! Id-preserving, write-if-absent import of records from an older store.
//!
//! The hosts that kept workflows as JSON files (the retired
//! `FileWorkflowStore`) move onto documents by reading those files back as the
//! `tinyflows::store` model and handing them to the methods below. Each one
//! stores a record under the id it already has, in the document shape the
//! store's own writers produce, and never replaces one the store already holds:
//! an import can be repeated (after a crash part-way), or run against a scope
//! that has since been written to, without clobbering anything. The return
//! value says whether this call wrote the record.
//!
//! Hosts own any file discovery and source-format decoding.

use serde_json::Value;
use tinyflows::store::types::{
    RunRecord, WorkflowError, WorkflowNote, WorkflowProposal, WorkflowRecord, WorkflowRevision,
};
use tinyflows::store::{prune_notes, validate_graph};
use tinyflows_storage::{DocumentStore, Precondition};

use super::definitions::{is_race, to_doc as definition_doc};
use super::history::to_doc as revision_doc;
use super::proposals::to_doc as proposal_doc;
use super::runs::to_doc as run_doc;
use super::{
    DEFINITIONS, DriverWorkflowStore, PROPOSALS, REVISIONS, RUNS, doc_id, put_doc, storage,
};

/// Writes `doc` at `collection/id` only if absent; `Ok(false)` when taken.
async fn put_if_absent(
    docs: &dyn DocumentStore,
    collection: &str,
    id: &str,
    doc: Value,
) -> Result<bool, WorkflowError> {
    match put_doc(docs, collection, id, doc, Precondition::Absent).await {
        Ok(_) => Ok(true),
        // An absent-precondition put that finds the id taken reports a
        // conflict on some backends and "already exists" on others.
        Err(error) if is_race(error.kind()) => Ok(false),
        Err(error) => Err(storage(error)),
    }
}

impl DriverWorkflowStore {
    /// Imports a workflow under its own id. Refused, like a save, when its
    /// graph would not compile.
    ///
    /// # Errors
    ///
    /// When the id is unusable, the graph is invalid, or the backend fails.
    pub async fn import_workflow(&self, record: &WorkflowRecord) -> Result<bool, WorkflowError> {
        self.inner.ensure().await?;
        let id = doc_id(&record.id)?;
        validate_graph(&record.id, &record.graph)?;
        put_if_absent(
            self.inner.docs.as_ref(),
            DEFINITIONS,
            &id,
            definition_doc(record)?,
        )
        .await
    }

    /// Imports a superseded copy under its own revision id, then prunes the
    /// workflow's history to the cap.
    ///
    /// # Errors
    ///
    /// When the backend fails.
    pub async fn import_revision(
        &self,
        revision: &WorkflowRevision,
    ) -> Result<bool, WorkflowError> {
        self.inner.ensure().await?;
        let doc = revision_doc(&revision.record, revision.superseded_at)?;
        let wrote = put_if_absent(self.inner.docs.as_ref(), REVISIONS, &revision.id, doc).await?;
        if wrote {
            self.inner
                .commit_capture(&revision.record.id, &revision.id)
                .await;
        }
        Ok(wrote)
    }

    /// Imports a run record under its own id.
    ///
    /// # Errors
    ///
    /// When the id is unusable or the backend fails.
    pub async fn import_run(&self, run: &RunRecord) -> Result<bool, WorkflowError> {
        self.inner.ensure().await?;
        put_if_absent(
            self.inner.docs.as_ref(),
            RUNS,
            &doc_id(&run.id)?,
            run_doc(run)?,
        )
        .await
    }

    /// Imports a proposal under its own id.
    ///
    /// # Errors
    ///
    /// When the id is unusable or the backend fails.
    pub async fn import_proposal(
        &self,
        proposal: &WorkflowProposal,
    ) -> Result<bool, WorkflowError> {
        self.inner.ensure().await?;
        put_if_absent(
            self.inner.docs.as_ref(),
            PROPOSALS,
            &doc_id(&proposal.id)?,
            proposal_doc(proposal)?,
        )
        .await
    }

    /// Imports `notes` into `workflow_id`'s journal: notes whose id the journal
    /// does not already hold are added (the ones it holds are kept as they
    /// are), then the journal is pruned to the cap. Returns how many were
    /// added.
    ///
    /// # Errors
    ///
    /// When the id is unusable or the backend fails.
    pub async fn import_notes(
        &self,
        workflow_id: &str,
        notes: &[WorkflowNote],
    ) -> Result<usize, WorkflowError> {
        let added = std::sync::atomic::AtomicUsize::new(0);
        self.inner
            .update_journal(workflow_id, |journal| {
                let mut count = 0;
                for note in notes {
                    if note.workflow_id == workflow_id && !journal.iter().any(|n| n.id == note.id) {
                        journal.push(note.clone());
                        count += 1;
                    }
                }
                added.store(count, std::sync::atomic::Ordering::SeqCst);
                if count > 0 {
                    prune_notes(journal);
                }
                count > 0
            })
            .await?;
        Ok(added.load(std::sync::atomic::Ordering::SeqCst))
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;
