//! `flows.workflow_revisions`: superseded copies of a workflow, so an edit can be taken
//! back, capped at [`MAX_REVISIONS`] per workflow.

use serde_json::Value;
use tinyflows::store::types::{WorkflowError, WorkflowRecord, WorkflowRevision};
use tinyflows::store::{MAX_REVISIONS, mint_revision_id};
use tinyflows_storage::{DocumentStoreExt, Filter, Precondition, Query, Sort, Versioned};

use super::codec::{revision_body, revision_record};
use super::{Inner, REVISIONS, now_ms, put_doc, stamp, storage};

fn decode(stored: &Versioned<Value>) -> Result<WorkflowRevision, String> {
    let superseded_at = stored
        .doc
        .get("superseded_ms")
        .and_then(Value::as_i64)
        .and_then(|at| u64::try_from(at).ok())
        .unwrap_or_default();
    Ok(WorkflowRevision {
        id: stored.id.clone(),
        superseded_at,
        record: revision_record(&stored.doc)?,
    })
}

/// The revision document for `record`, superseded at `superseded_at` (epoch ms).
pub(super) fn to_doc(record: &WorkflowRecord, superseded_at: u64) -> Result<Value, WorkflowError> {
    revision_body(record, stamp(superseded_at))
}

fn newest_first(workflow_id: &str) -> Query {
    Query::filter(Filter::eq("workflow_id", workflow_id)).sort(Sort::desc("_id"))
}

impl Inner {
    /// Snapshots `record` as a superseded version, returning the revision's id.
    /// The caller is about to overwrite the only copy, so a history it could not
    /// record is a failure rather than something to log past.
    pub(super) async fn capture(&self, record: &WorkflowRecord) -> Result<String, WorkflowError> {
        self.ensure().await?;
        let superseded_at = now_ms();
        let id = mint_revision_id(superseded_at);
        let doc = to_doc(record, superseded_at)?;
        put_doc(
            self.docs.as_ref(),
            REVISIONS,
            &id,
            doc,
            Precondition::Absent,
        )
        .await
        .map_err(storage)?;
        Ok(id)
    }

    /// Keeps a captured snapshot after the mutation it belongs to landed, and
    /// prunes to the cap. Pruning is housekeeping: the write has already
    /// happened, so a failure here must not turn it into a reported failure.
    pub(super) async fn commit_capture(&self, workflow_id: &str, _revision: &str) {
        if let Err(error) = self.prune_revisions(workflow_id).await {
            tracing::warn!(target: "workflows", workflow_id, %error, "[workflows] failed to prune workflow revisions");
        }
    }

    /// Takes back a snapshot whose mutation did not happen.
    pub(super) async fn rollback_capture(&self, revision: Option<&str>) {
        let Some(revision) = revision else { return };
        if let Err(error) = self
            .docs
            .delete(REVISIONS, revision, Precondition::None)
            .await
        {
            tracing::warn!(target: "workflows", revision, %error, "[workflows] failed to take back a revision for a lost write");
        }
    }

    pub(super) async fn prune_revisions(&self, workflow_id: &str) -> Result<(), WorkflowError> {
        let all = self
            .docs
            .query_all(REVISIONS, &newest_first(workflow_id))
            .await
            .map_err(storage)?;
        for stale in all.iter().skip(MAX_REVISIONS) {
            self.docs
                .delete(REVISIONS, &stale.id, Precondition::None)
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    /// Every snapshot of `workflow_id`, newest first. One this build cannot
    /// parse is skipped rather than failing the listing.
    pub(super) async fn list_revisions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowRevision>, WorkflowError> {
        self.ensure().await?;
        let page = self
            .docs
            .query(REVISIONS, &newest_first(workflow_id).limit(MAX_REVISIONS))
            .await
            .map_err(storage)?;
        Ok(page
            .items
            .iter()
            .filter_map(|stored| match decode(stored) {
                Ok(revision) => Some(revision),
                Err(reason) => {
                    tracing::warn!(target: "workflows", revision = %stored.id, %reason, "[workflows] skipping an unreadable revision");
                    None
                }
            })
            .collect())
    }

    /// One snapshot by id, scoped to its workflow: a revision id alone must not
    /// let one workflow's rollback reach another's history.
    pub(super) async fn revision(
        &self,
        workflow_id: &str,
        revision_id: &str,
    ) -> Result<Option<WorkflowRevision>, WorkflowError> {
        self.ensure().await?;
        let Some(stored) = self
            .docs
            .get(REVISIONS, revision_id)
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        if stored.doc.get("workflow_id").and_then(Value::as_str) != Some(workflow_id) {
            return Ok(None);
        }
        decode(&stored)
            .map(Some)
            .map_err(|reason| WorkflowError::Malformed(format!("revision {revision_id}: {reason}")))
    }
}
