//! `flows.workflow_proposals`: pending graph changes, one document each, always holding a
//! proposal's current state rather than a log to replay.

use serde_json::Value;
use tinyflows::store::types::{WorkflowError, WorkflowProposal, fingerprint};
use tinyflows_storage::{
    Capability, DocumentStoreExt, ExpectedDocument, Filter, Precondition, Query, ReadCheck, Sort,
    Versioned, WriteOp,
};

use super::codec::{proposal_body, proposal_from_body};
use super::definitions::{decode, is_race};
use super::{DEFINITIONS, Inner, PROPOSALS, doc_id, put_doc, storage};

pub(super) fn to_doc(proposal: &WorkflowProposal) -> Result<Value, WorkflowError> {
    proposal_body(proposal)
}

fn from_doc(stored: &Versioned<Value>) -> Result<WorkflowProposal, String> {
    proposal_from_body(&stored.doc)
}

impl Inner {
    /// Writes a proposal, replacing any earlier state for the same id.
    pub(super) async fn save_proposal(
        &self,
        proposal: &WorkflowProposal,
    ) -> Result<(), WorkflowError> {
        self.ensure().await?;
        put_doc(
            self.docs.as_ref(),
            PROPOSALS,
            &doc_id(&proposal.id)?,
            to_doc(proposal)?,
            Precondition::None,
        )
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// Saves a proposal only while its workflow still has
    /// `expected_fingerprint`. On a backend that validates read dependencies
    /// inside an atomic batch the definition is checked at the version read in
    /// the same batch as the proposal write (without rewriting it), so an edit
    /// landing in between refuses the proposal; without that the check and the
    /// write are separate (the weaker guarantee a lightweight store gives).
    pub(super) async fn save_proposal_if_fingerprint(
        &self,
        proposal: &WorkflowProposal,
        expected: &str,
    ) -> Result<bool, WorkflowError> {
        self.ensure().await?;
        let key = doc_id(&proposal.workflow_id)?;
        let write = PROPOSALS;
        let proposal_id = doc_id(&proposal.id)?;
        let transactional = self
            .docs
            .capabilities()
            .contains(Capability::ValidatedBatch);
        for _ in 0..super::CAS_ATTEMPTS {
            let Some(stored) = self.docs.get(DEFINITIONS, &key).await.map_err(storage)? else {
                return Ok(false);
            };
            let Ok(current) = decode(&stored.doc) else {
                return Ok(false);
            };
            if fingerprint(&current.graph) != expected {
                return Ok(false);
            }
            if !transactional {
                self.save_proposal(proposal).await?;
                return Ok(true);
            }
            // The definition is read-checked at the version seen, never
            // rewritten: a batched put declares no body version, so rewriting
            // it would reset its recorded schema.
            let checks = vec![ReadCheck {
                collection: DEFINITIONS.to_string(),
                id: key.clone(),
                expected: ExpectedDocument::Version(stored.version),
            }];
            let ops = vec![WriteOp::Put {
                collection: write.to_string(),
                id: proposal_id.clone(),
                doc: to_doc(proposal)?,
                precondition: Precondition::None,
            }];
            match self.docs.atomic_batch_checked(checks, ops).await {
                Ok(_) => return Ok(true),
                Err(error) if is_race(error.kind()) => {}
                Err(error) => return Err(storage(error)),
            }
        }
        Err(WorkflowError::Storage(format!(
            "workflow '{}' kept changing under {} attempts",
            proposal.workflow_id,
            super::CAS_ATTEMPTS
        )))
    }

    pub(super) async fn get_proposal(
        &self,
        id: &str,
    ) -> Result<Option<WorkflowProposal>, WorkflowError> {
        self.ensure().await?;
        let Some(stored) = self
            .docs
            .get(PROPOSALS, &doc_id(id)?)
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        from_doc(&stored)
            .map(Some)
            .map_err(|reason| WorkflowError::Malformed(format!("proposal {id}: {reason}")))
    }

    /// Every proposal for a workflow, newest first. One this build cannot parse
    /// is skipped, so a bad proposal does not hide the rest.
    pub(super) async fn list_proposals(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowProposal>, WorkflowError> {
        self.ensure().await?;
        let query = Query::filter(Filter::eq("workflow_id", workflow_id)).sort(Sort::desc("_id"));
        let stored = self
            .docs
            .query_all(PROPOSALS, &query)
            .await
            .map_err(storage)?;
        Ok(stored
            .iter()
            .filter_map(|stored| match from_doc(stored) {
                Ok(proposal) => Some(proposal),
                Err(reason) => {
                    tracing::warn!(target: "workflows", proposal = %stored.id, %reason, "[workflows] skipping an unreadable proposal");
                    None
                }
            })
            .collect())
    }
}
