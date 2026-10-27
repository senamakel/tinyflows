//! `flows.workflow_runs`: run records, one document each.

use serde_json::Value;
use tinyflows::store::types::{RunRecord, WorkflowError};
use tinyflows_storage::{DocumentStoreExt, Filter, Precondition, Query, Sort, Versioned};

use super::codec::{run_body, run_from_body};
use super::{Inner, RUNS, doc_id, put_doc, storage};

fn decode(stored: &Versioned<Value>) -> Result<RunRecord, String> {
    run_from_body(&stored.doc)
}

/// The run as its document keeps it.
pub(super) fn to_doc(run: &RunRecord) -> Result<Value, WorkflowError> {
    run_body(run)
}

/// Newest first, ties to the later id.
fn newest_first(filter: Filter) -> Query {
    Query::filter(filter)
        .sort(Sort::desc("started_ms"))
        .sort(Sort::desc("_id"))
}

impl Inner {
    /// Writes a run record, replacing any earlier state for the same run id.
    pub(super) async fn record_run(&self, run: &RunRecord) -> Result<(), WorkflowError> {
        self.ensure().await?;
        let doc = to_doc(run)?;
        put_doc(
            self.docs.as_ref(),
            RUNS,
            &doc_id(&run.id)?,
            doc,
            Precondition::None,
        )
        .await
        .map_err(storage)?;
        Ok(())
    }

    pub(super) async fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, WorkflowError> {
        self.ensure().await?;
        let Some(stored) = self
            .docs
            .get(RUNS, &doc_id(run_id)?)
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        decode(&stored)
            .map(Some)
            .map_err(|reason| WorkflowError::Malformed(format!("run {run_id}: {reason}")))
    }

    /// Every parsable record: history is diagnostic, and one corrupt document
    /// should not hide the rest of it.
    fn decode_all(stored: Vec<Versioned<Value>>) -> Vec<RunRecord> {
        stored
            .iter()
            .filter_map(|stored| match decode(stored) {
                Ok(run) => Some(run),
                Err(reason) => {
                    tracing::warn!(target: "workflows", run = %stored.id, %reason, "[workflows] skipping an unreadable run record");
                    None
                }
            })
            .collect()
    }

    pub(super) async fn list_runs(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<RunRecord>, WorkflowError> {
        self.ensure().await?;
        let stored = self
            .docs
            .query_all(RUNS, &newest_first(Filter::eq("workflow_id", workflow_id)))
            .await
            .map_err(storage)?;
        Ok(Self::decode_all(stored))
    }

    pub(super) async fn unsettled_runs(&self) -> Result<Vec<RunRecord>, WorkflowError> {
        self.ensure().await?;
        let stored = self
            .docs
            .query_all(RUNS, &newest_first(Filter::eq("settled", false)))
            .await
            .map_err(storage)?;
        Ok(Self::decode_all(stored))
    }
}
