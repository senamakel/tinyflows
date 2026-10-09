//! A run's steps, kept in the run document itself as one JSON string
//! (`steps_json`) — the same shape as SQLite's `flow_runs.steps_json` column.
//!
//! Keeping them on the run is what makes a step write and a run transition
//! one compare-and-swap: `finish_flow_run` settles the status and the step
//! list together, so a run is never terminal with a stale list, and an
//! incremental step write from a parallel branch either lands on the current
//! list or retries — nothing is lost and no write outlives a replacement.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tinyflows_catalog::FlowRunStep;

use super::{FlowCatalogDocuments, RUNS, compare_and_swap, is_conflict, text};

/// Attempts for a step upsert. Parallel branches of one run contend on the
/// run document, so this is higher than the default.
const STEP_ATTEMPTS: usize = 128;

/// The steps stored on run document `doc`.
pub(crate) fn steps_in(doc: &Value) -> serde_json::Result<Vec<FlowRunStep>> {
    serde_json::from_str(text(doc, "steps_json").unwrap_or("[]"))
}

/// `doc` with its step list replaced by `steps`.
pub(crate) fn with_steps(doc: &Value, steps: &[FlowRunStep]) -> Result<Value> {
    let mut next = doc.clone();
    next["steps_json"] =
        json!(serde_json::to_string(steps).context("Failed to serialize flow run steps")?);
    Ok(next)
}

impl FlowCatalogDocuments {
    /// Persists one step of a live run as it finishes, replacing an earlier
    /// step of the same node (a retry or a resumed run) in place. A no-op
    /// when the run has not been inserted yet.
    ///
    /// A compare-and-swap on the run: two parallel branches writing at once
    /// both land, the loser retrying on the winner's list — the lost update
    /// SQLite closed with `BEGIN IMMEDIATE` (R-m1).
    pub async fn upsert_flow_run_step(&self, run_id: &str, step: &FlowRunStep) -> Result<()> {
        let docs = self.docs().await?;
        for _ in 0..STEP_ATTEMPTS / super::CAS_ATTEMPTS {
            let attempt = compare_and_swap(docs, RUNS, run_id, |doc| {
                let mut steps = steps_in(doc).ok()?;
                match steps.iter_mut().find(|s| s.node_id == step.node_id) {
                    Some(slot) => *slot = step.clone(),
                    None => steps.push(step.clone()),
                }
                with_steps(doc, &steps).ok()
            })
            .await;
            match attempt {
                Ok(Some(stored)) => {
                    tracing::debug!(target: "flows", run_id, node = %step.node_id, step_count = steps_in(&stored.doc).map(|s| s.len()).unwrap_or(0), "[flows] persisted incremental flow run step");
                    return Ok(());
                }
                Ok(None) => {
                    let docs = self.docs().await?;
                    match docs.get(RUNS, run_id).await? {
                        None => {
                            tracing::debug!(target: "flows", run_id, node = %step.node_id, "[flows] upsert_flow_run_step: no run yet — skipping incremental step persist");
                            return Ok(());
                        }
                        Some(stored) => {
                            steps_in(&stored.doc)
                                .context("Failed to deserialize existing flow run steps")?;
                            anyhow::bail!("Failed to persist incremental flow run step");
                        }
                    }
                }
                Err(error) if is_conflict(&error) => {}
                Err(error) => {
                    return Err(error).context("Failed to persist incremental flow run step");
                }
            }
        }
        anyhow::bail!("run {run_id} kept changing under concurrent step writers")
    }
}

#[cfg(test)]
#[path = "steps_tests.rs"]
mod tests;
