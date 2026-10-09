//! `flows_run_steps`: one document per step of a run, so parallel branch
//! nodes of the same run persist their steps without contending.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::{Value, json};
use tinyflows_catalog::FlowRunStep;
use tinystoragedrivers_core::{
    DocumentStore, DocumentStoreExt, ErrorKind, Filter, Precondition, Query, Sort,
};

use super::{FlowCatalogDocuments, RUNS, STEPS, composite_id, required};

/// The step document id for `node_id` of `run_id`.
fn step_id(run_id: &str, node_id: &str) -> String {
    composite_id(&[run_id, node_id])
}

fn step_doc(run_id: &str, step: &FlowRunStep, order: i64) -> Result<Value> {
    Ok(json!({
        "run_id": run_id,
        "node_id": step.node_id,
        "order": order,
        "step_json": serde_json::to_string(step).context("Failed to serialize flow run step")?,
    }))
}

/// Reads the steps of every run in `run_ids`, each list in step order.
pub(crate) async fn steps_of(
    docs: &Arc<dyn DocumentStore>,
    run_ids: &[String],
) -> Result<HashMap<String, Vec<FlowRunStep>>> {
    let mut out: HashMap<String, Vec<FlowRunStep>> = HashMap::new();
    if run_ids.is_empty() {
        return Ok(out);
    }
    let query = Query::filter(Filter::one_of(
        "run_id",
        run_ids.iter().map(|id| json!(id)).collect::<Vec<_>>(),
    ))
    .sort(Sort::asc("run_id"))
    .sort(Sort::asc("order"))
    .sort(Sort::asc("_id"));
    for stored in docs.query_all(STEPS, &query).await? {
        let step: FlowRunStep = serde_json::from_str(required(&stored, "step_json")?)
            .with_context(|| format!("step {} is corrupt", stored.id))?;
        out.entry(required(&stored, "run_id")?.to_string())
            .or_default()
            .push(step);
    }
    Ok(out)
}

/// Replaces the steps of `run_id` with `steps`, in that order: the new list
/// is written before anything it no longer names is removed, so a reader
/// never sees the run emptied part-way.
pub(crate) async fn replace_steps(
    docs: &Arc<dyn DocumentStore>,
    run_id: &str,
    steps: &[FlowRunStep],
) -> Result<()> {
    let mut kept = HashSet::new();
    for (index, step) in steps.iter().enumerate() {
        let id = step_id(run_id, &step.node_id);
        let order = i64::try_from(index).unwrap_or(i64::MAX);
        docs.put(
            STEPS,
            &id,
            step_doc(run_id, step, order)?,
            Precondition::None,
        )
        .await?;
        kept.insert(id);
    }
    let existing = docs
        .query_all(STEPS, &Query::filter(Filter::eq("run_id", run_id)))
        .await?;
    for stale in existing.iter().filter(|stored| !kept.contains(&stored.id)) {
        docs.delete(STEPS, &stale.id, Precondition::None).await?;
    }
    Ok(())
}

impl FlowCatalogDocuments {
    /// Persists one step of a live run as it finishes, replacing an earlier
    /// step of the same node (a retry or a resumed run) in place. A no-op
    /// when the run has not been inserted yet.
    ///
    /// SQLite serialized a read-modify-write of the whole step list under
    /// `BEGIN IMMEDIATE` (R-m1). Here each node is its own document, so two
    /// nodes of one run never touch the same document; a node already present
    /// keeps its position, a new one is ordered after every earlier one.
    pub async fn upsert_flow_run_step(&self, run_id: &str, step: &FlowRunStep) -> Result<()> {
        let docs = self.docs().await?;
        if docs.get(RUNS, run_id).await?.is_none() {
            tracing::debug!(target: "flows", run_id, node = %step.node_id, "[flows] upsert_flow_run_step: no run yet — skipping incremental step persist");
            return Ok(());
        }
        let id = step_id(run_id, &step.node_id);
        for _ in 0..super::CAS_ATTEMPTS {
            let existing = docs.get(STEPS, &id).await?;
            let (order, precondition) = match &existing {
                Some(stored) => (
                    stored.doc.get("order").and_then(Value::as_i64).unwrap_or(0),
                    stored.unchanged(),
                ),
                None => (
                    Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX),
                    Precondition::Absent,
                ),
            };
            match docs
                .put(STEPS, &id, step_doc(run_id, step, order)?, precondition)
                .await
            {
                Ok(_) => {
                    tracing::debug!(target: "flows", run_id, node = %step.node_id, "[flows] persisted incremental flow run step");
                    return Ok(());
                }
                Err(error)
                    if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) => {}
                Err(error) => {
                    return Err(error).context("Failed to persist incremental flow run step");
                }
            }
        }
        anyhow::bail!("step {id} kept changing under concurrent writers")
    }
}

#[cfg(test)]
#[path = "steps_tests.rs"]
mod tests;
