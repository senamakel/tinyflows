//! Flow revision history: `update_flow_graph`'s guarded update with revision
//! capture, and reading revisions back.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::sync::Arc;
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::{Flow, FlowRevision};

use tinystoragedrivers_core::{
    DocumentStore, DocumentStoreExt, Filter, Precondition, Query, Sort, Versioned,
};
use uuid::Uuid;

use super::{
    DEFINITIONS, FlowCatalogDocuments, FlowUpdateError, MAX_REVISIONS_PER_FLOW, REVISIONS,
    best_effort, compare_and_swap, flag, instant_ns, next_stamp, required, text,
};

fn to_revision(stored: &Versioned<Value>) -> Result<FlowRevision> {
    // A revision's graph was written by a successful serialization, so a
    // decode failure means the document is corrupt: surface it rather than
    // read back a `null` graph that looks empty instead of broken.
    let graph: Value = serde_json::from_str(required(stored, "graph_json")?)
        .with_context(|| format!("revision {} graph_json is corrupt", stored.id))?;
    Ok(FlowRevision {
        id: stored.id.clone(),
        flow_id: required(stored, "flow_id")?.to_string(),
        graph,
        name: required(stored, "name")?.to_string(),
        require_approval: flag(&stored.doc, "require_approval"),
        created_at: required(stored, "created_at")?.to_string(),
    })
}

impl FlowCatalogDocuments {
    /// Replaces a flow's name, graph and `require_approval`, bumping
    /// `updated_at`, capturing the prior graph as a revision, and enforcing
    /// optimistic concurrency — the same contract, including the R-m2 disarm
    /// rules, as `tinyflows_sqlite::flows::update_flow_graph`.
    ///
    /// The revision is written first and the definition swapped second, keyed
    /// on the `updated_at` just read; when the swap loses to a concurrent
    /// writer the revision is removed again and the current flow comes back
    /// as [`FlowUpdateError::Conflict`].
    #[allow(clippy::too_many_arguments)]
    pub async fn update_flow_graph(
        &self,
        id: &str,
        name: String,
        graph: WorkflowGraph,
        require_approval: bool,
        enabled_override: Option<bool>,
        force_disarm_if_automatic: bool,
        expected_updated_at: Option<&str>,
    ) -> std::result::Result<Flow, FlowUpdateError> {
        let current = self
            .get_flow(id)
            .await
            .map_err(FlowUpdateError::Store)?
            .ok_or(FlowUpdateError::NotFound)?;
        if let Some(expected) = expected_updated_at
            && current.updated_at != expected
        {
            return Err(FlowUpdateError::Conflict(Box::new(current)));
        }

        // R-m2: whether it was automatic comes from the row just read, which
        // is the one the swap below is keyed on.
        let now_auto = tinyflows_catalog::graph_policy::trigger_is_automatic(&graph);
        let was_auto = tinyflows_catalog::graph_policy::trigger_is_automatic(&current.graph);
        let auto_disarm = now_auto && (!was_auto || force_disarm_if_automatic);
        if auto_disarm {
            tracing::debug!(
                target: "flows",
                flow_id = %id,
                was_auto,
                now_auto,
                "[flows] update_flow_graph: disarming — automatic-trigger transition detected (R-m2)"
            );
        }
        let new_enabled = if auto_disarm {
            false
        } else {
            enabled_override.unwrap_or(current.enabled)
        };

        let graph_json = serde_json::to_string(&graph)
            .context("Failed to serialize graph")
            .map_err(FlowUpdateError::Store)?;
        let prior_graph_json = serde_json::to_string(&current.graph)
            .context("Failed to serialize prior graph for revision capture")
            .map_err(FlowUpdateError::Store)?;
        // Strictly after the current stamp: `updated_at` is the concurrency
        // token and the revision order, so it must never repeat.
        let now = next_stamp(Some(&current.updated_at));
        let docs = self.docs().await.map_err(FlowUpdateError::Store)?;

        // A previous update that crashed after its swap left its revision
        // pending; the flow names it, so confirm it before moving past it.
        if let Ok(Some(stored)) = docs.get(DEFINITIONS, id).await
            && let Some(last) = text(&stored.doc, "last_revision_id")
        {
            best_effort("confirming the previous revision", confirm(docs, last)).await;
        }

        // Pending until the swap below names it: a lost race or a crash in
        // between never shows a revision for an update that did not happen.
        let revision_id = Uuid::new_v4().to_string();
        docs.put(
            REVISIONS,
            &revision_id,
            json!({
                "flow_id": id,
                "graph_json": prior_graph_json,
                "name": current.name,
                "require_approval": current.require_approval,
                "created_at": now,
                "created_ns": instant_ns(&now),
                "pending": true,
            }),
            Precondition::Absent,
        )
        .await
        .context("Failed to record flow revision")
        .map_err(FlowUpdateError::Store)?;

        let observed = current.updated_at.clone();
        let swapped = compare_and_swap(docs, DEFINITIONS, id, |doc| {
            if text(doc, "updated_at") != Some(observed.as_str()) {
                return None;
            }
            let mut next = doc.clone();
            next["name"] = json!(name);
            next["graph_json"] = json!(graph_json);
            next["updated_at"] = json!(now);
            next["require_approval"] = json!(require_approval);
            next["enabled"] = json!(new_enabled);
            next["last_revision_id"] = json!(revision_id);
            Some(next)
        })
        .await;
        let swapped = match swapped {
            Ok(swapped) => swapped,
            Err(error) => {
                best_effort("dropping an unused revision", async {
                    docs.delete(REVISIONS, &revision_id, Precondition::None)
                        .await
                        .map_err(Into::into)
                })
                .await;
                return Err(FlowUpdateError::Store(
                    error.context("Failed to update flow"),
                ));
            }
        };
        if swapped.is_none() {
            best_effort("dropping an unused revision", async {
                docs.delete(REVISIONS, &revision_id, Precondition::None)
                    .await
                    .map_err(Into::into)
            })
            .await;
            // Someone raced us between the read and the write.
            return match self.get_flow(id).await {
                Ok(Some(flow)) => Err(FlowUpdateError::Conflict(Box::new(flow))),
                Ok(None) => Err(FlowUpdateError::NotFound),
                Err(error) => Err(FlowUpdateError::Store(error)),
            };
        }

        best_effort("confirming the revision", confirm(docs, &revision_id)).await;
        best_effort("pruning flow revisions", self.prune_revisions(id)).await;
        self.get_flow(id)
            .await
            .map_err(FlowUpdateError::Store)?
            .ok_or(FlowUpdateError::NotFound)
    }

    /// Keeps the newest [`MAX_REVISIONS_PER_FLOW`] visible revisions of
    /// `flow_id`, and drops pending ones abandoned for over an hour (a
    /// younger one may still belong to an update in flight).
    async fn prune_revisions(&self, flow_id: &str) -> Result<()> {
        let docs = self.docs().await?;
        let (visible, abandoned) = self.partition(docs, flow_id).await?;
        let cutoff = instant_ns(&next_stamp(None)) - ABANDONED_AFTER_NS;
        for old in visible.iter().skip(MAX_REVISIONS_PER_FLOW) {
            docs.delete(REVISIONS, &old.id, Precondition::None).await?;
        }
        for orphan in abandoned.iter().filter(|stored| {
            stored
                .doc
                .get("created_ns")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                < cutoff
        }) {
            docs.delete(REVISIONS, &orphan.id, Precondition::None)
                .await?;
        }
        Ok(())
    }

    /// `flow_id`'s revisions newest first, split into the visible ones
    /// (confirmed, or named by the flow as its latest) and pending ones no
    /// committed update names.
    async fn partition(
        &self,
        docs: &Arc<dyn DocumentStore>,
        flow_id: &str,
    ) -> Result<(Vec<Versioned<Value>>, Vec<Versioned<Value>>)> {
        let latest = docs
            .get(DEFINITIONS, flow_id)
            .await?
            .and_then(|stored| text(&stored.doc, "last_revision_id").map(str::to_string));
        Ok(docs
            .query_all(REVISIONS, &newest_first(flow_id))
            .await?
            .into_iter()
            .partition(|stored| visible(stored, latest.as_deref())))
    }

    /// A flow's revisions, newest first, up to `limit`.
    pub async fn list_revisions(&self, flow_id: &str, limit: usize) -> Result<Vec<FlowRevision>> {
        let docs = self.docs().await?;
        let (visible, _) = self.partition(docs, flow_id).await?;
        visible.iter().take(limit).map(to_revision).collect()
    }

    /// One revision of `flow_id` by id, or `None`.
    pub async fn revision_by_id(
        &self,
        flow_id: &str,
        revision_id: &str,
    ) -> Result<Option<FlowRevision>> {
        let docs = self.docs().await?;
        let Some(stored) = docs.get(REVISIONS, revision_id).await? else {
            return Ok(None);
        };
        if text(&stored.doc, "flow_id") != Some(flow_id) {
            return Ok(None);
        }
        let latest = docs
            .get(DEFINITIONS, flow_id)
            .await?
            .and_then(|flow| text(&flow.doc, "last_revision_id").map(str::to_string));
        if !visible(&stored, latest.as_deref()) {
            return Ok(None);
        }
        to_revision(&stored).map(Some)
    }
}

/// How long a pending revision may wait for its update before pruning
/// treats it as abandoned.
const ABANDONED_AFTER_NS: i64 = 3_600 * 1_000_000_000;

/// Whether a revision is visible: confirmed, or the one the flow names.
fn visible(stored: &Versioned<Value>, latest: Option<&str>) -> bool {
    !flag(&stored.doc, "pending") || latest == Some(stored.id.as_str())
}

/// Marks revision `id` confirmed.
async fn confirm(docs: &Arc<dyn DocumentStore>, id: &str) -> Result<()> {
    compare_and_swap(docs, REVISIONS, id, |doc| {
        let mut next = doc.clone();
        next.as_object_mut()?.remove("pending")?;
        Some(next)
    })
    .await
    .map(|_| ())
}

fn newest_first(flow_id: &str) -> Query {
    Query::filter(Filter::eq("flow_id", flow_id))
        .sort(Sort::desc("created_ns"))
        .sort(Sort::desc("_id"))
}

#[cfg(test)]
#[path = "revisions_tests.rs"]
mod tests;
