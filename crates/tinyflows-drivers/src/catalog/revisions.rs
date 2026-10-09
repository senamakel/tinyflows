//! Flow revision history: `update_flow_graph`'s guarded update with revision
//! capture, and reading revisions back.

use anyhow::{Context, Result};
use chrono::Utc;
use serde_json::{Value, json};
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::{Flow, FlowRevision};
use tinystoragedrivers_core::{DocumentStoreExt, Filter, Precondition, Query, Sort, Versioned};
use uuid::Uuid;

use super::{
    DEFINITIONS, FlowCatalogDocuments, FlowUpdateError, MAX_REVISIONS_PER_FLOW, REVISIONS,
    best_effort, compare_and_swap, flag, instant_ns, required, text,
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
        let auto_disarm = (now_auto && !was_auto) || (force_disarm_if_automatic && now_auto);
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
        let now = Utc::now().to_rfc3339();
        let docs = self.docs().await.map_err(FlowUpdateError::Store)?;

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
                return Err(FlowUpdateError::Store(error.context("Failed to update flow")));
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

        best_effort("pruning flow revisions", self.prune_revisions(id)).await;
        self.get_flow(id)
            .await
            .map_err(FlowUpdateError::Store)?
            .ok_or(FlowUpdateError::NotFound)
    }

    /// Keeps the newest [`MAX_REVISIONS_PER_FLOW`] revisions of `flow_id`.
    async fn prune_revisions(&self, flow_id: &str) -> Result<()> {
        let docs = self.docs().await?;
        let all = docs.query_all(REVISIONS, &newest_first(flow_id)).await?;
        for old in all.iter().skip(MAX_REVISIONS_PER_FLOW) {
            docs.delete(REVISIONS, &old.id, Precondition::None).await?;
        }
        Ok(())
    }

    /// A flow's revisions, newest first, up to `limit`.
    pub async fn list_revisions(&self, flow_id: &str, limit: usize) -> Result<Vec<FlowRevision>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let docs = self.docs().await?;
        let page = docs
            .query(REVISIONS, &newest_first(flow_id).limit(limit))
            .await?;
        page.items.iter().map(to_revision).collect()
    }

    /// One revision of `flow_id` by id, or `None`.
    pub async fn revision_by_id(
        &self,
        flow_id: &str,
        revision_id: &str,
    ) -> Result<Option<FlowRevision>> {
        let docs = self.docs().await?;
        match docs.get(REVISIONS, revision_id).await? {
            Some(stored) if text(&stored.doc, "flow_id") == Some(flow_id) => {
                to_revision(&stored).map(Some)
            }
            _ => Ok(None),
        }
    }
}

fn newest_first(flow_id: &str) -> Query {
    Query::filter(Filter::eq("flow_id", flow_id))
        .sort(Sort::desc("created_ns"))
        .sort(Sort::desc("_id"))
}

#[cfg(test)]
#[path = "revisions_tests.rs"]
mod tests;
