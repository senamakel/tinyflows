//! `flows_definitions`: create, read, list, enable/disable, delete,
//! duplicate, and last-run bookkeeping (`record_run`).

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use serde_json::{Value, json};
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::Flow;
use tinystoragedrivers_core::{DocumentStoreExt, Filter, Precondition, Query, Sort, Versioned};
use uuid::Uuid;

use super::{
    DEFINITIONS, FlowCatalogDocuments, REVISIONS, RUNS, STEPS, compare_and_swap, flag, instant_ns,
    required, set_optional, text, upsert,
};

/// `flow` as a document, keeping `created_at` from `existing` when there is
/// one — SQLite's upsert leaves `created_at` alone on conflict.
fn to_doc(flow: &Flow, existing: Option<&Value>) -> Result<Value> {
    let created_at = existing
        .and_then(|doc| text(doc, "created_at"))
        .unwrap_or(&flow.created_at)
        .to_string();
    let mut doc = json!({
        "name": flow.name,
        "graph_json": serde_json::to_string(&flow.graph).context("Failed to serialize graph")?,
        "enabled": flow.enabled,
        "created_ns": instant_ns(&created_at),
        "created_at": created_at,
        "updated_at": flow.updated_at,
        "require_approval": flow.require_approval,
    });
    set_optional(&mut doc, "last_run_at", flow.last_run_at.as_deref());
    set_optional(&mut doc, "last_status", flow.last_status.as_deref());
    Ok(doc)
}

/// The stored graph, migrated from an older `schema_version` on read.
pub(crate) fn decode_graph(raw: &str) -> Result<WorkflowGraph> {
    let value: Value = serde_json::from_str(raw).context("graph_json is not JSON")?;
    let migrated = tinyflows::migrate::migrate(value).map_err(|e| anyhow!("{e}"))?;
    serde_json::from_value(migrated).context("graph_json does not decode")
}

/// A stored definition as a [`Flow`].
pub(crate) fn to_flow(stored: &Versioned<Value>) -> Result<Flow> {
    let doc = &stored.doc;
    Ok(Flow {
        id: stored.id.clone(),
        name: required(stored, "name")?.to_string(),
        graph: decode_graph(required(stored, "graph_json")?)?,
        enabled: flag(doc, "enabled"),
        created_at: required(stored, "created_at")?.to_string(),
        updated_at: required(stored, "updated_at")?.to_string(),
        last_run_at: text(doc, "last_run_at").map(str::to_string),
        last_status: text(doc, "last_status").map(str::to_string),
        require_approval: flag(doc, "require_approval"),
    })
}

impl FlowCatalogDocuments {
    /// Inserts or fully replaces a flow definition (`created_at` of an
    /// existing one is kept).
    pub async fn upsert_flow(&self, flow: &Flow) -> Result<()> {
        let docs = self.docs().await?;
        upsert(docs, DEFINITIONS, &flow.id, |existing| {
            to_doc(flow, existing)
        })
        .await
        .context("Failed to upsert flow definition")?;
        tracing::debug!(flow_id = %flow.id, "[flows] upserted flow definition");
        Ok(())
    }

    /// Duplicates `source` into a fresh, **disabled** flow named `new_name`,
    /// with no run history. See `tinyflows_sqlite::flows::insert_duplicate_flow`.
    pub async fn insert_duplicate_flow(&self, source: &Flow, new_name: String) -> Result<Flow> {
        let now = Utc::now().to_rfc3339();
        let flow = Flow {
            id: Uuid::new_v4().to_string(),
            name: new_name,
            enabled: false,
            graph: source.graph.clone(),
            created_at: now.clone(),
            updated_at: now,
            last_run_at: None,
            last_status: None,
            require_approval: source.require_approval,
        };
        self.upsert_flow(&flow).await?;
        tracing::debug!(target: "flows", source_id = %source.id, new_id = %flow.id, "[flows] inserted duplicate flow (disabled)");
        Ok(flow)
    }

    /// Creates a flow from a name and a validated graph, stamping a fresh id
    /// and timestamps.
    pub async fn create_flow(
        &self,
        name: String,
        graph: WorkflowGraph,
        require_approval: bool,
        enabled: bool,
    ) -> Result<Flow> {
        let now = Utc::now().to_rfc3339();
        let flow = Flow {
            id: Uuid::new_v4().to_string(),
            name,
            enabled,
            graph,
            created_at: now.clone(),
            updated_at: now,
            last_run_at: None,
            last_status: None,
            require_approval,
        };
        self.upsert_flow(&flow).await?;
        Ok(flow)
    }

    /// Loads one flow, migrating its graph on read. A corrupt document is an
    /// error, as a corrupt row is in SQLite.
    pub async fn get_flow(&self, id: &str) -> Result<Option<Flow>> {
        let docs = self.docs().await?;
        docs.get(DEFINITIONS, id)
            .await?
            .as_ref()
            .map(to_flow)
            .transpose()
    }

    /// All flows, oldest first, and the number skipped as undecodable
    /// (logged, never fatal — see `tinyflows_sqlite::flows::list_flows`).
    pub async fn list_flows(&self) -> Result<(Vec<Flow>, usize)> {
        self.list_where(Filter::All).await
    }

    /// Only enabled flows; see [`Self::list_flows`].
    pub async fn list_enabled_flows(&self) -> Result<(Vec<Flow>, usize)> {
        self.list_where(Filter::eq("enabled", true)).await
    }

    async fn list_where(&self, filter: Filter) -> Result<(Vec<Flow>, usize)> {
        let docs = self.docs().await?;
        let query = Query::filter(filter)
            .sort(Sort::asc("created_ns"))
            .sort(Sort::asc("_id"));
        let mut flows = Vec::new();
        let mut skipped = 0usize;
        for stored in docs.query_all(DEFINITIONS, &query).await? {
            match to_flow(&stored) {
                Ok(flow) => flows.push(flow),
                Err(error) => {
                    skipped += 1;
                    tracing::warn!(
                        target: "flows",
                        flow_id = %stored.id,
                        %error,
                        "[flows] skipping corrupt or unmigratable flow definition"
                    );
                }
            }
        }
        Ok((flows, skipped))
    }

    /// Deletes a flow and its revisions, runs and steps.
    ///
    /// # Errors
    ///
    /// When no such flow exists.
    pub async fn remove_flow(&self, id: &str) -> Result<()> {
        let docs = self.docs().await?;
        if !docs.delete(DEFINITIONS, id, Precondition::None).await? {
            bail!("flow '{id}' not found");
        }
        let runs = docs
            .query_all(RUNS, &Query::filter(Filter::eq("flow_id", id)))
            .await?;
        let run_ids: Vec<Value> = runs.iter().map(|run| json!(run.id)).collect();
        if !run_ids.is_empty() {
            docs.delete_where(STEPS, &Filter::one_of("run_id", run_ids))
                .await?;
        }
        docs.delete_where(RUNS, &Filter::eq("flow_id", id)).await?;
        docs.delete_where(REVISIONS, &Filter::eq("flow_id", id))
            .await?;
        tracing::debug!(flow_id = %id, "[flows] removed flow definition");
        Ok(())
    }

    /// Toggles a flow's `enabled` flag, returning the updated flow.
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<Flow> {
        let now = Utc::now().to_rfc3339();
        let docs = self.docs().await?;
        let updated = compare_and_swap(docs, DEFINITIONS, id, |doc| {
            let mut next = doc.clone();
            next["enabled"] = json!(enabled);
            next["updated_at"] = json!(now);
            Some(next)
        })
        .await
        .context("Failed to update flow enabled state")?
        .ok_or_else(|| anyhow!("flow '{id}' not found"))?;
        tracing::debug!(flow_id = %id, enabled, "[flows] set_enabled");
        to_flow(&updated)
    }

    /// Records a run's outcome on the flow (`last_run_at` / `last_status`).
    pub async fn record_run(&self, id: &str, status: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        let docs = self.docs().await?;
        compare_and_swap(docs, DEFINITIONS, id, |doc| {
            let mut next = doc.clone();
            next["last_run_at"] = json!(now);
            next["last_status"] = json!(status);
            Some(next)
        })
        .await
        .context("Failed to record flow run")?
        .ok_or_else(|| anyhow!("flow '{id}' not found"))?;
        tracing::debug!(flow_id = %id, status, "[flows] recorded run");
        Ok(())
    }

    /// Test-only: overwrites a flow's stored graph with arbitrary text, to
    /// stage the corrupt-row case `list_flows` must survive.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn force_corrupt_graph_json_for_test(
        &self,
        flow_id: &str,
        raw_graph_json: &str,
    ) -> Result<()> {
        let docs = self.docs().await?;
        let raw = raw_graph_json.to_string();
        let changed = compare_and_swap(docs, DEFINITIONS, flow_id, |doc| {
            let mut next = doc.clone();
            next["graph_json"] = json!(raw);
            Some(next)
        })
        .await
        .context("Failed to force corrupt graph_json (test fixture)")?;
        anyhow::ensure!(
            changed.is_some(),
            "flow '{flow_id}' not found (test fixture)"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "definitions_tests.rs"]
mod tests;
