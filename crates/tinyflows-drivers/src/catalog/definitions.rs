//! `flows_definitions`: create, read, list, enable/disable, delete,
//! duplicate, and last-run bookkeeping (`record_run`).

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use serde_json::{Value, json};
use tinyflows::model::WorkflowGraph;
use tinyflows_catalog::Flow;
use tinyflows_storage::{
    DocumentStoreExt, ErrorKind, Filter, Precondition, Query, Sort, Versioned,
};
use uuid::Uuid;

use super::lineage::{INCARNATION, belongs, incarnation};
use super::{
    DEFINITIONS, FlowCatalogDocuments, REVISIONS, RUNS, compare_and_swap, flag, instant_ms, int,
    monotonic_ms, new_id, next_stamp, required, rfc3339, set_optional, text, upsert,
};

/// `flow` as a document, keeping `created_ms` from `existing` when there is
/// one — SQLite's upsert leaves `created_at` alone on conflict.
pub(super) fn to_doc(flow: &Flow, existing: Option<&Value>) -> Result<Value> {
    let created_ms = existing
        .and_then(|doc| doc.get("created_ms").and_then(Value::as_i64))
        .unwrap_or_else(|| instant_ms(&flow.created_at));
    let mut doc = json!({
        "name": flow.name,
        "graph": serde_json::to_value(&flow.graph).context("Failed to serialize graph")?,
        "enabled": flow.enabled,
        "created_ms": created_ms,
        "updated_ms": instant_ms(&flow.updated_at),
        "require_approval": flow.require_approval,
    });
    // A definition starts at revision 1. Existing counters belong to the
    // store, not the caller; retain a legacy missing counter until the next
    // successful graph update can bootstrap it from visible history.
    if let Some(revision) = existing
        .and_then(|doc| doc.get("revision"))
        .and_then(Value::as_u64)
    {
        doc["revision"] = json!(revision);
    } else if existing.is_none() {
        doc["revision"] = json!(1);
    }
    // The last captured revision id is likewise store bookkeeping.
    set_optional(
        &mut doc,
        "last_revision_id",
        existing.and_then(|doc| text(doc, "last_revision_id")),
    );
    // Minted once, when the document is first created, and kept by every
    // later upsert: what ties runs and revisions to this flow and not to a
    // removed one with the same id (`lineage`).
    let incarnation = match existing {
        Some(existing) => incarnation(existing).map(str::to_string),
        None => Some(Uuid::new_v4().to_string()),
    };
    set_optional(&mut doc, INCARNATION, incarnation.as_deref());
    if let Some(at) = flow.last_run_at.as_deref() {
        doc["last_run_ms"] = json!(instant_ms(at));
    }
    set_optional(&mut doc, "last_status", flow.last_status.as_deref());
    Ok(doc)
}

/// The stored graph, migrated from an older `schema_version` on read.
pub(crate) fn decode_graph(value: &Value) -> Result<WorkflowGraph> {
    if !value.is_object() {
        bail!("graph is not an object");
    }
    let migrated = tinyflows::migrate::migrate(value.clone()).map_err(|e| anyhow!("{e}"))?;
    serde_json::from_value(migrated).context("graph does not decode")
}

/// A stored definition as a [`Flow`].
pub(crate) fn to_flow(stored: &Versioned<Value>) -> Result<Flow> {
    let doc = &stored.doc;
    Ok(Flow {
        id: stored.id.clone(),
        name: required(stored, "name")?.to_string(),
        graph: decode_graph(doc.get("graph").unwrap_or(&Value::Null))?,
        enabled: flag(doc, "enabled"),
        created_at: rfc3339(int(doc, "created_ms")),
        updated_at: rfc3339(int(doc, "updated_ms")),
        last_run_at: doc.get("last_run_ms").and_then(Value::as_i64).map(rfc3339),
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
    /// with no run history.
    pub async fn insert_duplicate_flow(&self, source: &Flow, new_name: String) -> Result<Flow> {
        let now = rfc3339(monotonic_ms());
        let flow = Flow {
            id: new_id(),
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
        let now = rfc3339(monotonic_ms());
        let flow = Flow {
            id: new_id(),
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
    /// (logged, never fatal).
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
            .sort(Sort::asc("created_ms"))
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

    /// Deletes a flow and its runs (with their steps) and revisions.
    ///
    /// Dependents go first and the definition last, so a failure part-way
    /// leaves the flow in place and calling this again finishes the job. Both
    /// are swept once more after the definition is gone. A run insert or
    /// graph update racing the removal can still land a document after that
    /// sweep; it names this flow's incarnation, which no longer exists, so no
    /// reader ever shows it and the next one to meet it deletes it
    /// (`lineage`) — even if a flow with the same id is created again.
    ///
    /// # Errors
    ///
    /// When no such flow exists.
    pub async fn remove_flow(&self, id: &str) -> Result<()> {
        let docs = self.docs().await?;
        if docs.get(DEFINITIONS, id).await?.is_none() {
            bail!("flow '{id}' not found");
        }
        docs.delete_where(RUNS, &Filter::eq("flow_id", id)).await?;
        docs.delete_where(REVISIONS, &Filter::eq("flow_id", id))
            .await?;
        if !docs.delete(DEFINITIONS, id, Precondition::None).await? {
            bail!("flow '{id}' not found");
        }
        // Once more for anything a concurrent run insert or graph update
        // wrote between the first sweep and the definition going.
        docs.delete_where(RUNS, &Filter::eq("flow_id", id)).await?;
        docs.delete_where(REVISIONS, &Filter::eq("flow_id", id))
            .await?;
        tracing::debug!(flow_id = %id, "[flows] removed flow definition");
        Ok(())
    }

    /// Toggles a flow's `enabled` flag, returning the updated flow.
    pub async fn set_enabled(&self, id: &str, enabled: bool) -> Result<Flow> {
        let docs = self.docs().await?;
        let updated = compare_and_swap(docs, DEFINITIONS, id, |doc| {
            let mut next = doc.clone();
            next["enabled"] = json!(enabled);
            next["updated_ms"] = json!(next_stamp(doc.get("updated_ms").and_then(Value::as_i64)));
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
        let now = Utc::now().timestamp_millis();
        let docs = self.docs().await?;
        compare_and_swap(docs, DEFINITIONS, id, |doc| {
            let mut next = doc.clone();
            next["last_run_ms"] = json!(now);
            next["last_status"] = json!(status);
            Some(next)
        })
        .await
        .context("Failed to record flow run")?
        .ok_or_else(|| anyhow!("flow '{id}' not found"))?;
        tracing::debug!(flow_id = %id, status, "[flows] recorded run");
        Ok(())
    }

    /// Records a run outcome only while its run row still belongs to the
    /// current incarnation of its workflow. A stale actor that settles after
    /// a workflow was deleted and recreated must not update the replacement's
    /// `last_run_at` or `last_status`.
    ///
    /// Returns `false` when the run no longer exists, is not settled to the
    /// requested status, or belongs to a removed workflow incarnation.
    pub async fn record_run_for_run(&self, run_id: &str, status: &str) -> Result<bool> {
        if status != "pending_approval"
            && !tinyflows_catalog::run_management::is_terminal_run_status(status)
        {
            return Ok(false);
        }
        let docs = self.docs().await?;
        let Some(run) = docs.get(RUNS, run_id).await? else {
            return Ok(false);
        };
        if text(&run.doc, "status") != Some(status) {
            return Ok(false);
        }
        let Some(flow_id) = text(&run.doc, "flow_id") else {
            return Ok(false);
        };
        let Some(mut flow) = docs.get(DEFINITIONS, flow_id).await? else {
            return Ok(false);
        };
        if !belongs(&run.doc, Some(&flow.doc)) {
            return Ok(false);
        }

        // Retry ordinary definition edits for fenced workflows, but bind
        // legacy unfenced documents to the version we observed: without an
        // incarnation id a replacement cannot otherwise be distinguished.
        let expected_incarnation = incarnation(&flow.doc).map(str::to_owned);
        let legacy_version = flow.version;
        for _ in 0..super::CAS_ATTEMPTS {
            let Some(current_run) = docs.get(RUNS, run_id).await? else {
                return Ok(false);
            };
            if text(&current_run.doc, "status") != Some(status)
                || text(&current_run.doc, "flow_id") != Some(flow_id)
                || !belongs(&current_run.doc, Some(&flow.doc))
            {
                return Ok(false);
            }

            let mut next = flow.doc.clone();
            next["last_run_ms"] = json!(Utc::now().timestamp_millis());
            next["last_status"] = json!(status);
            match docs.put(DEFINITIONS, flow_id, next, flow.unchanged()).await {
                Ok(_) => {
                    tracing::debug!(flow_id, run_id, status, "recorded settled flow run");
                    return Ok(true);
                }
                Err(error) if error.kind() == ErrorKind::Conflict => {
                    let Some(current) = docs.get(DEFINITIONS, flow_id).await? else {
                        return Ok(false);
                    };
                    let same_flow = match expected_incarnation.as_deref() {
                        Some(expected) => incarnation(&current.doc) == Some(expected),
                        None => current.version == legacy_version,
                    };
                    if !same_flow {
                        return Ok(false);
                    }
                    flow = current;
                }
                Err(error) => return Err(error).context("Failed to record flow run"),
            }
        }
        Ok(false)
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
        // JSON text is stored as the value it holds (an older-schema graph
        // still decodes); anything else stays a string, which never does.
        let raw: Value = serde_json::from_str(raw_graph_json)
            .unwrap_or_else(|_| Value::String(raw_graph_json.to_string()));
        let changed = compare_and_swap(docs, DEFINITIONS, flow_id, |doc| {
            let mut next = doc.clone();
            next["graph"] = json!(raw);
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
