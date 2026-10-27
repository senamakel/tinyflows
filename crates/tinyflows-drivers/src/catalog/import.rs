//! Id-preserving, write-if-absent import of records from an older store.
//!
//! The hosts that kept the catalog in `flows.db` and the drafts as JSON files
//! (the retired `tinyflows-sqlite`) move onto documents by reading those
//! records back as the catalog model and handing them to the methods below.
//! Each one stores a record under the id it already has, in the same document
//! shape the catalog's own writers produce, and only when no document with
//! that id exists: it never replaces a record the catalog already holds, so an
//! import can be repeated (after a crash part-way) or run against a scope that
//! has since been written to without clobbering anything. The return value says
//! whether this call wrote the record.
//!
//! Hosts own any file discovery and source-format decoding.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::sync::Arc;
use tinyflows_catalog::{Flow, FlowDraft, FlowRevision, FlowRun, FlowSuggestion};
use tinyflows_storage::{DocumentStore, ErrorKind, Precondition};

use super::definitions::to_doc as flow_doc;
use super::drafts::{check_id, to_doc as draft_doc};
use super::lineage::{FLOW_INCARNATION, incarnation};
use super::runs::set_finished;
use super::steps::with_steps;
use super::suggestions::suggestion_doc;
use super::{
    DEFINITIONS, DRAFTS, FlowCatalogDocuments, REVISIONS, RUNS, SUGGESTIONS, instant_ms, put_doc,
    set_optional,
};

/// Writes `doc` at `collection/id` only if absent. `Ok(false)` when a document
/// already held the id.
async fn put_if_absent(
    docs: &Arc<dyn DocumentStore>,
    collection: &str,
    id: &str,
    doc: Value,
) -> Result<bool> {
    match put_doc(docs, collection, id, doc, Precondition::Absent).await {
        Ok(_) => Ok(true),
        // An absent-precondition put that finds the id taken reports a
        // conflict on some backends and "already exists" on others.
        Err(error) if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) => {
            Ok(false)
        }
        Err(error) => Err(error).with_context(|| format!("importing {collection}/{id}")),
    }
}

impl FlowCatalogDocuments {
    /// Imports a flow under its own id, keeping its timestamps, enabled flag,
    /// approval requirement and last-run summary. The sequence is left
    /// unseeded so later imported history can define its legacy baseline.
    ///
    /// # Errors
    ///
    /// When the graph cannot be serialized or the backend fails.
    pub async fn import_flow(&self, flow: &Flow) -> Result<bool> {
        let docs = self.docs().await?;
        let mut doc = flow_doc(flow, None)?;
        if let Some(object) = doc.as_object_mut() {
            object.remove("revision");
        }
        let wrote = put_if_absent(docs, DEFINITIONS, &flow.id, doc).await?;
        tracing::debug!(target: "flows", flow_id = %flow.id, wrote, "[flows] import: flow");
        Ok(wrote)
    }

    /// Imports a superseded-graph revision under its own id, visible at once.
    /// The flow it belongs to must already exist (import flows first). History
    /// imports do not advance the current graph sequence; an unseeded imported
    /// flow derives its baseline from visible snapshots until its first update.
    ///
    /// # Errors
    ///
    /// When the flow is missing or the backend fails.
    pub async fn import_revision(&self, revision: &FlowRevision) -> Result<bool> {
        let docs = self.docs().await?;
        let flow = self.owner(&revision.flow_id, &revision.id).await?;
        let mut doc = json!({
            "flow_id": revision.flow_id,
            "graph": revision.graph,
            "name": revision.name,
            "require_approval": revision.require_approval,
            "created_ms": instant_ms(&revision.created_at),
        });
        set_optional(&mut doc, FLOW_INCARNATION, incarnation(&flow));
        let wrote = put_if_absent(docs, REVISIONS, &revision.id, doc).await?;
        tracing::debug!(target: "flows", revision_id = %revision.id, wrote, "[flows] import: revision");
        Ok(wrote)
    }

    /// Imports a run record, with its steps, pending approvals, error and
    /// parking hash, under its own id. The flow it belongs to must already
    /// exist (import flows first). A run still marked live stays live; the host
    /// decides whether to reconcile it.
    ///
    /// # Errors
    ///
    /// When the flow is missing or the backend fails.
    pub async fn import_run(&self, run: &FlowRun) -> Result<bool> {
        let docs = self.docs().await?;
        let flow = self.owner(&run.flow_id, &run.id).await?;
        let mut doc = json!({
            "flow_id": run.flow_id,
            "thread_id": run.thread_id,
            "status": run.status,
            "started_ms": instant_ms(&run.started_at),
            "steps": [],
            "pending_approvals": serde_json::to_value(&run.pending_approvals)
                .context("Failed to serialize flow run pending approvals")?,
        });
        doc = with_steps(&doc, &run.steps)?;
        set_finished(&mut doc, run.finished_at.as_deref());
        set_optional(&mut doc, "error", run.error.as_deref());
        set_optional(&mut doc, "graph_hash", run.graph_hash.as_deref());
        set_optional(&mut doc, FLOW_INCARNATION, incarnation(&flow));
        let wrote = put_if_absent(docs, RUNS, &run.id, doc).await?;
        tracing::debug!(target: "flows", run_id = %run.id, wrote, "[flows] import: run");
        Ok(wrote)
    }

    /// Imports one namespaced state value, keeping any value already stored.
    ///
    /// # Errors
    ///
    /// When the value cannot be serialized or the backend fails.
    pub async fn import_kv(&self, namespace: &str, key: &str, value: &Value) -> Result<bool> {
        // `tsd.kv` has no insert-if-absent, so the check and the write are two
        // steps; the import runs once per scope before the catalog is in use.
        if self.kv_get(namespace, key).await?.is_some() {
            return Ok(false);
        }
        self.kv_set(namespace, key, value).await?;
        Ok(true)
    }

    /// Imports a discovery suggestion, keeping its status and creation time.
    ///
    /// # Errors
    ///
    /// When the backend fails.
    pub async fn import_suggestion(&self, suggestion: &FlowSuggestion) -> Result<bool> {
        let docs = self.docs().await?;
        let doc = suggestion_doc(
            suggestion,
            suggestion.status.as_str(),
            instant_ms(&suggestion.created_at),
        )?;
        put_if_absent(docs, SUGGESTIONS, &suggestion.id, doc).await
    }

    /// Imports an authoring draft under its own id, keeping its timestamps.
    ///
    /// # Errors
    ///
    /// When the id is not a safe draft id or the backend fails.
    pub async fn import_draft(&self, draft: &FlowDraft) -> Result<bool> {
        check_id(&draft.id)?;
        let docs = self.docs().await?;
        put_if_absent(docs, DRAFTS, &draft.id, draft_doc(draft)?).await
    }

    /// The stored definition `flow_id` that record `record_id` hangs off.
    async fn owner(&self, flow_id: &str, record_id: &str) -> Result<Value> {
        let docs = self.docs().await?;
        match docs.get(DEFINITIONS, flow_id).await? {
            Some(stored) => Ok(stored.doc),
            None => bail!("cannot import {record_id}: flow '{flow_id}' does not exist"),
        }
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;
