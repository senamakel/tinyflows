//! `flows_drafts`: authoring drafts, one document per draft.
//!
//! The whole [`FlowDraft`] is stored as a JSON string beside `updated_ns`,
//! which orders the listing. `update_draft` is a compare-and-swap on the
//! draft, so the canvas and the authoring agent patching one draft at once
//! never drop each other's change — across processes too, which the file
//! store's process-local lock could not promise.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tinyflows_catalog::{DraftOrigin, FlowDraft};
use tinyflows_storage::{DocumentStoreExt, Precondition, Query, Sort, Versioned};

use super::{
    DRAFTS, FlowCatalogDocuments, compare_and_swap, instant_ms, monotonic_ms, new_id, next_stamp,
    put_doc, rfc3339, set_optional, text,
};

/// The same id rule as the file store, so an id valid on one backend is
/// valid on the other.
pub(super) fn check_id(id: &str) -> Result<()> {
    let safe = !id.is_empty()
        && id.len() <= 64
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !safe {
        bail!("invalid draft id: {id:?}");
    }
    Ok(())
}

pub(super) fn to_doc(draft: &FlowDraft) -> Result<Value> {
    let mut doc = json!({
        "name": draft.name,
        "graph": draft.graph,
        "origin": draft.origin.as_str(),
        "created_ms": instant_ms(&draft.created_at),
        "updated_ms": instant_ms(&draft.updated_at),
    });
    set_optional(&mut doc, "flow_id", draft.flow_id.as_deref());
    Ok(doc)
}

fn to_draft(stored: &Versioned<Value>) -> Result<FlowDraft> {
    from_doc(&stored.id, &stored.doc)
}

fn from_doc(id: &str, doc: &Value) -> Result<FlowDraft> {
    let corrupt = || format!("draft {id} is corrupt");
    Ok(FlowDraft {
        id: id.to_string(),
        flow_id: text(doc, "flow_id").map(str::to_string),
        name: text(doc, "name").with_context(corrupt)?.to_string(),
        graph: doc.get("graph").cloned().with_context(corrupt)?,
        origin: serde_json::from_value(doc.get("origin").cloned().unwrap_or_default())
            .with_context(corrupt)?,
        created_at: rfc3339(
            doc.get("created_ms")
                .and_then(Value::as_i64)
                .with_context(corrupt)?,
        ),
        updated_at: rfc3339(
            doc.get("updated_ms")
                .and_then(Value::as_i64)
                .with_context(corrupt)?,
        ),
    })
}

impl FlowCatalogDocuments {
    /// Creates a draft and returns it.
    pub async fn create_draft(
        &self,
        flow_id: Option<String>,
        name: String,
        graph: Value,
        origin: DraftOrigin,
    ) -> Result<FlowDraft> {
        let now = rfc3339(monotonic_ms());
        let draft = FlowDraft {
            id: new_id(),
            flow_id,
            name,
            graph,
            origin,
            created_at: now.clone(),
            updated_at: now,
        };
        let docs = self.docs().await?;
        put_doc(
            docs,
            DRAFTS,
            &draft.id,
            to_doc(&draft)?,
            Precondition::Absent,
        )
        .await
        .context("writing draft")?;
        tracing::debug!(target: "flows", draft_id = %draft.id, origin = draft.origin.as_str(), "[flows] draft_store: created draft");
        Ok(draft)
    }

    /// A draft by id, or `None`.
    pub async fn get_draft(&self, id: &str) -> Result<Option<FlowDraft>> {
        check_id(id)?;
        let docs = self.docs().await?;
        docs.get(DRAFTS, id)
            .await?
            .as_ref()
            .map(to_draft)
            .transpose()
    }

    /// Applies every `Some` field to draft `id`, bumps `updated_at` and
    /// returns the result.
    ///
    /// # Errors
    ///
    /// When the draft does not exist or is corrupt.
    pub async fn update_draft(
        &self,
        id: &str,
        name: Option<String>,
        graph: Option<Value>,
        flow_id: Option<Option<String>>,
    ) -> Result<FlowDraft> {
        check_id(id)?;
        let docs = self.docs().await?;
        if let Some(stored) = docs.get(DRAFTS, id).await? {
            to_draft(&stored)?;
        }
        let stored = compare_and_swap(docs, DRAFTS, id, |doc| {
            let mut draft = from_doc(id, doc).ok()?;
            if let Some(name) = &name {
                draft.name = name.clone();
            }
            if let Some(graph) = &graph {
                draft.graph = graph.clone();
            }
            if let Some(flow_id) = &flow_id {
                draft.flow_id = flow_id.clone();
            }
            // Stamped per attempt, strictly after the version being replaced,
            // so a retried update never lands with an older time.
            draft.updated_at = rfc3339(next_stamp(Some(instant_ms(&draft.updated_at))));
            to_doc(&draft).ok()
        })
        .await?
        .with_context(|| format!("draft {id} not found"))?;
        tracing::debug!(target: "flows", draft_id = %id, "[flows] draft_store: updated draft");
        to_draft(&stored)
    }

    /// Every draft, most recently updated first; a corrupt one is logged and
    /// skipped rather than failing the listing.
    pub async fn list_drafts(&self) -> Result<Vec<FlowDraft>> {
        let docs = self.docs().await?;
        let query = Query::all()
            .sort(Sort::desc("updated_ms"))
            .sort(Sort::desc("_id"));
        Ok(docs
            .query_all(DRAFTS, &query)
            .await?
            .iter()
            .filter_map(|stored| {
                to_draft(stored)
                    .map_err(|error| {
                        tracing::warn!(target: "flows", draft_id = %stored.id, %error, "[flows] draft_store: skipping corrupt draft");
                    })
                    .ok()
            })
            .collect())
    }

    /// Deletes a draft; `true` when one was removed.
    pub async fn delete_draft(&self, id: &str) -> Result<bool> {
        check_id(id)?;
        let docs = self.docs().await?;
        let removed = docs
            .delete(DRAFTS, id, Precondition::None)
            .await
            .with_context(|| format!("deleting draft {id}"))?;
        if removed {
            tracing::debug!(target: "flows", draft_id = %id, "[flows] draft_store: deleted draft");
        }
        Ok(removed)
    }
}

#[cfg(test)]
#[path = "drafts_tests.rs"]
mod tests;
