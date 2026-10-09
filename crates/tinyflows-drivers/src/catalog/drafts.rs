//! `flows_drafts`: authoring drafts, one document per draft (the SQLite
//! backend keeps one JSON file each).
//!
//! The whole [`FlowDraft`] is stored as a JSON string beside `updated_ns`,
//! which orders the listing. `update_draft` is a compare-and-swap on the
//! draft, so the canvas and the authoring agent patching one draft at once
//! never drop each other's change — across processes too, which the file
//! store's process-local lock could not promise.

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde_json::{Value, json};
use tinyflows_catalog::{DraftOrigin, FlowDraft};
use tinystoragedrivers_core::{DocumentStoreExt, Precondition, Query, Sort, Versioned};
use uuid::Uuid;

use super::{DRAFTS, FlowCatalogDocuments, compare_and_swap, instant_ns, required};

/// The same id rule as the file store, so an id valid on one backend is
/// valid on the other.
fn check_id(id: &str) -> Result<()> {
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

fn to_doc(draft: &FlowDraft) -> Result<Value> {
    Ok(json!({
        "draft_json": serde_json::to_string(draft).context("serializing draft")?,
        "updated_ns": instant_ns(&draft.updated_at),
    }))
}

fn to_draft(stored: &Versioned<Value>) -> Result<FlowDraft> {
    serde_json::from_str(required(stored, "draft_json")?)
        .with_context(|| format!("draft {} is corrupt", stored.id))
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
        let now = Utc::now().to_rfc3339();
        let draft = FlowDraft {
            id: Uuid::new_v4().to_string(),
            flow_id,
            name,
            graph,
            origin,
            created_at: now.clone(),
            updated_at: now,
        };
        let docs = self.docs().await?;
        docs.put(DRAFTS, &draft.id, to_doc(&draft)?, Precondition::Absent)
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
        let updated_at = Utc::now().to_rfc3339();
        let stored = compare_and_swap(docs, DRAFTS, id, |doc| {
            let mut draft: FlowDraft =
                serde_json::from_str(doc.get("draft_json")?.as_str()?).ok()?;
            if let Some(name) = &name {
                draft.name = name.clone();
            }
            if let Some(graph) = &graph {
                draft.graph = graph.clone();
            }
            if let Some(flow_id) = &flow_id {
                draft.flow_id = flow_id.clone();
            }
            draft.updated_at = updated_at.clone();
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
            .sort(Sort::desc("updated_ns"))
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
