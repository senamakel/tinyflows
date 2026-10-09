//! `flows_suggestions`: discovery suggestions — upsert, list, status.

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tinyflows_catalog::{FlowSuggestion, SuggestionStatus};
use tinystoragedrivers_core::{Filter, Query, Sort, Versioned};

use super::{
    FlowCatalogDocuments, SUGGESTIONS, compare_and_swap, instant_ns, required, set_optional, text,
    upsert,
};

fn strings(stored: &Versioned<Value>, field: &str) -> Result<Vec<String>> {
    serde_json::from_str(text(&stored.doc, field).unwrap_or("[]"))
        .with_context(|| format!("suggestion {} `{field}` is corrupt", stored.id))
}

fn to_suggestion(stored: &Versioned<Value>) -> Result<FlowSuggestion> {
    let doc = &stored.doc;
    Ok(FlowSuggestion {
        id: stored.id.clone(),
        title: required(stored, "title")?.to_string(),
        one_liner: required(stored, "one_liner")?.to_string(),
        rationale: required(stored, "rationale")?.to_string(),
        trigger_hint: text(doc, "trigger_hint").map(str::to_string),
        steps_outline: strings(stored, "steps_json")?,
        suggested_connections: strings(stored, "connections_json")?,
        suggested_slugs: strings(stored, "slugs_json")?,
        build_prompt: required(stored, "build_prompt")?.to_string(),
        confidence: doc.get("confidence").and_then(Value::as_f64).unwrap_or(0.0),
        status: SuggestionStatus::from_str_lossy(text(doc, "status").unwrap_or("new")),
        created_at: required(stored, "created_at")?.to_string(),
        source_run_id: text(doc, "source_run_id").map(str::to_string),
    })
}

impl FlowCatalogDocuments {
    /// Inserts a batch of suggestions. A suggestion already stored (its id is
    /// a content hash) has its pitch refreshed but keeps the `status` and
    /// `created_at` the user's history gave it, so a dismissed idea stays
    /// dismissed. Returns how many were written.
    pub async fn upsert_suggestions(&self, suggestions: &[FlowSuggestion]) -> Result<usize> {
        if suggestions.is_empty() {
            return Ok(0);
        }
        let docs = self.docs().await?;
        for s in suggestions {
            let steps = serde_json::to_string(&s.steps_outline)
                .context("Failed to serialize suggestion steps")?;
            let connections = serde_json::to_string(&s.suggested_connections)
                .context("Failed to serialize suggestion connections")?;
            let slugs = serde_json::to_string(&s.suggested_slugs)
                .context("Failed to serialize suggestion slugs")?;
            upsert(docs, SUGGESTIONS, &s.id, |existing| {
                let status = existing
                    .and_then(|doc| text(doc, "status"))
                    .unwrap_or(s.status.as_str());
                let created_at = existing
                    .and_then(|doc| text(doc, "created_at"))
                    .unwrap_or(&s.created_at);
                let mut doc = json!({
                    "title": s.title,
                    "one_liner": s.one_liner,
                    "rationale": s.rationale,
                    "steps_json": steps,
                    "connections_json": connections,
                    "slugs_json": slugs,
                    "build_prompt": s.build_prompt,
                    "confidence": s.confidence,
                    "status": status,
                    "created_at": created_at,
                    "created_ns": instant_ns(created_at),
                });
                set_optional(&mut doc, "trigger_hint", s.trigger_hint.as_deref());
                set_optional(&mut doc, "source_run_id", s.source_run_id.as_deref());
                Ok(doc)
            })
            .await
            .context("Failed to upsert flow suggestion")?;
        }
        tracing::debug!(
            count = suggestions.len(),
            "[flows] upserted flow suggestions"
        );
        Ok(suggestions.len())
    }

    /// Suggestions newest first, highest confidence first within a time,
    /// optionally only those in `status` (`limit` at least 1).
    pub async fn list_suggestions(
        &self,
        status: Option<SuggestionStatus>,
        limit: usize,
    ) -> Result<Vec<FlowSuggestion>> {
        let docs = self.docs().await?;
        let filter = status.map_or(Filter::All, |status| Filter::eq("status", status.as_str()));
        let query = Query::filter(filter)
            .sort(Sort::desc("created_ns"))
            .sort(Sort::desc("confidence"))
            .sort(Sort::asc("_id"))
            .limit(limit.max(1));
        let page = docs.query(SUGGESTIONS, &query).await?;
        page.items.iter().map(to_suggestion).collect()
    }

    /// Sets one suggestion's status; `false` when the id is unknown.
    pub async fn set_suggestion_status(&self, id: &str, status: SuggestionStatus) -> Result<bool> {
        let docs = self.docs().await?;
        let changed = compare_and_swap(docs, SUGGESTIONS, id, |doc| {
            let mut next = doc.clone();
            next["status"] = json!(status.as_str());
            Some(next)
        })
        .await
        .context("Failed to update flow suggestion status")?;
        tracing::debug!(suggestion_id = %id, status = %status.as_str(), changed = changed.is_some(), "[flows] set suggestion status");
        Ok(changed.is_some())
    }
}

#[cfg(test)]
#[path = "suggestions_tests.rs"]
mod tests;
