//! [`Ledger`] over a driver [`DocumentStore`]; see the module docs for the
//! layout and the tenancy model. The trait implementation is in
//! `trait_impl.rs`; this file holds the handle and its storage helpers.
//!
//! [`Ledger`]: crate::ledger::Ledger

use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, DocumentStoreExt, Filter, IndexSpec, Precondition, Query,
    StorageError, Versioned,
};
use tokio::sync::OnceCell;

use super::{CAS_ATTEMPTS, is_race};

mod trait_impl;
use crate::ledger::{Ledger, LedgerError, LedgerRow, Lesson, LessonKind, Result};

pub(super) const COUNTERS: &str = "adaptive_counters";
pub(super) const ROWS: &str = "adaptive_rows";
pub(super) const LESSONS: &str = "adaptive_lessons";
pub(super) const EVIDENCE: &str = "adaptive_evidence";
pub(super) const SCORES: &str = "adaptive_scores";
pub(super) const VARIANTS: &str = "adaptive_variants";
pub(super) const EPISODES: &str = "adaptive_episodes";
pub(super) const STEPS: &str = "adaptive_steps";

pub(super) fn backend(error: StorageError) -> LedgerError {
    LedgerError::Backend(error.to_string())
}

pub(super) fn corrupt(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Corrupt(error.to_string())
}

/// A record as stored: a JSON **string**, not a nested object. Records carry
/// arbitrary JSON (a goal, a step's output), and a MongoDB-backed document
/// port rejects object keys with a `.` or a leading `$`; the native Mongo
/// backend stores its records as strings for the same reason.
pub(super) fn encode(value: &impl Serialize) -> Result<Value> {
    serde_json::to_string(value)
        .map(Value::String)
        .map_err(corrupt)
}

/// The `record` field of a stored document, decoded.
pub(super) fn record<T: DeserializeOwned>(stored: &Versioned<Value>) -> Result<T> {
    let raw = stored
        .doc
        .get("record")
        .and_then(Value::as_str)
        .ok_or_else(|| corrupt(format!("{} has no record", stored.id)))?;
    serde_json::from_str(raw).map_err(|error| corrupt(format!("{}: {error}", stored.id)))
}

pub(super) fn counter(doc: &Value, field: &str) -> u32 {
    doc.get(field)
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

/// A ledger stored in a driver document store.
#[derive(Clone)]
pub struct DriverLedger {
    pub(super) docs: Arc<dyn DocumentStore>,
    pub(super) scope: Option<String>,
    declared: Arc<OnceCell<()>>,
}

impl std::fmt::Debug for DriverLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverLedger")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl DriverLedger {
    /// A ledger in `docs`, reading and writing the global bucket.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs,
            scope: None,
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// A handle onto the same store, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: impl Into<String>) -> Self {
        Self {
            docs: Arc::clone(&self.docs),
            scope: Some(scope.into()),
            declared: Arc::clone(&self.declared),
        }
    }

    /// This handle's bucket, as stored: `""` for global.
    pub(super) fn bucket(&self) -> &str {
        self.scope.as_deref().unwrap_or_default()
    }

    /// This bucket plus global, the one read rule everywhere.
    pub(super) fn visible(&self) -> Filter {
        Filter::one_of("scope_key", [self.bucket(), ""])
    }

    pub(super) async fn declared(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                let specs = [
                    CollectionSpec::new(COUNTERS),
                    CollectionSpec::new(ROWS).index(IndexSpec::new(
                        "by_episode",
                        ["scope_key", "episode", "seq"],
                    )),
                    CollectionSpec::new(LESSONS)
                        .index(IndexSpec::new("by_scope", ["scope_key", "seq"])),
                    CollectionSpec::new(EVIDENCE).index(IndexSpec::new("by_lesson", ["lesson_id"])),
                    CollectionSpec::new(SCORES),
                    CollectionSpec::new(VARIANTS)
                        .index(IndexSpec::new("by_parent", ["scope_key", "parent"])),
                    CollectionSpec::new(EPISODES)
                        .index(IndexSpec::new("by_scope", ["scope_key", "updated_at"])),
                    CollectionSpec::new(STEPS)
                        .index(IndexSpec::new("by_row", ["scope_key", "row_id", "seq"])),
                ];
                for spec in &specs {
                    self.docs.ensure_collection(spec).await.map_err(backend)?;
                }
                Ok::<(), LedgerError>(())
            })
            .await
            .map(|_| ())
    }

    /// The next value of the sequence `name`, by compare-and-swap so two
    /// writers never share one.
    pub(super) async fn next_seq(&self, name: &str) -> Result<u64> {
        for _ in 0..CAS_ATTEMPTS {
            let current = self.docs.get(COUNTERS, name).await.map_err(backend)?;
            let (next, precondition) = match &current {
                Some(found) => (
                    found.doc.get("seq").and_then(Value::as_u64).unwrap_or(0) + 1,
                    found.unchanged(),
                ),
                None => (1, Precondition::Absent),
            };
            match self
                .docs
                .put(COUNTERS, name, json!({ "seq": next }), precondition)
                .await
            {
                Ok(_) => return Ok(next),
                Err(error) if is_race(&error) => {}
                Err(error) => return Err(backend(error)),
            }
        }
        Err(LedgerError::Backend(format!(
            "the {name} sequence kept changing under {CAS_ATTEMPTS} attempts"
        )))
    }

    /// Adds one application (and one help when `helped`) to the counters of
    /// the document `id` in `collection`, creating it from `fresh` when
    /// absent; `None` from `fresh` means "do not create".
    pub(super) async fn bump(
        &self,
        collection: &str,
        id: &str,
        helped: bool,
        fresh: Option<Value>,
        visible: impl Fn(&Value) -> bool,
    ) -> Result<()> {
        for _ in 0..CAS_ATTEMPTS {
            let current = self.docs.get(collection, id).await.map_err(backend)?;
            let (mut doc, precondition) = match current {
                Some(found) if visible(&found.doc) => {
                    let pre = found.unchanged();
                    (found.doc, pre)
                }
                Some(_) => return Ok(()),
                None => match &fresh {
                    Some(fresh) => (fresh.clone(), Precondition::Absent),
                    None => return Ok(()),
                },
            };
            doc["applied"] = json!(u64::from(counter(&doc, "applied")) + 1);
            doc["helped"] = json!(u64::from(counter(&doc, "helped")) + u64::from(helped));
            match self.docs.put(collection, id, doc, precondition).await {
                Ok(_) => return Ok(()),
                Err(error) if is_race(&error) => {}
                Err(error) => return Err(backend(error)),
            }
        }
        Err(LedgerError::Backend(format!(
            "counters of {id} kept changing under {CAS_ATTEMPTS} attempts"
        )))
    }

    /// Writes `doc` at `id` only if nothing is there yet.
    pub(super) async fn insert_once(&self, collection: &str, id: &str, doc: Value) -> Result<()> {
        match self
            .docs
            .put(collection, id, doc, Precondition::Absent)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if is_race(&error) => Ok(()),
            Err(error) => Err(backend(error)),
        }
    }

    pub(super) async fn all(
        &self,
        collection: &str,
        query: Query,
    ) -> Result<Vec<Versioned<Value>>> {
        self.docs
            .query_all(collection, &query)
            .await
            .map_err(backend)
    }

    pub(super) fn lesson_from(stored: &Versioned<Value>) -> Result<Lesson> {
        let mut lesson: Lesson = record(stored)?;
        lesson.id.clone_from(&stored.id);
        lesson.applied = counter(&stored.doc, "applied");
        lesson.helped = counter(&stored.doc, "helped");
        let scope = stored
            .doc
            .get("scope_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        lesson.scope_key = (!scope.is_empty()).then(|| scope.to_string());
        Ok(lesson)
    }

    pub(super) fn row_from(stored: &Versioned<Value>) -> Result<LedgerRow> {
        let mut row: LedgerRow = record(stored)?;
        row.id.clone_from(&stored.id);
        Ok(row)
    }
}

pub(super) fn kind_str(kind: LessonKind) -> &'static str {
    match kind {
        LessonKind::Strategy => "strategy",
        LessonKind::Constraint => "constraint",
        LessonKind::FailureMode => "failure_mode",
        LessonKind::Calibration => "calibration",
    }
}

#[cfg(test)]
#[path = "../ledger_tests.rs"]
mod tests;
