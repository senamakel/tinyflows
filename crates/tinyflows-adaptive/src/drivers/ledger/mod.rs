//! [`Ledger`] over a driver [`DocumentStore`]; see the module docs for the
//! layout and the tenancy model. The trait implementation is in
//! `trait_impl.rs`; this file holds the handle and its storage helpers.
//!
//! [`Ledger`]: crate::ledger::Ledger

use std::sync::Arc;

use serde_json::{Map, Value, json};
use tinyflows_storage::{
    Counters, DocumentStore, DocumentStoreExt, Filter, Precondition, Query, StorageError,
    Versioned, WriteOptions, primitives,
};
use tokio::sync::OnceCell;

use super::convert::{from_object, lesson_from, row_from};
use super::{CAS_ATTEMPTS, COUNTER_OWNER, SCHEMA_VERSION, collections, is_race};

mod trait_impl;
use crate::ledger::{LedgerError, LedgerRow, Lesson, LessonKind, Result};

pub(super) use super::{EPISODES, EVIDENCE, LESSONS, ROWS, SCORES, STEPS, VARIANTS};

pub(super) fn backend(error: StorageError) -> LedgerError {
    LedgerError::Backend(error.to_string())
}

pub(super) fn corrupt(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Corrupt(error.to_string())
}

/// The record a stored document holds, decoded by `rebuild` (which undoes the
/// stored shape) and `T`'s own deserializer.
pub(super) fn decode<T: serde::de::DeserializeOwned>(
    stored: &Versioned<Value>,
    rebuild: fn(&Value) -> std::result::Result<Map<String, Value>, String>,
) -> Result<T> {
    rebuild(&stored.doc)
        .and_then(from_object)
        .map_err(|error| corrupt(format!("{}: {error}", stored.id)))
}

pub(super) fn counter(doc: &Value, field: &str) -> u32 {
    doc.get(field)
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

/// Writes `doc` at `id` declaring the stores' body version.
pub(super) async fn put_doc(
    docs: &dyn DocumentStore,
    collection: &str,
    id: &str,
    doc: Value,
    precondition: Precondition,
) -> std::result::Result<tinyflows_storage::Version, StorageError> {
    docs.put_with(
        collection,
        id,
        doc,
        precondition,
        WriteOptions::new().schema(SCHEMA_VERSION),
    )
    .await
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
                for spec in collections()
                    .into_iter()
                    .chain(primitives::collection_specs())
                {
                    self.docs.ensure_collection(&spec).await.map_err(backend)?;
                }
                Ok::<(), LedgerError>(())
            })
            .await
            .map(|_| ())
    }

    /// The next value of the sequence `name` (`tsd.counters`, owner `flows`),
    /// atomically, so two writers never share one.
    pub(super) async fn next_seq(&self, name: &str) -> Result<u64> {
        Counters::new(&self.docs)
            .next(COUNTER_OWNER, name)
            .await
            .map_err(backend)
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
            match put_doc(self.docs.as_ref(), collection, id, doc, precondition).await {
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
        match put_doc(
            self.docs.as_ref(),
            collection,
            id,
            doc,
            Precondition::Absent,
        )
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
        let mut lesson: Lesson = decode(stored, lesson_from)?;
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
        let mut row: LedgerRow = decode(stored, row_from)?;
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
