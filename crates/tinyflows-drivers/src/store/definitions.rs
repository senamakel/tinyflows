//! `flows.workflows`: saved workflows, with the revision of whatever a write
//! replaces captured first.

use serde_json::Value;
use tinyflows::store::types::{WorkflowError, WorkflowId, WorkflowRecord, WorkflowSummary};
use tinyflows_storage::{DocumentStoreExt, ErrorKind, Precondition, Query, Sort, Versioned};

use super::codec::{definition_body, definition_from_body};
use super::{CAS_ATTEMPTS, DEFINITIONS, Inner, doc_id, put_doc, storage};

/// The record as the definition document keeps it.
pub(super) fn to_doc(record: &WorkflowRecord) -> Result<Value, WorkflowError> {
    definition_body(record)
}

/// Decodes a stored record.
pub(super) fn decode(stored: &Value) -> Result<WorkflowRecord, String> {
    definition_from_body(stored.clone())
}

impl Inner {
    /// The stored definition `id` and the record it holds, `None` when absent.
    /// A document that no longer decodes, or that the policy now refuses, reads
    /// as absent (and is logged): a listing only ever shows workflows that
    /// would run, and the operator hears about the broken one by name.
    async fn current(
        &self,
        id: &str,
    ) -> Result<Option<(Versioned<Value>, WorkflowRecord)>, WorkflowError> {
        self.ensure().await?;
        let Some(stored) = self
            .docs
            .get(DEFINITIONS, &doc_id(id)?)
            .await
            .map_err(storage)?
        else {
            return Ok(None);
        };
        match self.judged(&stored) {
            Ok(record) => Ok(Some((stored, record))),
            Err(reason) => {
                tracing::warn!(target: "workflows", workflow_id = %id, %reason, "[workflows] skipping an unreadable definition");
                Ok(None)
            }
        }
    }

    /// The record `stored` holds, judged by the host policy and the engine's
    /// validation.
    fn judged(&self, stored: &Versioned<Value>) -> Result<WorkflowRecord, String> {
        let record = decode(&stored.doc)?;
        self.policy
            .check_defaults(&record.defaults)
            .map_err(|error| format!("invalid `defaults`: {error}"))?;
        tinyflows::store::validate_graph(&record.id, &record.graph)
            .map_err(|error| error.to_string())?;
        Ok(record)
    }

    pub(super) async fn list(&self) -> Result<Vec<WorkflowSummary>, WorkflowError> {
        self.ensure().await?;
        let query = Query::all().sort(Sort::asc("id")).sort(Sort::asc("_id"));
        let mut summaries = Vec::new();
        for stored in self
            .docs
            .query_all(DEFINITIONS, &query)
            .await
            .map_err(storage)?
        {
            match self.judged(&stored) {
                Ok(record) => summaries.push(record.summary()),
                Err(reason) => {
                    tracing::warn!(target: "workflows", document = %stored.id, %reason, "[workflows] skipping an unreadable definition");
                }
            }
        }
        Ok(summaries)
    }

    pub(super) async fn get(&self, id: &str) -> Result<Option<WorkflowRecord>, WorkflowError> {
        Ok(self.current(id).await?.map(|(_, record)| record))
    }

    pub(super) async fn save(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        // The id decides the document, so it is checked before anything else.
        let id = doc_id(&record.id)?;
        // Validate before writing so a listing can be trusted to be runnable.
        tinyflows::store::validate_graph(&record.id, &record.graph)?;
        let doc = to_doc(record)?;
        for _ in 0..CAS_ATTEMPTS {
            self.ensure().await?;
            let stored = self.docs.get(DEFINITIONS, &id).await.map_err(storage)?;
            // Snapshot what is about to be replaced, before replacing it. Doing
            // it here rather than at each call site is what makes every
            // authoring surface undoable without any of them opting in. A
            // document that no longer parses is not a version worth keeping,
            // and refusing the save over it would strand the operator with a
            // broken definition they cannot overwrite.
            let revision = match stored.as_ref().and_then(|s| self.superseded(s)) {
                Some(superseded) => Some(self.capture(&superseded).await?),
                None => None,
            };
            let precondition = stored
                .as_ref()
                .map_or(Precondition::Absent, Versioned::unchanged);
            match put_doc(
                self.docs.as_ref(),
                DEFINITIONS,
                &id,
                doc.clone(),
                precondition,
            )
            .await
            {
                Ok(_) => {
                    if let Some(revision) = revision {
                        self.commit_capture(&record.id, &revision).await;
                    }
                    return Ok(());
                }
                Err(error) if is_race(error.kind()) => {
                    self.rollback_capture(revision.as_deref()).await;
                }
                Err(error) => {
                    self.rollback_capture(revision.as_deref()).await;
                    return Err(storage(error));
                }
            }
        }
        Err(contended(&record.id))
    }

    /// Saves `record` only while the current record's `fingerprint` still
    /// equals `expected`. The comparison and the write are one compare-and-swap
    /// on the document, so two writers holding the same observation cannot both
    /// win.
    pub(super) async fn save_if_current(
        &self,
        record: &WorkflowRecord,
        expected: &str,
        fingerprint: impl Fn(&WorkflowRecord) -> String,
    ) -> Result<bool, WorkflowError> {
        let id = doc_id(&record.id)?;
        tinyflows::store::validate_graph(&record.id, &record.graph)?;
        let doc = to_doc(record)?;
        for _ in 0..CAS_ATTEMPTS {
            let Some((stored, current)) = self.current(&record.id).await? else {
                return Ok(false);
            };
            if fingerprint(&current) != expected {
                return Ok(false);
            }
            let revision = self.capture(&current).await?;
            match put_doc(
                self.docs.as_ref(),
                DEFINITIONS,
                &id,
                doc.clone(),
                stored.unchanged(),
            )
            .await
            {
                Ok(_) => {
                    self.commit_capture(&record.id, &revision).await;
                    return Ok(true);
                }
                // Lost to a concurrent writer: look again. If what it wrote no
                // longer matches the observation, the comparison above says so.
                Err(error) if is_race(error.kind()) => self.rollback_capture(Some(&revision)).await,
                Err(error) => {
                    self.rollback_capture(Some(&revision)).await;
                    return Err(storage(error));
                }
            }
        }
        Err(contended(&record.id))
    }

    pub(super) async fn delete(&self, id: &str) -> Result<(), WorkflowError> {
        let key = doc_id(id)?;
        for _ in 0..CAS_ATTEMPTS {
            self.ensure().await?;
            let Some(stored) = self.docs.get(DEFINITIONS, &key).await.map_err(storage)? else {
                return Err(WorkflowError::NotFound(WorkflowId::from(id)));
            };
            // Snapshot before removing. A delete is the one edit that leaves
            // nothing to diff against afterwards, so without this it is the one
            // edit that cannot be undone.
            let revision = match self.superseded(&stored) {
                Some(superseded) => Some(self.capture(&superseded).await?),
                None => None,
            };
            match self
                .docs
                .delete(DEFINITIONS, &key, stored.unchanged())
                .await
            {
                Ok(true) => {
                    if let Some(revision) = revision {
                        self.commit_capture(id, &revision).await;
                    }
                    return Ok(());
                }
                Ok(false) => {
                    self.rollback_capture(revision.as_deref()).await;
                    return Err(WorkflowError::NotFound(WorkflowId::from(id)));
                }
                Err(error) if is_race(error.kind()) => {
                    self.rollback_capture(revision.as_deref()).await;
                }
                Err(error) => {
                    self.rollback_capture(revision.as_deref()).await;
                    return Err(storage(error));
                }
            }
        }
        Err(contended(id))
    }

    /// The record `stored` holds if it is one worth keeping a revision of.
    fn superseded(&self, stored: &Versioned<Value>) -> Option<WorkflowRecord> {
        decode(&stored.doc).ok()
    }
}

/// A write that lost to a concurrent one, as opposed to a backend failure.
pub(super) fn is_race(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::Conflict | ErrorKind::AlreadyExists)
}

fn contended(id: &str) -> WorkflowError {
    WorkflowError::Storage(format!(
        "workflow '{id}' kept changing under {CAS_ATTEMPTS} attempts"
    ))
}
