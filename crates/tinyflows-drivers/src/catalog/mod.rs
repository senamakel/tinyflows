//! The flow catalog and authoring drafts on a [`DocumentStore`].
//!
//! [`FlowCatalogDocuments`] is the catalog a host keeps its saved flows in: one
//! `async fn` per operation (create, update with revision capture, runs and
//! their steps, state, suggestions, drafts), reporting [`FlowUpdateError`] for a
//! guarded graph update. The backend is whatever document handle the host
//! passes in. It replaces the `flows.db` and draft-file catalog of the retired
//! `tinyflows-sqlite`; hosts own any remaining file discovery or conversion.
//!
//! # Layout (storage schema v2, owner `flows`)
//!
//! | Collection | Document id | Holds |
//! | --- | --- | --- |
//! | `flows.definitions` | flow id (ULID for a new flow) | one saved [`Flow`](tinyflows_catalog::Flow) |
//! | `flows.revisions` | revision id (ULID) | one superseded graph |
//! | `flows.runs` | run id | one run and its steps |
//! | `flows.suggestions` | suggestion id | one discovery suggestion |
//! | `flows.drafts` | draft id (ULID) | one authoring draft |
//!
//! Namespaced state is not a collection of this owner: it lives in `tsd.kv`
//! (owner `flows`, namespace `kv:<namespace>`).
//!
//! Bodies are bare snake_case objects: graphs, step outputs, drafts and ids
//! lists are real JSON values, not strings. Optional fields are left out
//! rather than stored as `null`. Every instant is one epoch-millisecond field
//! (`created_ms`, `updated_ms`, `started_ms`, `finished_ms`, `last_run_ms`)
//! that the wire types render back as RFC 3339 with millisecond precision; the
//! `*_ns` copies are gone. The model's times are caller-supplied (an import
//! keeps a flow's original `created_at`), so they are body fields rather than
//! the driver-clock record metadata. Ordering is by the `_ms` field with the
//! id as the tie-break, which for ULID ids is creation order.
//!
//! # Where SQLite used a transaction
//!
//! The port has single-document compare-and-swap, not multi-document
//! transactions, so each guarded SQL `UPDATE … WHERE status = …` is a
//! compare-and-swap on the run or flow document, and the multi-row writes are
//! ordered so a crash part-way leaves nothing a reader would mistake for a
//! finished state:
//!
//! - `update_flow_graph` records the revision *before* swapping the graph, and
//!   removes it again when the swap loses; a crash in between leaves one extra
//!   revision, never a graph change without one.
//! - A run's steps live on the run document (`steps`, a JSON array), so
//!   settling a run and its step list is one compare-and-swap, and an
//!   incremental step write from a parallel branch retries on the winner's
//!   list instead of losing to it (R-m1).
//! - A revision is written *pending* before the graph swap and is visible
//!   only once the swap has named it (`last_revision_id`) or a later update
//!   has confirmed it, so a lost race or a crash never shows a revision for
//!   an update that did not happen.
//! - `updated_ms` advances strictly on every write, so it is
//!   a sound optimistic-concurrency token and revision order is total.
//! - Removing a flow removes its runs and revisions first and the definition
//!   last, so a failure part-way is retried by calling it again. A run or
//!   revision written concurrently can still land after the final sweep (or
//!   its writer be cancelled before undoing it), so every dependent records
//!   the incarnation of the flow it was written under and readers show only
//!   dependents of a flow that still exists with that incarnation: an orphan
//!   is never visible, and readers reclaim it (`lineage`).

use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;
use tinyflows_storage::{
    CollectionSpec, DocumentStore, ErrorKind, IndexSpec, Meta, Precondition, Repo, StorageError,
    Versioned, WriteOptions,
};
use tokio::sync::OnceCell;

pub use tinyflows_catalog::store::{
    FlowUpdateError, MAX_FLOW_RUNS_PER_FLOW, MAX_REVISIONS_PER_FLOW,
};

mod definitions;
mod drafts;
mod import;
mod kv;
mod lineage;
mod migrate;
mod revision_sequence;
mod revisions;
mod run_events;
mod runs;
mod runs_recovery;
mod state;
mod steps;
mod suggestions;

pub(crate) use migrate::extend_schema;
pub use state::FlowStateDocuments;

pub(crate) const DEFINITIONS: &str = "flows.definitions";
pub(crate) const REVISIONS: &str = "flows.revisions";
pub(crate) const RUNS: &str = "flows.runs";
pub(crate) const SUGGESTIONS: &str = "flows.suggestions";
pub(crate) const DRAFTS: &str = "flows.drafts";

/// The body version of every catalog collection (`flows_*` was version 1).
pub(crate) const SCHEMA_VERSION: u32 = 2;

/// Compare-and-swap attempts before a contended update gives up.
const CAS_ATTEMPTS: usize = 32;

/// The flow catalog and drafts over one (usually tenant-scoped) document
/// handle. Cheap to clone; clones share the handle.
#[derive(Debug, Clone)]
pub struct FlowCatalogDocuments {
    docs: Arc<dyn DocumentStore>,
    declared: Arc<OnceCell<()>>,
}

impl FlowCatalogDocuments {
    /// The catalog in `docs`.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs,
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// Every collection the catalog uses, with its indexes.
    pub fn collections() -> Vec<CollectionSpec> {
        vec![
            CollectionSpec::new(DEFINITIONS)
                .index(IndexSpec::new("by_enabled", ["enabled", "created_ms"])),
            CollectionSpec::new(REVISIONS)
                .index(IndexSpec::new("by_flow", ["flow_id", "created_ms"])),
            CollectionSpec::new(RUNS)
                .index(IndexSpec::new("by_flow", ["flow_id", "started_ms"]))
                .index(IndexSpec::new("by_status", ["status", "started_ms"]))
                .index(IndexSpec::new("by_started", ["started_ms"])),
            CollectionSpec::new(SUGGESTIONS)
                .index(IndexSpec::new("by_status", ["status", "created_ms"])),
            CollectionSpec::new(DRAFTS).index(IndexSpec::new("by_updated", ["updated_ms"])),
        ]
    }

    /// Declares [`Self::collections`]. Every method does this on first use;
    /// a host may call it at startup to fail early.
    ///
    /// # Errors
    ///
    /// When the backend refuses a collection or index.
    pub async fn ensure(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                for spec in Self::collections()
                    .into_iter()
                    .chain(tinyflows_storage::primitives::collection_specs())
                {
                    self.docs
                        .ensure_collection(&spec)
                        .await
                        .with_context(|| format!("declaring {}", spec.name))?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map(|_| ())
    }

    /// The handle, after declaring the collections.
    async fn docs(&self) -> Result<&Arc<dyn DocumentStore>> {
        self.ensure().await?;
        Ok(&self.docs)
    }
}

/// `raw` (RFC 3339) as epoch milliseconds; `0` when it does not parse, so a
/// malformed timestamp sorts first instead of failing the write.
pub(crate) fn instant_ms(raw: &str) -> i64 {
    DateTime::parse_from_rfc3339(raw).map_or(0, |at| at.with_timezone(&Utc).timestamp_millis())
}

/// Epoch milliseconds `ms` as the RFC 3339 text the wire types carry
/// (millisecond precision, `+00:00`).
pub(crate) fn rfc3339(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Millis, false)
}

/// The integer field `field` of `doc`; `0` when absent.
pub(crate) fn int(doc: &Value, field: &str) -> i64 {
    doc.get(field).and_then(Value::as_i64).unwrap_or(0)
}

pub(crate) use crate::ids::{monotonic_ms, new_id};

/// A typed handle on `collection` whose writes declare [`SCHEMA_VERSION`].
fn repo(docs: &Arc<dyn DocumentStore>, collection: &str) -> Result<Repo<Value>> {
    Ok(Repo::new(docs, collection)?.with_options(WriteOptions::new().schema(SCHEMA_VERSION)))
}

/// Writes `doc` at `collection/id` under `precondition`, declaring the
/// catalog's body version.
pub(crate) async fn put_doc(
    docs: &Arc<dyn DocumentStore>,
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

/// The string field `field` of `doc`.
pub(crate) fn text<'a>(doc: &'a Value, field: &str) -> Option<&'a str> {
    doc.get(field).and_then(Value::as_str)
}

/// The boolean field `field` of `doc`; `false` when absent.
pub(crate) fn flag(doc: &Value, field: &str) -> bool {
    doc.get(field).and_then(Value::as_bool).unwrap_or(false)
}

/// Sets `field` to `value` when `Some`, removes it when `None`.
pub(crate) fn set_optional(doc: &mut Value, field: &str, value: Option<&str>) {
    if let Some(object) = doc.as_object_mut() {
        match value {
            Some(value) => {
                object.insert(field.to_string(), Value::String(value.to_string()));
            }
            None => {
                object.remove(field);
            }
        }
    }
}

/// The required string field `field` of a stored document.
pub(crate) fn required<'a>(stored: &'a Versioned<Value>, field: &str) -> Result<&'a str> {
    text(&stored.doc, field)
        .with_context(|| format!("document {} has no `{field}` string", stored.id))
}

/// Applies `change` to document `id` under compare-and-swap (`Repo::update`);
/// returns what was stored, or `None` when the document is missing or `change`
/// declines.
pub(crate) async fn compare_and_swap(
    docs: &Arc<dyn DocumentStore>,
    collection: &str,
    id: &str,
    change: impl Fn(&Value) -> Option<Value> + Send + Sync,
) -> Result<Option<Versioned<Value>>> {
    let stored = repo(docs, collection)?
        .update(id, |current| Ok(current.and_then(|doc| change(&doc))))
        .await?;
    Ok(stored.map(|(doc, version)| Versioned {
        id: id.to_string(),
        version,
        doc,
        meta: Meta::unknown(),
    }))
}

/// Writes `id` as `build(existing)` — insert when absent, replace when
/// present — retrying when another writer gets in between.
pub(crate) async fn upsert<F>(
    docs: &Arc<dyn DocumentStore>,
    collection: &str,
    id: &str,
    build: F,
) -> Result<()>
where
    F: Fn(Option<&Value>) -> Result<Value> + Send + Sync,
{
    let mut failure = None;
    repo(docs, collection)?
        .update(id, |current| match build(current.as_ref()) {
            Ok(next) => Ok(Some(next)),
            Err(error) => {
                failure = Some(error);
                Ok(None)
            }
        })
        .await?;
    failure.map_or(Ok(()), Err)
}

/// Whether `error` is a storage write conflict (a compare-and-swap that kept
/// losing), as opposed to a backend failure.
pub(crate) fn is_conflict(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<StorageError>()
        .is_some_and(|error| error.kind() == ErrorKind::Conflict)
}

/// Now in epoch milliseconds, strictly after `previous`: the clock's reading
/// when it is later, else `previous` plus one. A field used as an
/// optimistic-concurrency token or an ordering key must never repeat, however
/// coarse the clock or however quick the succession.
pub(crate) fn next_stamp(previous: Option<i64>) -> i64 {
    let now = monotonic_ms();
    previous.map_or(now, |previous| now.max(previous + 1))
}

/// Runs `op` and logs (rather than fails on) its error: for the best-effort
/// cleanup SQLite also logged and swallowed.
pub(crate) async fn best_effort<F: Future<Output = Result<T>>, T>(what: &str, op: F) {
    if let Err(error) = op.await {
        tracing::warn!(target: "flows", %error, "[flows] {what} failed (kept going)");
    }
}

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
