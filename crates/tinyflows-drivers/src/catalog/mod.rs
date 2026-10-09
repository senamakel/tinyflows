//! The flow catalog and authoring drafts on a [`DocumentStore`].
//!
//! [`FlowCatalogDocuments`] is the document-port counterpart of
//! `tinyflows-sqlite`'s `flows` and `drafts` modules: one `async fn` per public
//! function there, with the same name, the same arguments minus the leading
//! catalog directory, and the same results — down to [`FlowUpdateError`] for a
//! guarded graph update. A host keeps one code path and picks the backend.
//!
//! # Layout
//!
//! | Collection | Document id | Holds |
//! | --- | --- | --- |
//! | `flows_definitions` | flow id | one saved [`Flow`](tinyflows_catalog::Flow) |
//! | `flows_revisions` | revision id | one superseded graph |
//! | `flows_runs` | run id | one run and its steps |
//! | `flows_kv` | `(namespace, key)` | one namespaced state value |
//! | `flows_suggestions` | suggestion id | one discovery suggestion |
//! | `flows_drafts` | draft id | one authoring draft |
//!
//! Graphs, step outputs, drafts and state values are JSON **strings**, so a
//! MongoDB-backed port never meets an arbitrary key. Optional fields are left
//! out rather than stored as `null`. Ordering uses epoch-nanosecond fields
//! (`created_ns`, `started_ns`, …) parsed from the RFC 3339 strings the model
//! carries, since strings with differing offsets or fractional digits do not
//! sort as instants.
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
//! - A run's steps live on the run document (`steps_json`, as in SQLite), so
//!   settling a run and its step list is one compare-and-swap, and an
//!   incremental step write from a parallel branch retries on the winner's
//!   list instead of losing to it (R-m1).
//! - A revision is written *pending* before the graph swap and is visible
//!   only once the swap has named it (`last_revision_id`) or a later update
//!   has confirmed it, so a lost race or a crash never shows a revision for
//!   an update that did not happen.
//! - `updated_at` advances strictly on every write, so it is
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
use chrono::DateTime;
use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, ErrorKind, IndexSpec, Precondition, StorageError, Versioned,
};
use tokio::sync::OnceCell;

pub use tinyflows_catalog::store::{
    FlowUpdateError, MAX_FLOW_RUNS_PER_FLOW, MAX_REVISIONS_PER_FLOW,
};

mod definitions;
mod drafts;
mod kv;
mod lineage;
mod revisions;
mod runs;
mod state;
mod steps;
mod suggestions;

pub use state::FlowStateDocuments;

pub(crate) const DEFINITIONS: &str = "flows_definitions";
pub(crate) const REVISIONS: &str = "flows_revisions";
pub(crate) const RUNS: &str = "flows_runs";
pub(crate) const KV: &str = "flows_kv";
pub(crate) const SUGGESTIONS: &str = "flows_suggestions";
pub(crate) const DRAFTS: &str = "flows_drafts";

/// Compare-and-swap attempts before a contended update gives up.
const CAS_ATTEMPTS: usize = 32;

/// Longest composite id stored as is; longer ones are hashed.
const MAX_KEY_LEN: usize = 400;

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
                .index(IndexSpec::new("by_enabled", ["enabled", "created_ns"])),
            CollectionSpec::new(REVISIONS)
                .index(IndexSpec::new("by_flow", ["flow_id", "created_ns"])),
            CollectionSpec::new(RUNS)
                .index(IndexSpec::new("by_flow", ["flow_id", "started_ns"]))
                .index(IndexSpec::new("by_status", ["status", "started_ns"]))
                .index(IndexSpec::new("by_started", ["started_ns"])),
            CollectionSpec::new(KV),
            CollectionSpec::new(SUGGESTIONS)
                .index(IndexSpec::new("by_status", ["status", "created_ns"])),
            CollectionSpec::new(DRAFTS).index(IndexSpec::new("by_updated", ["updated_ns"])),
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
                for spec in Self::collections() {
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

/// A document id for `parts`: length-prefixed so no two tuples collide, and
/// replaced by its SHA-256 when it would exceed the driver's id limit.
pub(crate) fn composite_id(parts: &[&str]) -> String {
    let joined: String = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<Vec<_>>()
        .join("/");
    if joined.len() <= MAX_KEY_LEN {
        joined
    } else {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(joined.as_bytes());
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        format!("h:{hex}")
    }
}

/// `raw` (RFC 3339) as epoch nanoseconds; `0` when it does not parse, so a
/// malformed timestamp sorts first instead of failing the write.
pub(crate) fn instant_ns(raw: &str) -> i64 {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .and_then(|at| at.timestamp_nanos_opt())
        .unwrap_or(0)
}

/// Whether instant `a` is strictly before `b`. Compared as instants when both
/// parse, else as strings — what SQLite's lexicographic `<` did.
pub(crate) fn instant_before(a: &str, b: &str) -> bool {
    match (
        DateTime::parse_from_rfc3339(a),
        DateTime::parse_from_rfc3339(b),
    ) {
        (Ok(a), Ok(b)) => a < b,
        _ => a < b,
    }
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

/// Applies `change` to document `id` under compare-and-swap; returns what
/// was stored, or `None` when the document is missing or `change` declines.
pub(crate) async fn compare_and_swap(
    docs: &Arc<dyn DocumentStore>,
    collection: &str,
    id: &str,
    change: impl Fn(&Value) -> Option<Value>,
) -> Result<Option<Versioned<Value>>> {
    for _ in 0..CAS_ATTEMPTS {
        let Some(stored) = docs.get(collection, id).await? else {
            return Ok(None);
        };
        let Some(next) = change(&stored.doc) else {
            return Ok(None);
        };
        match docs
            .put(collection, id, next.clone(), stored.unchanged())
            .await
        {
            Ok(version) => {
                return Ok(Some(Versioned {
                    id: id.to_string(),
                    version,
                    doc: next,
                }));
            }
            Err(error) if error.kind() == ErrorKind::Conflict => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(StorageError::conflict(format!(
        "{collection}/{id} kept changing under {CAS_ATTEMPTS} attempts"
    ))
    .into())
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
    F: Fn(Option<&Value>) -> Result<Value>,
{
    for _ in 0..CAS_ATTEMPTS {
        let existing = docs.get(collection, id).await?;
        let next = build(existing.as_ref().map(|stored| &stored.doc))?;
        let precondition = existing
            .as_ref()
            .map_or(Precondition::Absent, Versioned::unchanged);
        match docs.put(collection, id, next, precondition).await {
            Ok(_) => return Ok(()),
            Err(error)
                if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(StorageError::conflict(format!(
        "{collection}/{id} kept changing under {CAS_ATTEMPTS} attempts"
    ))
    .into())
}

/// Whether `error` is a storage write conflict (a compare-and-swap that kept
/// losing), as opposed to a backend failure.
pub(crate) fn is_conflict(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<StorageError>()
        .is_some_and(|error| error.kind() == ErrorKind::Conflict)
}

/// `now` as an RFC 3339 stamp strictly after `previous`: the clock's reading
/// when it is later, else `previous` plus one nanosecond. A field used as an
/// optimistic-concurrency token or an ordering key must never repeat, however
/// coarse the clock or however quick the succession.
pub(crate) fn next_stamp(previous: Option<&str>) -> String {
    use chrono::{SecondsFormat, Utc};
    let now = Utc::now();
    let floor = previous
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|at| at.with_timezone(&Utc) + chrono::Duration::nanoseconds(1));
    let stamp = match floor {
        Some(floor) if floor > now => floor,
        _ => now,
    };
    stamp.to_rfc3339_opts(SecondsFormat::Nanos, false)
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
