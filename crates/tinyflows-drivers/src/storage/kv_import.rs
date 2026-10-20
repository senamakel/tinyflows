//! Moving the v1 key-value collections into `tsd.kv`.
//!
//! A document step may only write its own owner's collections and `tsd.kv`
//! belongs to `tsd`, so the move is an importer the host calls once per scope
//! after `Registry::migrate` (the pattern `tinyagents-harness` uses):
//! [`import_legacy_kv`] copies `flows_kv` (the catalog's namespaced state,
//! `{namespace, key, value_json}`) and `flows_state` (`DriverStateStore`,
//! `{key, value}`), removes them and records completion in `tsd.migrations`.
//!
//! The copy is idempotent and resumable: entries are written with
//! `Precondition::Absent`, so a crash re-runs harmlessly and a newer v2 value
//! is never overwritten by a stale v1 row.

use serde_json::Value;
use std::sync::Arc;
use tinyflows_storage::{
    CollectionSpec, Cursor, DocumentStore, ErrorKind, Filter, ID_FIELD, Key, Migrations,
    Precondition, Query, Result, Sort, primitives,
};

/// The `tsd.kv` owner of every entry tinyflows stores.
pub const KV_OWNER: &str = "flows";

/// The `tsd.kv` namespace `DriverStateStore` uses by default (`flows_state`).
pub const STATE_NAMESPACE: &str = "state";

/// Prefix of the `tsd.kv` namespace of a catalog state namespace (`flows_kv`).
pub const CATALOG_NAMESPACE_PREFIX: &str = "kv:";

const STEP: &str = "kv_import";
const PAGE: usize = 200;

/// The `tsd.kv` namespace holding catalog state namespace `namespace`.
pub fn catalog_namespace(namespace: &str) -> String {
    format!("{CATALOG_NAMESPACE_PREFIX}{namespace}")
}

/// One v1 row as the `tsd.kv` entry it becomes: `(ns, key, value)`.
fn convert_catalog(body: &Value) -> Option<(String, String, Value)> {
    let namespace = body.get("namespace")?.as_str()?;
    let key = body.get("key")?.as_str()?.to_owned();
    let value = serde_json::from_str(body.get("value_json")?.as_str()?).ok()?;
    Some((catalog_namespace(namespace), key, value))
}

fn convert_state(body: &Value) -> Option<(String, String, Value)> {
    Some((
        STATE_NAMESPACE.to_owned(),
        body.get("key")?.as_str()?.to_owned(),
        body.get("value")?.clone(),
    ))
}

async fn copy(
    docs: &Arc<dyn DocumentStore>,
    legacy: &str,
    convert: fn(&Value) -> Option<(String, String, Value)>,
) -> Result<u64> {
    docs.ensure_collection(&CollectionSpec::new(legacy)).await?;
    let mut written = 0_u64;
    let mut cursor: Option<Cursor> = None;
    loop {
        let mut query = Query::filter(Filter::default())
            .sort(Sort::asc(ID_FIELD))
            .limit(PAGE);
        if let Some(after) = cursor.take() {
            query = query.after(after);
        }
        let page = docs.query(legacy, &query).await?;
        for row in page.items {
            let Some((ns, key, value)) = convert(&row.doc) else {
                tracing::debug!(collection = legacy, id = %row.id, "[flows-storage] kv import dropped a malformed row");
                continue;
            };
            let id = Key::of([KV_OWNER, ns.as_str(), key.as_str()])?;
            let body = serde_json::json!({"owner": KV_OWNER, "ns": ns, "key": key, "value": value});
            match docs
                .put(primitives::KV, id.as_str(), body, Precondition::Absent)
                .await
            {
                Ok(_) => written += 1,
                Err(error)
                    if matches!(error.kind(), ErrorKind::AlreadyExists | ErrorKind::Conflict) => {}
                Err(error) => return Err(error),
            }
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    docs.delete_where(legacy, &Filter::default()).await?;
    Ok(written)
}

/// Copy `flows_kv` and `flows_state` into `tsd.kv` (owner `flows`) for the
/// scope `storage`, remove them, and record the import in `tsd.migrations`
/// (owner `flows`, step `kv_import`). Returns how many entries were written.
///
/// A completed import is skipped; a missing collection counts as empty. Rows
/// that are malformed are dropped and entries already present are kept.
///
/// # Errors
///
/// The first storage error; the import resumes on the next call because
/// nothing is removed before its collection has been copied.
pub async fn import_legacy_kv(docs: &Arc<dyn DocumentStore>) -> Result<u64> {
    let migrations = Migrations::new(docs, KV_OWNER);
    if migrations.done(KV_OWNER, STEP).await? {
        tracing::debug!("[flows-storage] kv import already done");
        return Ok(0);
    }
    primitives::ensure(docs).await?;
    docs.ensure_collection(&CollectionSpec::new("flows.migrations")).await?;
    let written = copy(docs, "flows_kv", convert_catalog).await?
        + copy(docs, "flows_state", convert_state).await?;
    migrations.mark_done(KV_OWNER, STEP).await?;
    tracing::debug!(written, "[flows-storage] kv import complete");
    Ok(written)
}

#[cfg(test)]
#[path = "kv_import_tests.rs"]
mod tests;
