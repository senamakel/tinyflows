//! The adaptive stores' schema v2 declarations, their v1 -> v2 steps and the
//! counter importer. The adaptive crate declares and migrates its own records
//! under owner `flows`, preserving the existing `flows.adaptive_*` namespace.
//!
//! | v1 | v2 |
//! |---|---|
//! | `adaptive_rows` | `flows.adaptive_rows`: the row's fields flat, `at` becomes `at_ms`, id `ldg_<seq>` unchanged |
//! | `adaptive_lessons` | `flows.adaptive_lessons`: the lesson's fields flat; `applied`/`helped` come from the document's live counters |
//! | `adaptive_evidence` | `flows.adaptive_evidence`: id `Key::of([lesson, row])` |
//! | `adaptive_scores` | `flows.adaptive_scores`: id `Key::of([scope_key, workflow])` |
//! | `adaptive_variants` | `flows.adaptive_variants`: id `Key::of([scope_key, variant])` |
//! | `adaptive_episodes` | `flows.adaptive_episodes`: id `Key::of([scope_key, episode])`, `started_at`/`updated_at` become `*_ms`, `id` becomes `episode_id` |
//! | `adaptive_steps` | `flows.adaptive_steps`: id `Key::of([scope_key, row, seq])`, snake_case keys, `output` verbatim |
//! | `adaptive_workflows` | `flows.adaptive_workflows`: id `Key::of([scope_key, workflow])`, the workflow's fields flat beside `workflow_id` |
//! | `adaptive_counters` | `tsd.counters` owner `flows`, through [`import_legacy_adaptive_counters`] |
//!
//! Each v1 record was a JSON string under `record`; a record whose string does
//! not parse is carried as it was under its v1 id, so it stays corrupt and
//! visible (readers report it) instead of being lost.

use serde_json::{Map, Value};
use std::sync::Arc;
use tinyflows_storage::{
    Cursor, DocumentStore, Filter, ID_FIELD, Key, Migrations, Owner, Query, Result, Schema,
    ScopeClass, Sort, SourceRecord, Step, TargetRecord, primitives,
};

use super::convert::{episode_body, lesson_body, row_body, step_body, workflow_body};
use super::{
    COUNTER_OWNER, COUNTERS, EPISODES, EVIDENCE, LESSONS, ROWS, SCHEMA_VERSION, SCORES, STEPS,
    VARIANTS, WORKFLOWS, indexed_collection_specs,
};

const COUNTERS_STEP: &str = "adaptive_counters_import";
const PAGE: usize = 200;

/// Adaptive-owned `flows.adaptive_*` declarations and their v1 -> v2 steps.
#[must_use]
#[allow(clippy::expect_used, reason = "flows is a fixed valid owner name")]
pub fn storage_schema() -> Schema {
    let mut schema = Schema::new(Owner::new("flows").expect("valid flows owner"));
    for spec in indexed_collection_specs() {
        schema = schema.collection_with(SCHEMA_VERSION, spec, ScopeClass::Tenant);
    }
    schema
        .step(step("adaptive_rows_v2", ROWS, row))
        .step(step("adaptive_lessons_v2", LESSONS, lesson))
        .step(step("adaptive_evidence_v2", EVIDENCE, evidence))
        .step(step("adaptive_scores_v2", SCORES, score))
        .step(step("adaptive_variants_v2", VARIANTS, variant))
        .step(step("adaptive_episodes_v2", EPISODES, episode))
        .step(step("adaptive_steps_v2", STEPS, steps))
        .step(step("adaptive_workflows_v2", WORKFLOWS, workflow))
}

/// Pure adaptive collection declarations for host bootstrap and documentation.
#[must_use]
pub fn collections() -> Vec<tinyflows_storage::CollectionSpec> {
    storage_schema().collections()
}

type Convert = fn(&SourceRecord) -> Option<TargetRecord>;

/// A step from the v1 collection named like the v2 entity without its owner
/// prefix (`flows.adaptive_rows` <- `adaptive_rows`).
fn step(id: &'static str, item: &'static str, convert: Convert) -> Step {
    let source = item.strip_prefix("flows.").unwrap_or(item);
    Step::new(id, item, 1, 2, move |batch| {
        Ok(batch.iter().filter_map(convert).collect())
    })
    .source(source)
}

/// The JSON object a v1 document's `record` string holds.
fn record(source: &SourceRecord) -> Option<Map<String, Value>> {
    match serde_json::from_str(source.body.get("record")?.as_str()?).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

fn text<'a>(source: &'a SourceRecord, field: &str) -> &'a str {
    source
        .body
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn seq(source: &SourceRecord) -> u64 {
    source.body.get("seq").and_then(Value::as_u64).unwrap_or(0)
}

fn key(parts: &[&str], fallback: &str) -> String {
    Key::of(parts.iter().copied()).map_or_else(|_| fallback.to_owned(), |k| k.as_str().to_owned())
}

fn carried(collection: &str, source: &SourceRecord) -> TargetRecord {
    TargetRecord::new(collection, source.id.clone(), source.body.clone())
}

fn row(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(rec) = record(source) else {
        return Some(carried(ROWS, source));
    };
    let body = row_body(rec, text(source, "scope_key"), seq(source));
    let at = body.get("at_ms").and_then(Value::as_i64).unwrap_or(0);
    let target = TargetRecord::new(ROWS, source.id.clone(), body);
    Some(if at > 0 {
        target.created_ms(at)
    } else {
        target
    })
}

fn lesson(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(mut rec) = record(source) else {
        return Some(carried(LESSONS, source));
    };
    for counter in ["applied", "helped"] {
        if let Some(live) = source.body.get(counter) {
            rec.insert(counter.to_owned(), live.clone());
        }
    }
    let body = lesson_body(rec, text(source, "scope_key"), seq(source));
    Some(TargetRecord::new(LESSONS, source.id.clone(), body))
}

fn evidence(source: &SourceRecord) -> Option<TargetRecord> {
    let (lesson, row) = (text(source, "lesson_id"), text(source, "row_id"));
    if lesson.is_empty() || row.is_empty() {
        return Some(carried(EVIDENCE, source));
    }
    Some(TargetRecord::new(
        EVIDENCE,
        key(&[lesson, row], &source.id),
        source.body.clone(),
    ))
}

fn score(source: &SourceRecord) -> Option<TargetRecord> {
    let (scope, workflow) = (text(source, "scope_key"), text(source, "workflow_id"));
    if workflow.is_empty() {
        return Some(carried(SCORES, source));
    }
    Some(TargetRecord::new(
        SCORES,
        key(&[scope, workflow], &source.id),
        source.body.clone(),
    ))
}

fn variant(source: &SourceRecord) -> Option<TargetRecord> {
    let (scope, variant) = (text(source, "scope_key"), text(source, "variant"));
    if variant.is_empty() {
        return Some(carried(VARIANTS, source));
    }
    Some(TargetRecord::new(
        VARIANTS,
        key(&[scope, variant], &source.id),
        source.body.clone(),
    ))
}

fn episode(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(rec) = record(source) else {
        return Some(carried(EPISODES, source));
    };
    let scope = text(source, "scope_key");
    let id = rec
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let body = episode_body(rec, scope);
    let started = body.get("started_ms").and_then(Value::as_i64).unwrap_or(0);
    let target = TargetRecord::new(EPISODES, key(&[scope, &id], &source.id), body);
    Some(if started > 0 {
        target.created_ms(started)
    } else {
        target
    })
}

fn steps(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(rec) = record(source) else {
        return Some(carried(STEPS, source));
    };
    let (scope, row_id, n) = (
        text(source, "scope_key"),
        text(source, "row_id"),
        seq(source),
    );
    let body = step_body(rec, scope, row_id, n);
    Some(TargetRecord::new(
        STEPS,
        key(&[scope, row_id, &format!("{n:08}")], &source.id),
        body,
    ))
}

fn workflow(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(rec) = record(source) else {
        return Some(carried(WORKFLOWS, source));
    };
    let scope = text(source, "scope_key");
    let id = rec
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let target_key = key(&[scope, &id], &source.id);
    Some(TargetRecord::new(
        WORKFLOWS,
        target_key,
        workflow_body(rec, scope),
    ))
}

/// Move the v1 id sequences (`adaptive_counters`: `{seq}` per sequence name)
/// into `tsd.counters` (owner `flows`) for the scope `storage`, remove them,
/// and record the import in `tsd.migrations` (owner `flows`, step
/// `adaptive_counters_import`). New row and lesson ids keep counting from where
/// v1 stopped. Returns how many sequences were carried.
///
/// `tsd.counters` belongs to `tsd`, so a document step cannot write it. The
/// adaptive owner's `Storage::from_documents` open path calls this after its
/// schema migration. A completed import is skipped; with nothing to carry it
/// only records completion.
///
/// # Errors
///
/// The first storage error; the import resumes on the next call because
/// nothing is removed before the counters are raised.
pub async fn import_legacy_adaptive_counters(docs: &Arc<dyn DocumentStore>) -> Result<u64> {
    let migrations = Migrations::new(docs, COUNTER_OWNER);
    if migrations.done(COUNTER_OWNER, COUNTERS_STEP).await? {
        return Ok(0);
    }
    primitives::ensure(docs).await?;
    docs.ensure_collection(&tinyflows_storage::CollectionSpec::new(COUNTERS))
        .await?;
    let counters = tinyflows_storage::Counters::new(docs);
    let mut carried = 0_u64;
    let mut cursor: Option<Cursor> = None;
    loop {
        let mut query = Query::filter(Filter::default())
            .sort(Sort::asc(ID_FIELD))
            .limit(PAGE);
        if let Some(after) = cursor.take() {
            query = query.after(after);
        }
        let page = docs.query(COUNTERS, &query).await?;
        for found in page.items {
            let Some(last) = found.doc.get("seq").and_then(Value::as_u64) else {
                tracing::debug!(sequence = %found.id, "[adaptive-storage] counter import dropped a malformed row");
                continue;
            };
            counters
                .set_at_least(COUNTER_OWNER, &found.id, last)
                .await?;
            carried += 1;
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    docs.delete_where(COUNTERS, &Filter::default()).await?;
    migrations.mark_done(COUNTER_OWNER, COUNTERS_STEP).await?;
    tracing::debug!(carried, "[adaptive-storage] counters import complete");
    Ok(carried)
}

#[cfg(test)]
#[path = "migrate_tests.rs"]
mod tests;
