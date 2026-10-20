//! The cron store's schema v2 declarations and its v1 -> v2 steps.
//!
//! | v1 | v2 |
//! |---|---|
//! | `cron_jobs` | `cron.jobs`: `schedule`/`delivery`/`origin` become objects, `next_run`+`next_run_ns` become `next_run_ms`, `created_at`+`created_ms` become the record's creation metadata, `last_run` becomes `last_run_ms` |
//! | `cron_runs` | `cron.runs`: `started_at`+`started_ns` become `started_ms`, `finished_at` becomes `finished_ms` |
//! | `cron_counters` `admission` | `cron.admission` `ledger` (members as a list) |
//! | `cron_counters` `runs` | `tsd.counters` owner `cron` name `runs`, through [`import_legacy_cron_counters`] |
//! | embedded `occurrence_lease` | `tsd.leases` (owner `cron`, key `Key::of([job_id, occurrence_ms])`) |
//!
//! An embedded lease or completion receipt is not carried over (except the
//! number of the lease's queued placeholder run, kept as the job's
//! `queued_run` so the next claim removes the placeholder): a claim only
//! means something to the worker holding it in memory, which does not survive
//! the migration. The occurrence can be claimed again, so after an upgrade an
//! occurrence that was in flight may run once more (execution is at least once).
//!
//! `tsd.counters` belongs to `tsd`, so a document step cannot write it. The
//! step that reads `cron_counters` therefore files the run counter as
//! `cron.admission/runs_counter` (`{next}`), and [`import_legacy_cron_counters`]
//! moves it into `tsd.counters` once per scope after `Registry::migrate`.

use serde_json::{Map, Value, json};
use tinyflows_storage::{
    Counters, Migrations, Owner, Precondition, Result, Schema, ScopeClass, ScopedStorage,
    SourceRecord, Step, TargetRecord,
};

use super::admission::LEDGER;
use super::{ADMISSION, CronDocuments, JOBS, OWNER, RUN_COUNTER, RUNS, SCHEMA_VERSION};

/// Where the step files the v1 run counter until it is imported.
const STAGED_COUNTER: &str = "runs_counter";
const COUNTERS_STEP: &str = "counters_import";

/// The `cron` owner's declarations (`cron.jobs`, `cron.runs`,
/// `cron.admission`) and their v1 -> v2 steps. The host composes it with
/// `Registry::compose` and runs `Registry::migrate` per scope.
#[must_use]
#[allow(clippy::expect_used, reason = "`cron` is a valid owner name")]
pub fn storage_schema() -> Schema {
    let mut schema = Schema::new(Owner::new(OWNER).expect("`cron` is a valid owner"));
    for spec in CronDocuments::collections() {
        schema = schema.collection_with(SCHEMA_VERSION, spec, ScopeClass::Tenant);
    }
    schema
        .step(
            Step::new("jobs_v2", JOBS, 1, 2, |batch| Ok(map(batch, JOBS, job))).source("cron_jobs"),
        )
        .step(
            Step::new("runs_v2", RUNS, 1, 2, |batch| Ok(map(batch, RUNS, run))).source("cron_runs"),
        )
        .step(
            Step::new("admission_v2", ADMISSION, 1, 2, |batch| {
                Ok(batch.iter().filter_map(counter_record).collect())
            })
            .source("cron_counters"),
        )
}

/// Converts each record. One that does not convert is carried unchanged under
/// its v1 id (and logged), so it reads back as corrupt instead of vanishing
/// with its v1 source.
fn map(
    batch: Vec<SourceRecord>,
    collection: &str,
    convert: fn(&SourceRecord) -> Option<TargetRecord>,
) -> Vec<TargetRecord> {
    batch
        .iter()
        .map(|source| {
            convert(source).unwrap_or_else(|| {
                tracing::warn!(collection, id = %source.id, "[cron-storage] carrying unconvertible v1 record");
                TargetRecord::new(collection, source.id.clone(), source.body.clone())
            })
        })
        .collect()
}

fn instant_ms(rfc: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(rfc)
        .ok()
        .map(|at| at.timestamp_millis())
}

/// A v1 JSON string parsed into the value it holds.
fn parsed(raw: &Value) -> Option<Value> {
    match raw {
        Value::String(text) => serde_json::from_str(text).ok(),
        other => Some(other.clone()),
    }
}

/// A v1 schedule with a one-shot's `at` turned into `at_ms`.
fn schedule(raw: &Value) -> Option<Value> {
    let mut value = parsed(raw)?;
    if let Some(object) = value.as_object_mut()
        && object.get("kind").and_then(Value::as_str) == Some("at")
        && let Some(at) = object.remove("at")
    {
        object.insert("at_ms".into(), instant_ms(at.as_str()?)?.into());
    }
    Some(value)
}

fn job(source: &SourceRecord) -> Option<TargetRecord> {
    let body = &source.body;
    let mut out = Map::new();
    for field in [
        "claim_generation",
        "policy",
        "expression",
        "command",
        "job_type",
        "session_target",
        "enabled",
        "delete_after_run",
        "prompt",
        "name",
        "model",
        "agent_id",
        "last_status",
        "last_output",
        "incarnation",
    ] {
        if let Some(value) = body.get(field) {
            out.insert(field.to_owned(), value.clone());
        }
    }
    // The queued placeholder run of a v1 claim outlives the lease it hung on,
    // so it stays on the job for the next acquisition to remove.
    if let Some(queued) = body
        .get("occurrence_lease")
        .and_then(|lease| lease.get("queued_run"))
        .filter(|queued| queued.is_i64() || queued.is_u64())
    {
        out.insert("queued_run".into(), queued.clone());
    }
    out.insert("schedule".into(), schedule(body.get("schedule")?)?);
    if let Some(delivery) = body.get("delivery").and_then(parsed) {
        out.insert("delivery".into(), delivery);
    }
    if let Some(origin) = body.get("origin").and_then(parsed) {
        out.insert("origin".into(), origin);
    }
    let next_run = body
        .get("next_run_ms")
        .and_then(Value::as_i64)
        .or_else(|| {
            body.get("next_run")
                .and_then(Value::as_str)
                .and_then(instant_ms)
        })?;
    out.insert("next_run_ms".into(), next_run.into());
    if let Some(last) = body
        .get("last_run")
        .and_then(Value::as_str)
        .and_then(instant_ms)
    {
        out.insert("last_run_ms".into(), last.into());
    }
    let created = body
        .get("created_at")
        .and_then(Value::as_str)
        .and_then(instant_ms)
        .or_else(|| body.get("created_ms").and_then(Value::as_i64))
        .unwrap_or(source.meta.created_ms);
    let record = TargetRecord::new(JOBS, source.id.clone(), Value::Object(out));
    Some(if created > 0 {
        record.created_ms(created)
    } else {
        record
    })
}

fn run(source: &SourceRecord) -> Option<TargetRecord> {
    let body = &source.body;
    let mut out = Map::new();
    for field in [
        "seq",
        "job_id",
        "status",
        "duration_ms",
        "incarnation",
        "output",
        "delivery_status",
    ] {
        if let Some(value) = body.get(field) {
            out.insert(field.to_owned(), value.clone());
        }
    }
    let ms = |rfc: &str, ns: &str| {
        body.get(rfc)
            .and_then(Value::as_str)
            .and_then(instant_ms)
            .or_else(|| body.get(ns).and_then(Value::as_i64).map(|n| n / 1_000_000))
    };
    let started = ms("started_at", "started_ns")?;
    out.insert("started_ms".into(), started.into());
    out.insert(
        "finished_ms".into(),
        ms("finished_at", "finished_ns")?.into(),
    );
    Some(TargetRecord::new(RUNS, source.id.clone(), Value::Object(out)).created_ms(started))
}

/// `cron_counters` holds two documents: the run counter and the admission
/// ledger. The ledger moves to `cron.admission/ledger`; the counter is staged.
fn counter_record(source: &SourceRecord) -> Option<TargetRecord> {
    match source.id.as_str() {
        "admission" => {
            let members: Map<String, Value> = parsed(source.body.get("members_json")?)?
                .as_object()?
                .clone();
            let members: Vec<Value> = members
                .into_iter()
                .map(|(job_id, member)| {
                    let mut member = member.as_object().cloned().unwrap_or_default();
                    member.insert("job_id".into(), job_id.into());
                    Value::Object(member)
                })
                .collect();
            let mut out = Map::new();
            if let Some(limit) = source.body.get("limit").filter(|limit| !limit.is_null()) {
                out.insert("limit".into(), limit.clone());
            }
            out.insert("members".into(), Value::Array(members));
            Some(TargetRecord::new(ADMISSION, LEDGER, Value::Object(out)))
        }
        "runs" => Some(TargetRecord::new(
            ADMISSION,
            STAGED_COUNTER,
            json!({ "next": source.body.get("next")?.as_i64()? }),
        )),
        _ => None,
    }
}

/// Move the staged v1 run counter (`cron.admission/runs_counter`) into
/// `tsd.counters` (owner `cron`, name `runs`) for the scope `storage`, remove
/// it, and record the import in `tsd.migrations` (owner `cron`, step
/// `counters_import`). Run numbers keep counting from where v1 stopped.
/// Returns whether a counter was carried.
///
/// A completed import is skipped; with nothing staged it only records
/// completion.
///
/// # Errors
///
/// The first storage error; the import resumes on the next call.
pub async fn import_legacy_cron_counters(storage: &ScopedStorage) -> Result<bool> {
    let migrations = Migrations::new(storage);
    if migrations.done(OWNER, COUNTERS_STEP).await? {
        return Ok(false);
    }
    tinyflows_storage::primitives::ensure(storage).await?;
    let docs = storage.documents();
    docs.ensure_collection(&CronDocuments::collections().remove(2))
        .await?;
    let mut carried = false;
    if let Some(staged) = docs.get(ADMISSION, STAGED_COUNTER).await? {
        // v1 stored the next number to hand out; the counter holds the last one.
        let next = staged.doc.get("next").and_then(Value::as_u64).unwrap_or(1);
        Counters::new(docs)
            .set_at_least(OWNER, RUN_COUNTER, next.saturating_sub(1))
            .await?;
        docs.delete(ADMISSION, STAGED_COUNTER, Precondition::None)
            .await?;
        carried = true;
    }
    migrations.mark_done(OWNER, COUNTERS_STEP).await?;
    tracing::debug!(carried, "[cron-storage] counters import complete");
    Ok(carried)
}

#[cfg(test)]
#[path = "migrate_tests.rs"]
mod tests;
