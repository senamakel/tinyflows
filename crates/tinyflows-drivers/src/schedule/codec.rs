//! Jobs and runs to and from documents (storage schema v2, owner `cron`).
//!
//! Bodies are bare snake_case objects. Nested values (`schedule`, `delivery`,
//! `origin`, `policy`) are real objects, not JSON strings; every instant is one
//! `_ms` field (`next_run_ms`, `last_run_ms`, `started_ms`, `finished_ms`), and
//! a job's creation time is the document's `created_ms` metadata, not a body
//! field. Optional fields are left out rather than stored as `null`. A job
//! document that cannot be read back is an error naming the field, never a
//! silently defaulted job.

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use tinyflows_schedule::{
    CronJob, CronRun, DeliveryConfig, DeliveryStatus, JobOrigin, JobType, Schedule, SessionTarget,
};
use tinyflows_storage::Versioned;

/// The string field `field` of `doc`.
pub(super) fn text<'a>(doc: &'a Value, field: &str) -> Option<&'a str> {
    doc.get(field).and_then(Value::as_str)
}

fn required<'a>(doc: &'a Value, field: &str) -> Result<&'a str> {
    optional(doc, field)?.ok_or_else(|| anyhow!("cron store: document has no `{field}`"))
}

/// The string field `field` of `doc`: `None` when absent, an error when
/// present with another type, so a malformed document is never read as a
/// default.
fn optional<'a>(doc: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match doc.get(field) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(anyhow!("cron store: `{field}` is not a string")),
    }
}

/// A stored job type. Unknown values are an error rather than a shell job:
/// reading a tampered `job_type` as `shell` would run its `command`.
fn job_type(raw: &str) -> Result<JobType> {
    [JobType::Shell, JobType::Agent, JobType::Flow]
        .into_iter()
        .find(|known| known.as_str() == raw)
        .ok_or_else(|| anyhow!("cron store: unknown job type `{raw}`"))
}

/// The field holding a job's incarnation: a fresh id per created job, carried
/// by its runs, so a run written late for a removed job never attaches to a
/// later job that reuses its id (a flow's schedule job does).
pub(super) const INCARNATION: &str = "incarnation";

/// `doc`'s incarnation, when it has one.
pub(super) fn incarnation(doc: &Value) -> Option<&str> {
    text(doc, INCARNATION)
}

/// `ms` (epoch milliseconds) as an instant.
pub(super) fn from_ms(ms: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .ok_or_else(|| anyhow!("cron store: {ms} is not a valid instant"))
}

fn set(doc: &mut Map<String, Value>, field: &str, value: Option<Value>) {
    match value {
        Some(value) => {
            doc.insert(field.to_string(), value);
        }
        None => {
            doc.remove(field);
        }
    }
}

/// `schedule` as a storage value. Identical to its wire form except that a
/// one-shot's instant is `at_ms`, never a formatted date.
pub(super) fn schedule_to_value(schedule: &Schedule) -> Result<Value> {
    match schedule {
        Schedule::At { at } => Ok(json!({ "kind": "at", "at_ms": at.timestamp_millis() })),
        other => serde_json::to_value(other).context("serialize cron schedule"),
    }
}

/// The schedule a stored value holds.
pub(super) fn schedule_from_value(value: &Value) -> Result<Schedule> {
    if text(value, "kind") == Some("at") {
        let ms = integer(value, "at_ms")?.ok_or_else(|| anyhow!("cron schedule has no `at_ms`"))?;
        return Ok(Schedule::At { at: from_ms(ms)? });
    }
    serde_json::from_value(value.clone()).context("Failed to parse cron schedule")
}

/// The document for `job`. Its creation time is not part of the body.
pub(super) fn job_to_doc(job: &CronJob) -> Result<Value> {
    let mut doc = Map::new();
    doc.insert("claim_generation".into(), json!(job.claim_generation));
    if !job.policy.is_default() {
        doc.insert("policy".into(), serde_json::to_value(job.policy)?);
    }
    doc.insert("expression".into(), json!(job.expression));
    doc.insert("command".into(), json!(job.command));
    doc.insert("schedule".into(), schedule_to_value(&job.schedule)?);
    doc.insert("job_type".into(), json!(job.job_type.as_str()));
    doc.insert("session_target".into(), json!(job.session_target.as_str()));
    doc.insert("enabled".into(), json!(job.enabled));
    doc.insert(
        "delivery".into(),
        serde_json::to_value(&job.delivery).context("serialize cron delivery")?,
    );
    doc.insert("delete_after_run".into(), json!(job.delete_after_run));
    set_next_run(&mut doc, job.next_run);
    set(&mut doc, "prompt", job.prompt.as_ref().map(|v| json!(v)));
    set(&mut doc, "name", job.name.as_ref().map(|v| json!(v)));
    set(&mut doc, "model", job.model.as_ref().map(|v| json!(v)));
    set(
        &mut doc,
        "agent_id",
        job.agent_id.as_ref().map(|v| json!(v)),
    );
    set(
        &mut doc,
        "origin",
        job.origin
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .context("Failed to serialize cron origin")?,
    );
    set(
        &mut doc,
        "last_run_ms",
        job.last_run.map(|at| json!(at.timestamp_millis())),
    );
    set(
        &mut doc,
        "last_status",
        job.last_status.as_ref().map(|v| json!(v)),
    );
    set(
        &mut doc,
        "last_output",
        job.last_output.as_ref().map(|v| json!(v)),
    );
    Ok(Value::Object(doc))
}

/// Writes `next_run` as its one field, `next_run_ms`.
pub(super) fn set_next_run(doc: &mut Map<String, Value>, next_run: DateTime<Utc>) {
    doc.insert("next_run_ms".into(), json!(next_run.timestamp_millis()));
}

/// Records a run's outcome on a job document.
pub(super) fn set_last_run(
    doc: &mut Map<String, Value>,
    finished_at: DateTime<Utc>,
    success: bool,
    output: String,
) {
    doc.insert("last_run_ms".into(), json!(finished_at.timestamp_millis()));
    doc.insert(
        "last_status".into(),
        json!(if success { "ok" } else { "error" }),
    );
    doc.insert("last_output".into(), json!(output));
}

/// The job a stored document holds. Its creation time is the document's
/// `created_ms` metadata.
pub(super) fn doc_to_job(stored: &Versioned<Value>) -> Result<CronJob> {
    let doc = &stored.doc;
    let schedule = schedule_from_value(
        doc.get("schedule")
            .ok_or_else(|| anyhow!("cron store: document has no `schedule`"))?,
    )?;
    let delivery: DeliveryConfig = match doc.get("delivery") {
        Some(raw) => {
            serde_json::from_value(raw.clone()).context("Failed to parse cron delivery")?
        }
        None => DeliveryConfig::default(),
    };
    let origin: Option<JobOrigin> = doc
        .get("origin")
        .map(|raw| serde_json::from_value(raw.clone()))
        .transpose()
        .context("Failed to parse cron origin")?;
    let flag = |field: &str| match doc.get(field) {
        None => Ok(false),
        Some(Value::Bool(value)) => Ok(*value),
        Some(_) => Err(anyhow!("cron store: `{field}` is not a boolean")),
    };
    let owned = |field: &str| optional(doc, field).map(|value| value.map(str::to_string));
    Ok(CronJob {
        claim_generation: doc
            .get("claim_generation")
            .map(|v| serde_json::from_value(v.clone()))
            .transpose()?
            .unwrap_or_default(),
        policy: doc
            .get("policy")
            .map(|p| serde_json::from_value(p.clone()))
            .transpose()?
            .unwrap_or_default(),
        id: stored.id.clone(),
        expression: required(doc, "expression")?.to_string(),
        schedule,
        command: required(doc, "command")?.to_string(),
        prompt: owned("prompt")?,
        name: owned("name")?,
        job_type: job_type(required(doc, "job_type")?)?,
        session_target: SessionTarget::parse(
            optional(doc, "session_target")?.unwrap_or("isolated"),
        ),
        model: owned("model")?,
        agent_id: owned("agent_id")?,
        enabled: flag("enabled")?,
        delivery,
        delete_after_run: flag("delete_after_run")?,
        created_at: from_ms(stored.meta.created_ms)?,
        next_run: from_ms(
            integer(doc, "next_run_ms")?
                .ok_or_else(|| anyhow!("cron store: document has no `next_run_ms`"))?,
        )?,
        last_run: integer(doc, "last_run_ms")?.map(from_ms).transpose()?,
        last_status: owned("last_status")?,
        last_output: owned("last_output")?,
        origin,
    })
}

/// The document id of run number `seq`: zero-padded, so ids sort as numbers.
pub(super) fn run_id(seq: i64) -> String {
    format!("{seq:020}")
}

/// The document for one run.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_to_doc(
    seq: i64,
    job_id: &str,
    run_id: Option<&str>,
    incarnation: Option<&str>,
    started_at: DateTime<Utc>,
    finished_at: DateTime<Utc>,
    status: &str,
    output: Option<String>,
    duration_ms: i64,
    delivery_status: Option<&DeliveryStatus>,
) -> Value {
    let mut doc = Map::new();
    doc.insert("seq".into(), json!(seq));
    doc.insert("job_id".into(), json!(job_id));
    set(&mut doc, "run_id", run_id.map(|value| json!(value)));
    doc.insert("started_ms".into(), json!(started_at.timestamp_millis()));
    doc.insert("finished_ms".into(), json!(finished_at.timestamp_millis()));
    doc.insert("status".into(), json!(status));
    doc.insert("duration_ms".into(), json!(duration_ms));
    set(&mut doc, INCARNATION, incarnation.map(|value| json!(value)));
    set(&mut doc, "output", output.map(Value::String));
    set(
        &mut doc,
        "delivery_status",
        delivery_status.map(|status| json!(status.as_str())),
    );
    Value::Object(doc)
}

/// The run a stored document holds.
///
/// A field of the wrong type is an error naming it, never a default. A
/// delivery status string this build does not know reads as `None`, as it
/// does in the SQLite store.
pub(super) fn doc_to_run(stored: &Versioned<Value>) -> Result<CronRun> {
    let doc = &stored.doc;
    let instant = |field: &str| -> Result<DateTime<Utc>> {
        from_ms(integer(doc, field)?.ok_or_else(|| anyhow!("cron store: run has no `{field}`"))?)
    };
    Ok(CronRun {
        id: integer(doc, "seq")?.ok_or_else(|| anyhow!("cron store: run document has no `seq`"))?,
        job_id: required(doc, "job_id")?.to_string(),
        run_id: optional(doc, "run_id")?.map(str::to_string),
        started_at: instant("started_ms")?,
        finished_at: instant("finished_ms")?,
        status: required(doc, "status")?.to_string(),
        output: optional(doc, "output")?.map(str::to_string),
        duration_ms: integer(doc, "duration_ms")?,
        delivery_status: optional(doc, "delivery_status")?.and_then(DeliveryStatus::parse),
    })
}

/// The integer field `field` of `doc`: `None` when absent, an error when
/// present with another type.
pub(super) fn integer(doc: &Value, field: &str) -> Result<Option<i64>> {
    match doc.get(field) {
        None => Ok(None),
        Some(value) => value
            .as_i64()
            .map(Some)
            .ok_or_else(|| anyhow!("cron store: `{field}` is not an integer")),
    }
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
