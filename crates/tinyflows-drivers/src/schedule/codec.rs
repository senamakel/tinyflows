//! Jobs and runs to and from documents.
//!
//! Optional fields are left out of a document rather than stored as `null`;
//! nested values (`schedule`, `delivery`, `origin`) are JSON strings. A job
//! document that cannot be read back is an error naming the field, never a
//! silently defaulted job.

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use tinyflows_schedule::{
    CronJob, CronRun, DeliveryConfig, DeliveryStatus, JobOrigin, JobType, Schedule, SessionTarget,
};
use tinystoragedrivers_core::Versioned;

/// The string field `field` of `doc`.
pub(super) fn text<'a>(doc: &'a Value, field: &str) -> Option<&'a str> {
    doc.get(field).and_then(Value::as_str)
}

fn required<'a>(doc: &'a Value, field: &str) -> Result<&'a str> {
    text(doc, field).ok_or_else(|| anyhow!("cron store: document has no `{field}`"))
}

fn instant(raw: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("Invalid RFC3339 timestamp in cron store: {raw}"))?
        .with_timezone(&Utc))
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

/// The document for `job`.
pub(super) fn job_to_doc(job: &CronJob) -> Result<Value> {
    let mut doc = Map::new();
    doc.insert("expression".into(), json!(job.expression));
    doc.insert("command".into(), json!(job.command));
    doc.insert(
        "schedule".into(),
        json!(serde_json::to_string(&job.schedule).context("serialize cron schedule")?),
    );
    doc.insert("job_type".into(), json!(job.job_type.as_str()));
    doc.insert("session_target".into(), json!(job.session_target.as_str()));
    doc.insert("enabled".into(), json!(job.enabled));
    doc.insert(
        "delivery".into(),
        json!(serde_json::to_string(&job.delivery).context("serialize cron delivery")?),
    );
    doc.insert("delete_after_run".into(), json!(job.delete_after_run));
    doc.insert("created_at".into(), json!(job.created_at.to_rfc3339()));
    doc.insert(
        "created_ms".into(),
        json!(job.created_at.timestamp_millis()),
    );
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
            .map(|origin| serde_json::to_string(origin).map(Value::String))
            .transpose()
            .context("Failed to serialize cron origin")?,
    );
    set(
        &mut doc,
        "last_run",
        job.last_run.map(|at| json!(at.to_rfc3339())),
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

/// Writes `next_run` in both its readable and its ordering form.
pub(super) fn set_next_run(doc: &mut Map<String, Value>, next_run: DateTime<Utc>) {
    doc.insert("next_run".into(), json!(next_run.to_rfc3339()));
    doc.insert("next_run_ms".into(), json!(next_run.timestamp_millis()));
}

/// Records a run's outcome on a job document.
pub(super) fn set_last_run(
    doc: &mut Map<String, Value>,
    finished_at: DateTime<Utc>,
    success: bool,
    output: String,
) {
    doc.insert("last_run".into(), json!(finished_at.to_rfc3339()));
    doc.insert(
        "last_status".into(),
        json!(if success { "ok" } else { "error" }),
    );
    doc.insert("last_output".into(), json!(output));
}

/// The job a stored document holds.
pub(super) fn doc_to_job(stored: &Versioned<Value>) -> Result<CronJob> {
    let doc = &stored.doc;
    let schedule: Schedule = serde_json::from_str(required(doc, "schedule")?)
        .context("Failed to parse cron schedule JSON")?;
    let delivery: DeliveryConfig = match text(doc, "delivery") {
        Some(raw) => serde_json::from_str(raw).context("Failed to parse cron delivery JSON")?,
        None => DeliveryConfig::default(),
    };
    let origin: Option<JobOrigin> = text(doc, "origin")
        .map(serde_json::from_str)
        .transpose()
        .context("Failed to parse cron origin JSON")?;
    let flag = |field: &str| doc.get(field).and_then(Value::as_bool).unwrap_or(false);
    Ok(CronJob {
        id: stored.id.clone(),
        expression: text(doc, "expression").unwrap_or_default().to_string(),
        schedule,
        command: text(doc, "command").unwrap_or_default().to_string(),
        prompt: text(doc, "prompt").map(str::to_string),
        name: text(doc, "name").map(str::to_string),
        job_type: JobType::parse(required(doc, "job_type")?),
        session_target: SessionTarget::parse(text(doc, "session_target").unwrap_or("isolated")),
        model: text(doc, "model").map(str::to_string),
        agent_id: text(doc, "agent_id").map(str::to_string),
        enabled: flag("enabled"),
        delivery,
        delete_after_run: flag("delete_after_run"),
        created_at: instant(required(doc, "created_at")?)?,
        next_run: instant(required(doc, "next_run")?)?,
        last_run: text(doc, "last_run").map(instant).transpose()?,
        last_status: text(doc, "last_status").map(str::to_string),
        last_output: text(doc, "last_output").map(str::to_string),
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
    doc.insert("started_at".into(), json!(started_at.to_rfc3339()));
    doc.insert("started_ms".into(), json!(started_at.timestamp_millis()));
    doc.insert("finished_at".into(), json!(finished_at.to_rfc3339()));
    doc.insert("status".into(), json!(status));
    doc.insert("duration_ms".into(), json!(duration_ms));
    set(&mut doc, "output", output.map(Value::String));
    set(
        &mut doc,
        "delivery_status",
        delivery_status.map(|status| json!(status.as_str())),
    );
    Value::Object(doc)
}

/// The run a stored document holds. A delivery status this build does not
/// know reads as `None`, as it does in the SQLite store.
pub(super) fn doc_to_run(stored: &Versioned<Value>) -> Result<CronRun> {
    let doc = &stored.doc;
    Ok(CronRun {
        id: doc
            .get("seq")
            .and_then(Value::as_i64)
            .ok_or_else(|| anyhow!("cron store: run document has no `seq`"))?,
        job_id: required(doc, "job_id")?.to_string(),
        started_at: instant(required(doc, "started_at")?)?,
        finished_at: instant(required(doc, "finished_at")?)?,
        status: required(doc, "status")?.to_string(),
        output: text(doc, "output").map(str::to_string),
        duration_ms: doc.get("duration_ms").and_then(Value::as_i64),
        delivery_status: text(doc, "delivery_status").and_then(DeliveryStatus::parse),
    })
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
