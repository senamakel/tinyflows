//! The workflow store's schema v2 declarations and its v1 -> v2 steps.
//!
//! | v1 | v2 |
//! |---|---|
//! | `wf_definitions` | `flows.workflows`: `record_json` becomes the record's own fields; id `Key::of([record id])` (a hashed `h:` id is re-keyed from the record) |
//! | `wf_revisions` | `flows.workflow_revisions`: the workflow's fields beside `workflow_id` and `superseded_ms`; the record's creation time is the supersession time |
//! | `wf_runs` | `flows.workflow_runs`: `run_json` becomes a snake_case object, `*_at` become `*_ms`, `startedAtSecs` becomes `process_started_ms` |
//! | `wf_notes` | `flows.workflow_notes`: `notes_json` becomes a `notes` list |
//! | `wf_proposals` | `flows.workflow_proposals`: `proposal_json` becomes a snake_case object |
//! | `wf_quarantine` | `flows.workflow_quarantine`, carried verbatim (it is unreadable by definition) |
//! | `wf_locks` | `tsd.leases`: not carried over. A lease only means something to the holder waiting in memory, and lasts two minutes |
//!
//! A record whose JSON string does not parse is carried as it was, under its
//! v1 id, so a corrupt record stays corrupt and visible (readers skip it, and a
//! journal that does not parse is quarantined on first read) instead of being
//! lost.

use serde_json::{Map, Value, json};
use tinyflows_storage::{Key, Schema, ScopeClass, SourceRecord, Step, TargetRecord};

use super::codec::to_stored;
use super::{
    DEFINITIONS, DriverWorkflowStore, NOTES, PROPOSALS, QUARANTINE, REVISIONS, RUNS, SCHEMA_VERSION,
};

/// Declares the workflow store's collections on `schema` (owner `flows`) with
/// their steps.
pub(crate) fn extend_schema(mut schema: Schema) -> Schema {
    for spec in DriverWorkflowStore::collections() {
        schema = schema.collection_with(SCHEMA_VERSION, spec, ScopeClass::Tenant);
    }
    schema
        .step(step(
            "workflows_v2",
            DEFINITIONS,
            "wf_definitions",
            definition,
        ))
        .step(step(
            "workflow_revisions_v2",
            REVISIONS,
            "wf_revisions",
            revision,
        ))
        .step(step("workflow_runs_v2", RUNS, "wf_runs", run))
        .step(step("workflow_notes_v2", NOTES, "wf_notes", journal))
        .step(step(
            "workflow_proposals_v2",
            PROPOSALS,
            "wf_proposals",
            proposal,
        ))
        .step(step(
            "workflow_quarantine_v2",
            QUARANTINE,
            "wf_quarantine",
            quarantine,
        ))
}

type Convert = fn(&SourceRecord) -> Option<TargetRecord>;

fn step(id: &'static str, item: &'static str, source: &'static str, convert: Convert) -> Step {
    Step::new(id, item, 1, 2, move |batch| {
        Ok(batch.iter().filter_map(convert).collect())
    })
    .source(source)
}

/// The JSON a v1 string field holds, when it is an object.
fn parsed_object(body: &Value, field: &str) -> Option<Map<String, Value>> {
    match serde_json::from_str(body.get(field)?.as_str()?).ok()? {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

/// `id` as a v2 document id, or `fallback` when it is not a usable key.
fn key(id: &str, fallback: &str) -> String {
    Key::of([id]).map_or_else(|_| fallback.to_owned(), |key| key.as_str().to_owned())
}

/// The record carried unchanged under its v1 id.
fn carried(collection: &str, source: &SourceRecord) -> TargetRecord {
    TargetRecord::new(collection, source.id.clone(), source.body.clone())
        .preserve_expiry_from(source)
}

fn str_field<'a>(map: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    map.get(field).and_then(Value::as_str)
}

fn definition(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(mut record) = parsed_object(&source.body, "record_json") else {
        return Some(carried(DEFINITIONS, source));
    };
    let Some(id) = str_field(&record, "id").map(str::to_owned) else {
        return Some(carried(DEFINITIONS, source));
    };
    record.remove("source_path");
    Some(
        TargetRecord::new(
            DEFINITIONS,
            key(&id, &source.id),
            to_stored(Value::Object(record)),
        )
        .preserve_expiry_from(source),
    )
}

fn revision(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(mut record) = parsed_object(&source.body, "record_json") else {
        return Some(carried(REVISIONS, source));
    };
    let Some(workflow_id) = source
        .body
        .get("workflow_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| str_field(&record, "id").map(str::to_owned))
    else {
        return Some(carried(REVISIONS, source));
    };
    let superseded = source
        .body
        .get("superseded_at")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    record.remove("source_path");
    record.remove("id");
    let Value::Object(mut body) = to_stored(Value::Object(record)) else {
        return None;
    };
    body.insert("workflow_id".into(), json!(workflow_id));
    body.insert("superseded_ms".into(), json!(superseded));
    Some(
        TargetRecord::new(REVISIONS, source.id.clone(), Value::Object(body))
            .preserve_expiry_from(source),
    )
}

fn run(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(run) = parsed_object(&source.body, "run_json") else {
        return Some(carried(RUNS, source));
    };
    let Some(id) = str_field(&run, "id").map(str::to_owned) else {
        return Some(carried(RUNS, source));
    };
    let settled = source
        .body
        .get("settled")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| {
            !matches!(
                str_field(&run, "status"),
                Some("running" | "pending_approval")
            )
        });
    let Value::Object(mut body) = to_stored(Value::Object(run)) else {
        return None;
    };
    body.insert("settled".into(), json!(settled));
    Some(
        TargetRecord::new(RUNS, key(&id, &source.id), Value::Object(body))
            .preserve_expiry_from(source),
    )
}

fn journal(source: &SourceRecord) -> Option<TargetRecord> {
    let notes = source
        .body
        .get("notes_json")
        .and_then(Value::as_str)
        .and_then(|raw| serde_json::from_str::<Vec<Value>>(raw).ok());
    let workflow_id = source.body.get("workflow_id").and_then(Value::as_str);
    let (Some(notes), Some(workflow_id)) = (notes, workflow_id) else {
        return Some(carried(NOTES, source));
    };
    let notes: Vec<Value> = notes.into_iter().map(to_stored).collect();
    Some(
        TargetRecord::new(
            NOTES,
            key(workflow_id, &source.id),
            json!({ "workflow_id": workflow_id, "notes": notes }),
        )
        .preserve_expiry_from(source),
    )
}

fn proposal(source: &SourceRecord) -> Option<TargetRecord> {
    let Some(proposal) = parsed_object(&source.body, "proposal_json") else {
        return Some(carried(PROPOSALS, source));
    };
    let Some(id) = str_field(&proposal, "id").map(str::to_owned) else {
        return Some(carried(PROPOSALS, source));
    };
    Some(
        TargetRecord::new(
            PROPOSALS,
            key(&id, &source.id),
            to_stored(Value::Object(proposal)),
        )
        .preserve_expiry_from(source),
    )
}

fn quarantine(source: &SourceRecord) -> Option<TargetRecord> {
    Some(carried(QUARANTINE, source))
}
