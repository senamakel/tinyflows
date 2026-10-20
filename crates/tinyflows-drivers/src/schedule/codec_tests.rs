use super::*;

use serde_json::json;
use tinyflows_storage::{Meta, Version};

fn stored(doc: Value) -> Versioned<Value> {
    Versioned {
        id: "x".into(),
        version: Version::FIRST,
        doc,
        meta: Meta {
            created_ms: 1_000,
            ..Meta::unknown()
        },
    }
}

fn job() -> CronJob {
    let now = Utc::now();
    CronJob {
        claim_generation: 0,
        policy: Default::default(),
        id: "x".into(),
        expression: "0 9 * * *".into(),
        schedule: Schedule::Cron {
            expr: "0 9 * * *".into(),
            tz: None,
            active_hours: None,
        },
        command: "ls".into(),
        prompt: None,
        name: None,
        job_type: JobType::Shell,
        session_target: SessionTarget::Isolated,
        model: None,
        agent_id: None,
        enabled: true,
        delivery: DeliveryConfig::default(),
        delete_after_run: false,
        created_at: now,
        next_run: now,
        last_run: None,
        last_status: None,
        last_output: None,
        origin: None,
    }
}

#[test]
fn absent_optionals_are_left_out_not_null() {
    let doc = job_to_doc(&job()).unwrap();
    for field in [
        "prompt",
        "name",
        "model",
        "agent_id",
        "origin",
        "last_run_ms",
    ] {
        assert!(doc.get(field).is_none(), "{field}");
    }
    assert!(doc["schedule"].is_object() && doc["delivery"].is_object());
    assert!(doc.get("created_ms").is_none() && doc.get("created_at").is_none());
    let read = doc_to_job(&stored(doc)).unwrap();
    assert_eq!(read.command, "ls");
    assert_eq!(read.delivery, DeliveryConfig::default());
}

#[test]
fn a_missing_delivery_reads_as_the_default() {
    let mut doc = job_to_doc(&job()).unwrap();
    doc.as_object_mut().unwrap().remove("delivery");
    assert_eq!(
        doc_to_job(&stored(doc)).unwrap().delivery,
        DeliveryConfig::default()
    );
}

#[test]
fn malformed_fields_are_errors() {
    let mut doc = job_to_doc(&job()).unwrap();
    doc["origin"] = json!("{not json");
    assert!(doc_to_job(&stored(doc)).is_err());
    let mut doc = job_to_doc(&job()).unwrap();
    doc["next_run_ms"] = json!("yesterday");
    assert!(doc_to_job(&stored(doc)).is_err());
    let run = run_to_doc(
        1,
        "j",
        None,
        None,
        Utc::now(),
        Utc::now(),
        "ok",
        None,
        1,
        None,
    );
    let mut broken = run.clone();
    broken.as_object_mut().unwrap().remove("seq");
    assert!(doc_to_run(&stored(broken)).is_err());
    assert_eq!(doc_to_run(&stored(run)).unwrap().id, 1);
}

#[test]
fn run_history_identity_roundtrips_and_legacy_rows_remain_valid() {
    let now = Utc::now();
    let run = run_to_doc(
        1,
        "job",
        Some("invocation-1"),
        None,
        now,
        now,
        "ok",
        None,
        0,
        None,
    );
    assert_eq!(
        doc_to_run(&stored(run.clone())).unwrap().run_id.as_deref(),
        Some("invocation-1")
    );

    let mut legacy = run;
    legacy.as_object_mut().unwrap().remove("run_id");
    assert_eq!(doc_to_run(&stored(legacy)).unwrap().run_id, None);
}

#[test]
fn run_ids_sort_as_numbers() {
    assert!(run_id(9) < run_id(10));
    assert_eq!(run_id(1).len(), 20);
}

#[test]
fn wrong_types_and_unknown_job_types_are_errors() {
    for (field, value) in [
        ("job_type", json!("bogus")),
        ("delivery", json!(7)),
        ("origin", json!(7)),
        ("name", json!(1)),
        ("enabled", json!("yes")),
    ] {
        let mut doc = job_to_doc(&job()).unwrap();
        doc[field] = value;
        assert!(doc_to_job(&stored(doc)).is_err(), "{field}");
    }
    for field in ["expression", "command"] {
        let mut doc = job_to_doc(&job()).unwrap();
        doc.as_object_mut().unwrap().remove(field);
        assert!(doc_to_job(&stored(doc)).is_err(), "{field}");
    }
}

#[test]
fn creation_time_is_the_records_metadata_not_a_body_field() {
    let doc = job_to_doc(&job()).unwrap();
    let read = doc_to_job(&stored(doc)).unwrap();
    assert_eq!(read.created_at.timestamp_millis(), 1_000);
}

#[test]
fn a_one_shot_schedule_stores_its_instant_as_milliseconds() {
    let at = DateTime::parse_from_rfc3339("2030-01-02T03:04:05.678Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut value = job();
    value.schedule = Schedule::At { at };
    let doc = job_to_doc(&value).unwrap();
    assert_eq!(
        doc["schedule"],
        json!({"kind": "at", "at_ms": at.timestamp_millis()})
    );
    assert!(matches!(
        doc_to_job(&stored(doc)).unwrap().schedule,
        Schedule::At { at: read } if read == at
    ));
}

#[test]
fn wrong_typed_run_fields_are_errors() {
    let good = json!({
        "seq": 1, "job_id": "j", "started_ms": 1_767_225_600_000_i64,
        "finished_ms": 1_767_225_601_000_i64, "status": "ok",
    });
    assert!(doc_to_run(&stored(good.clone())).is_ok());
    for (field, bad) in [
        ("output", json!(7)),
        ("duration_ms", json!("12")),
        ("delivery_status", json!(true)),
        ("seq", json!("1")),
    ] {
        let mut doc = good.clone();
        doc[field] = bad;
        let error = doc_to_run(&stored(doc)).unwrap_err().to_string();
        assert!(error.contains(field), "{field}: {error}");
    }
    let mut unknown = good;
    unknown["delivery_status"] = json!("teleported");
    assert!(
        doc_to_run(&stored(unknown))
            .unwrap()
            .delivery_status
            .is_none()
    );
}

#[test]
fn next_run_is_written_as_one_millisecond_field() {
    let mut doc = serde_json::Map::new();
    let at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456789Z")
        .unwrap()
        .with_timezone(&Utc);
    set_next_run(&mut doc, at);
    assert_eq!(doc["next_run_ms"], json!(at.timestamp_millis()));
    assert_eq!(doc.len(), 1, "no ns or RFC 3339 copy");
}

#[test]
fn legacy_documents_default_claim_generation_and_keep_newer_generations() {
    let mut value = job();
    value.claim_generation = 17;
    let mut doc = job_to_doc(&value).unwrap();
    assert_eq!(
        doc_to_job(&stored(doc.clone())).unwrap().claim_generation,
        17
    );
    doc.as_object_mut().unwrap().remove("claim_generation");
    assert_eq!(doc_to_job(&stored(doc)).unwrap().claim_generation, 0);
    let mut wire = serde_json::to_value(&value).unwrap();
    wire.as_object_mut().unwrap().remove("claim_generation");
    assert_eq!(
        serde_json::from_value::<CronJob>(wire)
            .unwrap()
            .claim_generation,
        0
    );
}
