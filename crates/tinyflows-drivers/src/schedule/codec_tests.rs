use super::*;
use tinystoragedrivers_core::Version;

fn stored(doc: Value) -> Versioned<Value> {
    Versioned {
        id: "x".into(),
        version: Version::FIRST,
        doc,
    }
}

fn job() -> CronJob {
    let now = Utc::now();
    CronJob {
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
    for field in ["prompt", "name", "model", "agent_id", "origin", "last_run"] {
        assert!(doc.get(field).is_none(), "{field}");
    }
    assert!(doc["schedule"].is_string() && doc["delivery"].is_string());
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
    doc["next_run"] = json!("yesterday");
    assert!(doc_to_job(&stored(doc)).is_err());
    let run = run_to_doc(1, "j", Utc::now(), Utc::now(), "ok", None, 1, None);
    let mut broken = run.clone();
    broken.as_object_mut().unwrap().remove("seq");
    assert!(doc_to_run(&stored(broken)).is_err());
    assert_eq!(doc_to_run(&stored(run)).unwrap().id, 1);
}

#[test]
fn run_ids_sort_as_numbers() {
    assert!(run_id(9) < run_id(10));
    assert_eq!(run_id(1).len(), 20);
}
