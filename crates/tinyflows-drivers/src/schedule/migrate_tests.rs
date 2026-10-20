use super::*;
use chrono::TimeZone;
use serde_json::json;
use tinyflows_schedule::{Schedule, runtime::RunRecord};
use tinyflows_storage::{
    DocumentStoreExt, MigrateOptions, Precondition, Query, Scope, Storage, StorageBackend,
};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

fn scoped() -> Storage {
    MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .unwrap()
}

async fn seed(storage: &Storage, collection: &str, id: &str, body: Value) {
    storage
        .documents()
        .put(collection, id, body, Precondition::None)
        .await
        .unwrap();
}

async fn migrate(storage: &Storage) {
    storage_schema()
        .migrate(storage.documents(), &MigrateOptions::new())
        .await
        .unwrap();
}

fn v1_job(schedule: Value) -> Value {
    json!({
        "claim_generation": 3, "expression": "0 9 * * *", "command": "echo",
        "schedule": schedule.to_string(), "job_type": "shell", "session_target": "isolated",
        "enabled": true, "delivery": "{\"mode\":\"none\",\"channel\":null,\"to\":null,\"best_effort\":true}",
        "delete_after_run": false, "created_at": "2026-03-04T05:06:07.123456789+00:00",
        "created_ms": 1_772_600_767_123_i64, "next_run": "2026-03-05T09:00:00+00:00",
        "next_run_ms": 1_772_701_200_000_i64, "next_run_ns": 1_772_701_200_000_000_000_i64,
        "name": "n", "origin": "{\"kind\":\"web\",\"thread_id\":\"t\"}",
        "last_run": "2026-03-04T09:00:00+00:00", "last_status": "ok", "last_output": "out",
        "incarnation": "inc",
        "occurrence_lease": {"owner": "w", "token": "x"},
    })
}

#[test]
fn the_schema_composes_and_declares_the_cron_collections() {
    let schema = storage_schema();
    assert_eq!(schema.owner.as_str(), "cron");
    let names: Vec<String> = schema
        .items
        .iter()
        .map(|(item, _, _)| item.name.clone())
        .collect();
    assert_eq!(names, ["cron.jobs", "cron.runs", "cron.admission"]);
    assert!(
        schema
            .collections()
            .iter()
            .any(|item| item.name == "tsd.migrations")
    );
}

#[tokio::test]
async fn v1_jobs_runs_and_the_ledger_migrate_and_read_back() {
    let storage = scoped();
    seed(
        &storage,
        "cron_jobs",
        "j1",
        v1_job(json!({"kind": "cron", "expr": "0 9 * * *", "tz": null, "active_hours": null})),
    )
    .await;
    let at = chrono::Utc.with_ymd_and_hms(2030, 1, 2, 3, 4, 5).unwrap();
    seed(
        &storage,
        "cron_jobs",
        "j2",
        v1_job(json!({"kind": "at", "at": at.to_rfc3339()})),
    )
    .await;
    seed(
        &storage,
        "cron_runs",
        "00000000000000000007",
        json!({"seq": 7, "job_id": "j1", "started_at": "2026-03-04T09:00:00+00:00",
               "started_ns": 1_772_614_800_000_000_000_i64, "finished_at": "2026-03-04T09:00:02+00:00",
               "status": "success", "duration_ms": 2000, "incarnation": "inc", "output": "ok"}),
    )
    .await;
    seed(
        &storage,
        "cron_counters",
        "admission",
        json!({"limit": 5, "members_json": "{\"j1\":{\"incarnation\":\"inc\",\"enabled\":true}}"}),
    )
    .await;
    seed(&storage, "cron_counters", "runs", json!({"next": 8})).await;

    migrate(&storage).await;
    assert!(
        import_legacy_cron_counters(storage.documents())
            .await
            .unwrap()
    );
    assert!(
        !import_legacy_cron_counters(storage.documents())
            .await
            .unwrap(),
        "runs once"
    );

    let docs = storage.documents();
    let j1 = docs.get("cron.jobs", "j1").await.unwrap().unwrap();
    assert_eq!(
        j1.doc["schedule"],
        json!({"kind": "cron", "expr": "0 9 * * *", "tz": null, "active_hours": null})
    );
    assert_eq!(j1.doc["delivery"]["mode"], "none");
    assert_eq!(j1.doc["origin"], json!({"kind": "web", "thread_id": "t"}));
    assert_eq!(j1.doc["next_run_ms"], 1_772_701_200_000_i64);
    assert_eq!(j1.doc["last_run_ms"], 1_772_614_800_000_i64);
    assert_eq!(j1.doc["created_ms"], 1_772_600_767_123_i64);
    assert!(j1.meta.created_ms > 0, "backend owns record metadata");
    for gone in [
        "created_at",
        "next_run",
        "next_run_ns",
        "last_run",
        "occurrence_lease",
    ] {
        assert!(j1.doc.get(gone).is_none(), "{gone}");
    }
    let j2 = docs.get("cron.jobs", "j2").await.unwrap().unwrap();
    assert_eq!(
        j2.doc["schedule"],
        json!({"kind": "at", "at_ms": at.timestamp_millis()})
    );
    let run = docs
        .get("cron.runs", "00000000000000000007")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.doc["started_ms"], 1_772_614_800_000_i64);
    assert_eq!(run.doc["finished_ms"], 1_772_614_802_000_i64);
    let ledger = docs.get("cron.admission", "ledger").await.unwrap().unwrap();
    assert_eq!(
        ledger.doc,
        json!({"limit": 5, "members": [{"job_id": "j1", "incarnation": "inc", "enabled": true}]})
    );

    // Read through the store; run numbers continue from the v1 counter.
    let store = CronDocuments::new(std::sync::Arc::clone(docs));
    let job = store.get_job("j2").await.unwrap();
    assert!(matches!(job.schedule, Schedule::At { at: found } if found == at));
    assert_eq!(job.created_at.timestamp_millis(), 1_772_600_767_123_i64);
    assert_eq!(store.list_runs("j1", 10).await.unwrap()[0].id, 7);
    store
        .record_run("j1", at, at, "success", None, 1)
        .await
        .unwrap();
    let ids: Vec<i64> = store
        .list_runs("j1", 10)
        .await
        .unwrap()
        .iter()
        .map(|r| r.id)
        .collect();
    assert!(ids.contains(&8), "the next run number is 8: {ids:?}");
    assert!(
        docs.get("cron.admission", "runs_counter")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_claimed_run_accepts_a_migrated_jobs_body_creation_time() {
    let storage = scoped();
    seed(
        &storage,
        "cron_jobs",
        "migrated-job",
        v1_job(json!({"kind": "cron", "expr": "0 9 * * *", "tz": null, "active_hours": null})),
    )
    .await;
    migrate(&storage).await;

    let store = CronDocuments::new(std::sync::Arc::clone(storage.documents()));
    let created_at = chrono::DateTime::from_timestamp_millis(1_772_600_767_123_i64).unwrap();
    let now = chrono::Utc::now();
    store
        .record_claimed_run(&RunRecord {
            run_id: "migration-run".into(),
            job_id: "migrated-job".into(),
            job_created_at: created_at,
            started_at: now,
            finished_at: now,
            status: "ok".into(),
            output: "done".into(),
            delivery_status: None,
        })
        .await
        .expect("a migration's body creation time is the job identity");
    assert_eq!(store.list_runs("migrated-job", 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_v1_claims_queued_placeholder_run_is_kept_on_the_job_and_removed_by_the_next_claim() {
    let storage = scoped();
    let mut job = v1_job(json!({"kind": "every", "every_ms": 60000}));
    job["occurrence_lease"] = json!({"owner": "w", "token": "x", "queued_run": 7});
    job["next_run_ms"] = json!(1_000_i64);
    seed(&storage, "cron_jobs", "j1", job).await;
    seed(
        &storage,
        "cron_runs",
        "00000000000000000007",
        json!({"seq": 7, "job_id": "j1", "status": "queued", "incarnation": "inc",
               "started_at": "2026-03-04T05:06:07+00:00", "finished_at": "2026-03-04T05:06:07+00:00",
               "duration_ms": 0}),
    )
    .await;
    migrate(&storage).await;

    let docs = storage.documents();
    let migrated = docs.get("cron.jobs", "j1").await.unwrap().unwrap();
    assert_eq!(migrated.doc["queued_run"], 7);
    assert!(migrated.doc.get("occurrence_lease").is_none());

    let store = CronDocuments::new(std::sync::Arc::clone(docs));
    let claim = store
        .claim_due_job(
            "j1",
            "worker",
            chrono::Utc.timestamp_millis_opt(2_000).unwrap(),
            chrono::Duration::seconds(30),
        )
        .await
        .unwrap();
    assert!(claim.is_some());
    assert!(
        docs.get("cron.runs", "00000000000000000007")
            .await
            .unwrap()
            .is_none(),
        "the placeholder of the claim that did not survive the migration is gone"
    );
    let after = docs.get("cron.jobs", "j1").await.unwrap().unwrap();
    assert!(after.doc.get("queued_run").is_none());
}

#[tokio::test]
async fn migrating_twice_changes_nothing() {
    let storage = scoped();
    seed(
        &storage,
        "cron_jobs",
        "j1",
        v1_job(json!({"kind": "every", "every_ms": 60000})),
    )
    .await;
    migrate(&storage).await;
    migrate(&storage).await;
    let rows = storage
        .documents()
        .query_all("cron.jobs", &Query::all())
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn unconvertible_v1_jobs_and_runs_are_carried_not_dropped() {
    let storage = scoped();
    seed(&storage, "cron_jobs", "bad", json!({"name": "no schedule"})).await;
    seed(&storage, "cron_runs", "badrun", json!({"job_id": "bad"})).await;
    migrate(&storage).await;
    for (collection, id) in [("cron.jobs", "bad"), ("cron.runs", "badrun")] {
        let row = storage.documents().get(collection, id).await.unwrap();
        assert!(
            row.is_some(),
            "{collection}/{id} must survive the migration"
        );
    }
}
