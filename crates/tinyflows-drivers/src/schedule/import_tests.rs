use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
use super::*;
use crate::schedule::job_not_found;

use crate::schedule::test_support::{channel_origin, daily, docs_in, store};
use chrono::{TimeZone, Utc};
use tinyflows_schedule::{DeliveryStatus, Schedule};

/// The JSON form, for comparing jobs (`CronJob` is not `PartialEq`).
fn j<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

fn run(id: i64, job_id: &str, minute: u32, status: &str) -> CronRun {
    let started = Utc.with_ymd_and_hms(2025, 3, 2, 9, minute, 0).unwrap();
    CronRun {
        id,
        job_id: job_id.to_string(),
        run_id: None,
        started_at: started,
        finished_at: started + chrono::Duration::seconds(3),
        status: status.to_string(),
        output: Some(format!("output {id}")),
        duration_ms: Some(3000),
        delivery_status: Some(DeliveryStatus::NotRequested),
    }
}

/// A job made by a store of its own, so it carries a random id as the old
/// store's jobs did.
async fn legacy_job() -> CronJob {
    let source = store();
    let mut job = source
        .add_shell_job(Some("nightly".into()), daily(), "echo hi")
        .await
        .unwrap();
    job.origin = Some(channel_origin());
    job.last_status = Some("ok".into());
    job.last_output = Some("done".into());
    job.last_run = Some(Utc.with_ymd_and_hms(2025, 3, 1, 9, 0, 0).unwrap());
    job.claim_generation = 7;
    job
}

#[tokio::test]
async fn an_imported_job_keeps_its_id_and_every_field() {
    let target = store();
    let job = legacy_job().await;
    let imported = target.import_job(&job).await.unwrap();
    assert_eq!(
        imported,
        ImportedJob {
            id: job.id.clone(),
            written: true
        }
    );
    assert_eq!(j(&target.get_job(&job.id).await.unwrap()), j(&job));
}

#[tokio::test]
async fn importing_a_job_twice_keeps_the_first_copy() {
    let target = store();
    let job = legacy_job().await;
    target.import_job(&job).await.unwrap();
    let mut changed = job.clone();
    changed.name = Some("renamed".into());
    let again = target.import_job(&changed).await.unwrap();
    assert!(!again.written);
    assert_eq!(
        target.get_job(&job.id).await.unwrap().name.as_deref(),
        Some("nightly")
    );
}

#[tokio::test]
async fn an_import_never_replaces_a_job_the_store_already_holds() {
    let target = store();
    let live = target
        .add_shell_job(None, daily(), "echo live")
        .await
        .unwrap();
    let mut legacy = legacy_job().await;
    legacy.id = live.id.clone();
    assert!(!target.import_job(&legacy).await.unwrap().written);
    assert_eq!(target.get_job(&live.id).await.unwrap().command, "echo live");
}

#[tokio::test]
async fn a_flow_schedule_job_is_filed_where_the_store_looks_for_it() {
    let target = store();
    let source = store();
    let mut job = source
        .add_flow_schedule_job("flow-9", Schedule::Every { every_ms: 60_000 })
        .await
        .unwrap();
    job.id = "11111111-2222-3333-4444-555555555555".to_string();
    let imported = target.import_job(&job).await.unwrap();
    assert_eq!(imported.id, "flow:flow-9");
    assert!(imported.written);
    let found = target
        .find_flow_schedule_job("flow-9")
        .await
        .unwrap()
        .expect("found by flow id");
    assert_eq!(found.command, "flow-9");
    // Registering the flow afterwards returns the imported job, not a second.
    let registered = target
        .add_flow_schedule_job("flow-9", Schedule::Every { every_ms: 120_000 })
        .await
        .unwrap();
    assert_eq!(registered.id, "flow:flow-9");
    assert_eq!(target.list_jobs().await.unwrap().len(), 1);
    // Importing the same flow's job again finds it there.
    assert!(!target.import_job(&job).await.unwrap().written);
}

#[tokio::test]
async fn imported_runs_keep_their_numbers_and_the_counter_moves_past_them() {
    let target = store();
    let job = legacy_job().await;
    target.import_job(&job).await.unwrap();
    for (id, minute) in [(3, 1), (9, 2)] {
        assert!(
            target
                .import_run(&run(id, &job.id, minute, "ok"))
                .await
                .unwrap()
        );
    }
    let listed = target.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(listed.iter().map(|r| r.id).collect::<Vec<_>>(), [9, 3]);
    assert_eq!(listed[0].output.as_deref(), Some("output 9"));
    assert_eq!(listed[0].duration_ms, Some(3000));
    assert_eq!(
        listed[0].delivery_status,
        Some(DeliveryStatus::NotRequested)
    );
    // A run recorded afterwards is numbered past every imported one.
    let now = Utc::now();
    target
        .record_run(&job.id, now, now, "ok", None, 1)
        .await
        .unwrap();
    let newest = target.list_runs(&job.id, 1).await.unwrap();
    assert_eq!(newest[0].id, 10);
}

#[tokio::test]
async fn a_run_imported_twice_is_written_once() {
    let target = store();
    let job = legacy_job().await;
    target.import_job(&job).await.unwrap();
    let first = run(4, &job.id, 1, "ok");
    assert!(target.import_run(&first).await.unwrap());
    assert!(!target.import_run(&first).await.unwrap());
    assert_eq!(target.list_runs(&job.id, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn an_imported_run_whose_number_is_taken_gets_a_new_one_and_nothing_is_lost() {
    let target = store();
    let job = legacy_job().await;
    target.import_job(&job).await.unwrap();
    let now = Utc::now();
    target
        .record_run(&job.id, now, now, "live", Some("existing"), 1)
        .await
        .unwrap();
    // The live run holds number 1; the legacy run also wants number 1.
    assert!(target.import_run(&run(1, &job.id, 5, "ok")).await.unwrap());
    let listed = target.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(listed.len(), 2);
    let mut numbers: Vec<i64> = listed.iter().map(|r| r.id).collect();
    numbers.sort_unstable();
    assert_eq!(numbers, [1, 2]);
    assert!(
        listed
            .iter()
            .any(|r| r.output.as_deref() == Some("existing"))
    );
    assert!(
        listed
            .iter()
            .any(|r| r.output.as_deref() == Some("output 1"))
    );
}

#[tokio::test]
async fn a_run_needs_its_job() {
    let target = store();
    let error = target
        .import_run(&run(1, "missing", 1, "ok"))
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), job_not_found("missing").to_string());
}

#[tokio::test]
async fn imported_runs_belong_to_the_job_incarnation_and_go_with_it() {
    let target = store();
    let job = legacy_job().await;
    target.import_job(&job).await.unwrap();
    target.import_run(&run(1, &job.id, 1, "ok")).await.unwrap();
    target.remove_job(&job.id).await.unwrap();
    assert!(target.list_runs(&job.id, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn jobs_are_scoped_like_every_other_write() {
    let storage = MemoryStorage::new();
    let alice = CronDocuments::new(docs_in(&storage, "alice"));
    let bob = CronDocuments::new(docs_in(&storage, "bob"));
    let job = legacy_job().await;
    alice.import_job(&job).await.unwrap();
    assert!(bob.list_jobs().await.unwrap().is_empty());
    assert!(bob.import_job(&job).await.unwrap().written);
}

#[tokio::test]
async fn an_import_over_the_active_job_cap_is_refused() {
    let target = store();
    target.set_max_active_jobs(Some(1)).await.unwrap();
    let first = legacy_job().await;
    let second = legacy_job().await;
    target.import_job(&first).await.unwrap();
    let error = target.import_job(&second).await.unwrap_err();
    assert!(format!("{error:#}").contains("active job limit exceeded"));
    assert_eq!(target.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_refused_import_on_a_non_transactional_backend_writes_no_job() {
    use crate::schedule::test_support::FaultDocuments;
    use std::sync::{Arc, atomic::AtomicBool};
    let backend = MemoryStorage::new();
    CronDocuments::new(docs_in(&backend, "policy"))
        .set_max_active_jobs(Some(5))
        .await
        .unwrap();
    let target = CronDocuments::new(Arc::new(FaultDocuments {
        inner: docs_in(&backend, "policy"),
        transactions: false,
        fail_batch: AtomicBool::new(false),
        batch_barrier: Default::default(),
        faults: Default::default(),
    }));
    let job = legacy_job().await;
    assert!(target.import_job(&job).await.is_err());
    assert!(
        target.docs.get(JOBS, &job.id).await.unwrap().is_none(),
        "the refused job must not stay in cron.jobs"
    );
}
