use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
//! A completed receipt removes only its own unchanged incarnation.
use super::{
    CronDocuments,
    test_support::{daily, docs_in},
};
use chrono::Duration;
use tinyflows_storage::MemoryStorage;

#[tokio::test]
async fn completed_receipt_cannot_remove_a_recreated_job() {
    let a = super::test_support::store();
    let job = a.add_flow_schedule_job("flow", daily()).await.unwrap();
    let claim = a
        .claim_due_job(&job.id, "worker", job.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert!(!a.remove_completed_claim(&claim).await.unwrap());
    assert!(
        a.complete_claim(&claim, job.next_run, true, "completed", None)
            .await
            .unwrap()
    );
    a.remove_job(&job.id).await.unwrap();
    a.add_flow_schedule_job("flow", daily()).await.unwrap();
    assert!(!a.remove_completed_claim(&claim).await.unwrap());
    assert_eq!(a.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn completed_receipt_frees_admission_and_deletes_its_history() {
    let a = super::test_support::store();
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let claim = a
        .claim_due_job(&job.id, "worker", job.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert!(
        a.complete_claim(&claim, job.next_run, true, "completed", None)
            .await
            .unwrap()
    );
    assert!(a.remove_completed_claim(&claim).await.unwrap());
    assert!(!a.remove_completed_claim(&claim).await.unwrap());
    assert!(a.list_runs(&job.id, 10).await.unwrap().is_empty());
    a.add_shell_job(None, daily(), "new slot").await.unwrap();
}

#[tokio::test]
async fn removal_already_in_flight_spares_the_replacement_and_its_history() {
    use super::test_support::FaultDocuments;
    use std::sync::{Arc, atomic::AtomicBool};
    let backend = MemoryStorage::new();
    let setup = CronDocuments::new(docs_in(&backend, "delete-overlap"));
    let job = setup.add_flow_schedule_job("flow", daily()).await.unwrap();
    let old = setup
        .claim_due_job(&job.id, "old", job.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    setup
        .complete_claim(&old, job.next_run, true, "old completed", None)
        .await
        .unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let docs = Arc::new(FaultDocuments {
        inner: docs_in(&backend, "delete-overlap"),
        transactions: true,
        fail_batch: AtomicBool::new(false),
        batch_barrier: std::sync::Mutex::new(Some(gate.clone())),
        faults: Default::default(),
    });
    let deleting = CronDocuments::new(docs.clone());
    let deletion = tokio::spawn(async move { deleting.remove_completed_claim(&old).await });
    while docs.batch_barrier.lock().unwrap().is_some() {
        tokio::task::yield_now().await;
    }
    setup.remove_job(&job.id).await.unwrap();
    let replacement = setup.add_flow_schedule_job("flow", daily()).await.unwrap();
    let fresh = setup
        .claim_due_job(&job.id, "fresh", replacement.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    setup
        .complete_claim(&fresh, replacement.next_run, true, "new completed", None)
        .await
        .unwrap();
    gate.wait().await;
    assert!(!deletion.await.unwrap().unwrap());
    assert_eq!(
        setup.get_job(&job.id).await.unwrap().last_output.as_deref(),
        Some("new completed")
    );
    assert_eq!(setup.list_runs(&job.id, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn later_acquisition_or_edit_invalidates_the_completed_receipt() {
    use tinyflows_schedule::CronJobPatch;
    let a = super::test_support::store();
    let job = a.add_shell_job(None, daily(), "echo").await.unwrap();
    let old = a
        .claim_due_job(&job.id, "worker", job.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    a.complete_claim(&old, job.next_run, true, "completed", None)
        .await
        .unwrap();
    let next = a.get_job(&job.id).await.unwrap();
    let fresh = a
        .claim_due_job(&job.id, "worker", next.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    assert!(!a.remove_completed_claim(&old).await.unwrap());
    a.release_claim(&fresh, next.next_run).await.unwrap();
    assert!(!a.remove_completed_claim(&old).await.unwrap());
    let fresh = a
        .claim_due_job(&job.id, "worker", next.next_run, Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    a.complete_claim(&fresh, next.next_run, true, "completed again", None)
        .await
        .unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            name: Some("edited".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(!a.remove_completed_claim(&fresh).await.unwrap());
    assert_eq!(a.list_jobs().await.unwrap().len(), 1);
}
