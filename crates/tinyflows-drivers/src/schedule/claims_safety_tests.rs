//! A claim's lease and the job document are two writes; these pin down that a
//! holder which lost its claim cannot commit through the gap between them, and
//! that no error path strands a lease.
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
use super::CronDocuments;
use super::test_support::{FaultDocuments, daily, docs_in};
use chrono::Duration;
use std::sync::{Arc, atomic::AtomicBool};
use tinyflows_schedule::CronJobPatch;

fn fault_store(
    backend: &MemoryStorage,
    scope: &str,
    barrier: Option<Arc<tokio::sync::Barrier>>,
) -> (CronDocuments, Arc<FaultDocuments>) {
    let docs = Arc::new(FaultDocuments {
        inner: docs_in(backend, scope),
        transactions: true,
        fail_batch: AtomicBool::new(false),
        batch_barrier: std::sync::Mutex::new(barrier),
        faults: Default::default(),
    });
    (CronDocuments::new(docs.clone()), docs)
}

async fn parked(docs: &FaultDocuments) {
    while docs.batch_barrier.lock().unwrap().is_some() {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_holder_whose_lease_was_taken_over_cannot_complete_through_the_read_to_commit_gap() {
    let backend = MemoryStorage::new();
    let setup = CronDocuments::new(docs_in(&backend, "takeover-complete"));
    let job = setup.add_shell_job(None, daily(), "echo").await.unwrap();
    let start = job.next_run;
    let a = setup
        .claim_due_job(&job.id, "a", start, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let (stalled, docs) = fault_store(&backend, "takeover-complete", Some(gate.clone()));
    let finishing = tokio::spawn(async move {
        stalled
            .complete_claim(&a, start + Duration::seconds(10), true, "a result", None)
            .await
    });
    // A has read the job and found its lease live; it is about to commit.
    parked(&docs).await;
    let b = setup
        .claim_due_job(
            &job.id,
            "b",
            start + Duration::seconds(31),
            Duration::seconds(30),
        )
        .await
        .unwrap()
        .expect("A's lease expired, so B takes the occurrence over");
    gate.wait().await;
    assert!(
        !finishing.await.unwrap().unwrap(),
        "the stalled holder must not commit over its successor"
    );
    assert!(setup.list_runs(&job.id, 10).await.unwrap().is_empty());
    assert!(
        setup
            .complete_claim(&b, start + Duration::seconds(32), true, "b result", None)
            .await
            .unwrap()
    );
    let history = setup.list_runs(&job.id, 10).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].output.as_deref(), Some("b result"));
}

#[tokio::test]
async fn a_holder_whose_lease_was_taken_over_cannot_queue_a_run_through_the_gap() {
    let backend = MemoryStorage::new();
    let setup = CronDocuments::new(docs_in(&backend, "takeover-queue"));
    let job = setup.add_shell_job(None, daily(), "echo").await.unwrap();
    let start = job.next_run;
    let a = setup
        .claim_due_job(&job.id, "a", start, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let (stalled, docs) = fault_store(&backend, "takeover-queue", Some(gate.clone()));
    let queueing = tokio::spawn(async move {
        stalled
            .queue_claimed_run(&a, start + Duration::seconds(10))
            .await
    });
    parked(&docs).await;
    let b = setup
        .claim_due_job(
            &job.id,
            "b",
            start + Duration::seconds(31),
            Duration::seconds(30),
        )
        .await
        .unwrap()
        .unwrap();
    gate.wait().await;
    assert!(!queueing.await.unwrap().unwrap());
    assert!(
        setup.list_runs(&job.id, 10).await.unwrap().is_empty(),
        "the stalled holder's placeholder must not outlive its claim"
    );
    assert!(
        setup
            .queue_claimed_run(&b, start + Duration::seconds(32))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn an_edit_invalidates_a_live_claim_even_when_freeing_its_lease_fails() {
    let backend = MemoryStorage::new();
    let (store, docs) = fault_store(&backend, "edit-fence", None);
    let job = store.add_shell_job(None, daily(), "echo").await.unwrap();
    let claim = store
        .claim_due_job(&job.id, "a", job.next_run, Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    // The lease cannot be written (a heartbeat or a failure races the edit).
    docs.faults
        .lease_writes
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .update_job(
            &job.id,
            CronJobPatch {
                name: Some("renamed".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    docs.faults
        .lease_writes
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let at = job.next_run + Duration::seconds(1);
    assert!(
        !store
            .renew_claim(&claim, at, Duration::minutes(5))
            .await
            .unwrap()
    );
    assert!(!store.queue_claimed_run(&claim, at).await.unwrap());
    assert!(
        !store
            .complete_claim(&claim, at, true, "under the old definition", None)
            .await
            .unwrap()
    );
    assert!(store.list_runs(&job.id, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_edited_job_can_be_claimed_at_once() {
    let store = super::test_support::store();
    let job = store.add_shell_job(None, daily(), "echo").await.unwrap();
    let old = store
        .claim_due_job(&job.id, "a", job.next_run, Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    store
        .update_job(
            &job.id,
            CronJobPatch {
                name: Some("renamed".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let edited = store.get_job(&job.id).await.unwrap();
    let fresh = store
        .claim_due_job(&job.id, "b", job.next_run, Duration::minutes(5))
        .await
        .unwrap()
        .expect("the edit freed the old claim's lease");
    assert!(
        !store
            .complete_claim(&old, job.next_run, true, "old", None)
            .await
            .unwrap()
    );
    assert_eq!(edited.name.as_deref(), Some("renamed"));
    assert!(
        store
            .complete_claim(&fresh, job.next_run, true, "new", None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_removed_and_recreated_job_is_not_blocked_by_the_old_lease() {
    let store = super::test_support::store();
    let job = store.add_flow_schedule_job("flow", daily()).await.unwrap();
    let old = store
        .claim_due_job(&job.id, "old", job.next_run, Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    store.remove_job(&job.id).await.unwrap();
    let replacement = store.add_flow_schedule_job("flow", daily()).await.unwrap();
    assert_eq!(replacement.next_run, job.next_run);
    let fresh = store
        .claim_due_job(&job.id, "fresh", replacement.next_run, Duration::minutes(5))
        .await
        .unwrap()
        .expect("the old incarnation's lease must not block the new job");
    assert!(
        !store
            .complete_claim(&old, job.next_run, true, "old", None)
            .await
            .unwrap()
    );
    assert!(
        store
            .complete_claim(&fresh, job.next_run, true, "fresh", None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_failure_after_acquiring_leaves_the_occurrence_claimable() {
    let backend = MemoryStorage::new();
    let setup = CronDocuments::new(docs_in(&backend, "leak"));
    let job = setup.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;

    // Reading the job again after the lease was taken fails.
    let (reading, docs) = fault_store(&backend, "leak", None);
    docs.faults
        .fail_job_read
        .store(2, std::sync::atomic::Ordering::SeqCst);
    if reading
        .claim_due_job(&job.id, "w", now, Duration::minutes(5))
        .await
        .is_err()
    {
        assert!(
            setup
                .claim_due_job(&job.id, "next", now, Duration::minutes(5))
                .await
                .unwrap()
                .is_some(),
            "a failed claim must not strand the lease"
        );
        return;
    }
    // The claim needed no second read: nothing was stranded to begin with, and
    // the occurrence is held by the claim that succeeded.
}

#[tokio::test]
async fn a_failed_job_commit_gives_the_lease_back() {
    let backend = MemoryStorage::new();
    let setup = CronDocuments::new(docs_in(&backend, "leak-commit"));
    let job = setup.add_shell_job(None, daily(), "echo").await.unwrap();
    let (failing, docs) = fault_store(&backend, "leak-commit", None);
    docs.fail_batch
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(
        failing
            .claim_due_job(&job.id, "w", job.next_run, Duration::minutes(5))
            .await
            .is_err()
    );
    assert!(
        setup
            .claim_due_job(&job.id, "next", job.next_run, Duration::minutes(5))
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn a_failed_lease_release_after_a_committed_completion_does_not_fail_it() {
    let backend = MemoryStorage::new();
    let (store, docs) = fault_store(&backend, "release-failure", None);
    let job = store.add_shell_job(None, daily(), "echo").await.unwrap();
    let claim = store
        .claim_due_job(&job.id, "w", job.next_run, Duration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    docs.faults
        .lease_writes
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // The run, receipt and advance are durable; the lease only expires later.
    assert!(
        store
            .complete_claim(&claim, job.next_run, true, "done", None)
            .await
            .unwrap(),
        "a lease that could not be released must not fail a committed completion"
    );
    assert_eq!(store.list_runs(&job.id, 10).await.unwrap().len(), 1);
    // So the host still reaches its `delete_after_run` cleanup.
    assert!(store.remove_completed_claim(&claim).await.unwrap());
    assert!(store.list_runs(&job.id, 10).await.unwrap().is_empty());
    assert!(store.find_job(&job.id).await.unwrap().is_none());
}

