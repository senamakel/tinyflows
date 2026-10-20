//! Occurrence leases are compacted once the job has moved past them, without
//! weakening the fencing of stale workers.
use super::test_support::{daily, docs_in, store};
use super::*;
use chrono::{Duration, Utc};
use tinyflows_schedule::Schedule;
use tinyflows_storage::{DocumentStoreExt,  Query, primitives};

async fn lease_count(store: &CronDocuments) -> usize {
    store
        .docs
        .query_all(primitives::LEASES, &Query::all())
        .await
        .unwrap()
        .len()
}

#[tokio::test]
async fn completing_an_advancing_claim_drops_its_occurrence_lease() {
    let store = store();
    let job = store.add_shell_job(None, daily(), "echo").await.unwrap();
    let claim = store
        .claim_due_job(&job.id, "w", job.next_run, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease_count(&store).await, 1);
    assert!(
        store
            .complete_claim(&claim, job.next_run, true, "ok", None)
            .await
            .unwrap()
    );
    assert!(store.get_job(&job.id).await.unwrap().next_run > job.next_run);
    assert_eq!(
        lease_count(&store).await,
        0,
        "the lease of an occurrence the job moved past must not be kept"
    );
}

#[tokio::test]
async fn a_frequent_job_does_not_accumulate_lease_records() {
    let store = store();
    let job = store.add_shell_job(None, daily(), "echo").await.unwrap();
    let mut now = job.next_run;
    for _ in 0..5 {
        let claim = store
            .claim_due_job(&job.id, "w", now, Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        assert!(
            store
                .complete_claim(&claim, now, true, "ok", None)
                .await
                .unwrap()
        );
        now = store.get_job(&job.id).await.unwrap().next_run;
    }
    assert_eq!(lease_count(&store).await, 0);
}

#[tokio::test]
async fn fencing_still_holds_after_the_lease_is_compacted() {
    let store = store();
    let job = store.add_shell_job(None, daily(), "echo").await.unwrap();
    let now = job.next_run;
    let old = store
        .claim_due_job(&job.id, "old", now, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    // The old worker stalls; the lease expires and a successor takes over.
    let later = now + Duration::seconds(30);
    let fresh = store
        .claim_due_job(&job.id, "fresh", later, Duration::seconds(30))
        .await
        .unwrap()
        .unwrap();
    assert!(fresh.token() != old.token());
    assert!(
        store
            .complete_claim(&fresh, later, true, "fresh", None)
            .await
            .unwrap()
    );
    assert_eq!(lease_count(&store).await, 0);
    // The stalled worker wakes up after compaction: every operation is refused.
    let after = later + Duration::seconds(1);
    assert!(
        !store
            .renew_claim(&old, after, Duration::seconds(60))
            .await
            .unwrap()
    );
    assert!(!store.release_claim(&old, after).await.unwrap());
    assert!(
        !store
            .complete_claim(&old, after, true, "late", None)
            .await
            .unwrap()
    );
    // The completed occurrence cannot be claimed again.
    assert!(
        store
            .claim_due_job(&job.id, "again", after, Duration::seconds(30))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.get_job(&job.id).await.unwrap().last_output.as_deref(),
        Some("fresh")
    );
}

#[tokio::test]
async fn a_manual_claim_keeps_its_lease_because_the_schedule_did_not_move() {
    let backend = MemoryStorage::new();
    let store = CronDocuments::new(docs_in(&backend, "manual-compaction"));
    let now = Utc::now();
    let job = store
        .add_shell_job(
            None,
            Schedule::At {
                at: now + Duration::hours(1),
            },
            "echo",
        )
        .await
        .unwrap();
    let claim = store
        .claim_job_now(&job, "w", now, Duration::minutes(2))
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .complete_manual_claim(&claim, now, true, "ok", None)
            .await
            .unwrap()
    );
    assert_eq!(
        lease_count(&store).await,
        1,
        "the occurrence is still ahead of the job, so its lease record stays"
    );
}
