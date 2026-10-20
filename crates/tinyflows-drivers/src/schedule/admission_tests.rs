//! Shared admission policy fences both configured and existing handles.
use super::{
    CronDocuments,
    test_support::{daily, docs_in},
};
use tinyflows_schedule::CronJobPatch;
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

#[tokio::test]
async fn an_existing_unlimited_handle_cannot_bypass_installed_policy() {
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "admission"));
    let b = CronDocuments::new(docs_in(&backend, "admission"));
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let (first, second) = tokio::join!(
        a.add_shell_job(None, daily(), "a"),
        b.add_shell_job(None, daily(), "b")
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let job = a.list_jobs().await.unwrap().pop().unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let next = b.add_shell_job(None, daily(), "next").await.unwrap();
    assert!(
        a.update_job(
            &job.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    b.remove_job(&next.id).await.unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn policy_installation_counts_existing_jobs_and_refuses_a_lower_cap() {
    let a = super::test_support::store();
    a.add_shell_job(None, daily(), "a").await.unwrap();
    a.add_shell_job(None, daily(), "b").await.unwrap();
    assert!(a.set_max_active_jobs(Some(1)).await.is_err());
    a.set_max_active_jobs(Some(2)).await.unwrap();
    assert!(a.add_shell_job(None, daily(), "c").await.is_err());
    a.clear_all_jobs().await.unwrap();
    a.add_shell_job(None, daily(), "c").await.unwrap();
}

#[tokio::test]
async fn unsupported_backends_keep_unlimited_crud_but_refuse_policy_and_claims() {
    use super::test_support::FaultDocuments;
    use std::sync::{Arc, atomic::AtomicBool};
    let backend = MemoryStorage::new();
    let docs = Arc::new(FaultDocuments {
        inner: docs_in(&backend, "legacy"),
        transactions: false,
        fail_batch: AtomicBool::new(false),
        batch_barrier: Default::default(),
        faults: Default::default(),
    });
    let a = CronDocuments::new(docs);
    let job = a.add_shell_job(None, daily(), "legacy").await.unwrap();
    assert!(a.set_max_active_jobs(Some(1)).await.is_err());
    assert!(
        a.claim_due_job(&job.id, "w", job.next_run, chrono::Duration::minutes(1))
            .await
            .is_err()
    );
    a.update_job(
        &job.id,
        CronJobPatch {
            enabled: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    a.remove_job(&job.id).await.unwrap();
    assert!(a.list_jobs().await.unwrap().is_empty());
    assert!(
        a.docs
            .get(super::ADMISSION, "ledger")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn transaction_refusal_and_completion_failure_leave_jobs_unchanged() {
    use super::test_support::FaultDocuments;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    let backend = MemoryStorage::new();
    let docs = Arc::new(FaultDocuments {
        inner: docs_in(&backend, "failure"),
        transactions: true,
        fail_batch: AtomicBool::new(true),
        batch_barrier: Default::default(),
        faults: Default::default(),
    });
    let a = CronDocuments::new(docs.clone());
    assert!(a.set_max_active_jobs(Some(1)).await.is_err());
    assert!(a.add_shell_job(None, daily(), "refused").await.is_err());
    assert!(a.list_jobs().await.unwrap().is_empty());
    docs.fail_batch.store(false, Ordering::SeqCst);
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let job = a.add_shell_job(None, daily(), "job").await.unwrap();
    let claim = a
        .claim_due_job(&job.id, "w", job.next_run, chrono::Duration::minutes(1))
        .await
        .unwrap()
        .unwrap();
    docs.fail_batch.store(true, Ordering::SeqCst);
    assert!(
        a.complete_claim(&claim, job.next_run, true, "never recorded", None)
            .await
            .is_err()
    );
    assert!(a.get_job(&job.id).await.unwrap().last_output.is_none());
    assert_eq!(a.get_job(&job.id).await.unwrap().next_run, job.next_run);
    assert!(a.list_runs(&job.id, 10).await.unwrap().is_empty());
    assert!(a.remove_job(&job.id).await.is_err());
    assert!(
        a.update_job(
            &job.id,
            CronJobPatch {
                enabled: Some(false),
                ..Default::default()
            }
        )
        .await
        .is_err()
    );
    docs.fail_batch.store(false, Ordering::SeqCst);
    assert!(
        a.add_shell_job(None, daily(), "cap still occupied")
            .await
            .is_err()
    );
    assert!(
        a.complete_claim(&claim, job.next_run, true, "recorded", None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn malformed_admission_and_capability_downgrades_fail_closed() {
    use super::test_support::FaultDocuments;
    use std::sync::{Arc, atomic::AtomicBool};
    use tinyflows_storage::Precondition;
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "downgrade"));
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let b = CronDocuments::new(Arc::new(FaultDocuments {
        inner: docs_in(&backend, "downgrade"),
        transactions: false,
        fail_batch: AtomicBool::new(false),
        batch_barrier: Default::default(),
        faults: Default::default(),
    }));
    assert!(b.add_shell_job(None, daily(), "bypass").await.is_err());
    a.docs
        .put(
            super::ADMISSION,
            "ledger",
            serde_json::json!({"limit":"one","members":[]}),
            Precondition::None,
        )
        .await
        .unwrap();
    assert!(a.add_shell_job(None, daily(), "bad").await.is_err());
    assert!(a.set_max_active_jobs(None).await.is_err());
}

#[tokio::test]
async fn concurrent_reenable_and_installation_preserve_the_cap() {
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "race"));
    let b = CronDocuments::new(docs_in(&backend, "race"));
    let first = a.add_shell_job(None, daily(), "a").await.unwrap();
    let second = a.add_shell_job(None, daily(), "b").await.unwrap();
    for id in [&first.id, &second.id] {
        a.update_job(
            id,
            CronJobPatch {
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }
    a.set_max_active_jobs(Some(1)).await.unwrap();
    let (first, second) = tokio::join!(
        a.update_job(
            &first.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            }
        ),
        b.update_job(
            &second.id,
            CronJobPatch {
                enabled: Some(true),
                ..Default::default()
            }
        )
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    a.clear_all_jobs().await.unwrap();
    a.set_max_active_jobs(None).await.unwrap();
    let (policy, create) = tokio::join!(
        a.set_max_active_jobs(Some(1)),
        b.add_shell_job(None, daily(), "racer")
    );
    policy.unwrap();
    create.unwrap();
    assert!(b.add_shell_job(None, daily(), "over cap").await.is_err());
}

#[tokio::test]
async fn installing_policy_fences_an_unlimited_write_already_in_flight() {
    use super::test_support::FaultDocuments;
    use std::sync::{Arc, atomic::AtomicBool};
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(docs_in(&backend, "install-race"));
    a.set_max_active_jobs(None).await.unwrap();
    a.add_shell_job(None, daily(), "existing").await.unwrap();
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let docs = Arc::new(FaultDocuments {
        inner: docs_in(&backend, "install-race"),
        transactions: true,
        fail_batch: AtomicBool::new(false),
        batch_barrier: std::sync::Mutex::new(Some(gate.clone())),
        faults: Default::default(),
    });
    let b = CronDocuments::new(docs.clone());
    let create = tokio::spawn(async move { b.add_shell_job(None, daily(), "racer").await });
    // Wait for the writer to take the gate immediately before its transaction.
    while docs.batch_barrier.lock().unwrap().is_some() {
        tokio::task::yield_now().await;
    }
    a.set_max_active_jobs(Some(1)).await.unwrap();
    gate.wait().await;
    assert!(create.await.unwrap().is_err());
    assert_eq!(a.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn admission_wire_form_lists_members_with_their_job_ids() {
    let a = super::test_support::store();
    a.set_max_active_jobs(Some(2)).await.unwrap();
    let stored = a
        .docs
        .get(super::ADMISSION, "ledger")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.doc, serde_json::json!({"limit": 2, "members": []}));
    a.add_flow_schedule_job("dot.$key", daily()).await.unwrap();
    let stored = a
        .docs
        .get(super::ADMISSION, "ledger")
        .await
        .unwrap()
        .unwrap();
    assert!(stored.doc.get("jobs").is_none());
    assert!(stored.doc.get("members_json").is_none());
    assert_eq!(stored.doc["members"][0]["job_id"], "flow:dot.$key");
    a.clear_all_jobs().await.unwrap();
    a.set_max_active_jobs(None).await.unwrap();
    let stored = a
        .docs
        .get(super::ADMISSION, "ledger")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.doc, serde_json::json!({"members": []}));
}

#[tokio::test]
async fn policy_installation_reconciles_legacy_rows_written_before_adoption() {
    use tinyflows_storage::Precondition;
    let a = super::test_support::store();
    let first = a.add_shell_job(None, daily(), "upgraded").await.unwrap();
    // An older writer during a rolling upgrade did not know the ledger.
    // All such writers have stopped before installing the operator policy.
    let mut legacy = first.clone();
    legacy.id = "legacy-during-upgrade".into();
    a.docs
        .put(
            super::JOBS,
            &legacy.id,
            super::codec::job_to_doc(&legacy).unwrap(),
            Precondition::Absent,
        )
        .await
        .unwrap();
    assert!(a.set_max_active_jobs(Some(1)).await.is_err());
    a.set_max_active_jobs(Some(2)).await.unwrap();
    assert!(a.add_shell_job(None, daily(), "over cap").await.is_err());
    a.remove_job(&legacy.id).await.unwrap();
    a.add_shell_job(None, daily(), "new slot").await.unwrap();
}
