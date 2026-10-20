//! Removal compatibility and admission bookkeeping across supported backends.
use super::super::{
    CronDocuments, JOBS,
    test_support::{FaultDocuments, daily, docs_in},
};
use std::sync::{Arc, atomic::AtomicBool};
use tinyflows_storage::MemoryStorage;

#[tokio::test]
async fn nontransactional_deduplication_and_clear_keep_legacy_behavior() {
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(Arc::new(FaultDocuments {
        inner: docs_in(&backend, "legacy-removal"),
        transactions: false,
        fail_batch: AtomicBool::new(false),
        batch_barrier: Default::default(),
        faults: Default::default(),
    }));
    let keep = a
        .add_shell_job(Some("same".into()), daily(), "a")
        .await
        .unwrap();
    a.add_shell_job(Some("same".into()), daily(), "b")
        .await
        .unwrap();
    assert_eq!(a.dedup_named_jobs().await.unwrap(), 1);
    assert_eq!(a.list_jobs().await.unwrap().len(), 1);
    let snapshot = a.docs.get(JOBS, &keep.id).await.unwrap().unwrap();
    assert_eq!(a.clear_all_jobs().await.unwrap(), 1);
    assert!(!a.remove_unchanged(&snapshot).await.unwrap());
    assert!(!a.remove_current(&snapshot).await.unwrap());
}

#[tokio::test]
async fn current_removal_retries_an_edit_but_spares_a_new_incarnation() {
    use tinyflows_schedule::CronJobPatch;
    let a = super::super::test_support::store();
    let job = a.add_flow_schedule_job("flow", daily()).await.unwrap();
    let snapshot = a.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    a.update_job(
        &job.id,
        CronJobPatch {
            name: Some("edited".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(a.remove_current(&snapshot).await.unwrap());
    a.add_flow_schedule_job("flow", daily()).await.unwrap();
    assert!(!a.remove_current(&snapshot).await.unwrap());
    assert_eq!(a.list_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
async fn deduplication_frees_an_enabled_slot_and_leaves_disabled_jobs_uncounted() {
    let a = super::super::test_support::store();
    a.set_max_active_jobs(Some(2)).await.unwrap();
    a.add_shell_job(Some("same".into()), daily(), "a")
        .await
        .unwrap();
    a.add_shell_job(Some("same".into()), daily(), "b")
        .await
        .unwrap();
    assert_eq!(a.dedup_named_jobs().await.unwrap(), 1);
    a.add_shell_job(None, daily(), "next").await.unwrap();
    assert!(a.add_shell_job(None, daily(), "over cap").await.is_err());
}

#[tokio::test]
async fn legacy_rows_are_imported_before_admission_policy_is_installed() {
    use tinyflows_storage::Precondition;
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(Arc::new(FaultDocuments {
        inner: docs_in(&backend, "legacy-admission"),
        transactions: false,
        fail_batch: AtomicBool::new(false),
        batch_barrier: Default::default(),
        faults: Default::default(),
    }));
    let job = a.add_shell_job(None, daily(), "legacy").await.unwrap();
    let stored = a.docs.get(JOBS, &job.id).await.unwrap().unwrap();
    let mut doc = stored.doc.clone();
    doc.as_object_mut()
        .unwrap()
        .remove(super::super::codec::INCARNATION);
    a.docs
        .put(JOBS, &job.id, doc, Precondition::None)
        .await
        .unwrap();
    let b = CronDocuments::new(docs_in(&backend, "legacy-admission"));
    b.set_max_active_jobs(Some(1)).await.unwrap();
    assert!(b.add_shell_job(None, daily(), "over cap").await.is_err());
    let claim = b
        .claim_due_job(
            &job.id,
            "worker",
            job.next_run,
            chrono::Duration::minutes(1),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(!claim.incarnation().is_empty());
    assert!(
        b.complete_claim(&claim, job.next_run, true, "ok", None)
            .await
            .unwrap()
    );
    b.remove_job(&job.id).await.unwrap();
    b.add_shell_job(None, daily(), "new slot").await.unwrap();
}

#[tokio::test]
async fn a_legacy_row_without_enabled_is_imported_as_disabled() {
    use tinyflows_storage::Precondition;
    let backend = MemoryStorage::new();
    let a = CronDocuments::new(Arc::new(FaultDocuments {
        inner: docs_in(&backend, "legacy-disabled"),
        transactions: false,
        fail_batch: AtomicBool::new(false),
        batch_barrier: Default::default(),
        faults: Default::default(),
    }));
    let job = a.add_shell_job(None, daily(), "legacy").await.unwrap();
    let mut doc = a.docs.get(JOBS, &job.id).await.unwrap().unwrap().doc;
    doc.as_object_mut().unwrap().remove("enabled");
    a.docs
        .put(JOBS, &job.id, doc, Precondition::None)
        .await
        .unwrap();
    let b = CronDocuments::new(docs_in(&backend, "legacy-disabled"));
    assert!(!b.get_job(&job.id).await.unwrap().enabled);
    b.set_max_active_jobs(Some(1)).await.unwrap();
    b.add_shell_job(None, daily(), "enabled").await.unwrap();
    assert_eq!(b.list_jobs().await.unwrap().len(), 2);
}
