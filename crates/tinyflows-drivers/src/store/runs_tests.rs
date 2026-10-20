//! Run-record listing: newest first, scoped to their workflow, and a run that
//! was never recorded is a distinct error rather than a silent `None`.

use super::test_support::{block_on, store};
use tinyflows::store::types::{RunExecutor, RunStatus, WorkflowError};
use tinyflows::store::{WorkflowStore, new_run_record, require_run};

#[test]
fn runs_are_listed_newest_first_and_scoped_to_their_workflow() {
    let store = store();
    store
        .record_run(&new_run_record("r1", "alpha", 100))
        .unwrap();
    store
        .record_run(&new_run_record("r2", "alpha", 300))
        .unwrap();
    store
        .record_run(&new_run_record("r3", "beta", 200))
        .unwrap();

    let alpha = store.list_runs("alpha").unwrap();
    let ids: Vec<&str> = alpha.iter().map(|r| r.id.as_str()).collect();

    assert_eq!(ids, vec!["r2", "r1"], "newest first");
    assert_eq!(store.list_runs("beta").unwrap().len(), 1);
    assert_eq!(store.list_runs("unknown").unwrap().len(), 0);
}

#[test]
fn a_run_record_survives_being_rewritten_as_it_settles() {
    let store = store();
    let mut run = new_run_record("r1", "alpha", 100);
    store.record_run(&run).unwrap();

    run.status = RunStatus::PendingApproval;
    run.pending_approvals = vec!["review".into()];
    store.record_run(&run).unwrap();

    let loaded = require_run(&store, "r1").expect("found");
    assert_eq!(loaded.status, RunStatus::PendingApproval);
    assert_eq!(loaded.pending_approvals, vec!["review".to_string()]);
    assert!(!loaded.status.is_settled(), "an approval gate is resumable");
    assert_eq!(
        store.list_runs("alpha").unwrap().len(),
        1,
        "replaced, not added"
    );
}

#[test]
fn asking_for_a_run_that_was_never_recorded_is_an_error_not_a_silent_none() {
    let err = require_run(&store(), "ghost").expect_err("no such run");
    assert!(matches!(err, WorkflowError::RunNotFound(_)), "got {err:?}");
    assert!(store().get_run("ghost").unwrap().is_none());
}

#[test]
fn unsettled_runs_spans_every_workflow_and_skips_finished_ones() {
    // What a reconciliation sweep reads. It has to cross workflow boundaries —
    // "which records still claim to be live" is a question about the whole
    // scope — and it must not hand back runs that already have an outcome.
    let store = store();
    store
        .record_run(&new_run_record("live-alpha", "alpha", 100))
        .unwrap();
    store
        .record_run(&new_run_record("live-beta", "beta", 300))
        .unwrap();
    let mut parked = new_run_record("parked", "alpha", 200);
    parked.status = RunStatus::PendingApproval;
    store.record_run(&parked).unwrap();
    for (id, status) in [
        ("done", RunStatus::Succeeded),
        ("broke", RunStatus::Failed),
        ("stopped", RunStatus::Cancelled),
        ("cut-off", RunStatus::Interrupted),
    ] {
        let mut record = new_run_record(id, "alpha", 50);
        record.status = status;
        store.record_run(&record).unwrap();
    }

    let unsettled = store.unsettled_runs().unwrap();
    let ids: Vec<&str> = unsettled.iter().map(|r| r.id.as_str()).collect();

    assert_eq!(
        ids,
        vec!["live-beta", "parked", "live-alpha"],
        "newest first, both workflows, nothing settled"
    );
}

#[test]
fn a_run_that_settles_leaves_the_unsettled_listing() {
    let store = store();
    let mut run = new_run_record("r1", "alpha", 100);
    store.record_run(&run).unwrap();
    assert_eq!(store.unsettled_runs().unwrap().len(), 1);
    run.status = RunStatus::Succeeded;
    store.record_run(&run).unwrap();
    assert!(store.unsettled_runs().unwrap().is_empty());
}

#[test]
fn a_scope_that_has_never_run_anything_has_no_unsettled_runs() {
    assert!(store().unsettled_runs().unwrap().is_empty());
}

#[test]
fn an_executor_and_a_cancel_request_survive_a_write_and_read() {
    let store = store();
    let mut record = new_run_record("owned", "alpha", 100).with_executor(Some(RunExecutor {
        host: "somewhere".to_string(),
        pid: 4711,
        started_at_secs: Some(1_700_000_000),
    }));
    record.cancel_requested = true;

    store.record_run(&record).unwrap();

    let read = store.get_run("owned").unwrap().unwrap();
    assert_eq!(read.executor, record.executor);
    assert!(read.cancel_requested);
}

#[test]
fn an_unreadable_run_document_is_skipped_by_listings_and_named_by_a_direct_read() {
    use serde_json::json;
    use tinyflows_storage::{MemoryStorage, Precondition};
    let storage = MemoryStorage::new();
    let docs = super::test_support::docs_in(&storage, "local");
    let store = super::DriverWorkflowStore::new(docs.clone());
    store
        .record_run(&new_run_record("good", "alpha", 100))
        .unwrap();
    block_on(docs.put(
        super::RUNS,
        "bad",
        json!({ "workflow_id": "alpha", "started_at": 5, "settled": false, "run_json": "{not json" }),
        Precondition::None,
    ))
    .unwrap();

    let ids: Vec<String> = store
        .list_runs("alpha")
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(ids, ["good"]);
    assert!(matches!(
        store.get_run("bad").unwrap_err(),
        WorkflowError::Malformed(_)
    ));
}
