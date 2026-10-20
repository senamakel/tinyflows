use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
//! Proposals on documents. A proposal is the one artifact in this feature that
//! can change a saved graph, so what is asserted here is mostly about *not*
//! doing that by accident: a listing that cannot leak another workflow's
//! proposals, a decision that replaces rather than adds, a save that refuses
//! when the graph moved underneath it.

use std::sync::atomic::Ordering;

use serde_json::json;
use tinyflows::store::types::{ProposalStatus, WorkflowError, WorkflowProposal, fingerprint};
use tinyflows::store::{WorkflowStore, mint_proposal_id, require_proposal};
use tinyflows_storage::{DocumentStoreExt,  Precondition};

use super::test_support::{Faulty, block_on, docs_in, record, store, store_over};
use super::{DEFINITIONS, DriverWorkflowStore, PROPOSALS};

fn proposal(workflow_id: &str, created_at: u64) -> WorkflowProposal {
    WorkflowProposal {
        id: mint_proposal_id(created_at),
        workflow_id: workflow_id.to_string(),
        created_at,
        rationale: "the timeout is too short for a cold cache".into(),
        ops: json!([{ "op": "update_node_config", "id": "build", "config": { "timeout": 600 } }]),
        evidence_runs: vec!["run-1".into()],
        note_ids: Vec::new(),
        base_fingerprint: "abc123".into(),
        verification: None,
        status: ProposalStatus::Pending,
        decided_at: None,
        decision_reason: None,
    }
}

#[test]
fn a_proposal_round_trips() {
    let store = store();
    let written = proposal("sweep", 1);
    store.save_proposal(&written).unwrap();
    assert_eq!(
        store.get_proposal(&written.id).unwrap(),
        Some(written.clone())
    );
    assert_eq!(require_proposal(&store, &written.id).unwrap(), written);
}

#[test]
fn an_unknown_proposal_is_none_rather_than_an_error() {
    let store = store();
    assert!(store.get_proposal("no-such-proposal").unwrap().is_none());
    assert!(matches!(
        require_proposal(&store, "no-such-proposal").unwrap_err(),
        WorkflowError::Malformed(_)
    ));
}

#[test]
fn listing_is_scoped_to_one_workflow() {
    let store = store();
    store.save_proposal(&proposal("sweep", 1)).unwrap();
    store.save_proposal(&proposal("sweep", 2)).unwrap();
    store.save_proposal(&proposal("deploy", 3)).unwrap();
    assert_eq!(store.list_proposals("sweep").unwrap().len(), 2);
    assert_eq!(store.list_proposals("deploy").unwrap().len(), 1);
    assert!(store.list_proposals("unrelated").unwrap().is_empty());
}

#[test]
fn proposals_come_back_newest_first() {
    let store = store();
    let first = proposal("sweep", 1);
    let second = proposal("sweep", 2);
    store.save_proposal(&first).unwrap();
    store.save_proposal(&second).unwrap();
    let listed = store.list_proposals("sweep").unwrap();
    assert_eq!(listed[0].id, second.id);
    assert_eq!(listed[1].id, first.id);
}

#[test]
fn a_decision_replaces_the_document_rather_than_adding_one() {
    let store = store();
    let mut written = proposal("sweep", 1);
    store.save_proposal(&written).unwrap();

    written.status = ProposalStatus::Rejected;
    written.decided_at = Some(9);
    written.decision_reason = Some("the cache is the real problem".into());
    store.save_proposal(&written).unwrap();

    let listed = store.list_proposals("sweep").unwrap();
    assert_eq!(
        listed.len(),
        1,
        "a decision is a state change, not a new row"
    );
    assert_eq!(listed[0].status, ProposalStatus::Rejected);
    assert_eq!(
        listed[0].decision_reason.as_deref(),
        Some("the cache is the real problem")
    );
}

#[test]
fn proposals_are_scoped_like_every_other_document() {
    let storage = MemoryStorage::new();
    let alice = store_over(&storage, "alice");
    let bob = store_over(&storage, "bob");
    let written = proposal("sweep", 1);
    alice.save_proposal(&written).unwrap();
    assert!(bob.get_proposal(&written.id).unwrap().is_none());
    assert!(bob.list_proposals("sweep").unwrap().is_empty());
}

#[test]
fn an_unreadable_proposal_is_skipped_by_the_listing_and_named_by_a_direct_read() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    let good = proposal("sweep", 1);
    store.save_proposal(&good).unwrap();
    block_on(docs.put(
        PROPOSALS,
        "broken",
        json!({ "workflow_id": "sweep", "created_at": 5, "proposal_json": "not json at all" }),
        Precondition::None,
    ))
    .unwrap();

    let listed = store.list_proposals("sweep").unwrap();
    assert_eq!(listed.len(), 1, "one bad document is not a failure");
    assert_eq!(listed[0].id, good.id);
    assert!(matches!(
        store.get_proposal("broken").unwrap_err(),
        WorkflowError::Malformed(_)
    ));
}

#[test]
fn a_proposal_id_that_is_not_usable_is_refused() {
    let store = store();
    let mut bad = proposal("sweep", 1);
    bad.id = String::new();
    assert!(store.save_proposal(&bad).is_err());
    assert!(store.get_proposal("").is_err());
}

#[test]
fn ops_survive_as_the_json_they_arrived_as() {
    // The reason `ops` is a raw `Value`: a stored proposal has to stay readable
    // even if the engine's op enum changes shape under it.
    let store = store();
    let mut written = proposal("sweep", 1);
    written.ops = json!([{ "op": "some_future_op", "wholly": { "unknown": ["shape"] } }]);
    store.save_proposal(&written).unwrap();
    assert_eq!(
        store.get_proposal(&written.id).unwrap().unwrap().ops,
        written.ops
    );
}

#[test]
fn a_proposal_is_saved_while_its_workflow_still_has_the_fingerprint() {
    let store = store();
    let saved = record("sweep", "x");
    store.save(&saved).unwrap();
    let pending = proposal("sweep", 1);

    assert!(
        store
            .save_proposal_if_fingerprint(&pending, &fingerprint(&saved.graph))
            .unwrap()
    );
    assert!(store.get_proposal(&pending.id).unwrap().is_some());
}

#[test]
fn a_proposal_is_refused_when_the_graph_has_moved() {
    let store = store();
    store.save(&record("sweep", "x")).unwrap();
    let pending = proposal("sweep", 1);

    assert!(
        !store
            .save_proposal_if_fingerprint(&pending, "fingerprint-of-an-older-graph")
            .unwrap()
    );
    assert!(store.get_proposal(&pending.id).unwrap().is_none());
}

#[test]
fn a_proposal_for_a_workflow_that_does_not_exist_is_refused() {
    let store = store();
    assert!(
        !store
            .save_proposal_if_fingerprint(&proposal("ghost", 1), "anything")
            .unwrap()
    );
}

#[test]
fn a_proposal_for_a_definition_that_cannot_be_read_is_refused() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    block_on(docs.put(
        DEFINITIONS,
        "sweep",
        json!({ "id": "sweep", "record_json": "{broken" }),
        Precondition::None,
    ))
    .unwrap();
    assert!(
        !store
            .save_proposal_if_fingerprint(&proposal("sweep", 1), "anything")
            .unwrap()
    );
}

#[test]
fn without_atomic_batches_the_check_and_the_write_are_separate_but_still_work() {
    let storage = MemoryStorage::new();
    let faulty = Faulty::wrap_with(docs_in(&storage, "local"), PROPOSALS, false);
    let store = DriverWorkflowStore::new(faulty);
    let saved = record("sweep", "x");
    store.save(&saved).unwrap();
    let pending = proposal("sweep", 1);
    assert!(
        store
            .save_proposal_if_fingerprint(&pending, &fingerprint(&saved.graph))
            .unwrap()
    );
    assert!(store.get_proposal(&pending.id).unwrap().is_some());
    assert!(
        !store
            .save_proposal_if_fingerprint(&proposal("sweep", 2), "stale")
            .unwrap()
    );
}

#[test]
fn a_failing_batch_is_reported_not_swallowed() {
    let storage = MemoryStorage::new();
    let faulty = Faulty::wrap(docs_in(&storage, "local"), PROPOSALS);
    let store = DriverWorkflowStore::new(faulty.clone());
    let saved = record("sweep", "x");
    store.save(&saved).unwrap();
    faulty.fail_batches.store(true, Ordering::SeqCst);

    let err = store
        .save_proposal_if_fingerprint(&proposal("sweep", 1), &fingerprint(&saved.graph))
        .unwrap_err();

    assert!(matches!(err, WorkflowError::Storage(_)), "got {err:?}");
    assert!(store.list_proposals("sweep").unwrap().is_empty());
}

#[test]
fn an_edit_that_lands_between_the_check_and_the_write_refuses_the_proposal() {
    // Fencing the definition in the same batch is what closes the gap: the
    // definition was edited after the proposal's fingerprint was checked, so the
    // batch must fail rather than land a proposal against a graph that moved.
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    let mut saved = record("sweep", "x");
    store.save(&saved).unwrap();
    let observed = fingerprint(&saved.graph);
    let pending = proposal("sweep", 1);

    // Another writer edits the graph as the proposal is being saved.
    let racing = store.clone();
    saved.graph.nodes[1].name = "renamed meanwhile".into();
    racing.save(&saved).unwrap();

    assert!(
        !store
            .save_proposal_if_fingerprint(&pending, &observed)
            .unwrap(),
        "the graph moved, so the observation is stale"
    );
    assert!(
        block_on(docs.query_all(PROPOSALS, &tinyflows_storage::Query::all()))
            .unwrap()
            .is_empty()
    );
}
