use serde_json::json;
use tinyflows::store::types::{
    NoteKind, NoteSource, ProposalStatus, RunStatus, WorkflowError, WorkflowNote, WorkflowProposal,
    WorkflowRevision,
};
use tinyflows::store::{MAX_NOTES, MAX_REVISIONS, WorkflowStore, mint_note_id, new_run_record};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

use crate::store::test_support::{record, store, store_over};

fn note(id: u64, text: &str) -> WorkflowNote {
    WorkflowNote {
        id: mint_note_id(id),
        workflow_id: "sweep".to_string(),
        kind: NoteKind::Observation,
        text: text.to_string(),
        recorded_at: id,
        source: NoteSource::System,
        run_ids: Vec::new(),
        superseded_by: None,
        pinned: false,
    }
}

#[tokio::test]
async fn an_imported_workflow_keeps_its_id_and_fields() {
    let store = store();
    let mut original = record("legacy", "from a file");
    original.enabled = false;
    original.defaults.harness = Some("codex".into());
    assert!(store.import_workflow(&original).await.unwrap());
    let loaded = store.get("legacy").unwrap().unwrap();
    assert_eq!(loaded, original);
}

#[tokio::test]
async fn importing_a_workflow_twice_keeps_the_first_copy_and_leaves_no_history() {
    let store = store();
    assert!(store.import_workflow(&record("w", "first")).await.unwrap());
    assert!(!store.import_workflow(&record("w", "second")).await.unwrap());
    assert_eq!(store.get("w").unwrap().unwrap().description, "first");
    assert!(store.list_revisions("w").unwrap().is_empty());
}

#[tokio::test]
async fn an_import_never_replaces_a_workflow_the_store_already_holds() {
    let store = store();
    store.save(&record("w", "live")).unwrap();
    assert!(!store.import_workflow(&record("w", "stale")).await.unwrap());
    assert_eq!(store.get("w").unwrap().unwrap().description, "live");
}

#[tokio::test]
async fn an_imported_workflow_that_would_not_compile_is_refused() {
    let store = store();
    let mut broken = record("broken", "x");
    broken.graph.nodes.clear();
    assert!(matches!(
        store.import_workflow(&broken).await.unwrap_err(),
        WorkflowError::Invalid { .. }
    ));
    assert!(store.list().unwrap().is_empty());
}

#[tokio::test]
async fn imported_revisions_are_listed_newest_first_and_scoped_to_their_workflow() {
    let store = store();
    store.import_workflow(&record("w", "now")).await.unwrap();
    let older = WorkflowRevision {
        id: "0000000000001-000000000000-a".to_string(),
        superseded_at: 1,
        record: record("w", "older"),
    };
    let newer = WorkflowRevision {
        id: "0000000000002-000000000000-b".to_string(),
        superseded_at: 2,
        record: record("w", "newer"),
    };
    assert!(store.import_revision(&older).await.unwrap());
    assert!(store.import_revision(&newer).await.unwrap());
    assert!(!store.import_revision(&newer).await.unwrap());

    let history = store.list_revisions("w").unwrap();
    let descriptions: Vec<&str> = history
        .iter()
        .map(|r| r.record.description.as_str())
        .collect();
    assert_eq!(descriptions, ["newer", "older"]);
    assert_eq!(history[1].superseded_at, 1);
    assert!(store.list_revisions("other").unwrap().is_empty());
}

#[tokio::test]
async fn imported_history_is_pruned_to_the_cap() {
    let store = store();
    for n in 0..(MAX_REVISIONS as u64 + 4) {
        let revision = WorkflowRevision {
            id: format!("{n:013}-000000000000-x"),
            superseded_at: n,
            record: record("w", &format!("v{n}")),
        };
        store.import_revision(&revision).await.unwrap();
    }
    assert_eq!(store.list_revisions("w").unwrap().len(), MAX_REVISIONS);
}

#[tokio::test]
async fn imported_runs_keep_their_ids_and_unsettled_state() {
    let store = store();
    let mut settled = new_run_record("done", "w", 100);
    settled.status = RunStatus::Succeeded;
    let live = new_run_record("live", "w", 200);
    assert!(store.import_run(&settled).await.unwrap());
    assert!(store.import_run(&live).await.unwrap());
    assert!(!store.import_run(&live).await.unwrap());
    assert_eq!(
        store.get_run("done").unwrap().unwrap().status,
        RunStatus::Succeeded
    );
    let unsettled = store.unsettled_runs().unwrap();
    assert_eq!(unsettled.len(), 1);
    assert_eq!(unsettled[0].id, "live");
}

#[tokio::test]
async fn imported_notes_merge_into_the_journal_without_duplicating_or_overwriting() {
    let store = store();
    let first = note(1, "first");
    store.append_note(&first).unwrap();
    let mut changed = first.clone();
    changed.text = "changed in the legacy copy".into();
    let second = note(2, "second");

    let added = store
        .import_notes("sweep", &[changed, second.clone()])
        .await
        .unwrap();

    assert_eq!(added, 1, "the note the journal had is kept as it was");
    let listed = store.list_notes("sweep").unwrap();
    assert_eq!(listed.len(), 2);
    assert_eq!(
        listed.iter().find(|n| n.id == first.id).unwrap().text,
        "first"
    );
    // Importing the same notes again adds nothing.
    assert_eq!(store.import_notes("sweep", &[second]).await.unwrap(), 0);
}

#[tokio::test]
async fn imported_notes_for_another_workflow_are_left_out() {
    let store = store();
    let mut stray = note(1, "stray");
    stray.workflow_id = "other".into();
    assert_eq!(store.import_notes("sweep", &[stray]).await.unwrap(), 0);
    assert!(store.list_notes("sweep").unwrap().is_empty());
}

#[tokio::test]
async fn imported_notes_are_pruned_to_the_cap() {
    let store = store();
    let notes: Vec<_> = (0..(MAX_NOTES as u64 + 10))
        .map(|at| note(at, &format!("note {at}")))
        .collect();
    store.import_notes("sweep", &notes).await.unwrap();
    assert_eq!(store.list_notes("sweep").unwrap().len(), MAX_NOTES);
}

#[tokio::test]
async fn an_imported_proposal_keeps_its_id_and_state() {
    let store = store();
    let proposal = WorkflowProposal {
        id: "0000000000001-000000000000-p".to_string(),
        workflow_id: "w".to_string(),
        created_at: 1,
        rationale: "because".into(),
        ops: json!([{ "op": "from_the_future" }]),
        evidence_runs: vec![],
        note_ids: vec![],
        base_fingerprint: "fp".into(),
        verification: None,
        status: ProposalStatus::Rejected,
        decided_at: Some(5),
        decision_reason: Some("no".into()),
    };
    assert!(store.import_proposal(&proposal).await.unwrap());
    assert!(!store.import_proposal(&proposal).await.unwrap());
    assert_eq!(
        store.get_proposal(&proposal.id).unwrap(),
        Some(proposal.clone())
    );
    assert_eq!(store.list_proposals("w").unwrap(), vec![proposal]);
}

#[tokio::test]
async fn imports_land_in_the_scope_they_were_given() {
    let storage = MemoryStorage::new();
    let alice = store_over(&storage, "alice");
    let bob = store_over(&storage, "bob");
    alice.import_workflow(&record("w", "x")).await.unwrap();
    assert!(bob.get("w").unwrap().is_none());
    assert!(bob.import_workflow(&record("w", "y")).await.unwrap());
}

#[test]
fn import_helpers_are_usable_without_a_runtime_of_the_callers_own() {
    // The synchronous trait still works on the same store afterwards.
    let store = store();
    store.save(&record("sync", "x")).unwrap();
    assert!(store.get("sync").unwrap().is_some());
}
