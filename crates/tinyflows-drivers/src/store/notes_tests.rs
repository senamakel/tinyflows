//! The journal on documents: ordering, scoping, supersession, the cap, and what
//! happens when things go wrong: a journal that cannot be parsed, an id that is
//! not usable, concurrent appenders.

use serde_json::json;
use tinyflows::store::types::{NoteKind, NoteSource, WorkflowError, WorkflowNote};
use tinyflows::store::{MAX_NOTES, WorkflowStore, current_notes, mint_note_id};
use tinyflows_storage::{DocumentStoreExt, Precondition, Query};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

use super::test_support::{block_on, docs_in, store, store_over};
use super::{DriverWorkflowStore, NOTES, QUARANTINE};

fn note(workflow_id: &str, recorded_at: u64, text: &str) -> WorkflowNote {
    WorkflowNote {
        id: mint_note_id(recorded_at),
        workflow_id: workflow_id.to_string(),
        kind: NoteKind::Observation,
        text: text.to_string(),
        recorded_at,
        source: NoteSource::System,
        run_ids: Vec::new(),
        superseded_by: None,
        pinned: false,
    }
}

#[test]
fn notes_come_back_newest_first() {
    let store = store();
    for (at, text) in [(1, "first"), (2, "second"), (3, "third")] {
        store.append_note(&note("sweep", at, text)).unwrap();
    }
    let listed = store.list_notes("sweep").unwrap();
    let texts: Vec<&str> = listed.iter().map(|n| n.text.as_str()).collect();
    assert_eq!(texts, ["third", "second", "first"]);
}

#[test]
fn a_workflow_with_no_journal_has_no_notes_rather_than_an_error() {
    assert!(store().list_notes("never-written").unwrap().is_empty());
}

#[test]
fn journals_are_kept_apart_by_workflow() {
    let store = store();
    store.append_note(&note("sweep", 1, "about sweep")).unwrap();
    store
        .append_note(&note("deploy", 1, "about deploy"))
        .unwrap();
    assert_eq!(store.list_notes("sweep").unwrap().len(), 1);
    assert_eq!(store.list_notes("deploy").unwrap()[0].text, "about deploy");
}

#[test]
fn journals_are_scoped_like_every_other_document() {
    let storage = MemoryStorage::new();
    let alice = store_over(&storage, "alice");
    let bob = store_over(&storage, "bob");
    alice.append_note(&note("sweep", 1, "private")).unwrap();
    assert!(bob.list_notes("sweep").unwrap().is_empty());
}

#[test]
fn superseding_marks_the_note_without_removing_it() {
    let store = store();
    let first = note("sweep", 1, "the timeout is too short");
    let second = note("sweep", 2, "the timeout was never the problem");
    store.append_note(&first).unwrap();
    store.append_note(&second).unwrap();

    assert!(
        store
            .supersede_note("sweep", &first.id, &second.id)
            .unwrap()
    );

    let listed = store.list_notes("sweep").unwrap();
    assert_eq!(listed.len(), 2, "history keeps the superseded note");
    let superseded = listed.iter().find(|n| n.id == first.id).unwrap();
    assert_eq!(
        superseded.superseded_by.as_deref(),
        Some(second.id.as_str())
    );
    assert!(!superseded.is_current());
    let current = current_notes(&store, "sweep").unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].id, second.id);
}

#[test]
fn superseding_twice_changes_nothing_the_second_time() {
    let store = store();
    let first = note("sweep", 1, "a");
    store.append_note(&first).unwrap();
    assert!(store.supersede_note("sweep", &first.id, "x").unwrap());
    assert!(!store.supersede_note("sweep", &first.id, "y").unwrap());
    let listed = store.list_notes("sweep").unwrap();
    assert_eq!(listed[0].superseded_by.as_deref(), Some("x"));
}

#[test]
fn superseding_a_note_that_is_not_there_is_not_a_failure() {
    let store = store();
    store.append_note(&note("sweep", 1, "something")).unwrap();
    assert!(
        !store
            .supersede_note("sweep", "no-such-note", "whatever")
            .unwrap()
    );
    assert!(!store.supersede_note("no-such-workflow", "n", "w").unwrap());
}

#[test]
fn a_workflow_id_that_is_not_usable_is_refused() {
    let store = store();
    let bad = note("", 1, "nope");
    assert!(matches!(
        store.append_note(&bad),
        Err(WorkflowError::Malformed(_))
    ));
    assert!(store.list_notes("").is_err());
}

#[test]
fn an_unreadable_journal_reads_as_empty_and_is_set_aside_not_destroyed() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    store.append_note(&note("sweep", 1, "something")).unwrap();
    block_on(docs.put(
        NOTES,
        "sweep",
        json!({ "workflow_id": "sweep", "notes_json": "{ this is not json" }),
        Precondition::None,
    ))
    .unwrap();

    // One bad document must not make the workflow unreadable everywhere its
    // notes are shown.
    assert!(store.list_notes("sweep").unwrap().is_empty());

    let kept = block_on(docs.query_all(QUARANTINE, &Query::all())).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(kept[0].doc["workflow_id"], "sweep");
    assert_eq!(
        kept[0].doc["journal"]["notes_json"], "{ this is not json",
        "what an operator could still recover is preserved"
    );
    // Moved, not copied: reading again does not set it aside a second time.
    assert!(store.list_notes("sweep").unwrap().is_empty());
    assert_eq!(
        block_on(docs.query_all(QUARANTINE, &Query::all()))
            .unwrap()
            .len(),
        1
    );
    // And the journal is usable again from scratch.
    store.append_note(&note("sweep", 2, "fresh")).unwrap();
    assert_eq!(store.list_notes("sweep").unwrap().len(), 1);
}

#[test]
fn repeated_corruption_preserves_each_quarantined_journal() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    for body in ["{ first corruption", "{ second corruption"] {
        block_on(docs.put(
            NOTES,
            "sweep",
            json!({ "workflow_id": "sweep", "notes_json": body }),
            Precondition::None,
        ))
        .unwrap();
        assert!(store.list_notes("sweep").unwrap().is_empty());
    }
    assert_eq!(
        block_on(docs.query_all(QUARANTINE, &Query::all()))
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn a_journal_document_without_notes_is_treated_as_unreadable() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    block_on(docs.put(
        NOTES,
        "sweep",
        json!({ "workflow_id": "sweep" }),
        Precondition::None,
    ))
    .unwrap();
    assert!(store.list_notes("sweep").unwrap().is_empty());
    assert_eq!(
        block_on(docs.query_all(QUARANTINE, &Query::all()))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn the_journal_is_capped_and_drops_the_oldest_first() {
    let store = store();
    for at in 0..(MAX_NOTES as u64 + 5) {
        store
            .append_note(&note("sweep", at, &format!("note {at}")))
            .unwrap();
    }
    let listed = store.list_notes("sweep").unwrap();
    assert_eq!(listed.len(), MAX_NOTES);
    assert_eq!(
        listed.last().unwrap().text,
        "note 5",
        "the five oldest went, not the five newest"
    );
}

#[test]
fn pinned_notes_survive_the_cap() {
    let store = store();
    let mut pinned = note("sweep", 0, "what the operator said");
    pinned.pinned = true;
    store.append_note(&pinned).unwrap();
    for at in 1..(MAX_NOTES as u64 + 20) {
        store
            .append_note(&note("sweep", at, &format!("note {at}")))
            .unwrap();
    }
    let listed = store.list_notes("sweep").unwrap();
    assert_eq!(listed.len(), MAX_NOTES);
    assert!(listed.iter().any(|n| n.id == pinned.id));
}

#[test]
fn concurrent_appenders_do_not_overwrite_each_other() {
    let store = store();
    let gate = std::sync::Arc::new(std::sync::Barrier::new(16));
    let writers: Vec<_> = (0..16)
        .map(|at| {
            let (store, gate) = (store.clone(), gate.clone());
            std::thread::spawn(move || {
                gate.wait();
                store
                    .append_note(&note("sweep", at, &format!("note {at}")))
                    .unwrap();
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    assert_eq!(store.list_notes("sweep").unwrap().len(), 16);
}

#[test]
fn separate_store_instances_append_to_one_journal() {
    let storage = MemoryStorage::new();
    let first = store_over(&storage, "local");
    let second = store_over(&storage, "local");
    first.append_note(&note("sweep", 1, "one")).unwrap();
    second.append_note(&note("sweep", 2, "two")).unwrap();
    assert_eq!(first.list_notes("sweep").unwrap().len(), 2);
}

#[test]
fn a_note_round_trips_every_field() {
    let store = store();
    let written = WorkflowNote {
        id: mint_note_id(7),
        workflow_id: "sweep".into(),
        kind: NoteKind::Constraint,
        text: "the deploy step must never run before tests".into(),
        recorded_at: 7,
        source: NoteSource::Agent {
            model: Some("claude-opus-5".into()),
        },
        run_ids: vec!["run-1".into(), "run-2".into()],
        superseded_by: None,
        pinned: true,
    };
    store.append_note(&written).unwrap();
    assert_eq!(store.list_notes("sweep").unwrap().remove(0), written);
}
