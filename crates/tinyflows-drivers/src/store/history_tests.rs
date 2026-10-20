use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
//! Revision snapshotting, undo, and rollback: saving over a workflow snapshots
//! what it replaced, undo restores (and is itself undoable), a named rollback
//! restores exactly that revision, and history never leaks into the ordinary
//! workflow listing.

use super::test_support::{record, store, store_over};
use tinyflows::store::types::WorkflowError;
use tinyflows::store::{MAX_REVISIONS, WorkflowStore, require, rollback, undo_last};
use tinyflows_storage::MemoryStorage;

#[test]
fn a_workflow_saved_for_the_first_time_has_no_history_to_go_back_to() {
    let store = store();
    store.save(&record("greet", "says hello")).unwrap();

    // Nothing was replaced, so nothing was superseded.
    assert!(store.list_revisions("greet").unwrap().is_empty());
    assert!(undo_last(&store, "greet").unwrap().is_none());
}

#[test]
fn saving_over_a_workflow_snapshots_the_version_it_replaced() {
    let store = store();
    let mut record = record("greet", "says hello");
    store.save(&record).unwrap();

    record.description = "rewritten by the copilot".into();
    store.save(&record).unwrap();

    let history = store.list_revisions("greet").unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].record.description, "says hello");
    assert!(history[0].superseded_at > 0);
}

#[test]
fn two_snapshots_taken_in_the_same_millisecond_are_both_kept_in_order() {
    let store = store();
    let mut record = record("greet", "v0");
    store.save(&record).unwrap();
    for n in 1..=5 {
        record.description = format!("v{n}");
        store.save(&record).unwrap();
    }
    let descriptions: Vec<String> = store
        .list_revisions("greet")
        .unwrap()
        .into_iter()
        .map(|r| r.record.description)
        .collect();
    assert_eq!(descriptions, ["v4", "v3", "v2", "v1", "v0"], "newest first");
}

#[test]
fn history_is_capped_and_the_oldest_go_first() {
    let store = store();
    let mut record = record("greet", "v0");
    store.save(&record).unwrap();
    for n in 1..=(MAX_REVISIONS + 5) {
        record.description = format!("v{n}");
        store.save(&record).unwrap();
    }
    let history = store.list_revisions("greet").unwrap();
    assert_eq!(history.len(), MAX_REVISIONS);
    assert_eq!(
        history.first().unwrap().record.description,
        format!("v{}", MAX_REVISIONS + 4),
        "the newest superseded copy is kept"
    );
    assert_eq!(
        history.last().unwrap().record.description,
        "v5",
        "the five oldest went"
    );
}

#[test]
fn one_workflow_cannot_read_another_workflows_history() {
    let store = store();
    let mut greet = record("greet", "a");
    store.save(&greet).unwrap();
    greet.description = "b".into();
    store.save(&greet).unwrap();
    let revision = store.list_revisions("greet").unwrap().remove(0);

    assert!(store.revision("greet", &revision.id).unwrap().is_some());
    assert!(store.revision("other", &revision.id).unwrap().is_none());
    assert!(store.list_revisions("other").unwrap().is_empty());
}

#[test]
fn a_snapshot_forgets_where_the_record_was_read_from() {
    let store = store();
    let mut record = record("greet", "a");
    record.source_path = Some("/somewhere/greet.json".into());
    store.save(&record).unwrap();
    record.description = "b".into();
    store.save(&record).unwrap();

    let revision = store.list_revisions("greet").unwrap().remove(0);
    assert!(revision.record.source_path.is_none());
    assert!(store.get("greet").unwrap().unwrap().source_path.is_none());
}

#[test]
fn history_is_shared_by_every_store_instance_over_the_same_storage() {
    let storage = MemoryStorage::new();
    let first = store_over(&storage, "local");
    let second = store_over(&storage, "local");
    let mut greet = record("greet", "original");
    first.save(&greet).unwrap();
    greet.description = "edit".into();
    second.save(&greet).unwrap();

    assert_eq!(first.list_revisions("greet").unwrap().len(), 1);
    assert_eq!(second.list_revisions("greet").unwrap().len(), 1);
}

#[test]
fn scopes_keep_their_workflows_and_history_apart() {
    let storage = MemoryStorage::new();
    let alice = store_over(&storage, "alice");
    let bob = store_over(&storage, "bob");
    let mut greet = record("greet", "alice original");
    alice.save(&greet).unwrap();
    greet.description = "alice edit".into();
    alice.save(&greet).unwrap();

    assert!(bob.list().unwrap().is_empty());
    assert!(bob.get("greet").unwrap().is_none());
    assert!(bob.list_revisions("greet").unwrap().is_empty());
}

#[test]
fn undo_restores_the_previous_version_and_is_itself_undoable() {
    let store = store();
    let mut record = record("greet", "says hello");
    store.save(&record).unwrap();
    record.description = "rewritten by the copilot".into();
    store.save(&record).unwrap();

    let (revision, restored) = undo_last(&store, "greet").unwrap().expect("history");

    assert_eq!(restored.description, "says hello");
    assert_eq!(
        store.get("greet").unwrap().unwrap().description,
        "says hello"
    );
    // The rollback went through `save`, so the version it replaced was
    // snapshotted too: pressing undo twice returns to where you started.
    let history = store.list_revisions("greet").unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].record.description, "rewritten by the copilot");
    assert_ne!(history[0].id, revision.id);
}

#[test]
fn rolling_back_to_a_named_revision_restores_exactly_that_one() {
    let store = store();
    let mut record = record("greet", "zero");
    store.save(&record).unwrap();
    for description in ["first", "second", "third"] {
        record.description = description.into();
        store.save(&record).unwrap();
    }

    // Three saves over a first one: history holds the superseded versions,
    // newest first, and the last entry is the oldest of those.
    let history = store.list_revisions("greet").unwrap();
    assert_eq!(history.len(), 3);
    let oldest = history.last().expect("history");
    let restored = rollback(&store, "greet", &oldest.id).unwrap();

    assert_eq!(restored.description, "zero");
}

#[test]
fn rolling_back_to_a_revision_that_does_not_exist_is_an_error() {
    let store = store();
    store.save(&record("greet", "x")).unwrap();

    let err = rollback(&store, "greet", "no-such-revision").expect_err("must refuse");

    assert!(matches!(err, WorkflowError::Malformed(_)), "got {err:?}");
}

#[test]
fn deleting_a_workflow_leaves_it_recoverable() {
    let store = store();
    store.save(&record("greet", "x")).unwrap();

    store.delete("greet").unwrap();

    // A delete has nothing left to diff against, which is exactly why it is
    // snapshotted: without this it is the one edit that cannot be taken back.
    assert!(store.get("greet").unwrap().is_none());
    let history = store.list_revisions("greet").unwrap();
    assert_eq!(history.len(), 1);
    rollback(&store, "greet", &history[0].id).unwrap();
    assert!(store.get("greet").unwrap().is_some());
}

#[test]
fn history_does_not_show_up_in_the_workflow_listing() {
    let store = store();
    let mut record = record("greet", "x");
    store.save(&record).unwrap();
    record.description = "again".into();
    store.save(&record).unwrap();

    assert_eq!(store.list().unwrap().len(), 1);
    assert_eq!(require(&store, "greet").unwrap().description, "again");
}
