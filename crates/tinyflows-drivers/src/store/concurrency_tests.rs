//! Racing writers: the compare-and-swap on a definition and the revision
//! captured beside it must hold up when two threads, or two store instances,
//! write at once.

use std::sync::{Arc, Barrier};

use tinyflows::store::WorkflowStore;
use tinyflows_storage::MemoryStorage;

use super::test_support::{record, store, store_over};

fn named(id: &str, description: &str) -> tinyflows::store::types::WorkflowRecord {
    record(id, description)
}

#[test]
fn two_threads_saving_the_same_id_at_once_never_lose_a_revision() {
    let store = store();
    store.save(&named("race", "v0")).expect("seed save");

    // Released together so both saves' read-modify-write genuinely overlaps
    // rather than happening to run one after the other by scheduling luck.
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = ["v1", "v2"]
        .into_iter()
        .map(|description| {
            let (store, barrier) = (store.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                store.save(&named("race", description)).expect("save");
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("thread panicked");
    }

    // Three versions existed in total: the seed and one per racing save. Which
    // save wins the final document is a last-write-wins race, as for any single
    // writer; what must hold is that each save captured the version it actually
    // superseded, so two are in history.
    let revisions = store.list_revisions("race").expect("list revisions");
    assert_eq!(revisions.len(), 2, "{revisions:?}");
    let current = store.get("race").unwrap().expect("still exists");
    assert!(
        current.description == "v1" || current.description == "v2",
        "{}",
        current.description
    );
}

#[test]
fn a_save_racing_a_delete_leaves_the_deletion_recoverable() {
    let store = store();
    store.save(&named("race", "v0")).expect("seed save");

    let barrier = Arc::new(Barrier::new(2));
    let saver = {
        let (store, barrier) = (store.clone(), barrier.clone());
        std::thread::spawn(move || {
            barrier.wait();
            // Either order is fine, but it must not panic or corrupt the store.
            let _ = store.save(&named("race", "v1"));
        })
    };
    let deleter = {
        let (store, barrier) = (store.clone(), barrier);
        std::thread::spawn(move || {
            barrier.wait();
            let _ = store.delete("race");
        })
    };
    saver.join().expect("save thread panicked");
    deleter.join().expect("delete thread panicked");

    let revisions = store.list_revisions("race").expect("list revisions");
    assert!(
        !revisions.is_empty(),
        "at least the seed version must have been captured: {revisions:?}"
    );
}

#[test]
fn separate_store_instances_racing_the_same_save_lose_nothing() {
    let storage = MemoryStorage::new();
    let first = store_over(&storage, "local");
    let second = store_over(&storage, "local");
    first.save(&named("race", "v0")).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = [(first.clone(), "a"), (second.clone(), "b")]
        .into_iter()
        .map(|(store, description)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                store.save(&named("race", description)).unwrap();
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(first.list_revisions("race").unwrap().len(), 2);
    assert_eq!(second.list().unwrap().len(), 1);
}

#[test]
fn a_fingerprint_guarded_save_loses_to_an_earlier_writer_and_says_so() {
    let store = store();
    let seed = named("guarded", "v0");
    store.save(&seed).unwrap();
    let observed = tinyflows::store::types::record_fingerprint(&seed);

    // Someone else edits the record after the observation was taken.
    store.save(&named("guarded", "v1")).unwrap();

    assert!(
        !store
            .save_if_record_fingerprint(&named("guarded", "mine"), &observed)
            .unwrap(),
        "the observation is stale, so the write must be refused"
    );
    assert_eq!(store.get("guarded").unwrap().unwrap().description, "v1");
}

#[test]
fn a_fingerprint_guarded_save_wins_when_nothing_moved() {
    let store = store();
    let seed = named("guarded", "v0");
    store.save(&seed).unwrap();
    let observed = tinyflows::store::types::record_fingerprint(&seed);

    assert!(
        store
            .save_if_record_fingerprint(&named("guarded", "mine"), &observed)
            .unwrap()
    );
    assert_eq!(store.get("guarded").unwrap().unwrap().description, "mine");
    let history = store.list_revisions("guarded").unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].record.description, "v0");
}

#[test]
fn a_graph_fingerprint_guard_ignores_edits_that_leave_the_graph_alone() {
    let store = store();
    let seed = named("guarded", "v0");
    store.save(&seed).unwrap();
    let observed = tinyflows::store::types::fingerprint(&seed.graph);

    // The description changes; the graph does not.
    store.save(&named("guarded", "edited elsewhere")).unwrap();

    assert!(
        store
            .save_if_fingerprint(&named("guarded", "mine"), &observed)
            .unwrap()
    );
}

#[test]
fn a_guarded_save_of_a_workflow_that_is_gone_is_refused() {
    let store = store();
    assert!(
        !store
            .save_if_record_fingerprint(&named("ghost", "x"), "anything")
            .unwrap()
    );
    assert!(
        !store
            .save_if_fingerprint(&named("ghost", "x"), "anything")
            .unwrap()
    );
}

#[test]
fn a_guarded_save_of_an_invalid_graph_is_refused_before_anything_is_written() {
    let store = store();
    let seed = named("guarded", "v0");
    store.save(&seed).unwrap();
    let mut broken = seed.clone();
    broken.graph.nodes.clear();
    let observed = tinyflows::store::types::record_fingerprint(&seed);
    assert!(
        store
            .save_if_record_fingerprint(&broken, &observed)
            .is_err()
    );
    assert!(store.list_revisions("guarded").unwrap().is_empty());
}
