//! Save/delete round-tripping: the graph and host fields survive a save, the
//! `defaults` block round-trips, an uncompilable graph is refused, ids are
//! guarded, and the host policy judges reads as well as writes.

use std::sync::Arc;

use serde_json::json;
use tinyflows::store::types::{WorkflowDefaults, WorkflowError, WorkflowRecord};
use tinyflows::store::{HostPolicy, WorkflowStore, parse_workflow, require};
use tinyflows_storage::{MemoryStorage, Precondition};

use super::test_support::{Faulty, block_on, docs_in, record, store, valid_document};
use super::*;

#[test]
fn saving_then_loading_round_trips_the_host_fields_and_the_graph() {
    let store = store();
    let mut record = record("round", "says hello");
    record.description = "edited".into();
    record.enabled = false;

    store.save(&record).expect("saves");
    let loaded = require(&store, "round").expect("found");

    assert_eq!(loaded.description, "edited");
    assert!(!loaded.enabled, "enabled must survive the round trip");
    assert_eq!(loaded.graph, record.graph);
    assert_eq!(loaded.trigger_kind().as_deref(), Some("manual"));
}

#[test]
fn saving_round_trips_the_defaults_block() {
    let store = store();
    let mut record = record("pinned", "x");
    record.defaults = WorkflowDefaults {
        harness: Some("codex".into()),
        model: Some("gpt-5-codex".into()),
    };

    store.save(&record).expect("saves");

    assert_eq!(require(&store, "pinned").unwrap().defaults, record.defaults);
}

#[test]
fn the_listing_is_sorted_by_id_and_summarises_each_workflow() {
    let store = store();
    for id in ["charlie", "alpha", "bravo"] {
        store.save(&record(id, "x")).unwrap();
    }
    let listed = store.list().unwrap();
    let ids: Vec<&str> = listed.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(ids, ["alpha", "bravo", "charlie"]);
    assert_eq!(listed[0].node_count, 2);
    assert_eq!(listed[0].trigger_kind.as_deref(), Some("manual"));
}

#[test]
fn saving_over_an_id_replaces_it_rather_than_adding_one() {
    let store = store();
    let mut record = record("one", "first");
    store.save(&record).unwrap();
    record.description = "second".into();
    store.save(&record).unwrap();
    assert_eq!(store.list().unwrap().len(), 1);
    assert_eq!(store.get("one").unwrap().unwrap().description, "second");
}

#[test]
fn saving_refuses_a_graph_the_engine_would_not_compile() {
    let store = store();
    let record = WorkflowRecord {
        id: "no-trigger".into(),
        name: "no trigger".into(),
        description: String::new(),
        enabled: true,
        defaults: Default::default(),
        graph: serde_json::from_value(json!({ "nodes": [], "edges": [] })).unwrap(),
        source_path: None,
    };

    let err = store.save(&record).expect_err("must not persist");

    assert!(matches!(err, WorkflowError::Invalid { .. }), "got {err:?}");
    assert!(
        store.list().unwrap().is_empty(),
        "an invalid graph must not reach the catalog"
    );
}

#[test]
fn deleting_removes_the_workflow_and_a_second_delete_is_not_found() {
    let store = store();
    store.save(&record("gone", "x")).unwrap();
    store.delete("gone").unwrap();
    assert!(store.get("gone").unwrap().is_none());
    assert!(matches!(
        store.delete("gone").unwrap_err(),
        WorkflowError::NotFound(_)
    ));
    assert!(matches!(
        require(&store, "gone").unwrap_err(),
        WorkflowError::NotFound(_)
    ));
}

#[test]
fn an_id_that_is_empty_or_has_a_control_character_is_refused() {
    let store = store();
    for bad in ["", "tab\there", "nul\0byte", "line\nbreak"] {
        let mut record = record("fine", "x");
        record.id = bad.to_string();
        assert!(
            matches!(store.save(&record), Err(WorkflowError::Malformed(_))),
            "{bad:?}"
        );
        assert!(store.get(bad).is_err(), "{bad:?}");
        assert!(store.delete(bad).is_err(), "{bad:?}");
    }
    assert!(store.list().unwrap().is_empty());
}

#[test]
fn ids_with_separators_and_dots_are_just_ids() {
    // They decided a filename once; as document ids they are inert.
    let store = store();
    for id in [
        "../../escape",
        "a/b",
        "dotted.name",
        "with space",
        "ünïcode",
    ] {
        store.save(&record(id, "x")).unwrap();
        assert_eq!(store.get(id).unwrap().unwrap().id, id);
    }
    assert_eq!(store.list().unwrap().len(), 5);
}

#[test]
fn an_id_too_long_to_be_a_key_still_round_trips() {
    let store = store();
    let long = "x".repeat(2_000);
    store.save(&record(&long, "x")).unwrap();
    assert_eq!(store.get(&long).unwrap().unwrap().id, long);
    assert_eq!(store.list().unwrap()[0].id, long);
    store.delete(&long).unwrap();
    assert!(store.get(&long).unwrap().is_none());
}

#[test]
fn workflows_are_scoped_like_every_other_document() {
    let storage = MemoryStorage::new();
    let alice = DriverWorkflowStore::new(docs_in(&storage, "alice"));
    let bob = DriverWorkflowStore::new(docs_in(&storage, "bob"));
    alice.save(&record("greet", "x")).unwrap();
    assert!(bob.get("greet").unwrap().is_none());
    assert!(bob.list().unwrap().is_empty());
    assert!(matches!(
        bob.delete("greet").unwrap_err(),
        WorkflowError::NotFound(_)
    ));
}

#[derive(Debug)]
struct NoCodex;
impl HostPolicy for NoCodex {
    fn check_defaults(&self, defaults: &WorkflowDefaults) -> Result<(), String> {
        match defaults.harness.as_deref() {
            Some("codex") => Err("no codex here".into()),
            _ => Ok(()),
        }
    }
}

#[test]
fn a_document_the_policy_now_refuses_is_hidden_and_named_in_the_log_not_run() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let lenient = DriverWorkflowStore::new(docs.clone());
    let mut pinned = record("pinned", "x");
    pinned.defaults.harness = Some("codex".into());
    lenient.save(&pinned).unwrap();
    lenient.save(&record("plain", "x")).unwrap();

    let strict = DriverWorkflowStore::new(docs).with_policy(Arc::new(NoCodex));

    assert!(strict.get("pinned").unwrap().is_none());
    let ids: Vec<String> = strict.list().unwrap().into_iter().map(|s| s.id).collect();
    assert_eq!(ids, ["plain"]);
    assert!(lenient.get("pinned").unwrap().is_some());
}

#[test]
fn a_definition_saved_by_an_older_build_is_migrated_on_read() {
    // A graph without a schema version predates the field; the migration runs
    // before the record is deserialized, as it does for a document on disk.
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    let modern = record("old", "x");
    let mut as_json = serde_json::to_value(&modern).unwrap();
    as_json["graph"]
        .as_object_mut()
        .unwrap()
        .remove("schema_version");
    block_on(docs.put(super::DEFINITIONS, "old", as_json, Precondition::None)).unwrap();

    let loaded = store.get("old").unwrap().expect("migrates and loads");
    assert_eq!(loaded.graph.nodes.len(), 2);
}

#[test]
fn an_unreadable_definition_costs_only_itself() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    store.save(&record("fine", "x")).unwrap();
    for (id, doc) in [
        ("junk", json!({ "id": "junk", "record_json": "{not json" })),
        ("missing", json!({ "id": "missing" })),
        (
            "not-a-graph",
            json!({ "id": "x", "name": "x", "graph": [1] }),
        ),
    ] {
        block_on(docs.put(super::DEFINITIONS, id, doc, Precondition::None)).unwrap();
    }

    let ids: Vec<String> = store.list().unwrap().into_iter().map(|s| s.id).collect();
    assert_eq!(ids, ["fine"]);
    assert!(store.get("junk").unwrap().is_none());
    assert!(store.get("missing").unwrap().is_none());
}

#[test]
fn a_document_that_decodes_but_will_not_compile_is_not_listed() {
    let storage = MemoryStorage::new();
    let docs = docs_in(&storage, "local");
    let store = DriverWorkflowStore::new(docs.clone());
    let mut hollow = serde_json::to_value(record("hollow", "x")).unwrap();
    hollow["graph"] = json!({ "nodes": [], "edges": [] });
    block_on(docs.put(super::DEFINITIONS, "hollow", hollow, Precondition::None)).unwrap();
    assert!(store.list().unwrap().is_empty());
}

#[test]
fn a_backend_failure_while_saving_is_reported_and_leaves_no_revision() {
    let storage = MemoryStorage::new();
    let faulty = Faulty::wrap(docs_in(&storage, "local"), super::DEFINITIONS);
    let store = DriverWorkflowStore::new(faulty.clone());
    store.save(&record("blocked", "v0")).unwrap();

    faulty
        .fail_puts
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let err = store.save(&record("blocked", "v1")).unwrap_err();

    assert!(matches!(err, WorkflowError::Storage(_)), "got {err:?}");
    faulty
        .fail_puts
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(
        store.list_revisions("blocked").unwrap().is_empty(),
        "a version that was never superseded must not enter history"
    );
    assert_eq!(store.get("blocked").unwrap().unwrap().description, "v0");
}

#[test]
fn collections_are_declared_once_and_cover_every_document_kind() {
    let store = store();
    store.ensure().unwrap();
    store.ensure().unwrap();
    let names: Vec<String> = DriverWorkflowStore::collections()
        .into_iter()
        .map(|spec| spec.name)
        .collect();
    assert_eq!(
        names,
        [
            super::DEFINITIONS,
            super::REVISIONS,
            super::RUNS,
            super::NOTES,
            super::PROPOSALS,
            super::QUARANTINE
        ]
    );
}

#[test]
fn the_decision_scope_names_the_store_and_follows_the_label() {
    let plain = store();
    assert_eq!(plain.proposal_decision_scope(), "documents:documents");
    let labelled = plain.with_scope_label("tenant-7");
    assert_eq!(labelled.proposal_decision_scope(), "documents:tenant-7");
}

#[test]
fn a_store_can_block_on_a_bridge_of_its_own() {
    let bridge = Arc::new(Blocking::new().unwrap());
    let store = store().with_bridge(bridge);
    store.save(&record("bridged", "x")).unwrap();
    assert!(store.get("bridged").unwrap().is_some());
}

#[test]
fn the_store_works_from_inside_an_async_runtime() {
    let store = store();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        store.save(&record("inside", "x")).unwrap();
        assert!(store.get("inside").unwrap().is_some());
    });
}

#[test]
fn a_parsed_document_and_a_stored_one_agree() {
    let store = store();
    let parsed = parse_workflow(&valid_document("agree"), "agree").unwrap();
    store.save(&parsed).unwrap();
    assert_eq!(store.get("agree").unwrap().unwrap(), parsed);
}

#[test]
fn debug_names_the_store_without_dumping_the_handle() {
    assert!(format!("{:?}", store()).contains("DriverWorkflowStore"));
}

#[test]
fn the_journal_cap_is_the_engines() {
    assert_eq!(JOURNAL_CAP, tinyflows::store::MAX_NOTES);
}
