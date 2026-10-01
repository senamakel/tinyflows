use super::*;
use crate::ledger::memory::MemoryLedger;
use tinyflows::model::WorkflowGraph;
use tinyflows::store::{FileWorkflowStore, types::WorkflowRecord};

/// The store validates on save, so a fixture needs a graph that compiles.
fn tiny_graph(id: &str) -> WorkflowGraph {
    WorkflowGraph {
        schema_version: 1,
        id: Some(id.into()),
        name: id.into(),
        inputs: Vec::new(),
        agents: Vec::new(),
        nodes: vec![tinyflows::model::Node {
            id: "start".into(),
            kind: tinyflows::model::NodeKind::Trigger,
            type_version: 1,
            name: "manual".into(),
            config: serde_json::json!({ "trigger_kind": "manual" }),
            ports: Vec::new(),
            position: None,
        }],
        edges: Vec::new(),
    }
}

fn stored(id: &str, enabled: bool) -> WorkflowRecord {
    WorkflowRecord {
        id: id.into(),
        name: id.into(),
        description: "does a thing".into(),
        enabled,
        defaults: tinyflows::store::types::WorkflowDefaults::default(),
        graph: tiny_graph(id),
        source_path: None,
    }
}

fn store(tag: &str) -> Arc<dyn WorkflowStore> {
    let root = std::env::temp_dir().join(format!("adaptive-shelf-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("workflows")).expect("temp dir");
    Arc::new(FileWorkflowStore::new(
        vec![root.join("workflows")],
        root.join("runs"),
    ))
}

#[tokio::test]
async fn it_reports_the_disabled_and_the_already_tried_rather_than_hiding_them() {
    // The difference from intake's catalogue, which drops both. Someone
    // asking what a tenant has is often asking precisely about the one that
    // is switched off.
    let store = store("all");
    store.save(&stored("weekly", true)).expect("save");
    store.save(&stored("retired", false)).expect("save");
    let ledger = MemoryLedger::new();

    let shelf = shelf(&store, &ledger).await.expect("shelf");
    assert_eq!(shelf.len(), 2);
    assert!(shelf.iter().any(|l| l.id == "retired" && !l.enabled));
}

#[tokio::test]
async fn a_family_is_reported_whole_with_each_members_standing() {
    // intake collapses this to one row. A reader wants to see that the
    // variant exists and where it stands.
    let store = store("family");
    store.save(&stored("weekly", true)).expect("save");
    store.save(&stored("weekly-fix-1", true)).expect("save");
    let ledger = MemoryLedger::new();
    ledger
        .link_variant("weekly", "weekly-fix-1")
        .await
        .expect("link");
    for _ in 0..4 {
        ledger.score_workflow("weekly", true).await.expect("score");
    }

    let shelf = shelf(&store, &ledger).await.expect("shelf");
    assert_eq!(shelf.len(), 2, "both members, not just the champion");

    let parent = shelf.iter().find(|l| l.id == "weekly").expect("parent");
    assert_eq!(parent.standing, Standing::Champion);
    assert_eq!((parent.score.applied, parent.score.helped), (4, 4));
    assert_eq!(parent.parent, None);

    let variant = shelf
        .iter()
        .find(|l| l.id == "weekly-fix-1")
        .expect("variant");
    assert_eq!(variant.standing, Standing::Unproven, "no trials yet");
    assert_eq!(variant.parent.as_deref(), Some("weekly"));
}

#[tokio::test]
async fn what_the_loop_wrote_is_distinguishable_from_what_a_person_did() {
    let store = store("learned");
    store.save(&stored("weekly", true)).expect("save");
    store.save(&stored("learned-a1b2c3d", true)).expect("save");

    let shelf = shelf(&store, &MemoryLedger::new()).await.expect("shelf");
    assert!(!shelf.iter().find(|l| l.id == "weekly").expect("w").learned);
    assert!(
        shelf
            .iter()
            .find(|l| l.id == "learned-a1b2c3d")
            .expect("l")
            .learned
    );
}

#[tokio::test]
async fn a_workflow_nobody_has_run_reports_zero_rather_than_erroring() {
    let store = store("cold");
    store.save(&stored("fresh", true)).expect("save");
    let shelf = shelf(&store, &MemoryLedger::new()).await.expect("shelf");
    assert_eq!((shelf[0].score.applied, shelf[0].score.helped), (0, 0));
    assert_eq!(shelf[0].standing, Standing::Unproven);
}
