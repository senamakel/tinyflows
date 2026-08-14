//! Intake, end to end, against a scripted model and a real store.
//!
//! The unit tests cover rendering and parsing. These cover the decision: that
//! selection is preferred, that authoring is the fallback rather than the
//! default, and that the exclusion list actually excludes — which is the
//! property the whole retry edge rests on and the one that is invisible until
//! an episode has spent an attempt.

use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{Value, json};
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::caps::{Capabilities, LlmProvider};
use tinyflows::error::Result as EngineResult;
use tinyflows::model::{Edge, InputType, Node, NodeKind, WorkflowGraph, WorkflowInput};
use tinyflows::store::types::WorkflowRecord;
use tinyflows::store::{FileWorkflowStore, WorkflowStore};
use tinyflows_adaptive::contracts::{Approach, Goal};
use tinyflows_adaptive::intake::decide;
use tinyflows_adaptive::ledger::{Ledger, sqlite::SqliteLedger};

/// A provider that answers from a script and records what it was asked.
struct Scripted {
    replies: Mutex<Vec<Value>>,
    seen: Mutex<Vec<String>>,
}

impl Scripted {
    fn new(replies: Vec<Value>) -> Self {
        Self {
            replies: Mutex::new(replies),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn prompts(&self) -> Vec<String> {
        self.seen.lock().expect("lock").clone()
    }
}

#[async_trait]
impl LlmProvider for Scripted {
    async fn complete(&self, request: Value, _conn: Option<&str>) -> EngineResult<Value> {
        let text = request["messages"]
            .as_array()
            .map(|m| {
                m.iter()
                    .filter_map(|msg| msg["content"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();
        self.seen.lock().expect("lock").push(text);

        let mut replies = self.replies.lock().expect("lock");
        if replies.is_empty() {
            panic!("the model was asked more times than the script has answers");
        }
        Ok(replies.remove(0))
    }
}

/// The engine's mock bundle with only the model replaced: nothing in intake
/// touches tools, HTTP, code or state, so scripting those too would be noise.
fn caps_with(llm: std::sync::Arc<Scripted>) -> Capabilities {
    Capabilities {
        llm,
        ..mock_capabilities()
    }
}

/// A store on a fresh temp directory, so each case starts empty.
fn empty_store(tag: &str) -> (FileWorkflowStore, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("adaptive-intake-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("workflows")).expect("temp dir");
    let store = FileWorkflowStore::new(vec![root.join("workflows")], root.join("runs"));
    (store, root)
}

/// A minimal graph that validates: one trigger, one transform.
fn tiny_graph(name: &str, required_input: Option<&str>) -> WorkflowGraph {
    WorkflowGraph {
        schema_version: 1,
        id: Some(name.to_string()),
        name: name.to_string(),
        inputs: required_input
            .map(|n| vec![WorkflowInput::new(n, InputType::String).required()])
            .unwrap_or_default(),
        agents: Vec::new(),
        nodes: vec![
            Node {
                id: "start".into(),
                kind: NodeKind::Trigger,
                type_version: 1,
                name: "manual".into(),
                config: json!({ "trigger_kind": "manual" }),
                ports: Vec::new(),
                position: None,
            },
            Node {
                id: "done".into(),
                kind: NodeKind::Transform,
                type_version: 1,
                name: "done".into(),
                config: json!({ "set": { "ok": true } }),
                ports: Vec::new(),
                position: None,
            },
        ],
        edges: vec![Edge {
            from_node: "start".into(),
            from_port: "main".into(),
            to_node: "done".into(),
            to_port: "main".into(),
        }],
    }
}

fn stored(id: &str, description: &str, required_input: Option<&str>) -> WorkflowRecord {
    WorkflowRecord {
        id: id.to_string(),
        name: id.to_string(),
        description: description.to_string(),
        enabled: true,
        defaults: Default::default(),
        graph: tiny_graph(id, required_input),
        source_path: None,
    }
}

#[tokio::test]
async fn an_empty_store_authors_without_asking_whether_to_select() {
    // With nothing to choose from the answer can only be "none". Spending a
    // call to be told so is the cost of every cold start.
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("fresh", None),
        "why": "nothing stored",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("1");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let attempt = decide(
        &Goal::new("do a new thing"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect("decide");

    assert!(matches!(attempt.approach, Approach::Authored { .. }));
    assert_eq!(
        llm.prompts().len(),
        1,
        "exactly one call: the authoring one"
    );
    assert!(
        llm.prompts()[0].contains("Node catalogue"),
        "authoring must be grounded on the catalogue"
    );
}

#[tokio::test]
async fn a_matching_workflow_is_selected_and_its_graph_is_loaded() {
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "workflow_id": "pr-review",
        "why": "does exactly this",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("2");
    store
        .save(&stored("pr-review", "reviews a closed issue", None))
        .expect("save");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let attempt = decide(
        &Goal::new("review a closed issue"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect("decide");

    match attempt.approach {
        Approach::Selected { workflow_id, .. } => assert_eq!(workflow_id, "pr-review"),
        other => panic!("expected a selection, got {other:?}"),
    }
    // The bug this catches: `select` answers with an id, and returning that
    // unbound hands the engine an empty graph that compiles to nothing.
    assert_eq!(
        attempt.graph.nodes.len(),
        2,
        "the stored graph must be loaded"
    );
    assert_eq!(llm.prompts().len(), 1, "a hit must not also author");
}

#[tokio::test]
async fn declining_falls_through_to_authoring() {
    let llm = std::sync::Arc::new(Scripted::new(vec![
        json!({ "workflow_id": null, "why": "none of these fetch anything" }),
        json!({ "graph": tiny_graph("written", None), "why": "had to write one", "inputs": {} }),
    ]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("3");
    store
        .save(&stored("unrelated", "does something else", None))
        .expect("save");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let attempt = decide(
        &Goal::new("something new"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect("decide");

    assert!(matches!(attempt.approach, Approach::Authored { .. }));
    assert_eq!(
        llm.prompts().len(),
        2,
        "selection was asked first, then authoring"
    );
}

#[tokio::test]
async fn a_workflow_already_tried_this_episode_is_not_offered_again() {
    // The property the whole retry edge rests on. Without it attempt two
    // re-selects what attempt one already failed on, and the episode pays
    // twice for one dead end.
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("written", None),
        "why": "the only candidate was already spent",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("4");
    store
        .save(&stored("pr-review", "reviews a closed issue", None))
        .expect("save");

    let ledger = SqliteLedger::in_memory().expect("ledger");
    let mut spent = tinyflows_adaptive::ledger::conformance::row("ep1", 1, "selected:pr-review");
    spent.workflow_id = Some("pr-review".to_string());
    ledger.append(&spent).await.expect("append");

    let attempt = decide(
        &Goal::new("review a closed issue"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect("decide");

    assert!(
        matches!(attempt.approach, Approach::Authored { .. }),
        "the only stored workflow was excluded, so authoring is the only path left"
    );
    assert_eq!(
        llm.prompts().len(),
        1,
        "with every candidate excluded the list is empty and selection is skipped entirely"
    );
}

#[tokio::test]
async fn a_selection_whose_required_input_is_missing_is_refused_before_it_runs() {
    // The model is confident about inputs it did not find in the goal. The
    // cheap deterministic check catches what the expensive one asserted.
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "workflow_id": "needs-repo",
        "why": "matches",
        "inputs": {},
    })]));
    let caps = caps_with(llm);
    let (store, _root) = empty_store("5");
    store
        .save(&stored("needs-repo", "reviews PRs in a repo", Some("repo")))
        .expect("save");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let err = decide(
        &Goal::new("review the PRs"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect_err("an unbindable selection must not reach the engine");

    assert!(
        err.to_string().contains("repo"),
        "the error names the missing input: {err}"
    );
}

#[tokio::test]
async fn a_hallucinated_workflow_id_reads_as_a_decline() {
    let llm = std::sync::Arc::new(Scripted::new(vec![
        json!({ "workflow_id": "pr-reviewer", "why": "close, but no such id" }),
        json!({ "graph": tiny_graph("written", None), "why": "wrote one", "inputs": {} }),
    ]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("6");
    store
        .save(&stored("pr-review", "reviews a closed issue", None))
        .expect("save");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let attempt = decide(
        &Goal::new("review something"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect("decide");

    assert!(
        matches!(attempt.approach, Approach::Authored { .. }),
        "a name that is not on the list is a hallucination, not a lookup"
    );
}

#[tokio::test]
async fn an_authored_graph_that_does_not_validate_is_an_error_not_a_return_value() {
    // Handing it back would turn an authoring mistake into a run-time failure
    // that reads like the work failing.
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": { "schema_version": 1, "name": "empty", "nodes": [], "edges": [] },
        "why": "forgot the trigger",
        "inputs": {},
    })]));
    let caps = caps_with(llm);
    let (store, _root) = empty_store("7");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let err = decide(&Goal::new("anything"), "ep1", &store, &ledger, &caps, None)
        .await
        .expect_err("an invalid graph must not leave intake");
    assert!(err.to_string().contains("invalid"), "{err}");
}

#[tokio::test]
async fn a_disabled_workflow_is_never_offered() {
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("written", None),
        "why": "the only one was disabled",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("8");
    let mut off = stored("switched-off", "would have matched", None);
    off.enabled = false;
    store.save(&off).expect("save");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    decide(
        &Goal::new("do the thing"),
        "ep1",
        &store,
        &ledger,
        &caps,
        None,
    )
    .await
    .expect("decide");

    assert_eq!(
        llm.prompts().len(),
        1,
        "offering a disabled workflow invites a choice that cannot be honoured"
    );
}
