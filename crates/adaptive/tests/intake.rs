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
use tinyflows_adaptive::host::HostFacts;
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
        &HostFacts::unknown(),
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
        &HostFacts::unknown(),
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
        &HostFacts::unknown(),
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
        &HostFacts::unknown(),
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
        &HostFacts::unknown(),
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
        &HostFacts::unknown(),
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

    let err = decide(
        &Goal::new("anything"),
        "ep1",
        &store,
        &ledger,
        &HostFacts::unknown(),
        &caps,
        None,
    )
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
        &HostFacts::unknown(),
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

#[tokio::test]
async fn a_graph_naming_a_worker_this_host_lacks_is_refused_before_it_runs() {
    // The whole point of collecting host facts. Without this the graph saves
    // cleanly, validates cleanly, and fails at run time — usually overnight,
    // to nobody watching.
    let mut agent_graph = tiny_graph("uses-an-agent", None);
    agent_graph.nodes[1] = Node {
        id: "work".into(),
        kind: NodeKind::Agent,
        type_version: 1,
        name: "do it".into(),
        config: json!({ "prompt": "do the thing", "agent_ref": "desktop" }),
        ports: Vec::new(),
        position: None,
    };
    agent_graph.edges[0].to_node = "work".into();

    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": agent_graph, "why": "needs an agent", "inputs": {},
    })]));
    let caps = caps_with(llm);
    let (store, _root) = empty_store("gated");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let facts = HostFacts {
        workers: vec!["laptop".into(), "ci".into()],
        default_worker: Some("laptop".into()),
        ..HostFacts::unknown()
    };

    let err = decide(
        &Goal::new("do the thing"),
        "ep1",
        &store,
        &ledger,
        &facts,
        &caps,
        None,
    )
    .await
    .expect_err("a worker this host lacks must not reach the engine");

    assert!(
        err.to_string().contains("desktop"),
        "the error names it: {err}"
    );
    assert!(
        err.to_string().contains("laptop"),
        "and offers the alternatives: {err}"
    );
}

#[tokio::test]
async fn the_authoring_prompt_carries_what_the_host_permits() {
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("fine", None), "why": "ok", "inputs": {},
    })]));
    let caps = caps_with(llm.clone());
    let (store, _root) = empty_store("facts-rendered");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let facts = HostFacts {
        workers: vec!["laptop".into()],
        default_worker: None,
        allow_code: Some(false),
        notes: vec!["Only manual triggers fire here.".into()],
        ..HostFacts::unknown()
    };

    decide(
        &Goal::new("anything"),
        "ep1",
        &store,
        &ledger,
        &facts,
        &caps,
        None,
    )
    .await
    .expect("decide");

    let prompt = &llm.prompts()[0];
    assert!(prompt.contains("What this host permits"), "{prompt}");
    assert!(prompt.contains("every agent node must name config.agent_ref"));
    assert!(prompt.contains("Only manual triggers fire here."));
}

// ---------------------------------------------------------------------------
// Promotion: a repaired family is one row, and score decides which.
// ---------------------------------------------------------------------------

/// A parent and one variant, both stored and linked, with scores applied.
async fn repaired_family(
    tag: &str,
    parent: (u32, u32),
    variant: (u32, u32),
) -> (FileWorkflowStore, SqliteLedger, std::path::PathBuf) {
    let (store, root) = empty_store(tag);
    store
        .save(&stored("weekly", "writes the weekly report", None))
        .expect("save");
    store
        .save(&stored(
            "weekly-fix-1",
            "writes the weekly report, with the binding corrected",
            None,
        ))
        .expect("save");

    let ledger = SqliteLedger::in_memory().expect("ledger");
    ledger
        .link_variant("weekly", "weekly-fix-1")
        .await
        .expect("link");
    for (id, (applied, helped)) in [("weekly", parent), ("weekly-fix-1", variant)] {
        for n in 0..applied {
            ledger.score_workflow(id, n < helped).await.expect("score");
        }
    }
    (store, ledger, root)
}

/// What the selector was actually shown.
async fn offered(store: &FileWorkflowStore, ledger: &SqliteLedger) -> String {
    let llm = std::sync::Arc::new(Scripted::new(vec![
        json!({"workflow_id": "none"}),
        json!({
            "graph": tiny_graph("fallback", None),
            "why": "declined",
            "inputs": {},
        }),
    ]));
    let caps = caps_with(llm.clone());
    let _ = decide(
        &Goal::new("write the weekly report"),
        "ep-promo",
        store,
        ledger,
        &HostFacts::unknown(),
        &caps,
        None,
    )
    .await;
    llm.prompts().first().cloned().unwrap_or_default()
}

#[tokio::test]
async fn a_repaired_family_is_offered_as_one_row_not_two() {
    // Two near-identical graphs whose descriptions differ by a clause is not a
    // choice, it is noise.
    let (store, ledger, _root) = repaired_family("promo-1", (40, 40), (0, 0)).await;
    let shown = offered(&store, &ledger).await;
    let rows = shown.matches("weekly").count();
    assert!(rows > 0, "the family must be offered at all: {shown}");
    assert!(
        !shown.contains("weekly-fix-1"),
        "an unproven variant must not appear beside its proven parent: {shown}"
    );
}

#[tokio::test]
async fn a_fresh_variant_does_not_displace_a_proven_parent() {
    let (store, ledger, _root) = repaired_family("promo-2", (40, 40), (0, 0)).await;
    let shown = offered(&store, &ledger).await;
    assert!(shown.contains("weekly"), "{shown}");
    assert!(!shown.contains("weekly-fix-1"), "{shown}");
}

#[tokio::test]
async fn a_variant_that_has_proven_better_is_the_one_offered() {
    // Promotion on score, not on having been written.
    let (store, ledger, _root) = repaired_family("promo-3", (10, 5), (4, 4)).await;
    let shown = offered(&store, &ledger).await;
    assert!(
        shown.contains("weekly-fix-1"),
        "the better member must take the position: {shown}"
    );
}

#[tokio::test]
async fn a_family_whose_champion_was_already_tried_still_offers_its_variant() {
    // The case that matters most and is easiest to get wrong: this episode just
    // failed with the parent, so the parent is excluded — and the variant
    // exists *because* the parent fell short. Dropping the whole family would
    // hide the one graph written for this exact situation.
    let (store, ledger, _root) = repaired_family("promo-4", (40, 40), (0, 0)).await;
    ledger
        .append(&tinyflows_adaptive::ledger::LedgerRow {
            id: String::new(),
            episode: "ep-promo".into(),
            attempt: 1,
            approach_sig: "selected:weekly".into(),
            approach_desc: "the champion".into(),
            workflow_id: Some("weekly".into()),
            outcome: "fell short".into(),
            cause: String::new(),
            cost_usd: 0.0,
            at: "2026-01-01T00:00:00Z".into(),
            satisfied: false,
            advanced: false,
        })
        .await
        .expect("append");

    let shown = offered(&store, &ledger).await;
    assert!(
        shown.contains("weekly-fix-1"),
        "the variant must survive its champion being excluded: {shown}"
    );
}

// ---------------------------------------------------------------------------
// The retry edge: attempt four must not be attempt two in different words.
// ---------------------------------------------------------------------------

async fn with_history(tag: &str) -> (FileWorkflowStore, SqliteLedger, std::path::PathBuf) {
    let (store, root) = empty_store(tag);
    let ledger = SqliteLedger::in_memory().expect("ledger");
    for (attempt, sig, desc, cause) in [
        (
            1u32,
            "authored:aaa",
            "fetched the log and summarised it",
            "no numbers in it",
        ),
        (
            2,
            "authored:bbb",
            "asked an agent to write it from memory",
            "it invented the figures",
        ),
    ] {
        ledger
            .append(&tinyflows_adaptive::ledger::LedgerRow {
                id: String::new(),
                episode: "ep-retry".into(),
                attempt,
                approach_sig: sig.into(),
                approach_desc: desc.into(),
                workflow_id: None,
                outcome: "fell short".into(),
                cause: cause.into(),
                cost_usd: 0.0,
                at: "2026-01-01T00:00:00Z".into(),
                satisfied: false,
                advanced: false,
            })
            .await
            .expect("append");
    }
    (store, ledger, root)
}

#[tokio::test]
async fn the_author_is_shown_what_this_episode_already_tried() {
    // Without this the author writes attempt two's graph again, confidently,
    // because nothing told it otherwise. The exclusion list only guards
    // *selection*; authoring has no structural guard at all.
    let (store, ledger, _root) = with_history("retry-1").await;
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("third-idea", None),
        "why": "the first two both trusted the model for figures",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());

    decide(
        &Goal::new("write the weekly report"),
        "ep-retry",
        &store,
        &ledger,
        &HostFacts::unknown(),
        &caps,
        None,
    )
    .await
    .expect("decide");

    let prompt = &llm.prompts()[0];
    assert!(prompt.contains("Already tried this episode"), "{prompt}");
    assert!(
        prompt.contains("asked an agent to write it from memory"),
        "{prompt}"
    );
    assert!(prompt.contains("it invented the figures"), "{prompt}");
    assert!(prompt.contains("write something\nDIFFERENT"), "{prompt}");
}

#[tokio::test]
async fn the_selector_is_shown_the_same_history_in_the_same_words() {
    let (store, ledger, _root) = with_history("retry-2").await;
    store
        .save(&stored("weekly", "writes the weekly report", None))
        .expect("save");

    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "workflow_id": "weekly",
        "why": "it does this",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());

    decide(
        &Goal::new("write the weekly report"),
        "ep-retry",
        &store,
        &ledger,
        &HostFacts::unknown(),
        &caps,
        None,
    )
    .await
    .expect("decide");

    let prompt = &llm.prompts()[0];
    assert!(prompt.contains("Already tried this episode"), "{prompt}");
    assert!(prompt.contains("no numbers in it"), "{prompt}");
}

#[tokio::test]
async fn lessons_from_other_episodes_reach_the_planner() {
    // consolidate() was writing these and nothing was reading them — a
    // knowledge store that costs money and returns nothing.
    let (store, root) = empty_store("retry-3");
    let _ = root;
    let ledger = SqliteLedger::in_memory().expect("ledger");
    ledger
        .promote(
            &tinyflows_adaptive::ledger::Lesson {
                id: String::new(),
                kind: tinyflows_adaptive::ledger::LessonKind::Constraint,
                trigger: "a report that must cite figures".into(),
                mechanism: "the model has no access to the numbers".into(),
                claim: "read them from the source rather than asking an agent".into(),
                applied: 0,
                helped: 0,
                scope_key: None,
            },
            &[],
        )
        .await
        .expect("promote");

    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("informed", None),
        "why": "nothing stored",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());

    decide(
        &Goal::new("write the weekly report"),
        "ep-fresh",
        &store,
        &ledger,
        &HostFacts::unknown(),
        &caps,
        None,
    )
    .await
    .expect("decide");

    let prompt = &llm.prompts()[0];
    assert!(prompt.contains("Learned from earlier episodes"), "{prompt}");
    assert!(prompt.contains("read them from the source"), "{prompt}");
}

#[tokio::test]
async fn a_first_attempt_is_told_nothing_it_would_have_to_ignore() {
    // An empty history section is noise a model has to read past, and an
    // empty "already tried" heading reads as a claim that something was.
    let (store, _root) = empty_store("retry-4");
    let ledger = SqliteLedger::in_memory().expect("ledger");
    let llm = std::sync::Arc::new(Scripted::new(vec![json!({
        "graph": tiny_graph("first", None),
        "why": "nothing stored",
        "inputs": {},
    })]));
    let caps = caps_with(llm.clone());

    decide(
        &Goal::new("write the weekly report"),
        "ep-first",
        &store,
        &ledger,
        &HostFacts::unknown(),
        &caps,
        None,
    )
    .await
    .expect("decide");

    let prompt = &llm.prompts()[0];
    assert!(!prompt.contains("Already tried"), "{prompt}");
    assert!(!prompt.contains("Learned from earlier"), "{prompt}");
}

#[tokio::test]
async fn two_authored_attempts_leave_two_distinct_signatures() {
    // The fingerprint end to end: a differently-shaped graph must not fold into
    // the same exclusion-list entry as the one before it.
    let (store, _root) = empty_store("retry-5");
    let ledger = SqliteLedger::in_memory().expect("ledger");

    let mut signatures = Vec::new();
    for (n, name) in [(0, "shape-one"), (1, "shape-two")] {
        let llm = std::sync::Arc::new(Scripted::new(vec![json!({
            "graph": tiny_graph(name, if n == 1 { Some("repo") } else { None }),
            "why": "nothing stored",
            "inputs": { "repo": "acme/thing" },
        })]));
        let attempt = decide(
            &Goal::new("write the weekly report"),
            "ep-sigs",
            &store,
            &ledger,
            &HostFacts::unknown(),
            &caps_with(llm),
            None,
        )
        .await
        .expect("decide");
        signatures.push(attempt.approach.signature());
    }

    assert_ne!(signatures[0], signatures[1], "{signatures:?}");
    assert!(signatures[0].starts_with("authored:"), "{signatures:?}");
}
