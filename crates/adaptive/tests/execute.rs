//! Execute, against the real engine.
//!
//! Not a mocked engine: these compile and run actual graphs, because the whole
//! point of the layer is the gap between what the engine returns and what the
//! judge needs, and a mock of the engine would be a mock of exactly that gap.

use async_trait::async_trait;
use serde_json::{Map, Value, json};
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::model::{Edge, Node, NodeKind, WorkflowGraph};
use tinyflows_adaptive::contracts::Approach;
use tinyflows_adaptive::execute::{Unobserved, Workspace, run_attempt};
use tinyflows_adaptive::intake::Attempt;

/// Records whether the baseline was taken before the run.
#[derive(Default)]
struct Recording {
    calls: std::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl Workspace for Recording {
    async fn mark(&self) -> String {
        self.calls.lock().expect("lock").push("mark".into());
        "baseline-7".into()
    }
    async fn changed_since(&self, mark: &str) -> String {
        self.calls
            .lock()
            .expect("lock")
            .push(format!("changed_since({mark})"));
        "wrote report.md".into()
    }
}

fn node(id: &str, kind: NodeKind, config: Value) -> Node {
    Node {
        id: id.into(),
        kind,
        type_version: 1,
        name: id.into(),
        config,
        ports: Vec::new(),
        position: None,
    }
}

fn edge(from: &str, to: &str) -> Edge {
    Edge {
        from_node: from.into(),
        from_port: "main".into(),
        to_node: to.into(),
        to_port: "main".into(),
    }
}

fn graph(nodes: Vec<Node>, edges: Vec<Edge>) -> WorkflowGraph {
    WorkflowGraph {
        schema_version: 1,
        id: Some("t".into()),
        name: "t".into(),
        inputs: Vec::new(),
        agents: Vec::new(),
        nodes,
        edges,
    }
}

fn attempt(graph: WorkflowGraph) -> Attempt {
    Attempt {
        approach: Approach::Authored {
            why: "for the test".into(),
        },
        graph,
        inputs: Map::new(),
    }
}

/// One trigger into one transform: the smallest graph that actually does work.
fn working() -> WorkflowGraph {
    graph(
        vec![
            node(
                "start",
                NodeKind::Trigger,
                json!({"trigger_kind": "manual"}),
            ),
            node("done", NodeKind::Transform, json!({"set": {"ok": true}})),
        ],
        vec![edge("start", "done")],
    )
}

#[tokio::test]
async fn a_clean_run_comes_back_with_a_clean_diagnosis_and_the_host_s_reading() {
    let workspace = Recording::default();
    let ran = run_attempt(&attempt(working()), &mock_capabilities(), &workspace).await;

    assert!(ran.failed.is_none(), "{:?}", ran.failed);
    assert_eq!(ran.changed, "wrote report.md");
    assert!(
        ran.diagnosis.never_ran.is_empty(),
        "both nodes ran: {:?}",
        ran.diagnosis.never_ran
    );

    // The ordering the trait exists for: a baseline, then the run, then the
    // comparison against that same baseline.
    let calls = workspace.calls.lock().expect("lock").clone();
    assert_eq!(calls, vec!["mark", "changed_since(baseline-7)"]);
}

#[tokio::test]
async fn the_diagnosis_is_populated_which_is_the_reason_an_observer_is_attached() {
    // A condition that routes past a node. `RunOutcome` alone cannot say this
    // happened — the run is green either way — and every downstream gate reads
    // `never_ran` to find out.
    let g = graph(
        vec![
            node(
                "start",
                NodeKind::Trigger,
                json!({"trigger_kind": "manual"}),
            ),
            node(
                "gate",
                NodeKind::Condition,
                json!({"conditions": [{"left": "=item.nope", "operator": "equals", "right": "yes"}]}),
            ),
            // An `http_request`, not a transform: `never_ran` deliberately
            // reports only the kinds that do outside work, because a routed-past
            // transform is not a surprise worth warning about.
            node(
                "skipped",
                NodeKind::HttpRequest,
                json!({"url": "https://example.invalid/report", "method": "GET"}),
            ),
        ],
        vec![
            edge("start", "gate"),
            Edge {
                from_node: "gate".into(),
                from_port: "true".into(),
                to_node: "skipped".into(),
                to_port: "main".into(),
            },
        ],
    );

    let ran = run_attempt(&attempt(g), &mock_capabilities(), &Unobserved).await;

    assert!(
        ran.failed.is_none(),
        "the run itself is fine: {:?}",
        ran.failed
    );
    assert!(
        ran.diagnosis
            .never_ran
            .iter()
            .any(|n| n.node_id == "skipped"),
        "a blank diagnosis here would mean nobody looked: {:?}",
        ran.diagnosis
    );
}

#[tokio::test]
async fn a_graph_that_does_not_compile_is_an_attempt_not_an_error() {
    // No trigger node. Intake would never return this, but a caller that hand-
    // builds an `Attempt` can, and it still has to leave a ledger row.
    let g = graph(
        vec![node(
            "lonely",
            NodeKind::Transform,
            json!({"set": {"ok": true}}),
        )],
        Vec::new(),
    );

    let ran = run_attempt(&attempt(g), &mock_capabilities(), &Unobserved).await;

    let failure = ran.failed.expect("it did not compile");
    assert!(!failure.is_empty());
    // Readable through the ordinary evidence path, with no special case.
    assert_eq!(ran.outcome.output["error"], json!(failure));
    // And no `nodes` key, so the mechanical missing-evidence check fires.
    assert!(ran.outcome.output.get("nodes").is_none());
}

#[tokio::test]
async fn a_silent_host_is_silent_rather_than_wrong() {
    let ran = run_attempt(&attempt(working()), &mock_capabilities(), &Unobserved).await;
    assert!(ran.changed.is_empty());
    assert!(ran.failed.is_none());
    // Empty reads as "nothing reported", never as "nothing happened".
    assert!(ran.evidence().changed.is_empty());
}

#[tokio::test]
async fn the_evidence_borrows_what_ran_owns() {
    let ran = run_attempt(&attempt(working()), &mock_capabilities(), &Unobserved).await;
    let evidence = ran.evidence();
    assert!(std::ptr::eq(evidence.outcome, &ran.outcome));
    assert!(std::ptr::eq(evidence.diagnosis, &ran.diagnosis));
}
