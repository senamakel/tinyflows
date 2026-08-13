#![cfg(feature = "mock")]
//! Named, hand-built compositions whose ordering cannot be covered by shallow
//! generated graphs alone.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tinyflows::caps::ToolInvoker;
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::compiler::compile;
use tinyflows::engine::{run_resumable, run_with_observer};
use tinyflows::model::{Edge, Node, NodeKind, WorkflowGraph};
use tinyflows::observability::RunObserver;

const GUARD: Duration = Duration::from_secs(10);

fn node(id: &str, kind: NodeKind, config: Value) -> Node {
    Node {
        id: id.to_string(),
        kind,
        type_version: 1,
        name: id.to_string(),
        config,
        ports: vec![],
        position: None,
    }
}

fn edge(from: &str, to: &str) -> Edge {
    port_edge(from, "main", to)
}

fn port_edge(from: &str, port: &str, to: &str) -> Edge {
    Edge {
        from_node: from.to_string(),
        from_port: port.to_string(),
        to_node: to.to_string(),
        to_port: "main".to_string(),
    }
}

#[derive(Default)]
struct Trace(Mutex<Vec<String>>);

impl RunObserver for Trace {
    fn on_step_finish(&self, step: &tinyflows::observability::ExecutionStep) {
        self.0
            .lock()
            .expect("trace mutex poisoned")
            .push(step.node_id.clone());
    }
}

fn child_transform() -> WorkflowGraph {
    WorkflowGraph {
        name: "lane_child".to_string(),
        nodes: vec![
            node("child_trigger", NodeKind::Trigger, Value::Null),
            node(
                "child_agent",
                NodeKind::Agent,
                json!({ "prompt": "refine this candidate" }),
            ),
            node(
                "child_tag",
                NodeKind::Transform,
                json!({ "set": { "child_complete": true } }),
            ),
        ],
        edges: vec![
            edge("child_trigger", "child_agent"),
            edge("child_agent", "child_tag"),
        ],
        ..Default::default()
    }
}

/// scatter → per-lane child workflow → gather → accumulator loop → scatter.
///
/// This crosses both state namespaces (lane slots and child `nodes`) before a
/// repeated reducer write and then fans the accumulated result out again.
#[tokio::test]
async fn scatter_child_gather_loop_and_second_scatter_compose() {
    let child = serde_json::to_value(child_transform()).expect("serialize child");
    let graph = WorkflowGraph {
        name: "two_stage_refinement".to_string(),
        nodes: vec![
            node(
                "trigger",
                NodeKind::Trigger,
                json!({ "recursion_limit": 500, "max_node_visits": 200, "max_concurrency": 3 }),
            ),
            node(
                "first_scatter",
                NodeKind::Scatter,
                json!({ "path": "rows" }),
            ),
            node("child", NodeKind::SubWorkflow, json!({ "workflow": child })),
            node(
                "first_gather",
                NodeKind::Gather,
                json!({ "from": ["child"], "release": "quorum", "n": 3, "poll_interval_ms": 1 }),
            ),
            node(
                "refine_loop",
                NodeKind::Loop,
                json!({
                    "max_iterations": 2,
                    "on_exceeded": "continue",
                    "emit": "both",
                    "state": {
                        "init": { "passes": 0 },
                        "update": { "passes": "=state.passes + 1" },
                    },
                }),
            ),
            node(
                "loop_body",
                NodeKind::Transform,
                json!({ "set": { "refined": true } }),
            ),
            node("second_scatter", NodeKind::Scatter, Value::Null),
            node(
                "finalize",
                NodeKind::Transform,
                json!({ "set": { "final": true } }),
            ),
            node(
                "second_gather",
                NodeKind::Gather,
                json!({ "from": ["finalize"], "poll_interval_ms": 1 }),
            ),
        ],
        edges: vec![
            edge("trigger", "first_scatter"),
            edge("first_scatter", "child"),
            edge("child", "first_gather"),
            edge("first_gather", "refine_loop"),
            port_edge("refine_loop", "body", "loop_body"),
            edge("loop_body", "refine_loop"),
            port_edge("refine_loop", "done", "second_scatter"),
            edge("second_scatter", "finalize"),
            edge("finalize", "second_gather"),
        ],
        ..Default::default()
    };

    let compiled = compile(&graph).expect("complex graph compiles");
    let trace = Arc::new(Trace::default());
    let observer: Arc<dyn RunObserver> = trace.clone();
    let outcome = tokio::time::timeout(
        GUARD,
        run_with_observer(
            &compiled,
            json!({ "rows": [{"id": 0}, {"id": 1}, {"id": 2}] }),
            &mock_capabilities(),
            &observer,
        ),
    )
    .await
    .expect("complex graph hung")
    .expect("complex graph runs");

    assert_eq!(
        outcome.output["nodes"]["child"]["lanes"]
            .as_object()
            .map(serde_json::Map::len),
        Some(3),
        "one child workflow ran in each first-stage lane"
    );
    assert_eq!(outcome.output["nodes"]["refine_loop"]["iteration"], 2);
    assert_eq!(
        outcome.output["nodes"]["refine_loop"]["state"],
        json!({ "passes": 2 }),
        "the accumulator was replaced cleanly on both passes"
    );
    let final_items = outcome.output["nodes"]["second_gather"]["items"]
        .as_array()
        .expect("second gather output");
    assert_eq!(final_items.len(), 4, "three child results plus accumulator");
    assert!(
        final_items.iter().all(|item| item["json"]["final"] == true),
        "every second-stage lane ran the finalizer"
    );

    let order = trace.0.lock().expect("trace mutex poisoned").clone();
    let first_gather = order.iter().position(|id| id == "first_gather").unwrap();
    let second_scatter = order.iter().position(|id| id == "second_scatter").unwrap();
    assert!(first_gather < second_scatter, "observed order: {order:?}");
    assert_eq!(
        order.iter().filter(|id| id.as_str() == "loop_body").count(),
        2,
        "the loop body must activate once per pass: {order:?}"
    );
}

/// A child starts and collects asynchronous work, then pauses for approval;
/// the resumed parent starts and collects a second asynchronous task.
#[tokio::test]
async fn nested_async_gate_and_approval_resume_across_a_subworkflow_boundary() {
    let child = WorkflowGraph {
        name: "async_child".to_string(),
        nodes: vec![
            node("ct", NodeKind::Trigger, Value::Null),
            node(
                "cspawn",
                NodeKind::Spawn,
                json!({ "target": "tool", "slug": "child.lookup" }),
            ),
            node(
                "cgate",
                NodeKind::Gate,
                json!({ "from": ["cspawn"], "poll_interval_ms": 1 }),
            ),
            node(
                "approve",
                NodeKind::OutputParser,
                json!({ "requires_approval": true }),
            ),
            node("cdone", NodeKind::OutputParser, Value::Null),
        ],
        edges: vec![
            edge("ct", "cspawn"),
            edge("cspawn", "cgate"),
            edge("cgate", "approve"),
            edge("approve", "cdone"),
        ],
        ..Default::default()
    };
    let graph = WorkflowGraph {
        name: "nested_async_approval".to_string(),
        nodes: vec![
            node("t", NodeKind::Trigger, json!({ "recursion_limit": 100 })),
            node(
                "sub",
                NodeKind::SubWorkflow,
                json!({ "workflow": serde_json::to_value(child).unwrap() }),
            ),
            node(
                "pspawn",
                NodeKind::Spawn,
                json!({ "target": "tool", "slug": "parent.publish" }),
            ),
            node(
                "pgate",
                NodeKind::Gate,
                json!({ "from": ["pspawn"], "poll_interval_ms": 1 }),
            ),
        ],
        edges: vec![
            edge("t", "sub"),
            edge("sub", "pspawn"),
            edge("pspawn", "pgate"),
        ],
        ..Default::default()
    };

    let compiled = compile(&graph).expect("compile");
    let caps = mock_capabilities();
    let resumable = tokio::time::timeout(GUARD, run_resumable(&compiled, json!({}), &caps))
        .await
        .expect("initial run hung")
        .expect("initial run");
    assert_eq!(
        resumable.outcome().pending_approvals,
        vec!["sub::approve".to_string()]
    );
    assert!(resumable.outcome().output["nodes"]["pspawn"].is_null());

    let done = tokio::time::timeout(GUARD, resumable.resume(vec!["sub::approve".to_string()]))
        .await
        .expect("resume hung")
        .expect("resume");
    assert!(done.pending_approvals.is_empty());
    assert_eq!(done.output["nodes"]["pgate"]["arrived"], 1);
    assert_eq!(
        done.output["nodes"]["pgate"]["items"][0]["json"]["slug"],
        "parent.publish"
    );
}

struct ConcurrencyProbe {
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

#[async_trait::async_trait]
impl ToolInvoker for ConcurrencyProbe {
    async fn invoke(
        &self,
        _slug: &str,
        args: Value,
        _conn: Option<&str>,
    ) -> tinyflows::error::Result<Value> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        for _ in 0..6 {
            tokio::task::yield_now().await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(args)
    }
}

/// The maximum graph concurrency also bounds a 256-lane scatter, rather than
/// applying only to ordinary static fan-out branches.
#[tokio::test]
async fn a_wide_scatter_honours_the_global_admission_bound() {
    let graph = WorkflowGraph {
        name: "wide_bounded_scatter".to_string(),
        nodes: vec![
            node(
                "t",
                NodeKind::Trigger,
                json!({ "max_concurrency": 4, "recursion_limit": 500 }),
            ),
            node("scatter", NodeKind::Scatter, json!({ "path": "rows" })),
            node(
                "work",
                NodeKind::ToolCall,
                json!({ "slug": "lane.work", "args": "=item" }),
            ),
            node(
                "gather",
                NodeKind::Gather,
                json!({ "from": ["work"], "poll_interval_ms": 1 }),
            ),
        ],
        edges: vec![
            edge("t", "scatter"),
            edge("scatter", "work"),
            edge("work", "gather"),
        ],
        ..Default::default()
    };
    let probe = Arc::new(ConcurrencyProbe {
        in_flight: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let mut caps = mock_capabilities();
    caps.tools = probe.clone();
    let compiled = compile(&graph).expect("compile");
    let rows: Vec<Value> = (0..256).map(|index| json!({ "index": index })).collect();

    let outcome = tokio::time::timeout(
        GUARD,
        tinyflows::engine::run(&compiled, json!({ "rows": rows }), &caps),
    )
    .await
    .expect("wide scatter hung")
    .expect("wide scatter runs");
    let peak = probe.peak.load(Ordering::SeqCst);
    assert!(peak > 1, "lanes should overlap, observed peak {peak}");
    assert!(peak <= 4, "max_concurrency=4 admitted {peak} lanes at once");
    assert_eq!(
        outcome.output["nodes"]["gather"]["items"]
            .as_array()
            .map(Vec::len),
        Some(256)
    );
}

struct SelectiveFailure;

#[async_trait::async_trait]
impl ToolInvoker for SelectiveFailure {
    async fn invoke(
        &self,
        _slug: &str,
        args: Value,
        _conn: Option<&str>,
    ) -> tinyflows::error::Result<Value> {
        if args.get("fail").and_then(Value::as_bool) == Some(true) {
            Err(tinyflows::error::EngineError::Capability(
                "scheduled lane failure".to_string(),
            ))
        } else {
            Ok(args)
        }
    }
}

fn failing_lane_graph(policy: &str) -> WorkflowGraph {
    WorkflowGraph {
        name: format!("lane_errors_{policy}"),
        nodes: vec![
            node("t", NodeKind::Trigger, json!({ "recursion_limit": 100 })),
            node("scatter", NodeKind::Scatter, json!({ "path": "rows" })),
            node(
                "work",
                NodeKind::ToolCall,
                json!({ "slug": "lane.maybe_fail", "args": "=item" }),
            ),
            node(
                "gather",
                NodeKind::Gather,
                json!({
                    "from": ["work"],
                    "on_lane_error": policy,
                    "poll_interval_ms": 1,
                }),
            ),
        ],
        edges: vec![
            edge("t", "scatter"),
            edge("scatter", "work"),
            edge("work", "gather"),
        ],
        ..Default::default()
    }
}

/// One lane fails while its siblings succeed, under all three gather policies.
#[tokio::test]
async fn lane_failures_collect_skip_or_fail_fast_as_configured() {
    let input = json!({ "rows": [
        { "id": 0 },
        { "id": 1, "fail": true },
        { "id": 2 }
    ] });
    let mut caps = mock_capabilities();
    caps.tools = Arc::new(SelectiveFailure);

    let collect = compile(&failing_lane_graph("collect")).expect("compile collect");
    let collected = tinyflows::engine::run(&collect, input.clone(), &caps)
        .await
        .expect("collect run");
    let items = collected.output["nodes"]["gather"]["items"]
        .as_array()
        .expect("collected items");
    assert_eq!(items.len(), 3);
    assert_eq!(
        items
            .iter()
            .filter(|item| item["json"]["failed"] == true)
            .count(),
        1
    );

    let skip = compile(&failing_lane_graph("skip")).expect("compile skip");
    let skipped = tinyflows::engine::run(&skip, input.clone(), &caps)
        .await
        .expect("skip run");
    assert_eq!(
        skipped.output["nodes"]["gather"]["items"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );

    let fail_fast = compile(&failing_lane_graph("fail_fast")).expect("compile fail_fast");
    let error = tinyflows::engine::run(&fail_fast, input, &caps)
        .await
        .expect_err("fail_fast must fail the run");
    assert!(error.to_string().contains("scheduled lane failure"));
}

/// Handled lane errors stay in lane-local state for both recovery policies.
/// The routed form also proves that different lanes can take different ports
/// and still reconverge at one gather.
#[tokio::test]
async fn lane_error_continue_and_route_never_write_the_top_level_slot() {
    let input = json!({ "rows": [
        { "id": 0 },
        { "id": 1, "fail": true },
        { "id": 2 }
    ] });
    let mut caps = mock_capabilities();
    caps.tools = Arc::new(SelectiveFailure);

    let mut continued = failing_lane_graph("collect");
    continued.name = "lane_continue".to_string();
    continued
        .nodes
        .iter_mut()
        .find(|node| node.id == "work")
        .expect("work node")
        .config["on_error"] = json!("continue");
    let outcome = tinyflows::engine::run(
        &compile(&continued).expect("compile continue"),
        input.clone(),
        &caps,
    )
    .await
    .expect("continue run");
    assert!(outcome.output["nodes"]["work"].get("items").is_none());
    assert_eq!(
        outcome.output["nodes"]["gather"]["items"]
            .as_array()
            .map(Vec::len),
        Some(3)
    );

    let routed = WorkflowGraph {
        name: "lane_error_route".to_string(),
        nodes: vec![
            node("t", NodeKind::Trigger, json!({ "recursion_limit": 100 })),
            node("scatter", NodeKind::Scatter, json!({ "path": "rows" })),
            node(
                "work",
                NodeKind::ToolCall,
                json!({
                    "slug": "lane.maybe_fail",
                    "args": "=item",
                    "on_error": "route",
                }),
            ),
            node(
                "success",
                NodeKind::Transform,
                json!({ "set": { "route": "main" } }),
            ),
            node(
                "recover",
                NodeKind::Transform,
                json!({ "set": { "route": "error" } }),
            ),
            node(
                "gather",
                NodeKind::Gather,
                json!({ "from": ["success", "recover"], "poll_interval_ms": 1 }),
            ),
        ],
        edges: vec![
            edge("t", "scatter"),
            edge("scatter", "work"),
            port_edge("work", "main", "success"),
            port_edge("work", "error", "recover"),
            edge("success", "gather"),
            edge("recover", "gather"),
        ],
        ..Default::default()
    };
    let outcome = tinyflows::engine::run(&compile(&routed).expect("compile route"), input, &caps)
        .await
        .expect("route run");
    assert!(outcome.output["nodes"]["work"].get("items").is_none());
    let items = outcome.output["nodes"]["gather"]["items"]
        .as_array()
        .expect("gather items");
    assert_eq!(items.len(), 3);
    assert_eq!(
        items
            .iter()
            .filter(|item| item["json"]["route"] == "error")
            .count(),
        1,
        "only the failing lane takes the error arm"
    );
}
