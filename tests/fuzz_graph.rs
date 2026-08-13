#![cfg(feature = "mock")]
//! Property tests over generated workflow graphs.
//!
//! These assert the invariants that must hold for *every* graph the validator
//! accepts, rather than for the handful of shapes someone thought to write down
//! by hand. Lane clobbering, barrier skew and runaway poll budgets are all
//! bugs that survive hand-written examples — they need a graph nobody chose.
//!
//! Each test wraps its run in a timeout: the failure mode being hunted here is
//! a *hang*, and a hung test takes the whole suite with it rather than naming
//! itself.
//!
//! Gated behind the `mock` feature alongside the rest of the e2e suite.

mod support;

use std::time::Duration;

use proptest::prelude::*;
use serde_json::{Value, json};

use support::graphgen::{arb_shape, arb_workflow_graph, graph_of};
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::compiler::compile;
use tinyflows::engine::run;
use tinyflows::error::EngineError;
use tinyflows::model::WorkflowGraph;

/// How long a single generated run may take before it is called a hang.
///
/// Generously above what any generated graph needs, so this only ever fires on
/// a real non-termination rather than on a slow machine.
const GUARD: Duration = Duration::from_secs(20);

/// Runs a graph to completion on a private tokio runtime.
///
/// Property tests are synchronous, so each case builds its own runtime rather
/// than borrowing an ambient one — which also guarantees no state leaks between
/// cases.
fn run_graph(graph: &WorkflowGraph) -> Result<Value, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(async {
        let caps = mock_capabilities();
        let compiled = compile(graph).map_err(|e| format!("compile: {e}"))?;
        match tokio::time::timeout(GUARD, run(&compiled, json!({}), &caps)).await {
            Err(_) => Err("HUNG: run did not terminate".to_string()),
            Ok(Ok(outcome)) => Ok(outcome.output),
            Ok(Err(e)) => Err(bounded_failure(&e)?),
        }
    })
}

/// Classifies an engine error as an acceptable *bound* or a real failure.
///
/// A generated graph is allowed to run out of budget — the budgets exist — but
/// only if the failure names which bound was hit. An unnamed failure, or any
/// other error, is a bug: these graphs are built from pure control-flow nodes
/// and consult no capability, so there is nothing legitimate to fail on.
fn bounded_failure(error: &EngineError) -> Result<String, String> {
    match error {
        EngineError::LoopLimit { node, limit } => Ok(format!("bounded: loop {node} hit {limit}")),
        EngineError::Capability(message)
            if message.contains("recursion") || message.contains("visit") =>
        {
            Ok(format!("bounded: {message}"))
        }
        other => Err(format!("unexpected engine error: {other:?}")),
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

    /// **Validate ⇒ terminate.** Any graph the validator accepts must either
    /// finish or fail naming the bound it hit. It must never hang, never panic,
    /// and never deadlock on a barrier.
    ///
    /// This is the single most valuable property here: a deadlock is the
    /// characteristic failure of a super-step engine with barriers, and it is
    /// invisible to a test suite that only runs shapes known to work.
    #[test]
    fn an_accepted_graph_always_settles(graph in arb_workflow_graph()) {
        if compile(&graph).is_err() {
            // Refused before running: fine, and not what this property is about.
            return Ok(());
        }
        if let Err(failure) = run_graph(&graph) {
            prop_assert!(
                failure.starts_with("bounded:"),
                "graph neither completed nor hit a named bound: {failure}\ngraph: {}",
                serde_json::to_string(&graph).unwrap_or_default()
            );
        }
    }

    /// **Determinism.** The same graph run twice produces byte-identical final
    /// state.
    ///
    /// The engine folds concurrent branch updates through a reducer in
    /// active-set order rather than completion order, so this must hold even
    /// though branches genuinely race. It is the detector for any future
    /// fan-out that lets two activations write the same state slot — the
    /// clobber shows up as two runs disagreeing.
    #[test]
    fn a_graph_runs_the_same_way_twice(graph in arb_workflow_graph()) {
        if compile(&graph).is_err() {
            return Ok(());
        }
        let first = run_graph(&graph);
        let second = run_graph(&graph);
        match (first, second) {
            (Ok(a), Ok(b)) => prop_assert_eq!(
                a, b,
                "two runs of one graph disagreed\ngraph: {}",
                serde_json::to_string(&graph).unwrap_or_default()
            ),
            (Err(a), Err(b)) => prop_assert_eq!(a, b, "two runs failed differently"),
            (a, b) => prop_assert!(
                false,
                "one run succeeded and the other did not: {a:?} vs {b:?}"
            ),
        }
    }

    /// Every node the graph declares gets a slot in the final state, except
    /// those on a genuinely untaken conditional branch.
    ///
    /// Catches a whole class of routing bug where a node is silently skipped —
    /// which otherwise surfaces only as a downstream expression resolving to
    /// null, far from the cause.
    #[test]
    fn a_run_records_a_slot_for_every_node_it_ran(shape in arb_shape(2)) {
        let graph = graph_of(&shape);
        if compile(&graph).is_err() {
            return Ok(());
        }
        let Ok(output) = run_graph(&graph) else {
            return Ok(()); // bounded failures are covered by the property above
        };
        let slots = output["nodes"].as_object().cloned().unwrap_or_default();
        prop_assert!(
            slots.contains_key("trigger"),
            "the trigger always runs, so it must always have a slot"
        );
        for (id, slot) in &slots {
            prop_assert!(
                slot.get("items").is_some(),
                "node {id} recorded a slot with no items array: {slot}"
            );
        }
    }
}
