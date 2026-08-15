#![cfg(feature = "mock")]
//! TEMPORARY repro harness for the resume determinism race. Not for commit.

mod support;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use support::graphgen::{Shape, graph_of};
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::compiler::compile;
use tinyflows::engine::{
    InMemoryCheckpointer, resume_with_checkpointer, run, run_with_checkpointer,
};

const GUARD: Duration = Duration::from_secs(20);

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

fn run_both_ways(shape: &Shape) -> Option<(Value, Value)> {
    let graph = graph_of(shape);
    let compiled = compile(&graph).ok()?;
    let gates = shape.gate_ids();

    runtime().block_on(async {
        let caps = mock_capabilities();

        let straight = tokio::time::timeout(
            GUARD,
            run(&compiled, json!({ "approvals": gates.clone() }), &caps),
        )
        .await
        .expect("pre-approved run hung")
        .expect("pre-approved run should complete");

        let checkpointer = Arc::new(InMemoryCheckpointer::default());
        let thread_id = "fuzz-resume";
        let mut outcome = tokio::time::timeout(
            GUARD,
            run_with_checkpointer(&compiled, json!({}), &caps, checkpointer.clone(), thread_id),
        )
        .await
        .expect("suspending run hung")
        .expect("suspending run should pause rather than fail");

        let mut rounds = 0;
        while !outcome.pending_approvals.is_empty() {
            rounds += 1;
            assert!(rounds <= gates.len() + 2, "resume did not converge");
            let pending = outcome.pending_approvals.clone();
            outcome = tokio::time::timeout(
                GUARD,
                resume_with_checkpointer(&compiled, &caps, checkpointer.clone(), thread_id, pending),
            )
            .await
            .expect("resume hung")
            .expect("resume should succeed");
        }

        Some((straight.output, outcome.output))
    })
}

fn shape() -> Shape {
    Shape::Gate(Box::new(Shape::Branch(
        Box::new(Shape::Spawned {
            tasks: 3,
            release: "all",
            n: 3,
        }),
        Box::new(Shape::Fanout(vec![
            Shape::Linear(2),
            Shape::Linear(0),
            Shape::Spawned {
                tasks: 3,
                release: "all",
                n: 3,
            },
        ])),
    )))
}

#[test]
fn repro_resume_determinism() {
    let threads: usize = std::env::var("REPRO_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let iters: usize = std::env::var("REPRO_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let handles: Vec<_> = (0..threads)
        .map(|t| {
            std::thread::spawn(move || {
                let shape = shape();
                let mut counts: std::collections::BTreeMap<u64, usize> =
                    std::collections::BTreeMap::new();
                let mut baseline: Option<Value> = None;
                let mut diverged = 0usize;
                for _ in 0..iters {
                    let (_, resumed) = run_both_ways(&shape).expect("shape should compile");
                    let polls = resumed["nodes"]["n21"]["polls"].as_u64().unwrap_or(0);
                    *counts.entry(polls).or_default() += 1;
                    match &baseline {
                        None => baseline = Some(resumed),
                        Some(first) => {
                            if first != &resumed {
                                diverged += 1;
                                let a = first["nodes"].as_object().cloned().unwrap_or_default();
                                let b = resumed["nodes"].as_object().cloned().unwrap_or_default();
                                for (id, slot) in &a {
                                    let other = b.get(id);
                                    if other != Some(slot) {
                                        println!(
                                            "  node {id}:\n    A = {slot}\n    B = {}",
                                            other.cloned().unwrap_or(Value::Null)
                                        );
                                    }
                                }
                                for (id, slot) in &b {
                                    if !a.contains_key(id) {
                                        println!("  node {id} only in B: {slot}");
                                    }
                                }
                            }
                        }
                    }
                }
                println!("thread {t}: poll histogram {counts:?}, diverged {diverged}/{iters}");
                diverged
            })
        })
        .collect();
    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total, 0, "{total} runs diverged");
}
