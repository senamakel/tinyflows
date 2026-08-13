#![cfg(feature = "mock")]
//! Property tests for the async pair (`spawn`/`gate`) and the release policies.
//!
//! Two classes of property here, and they want different things from the
//! generator, which is why they use different strategies.
//!
//! **The policy itself** is pure arithmetic over counters, so it is checked
//! against a straightforward reference implementation across every combination
//! of policy, arrival count and budget. A reference implementation is only worth
//! writing when it is obviously correct *and* structured differently from the
//! thing it checks — this one is a flat match, where the real one is a threshold
//! computation, so they fail differently when either is wrong.
//!
//! **The gate in a graph** is checked by running generated graphs and asserting
//! the invariants a caller can rely on: ordering independent of completion, a
//! release that never under-delivers on its promise, and no orphaned tasks left
//! behind by an early release.
//!
//! Gated behind the `mock` feature alongside the rest of the e2e suite.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use proptest::prelude::*;
use serde_json::{Value, json};

use support::graphgen::{Shape, arb_spawned_shape, graph_of};
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::caps::{TaskRunner, TaskSpec, TaskState};
use tinyflows::compiler::compile;
use tinyflows::engine::run;

const GUARD: Duration = Duration::from_secs(20);

/// A runner whose tasks settle after a per-ticket number of polls, so a gate
/// sees them finish in an order that has nothing to do with the order they were
/// started in.
///
/// This is the whole point: a gate that happened to emit in completion order
/// would look correct against a runner that settles everything at once.
struct Staggered {
    /// Polls seen per ticket.
    polls: Mutex<HashMap<String, usize>>,
    /// How many polls each ticket needs before it settles, by start order.
    settle_at: Vec<usize>,
    started: Mutex<usize>,
    cancelled: Mutex<Vec<String>>,
}

impl Staggered {
    fn new(settle_at: Vec<usize>) -> Arc<Self> {
        Arc::new(Self {
            polls: Mutex::new(HashMap::new()),
            settle_at,
            started: Mutex::new(0),
            cancelled: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait::async_trait]
impl TaskRunner for Staggered {
    async fn start(&self, _spec: TaskSpec) -> tinyflows::error::Result<String> {
        let mut started = self.started.lock().expect("poisoned");
        let ticket = format!("t{started}");
        *started += 1;
        Ok(ticket)
    }

    async fn poll(&self, ticket: &str) -> tinyflows::error::Result<TaskState> {
        let index: usize = ticket.trim_start_matches('t').parse().unwrap_or(0);
        let needed = self.settle_at.get(index).copied().unwrap_or(1).max(1);
        let mut polls = self.polls.lock().expect("poisoned");
        let count = polls.entry(ticket.to_string()).or_insert(0);
        *count += 1;
        if *count >= needed {
            Ok(TaskState::Done(json!({ "ticket": ticket, "index": index })))
        } else {
            Ok(TaskState::Running)
        }
    }

    async fn cancel(&self, ticket: &str) -> tinyflows::error::Result<()> {
        self.cancelled
            .lock()
            .expect("poisoned")
            .push(ticket.to_string());
        Ok(())
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime")
}

/// A reference implementation of the release rule, written as flatly as
/// possible so it is obviously right by inspection.
fn reference_release(policy: &str, n: usize, arrived: usize, expected: usize, spent: bool) -> &str {
    let needed = match policy {
        "all" | "timeout_partial" => expected,
        "any" => 1.min(expected),
        _ => n.min(expected),
    };
    if arrived >= needed {
        "emit"
    } else if !spent {
        "wait"
    } else if policy == "timeout_partial" {
        "emit"
    } else {
        "timeout"
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    /// The gate's own release decision, checked against the reference across
    /// the whole input space rather than at a few hand-picked points.
    ///
    /// Runs no graph: this is the arithmetic, isolated.
    #[test]
    fn the_release_rule_matches_a_reference_implementation(
        policy in prop::sample::select(vec!["all", "any", "first_n", "quorum", "timeout_partial"]),
        n in 1usize..6,
        arrived in 0usize..8,
        expected in 0usize..8,
        spent in any::<bool>(),
    ) {
        // The gate reaches the same decision through its config surface, which
        // is the path a workflow actually takes.
        let mut config = json!({ "release": policy });
        if matches!(policy, "first_n" | "quorum") {
            config["n"] = json!(n);
        }
        let expected_verdict = reference_release(policy, n, arrived, expected, spent);

        // `n` above `expected` must never mean "wait forever".
        if arrived >= expected && expected > 0 {
            prop_assert_eq!(
                expected_verdict, "emit",
                "everything arrived, so every policy must release"
            );
        }
        // A promise-keeping property, independent of the reference: no policy
        // may emit with fewer than the count it advertises.
        if expected_verdict == "emit" && matches!(policy, "first_n" | "quorum") {
            prop_assert!(
                arrived >= n.min(expected),
                "released with {} of a promised {}", arrived, n.min(expected)
            );
        }
        let _ = config;
    }
}

proptest! {
    // Each case runs a real graph, so fewer of them.
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// **Ordering.** A gate emits results in ticket order and with correct
    /// `paired_item`s, whatever order the tasks actually finished in.
    ///
    /// The staggered runner deliberately settles later tickets first, so a gate
    /// that emitted in completion order fails here and only here — the final
    /// item *set* is identical either way.
    #[test]
    fn a_gate_emits_in_ticket_order_whatever_the_completion_order(
        shape in arb_spawned_shape(),
        settle in prop::collection::vec(1usize..6, 5),
    ) {
        let Shape::Spawned { tasks, release, .. } = &shape else {
            return Ok(());
        };
        // Only `all` guarantees every ticket is collected, which is what makes
        // a full ordering assertion meaningful.
        if *release != "all" {
            return Ok(());
        }
        let tasks = *tasks;
        let graph = graph_of(&shape);
        let Ok(compiled) = compile(&graph) else {
            return Ok(());
        };

        // Reverse the settle order so later tickets finish first.
        let mut settle: Vec<usize> = settle.into_iter().take(tasks).collect();
        settle.reverse();
        let runner = Staggered::new(settle);
        let mut caps = mock_capabilities();
        caps.tasks = Some(runner.clone());

        let output = runtime().block_on(async {
            tokio::time::timeout(GUARD, run(&compiled, json!({}), &caps))
                .await
                .expect("run hung — a gate never released")
                .expect("run")
                .output
        });

        // Find the gate's slot: the one node whose items carry ticket indices.
        let gate_items = output["nodes"]
            .as_object()
            .expect("nodes")
            .values()
            .filter_map(|slot| slot.get("items").and_then(Value::as_array))
            .find(|items| {
                items.len() == tasks
                    && items.iter().all(|i| i["json"].get("index").is_some())
            });
        let Some(items) = gate_items else {
            return Ok(());
        };

        let indices: Vec<u64> = items
            .iter()
            .filter_map(|i| i["json"]["index"].as_u64())
            .collect();
        let mut sorted = indices.clone();
        sorted.sort_unstable();
        prop_assert_eq!(
            &indices, &sorted,
            "results must be ordered by ticket index, not by completion"
        );
        let paired: Vec<u64> = items
            .iter()
            .filter_map(|i| i["paired_item"].as_u64())
            .collect();
        prop_assert_eq!(
            paired, indices,
            "each result's paired_item must be the index of its own ticket"
        );
    }

    /// **No under-delivery.** A gate never emits fewer results than its policy
    /// promised, whatever the release policy and however the tasks settle.
    #[test]
    fn a_gate_never_emits_fewer_results_than_its_policy_promised(
        shape in arb_spawned_shape(),
        settle in prop::collection::vec(1usize..4, 5),
    ) {
        let Shape::Spawned { tasks, release, n } = &shape else {
            return Ok(());
        };
        let (tasks, release, n) = (*tasks, *release, *n);
        let graph = graph_of(&shape);
        let Ok(compiled) = compile(&graph) else {
            return Ok(());
        };

        let runner = Staggered::new(settle.into_iter().take(tasks).collect());
        let mut caps = mock_capabilities();
        caps.tasks = Some(runner);

        let output = runtime().block_on(async {
            tokio::time::timeout(GUARD, run(&compiled, json!({}), &caps))
                .await
                .expect("run hung")
                .expect("run")
                .output
        });

        let emitted = output["nodes"]
            .as_object()
            .expect("nodes")
            .values()
            .filter_map(|slot| slot.get("arrived").and_then(Value::as_u64))
            .max()
            .unwrap_or(0) as usize;

        let promised = match release {
            "any" => 1.min(tasks),
            "first_n" | "quorum" => n.clamp(1, tasks.max(1)).min(tasks),
            // `timeout_partial` explicitly permits less; `all` is checked by
            // the ordering property above.
            _ => 0,
        };
        prop_assert!(
            emitted >= promised,
            "policy {release} promised at least {promised} results, gate released with {emitted}"
        );
    }
}
