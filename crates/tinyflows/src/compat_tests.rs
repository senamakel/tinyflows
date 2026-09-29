//! Tests for the engine-compatibility refusal.
//!
//! This gate fails closed, which makes its false-positive obligation the sharp
//! one: it refuses a graph outright, so every shape it *accepts* is a shape a
//! user can still save. The accepting cases below are therefore not padding —
//! each is a topology that is fine and that a coarser rule ("any fan-in behind
//! any condition") would have taken away.

use serde_json::json;

use super::*;

fn graph(value: serde_json::Value) -> WorkflowGraph {
    serde_json::from_value(value).expect("graph parses")
}

/// The refused shape: `a` reaches the fan-in from behind two branching
/// decisions, so the relief picks only the outer one and either fires early —
/// dropping `a`'s data — or never fires at all.
#[test]
fn a_fan_in_predecessor_behind_two_branchers_is_refused() {
    let g = graph(json!({
        "name": "nested-conditional-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "o" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "i" } },
            { "id": "outer_else", "kind": "output_parser", "name": "Outer else" },
            { "id": "inner_else", "kind": "output_parser", "name": "Inner else" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "true", "to_node": "inner" },
            { "from_node": "outer", "from_port": "false", "to_node": "outer_else" },
            { "from_node": "inner", "from_port": "true", "to_node": "a" },
            { "from_node": "inner", "from_port": "false", "to_node": "inner_else" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));

    let errors = errors(&g);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(errors[0].code, UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN);
    assert_eq!(errors[0].node_id.as_deref(), Some("m"));
}

/// One brancher is exactly what the relief handles, so this must stay savable.
#[test]
fn a_fan_in_predecessor_behind_one_brancher_is_accepted() {
    let g = graph(json!({
        "name": "one-level-mixed-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "cond", "kind": "condition", "name": "Condition", "config": { "field": "f" } },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "other", "kind": "output_parser", "name": "Other" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "cond" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "cond", "from_port": "true", "to_node": "a" },
            { "from_node": "cond", "from_port": "false", "to_node": "other" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));

    assert!(errors(&g).is_empty(), "{:?}", errors(&g));
}

/// Nesting on its own is not the problem — an unrelieved fan-in is. Without one
/// there is nothing for the lowering to get wrong.
#[test]
fn nesting_without_a_fan_in_is_accepted() {
    let g = graph(json!({
        "name": "nested-without-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "o" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "i" } },
            { "id": "outer_else", "kind": "output_parser", "name": "Outer else" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "inner_else", "kind": "output_parser", "name": "Inner else" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "outer", "from_port": "true", "to_node": "inner" },
            { "from_node": "outer", "from_port": "false", "to_node": "outer_else" },
            { "from_node": "inner", "from_port": "true", "to_node": "a" },
            { "from_node": "inner", "from_port": "false", "to_node": "inner_else" }
        ]
    }));

    assert!(errors(&g).is_empty(), "{:?}", errors(&g));
}

/// A predecessor reachable from the trigger by `main`-only edges is
/// unconditional: it always runs, so the barrier never needs relieving.
#[test]
fn an_unconditional_fan_in_is_accepted() {
    let g = graph(json!({
        "name": "unconditional-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "a" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));

    assert!(errors(&g).is_empty(), "{:?}", errors(&g));
}

/// An inline child is part of the graph and is walked, with the refusal
/// attributed to the node that carries it — otherwise the author is told a node
/// id that appears nowhere in the file they are editing.
#[test]
fn an_inline_sub_workflow_child_is_walked_and_its_refusal_is_attributed() {
    let child = json!({
        "name": "child",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "o" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "i" } },
            { "id": "outer_else", "kind": "output_parser", "name": "Outer else" },
            { "id": "inner_else", "kind": "output_parser", "name": "Inner else" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "true", "to_node": "inner" },
            { "from_node": "outer", "from_port": "false", "to_node": "outer_else" },
            { "from_node": "inner", "from_port": "true", "to_node": "a" },
            { "from_node": "inner", "from_port": "false", "to_node": "inner_else" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    });
    let g = graph(json!({
        "name": "parent",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "call", "kind": "sub_workflow", "name": "Call",
              "config": { "workflow": child } }
        ],
        "edges": [{ "from_node": "start", "from_port": "main", "to_node": "call" }]
    }));

    let errors = errors(&g);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0]
            .message
            .starts_with("Inline sub_workflow node 'call'"),
        "{}",
        errors[0].message
    );
}

/// The depth budget is the run's, not the child's, which is why it can be
/// passed in: a host resolving a *saved* child mid-chain has to check it to the
/// remaining depth the root allows.
#[test]
fn a_zero_depth_budget_stops_the_walk_before_any_inline_child() {
    let child = json!({
        "name": "child",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "o" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "i" } },
            { "id": "outer_else", "kind": "output_parser", "name": "Outer else" },
            { "id": "inner_else", "kind": "output_parser", "name": "Inner else" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "true", "to_node": "inner" },
            { "from_node": "outer", "from_port": "false", "to_node": "outer_else" },
            { "from_node": "inner", "from_port": "true", "to_node": "a" },
            { "from_node": "inner", "from_port": "false", "to_node": "inner_else" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    });
    let g = graph(json!({
        "name": "parent",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "call", "kind": "sub_workflow", "name": "Call",
              "config": { "workflow": child } }
        ],
        "edges": [{ "from_node": "start", "from_port": "main", "to_node": "call" }]
    }));

    assert!(errors_with_max_depth(&g, 0).is_empty());
    assert_eq!(errors_with_max_depth(&g, 1).len(), 1);
}

/// The cap comes off the trigger when it declares one, so a graph that
/// legitimately nests deeper is still checked all the way down.
#[test]
fn the_depth_budget_is_read_off_the_trigger() {
    let g = graph(json!({
        "name": "declares-a-cap",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger",
              "config": { "max_sub_workflow_depth": 9 } }
        ],
        "edges": []
    }));
    assert_eq!(max_sub_workflow_depth(&g), 9);

    let bare = graph(json!({
        "name": "declares-nothing",
        "nodes": [{ "id": "start", "kind": "trigger", "name": "Trigger" }],
        "edges": []
    }));
    assert_eq!(
        max_sub_workflow_depth(&bare),
        crate::engine::MAX_SUB_WORKFLOW_DEPTH
    );
}

// ---- main-port labels, loop back-edges, router exhaustiveness ----
//
// Ported from OpenHuman's host-side compatibility tests, which only ever
// exercised this crate's `errors` through a wrapper.

/// A switch's `main` label reaching a fan-in beside a fan-out sibling.
fn main_port_conditional_fan_in_graph() -> WorkflowGraph {
    graph(json!({
        "name": "main-port-conditional-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "route", "kind": "switch", "name": "Route", "config": { "field": "kind" } },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "other", "kind": "output_parser", "name": "Other" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "route" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "route", "from_port": "main", "to_node": "a" },
            { "from_node": "route", "from_port": "other", "to_node": "other" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }))
}

/// `outer` (condition) -> `inner` (`inner_kind`, wired on `inner_ports`) -> `a`,
/// reconverging with `c` at merge `m`.
fn nested_router_reconvergence_graph(inner_kind: &str, inner_ports: &[&str]) -> WorkflowGraph {
    let mut edges = vec![
        json!({ "from_node": "start", "from_port": "main", "to_node": "outer" }),
        json!({ "from_node": "start", "from_port": "main", "to_node": "c" }),
        json!({ "from_node": "outer", "from_port": "true", "to_node": "inner" }),
        json!({ "from_node": "outer", "from_port": "false", "to_node": "outer_else" }),
    ];
    edges.extend(
        inner_ports
            .iter()
            .map(|port| json!({ "from_node": "inner", "from_port": port, "to_node": "a" })),
    );
    edges.extend([
        json!({ "from_node": "a", "from_port": "main", "to_node": "m" }),
        json!({ "from_node": "c", "from_port": "main", "to_node": "m" }),
    ]);

    graph(json!({
        "name": "nested-router-reconvergence",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "outer" } },
            { "id": "inner", "kind": inner_kind, "name": "Inner", "config": { "field": "inner" } },
            { "id": "outer_else", "kind": "output_parser", "name": "Outer else" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": edges
    }))
}

#[test]
fn engine_compatibility_rejects_main_label_on_conditional_fan_in_path() {
    let g = main_port_conditional_fan_in_graph();
    let errs = errors(&g);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].code, UNSUPPORTED_MAIN_PORT_CONDITIONAL_FAN_IN);
    assert_eq!(errs[0].node_id.as_deref(), Some("m"));

    let reconverged = graph(json!({
        "name": "main-port-reconverges-before-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "route", "kind": "switch", "name": "Route", "config": { "field": "kind" } },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "route" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "route", "from_port": "main", "to_node": "a" },
            { "from_node": "route", "from_port": "default", "to_node": "a" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));
    assert!(errors(&reconverged).is_empty());
}

/// A loop head has two incoming edges, and this gate mirrors the engine's
/// fan-in classification — so without excluding back-edges it would report
/// every legal bounded loop as an unrelieved fan-in and refuse to save it.
#[test]
fn engine_compatibility_does_not_treat_a_loop_back_edge_as_a_fan_in() {
    let looping = graph(json!({
        "name": "bounded-loop",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "l", "kind": "loop", "name": "Loop",
              "config": { "max_iterations": 3, "on_exceeded": "continue" } },
            { "id": "work", "kind": "output_parser", "name": "Work" },
            { "id": "out", "kind": "output_parser", "name": "Out" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "l" },
            { "from_node": "l", "from_port": "body", "to_node": "work" },
            { "from_node": "work", "from_port": "main", "to_node": "l" },
            { "from_node": "l", "from_port": "done", "to_node": "out" }
        ]
    }));
    assert!(
        errors(&looping).is_empty(),
        "a bounded loop must save cleanly: {:?}",
        errors(&looping)
    );
}

#[test]
fn engine_compatibility_requires_exhaustive_router_choices_for_reconvergence() {
    let exhaustive_condition = nested_router_reconvergence_graph("condition", &["true", "false"]);
    assert!(errors(&exhaustive_condition).is_empty());

    let missing_condition_branch = nested_router_reconvergence_graph("condition", &["true"]);
    let errs = errors(&missing_condition_branch);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].code, UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN);

    let exhaustive_switch = nested_router_reconvergence_graph("switch", &["known-case", "default"]);
    assert!(errors(&exhaustive_switch).is_empty());

    // Same-port fan-out is unconditional: TinyFlows schedules both `main`
    // successors. A side path after an exhaustive router must not make the
    // reconverging path look like another conditional choice.
    let exhaustive_switch_with_main_fanout = graph(json!({
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "outer" } },
            { "id": "inner", "kind": "switch", "name": "Inner", "config": { "field": "inner" } },
            { "id": "outer_else", "kind": "output_parser", "name": "Outer else" },
            { "id": "fanout", "kind": "output_parser", "name": "Fan out" },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "side", "kind": "output_parser", "name": "Side" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "true", "to_node": "inner" },
            { "from_node": "outer", "from_port": "false", "to_node": "outer_else" },
            { "from_node": "inner", "from_port": "known-case", "to_node": "fanout" },
            { "from_node": "inner", "from_port": "default", "to_node": "fanout" },
            { "from_node": "fanout", "from_port": "main", "to_node": "a" },
            { "from_node": "fanout", "from_port": "main", "to_node": "side" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));
    assert!(errors(&exhaustive_switch_with_main_fanout).is_empty());

    // A switch with only `default` is exhaustive: every input takes that edge,
    // so it is an unconditional step even though it has a single wired port.
    let default_only_switch = nested_router_reconvergence_graph("switch", &["default"]);
    assert!(errors(&default_only_switch).is_empty());

    let missing_switch_default =
        nested_router_reconvergence_graph("switch", &["known-case", "other-case"]);
    let errs = errors(&missing_switch_default);
    assert!(!errs.is_empty());
    assert!(errs
        .iter()
        .all(|error| error.code == UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN));
    // Both the switch's own reconvergence and the downstream merge are unsafe;
    // multiple switch ports may also report the same predecessor. Pin the
    // affected fan-ins without coupling the test to diagnostic multiplicity.
    assert!(errs
        .iter()
        .any(|error| error.node_id.as_deref() == Some("a")));
    assert!(errs
        .iter()
        .any(|error| error.node_id.as_deref() == Some("m")));
}

#[test]
fn engine_compatibility_rejects_reconvergence_before_nested_router() {
    let g = graph(json!({
        "name": "reconverged-before-nested-router",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "condition", "name": "Outer", "config": { "field": "outer" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "inner" } },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "inner_else", "kind": "output_parser", "name": "Inner else" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "true", "to_node": "inner" },
            { "from_node": "outer", "from_port": "false", "to_node": "inner" },
            { "from_node": "inner", "from_port": "true", "to_node": "a" },
            { "from_node": "inner", "from_port": "false", "to_node": "inner_else" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));
    let errs = errors(&g);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].code, UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN);
}

#[test]
fn engine_compatibility_treats_single_wired_router_outputs_as_conditional() {
    let g = graph(json!({
        "name": "single-wired-nested-router-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "switch", "name": "Outer", "config": { "field": "outer" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "inner" } },
            { "id": "a", "kind": "output_parser", "name": "A" },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "case", "to_node": "inner" },
            { "from_node": "inner", "from_port": "true", "to_node": "a" },
            { "from_node": "a", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));

    let errs = errors(&g);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].code, UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN);
    assert_eq!(errs[0].node_id.as_deref(), Some("m"));
}

#[test]
fn engine_compatibility_detects_a_router_directly_preceding_fan_in() {
    let nested = graph(json!({
        "name": "direct-nested-router-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "outer", "kind": "switch", "name": "Outer", "config": { "field": "outer" } },
            { "id": "inner", "kind": "condition", "name": "Inner", "config": { "field": "inner" } },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "outer" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "outer", "from_port": "case", "to_node": "inner" },
            { "from_node": "inner", "from_port": "true", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));
    let errs = errors(&nested);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].code, UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN);

    let main_port = graph(json!({
        "name": "direct-main-port-router-fan-in",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "route", "kind": "switch", "name": "Route", "config": { "field": "kind" } },
            { "id": "c", "kind": "output_parser", "name": "C" },
            { "id": "m", "kind": "merge", "name": "Merge" }
        ],
        "edges": [
            { "from_node": "start", "from_port": "main", "to_node": "route" },
            { "from_node": "start", "from_port": "main", "to_node": "c" },
            { "from_node": "route", "from_port": "main", "to_node": "m" },
            { "from_node": "c", "from_port": "main", "to_node": "m" }
        ]
    }));
    let errs = errors(&main_port);
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].code, UNSUPPORTED_MAIN_PORT_CONDITIONAL_FAN_IN);
}

