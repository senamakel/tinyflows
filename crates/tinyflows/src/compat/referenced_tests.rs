use std::collections::HashMap;

use serde_json::{Value, json};

use super::*;

fn graph(value: Value) -> WorkflowGraph {
    serde_json::from_value(value).expect("graph parses")
}

/// A root that only calls the saved workflow `id`.
fn calls(id: &str) -> WorkflowGraph {
    graph(json!({
        "name": "parent",
        "nodes": [
            { "id": "start", "kind": "trigger", "name": "Trigger" },
            { "id": "child", "kind": "sub_workflow", "name": "Child",
              "config": { "workflow_id": id } }
        ],
        "edges": [{ "from_node": "start", "from_port": "main", "to_node": "child" }]
    }))
}

/// The refused shape: `a` reaches the fan-in from behind two branching decisions.
fn unsafe_child() -> WorkflowGraph {
    graph(json!({
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
    }))
}

fn store(entries: Vec<(&str, WorkflowGraph)>) -> impl Fn(&str) -> Option<WorkflowGraph> {
    let map: HashMap<String, WorkflowGraph> = entries
        .into_iter()
        .map(|(id, g)| (id.to_string(), g))
        .collect();
    move |id| map.get(id).cloned()
}

#[test]
fn an_unsafe_saved_child_is_refused_with_its_path_and_code() {
    let resolve = store(vec![("bad", unsafe_child())]);
    let errors = referenced_workflow_errors(&calls("bad"), &resolve);
    assert_eq!(
        errors,
        vec![format!(
            "Sub_workflow path 'child' references workflow_id 'bad' with an unsupported \
             engine topology: {}: {}",
            crate::compat::UNSUPPORTED_NESTED_CONDITIONAL_FAN_IN,
            crate::compat::errors(&unsafe_child())[0].message,
        )]
    );
}

#[test]
fn a_grandchild_reached_through_a_safe_child_is_attributed_through_both_levels() {
    let resolve = store(vec![("mid", calls("bad")), ("bad", unsafe_child())]);
    let errors = referenced_workflow_errors(&calls("mid"), &resolve);
    assert_eq!(errors.len(), 1);
    assert!(
        errors[0].starts_with("Sub_workflow path 'child -> child' references workflow_id 'bad'"),
        "{errors:?}"
    );
}

#[test]
fn safe_missing_and_dynamic_references_are_not_refused() {
    let safe = graph(json!({
        "name": "safe",
        "nodes": [{ "id": "start", "kind": "trigger", "name": "Trigger" }],
        "edges": []
    }));
    let resolve = store(vec![("safe", safe)]);
    assert!(referenced_workflow_errors(&calls("safe"), &resolve).is_empty());
    assert!(referenced_workflow_errors(&calls("missing"), &resolve).is_empty());
    assert!(referenced_workflow_errors(&calls("=inputs.which"), &resolve).is_empty());
    assert!(referenced_workflow_errors(&calls("  "), &resolve).is_empty());
}

#[test]
fn a_reference_cycle_terminates_and_is_not_an_error() {
    let resolve = store(vec![("a", calls("b")), ("b", calls("a"))]);
    assert!(referenced_workflow_errors(&calls("a"), &resolve).is_empty());
}

#[test]
fn a_root_depth_cap_of_zero_never_resolves_a_child() {
    let mut root = calls("bad");
    root.nodes[0].config = json!({ "max_sub_workflow_depth": 0 });
    let resolve = |_: &str| -> Option<WorkflowGraph> { panic!("resolver must not be called") };
    assert!(referenced_workflow_errors(&root, &resolve).is_empty());
}
