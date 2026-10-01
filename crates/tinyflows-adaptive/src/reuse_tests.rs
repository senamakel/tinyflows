use super::*;
use serde_json::json;
use tinyflows::model::{Node, NodeKind};

fn graph_with(config: Value) -> WorkflowGraph {
    WorkflowGraph {
        schema_version: 1,
        id: Some("g".into()),
        name: "g".into(),
        inputs: Vec::new(),
        agents: Vec::new(),
        nodes: vec![Node {
            id: "step".into(),
            kind: NodeKind::Agent,
            type_version: 1,
            name: "step".into(),
            config,
            ports: Vec::new(),
            position: None,
        }],
        edges: Vec::new(),
    }
}

fn inputs(pairs: &[(&str, &str)]) -> serde_json::Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), json!(v)))
        .collect()
}

#[test]
fn a_value_read_through_an_input_is_reusable() {
    let graph = graph_with(json!({ "prompt": "review the PRs on =run.inputs.repo" }));
    assert!(baked_in(&graph, &inputs(&[("repo", "acme/thing")])).is_empty());
}

#[test]
fn the_same_value_pasted_in_is_a_one_off() {
    let graph = graph_with(json!({ "prompt": "review the PRs on acme/thing" }));
    assert_eq!(
        baked_in(&graph, &inputs(&[("repo", "acme/thing")])),
        vec!["acme/thing"]
    );
}

#[test]
fn it_looks_inside_nested_config_not_just_the_top_level() {
    let graph = graph_with(json!({
        "args": { "targets": ["/docs/q3.pdf", "=run.inputs.other"] }
    }));
    assert_eq!(
        baked_in(&graph, &inputs(&[("path", "/docs/q3.pdf")])),
        vec!["/docs/q3.pdf"]
    );
}

#[test]
fn an_expression_reading_the_value_by_path_is_not_a_paste() {
    // `=run.inputs.repo | ascii_downcase` resolves the value at run time —
    // the graph works for the next repo too.
    let graph = graph_with(json!({ "prompt": "=run.inputs.repo | ascii_downcase" }));
    assert!(baked_in(&graph, &inputs(&[("repo", "acme/thing")])).is_empty());
}

#[test]
fn a_value_welded_into_an_expressions_quoted_literal_is_a_paste() {
    // `="acme/thing"` evaluates to exactly the pasted text: expression
    // syntax around a literal changes nothing about its reusability. Since
    // recipe lowering made every generated prompt an expression, this is
    // the common shape of a paste, not an edge case.
    let graph = graph_with(json!({ "prompt": "=\"review acme/thing directly\"" }));
    assert_eq!(
        baked_in(&graph, &inputs(&[("repo", "acme/thing")])),
        vec!["acme/thing".to_string()]
    );
}

#[test]
fn plain_short_words_prove_nothing_and_are_not_evidence() {
    // `main` is the default port name on every edge in the graph, so a node
    // containing it says nothing about where the input went. A gate that
    // fires on that refuses perfectly reusable procedures.
    let graph = graph_with(json!({ "branch": "main", "mode": "on" }));
    assert!(baked_in(&graph, &inputs(&[("branch", "main"), ("mode", "on")])).is_empty());
}

#[test]
fn a_bare_short_digit_is_not_evidence() {
    // "1" appears in half of all configs; treating it as a paste would
    // refuse reusable procedures on noise.
    let graph = graph_with(json!({ "max_items": "10", "prompt": "top 1 result" }));
    assert!(baked_in(&graph, &inputs(&[("n", "1"), ("count", "10")])).is_empty());
}

#[test]
fn a_short_value_with_structure_is_still_evidence() {
    // Short but unmistakable: nothing else in a config is `a/b` or has a
    // ticket number in it by coincidence.
    for (key, value) in [("repo", "a/b"), ("ticket", "P-91")] {
        let graph = graph_with(json!({ "prompt": format!("do {value}") }));
        assert_eq!(
            baked_in(&graph, &inputs(&[(key, value)])),
            vec![value.to_string()],
            "{value} should read as pasted"
        );
    }
}

#[test]
fn a_key_that_matches_is_not_a_paste() {
    // Configs are keyed by field names, and a goal may name one. Only the
    // values a node would send are evidence.
    let graph = graph_with(json!({ "acme/thing": "=run.inputs.repo" }));
    assert!(baked_in(&graph, &inputs(&[("repo", "acme/thing")])).is_empty());
}

#[test]
fn a_graph_with_no_inputs_at_all_is_reusable_by_default() {
    // "summarise today's pull requests" has no parameters. Nothing was
    // given, so nothing could have been baked in.
    let graph = graph_with(json!({ "prompt": "summarise today's pull requests" }));
    assert!(baked_in(&graph, &inputs(&[])).is_empty());
}

#[test]
fn the_digest_is_the_documented_algorithm_not_a_std_implementation_detail() {
    // Pinned to FNV-1a's published test vectors: if this fails, persisted
    // identifiers changed and every stored score is orphaned.
    assert_eq!(digest_hex(b""), "cbf29ce484222325");
    assert_eq!(digest_hex(b"a"), "af63dc4c8601ec8c");
}

#[test]
fn every_pasted_value_is_reported_not_only_the_first() {
    // A caller renders these into an explanation of why a graph was not
    // kept, and one at a time turns that into a conversation.
    let graph = graph_with(json!({
        "prompt": "review acme/thing at /docs/q3.pdf"
    }));
    let found = baked_in(
        &graph,
        &inputs(&[("repo", "acme/thing"), ("path", "/docs/q3.pdf")]),
    );
    assert_eq!(found.len(), 2, "{found:?}");
}
