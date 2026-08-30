use super::*;
use serde_json::json;

/// A graph with one `trigger` node carrying `config`, plus one node of
/// `kind` wired to it.
fn graph(trigger_config: serde_json::Value, kind: Option<NodeKind>) -> WorkflowGraph {
    let mut g: WorkflowGraph = serde_json::from_value(json!({
        "nodes": [{ "id": "t", "kind": "trigger", "name": "T", "config": trigger_config }],
        "edges": []
    }))
    .expect("trigger-only graph");
    if let Some(kind) = kind {
        g.nodes.push(
            serde_json::from_value(json!({
                "id": "n", "kind": kind, "name": "N", "config": {}
            }))
            .expect("second node"),
        );
        g.edges =
            serde_json::from_value(json!([{ "from_node": "t", "to_node": "n" }])).expect("edge");
    }
    g
}

#[test]
fn a_schedule_trigger_is_automatic_and_a_manual_one_is_not() {
    assert!(trigger_is_automatic(&graph(
        json!({ "trigger_kind": "schedule" }),
        None
    )));
    assert!(!trigger_is_automatic(&graph(
        json!({ "trigger_kind": "manual" }),
        None
    )));
}

/// A graph with no discriminator never self-fires, so it is not automatic —
/// the conservative answer here is the *permissive* one, and that is only
/// safe because "no trigger kind" genuinely means "nothing dispatches it".
#[test]
fn a_trigger_without_a_kind_is_not_automatic() {
    assert!(!trigger_is_automatic(&graph(json!({}), None)));
}

/// A webhook fires unattended the moment a host wires it, so it counts even
/// where no host dispatches one yet. Handing an author a pre-armed webhook
/// flow is the failure this prevents.
#[test]
fn a_webhook_trigger_counts_as_automatic_before_any_host_dispatches_one() {
    assert!(trigger_is_automatic(&graph(
        json!({ "trigger_kind": "webhook" }),
        None
    )));
}

#[test]
fn the_four_acting_node_kinds_are_outbound_side_effects() {
    for kind in [
        NodeKind::ToolCall,
        NodeKind::HttpRequest,
        NodeKind::Code,
        NodeKind::Shell,
    ] {
        let label = format!("{kind:?}");
        assert!(
            graph_has_outbound_side_effect(&graph(json!({}), Some(kind))),
            "{label} must count as an outbound side effect"
        );
    }
    assert!(!graph_has_outbound_side_effect(&graph(
        json!({}),
        Some(NodeKind::Agent)
    )));
}

/// The override is one-way: a side effect forces approval on, and a caller
/// who already asked for approval is not reported as having been forced.
#[test]
fn approval_is_forced_on_only_when_the_caller_asked_for_less() {
    let acting = graph(json!({}), Some(NodeKind::HttpRequest));
    assert_eq!(enforce_side_effect_approval(&acting, false), (true, true));
    assert_eq!(enforce_side_effect_approval(&acting, true), (true, false));

    let readonly = graph(json!({}), Some(NodeKind::Agent));
    assert_eq!(
        enforce_side_effect_approval(&readonly, false),
        (false, false)
    );
    assert_eq!(enforce_side_effect_approval(&readonly, true), (true, false));
}

/// A bare trigger has nothing to do, and saying so is the whole point: such
/// a graph otherwise completes successfully having run nothing.
#[test]
fn a_trigger_only_graph_has_nothing_to_do() {
    assert!(!graph_has_actionable_nodes(&graph(json!({}), None)));
    assert!(graph_has_actionable_nodes(&graph(
        json!({}),
        Some(NodeKind::Agent)
    )));
}

/// Reachability, not "an edge exists somewhere": a component wired only to
/// itself never runs, and counting its edges would suppress the note that
/// the graph does nothing.
#[test]
fn nodes_unreachable_from_the_trigger_do_not_count_as_actionable() {
    let mut g = graph(json!({}), None);
    g.nodes.extend(
        serde_json::from_value::<Vec<_>>(json!([
            { "id": "a", "kind": "agent", "name": "A", "config": {} },
            { "id": "b", "kind": "agent", "name": "B", "config": {} }
        ]))
        .expect("orphan nodes"),
    );
    g.edges =
        serde_json::from_value(json!([{ "from_node": "a", "to_node": "b" }])).expect("orphan edge");
    assert!(!graph_has_actionable_nodes(&g));
}
