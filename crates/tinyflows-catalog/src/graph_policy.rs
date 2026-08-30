//! Policy predicates over a [`WorkflowGraph`] — the questions a host has to
//! answer *before* it saves or runs a graph, and whose answers do not depend on
//! which host is asking.
//!
//! Two safety rules live here, and both exist because the honest default for a
//! freshly authored workflow is "does nothing until a human says so":
//!
//! 1. **A graph that fires unattended must not save itself armed.**
//!    [`trigger_is_automatic`] says whether a graph's trigger can fire without
//!    anyone asking it to; a host uses that to persist `enabled: false` until
//!    the author arms it explicitly.
//! 2. **A graph that can act on the world must require approval.**
//!    [`enforce_side_effect_approval`] overrides a caller's `require_approval:
//!    false` when [`graph_has_outbound_side_effect`] holds, on create *and* on
//!    a later edit that adds such a node to a previously read-only graph.
//!
//! [`graph_has_actionable_nodes`] is a third, quieter check: whether a graph
//! has anything to do at all, so a host can say so instead of reporting a
//! successful run that did nothing.
//!
//! What is *not* here is anything about which trigger kinds a particular host
//! has actually wired to a dispatcher. "This host does not deliver webhooks
//! yet" is a fact about that host and belongs in its own overlay.

use tinyflows::model::{NodeKind, TriggerKind, WorkflowGraph};

/// Whether `graph`'s trigger fires **without a human in the loop** — i.e. on
/// a timer, an inbound webhook, or a connected-app event, as opposed to
/// `manual` (only ever fired by an explicit `flows_run`). Used by
/// a host to decide whether a freshly-saved flow may persist `enabled: true`
/// or must persist `enabled: false` until the author arms it explicitly.
///
/// Deliberately broader than "which kinds does this host dispatch today":
/// a host that has not wired webhooks yet WILL fire them unattended the moment
/// it does, so a webhook-trigger flow must not be handed to the author
/// pre-armed either. Returns `false` for a graph with no single
/// resolvable trigger node or no `trigger_kind` discriminator (never a
/// surprise — it never self-fires).
pub fn trigger_is_automatic(graph: &WorkflowGraph) -> bool {
    let Some(trigger) = graph.trigger() else {
        return false;
    };
    let Some(kind_value) = trigger.config.get("trigger_kind") else {
        return false;
    };
    let Ok(kind) = serde_json::from_value::<TriggerKind>(kind_value.clone()) else {
        return false;
    };
    matches!(
        kind,
        TriggerKind::Schedule | TriggerKind::AppEvent | TriggerKind::Webhook
    )
}

/// Whether `graph` contains a node that can produce a real outbound side
/// effect — `tool_call` (a curated integration action), `http_request`, or
/// `code` (sandboxed but Turing-complete, can reach the network). Used by
/// a host to force `require_approval: true` on
/// any graph that can act on the world, regardless of what the caller
/// passed. A graph built only from `trigger` / `agent` / `transform` /
/// `condition` / data-flow nodes is read-only and unaffected.
pub fn graph_has_outbound_side_effect(graph: &WorkflowGraph) -> bool {
    graph.nodes.iter().any(|n| {
        matches!(
            n.kind,
            NodeKind::ToolCall | NodeKind::HttpRequest | NodeKind::Code
        )
    })
}

/// Shared side-effect enforcement: forces `require_approval` to `true` when `graph` contains an
/// outbound side-effect node, no matter what the caller asked for. Used by both
/// the create and the update paths so a flow can never persist
/// `require_approval: false` alongside a `tool_call` / `http_request` / `code`
/// node — on create OR on a later edit that *adds* such a node to a
/// previously-read-only graph.
///
/// Returns `(effective_require_approval, was_forced)`: `was_forced` is `true`
/// only when the caller's own toggle was `false` but a side-effect node
/// required the override — callers use it to decide whether to emit the
/// loud "forced to true" log/result note.
pub fn enforce_side_effect_approval(
    graph: &WorkflowGraph,
    caller_require_approval: bool,
) -> (bool, bool) {
    let has_side_effect = graph_has_outbound_side_effect(graph);
    let effective_require_approval = caller_require_approval || has_side_effect;
    let was_forced = has_side_effect && !caller_require_approval;
    (effective_require_approval, was_forced)
}

/// Whether `graph` has anything for a run to actually *do* — i.e. at
/// least one non-`trigger` node **reachable from the trigger** by following
/// directed edges. A graph made of nothing but a bare `trigger` node (or a
/// `trigger` plus unreachable/disconnected nodes — even ones wired to each
/// other by their own edges, just not to the trigger) can compile and "run"
/// cleanly while producing no work whatsoever — the exact live finding this
/// guards: a trigger-only flow reported `status="completed"
/// pending_approvals=0` having done nothing, which reads as a successful
/// automation to anyone not staring at the node count. A host uses it to attach a human-readable note to an otherwise-silent "success".
///
/// Deliberately a reachability walk rather than "any edge at all exists":
/// `nodes.len() > 1 && !edges.is_empty()` would count a disconnected
/// component's internal edges as actionable even though nothing downstream
/// of the trigger ever runs.
pub fn graph_has_actionable_nodes(graph: &WorkflowGraph) -> bool {
    let Some(trigger) = graph.trigger() else {
        // No single resolvable trigger to walk from — fall back to the
        // coarse "any non-trigger node wired up by an edge" check so a
        // malformed/ambiguous-trigger graph doesn't spuriously suppress the
        // empty-flow note.
        return graph.nodes.iter().any(|n| n.kind != NodeKind::Trigger) && !graph.edges.is_empty();
    };

    let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut stack = vec![trigger.id.as_str()];
    while let Some(current) = stack.pop() {
        if !visited.insert(current) {
            continue;
        }
        for next in graph.successors(current) {
            if !visited.contains(next) {
                stack.push(next);
            }
        }
    }

    visited
        .into_iter()
        .filter_map(|id| graph.node(id))
        .any(|n| n.kind != NodeKind::Trigger)
}


#[cfg(test)]
mod tests {
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
            g.nodes.push(serde_json::from_value(json!({
                "id": "n", "kind": kind, "name": "N", "config": {}
            }))
            .expect("second node"));
            g.edges = serde_json::from_value(json!([{ "from_node": "t", "to_node": "n" }])).expect("edge");
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
    fn the_three_acting_node_kinds_are_outbound_side_effects() {
        for kind in [NodeKind::ToolCall, NodeKind::HttpRequest, NodeKind::Code] {
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
        assert_eq!(enforce_side_effect_approval(&readonly, false), (false, false));
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
        g.edges = serde_json::from_value(json!([{ "from_node": "a", "to_node": "b" }])).expect("orphan edge");
        assert!(!graph_has_actionable_nodes(&g));
    }
}
