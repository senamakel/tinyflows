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

use serde::{Deserialize, Serialize};
use tinyflows::model::{NodeKind, WorkflowGraph};

/// The trigger discriminator carried in a `trigger` node's
/// `config.trigger_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    Manual,
    Schedule,
    Webhook,
    AppEvent,
    Form,
    ExecuteByWorkflow,
    ChatMessage,
    Evaluation,
    System,
}

/// Whether `graph`'s trigger fires **without a human in the loop** — i.e. on
/// a timer, an inbound webhook, or a connected-app event, as opposed to
/// `manual` (only ever fired by an explicit `flows_run`). Used by
/// [`flows_create`] (issue B29 — save/enable safety, Rule 1) to decide
/// whether a freshly-saved flow may persist `enabled: true` or must persist
/// `enabled: false` until the user arms it explicitly via
/// `flows_set_enabled`.
///
/// Deliberately broader than [`trigger_kind_fires`]: `webhook` is not yet
/// wired to auto-dispatch in this host (see that fn's doc), but it WILL fire
/// unattended the moment it is — so a webhook-trigger flow must not be handed
/// to the user pre-armed either. Returns `false` for a graph with no single
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
/// [`flows_create`] (issue B29, Rule 2) to force `require_approval: true` on
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

/// Shared Rule 2 enforcement (issue B29, and its `flows_update` compound-bypass
/// closure): forces `require_approval` to `true` when `graph` contains an
/// outbound side-effect node, no matter what the caller asked for. Used by both
/// [`flows_create`] and [`flows_update`] so a flow can never persist
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

/// Whether `graph` has anything for [`flows_run`] to actually *do* — i.e. at
/// least one non-`trigger` node **reachable from the trigger** by following
/// directed edges. A graph made of nothing but a bare `trigger` node (or a
/// `trigger` plus unreachable/disconnected nodes — even ones wired to each
/// other by their own edges, just not to the trigger) can compile and "run"
/// cleanly while producing no work whatsoever — the exact live finding this
/// guards: a trigger-only flow reported `status="completed"
/// pending_approvals=0` having done nothing, which reads as a successful
/// automation to anyone not staring at the node count. Used by `flows_run`
/// to attach a human-readable note to an otherwise-silent "success".
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

