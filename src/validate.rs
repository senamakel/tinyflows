//! Structural validation for [`WorkflowGraph`]s, run before compilation.

use std::collections::HashSet;

use serde_json::Value;

use crate::error::ValidationError;
use crate::model::{NodeKind, WorkflowGraph, is_valid_input_name};

/// The node kind's wire discriminator (`tool_call`, `sub_workflow`, …) for use
/// in error messages, so a validation error names the kind the way the graph
/// JSON spells it rather than in Rust's `PascalCase`.
fn kind_name(kind: &NodeKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{kind:?}"))
}

/// Validates a workflow graph's structure.
///
/// Currently checks: unique node ids, exactly one trigger node, that every edge
/// references existing nodes, no duplicate edges, per-node `on_error` policy
/// sanity (a known value, and an `error` edge when the policy is `route`),
/// declared-input sanity (addressable, unique names; defaults that match their
/// declared type), and loop legality (see [`validate_loops`] — cycles are
/// permitted; only the ones that cannot iterate are refused).
///
/// # Errors
/// Returns the first [`ValidationError`] encountered. For a full list of every
/// structural problem in one pass (so an author can fix them all at once
/// instead of one round-trip per error), use [`validate_all`]; this function is
/// exactly its first element and is kept for the fail-fast compile path.
pub fn validate(graph: &WorkflowGraph) -> Result<(), ValidationError> {
    match validate_all(graph).into_iter().next() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Validates a workflow graph's structure, collecting **every** independent
/// error in one pass.
///
/// Returns an empty `Vec` for a valid graph. The checks are ordered
/// deterministically (duplicate ids → trigger count → edge integrity →
/// `on_error` policy → per-kind config → condition routing → declared inputs),
/// and every error is self-contained (no check can panic on a graph that failed
/// an earlier one), so accumulating is safe. The first element is identical to what
/// [`validate`] returns, preserving the historical single-error contract.
///
/// This is what a host should surface to an author or agent: fixing five
/// problems then costs one validate call, not five.
pub fn validate_all(graph: &WorkflowGraph) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    let mut seen = HashSet::new();
    for node in &graph.nodes {
        if !seen.insert(node.id.as_str()) {
            errors.push(ValidationError::DuplicateNodeId(node.id.clone()));
        }
    }

    let triggers: Vec<String> = graph
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Trigger)
        .map(|n| n.id.clone())
        .collect();
    match triggers.len() {
        0 => errors.push(ValidationError::MissingTrigger),
        1 => {}
        _ => errors.push(ValidationError::MultipleTriggers(triggers)),
    }

    let mut seen_edges = HashSet::new();
    for edge in &graph.edges {
        if graph.node(&edge.from_node).is_none() {
            errors.push(ValidationError::UnknownNode(edge.from_node.clone()));
        }
        if graph.node(&edge.to_node).is_none() {
            errors.push(ValidationError::UnknownNode(edge.to_node.clone()));
        }
        // Reject two identical edges (same source node/port and destination
        // node/port); a redundant duplicate is almost always an authoring slip.
        if !seen_edges.insert((
            edge.from_node.as_str(),
            edge.from_port.as_str(),
            edge.to_node.as_str(),
            edge.to_port.as_str(),
        )) {
            errors.push(ValidationError::DuplicateEdge {
                from_node: edge.from_node.clone(),
                from_port: edge.from_port.clone(),
                to_node: edge.to_node.clone(),
                to_port: edge.to_port.clone(),
            });
        }
    }

    // Per-node `on_error` policy checks. The policy is free-form config read at
    // run time; catch mistakes at author time: an unknown value (which would
    // silently fall through to `stop`) and a `route` policy with no `error`
    // edge to carry the routed item (which would be silently dropped).
    for node in &graph.nodes {
        let Some(on_error) = node.config.get("on_error").and_then(Value::as_str) else {
            continue;
        };
        match on_error {
            "stop" | "continue" => {}
            "route" => {
                if node.kind == NodeKind::Void {
                    // An `error` edge is still an outgoing edge, which the
                    // `void` check below forbids. Caught here instead of
                    // letting `MissingErrorRoute` fire, or the author would be
                    // told to add an edge that the next rule then rejects —
                    // advice with no fixed point.
                    errors.push(ValidationError::InvalidNodeConfig {
                        node: node.id.clone(),
                        reason: "`void` is a terminal sink, so on_error \"route\" has nowhere to \
                                 route to (an `error` edge is an outgoing edge); use \"stop\" or \
                                 \"continue\""
                            .to_string(),
                    });
                } else {
                    let has_error_edge = graph
                        .edges
                        .iter()
                        .any(|e| e.from_node == node.id && e.from_port == "error");
                    if !has_error_edge {
                        errors.push(ValidationError::MissingErrorRoute(node.id.clone()));
                    }
                }
            }
            other => {
                errors.push(ValidationError::InvalidOnError {
                    node: node.id.clone(),
                    value: other.to_string(),
                });
            }
        }
    }

    // Per-kind config checks. A `sub_workflow` node must reference its child
    // exactly one way: an inline `workflow` graph OR a `workflow_id` reference,
    // never both and never neither (the reference form is resolved at run time
    // via the host `WorkflowResolver`).
    for node in &graph.nodes {
        if node.kind == NodeKind::SubWorkflow {
            let has_inline = node.config.get("workflow").is_some();
            let has_ref = node
                .config
                .get("workflow_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.is_empty());
            if has_inline == has_ref {
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: "sub_workflow requires exactly one of `workflow` (inline) or \
                             `workflow_id` (reference)"
                        .to_string(),
                });
            }
        }
    }

    // Per-item fan-out config (`execution` / `concurrency` / `on_item_error`).
    // These select the execution strategy, so an unrecognized value cannot be
    // caught at run time without silently changing behaviour — a bad
    // `concurrency` would quietly stay sequential and a bad `on_item_error`
    // would quietly pick a default. Reject them here, where the message can name
    // the node.
    for node in &graph.nodes {
        let fans_out = matches!(
            node.kind,
            NodeKind::Agent
                | NodeKind::ToolCall
                | NodeKind::HttpRequest
                | NodeKind::Memory
                | NodeKind::SubWorkflow
        );

        if let Some(execution) = node.config.get("execution") {
            match execution.as_str() {
                Some("once" | "per_item") if fans_out => {}
                Some("once" | "per_item") => {
                    errors.push(ValidationError::InvalidNodeConfig {
                        node: node.id.clone(),
                        reason: format!(
                            "`execution` is not supported on a {} node (only agent, tool_call, \
                             http_request, memory, and sub_workflow map over their input)",
                            kind_name(&node.kind)
                        ),
                    });
                }
                _ => {
                    errors.push(ValidationError::InvalidNodeConfig {
                        node: node.id.clone(),
                        reason: format!(
                            "unknown `execution` value {execution} (expected \"once\" or \
                             \"per_item\")"
                        ),
                    });
                }
            }
        }

        // Whether this node actually maps over its input, accounting for the
        // per-kind default: `tool_call` / `http_request` / `memory` are per-item
        // unless told otherwise; `agent` / `sub_workflow` are not.
        let per_item = match node.config.get("execution").and_then(Value::as_str) {
            Some("per_item") => true,
            Some("once") => false,
            _ => matches!(
                node.kind,
                NodeKind::ToolCall | NodeKind::HttpRequest | NodeKind::Memory
            ),
        };

        for key in ["concurrency", "on_item_error"] {
            let Some(value) = node.config.get(key) else {
                continue;
            };
            // A fan-out knob on a node that runs once is a no-op, and a silent
            // no-op reads as "I asked for parallelism and got none".
            if !per_item {
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: format!(
                        "`{key}` has no effect without `execution: \"per_item\"` on a {} node",
                        kind_name(&node.kind)
                    ),
                });
                continue;
            }
            let ok = match key {
                "concurrency" => {
                    matches!(
                        value,
                        Value::Number(n) if n.as_u64().is_some(),
                    ) || value.as_str() == Some("all")
                }
                _ => matches!(value.as_str(), Some("collect" | "fail_fast" | "skip")),
            };
            if !ok {
                let expected = if key == "concurrency" {
                    "a non-negative integer or \"all\""
                } else {
                    "\"collect\", \"fail_fast\", or \"skip\""
                };
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: format!("`{key}` must be {expected}, got {value}"),
                });
            }
        }
    }

    // `memory` node config checks, including THE hard security invariant: a
    // `remember`/`forget` operation may never target `scope: "user"` — the
    // caller's durable, cross-flow memory. Rejecting this structurally, at the
    // door, means a workflow (or an LLM authoring one) can never plant or erase
    // durable facts about the user by way of a `remember`/`forget` node; the
    // only scope those two operations may write through is `"flow"`.
    for node in &graph.nodes {
        if node.kind != NodeKind::Memory {
            continue;
        }

        let operation = node.config.get("operation").and_then(Value::as_str);
        let Some(operation) = operation else {
            errors.push(ValidationError::InvalidNodeConfig {
                node: node.id.clone(),
                reason: "memory node requires `operation` (recall|search|flavour|people|\
                         remember|forget)"
                    .to_string(),
            });
            continue;
        };
        if !matches!(
            operation,
            "recall" | "search" | "flavour" | "people" | "remember" | "forget"
        ) {
            errors.push(ValidationError::InvalidNodeConfig {
                node: node.id.clone(),
                reason: format!(
                    "memory node has unknown operation {operation:?} (expected one of \
                     recall|search|flavour|people|remember|forget)"
                ),
            });
            continue;
        }

        let scope = node.config.get("scope").and_then(Value::as_str);

        // THE hard invariant (see the block comment above): reject before any
        // other config check, so it can never be masked by a different error.
        // remember/forget may write ONLY scope "flow". BOTH read-only scopes are
        // rejected here — "user" (the user's durable memory) and "flows"
        // (cross-flow read). This gate is unbypassable precisely because `scope`
        // is validated as a literal enum (below): an "=expr" binding resolves at
        // runtime and is never one of user|flow|flows, so it fails the enum
        // check and can never smuggle a write past this into
        // provider.remember/forget. If a future change makes `scope` bindable,
        // this invariant reopens — keep the enum check.
        if matches!(operation, "remember" | "forget")
            && matches!(scope, Some("user") | Some("flows"))
        {
            let bad = scope.unwrap_or_default();
            errors.push(ValidationError::InvalidNodeConfig {
                node: node.id.clone(),
                reason: format!(
                    "memory node operation {operation:?} may not target scope {bad:?} — \
                     remember/forget may only write scope \"flow\"; scopes \"user\" and \
                     \"flows\" are read-only from a workflow"
                ),
            });
        }

        if let Some(scope) = scope {
            if !matches!(scope, "user" | "flow" | "flows") {
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: format!(
                        "memory node has unknown scope {scope:?} (expected \
                         user|flow|flows)"
                    ),
                });
            }
        }

        // `scope` is required for recall/remember/forget (not search/flavour/
        // people — see the catalog contract for the exact per-operation table).
        if matches!(operation, "recall" | "remember" | "forget") && scope.is_none() {
            errors.push(ValidationError::InvalidNodeConfig {
                node: node.id.clone(),
                reason: format!("memory node operation {operation:?} requires `scope`"),
            });
        }

        if matches!(operation, "recall" | "search") {
            let has_query = node
                .config
                .get("query")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty());
            if !has_query {
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: format!("memory node operation {operation:?} requires `query`"),
                });
            }
        }

        if operation == "flavour" {
            let has_flavour = node
                .config
                .get("flavour")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty());
            if !has_flavour {
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: "memory node operation \"flavour\" requires `flavour` (slug)"
                        .to_string(),
                });
            }
        }

        if matches!(operation, "remember" | "forget") {
            let has_key = node
                .config
                .get("key")
                .and_then(Value::as_str)
                .is_some_and(|s| !s.is_empty());
            if !has_key {
                errors.push(ValidationError::InvalidNodeConfig {
                    node: node.id.clone(),
                    reason: format!("memory node operation {operation:?} requires `key`"),
                });
            }
        }

        if operation == "remember" && node.config.get("value").is_none() {
            errors.push(ValidationError::InvalidNodeConfig {
                node: node.id.clone(),
                reason: "memory node operation \"remember\" requires `value`".to_string(),
            });
        }
    }

    // `dedup` node config checks: `key` (the per-item "=expr" dedup key) is the
    // only config field, and it is required — a dedup node with no `key` can
    // never resolve anything to compare, which is always an authoring mistake
    // (as opposed to a `key` that *resolves* to null at run time, which is the
    // intentional, per-item fail-open path the executor handles).
    for node in &graph.nodes {
        if node.kind != NodeKind::Dedup {
            continue;
        }
        let has_key = node
            .config
            .get("key")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty());
        if !has_key {
            errors.push(ValidationError::InvalidNodeConfig {
                node: node.id.clone(),
                reason: "dedup node requires `key` (an \"=expr\" resolved per item, e.g. \
                         \"=item.id\")"
                    .to_string(),
            });
        }
    }

    // A `condition` node's outgoing edges must emit on `from_port` "true" or
    // "false" — routing is keyed EXCLUSIVELY on `from_port` (see
    // `engine::outgoing_by_port` / `handler_routing`), so any other value
    // (most commonly the default `"main"`, from an authoring mistake that put
    // the branch label on `to_port` instead) is a hard authoring bug: it
    // silently degrades to a parallel `FanOut` that drives BOTH branches
    // unconditionally, with no runtime error or warning to point at the
    // mistake. Caught here, at the door, instead.
    let condition_node_ids: HashSet<&str> = graph
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::Condition)
        .map(|n| n.id.as_str())
        .collect();
    for edge in &graph.edges {
        if condition_node_ids.contains(edge.from_node.as_str())
            && edge.from_port != "true"
            && edge.from_port != "false"
        {
            errors.push(ValidationError::InvalidConditionRouting {
                node: edge.from_node.clone(),
                from_port: edge.from_port.clone(),
            });
        }
    }

    validate_loops(graph, &mut errors);
    validate_scatter_regions(graph, &mut errors);
    // Declared-input checks. These are author-time mistakes that would otherwise
    // surface as a confusing runtime `null`: a name that `=inputs.<name>` cannot
    // address, two declarations racing for the same key, a default the input's
    // own type would reject, or `required` alongside a default (which makes the
    // requirement unreachable — a default always supplies a value).
    let mut seen_inputs = HashSet::new();
    for input in &graph.inputs {
        if !is_valid_input_name(&input.name) {
            errors.push(ValidationError::InvalidInputName(input.name.clone()));
        }
        if !seen_inputs.insert(input.name.as_str()) {
            errors.push(ValidationError::DuplicateInputName(input.name.clone()));
        }
        match &input.default {
            Some(default) if !input.ty.accepts(default) => {
                errors.push(ValidationError::InputDefaultTypeMismatch {
                    name: input.name.clone(),
                    expected: input.ty.as_str(),
                });
            }
            Some(_) if input.required => {
                errors.push(ValidationError::RequiredInputWithDefault(
                    input.name.clone(),
                ));
            }
            _ => {}
        }
    }

    validate_agents(graph, &mut errors);

    errors
}

mod agents;
pub use agents::unresolved_agent_refs;
use agents::validate_agents;

mod loops;
use loops::validate_loops;

mod scatter;
use scatter::{nodes_on_cycle, path_exists, validate_scatter_regions};

#[cfg(test)]
#[path = "validate_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "fanout_tests.rs"]
mod fanout_tests;

#[cfg(test)]
#[path = "loop_tests.rs"]
mod loop_tests;
