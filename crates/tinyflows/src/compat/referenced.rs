//! Compatibility of *saved* workflows a graph references by id.
//!
//! [`errors`](super::errors) walks inline children. A `sub_workflow` that names a
//! saved workflow (`workflow_id`) needs the host's store to look the child up, so
//! the walk takes a resolver closure and the host keeps the store.

use std::collections::HashMap;

use serde_json::Value;

use super::{errors_with_max_depth, max_sub_workflow_depth};
use crate::model::{NodeKind, WorkflowGraph};

/// The first engine-incompatible topology reachable through literal
/// `workflow_id` children of `graph`, as a one-element list (empty when none).
///
/// `resolve` maps a saved workflow id to its graph, or `None` when it is
/// missing or cannot be loaded; such references are skipped and keep their
/// runtime diagnostics. Dynamic `=` expressions and ids sharing a node with an
/// inline `workflow` are skipped too: this only rejects a saved graph whose
/// topology is demonstrably unsafe.
///
/// The walk descends as deep as the root declares it may nest
/// ([`max_sub_workflow_depth`]), and checks each saved child to the *remaining*
/// depth budget, because the engine's runtime depth counter is one budget shared
/// across the whole inline-plus-referenced chain.
pub fn referenced_workflow_errors(
    graph: &WorkflowGraph,
    resolve: &dyn Fn(&str) -> Option<WorkflowGraph>,
) -> Vec<String> {
    let max_depth = max_sub_workflow_depth(graph);
    let mut pending = vec![(graph.clone(), 0_u64, Vec::<String>::new())];
    // Record the shallowest visit, not just whether an id was seen. The same
    // child can be referenced by multiple branches; a deep DFS visit must not
    // suppress a later shallower visit that has more depth budget remaining.
    let mut visited_depths = HashMap::<String, u64>::new();

    while let Some((current, depth, path)) = pending.pop() {
        if depth >= max_depth {
            continue;
        }

        for node in &current.nodes {
            if node.kind != NodeKind::SubWorkflow {
                continue;
            }

            let mut child_path = path.clone();
            child_path.push(node.id.clone());

            let inline = node.config.get("workflow");
            let configured_workflow_id = node
                .config
                .get("workflow_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty());
            // Structural validation requires exactly one source and runs before
            // this helper. Retain that precedence defensively if a caller passes
            // an invalid graph directly: do not inspect either source as though
            // the engine could choose between them at runtime.
            if inline.is_some() && configured_workflow_id.is_some() {
                continue;
            }

            if let Some(inline) = inline {
                if let Ok(child) = serde_json::from_value::<WorkflowGraph>(inline.clone()) {
                    pending.push((child, depth + 1, child_path.clone()));
                }
                continue;
            }

            let Some(workflow_id) = configured_workflow_id.filter(|id| !id.starts_with('=')) else {
                continue;
            };
            let child_depth = depth + 1;
            if visited_depths
                .get(workflow_id)
                .is_some_and(|seen_depth| *seen_depth <= child_depth)
            {
                continue;
            }
            visited_depths.insert(workflow_id.to_string(), child_depth);

            let Some(child) = resolve(workflow_id) else {
                continue;
            };
            // Thread the root's remaining depth budget through, not the child's
            // own cap.
            let remaining_depth = max_depth.saturating_sub(child_depth);
            if let Some(error) = errors_with_max_depth(&child, remaining_depth)
                .into_iter()
                .next()
            {
                return vec![format!(
                    "Sub_workflow path '{}' references workflow_id '{}' with an unsupported \
                     engine topology: {}: {}",
                    child_path.join(" -> "),
                    workflow_id,
                    error.code,
                    error.message
                )];
            }
            pending.push((child, child_depth, child_path));
        }
    }

    Vec::new()
}

#[cfg(test)]
#[path = "referenced_tests.rs"]
mod tests;
