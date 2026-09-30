//! A short, human-readable summary of a workflow graph: the trigger in one line
//! and one entry per step with an optional config hint.
//!
//! Surfaced to a model (in a proposal tool's result) and to a UI proposal card,
//! so the shape — `{ "trigger": <string>, "steps": [{ "kind", "name",
//! "config_hint"? }] }` — is a wire contract pinned by `summary_tests`.

use serde_json::{Value, json};

use crate::model::{Node, NodeKind, WorkflowGraph};

/// Longest `config_hint` before it is cut with an ellipsis.
pub const MAX_CONFIG_HINT_CHARS: usize = 80;

/// Builds the `{ trigger, steps }` summary of `graph`.
pub fn summarize(graph: &WorkflowGraph) -> Value {
    let trigger = graph
        .trigger()
        .map(describe_trigger)
        .unwrap_or_else(|| "no trigger".to_string());

    let steps: Vec<Value> = graph
        .nodes
        .iter()
        .filter(|n| n.kind != NodeKind::Trigger)
        .map(|n| {
            let mut step = json!({
                "kind": node_kind_str(&n.kind),
                "name": n.name,
            });
            if let Some(hint) = config_hint(n) {
                step["config_hint"] = json!(hint);
            }
            step
        })
        .collect();

    json!({ "trigger": trigger, "steps": steps })
}

/// The `snake_case` wire string for a [`NodeKind`] (its `Serialize` impl),
/// for the summary/step JSON. Falls back to `"unknown"` only if serializing
/// ever somehow fails — `NodeKind`'s derive is infallible in practice.
fn node_kind_str(kind: &NodeKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

/// One-line human description of a trigger node, for the summary's
/// `"trigger"` field — e.g. `"schedule: 0 9 * * *"`, `"app event:
/// gmail/GMAIL_NEW_GMAIL_MESSAGE"`, `"manual"`.
///
/// pub fn describe_trigger(node: &Node) -> String {
    let trigger_kind = node
        .config
        .get("trigger_kind")
        .and_then(Value::as_str)
        .unwrap_or("manual");

    match trigger_kind {
        "schedule" => {
            let schedule = node.config.get("schedule");
            if let Some(expr) = schedule.and_then(|s| s.get("expr")).and_then(Value::as_str) {
                format!("schedule: {expr}")
            } else if let Some(ms) = schedule
                .and_then(|s| s.get("every_ms"))
                .and_then(Value::as_u64)
            {
                format!("schedule: every {ms}ms")
            } else if let Some(at) = schedule.and_then(|s| s.get("at")).and_then(Value::as_str) {
                format!("schedule: once at {at}")
            } else {
                "schedule (unspecified)".to_string()
            }
        }
        "app_event" => {
            let toolkit = node
                .config
                .get("toolkit")
                .and_then(Value::as_str)
                .unwrap_or("?");
            let slug = node
                .config
                .get("trigger_slug")
                .and_then(Value::as_str)
                .unwrap_or("?");
            format!("app event: {toolkit}/{slug}")
        }
        other => other.to_string(),
    }
}

/// Short, human-readable hint for a non-trigger node's config, for the
/// step's optional `"config_hint"` field. `None` when the kind has nothing
/// worth surfacing (e.g. `merge`, `output_parser`).
pub fn config_hint(node: &Node) -> Option<String> {
    let cfg = &node.config;
    match &node.kind {
        NodeKind::Agent => cfg.get("prompt").and_then(Value::as_str).map(truncate_hint),
        NodeKind::ToolCall => cfg.get("slug").and_then(Value::as_str).map(str::to_string),
        NodeKind::HttpRequest => {
            let method = cfg.get("method").and_then(Value::as_str).unwrap_or("GET");
            let url = cfg.get("url").and_then(Value::as_str).unwrap_or("?");
            Some(truncate_hint(&format!("{method} {url}")))
        }
        NodeKind::Code => cfg
            .get("language")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some("javascript".to_string())),
        NodeKind::Shell => cfg
            .get("script_path")
            .and_then(Value::as_str)
            .map(|path| truncate_hint(&format!("script: {path}")))
            .or_else(|| cfg.get("source").and_then(Value::as_str).map(truncate_hint)),
        NodeKind::Condition => cfg
            .get("field")
            .and_then(Value::as_str)
            .map(|f| format!("field: {f}")),
        NodeKind::Switch => cfg
            .get("expression")
            .and_then(Value::as_str)
            .or_else(|| cfg.get("field").and_then(Value::as_str))
            .map(truncate_hint),
        NodeKind::Transform => cfg.get("set").and_then(Value::as_object).map(|set| {
            let keys: Vec<&str> = set.keys().map(String::as_str).collect();
            truncate_hint(&format!("sets: {}", keys.join(", ")))
        }),
        NodeKind::SplitOut => cfg
            .get("path")
            .and_then(Value::as_str)
            .map(|p| format!("path: {p}")),
        NodeKind::SubWorkflow => Some("embedded sub-workflow".to_string()),
        NodeKind::Memory => {
            let operation = cfg.get("operation").and_then(Value::as_str).unwrap_or("?");
            let hint = match cfg.get("scope").and_then(Value::as_str) {
                Some(scope) => format!("{operation} · {scope}"),
                None => operation.to_string(),
            };
            Some(truncate_hint(&hint))
        }
        NodeKind::Dedup => cfg
            .get("key")
            .and_then(Value::as_str)
            .map(|k| truncate_hint(&format!("key: {k}"))),
        // The cap is the one thing worth surfacing at a glance; the engine
        // applies its own default when the key is absent, so say so rather than
        // showing nothing.
        NodeKind::Loop => {
            let max = cfg
                .get("max_iterations")
                .and_then(Value::as_u64)
                .map_or_else(|| "default".to_string(), |n| n.to_string());
            Some(match cfg.get("condition").and_then(Value::as_str) {
                Some(condition) => truncate_hint(&format!("max {max} · while {condition}")),
                None => format!("max {max}"),
            })
        }
        // What was started is the one thing worth seeing at a glance; which
        // gate collects it is an edge, and the timeline already shows edges.
        NodeKind::Spawn => {
            let target = cfg.get("target").and_then(Value::as_str).unwrap_or("?");
            let what = cfg
                .get("slug")
                .and_then(Value::as_str)
                .or_else(|| cfg.get("name").and_then(Value::as_str));
            Some(truncate_hint(&match what {
                Some(what) => format!("{target}: {what}"),
                None => target.to_string(),
            }))
        }
        // A gate and a gather both wait, and the release policy is the whole
        // question — `any` versus `all` is the difference between a run that
        // proceeds on one result and one that blocks on the slowest.
        NodeKind::Gate | NodeKind::Gather => {
            let release = cfg
                .get("release")
                .and_then(Value::as_str)
                .unwrap_or("all")
                .to_string();
            Some(match cfg.get("n").and_then(Value::as_u64) {
                Some(n) => format!("{release} ({n})"),
                None => release,
            })
        }
        NodeKind::Scatter => {
            let over = cfg
                .get("path")
                .and_then(Value::as_str)
                .map_or_else(|| "input items".to_string(), |p| format!("path: {p}"));
            Some(truncate_hint(
                &match cfg.get("lanes").and_then(Value::as_u64) {
                    Some(lanes) => format!("{over} · {lanes} lanes"),
                    None => over,
                },
            ))
        }
        NodeKind::Approval => cfg
            .get("title")
            .and_then(Value::as_str)
            .or_else(|| cfg.get("prompt").and_then(Value::as_str))
            .map(truncate_hint)
            .or_else(|| Some("human review".to_string())),
        // A void takes no config, and "discards its input" is what the kind
        // already says on the timeline.
        NodeKind::Void => None,
        NodeKind::Merge | NodeKind::OutputParser | NodeKind::Trigger => None,
    }
}

/// Truncates a hint string to [`MAX_CONFIG_HINT_CHARS`], appending an
/// ellipsis when it was cut — mirrors
/// `tinytools::render_context_value`'s truncation
/// behavior for tool-call timeline details.
fn truncate_hint(s: &str) -> String {
    if s.chars().count() <= MAX_CONFIG_HINT_CHARS {
        return s.to_string();
    }
    let truncated: String = s
        .chars()
        .take(MAX_CONFIG_HINT_CHARS.saturating_sub(1))
        .collect();
    format!("{truncated}…")
}

#[cfg(test)]
#[path = "summary_tests.rs"]
mod tests;
