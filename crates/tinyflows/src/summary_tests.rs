use super::*;
use serde_json::json;

fn node(id: &str, kind: NodeKind, name: &str, config: Value) -> Node {
    Node {
        id: id.to_string(),
        kind,
        type_version: 1,
        name: name.to_string(),
        config,
        ports: Vec::new(),
        position: None,
    }
}

#[test]
fn summary_wire_shape_is_pinned() {
    let graph = WorkflowGraph {
        nodes: vec![
            node(
                "t",
                NodeKind::Trigger,
                "T",
                json!({ "trigger_kind": "schedule", "schedule": { "expr": "0 9 * * *" } }),
            ),
            node(
                "a",
                NodeKind::Agent,
                "Summarize",
                json!({ "prompt": "Summarize it" }),
            ),
            node("m", NodeKind::Merge, "Join", json!({})),
        ],
        ..Default::default()
    };
    assert_eq!(
        summarize(&graph),
        json!({
            "trigger": "schedule: 0 9 * * *",
            "steps": [
                { "kind": "agent", "name": "Summarize", "config_hint": "Summarize it" },
                { "kind": "merge", "name": "Join" },
            ],
        })
    );
}

#[test]
fn trigger_descriptions_cover_every_kind() {
    let t = |cfg: Value| describe_trigger(&node("t", NodeKind::Trigger, "T", cfg));
    assert_eq!(t(json!({})), "manual");
    assert_eq!(
        t(json!({ "trigger_kind": "schedule", "schedule": { "every_ms": 5000 } })),
        "schedule: every 5000ms"
    );
    assert_eq!(
        t(json!({ "trigger_kind": "schedule", "schedule": { "at": "2026-01-01T00:00:00Z" } })),
        "schedule: once at 2026-01-01T00:00:00Z"
    );
    assert_eq!(
        t(json!({ "trigger_kind": "schedule" })),
        "schedule (unspecified)"
    );
    assert_eq!(
        t(json!({ "trigger_kind": "app_event", "toolkit": "gmail", "trigger_slug": "NEW" })),
        "app event: gmail/NEW"
    );
    assert_eq!(t(json!({ "trigger_kind": "app_event" })), "app event: ?/?");
    assert_eq!(t(json!({ "trigger_kind": "webhook" })), "webhook");
    assert_eq!(
        summarize(&WorkflowGraph::default())["trigger"],
        "no trigger"
    );
}

#[test]
fn dedup_config_hint_is_truncated_for_a_long_key_expression() {
    // CodeRabbit (PR #5265): unlike the other config_hint branches, the
    // dedup branch returned `format!("key: {k}")` unwrapped by
    // `truncate_hint`, so an oversized `config.key` expression could make
    // the proposal/summary payload unbounded.
    let long_key = format!("=item.{}", "x".repeat(200));
    let graph = WorkflowGraph {
        nodes: vec![Node {
            id: "dd".to_string(),
            kind: NodeKind::Dedup,
            type_version: 1,
            name: "Dedup".to_string(),
            config: json!({ "key": long_key }),
            ports: Vec::new(),
            position: None,
        }],
        ..Default::default()
    };

    let summary = summarize(&graph);
    let hint = summary["steps"][0]["config_hint"].as_str().unwrap();
    assert!(
        hint.chars().count() <= MAX_CONFIG_HINT_CHARS,
        "hint not truncated: {} chars: {hint}",
        hint.chars().count()
    );
    assert!(hint.ends_with('…'), "expected an ellipsis marker: {hint}");
    assert!(
        hint.starts_with("key: "),
        "expected the key: prefix: {hint}"
    );
}

#[test]
fn approval_config_hint_prefers_the_review_title() {
    let graph = WorkflowGraph {
        nodes: vec![Node {
            id: "review".to_string(),
            kind: NodeKind::Approval,
            type_version: 1,
            name: "Review".to_string(),
            config: json!({
                "title": "Publish this draft?",
                "prompt": "Approve publication"
            }),
            ports: Vec::new(),
            position: None,
        }],
        ..Default::default()
    };

    let summary = summarize(&graph);
    assert_eq!(summary["steps"][0]["config_hint"], "Publish this draft?");
}

#[test]
fn shell_config_hint_prefers_the_script_path_and_truncates_inline_source() {
    let graph = WorkflowGraph {
        nodes: vec![
            Node {
                id: "path".to_string(),
                kind: NodeKind::Shell,
                type_version: 1,
                name: "Script file".to_string(),
                config: json!({
                    "script_path": "scripts/report.sh",
                    "source": "ignored when a path is present"
                }),
                ports: Vec::new(),
                position: None,
            },
            Node {
                id: "inline".to_string(),
                kind: NodeKind::Shell,
                type_version: 1,
                name: "Inline script".to_string(),
                config: json!({ "source": "x".repeat(200) }),
                ports: Vec::new(),
                position: None,
            },
        ],
        ..Default::default()
    };

    let summary = summarize(&graph);
    assert_eq!(
        summary["steps"][0]["config_hint"],
        "script: scripts/report.sh"
    );
    let inline_hint = summary["steps"][1]["config_hint"].as_str().unwrap();
    assert_eq!(inline_hint.chars().count(), MAX_CONFIG_HINT_CHARS);
    assert!(inline_hint.ends_with('…'));
}
