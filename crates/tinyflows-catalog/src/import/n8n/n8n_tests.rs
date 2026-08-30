use super::*;

#[test]
fn looks_like_n8n_detects_connections_and_typed_nodes() {
    assert!(looks_like_n8n(&json!({ "connections": {} })));
    assert!(looks_like_n8n(&json!({
        "nodes": [{ "name": "x", "type": "n8n-nodes-base.httpRequest" }]
    })));
    // A native tinyflows graph is not mistaken for n8n.
    assert!(!looks_like_n8n(&json!({
        "nodes": [{ "id": "t", "kind": "trigger", "name": "start" }],
        "edges": []
    })));
}

#[test]
fn maps_if_node_to_condition_with_true_false_ports() {
    let wf = json!({
        "name": "branch",
        "nodes": [
            { "id": "s", "name": "Schedule Trigger", "type": "n8n-nodes-base.scheduleTrigger", "position": [0, 0] },
            { "id": "c", "name": "IF", "type": "n8n-nodes-base.if", "position": [200, 0] },
            { "id": "a", "name": "Yes", "type": "n8n-nodes-base.httpRequest", "position": [400, -100] },
            { "id": "b", "name": "No", "type": "n8n-nodes-base.httpRequest", "position": [400, 100] }
        ],
        "connections": {
            "Schedule Trigger": { "main": [[{ "node": "IF", "type": "main", "index": 0 }]] },
            "IF": { "main": [
                [{ "node": "Yes", "type": "main", "index": 0 }],
                [{ "node": "No", "type": "main", "index": 0 }]
            ] }
        }
    });
    let result = map_n8n_workflow(&wf).expect("map");
    let g = &result.graph;
    assert_eq!(g.name, "branch");

    let cond = g.node("c").expect("condition node");
    assert_eq!(cond.kind, NodeKind::Condition);

    let trig = g.node("s").expect("trigger node");
    assert_eq!(trig.kind, NodeKind::Trigger);
    assert_eq!(trig.config["trigger_kind"], json!("schedule"));
    assert_eq!(trig.position, Some(Position { x: 0.0, y: 0.0 }));

    // The IF node's two outputs route to `true`/`false` ports.
    let true_edge = g
        .edges
        .iter()
        .find(|e| e.from_node == "c" && e.to_node == "a")
        .expect("true edge");
    assert_eq!(true_edge.from_port, "true");
    let false_edge = g
        .edges
        .iter()
        .find(|e| e.from_node == "c" && e.to_node == "b")
        .expect("false edge");
    assert_eq!(false_edge.from_port, "false");

    // Whole graph is structurally valid (exactly one trigger, real edges).
    tinyflows::validate::validate(g).expect("valid graph");
}

#[test]
fn unmapped_type_becomes_annotated_placeholder_not_a_failure() {
    let wf = json!({
        "name": "exotic",
        "nodes": [
            { "id": "t", "name": "Manual", "type": "n8n-nodes-base.manualTrigger" },
            { "id": "x", "name": "Airtable", "type": "n8n-nodes-base.airtable",
              "parameters": { "operation": "append", "table": "leads" } }
        ],
        "connections": {
            "Manual": { "main": [[{ "node": "Airtable", "type": "main", "index": 0 }]] }
        }
    });
    let result = map_n8n_workflow(&wf).expect("map");
    let node = result.graph.node("x").expect("placeholder node");
    assert_eq!(node.kind, NodeKind::Transform);
    assert_eq!(
        node.config["_n8n_import"]["original_type"],
        json!("n8n-nodes-base.airtable")
    );
    // Original parameters are preserved for editing.
    assert_eq!(node.config["parameters"]["table"], json!("leads"));
    // The unmapped type produced a warning.
    assert!(result.warnings.iter().any(|w| w.contains("airtable")));
    tinyflows::validate::validate(&result.graph).expect("valid graph");
}

#[test]
fn synthesizes_manual_trigger_when_none_present() {
    let wf = json!({
        "name": "no-trigger",
        "nodes": [
            { "id": "h", "name": "HTTP", "type": "n8n-nodes-base.httpRequest" }
        ],
        "connections": {}
    });
    let result = map_n8n_workflow(&wf).expect("map");
    let trigger = result
        .graph
        .nodes
        .iter()
        .find(|n| n.kind == NodeKind::Trigger)
        .expect("a trigger was synthesized");
    assert!(result.warnings.iter().any(|w| w.contains("manual trigger")));
    tinyflows::validate::validate(&result.graph).expect("valid graph");

    // The synthesized trigger must be wired to the graph's actual entry
    // point — otherwise the flow validates but running it executes only the
    // disconnected trigger and none of the imported workflow.
    assert!(
        result
            .graph
            .edges
            .iter()
            .any(|e| e.from_node == trigger.id && e.to_node == "h"),
        "synthesized trigger must connect to the imported root node, got edges: {:?}",
        result.graph.edges
    );
}

#[test]
fn synthesized_trigger_id_avoids_colliding_with_an_existing_node() {
    // The n8n graph already has a (non-trigger) node literally id'd
    // "trigger" — the synthesized manual trigger must not collide with it.
    let wf = json!({
        "name": "id-collision",
        "nodes": [
            { "id": "trigger", "name": "HTTP", "type": "n8n-nodes-base.httpRequest" }
        ],
        "connections": {}
    });
    let result = map_n8n_workflow(&wf).expect("map");
    let ids: Vec<&str> = result.graph.nodes.iter().map(|n| n.id.as_str()).collect();
    // Both the original node and the synthesized trigger survive, under
    // distinct ids.
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"trigger"));
    assert!(ids.iter().any(|id| *id != "trigger"));
    assert_eq!(
        result
            .graph
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Trigger)
            .count(),
        1
    );
    tinyflows::validate::validate(&result.graph).expect("valid graph");
}

#[test]
fn demotes_extra_triggers_to_placeholders() {
    let wf = json!({
        "name": "two-triggers",
        "nodes": [
            { "id": "s", "name": "Schedule", "type": "n8n-nodes-base.scheduleTrigger" },
            { "id": "w", "name": "Webhook", "type": "n8n-nodes-base.webhook" }
        ],
        "connections": {}
    });
    let result = map_n8n_workflow(&wf).expect("map");
    assert_eq!(
        result
            .graph
            .nodes
            .iter()
            .filter(|n| n.kind == NodeKind::Trigger)
            .count(),
        1
    );
    // The demoted trigger is now a placeholder transform.
    let demoted = result.graph.node("w").expect("webhook node");
    assert_eq!(demoted.kind, NodeKind::Transform);
    tinyflows::validate::validate(&result.graph).expect("valid graph");
}

#[test]
fn translates_trivial_json_expression_to_jq() {
    // R-C1: n8n's `$json` is the CURRENT INPUT ITEM, which the tinyflows
    // scope binds under `item` (scope root is `{item, items, run,
    // nodes}` — `vendor/tinyflows/src/nodes/mod.rs::expr_scope_for`).
    // The translated jq path must therefore dereference `.item` first —
    // `=.email` (the old, buggy output) dereferences a key that doesn't
    // exist at the scope root and is GUARANTEED to resolve `null`.
    let mut warnings = Vec::new();
    assert_eq!(
        translate_expr("={{ $json.email }}", &mut warnings, "n"),
        "=.item.email"
    );
    assert_eq!(
        translate_expr("={{ $json.user.name }}", &mut warnings, "n"),
        "=.item.user.name"
    );
    // A bracket key with a space isn't a bare jq identifier — must come out
    // quoted (`."first name"`), not `.first name` (invalid jq).
    assert_eq!(
        translate_expr("={{ $json[\"first name\"] }}", &mut warnings, "n"),
        "=.item.\"first name\""
    );
    // `{{ $json }}` (no tail) must become `.item`, NOT the bare `.` root —
    // `.` would return the whole `{item, items, run, nodes}` scope object,
    // not the item.
    assert_eq!(translate_expr("={{ $json }}", &mut warnings, "n"), "=.item");
    assert!(warnings.is_empty());
}

/// R-C1 regression proof: actually evaluate the translated expression
/// against a scope shaped exactly like the tinyflows engine's real
/// `expr_scope_for` (`vendor/tinyflows/src/nodes/mod.rs`), and assert it
/// resolves the real field — NOT `null`. This is the check that would
/// have caught the original bug: the old `=.email` translation compiles
/// fine and passes structural validation, it just silently resolves
/// `null` at runtime because `email` isn't a key at the scope root.
#[test]
fn translated_json_expression_resolves_against_the_real_engine_scope() {
    let mut warnings = Vec::new();
    let translated = translate_expr("={{ $json.email }}", &mut warnings, "n");
    assert!(warnings.is_empty());

    let scope = json!({
        "item": { "email": "person@example.com" },
        "items": [{ "email": "person@example.com" }],
        "run": {},
        "nodes": {},
    });
    assert_eq!(
        tinyflows::expr::evaluate(&json!(translated), &scope),
        json!("person@example.com"),
        "translated expression `{translated}` must resolve the real field, not null"
    );

    // The pre-fix translation (`=.email`) is left here as a negative
    // control: it dereferences a key that does not exist at the scope
    // root and is GUARANTEED to resolve null — exactly the bug R-C1
    // describes.
    assert_eq!(
        tinyflows::expr::evaluate(&json!("=.email"), &scope),
        Value::Null
    );

    // Bare `{{ $json }}` → `.item` must resolve the WHOLE item, not the
    // whole scope (which would additionally carry `items`/`run`/`nodes`).
    let translated_whole = translate_expr("={{ $json }}", &mut warnings, "n");
    assert_eq!(
        tinyflows::expr::evaluate(&json!(translated_whole), &scope),
        json!({ "email": "person@example.com" })
    );
}

#[test]
fn jq_field_quotes_non_bare_identifiers() {
    // Plain identifiers stay bare.
    assert_eq!(jq_field("foo"), "foo");
    assert_eq!(jq_field("foo_bar"), "foo_bar");
    // Spaces, punctuation, and digit-leading keys aren't bare jq
    // identifiers — jq requires the dot-plus-quoted-string form for these.
    assert_eq!(jq_field("first name"), "\"first name\"");
    assert_eq!(jq_field("foo-bar"), "\"foo-bar\"");
    assert_eq!(jq_field("123key"), "\"123key\"");
}

#[test]
fn leaves_untranslatable_expression_raw_with_warning() {
    let mut warnings = Vec::new();
    let raw = "={{ $json.a + $json.b }}";
    assert_eq!(translate_expr(raw, &mut warnings, "Math"), raw);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("not automatically translated"));
}

#[test]
fn plain_string_is_not_treated_as_expression() {
    let mut warnings = Vec::new();
    assert_eq!(translate_expr("hello", &mut warnings, "n"), "hello");
    assert!(warnings.is_empty());
}

#[test]
fn http_request_maps_url_and_method() {
    let mut warnings = Vec::new();
    let cfg = map_http_request(
        &json!({ "url": "https://api.example.com", "requestMethod": "POST" }),
        &mut warnings,
        "HTTP",
    );
    assert_eq!(cfg["url"], json!("https://api.example.com"));
    assert_eq!(cfg["method"], json!("POST"));
    // Expression in the url is translated in place.
    let cfg2 = map_http_request(
        &json!({ "url": "={{ $json.endpoint }}" }),
        &mut warnings,
        "HTTP",
    );
    assert_eq!(cfg2["url"], json!("=.item.endpoint"));
    assert_eq!(cfg2["method"], json!("GET"));
}

#[test]
fn code_node_pulls_source_and_language() {
    let mut warnings = Vec::new();
    let cfg = map_code(&json!({ "jsCode": "return items;" }), &mut warnings, "Code");
    assert_eq!(cfg["source"], json!("return items;"));
    assert_eq!(cfg["language"], json!("javascript"));
}

#[test]
fn switch_ports_are_numeric_indices() {
    assert_eq!(output_port_name(Some(&NodeKind::Switch), 0), "0");
    assert_eq!(output_port_name(Some(&NodeKind::Switch), 2), "2");
    assert_eq!(output_port_name(Some(&NodeKind::Condition), 0), "true");
    assert_eq!(output_port_name(Some(&NodeKind::Condition), 1), "false");
    assert_eq!(output_port_name(Some(&NodeKind::Merge), 0), "main");
}

#[test]
fn missing_nodes_array_is_an_error() {
    let err = map_n8n_workflow(&json!({ "name": "x" })).unwrap_err();
    assert!(err.contains("nodes"));
}

#[test]
fn drops_connection_to_unknown_node_with_warning() {
    let wf = json!({
        "name": "dangling",
        "nodes": [
            { "id": "t", "name": "Manual", "type": "n8n-nodes-base.manualTrigger" }
        ],
        "connections": {
            "Manual": { "main": [[{ "node": "Ghost", "type": "main", "index": 0 }]] }
        }
    });
    let result = map_n8n_workflow(&wf).expect("map");
    assert!(result.graph.edges.is_empty());
    assert!(result.warnings.iter().any(|w| w.contains("Ghost")));
}

// ── R-m6: duplicate n8n node names ──────────────────────────────────────

#[test]
fn duplicate_node_name_collision_emits_a_warning() {
    // n8n's `connections` map is keyed by node NAME, not id. Two nodes
    // sharing the name "HTTP" mean `name_to_id` last-wins onto id "b" —
    // any connection naming "HTTP" (including the trigger's own edge)
    // silently rewires onto "b" instead of "a" with no warning, unless
    // this collision is reported (R-m6: every other approximation in
    // this importer warns; this was the one silent mis-wiring).
    let wf = json!({
        "name": "dup-names",
        "nodes": [
            { "id": "t", "name": "Manual", "type": "n8n-nodes-base.manualTrigger" },
            { "id": "a", "name": "HTTP", "type": "n8n-nodes-base.httpRequest",
              "parameters": { "url": "https://a.example.com" } },
            { "id": "b", "name": "HTTP", "type": "n8n-nodes-base.httpRequest",
              "parameters": { "url": "https://b.example.com" } }
        ],
        "connections": {
            "Manual": { "main": [[{ "node": "HTTP", "type": "main", "index": 0 }]] }
        }
    });
    let result = map_n8n_workflow(&wf).expect("map");

    // The collision itself is reported.
    let collision_warning = result
        .warnings
        .iter()
        .find(|w| w.contains("named 'HTTP'"))
        .unwrap_or_else(|| {
            panic!(
                "expected a duplicate-name warning, got {:?}",
                result.warnings
            )
        });
    assert!(collision_warning.contains('a'));
    assert!(collision_warning.contains('b'));

    // Both original nodes still exist under their own ids — nothing was
    // dropped, only the *name*-keyed connection lookup collided.
    assert!(result.graph.node("a").is_some());
    assert!(result.graph.node("b").is_some());

    // The connection resolves deterministically onto exactly one target
    // (last-wins onto "b") rather than silently vanishing or duplicating
    // — the fix is the warning, not a change to which id wins.
    assert_eq!(result.graph.edges.len(), 1);
    assert_eq!(result.graph.edges[0].to_node, "b");

    tinyflows::validate::validate(&result.graph).expect("valid graph");
}

// ── R-C1 end-to-end: n8n `$json` import passes binding-resolvability ───

// ── Node-mapping fidelity: warn instead of silently mis-executing ──────────

#[test]
fn if_node_with_untranslatable_conditions_warns_and_preserves_them() {
    let mut warnings = Vec::new();
    let cfg = map_condition(
        &json!({
            "conditions": {
                "options": {},
                "conditions": [{ "leftValue": "={{ $json.status }}", "rightValue": "ok", "operator": { "operation": "equals" } }],
            }
        }),
        &mut warnings,
        "IF",
    );
    // No `field` could be derived, so the node would otherwise silently
    // route every input the same way; the original conditions are kept for
    // the author to rebuild from, and a warning is raised.
    assert!(cfg.get("field").is_none());
    assert!(cfg["_n8n_import"]["conditions"].is_object());
    assert!(warnings.iter().any(|w| w.contains("IF") && w.contains("conditions")));
}

#[test]
fn switch_node_with_untranslatable_rules_warns_and_preserves_them() {
    let mut warnings = Vec::new();
    let cfg = map_switch(
        &json!({ "rules": { "values": [{ "conditions": {}, "outputKey": "a" }] } }),
        &mut warnings,
        "Switch",
    );
    assert!(cfg.get("field").is_none());
    assert!(cfg.get("expression").is_none());
    assert!(cfg["_n8n_import"]["rules"].is_object());
    assert!(warnings.iter().any(|w| w.contains("Switch") && w.contains("rules")));
}

#[test]
fn split_out_maps_field_to_split_out_to_path() {
    let mut warnings = Vec::new();
    let cfg = map_split_out(&json!({ "fieldToSplitOut": "data.items" }), &mut warnings, "Split");
    assert_eq!(cfg["path"], json!("data.items"));
    assert!(cfg.get("fieldToSplitOut").is_none());
}

#[test]
fn item_lists_only_maps_to_split_out_for_the_split_out_operation() {
    let wf = json!({
        "name": "item-lists",
        "nodes": [
            { "id": "t", "name": "Manual", "type": "n8n-nodes-base.manualTrigger" },
            { "id": "s", "name": "Split", "type": "n8n-nodes-base.itemLists",
              "parameters": { "operation": "splitOutItems", "fieldToSplitOut": "items" } },
            { "id": "a", "name": "Aggregate", "type": "n8n-nodes-base.itemLists",
              "parameters": { "operation": "aggregateItems" } }
        ],
        "connections": {
            "Manual": { "main": [[{ "node": "Split", "type": "main", "index": 0 }]] },
            "Split": { "main": [[{ "node": "Aggregate", "type": "main", "index": 0 }]] }
        }
    });
    let result = map_n8n_workflow(&wf).expect("map");
    assert_eq!(
        result.graph.node("s").expect("split node").kind,
        NodeKind::SplitOut
    );
    // A non-split-out operation is not force-mapped to `split_out`; it falls
    // through to the unmapped-type placeholder like any other unrecognized
    // config, rather than silently claiming to aggregate.
    assert_eq!(
        result.graph.node("a").expect("aggregate node").kind,
        NodeKind::Transform
    );
}

#[test]
fn code_node_with_n8n_globals_or_top_level_return_warns() {
    let mut warnings = Vec::new();
    map_code(&json!({ "jsCode": "return items;" }), &mut warnings, "Code");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("Code") && w.contains("n8n-only globals"))
    );

    let mut clean_warnings = Vec::new();
    map_code(
        &json!({ "jsCode": "const x = 1; module.exports = x;" }),
        &mut clean_warnings,
        "Clean",
    );
    assert!(
        !clean_warnings
            .iter()
            .any(|w| w.contains("n8n-only globals")),
        "code with no n8n tell-tales should not warn: {clean_warnings:?}"
    );
}

#[test]
fn cron_node_maps_cron_expression_to_schedule() {
    let mut warnings = Vec::new();
    let cfg = trigger_config(
        "schedule",
        &json!({ "cronExpression": "0 9 * * *" }),
        &mut warnings,
        "Cron",
    );
    assert_eq!(cfg["schedule"], json!({ "kind": "cron", "expr": "0 9 * * *" }));
    assert!(!warnings.iter().any(|w| w.contains("could not be translated")));
}

#[test]
fn interval_node_maps_unit_and_value_to_every_ms() {
    let mut warnings = Vec::new();
    let cfg = trigger_config(
        "schedule",
        &json!({ "unit": "minutes", "value": 15 }),
        &mut warnings,
        "Interval",
    );
    assert_eq!(cfg["schedule"], json!({ "kind": "every", "every_ms": 900000.0 }));
    assert!(!warnings.iter().any(|w| w.contains("could not be translated")));
}

#[test]
fn schedule_trigger_maps_a_cron_expression_rule() {
    let mut warnings = Vec::new();
    let cfg = trigger_config(
        "schedule",
        &json!({
            "rule": { "interval": [{ "field": "cronExpression", "expression": "*/5 * * * *" }] }
        }),
        &mut warnings,
        "ScheduleTrigger",
    );
    assert_eq!(
        cfg["schedule"],
        json!({ "kind": "cron", "expr": "*/5 * * * *" })
    );
    assert!(!warnings.iter().any(|w| w.contains("could not be translated")));
}

#[test]
fn schedule_trigger_maps_a_fixed_unit_interval_rule() {
    let mut warnings = Vec::new();
    let cfg = trigger_config(
        "schedule",
        &json!({
            "rule": { "interval": [{ "field": "hours", "hoursInterval": 2 }] }
        }),
        &mut warnings,
        "ScheduleTrigger",
    );
    assert_eq!(
        cfg["schedule"],
        json!({ "kind": "every", "every_ms": 7200000.0 })
    );
}

#[test]
fn unrecognized_schedule_shape_warns_instead_of_guessing() {
    let mut warnings = Vec::new();
    let cfg = trigger_config(
        "schedule",
        &json!({ "rule": { "interval": [{ "field": "weekday", "weekday": 1 }] } }),
        &mut warnings,
        "Weekly",
    );
    assert!(cfg.get("schedule").is_none());
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("Weekly") && w.contains("could not be translated"))
    );
}
