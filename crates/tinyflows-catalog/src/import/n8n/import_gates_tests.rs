//! An imported graph must clear the engine's own save gates, not just satisfy
//! the mapping assertions: an import that produced a graph a builder would
//! refuse is a broken import.

use super::*;

/// Imports a small n8n fixture that uses `$json` bindings end-to-end: map,
/// re-serialize, deserialize + validate + engine-compat, then the
/// binding-resolvability gate (`tinyflows::gates::failures`) that a builder
/// enforces before accepting ANY graph. The imported graph must clear it with
/// zero errors, exactly as a hand-authored one does.
#[test]
fn imported_json_bindings_pass_binding_resolvability_and_resolve_the_real_item() {
    let wf = json!({
        "name": "json-binding-import",
        "nodes": [
            { "id": "t", "name": "Webhook", "type": "n8n-nodes-base.webhook" },
            { "id": "h", "name": "Notify", "type": "n8n-nodes-base.httpRequest",
              "parameters": {
                  "url": "={{ $json.callback_url }}",
                  "requestMethod": "POST"
              } }
        ],
        "connections": {
            "Webhook": { "main": [[{ "node": "Notify", "type": "main", "index": 0 }]] }
        }
    });

    let mapped = map_n8n_workflow(&wf).expect("map");
    // No warning for the `$json.callback_url` binding — it's the trivial
    // translatable case.
    assert!(
        !mapped
            .warnings
            .iter()
            .any(|w| w.contains("callback_url") || w.contains("not automatically translated")),
        "{:?}",
        mapped.warnings
    );

    let http_node = mapped.graph.node("h").expect("http node");
    assert_eq!(http_node.config["url"], json!("=.item.callback_url"));

    // Re-enter the same migrate + validate path `flows_import` uses.
    let value = serde_json::to_value(&mapped.graph).expect("serialize graph");
    let graph = tinyflows::migrate::deserialize_graph(value).expect("imported graph deserializes");
    tinyflows::validate::validate(&graph).expect("imported graph is structurally valid");
    tinyflows::compat::ensure_compatible(&graph).expect("imported graph is engine-compatible");

    // The domain's hard binding-resolvability gate accepts the imported
    // graph — the same gate a hand-authored graph must clear before
    // `propose_workflow`/`save_workflow` will accept it.
    assert!(
        tinyflows::gates::failures(&graph).is_empty(),
        "{:?}",
        tinyflows::gates::failures(&graph)
    );

    // The direct proof this whole importer exists to guarantee: the
    // translated binding actually resolves the real upstream item field
    // at runtime, evaluated against a scope shaped like the tinyflows
    // engine's real `expr_scope_for` — NOT null, which is exactly what
    // the pre-fix `=.callback_url` translation would have produced.
    let http_node = graph.node("h").expect("http node");
    let scope = json!({
        "item": { "callback_url": "https://example.com/hook" },
        "items": [{ "callback_url": "https://example.com/hook" }],
        "run": {},
        "nodes": {},
    });
    let resolved = tinyflows::expr::evaluate(&http_node.config["url"], &scope);
    assert_eq!(resolved, json!("https://example.com/hook"));
}
