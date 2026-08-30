//! n8n node-type -> tinyflows `NodeKind` + config mapping.

use serde_json::{Value, json};
use tinyflows::model::NodeKind;

use super::expr::translate_config;

pub(super) fn map_node(
    n8n_type: &str,
    params: &Value,
    n8n_name: &str,
    warnings: &mut Vec<String>,
) -> (NodeKind, Value) {
    // Strip the vendor prefix so both `n8n-nodes-base.if` and a bare `if` match.
    let short = n8n_type
        .rsplit_once('.')
        .map(|(_, s)| s)
        .unwrap_or(n8n_type);

    match short {
        "if" => (
            NodeKind::Condition,
            translate_config(params, warnings, n8n_name),
        ),
        "switch" => (
            NodeKind::Switch,
            translate_config(params, warnings, n8n_name),
        ),
        "merge" => (
            NodeKind::Merge,
            translate_config(params, warnings, n8n_name),
        ),
        "splitOut" | "itemLists" => (
            NodeKind::SplitOut,
            translate_config(params, warnings, n8n_name),
        ),
        "httpRequest" => (
            NodeKind::HttpRequest,
            map_http_request(params, warnings, n8n_name),
        ),
        "code" | "function" | "functionItem" => {
            (NodeKind::Code, map_code(params, warnings, n8n_name))
        }
        "scheduleTrigger" | "cron" | "interval" => (
            NodeKind::Trigger,
            trigger_config("schedule", params, warnings, n8n_name),
        ),
        "webhook" => (
            NodeKind::Trigger,
            trigger_config("webhook", params, warnings, n8n_name),
        ),
        "manualTrigger" | "start" => (
            NodeKind::Trigger,
            trigger_config("manual", params, warnings, n8n_name),
        ),
        _ => {
            warnings.push(format!(
                "Node '{n8n_name}' has n8n type '{n8n_type}', which has no tinyflows equivalent — \
                 imported as an editable placeholder that carries its original configuration. \
                 Replace it with a supported node before enabling the flow."
            ));
            let config = json!({
                "_n8n_import": {
                    "original_type": n8n_type,
                    "note": "Unmapped n8n node imported as a placeholder; original parameters preserved below.",
                },
                "parameters": params,
            });
            (NodeKind::Transform, config)
        }
    }
}

/// Builds a tinyflows `trigger` config carrying the given `trigger_kind`
/// discriminator plus any (expression-translated) source parameters.
pub(super) fn trigger_config(
    trigger_kind: &str,
    params: &Value,
    warnings: &mut Vec<String>,
    n8n_name: &str,
) -> Value {
    let mut cfg = match translate_config(params, warnings, n8n_name) {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    cfg.insert(
        "trigger_kind".to_string(),
        Value::String(trigger_kind.to_string()),
    );
    Value::Object(cfg)
}

/// Maps n8n `httpRequest` parameters onto tinyflows' `{ method, url, ... }`
/// http_request config. n8n uses `url` + `method`/`requestMethod`; anything
/// else is carried through after expression translation.
pub(super) fn map_http_request(params: &Value, warnings: &mut Vec<String>, n8n_name: &str) -> Value {
    let translated = translate_config(params, warnings, n8n_name);
    let mut cfg = match translated {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    // Normalize the method key (`requestMethod` is the older n8n spelling).
    if !cfg.contains_key("method") {
        if let Some(method) = cfg.remove("requestMethod") {
            cfg.insert("method".to_string(), method);
        }
    }
    cfg.entry("method".to_string())
        .or_insert_with(|| Value::String("GET".to_string()));
    Value::Object(cfg)
}

/// Maps n8n `code`/`function` parameters onto tinyflows' code config, pulling
/// the source out of n8n's `jsCode`/`functionCode`/`pythonCode` fields into the
/// `source` key tinyflows' `code` node actually reads (`vendor/tinyflows/src/nodes/integration/code.rs`)
/// while preserving the language hint.
pub(super) fn map_code(params: &Value, warnings: &mut Vec<String>, n8n_name: &str) -> Value {
    let translated = translate_config(params, warnings, n8n_name);
    let mut cfg = match translated {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    for (src, lang) in [
        ("jsCode", "javascript"),
        ("functionCode", "javascript"),
        ("pythonCode", "python"),
    ] {
        if let Some(code) = cfg.remove(src) {
            cfg.entry("source".to_string()).or_insert(code);
            cfg.entry("language".to_string())
                .or_insert_with(|| Value::String(lang.to_string()));
        }
    }
    Value::Object(cfg)
}

/// Recursively translates n8n `={{ … }}` expressions inside a config `Value`
/// into tinyflows' `=`-prefixed jq form where trivially possible; anything not
/// trivially translatable is left as its raw string and a warning is recorded.
