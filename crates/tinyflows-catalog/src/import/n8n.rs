//! Best-effort importer that maps an **n8n** workflow export into a tinyflows
//! [`WorkflowGraph`]. Backs the `format: "n8n"` branch of `flows_import`
//! (`schemas::handle_import` → `ops::flows_import`).
//!
//! n8n and tinyflows share a large slice of automation vocabulary (branching,
//! merging, HTTP, code, triggers), so this maps the overlap directly:
//!
//! | n8n node type (`n8n-nodes-base.*`)      | tinyflows kind          |
//! | --------------------------------------- | ----------------------- |
//! | `if`                                    | `condition`             |
//! | `switch`                                | `switch`                |
//! | `merge`                                 | `merge`                 |
//! | `splitOut` / `itemLists`(splitOut mode) | `split_out`             |
//! | `httpRequest`                           | `http_request`          |
//! | `code` / `function` / `functionItem`    | `code`                  |
//! | `scheduleTrigger` / `cron` / `interval` | `trigger` (schedule)    |
//! | `webhook`                               | `trigger` (webhook)     |
//! | `manualTrigger`                         | `trigger` (manual)      |
//!
//! **Everything else is not a failed import** — an unmapped node type lands as
//! an annotated placeholder (`transform`) node carrying the original n8n type
//! and parameters in its `config`, plus a `_n8n_import` note, so the graph
//! still loads, validates, and can be edited on the canvas. Connections and
//! canvas positions are preserved wherever the source provides them.
//!
//! The mapping is intentionally lossy and advisory: every approximation
//! (unmapped type, untranslated expression, synthesized/demoted trigger) is
//! reported as a warning string the UI surfaces next to the imported draft.

use serde_json::{Map, Value, json};
use tinyflows::model::{Edge, Node, NodeKind, Position, WorkflowGraph};

/// The outcome of mapping an n8n workflow: the best-effort tinyflows graph plus
/// the list of advisory warnings collected during the mapping.
#[derive(Debug)]
pub struct N8nImportResult {
    /// The mapped graph (still passed through `migrate` + `validate` by the
    /// caller before it is handed to the UI).
    pub graph: WorkflowGraph,
    /// Human-readable, non-fatal notes: unmapped node types, untranslated
    /// expressions, and any synthesized/demoted trigger.
    pub warnings: Vec<String>,
}

/// Returns `true` when `value` looks like an n8n workflow export rather than a
/// native tinyflows `WorkflowGraph` — used by `flows_import`'s auto-detect. The
/// tell-tales are a top-level `connections` object and/or nodes carrying an
/// `n8n-nodes-base.*`/`type`-style discriminator (tinyflows nodes use `kind`).
pub fn looks_like_n8n(value: &Value) -> bool {
    if value.get("connections").map(Value::is_object) == Some(true) {
        return true;
    }
    let Some(nodes) = value.get("nodes").and_then(Value::as_array) else {
        return false;
    };
    nodes.iter().any(|n| {
        // A native tinyflows node has `kind`; an n8n node has `type` and no `kind`.
        n.get("kind").is_none() && n.get("type").and_then(Value::as_str).is_some()
    })
}

/// Maps a parsed n8n workflow JSON `value` into a tinyflows [`WorkflowGraph`].
///
/// Never returns `Err` for an unrecognized node type — those become annotated
/// placeholders. `Err` is reserved for input that is not shaped like an n8n
/// export at all (e.g. `nodes` is not an array).
pub fn map_n8n_workflow(value: &Value) -> Result<N8nImportResult, String> {
    let mut warnings: Vec<String> = Vec::new();

    let name = value
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Imported workflow")
        .to_string();

    let raw_nodes = value
        .get("nodes")
        .and_then(Value::as_array)
        .ok_or_else(|| "n8n workflow is missing a `nodes` array".to_string())?;

    tracing::debug!(
        target: "flows",
        %name,
        node_count = raw_nodes.len(),
        "[flows] n8n_import: mapping n8n workflow"
    );

    // n8n connections key nodes by *name*; tinyflows edges reference node *ids*.
    // Build a name → id lookup so connections can be rewired onto ids.
    let mut name_to_id: Map<String, Value> = Map::new();
    let mut nodes: Vec<Node> = Vec::new();

    for raw in raw_nodes {
        let n8n_name = raw
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("node")
            .to_string();
        let id = raw
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| slug(&n8n_name));
        // n8n's `connections` map is keyed by node NAME (not id), and
        // `map_connections` below rewires every edge that names this node
        // through this lookup. A duplicate name is last-wins here, which
        // silently mis-wires every connection naming it onto whichever node
        // happened to be inserted last — R-m6. Every other approximation in
        // this importer reports a warning; a duplicate name is the one
        // silent mis-wiring, so warn on the collision the same way.
        if let Some(previous_id) = name_to_id
            .insert(n8n_name.clone(), Value::String(id.clone()))
            .and_then(|v| v.as_str().map(str::to_string))
        {
            tracing::warn!(
                target: "flows",
                name = %n8n_name,
                previous_id = %previous_id,
                new_id = %id,
                "[flows] n8n_import: duplicate node name collision — connections will mis-wire"
            );
            warnings.push(format!(
                "Multiple n8n nodes are named '{n8n_name}' (ids '{previous_id}' and '{id}') — \
                 n8n connections are keyed by node name, so every connection naming \
                 '{n8n_name}' was rewired onto node '{id}' and may now point at the wrong \
                 node. Rename the duplicates in the source n8n workflow and re-import, or fix \
                 the affected edges by hand."
            ));
        }

        let n8n_type = raw.get("type").and_then(Value::as_str).unwrap_or("");
        let params = raw.get("parameters").cloned().unwrap_or(Value::Null);
        let position = parse_position(raw.get("position"));

        let (kind, config) = map_node(n8n_type, &params, &n8n_name, &mut warnings);
        nodes.push(Node {
            id,
            kind,
            type_version: 1,
            name: n8n_name,
            config,
            ports: Vec::new(),
            position,
        });
    }

    // tinyflows requires exactly one trigger. Reconcile the mapped triggers:
    // synthesize a manual one when none survived, or demote extras to
    // placeholders when several did — either way `validate` will pass.
    let synthesized_trigger_id = reconcile_triggers(&mut nodes, &mut warnings);

    let mut edges = map_connections(value.get("connections"), &name_to_id, &nodes, &mut warnings);

    // A synthesized trigger starts with no outgoing edges — nothing in the
    // source export names it, since it never existed there. Without wiring it
    // to the graph's actual entry points, the flow "validates" but running it
    // executes only the disconnected trigger and none of the imported
    // workflow. Wire it to every node that has no incoming edge of its own
    // (the graph's roots), excluding the trigger itself.
    if let Some(trigger_id) = synthesized_trigger_id {
        let has_incoming: std::collections::HashSet<&str> =
            edges.iter().map(|e| e.to_node.as_str()).collect();
        for node in &nodes {
            if node.id != trigger_id && !has_incoming.contains(node.id.as_str()) {
                edges.push(Edge {
                    from_node: trigger_id.clone(),
                    from_port: "main".to_string(),
                    to_node: node.id.clone(),
                    to_port: "main".to_string(),
                });
            }
        }
    }

    let graph = WorkflowGraph {
        schema_version: tinyflows::model::CURRENT_SCHEMA_VERSION,
        id: None,
        name,
        // n8n has no equivalent of a declared workflow input — its workflows are
        // parameterized through trigger/node config — so an import declares
        // none. The author adds them afterwards if the flow needs them.
        inputs: Vec::new(),
        // n8n agent nodes carry their configuration inline; they do not define
        // reusable TinyFlows agent-registry entries, so every `agent_ref`
        // resolves against this host's own registry instead.
        agents: Vec::new(),
        nodes,
        edges,
    };

    tracing::debug!(
        target: "flows",
        node_count = graph.nodes.len(),
        edge_count = graph.edges.len(),
        warning_count = warnings.len(),
        "[flows] n8n_import: mapping complete"
    );

    Ok(N8nImportResult { graph, warnings })
}

/// Maps a single n8n node `type` + `parameters` to a tinyflows kind and config.
/// Unrecognized types return a `transform` placeholder carrying the original
/// type/params under `_n8n_import` and record a warning.
fn map_node(
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
fn trigger_config(
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
fn map_http_request(params: &Value, warnings: &mut Vec<String>, n8n_name: &str) -> Value {
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
fn map_code(params: &Value, warnings: &mut Vec<String>, n8n_name: &str) -> Value {
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
fn translate_config(value: &Value, warnings: &mut Vec<String>, n8n_name: &str) -> Value {
    match value {
        Value::String(s) => Value::String(translate_expr(s, warnings, n8n_name)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|v| translate_config(v, warnings, n8n_name))
                .collect(),
        ),
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), translate_config(v, warnings, n8n_name));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Translates a single n8n expression string. An n8n expression is a string
/// beginning with `=` whose body is `{{ … }}`. The trivially-translatable case
/// is a single `{{ $json.<path> }}` reference, which becomes tinyflows'
/// `=.item.<path>` jq. Anything richer is returned unchanged with a warning.
///
/// n8n's `$json` is the current input item's json — the tinyflows expression
/// scope root is `{item, items, run, nodes}` (`vendor/tinyflows/src/nodes/mod.rs::expr_scope_for`),
/// which binds exactly that under `item`. So `$json` must translate to
/// `.item`, never the bare `.` root: `.` would resolve the WHOLE scope object
/// (`item` + `items` + `run` + `nodes`), not the item, and `.<path>` without
/// the `item` segment dereferences a key that does not exist at the scope
/// root and is GUARANTEED to resolve `null` at runtime (R-C1).
fn translate_expr(raw: &str, warnings: &mut Vec<String>, n8n_name: &str) -> String {
    // Only n8n expression strings start with `=`; plain values pass through.
    if !raw.starts_with('=') {
        return raw.to_string();
    }
    let body = raw[1..].trim();
    let Some(inner) = body
        .strip_prefix("{{")
        .and_then(|b| b.strip_suffix("}}"))
        .map(str::trim)
    else {
        // A `=`-prefixed non-`{{ }}` string: keep raw, warn.
        warnings.push(untranslated_warning(n8n_name, raw));
        return raw.to_string();
    };

    // Trivial single-reference case: `$json.foo.bar` (or `$json["foo"]`).
    if let Some(path) = inner.strip_prefix("$json") {
        if let Some(jq_path) = json_path_to_jq(path) {
            // `json_path_to_jq` returns the tail path RELATIVE to `$json`
            // (`.foo`, `.foo.bar`, or `""` when there's no tail at all — see
            // its doc). Prefixing `.item` here binds it at the tinyflows
            // scope's `item` key instead of the scope root (see this fn's
            // doc, R-C1): the empty-tail case naturally becomes `.item` (not
            // `.item.`) because the tail is `""`, with no special-casing
            // needed here.
            tracing::debug!(
                target: "flows",
                node = %n8n_name,
                %raw,
                translated = %format!("=.item{jq_path}"),
                "[flows] n8n_import: translated $json expression"
            );
            return format!("=.item{jq_path}");
        }
    }

    warnings.push(untranslated_warning(n8n_name, raw));
    raw.to_string()
}

/// Turns an n8n `$json` accessor tail (`.foo.bar`, `["foo"]`, or empty) into a
/// jq path relative to `$json` itself (`.foo.bar`, `.foo`, or `""` for an
/// empty tail — deliberately NOT `.`, so the caller can prepend `.item`
/// without special-casing the empty case, see [`translate_expr`]). Returns
/// `None` for anything that isn't a plain dotted / bracketed-string path
/// (arithmetic, function calls, bracket-index into arrays, etc.), so the
/// caller falls back to raw + warn.
fn json_path_to_jq(tail: &str) -> Option<String> {
    let tail = tail.trim();
    if tail.is_empty() {
        return Some(String::new());
    }
    let mut jq = String::new();
    let mut rest = tail;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix('.') {
            // `.identifier`
            let end = after
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(after.len());
            if end == 0 {
                return None;
            }
            jq.push('.');
            jq.push_str(&after[..end]);
            rest = &after[end..];
        } else {
            // `["identifier"]` or `['identifier']` — string keys only.
            let after = rest.strip_prefix('[')?;
            let close = after.find(']')?;
            let key = after[..close].trim();
            let key = key
                .strip_prefix('"')
                .and_then(|k| k.strip_suffix('"'))
                .or_else(|| key.strip_prefix('\'').and_then(|k| k.strip_suffix('\'')))?;
            if key.is_empty() {
                return None;
            }
            jq.push('.');
            jq.push_str(&jq_field(key));
            rest = &after[close + 1..];
        }
    }
    Some(jq)
}

/// Renders a single jq field-access key: bare (`foo`) when `key` is a plain
/// identifier (alphanumeric/underscore, not digit-leading), else quoted
/// (`"first name"`) per jq's dot-plus-quoted-string syntax — required for any
/// key containing spaces or punctuation, which `.foo bar` (unquoted) is not
/// valid jq for.
fn jq_field(key: &str) -> String {
    let is_bare_identifier = !key.is_empty()
        && !key.starts_with(|c: char| c.is_ascii_digit())
        && key.chars().all(|c| c.is_alphanumeric() || c == '_');
    if is_bare_identifier {
        key.to_string()
    } else {
        format!("{:?}", key)
    }
}

fn untranslated_warning(n8n_name: &str, raw: &str) -> String {
    format!(
        "Node '{n8n_name}' uses an n8n expression that was not automatically translated \
         (`{raw}`) — it was kept as a raw string. Review and rewrite it as a tinyflows \
         `=`-jq expression."
    )
}

/// Ensures the graph has exactly one trigger, mutating `nodes` in place:
/// - zero triggers → prepend a synthesized `manual` trigger (with a warning);
/// - multiple triggers → keep the first, demote the rest to placeholders.
///
/// Returns the id of the synthesized trigger, if one was added, so the caller
/// can wire it to the graph's root nodes once edges are computed.
fn reconcile_triggers(nodes: &mut Vec<Node>, warnings: &mut Vec<String>) -> Option<String> {
    let trigger_idxs: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(_, n)| n.kind == NodeKind::Trigger)
        .map(|(i, _)| i)
        .collect();

    match trigger_idxs.len() {
        0 => {
            warnings.push(
                "The n8n workflow had no importable trigger — a manual trigger was added so the \
                 flow is runnable. Attach a schedule or app-event trigger to run it automatically."
                    .to_string(),
            );
            // Collision-free synthetic id: an n8n graph may already have a
            // (non-trigger) node literally named "trigger" — colliding with a
            // hardcoded id would produce a duplicate-id graph and fail
            // validation, turning an otherwise-recoverable import into a hard
            // failure.
            let mut trigger_id = "trigger".to_string();
            let mut suffix = 2;
            while nodes.iter().any(|n| n.id == trigger_id) {
                trigger_id = format!("trigger_{suffix}");
                suffix += 1;
            }

            nodes.insert(
                0,
                Node {
                    id: trigger_id.clone(),
                    kind: NodeKind::Trigger,
                    type_version: 1,
                    name: "Manual Trigger".to_string(),
                    config: json!({ "trigger_kind": "manual" }),
                    ports: Vec::new(),
                    position: None,
                },
            );
            return Some(trigger_id);
        }
        1 => {}
        _ => {
            // Keep the first trigger; demote the rest so `validate` accepts the
            // graph. Their ids are unchanged, so edges stay wired.
            for &idx in trigger_idxs.iter().skip(1) {
                let node = &mut nodes[idx];
                warnings.push(format!(
                    "The n8n workflow had more than one trigger; '{}' was imported as a \
                     placeholder because a tinyflows flow allows only one trigger.",
                    node.name
                ));
                let original = node.config.clone();
                node.kind = NodeKind::Transform;
                node.config = json!({
                    "_n8n_import": {
                        "original_type": "trigger",
                        "note": "Extra trigger demoted to a placeholder (a flow allows one trigger).",
                    },
                    "parameters": original,
                });
            }
        }
    }
}

/// Rewrites n8n's name-keyed `connections` map onto tinyflows edges (id-keyed),
/// preserving output-port routing: an `if`/`condition` source routes output 0 →
/// `true` and 1 → `false`; a `switch` source routes output _i_ → `"i"`; every
/// other source uses `main`. Connections that reference an unknown node are
/// dropped with a warning.
fn map_connections(
    connections: Option<&Value>,
    name_to_id: &Map<String, Value>,
    nodes: &[Node],
    warnings: &mut Vec<String>,
) -> Vec<Edge> {
    let mut edges = Vec::new();
    let Some(Value::Object(conns)) = connections else {
        return edges;
    };

    for (src_name, outputs) in conns {
        let Some(src_id) = name_to_id.get(src_name).and_then(Value::as_str) else {
            continue;
        };
        let src_kind = nodes
            .iter()
            .find(|n| n.id == src_id)
            .map(|n| n.kind.clone());
        // n8n groups outputs by connection type (`main`, `ai_tool`, …); we only
        // wire `main` — other connection families have no tinyflows analogue.
        let Some(main) = outputs.get("main").and_then(Value::as_array) else {
            continue;
        };
        for (port_index, port_targets) in main.iter().enumerate() {
            let from_port = output_port_name(src_kind.as_ref(), port_index);
            let Some(targets) = port_targets.as_array() else {
                continue;
            };
            for target in targets {
                let Some(tgt_name) = target.get("node").and_then(Value::as_str) else {
                    continue;
                };
                match name_to_id.get(tgt_name).and_then(Value::as_str) {
                    Some(tgt_id) => edges.push(Edge {
                        from_node: src_id.to_string(),
                        from_port: from_port.clone(),
                        to_node: tgt_id.to_string(),
                        to_port: "main".to_string(),
                    }),
                    None => warnings.push(format!(
                        "Connection from '{src_name}' to unknown node '{tgt_name}' was dropped."
                    )),
                }
            }
        }
    }
    edges
}

/// The tinyflows output-port name for source `kind`'s n8n output index.
fn output_port_name(kind: Option<&NodeKind>, index: usize) -> String {
    match kind {
        Some(NodeKind::Condition) => {
            if index == 0 {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Some(NodeKind::Switch) => index.to_string(),
        _ => {
            if index == 0 {
                "main".to_string()
            } else {
                index.to_string()
            }
        }
    }
}

/// Parses n8n's `position: [x, y]` array into a tinyflows [`Position`].
fn parse_position(value: Option<&Value>) -> Option<Position> {
    let arr = value?.as_array()?;
    let x = arr.first()?.as_f64()?;
    let y = arr.get(1)?.as_f64()?;
    Some(Position { x, y })
}

/// Derives a stable, id-safe slug from an n8n node name when the node carries
/// no `id` of its own.
fn slug(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    if s.is_empty() { "node".to_string() } else { s }
}

#[cfg(test)]
#[path = "n8n_tests.rs"]
mod tests;
