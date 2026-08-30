//! Running a graph against schema-aware mocks to find bindings that can never
//! resolve, before an author is allowed to save it.
//!
//! [`gates`](crate::gates) refuses what is *statically* guaranteed wrong. This
//! module catches the rest of the same failure by actually running the graph:
//! the engine records, per step, which `=`-expressions resolved to null while it
//! was assembling a node's config, and an outbound argument that resolved to
//! null is a step that will call a provider with nothing in a field the provider
//! cares about.
//!
//! Neither the engine nor a run record will ever complain about this on its own.
//! Null is a legal value, so the run reports every node green and the author
//! learns nothing until the provider rejects the call — or worse, accepts it.
//!
//! # Three ways a null is *not* a bug, and all three matter
//!
//! A gate that refuses a graph that would have worked is worse than no gate, so
//! this one skips more than it reports:
//!
//! - **Trigger-scoped data.** The sandbox runs on an empty trigger payload, so
//!   anything reading `run`, or reading `item`/`items` fed entirely by the
//!   trigger, is null *here* and populated in a real run. Rejecting those would
//!   reject nearly every workflow ever written.
//! - **Opaque upstream tool output.** A mock renders a `tool_call` as an echo
//!   and can never produce the provider's real output fields, so a binding onto
//!   one is *unverifiable*, not broken. A dry run reports it; this refuses it
//!   only when it can prove it.
//! - **Anything that did not settle.** A compile failure, a capability error or
//!   a timeout is a different problem, reported elsewhere. This adds only what
//!   a run that finished actually observed.
//!
//! What is left — a null read from an upstream node whose real output the
//! sandbox *does* produce — is a binding that stays broken no matter what the
//! trigger carries.

use std::sync::Arc;

use serde_json::{Value, json};

use crate::caps::mock_schema_aware::{SchemaAwareMockAgentRunner, SchemaAwareMockLlm};
use crate::model::{NodeKind, WorkflowGraph};
use crate::observability::CapturingObserver;

/// Wall-clock bound on the sandbox run this gate performs. Mirrors
/// `builder_tools::DRY_RUN_TIMEOUT_SECS`'s purpose but kept short: unlike the
/// opt-in `dry_run_workflow` tool, this check runs on EVERY
/// propose/revise/save call, so a slow or pathological draft must not stall
/// authoring.
const REQUIRED_ARG_NULL_CHECK_TIMEOUT_SECS: u64 = 15;

/// Sandbox-executes `graph` against `tinyflows`' deterministic MOCK
/// capabilities (the same shape `DryRunWorkflowTool` uses — see this
/// section's module doc) and returns one human-readable error per arg of a
/// real (non-`=`-derived, non-native) `tool_call` node whose `=`-expression
/// resolved to `null` during that run **and** whose expression is wired to a
/// specific upstream node's output (directly, via the implicit
/// `item`/`items` scope, or explicitly via `nodes.<id>...`) rather than to
/// the trigger.
///
/// This run always sandboxes against `json!({})` as the trigger payload (see
/// below), so any arg wired to trigger-scoped data — `=item.<field>` /
/// `=items...` fed directly from the trigger node, or `=run.<field>` (the
/// trigger metadata itself) — legitimately resolves `null` here even though a
/// real webhook/app-event/manual trigger WILL populate it at runtime. Hard
/// gate that on an empty mock run would reject every ordinary trigger-bound
/// workflow (Codex feedback on PR #4826). Only a `null` resolved from a
/// genuine upstream **node** reference is escalated — that's the real B18
/// bug this gate exists to catch: an arg wired to a node output path that can
/// never resolve (e.g. `GMAIL_SEND_EMAIL.subject =
/// "=nodes.build_body.item.subject"` where `build_body` never produces
/// `subject`), which stays broken no matter what the trigger payload is.
///
/// Deliberately does **not** wrap the mock `ToolInvoker` in
/// [`crate::openhuman::flows::tinyflows::caps::PreflightToolInvoker`] the way
/// `DryRunWorkflowTool` does: that wrapper aborts the WHOLE sandbox run the
/// instant a node with a `stop` `on_error` policy (the default) hits a
/// schema-required null arg, which would lose the per-field diagnostic this
/// gate exists to report for every OTHER node — and this check cares about
/// EVERY arg, not just ones the schema happens to mark `required`. The plain
/// mock tool invoker always "succeeds" (a deterministic echo), so the run
/// settles and every node's config-resolution diagnostics get captured
/// regardless of on_error policy or schema required-ness.
///
/// Best-effort, same posture as [`validate_tool_contracts`]: a compile
/// failure (structural errors are already caught by
/// [`validate_and_migrate_graph`] before this gate ever runs) or a sandbox
/// error/timeout is SKIPPED — never turned into a false rejection. This
/// check only ever adds a diagnostic the sandbox actually observed.
pub(crate) async fn validate_required_arg_resolvability(graph: &WorkflowGraph) -> Vec<String> {
    use crate::openhuman::flows::builder_tools::CapturingObserver;
    use crate::openhuman::flows::tinyflows::caps::{
        SchemaAwareMockAgentRunner, SchemaAwareMockLlm,
    };

    let Ok(compiled) = tinyflows::compiler::compile(graph) else {
        return Vec::new();
    };

    let mut caps = tinyflows::caps::mock::mock_capabilities_with_agent(SchemaAwareMockAgentRunner);
    // Same fix as `DryRunWorkflowTool`: a plain agent node (no `agent_ref`)
    // routes to the `llm` slot, not the runner above, so the vendored `MockLlm`
    // echo would fail its `output_parser.schema` sub-port and make this gate
    // reject a correct graph (which is why `propose_workflow` was rejecting
    // valid graphs). The schema-aware mock LLM honors the schema instead.
    caps.llm = Arc::new(SchemaAwareMockLlm);

    let observer = Arc::new(CapturingObserver::default());
    let observer_dyn: Arc<dyn tinyflows::observability::RunObserver> = observer.clone();
    let run = tinyflows::engine::run_with_observer(&compiled, json!({}), &caps, &observer_dyn);
    if tokio::time::timeout(
        std::time::Duration::from_secs(REQUIRED_ARG_NULL_CHECK_TIMEOUT_SECS),
        run,
    )
    .await
    .is_err()
    {
        // Timed out — a different class of problem than this gate exists to
        // catch; never block authoring on it here.
        return Vec::new();
    }
    // A sandbox `Err` outcome here is a compile/capability issue unrelated
    // to null args (the plain mock invoker never itself fails) — surfaced by
    // the other gates / `dry_run_workflow` instead; this gate only adds
    // diagnostics from a run that actually settled, so an error is silently
    // skipped rather than turned into a (misleading) empty-errors success.

    let tool_call_slugs: std::collections::HashMap<&str, &str> = graph
        .nodes
        .iter()
        .filter(|n| n.kind == NodeKind::ToolCall)
        .filter_map(|n| {
            let slug = n.config.get("slug").and_then(Value::as_str)?;
            Some((n.id.as_str(), slug))
        })
        .collect();

    // The trigger node's id, if any — used below to tell a trigger-scoped
    // `item`/`items` reference (the direct predecessor IS the trigger) apart
    // from a real upstream-node reference. Graphs are expected to have
    // exactly one trigger; `flows_validate` rejects zero/multiple before this
    // gate ever runs, so `first()` here doesn't hide ambiguity.
    let trigger_id: Option<&str> = graph
        .nodes
        .iter()
        .find(|n| n.kind == NodeKind::Trigger)
        .map(|n| n.id.as_str());

    let mut errors = Vec::new();
    for step in observer.steps() {
        let Some(&slug) = tool_call_slugs.get(step.node_id.as_str()) else {
            continue;
        };
        // `=`-derived slugs resolve from upstream/trigger data at runtime;
        // native `oh:` tools have no external-provider rejection mode.
        if slug.starts_with('=') || slug.starts_with("oh:") {
            continue;
        }
        for diag in &step.diagnostics {
            let Some(field) = diag.location.strip_prefix("args.") else {
                continue;
            };
            if is_trigger_scoped_expression(&diag.expression, graph, &step.node_id, trigger_id) {
                // Legitimately empty in this gate's `{}` mock run — the real
                // trigger (webhook/app-event/manual) will populate it. Not
                // the B18 broken-wiring case this gate exists to catch.
                tracing::debug!(
                    target: "flows",
                    node = %step.node_id,
                    %slug,
                    %field,
                    expression = %diag.expression,
                    "[flows] required-arg resolvability check: trigger-scoped null in empty \
                     mock run — not rejecting"
                );
                continue;
            }
            // A null bound to the OUTPUT of an upstream Composio-or-native
            // `tool_call` node is UNVERIFIABLE in this echo sandbox — the mock
            // renders BOTH a Composio and a native `oh:` `tool_call` as
            // `{tool, args, connection}` and can NEVER produce their real output
            // fields (`.item.json.data.<field>` for Composio, `.item.json.<field>`
            // for a native tool), so a downstream binding to one resolves `null`
            // here even when the wiring is perfectly correct. Hard-rejecting it
            // (WS6) would block a possibly-correct graph from ever being proposed
            // — the exact false-negative the transcript audit caught, and the one
            // that made this gate reject #5148's own native-attachment chain.
            // Downgrade to a debug-logged skip; `dry_run_workflow` remains the
            // surface that reports it (as an `unverifiable` diagnostic the agent
            // can act on via get_tool_contract / get_tool_output_sample).
            if let Some(upstream) =
                mock_opaque_tool_call_upstream_ref(&diag.expression, graph, &step.node_id)
            {
                tracing::debug!(
                    target: "flows",
                    node = %step.node_id,
                    %slug,
                    %field,
                    upstream = %upstream,
                    expression = %diag.expression,
                    "[flows] required-arg resolvability check: arg binds to a Composio-or-native \
                     tool_call's output — UNVERIFIABLE in the echo sandbox (the mock cannot \
                     produce real tool output fields), not rejecting; dry_run_workflow \
                     reports it instead"
                );
                continue;
            }
            tracing::warn!(
                target: "flows",
                node = %step.node_id,
                %slug,
                %field,
                expression = %diag.expression,
                "[flows] required-arg resolvability check: arg resolved null in sandbox — \
                 rejecting"
            );
            errors.push(format!(
                "Node '{}': arg `{field}` of `{slug}` (`{}`) resolved to `null` during a \
                 sandboxed test run — an empty/missing `{field}` can be rejected by the real \
                 provider at runtime (e.g. Gmail rejects a send with no subject or body). \
                 Rewire it from an upstream node's output that actually has a value — call \
                 dry_run_workflow to see exactly which upstream field is null — or drop the \
                 field from args if it isn't really needed.",
                step.node_id, diag.expression
            ));
        }
    }
    errors
}

/// Returns the node id an explicit `nodes.<id>...` expression addresses —
/// either the legacy dotted shorthand (`=nodes.build_body.item.subject`) or
/// the jq bracket form (`=.nodes["build_body"].item.subject`) — or `None` if
/// the expression's root isn't the `nodes` scope key at all. The expression
/// scope's shape (`item` / `items` / `run` / `nodes`) is documented on
/// `tinyflows`'s `expr` module and `nodes::expr_scope`.
fn explicit_nodes_ref(expr: &str) -> Option<&str> {
    let body = expr.strip_prefix('=')?.trim();
    let body = body.strip_prefix('.').unwrap_or(body);
    let rest = body.strip_prefix("nodes")?;
    if let Some(after_dot) = rest.strip_prefix('.') {
        // Dotted shorthand: `nodes.<id>.item.<field>` — the id ends at the
        // next `.` or `[`.
        let id = after_dot.split(['.', '[']).next()?;
        (!id.is_empty()).then_some(id)
    } else if let Some(after_bracket) = rest.strip_prefix('[') {
        // jq bracket form: `nodes["<id>"]` / `nodes['<id>']`.
        let after_bracket = after_bracket.trim_start();
        let after_bracket = after_bracket
            .strip_prefix('"')
            .or_else(|| after_bracket.strip_prefix('\''))
            .unwrap_or(after_bracket);
        let id = after_bracket.split(['"', '\'', ']']).next()?;
        (!id.is_empty()).then_some(id)
    } else {
        // `rest` is empty (bare `nodes`) or continues some other identifier
        // (e.g. a hypothetical `nodesomething` — not this scope key at all).
        None
    }
}

/// Whether a null-resolved config expression on `node_id` is scoped to the
/// TRIGGER's data rather than a specific upstream node's output — and
/// therefore legitimately empty in [`validate_required_arg_resolvability`]'s
/// `{}` mock run rather than evidence of broken wiring (see that function's
/// doc comment and the Codex feedback it links).
///
/// - `=run...` always addresses the trigger payload/metadata directly
///   (`crate::openhuman::flows::tinyflows`'s `expr_scope` docs) — always
///   trigger-scoped.
/// - `=nodes.<id>...` / `=.nodes["<id>"]...` explicitly names an upstream
///   node. Trigger-scoped only if `<id>` IS the trigger node; naming any
///   other node is exactly the B18 broken-wiring case this gate exists to
///   catch, so it is never treated as trigger-scoped.
/// - `=item...` / `=items...` implicitly addresses `node_id`'s direct
///   predecessor(s) output. Trigger-scoped only when EVERY incoming edge to
///   `node_id` comes from the trigger node — a fan-in that mixes the trigger
///   with a real upstream node, or an `item`/`items` reference fed entirely
///   by real upstream nodes, keeps the existing (reject) behavior, since a
///   node that already ran in the sandbox is expected to have produced its
///   real, deterministic output.
/// - Anything else (a jq expression not rooted at one of the above, or a
///   malformed one) is conservatively treated as NOT trigger-scoped, matching
///   this gate's pre-existing behavior.
fn is_trigger_scoped_expression(
    expr: &str,
    graph: &WorkflowGraph,
    node_id: &str,
    trigger_id: Option<&str>,
) -> bool {
    let body = expr.strip_prefix('=').unwrap_or(expr).trim();
    let body = body.strip_prefix('.').unwrap_or(body);

    if body == "run" || body.starts_with("run.") || body.starts_with("run[") {
        return true;
    }

    if let Some(referenced_id) = explicit_nodes_ref(expr) {
        return trigger_id == Some(referenced_id);
    }

    let is_item_scoped = body == "item"
        || body.starts_with("item.")
        || body.starts_with("item[")
        || body == "items"
        || body.starts_with("items.")
        || body.starts_with("items[");
    if !is_item_scoped {
        return false;
    }

    let Some(trigger_id) = trigger_id else {
        return false;
    };
    let mut predecessors = graph
        .edges
        .iter()
        .filter(|e| e.to_node == node_id)
        .peekable();
    predecessors.peek().is_some() && predecessors.all(|e| e.from_node == trigger_id)
}

/// If a null-resolved config expression on `node_id` is bound to the OUTPUT of
/// an upstream **`tool_call`** node whose sandbox output is an opaque echo — a
/// Composio curated action OR a native `oh:` tool (anything but a `=`-derived
/// dynamic slug) — returns that upstream node's id; otherwise `None`.
///
/// The dry-run / gate sandbox renders BOTH a Composio `tool_call` and a native
/// `oh:` `tool_call` as a deterministic echo (`{tool, args, connection}`) and
/// can NEVER produce their real output fields, so a downstream binding off such
/// a node (`.item.json.data.<field>` for Composio, or `.item.json.<field>` for
/// a native tool after `native_tool_payload`'s unwrap) resolves `null` in the
/// sandbox **even when the wiring is correct** — the binding is UNVERIFIABLE
/// here, not necessarily broken. Callers use this to tell that honest-
/// uncertainty case apart from a genuinely broken binding (one wired to an
/// `agent` / `transform` / `code` / trigger upstream, whose real output the
/// sandbox DOES produce, so a null there IS a real bug).
///
/// The native `oh:` case is why this exists beyond Composio: #5148's guidance
/// prescribes a `produce -> oh:storage_upload_file -> oh:storage_get_link ->
/// send` chain where the send binds `=nodes.get_link.item.json.url`; excluding
/// native upstreams here made the gate hard-reject that exact (correct) chain.
///
/// Handles both addressing forms the engine can trace:
/// - explicit `=nodes.<id>...` / `=.nodes["<id>"]...` (parsed via
///   [`explicit_nodes_ref`]), and
/// - implicit `=item...` / `=items...`, resolved against `node_id`'s direct
///   predecessor — but only when there is exactly ONE incoming edge, so an
///   ambiguous fan-in is never mis-attributed to a single upstream node.
///
/// Anything else (a `=run...` trigger reference, a jq expression not rooted at
/// one of the above, or a reference to a non-`tool_call` / `=`-dynamic node)
/// returns `None`.
pub(crate) fn mock_opaque_tool_call_upstream_ref<'a>(
    expr: &str,
    graph: &'a WorkflowGraph,
    node_id: &str,
) -> Option<&'a str> {
    let referenced_id: String = if let Some(id) = explicit_nodes_ref(expr) {
        id.to_string()
    } else {
        let body = expr.strip_prefix('=').unwrap_or(expr).trim();
        let body = body.strip_prefix('.').unwrap_or(body);
        let is_item_scoped = body == "item"
            || body.starts_with("item.")
            || body.starts_with("item[")
            || body == "items"
            || body.starts_with("items.")
            || body.starts_with("items[");
        if !is_item_scoped {
            return None;
        }
        let mut preds = graph
            .edges
            .iter()
            .filter(|e| e.to_node == node_id)
            .map(|e| e.from_node.as_str());
        let first = preds.next()?;
        if preds.next().is_some() {
            // Ambiguous fan-in — cannot attribute the null to one upstream node.
            return None;
        }
        first.to_string()
    };
    let node = graph.nodes.iter().find(|n| n.id == referenced_id)?;
    if node.kind != NodeKind::ToolCall {
        return None;
    }
    let slug = node.config.get("slug").and_then(Value::as_str)?;
    // A `=`-derived slug is a dynamic runtime slug we can't reason about. But a
    // native `oh:` tool_call IS opaque-echoed by the mock exactly like a
    // Composio one, so its downstream null is equally unverifiable, not broken —
    // do NOT exclude it (that exclusion made the gate reject #5148's own chain).
    if slug.starts_with('=') {
        return None;
    }
    Some(node.id.as_str())
}

