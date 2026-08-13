//! Node execution: the [`NodeExecutor`] trait and per-kind implementations.
//!
//! Each [`crate::model::NodeKind`] maps to a `NodeExecutor`: native control-flow
//! kinds resolve to executors in [`control_flow`], while capability-backed kinds
//! (which reach the outside world via [`crate::caps`]) resolve to executors in
//! [`integration`]. The engine dispatches each node to its executor through
//! `executor_for`.

pub mod control_flow;
pub mod integration;
pub(crate) mod map;
pub(crate) mod release;

use async_trait::async_trait;
use serde_json::Value;

use crate::caps::Capabilities;
use crate::data::Item;
use crate::engine::CancellationToken;
use crate::error::Result;
use crate::model::{Node, NodeKind};

/// The runtime context handed to a node when it executes.
///
/// A node receives its resolved **input items** (the data-flow currency; see
/// [`crate::data`]) plus the run metadata, and returns a [`NodeOutput`].
pub struct NodeContext<'a> {
    /// The node being executed.
    pub node: &'a Node,
    /// The input items delivered to this node, resolved by the engine from the
    /// node's incoming edges. Nodes typically map their logic over these.
    pub input: &'a [Item],
    /// Run metadata and the trigger payload (the `run` slice of the run state).
    pub run: &'a Value,
    /// The `nodes` slice of the run state: every node that has completed so far,
    /// keyed by node id, each slot shaped `{ "items": [<serialized Item>…] }`.
    /// This is what lets an expression address **any** upstream node's output by
    /// id (not just the direct predecessors delivered in `input`). Pass
    /// [`Value::Null`] when no run state exists (e.g. direct executor tests).
    pub nodes: &'a Value,
    /// Host-provided capabilities.
    pub caps: &'a Capabilities,
    /// The graph's own agent registry
    /// ([`WorkflowGraph::agents`](crate::model::WorkflowGraph::agents)).
    ///
    /// An `agent` node resolves its `agent_ref` here first, before falling back
    /// to the harness's registry via
    /// [`AgentRunner::resolve_agent`](crate::caps::AgentRunner::resolve_agent),
    /// so a workflow carrying its own definitions behaves identically on every
    /// host. Empty for a graph that declares none. Every other node kind ignores
    /// it.
    pub agents: &'a [crate::model::AgentDefinition],
    /// Host observer used to report per-item fan-out progress.
    pub observer: &'a dyn crate::observability::RunObserver,
    /// The run's cooperative-cancellation token (see
    /// [`crate::engine::CancellationToken`]). An **owned clone** of the run
    /// token, not a borrow — an executor that spawns nested engine work (today
    /// only [`sub_workflow`](crate::nodes::integration)) must thread a clone
    /// into that child run, so a parent cancel winds the whole subtree down at
    /// the next node boundary instead of orphaning it. Executors that touch the
    /// outside world within a single node need not consult it; the engine
    /// already checks it at the node boundary before this node runs.
    pub token: CancellationToken,
    /// The parallel lane this activation belongs to, when it is one of several
    /// concurrent activations of the *same* node produced by a fan-out.
    ///
    /// `None` for an ordinary activation, which is every activation today. A
    /// lane activation takes its input from the lane envelope rather than from
    /// its predecessors' run-state slots, because every lane sees the same
    /// committed state snapshot and would otherwise all read identical items.
    pub lane: Option<LaneContext>,
    /// The value a checkpointed resume delivered to this node, when this
    /// activation is the re-run of a node that had interrupted.
    ///
    /// `None` on an ordinary activation. A node that pauses the run with
    /// [`NodeControl::Interrupt`] reads this on its re-run to learn what the
    /// host decided — which is the only channel on the checkpointed resume path,
    /// since that path replays from the checkpoint rather than re-executing with
    /// a merged run input.
    pub resume: Option<Value>,
    /// The super-step that is running this activation, counting from 0.
    ///
    /// A monotonic tick that does not depend on the wall clock, so it stays
    /// meaningful across a checkpointed resume. A node that must recognise its
    /// own earlier activations (a poll budget, a fold that must not re-apply)
    /// compares against a step it recorded in its slot.
    pub step: usize,
}

/// Which lane of a fan-out an activation belongs to.
///
/// Travels with the activation itself rather than through the run state, so N
/// concurrent activations of one node id are told apart without any of them
/// having to write a shared slot.
#[derive(Debug, Clone, PartialEq)]
pub struct LaneContext {
    /// Unique across the run; conventionally `"<fan-out node id>#<index>"`.
    pub id: String,
    /// The node whose fan-out created this lane.
    pub origin: String,
    /// This lane's position in the fan-out, from 0. Output is ordered by this
    /// rather than by completion, so a run is deterministic under any timing.
    pub index: usize,
    /// How many lanes the fan-out created, which is what a collector counts
    /// arrivals against.
    pub count: usize,
}

/// Builds the expression scope for a node from its runtime [`NodeContext`].
///
/// The returned object is the `.` input for `=`-expressions evaluated over a
/// node's config (see [`crate::expr::resolve`]). It exposes exactly what
/// `NodeContext` makes available:
///
/// - `item` — the first input item's `json`, or [`Value::Null`] when there is
///   no input;
/// - `items` — the `json` of every input item, in order;
/// - `run` — the run metadata / trigger payload (`ctx.run`);
/// - `nodes` — every **completed** node's output, keyed by node id, each entry
///   shaped `{ "item": <first json>, "items": [<json>…] }`. This lets an
///   expression reference any upstream node by id — e.g.
///   `=nodes.fetch_recipient.item.email` or jq
///   `=.nodes["fetch_recipient"].items[0].email` — including non-adjacent
///   (grandparent) nodes and specific predecessors of a fan-in node. Node
///   **id** is the addressing key (stable across renames); names are not
///   indexed. A node that recorded slot state about itself also exposes it
///   here — currently only a `loop` node's `iteration`, so
///   `=nodes.my_loop.iteration` is the current pass number;
/// - `inputs` — the workflow's resolved declared inputs, keyed by name (see
///   [`crate::model::WorkflowInput`]), so a config field reads
///   `=inputs.repo`. One entry per declaration with defaults already applied,
///   so a binding to a declared name is never *absent* — at worst it is the
///   explicit `null` of an optional input nobody supplied.
///
///   **Write `.inputs.<name>` inside a real jq program.** `=inputs.repo` works
///   because a simple dotted path is walked directly, never compiled. Anything
///   jq actually compiles — a concatenation, a conditional, a pipe — resolves
///   bare `inputs` as jq's own `inputs` *builtin* (which reads further program
///   inputs) rather than this scope key, and the expression quietly yields
///   nothing instead of erroring. The leading dot forces the object lookup:
///   `="Review " + .inputs.repo` is right, `="Review " + inputs.repo` is not.
///   No other scope key has this problem; `inputs` is the one name jq already
///   uses.
#[must_use]
pub(crate) fn expr_scope(ctx: &NodeContext) -> Value {
    let item = ctx
        .input
        .first()
        .map(|i| i.json.clone())
        .unwrap_or(Value::Null);
    expr_scope_for(ctx, item)
}

/// Like [`expr_scope`], but binds `item` to an explicit value rather than the
/// first input item. Used by per-item execution: each iteration resolves the
/// node config against *its own* item, so `=item.<field>` means the current
/// item, while `items`/`run`/`nodes` stay identical across the batch.
#[must_use]
pub(crate) fn expr_scope_for(ctx: &NodeContext, item: Value) -> Value {
    let items: Vec<Value> = ctx.input.iter().map(|i| i.json.clone()).collect();
    build_expr_scope(item, items, ctx.run, nodes_scope(ctx.nodes))
}

/// THE single constructor for an expression scope — every `=`-expression in the
/// crate is evaluated against an object built here.
///
/// Takes an already-projected `nodes` scope so a per-item loop can project it
/// once and reuse it across the batch (see the `transform` node), while callers
/// with a [`NodeContext`] go through [`expr_scope`] / [`expr_scope_for`].
///
/// Keeping this in one place is load-bearing, not tidiness: a node that
/// hand-rolls the object silently loses whatever key it was written before, and
/// the binding fails as a quiet `null` rather than an error. Add a key here and
/// every node sees it.
#[must_use]
pub(crate) fn build_expr_scope(item: Value, items: Vec<Value>, run: &Value, nodes: Value) -> Value {
    serde_json::json!({
        "item": item,
        "items": items,
        "run": run,
        "nodes": nodes,
        // Lifted out of `run` rather than carried separately on `NodeContext`:
        // the engine seeds the resolved declared inputs at `run.inputs`, and
        // this promotes them to a top-level key so authors write the short
        // `=inputs.<name>` while jq programs walking `run` still find them.
        // `Null` when the run predates inputs or the graph declares none.
        "inputs": run.get("inputs").cloned().unwrap_or(Value::Null),
    })
}

/// How a capability node maps over its input items.
///
/// - [`Once`](ExecutionMode::Once): run a single time, binding config against
///   the first input item — one output item regardless of input count.
/// - [`PerItem`](ExecutionMode::PerItem): map over the input, re-resolving
///   config against each item and emitting one output item per input (carrying
///   `paired_item`). This is the n8n-style default for `tool_call` /
///   `http_request`, so a fan-out (`split_out` → node) actually runs per element
///   instead of silently dropping all but the first.
///
/// `PerItem` says *that* the node maps over its input; [`map`] says **how many
/// items run at a time** (`config.concurrency`) and what a failing item does to
/// the batch (`config.on_item_error`). Concurrency defaults to `1`, so a node
/// that does not opt in keeps the sequential ordering and timing it always had.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionMode {
    /// Single invocation against the first item.
    Once,
    /// One invocation per input item.
    PerItem,
}

/// Reads the node's `execution` config (`"once"` | `"per_item"`), falling back
/// to `default` when unset or unrecognized. Read from the raw config (not an
/// `=`-expression) since it selects the resolution strategy itself.
#[must_use]
pub(crate) fn execution_mode(config: &Value, default: ExecutionMode) -> ExecutionMode {
    match config.get("execution").and_then(Value::as_str) {
        Some("per_item") => ExecutionMode::PerItem,
        Some("once") => ExecutionMode::Once,
        _ => default,
    }
}

/// Projects the run state's `nodes` map into the expression-scope shape:
/// `{ "<id>": { "item": <first item json>, "items": [<item json>…] } }`.
///
/// Each state slot stores serialized [`Item`]s (`{ "json": …, … }`); the scope
/// exposes just the `json` payloads, mirroring how `item`/`items` are projected
/// from the node's own input. Slots without an `items` array (or a non-object
/// `nodes` value) are skipped, so an absent run state yields `{}`.
pub(crate) fn nodes_scope(nodes: &Value) -> Value {
    let mut scope = serde_json::Map::new();
    if let Value::Object(map) = nodes {
        for (id, slot) in map {
            let Some(items) = slot.get("items").and_then(Value::as_array) else {
                continue;
            };
            let jsons: Vec<Value> = items
                .iter()
                .map(|item| item.get("json").cloned().unwrap_or(Value::Null))
                .collect();
            let first = jsons.first().cloned().unwrap_or(Value::Null);
            let mut entry = serde_json::json!({ "item": first, "items": jsons });
            // Slot state a node recorded about itself via `NodeOutput::meta`,
            // promoted so expressions can read it the same way they read items.
            // Currently just the `loop` node's pass counter, which is what makes
            // `=nodes.<loop id>.iteration` resolve from anywhere in the graph.
            if let Some(iteration) = slot.get("iteration") {
                entry["iteration"] = iteration.clone();
            }
            scope.insert(id.clone(), entry);
        }
    }
    Value::Object(scope)
}

/// Resolves a node's config against its expression scope, tracing and logging
/// every `=`-expression that resolved to `null`.
///
/// The shared data-binding entry point for capability-backed nodes: the
/// resolved config is identical to `expr::resolve`'s, and each null-resolved
/// expression is `tracing::warn!`ed with the node id, config location, and the
/// original expression, then returned so the node can attach it to its
/// [`NodeOutput::diagnostics`]. Diagnostics are non-fatal by design — a null
/// may be intended, and failure policy belongs to routing/`on_error`.
pub(crate) fn resolve_config_traced(
    ctx: &NodeContext,
) -> (Value, Vec<crate::expr::NullResolution>) {
    resolve_config_traced_with_scope(ctx, expr_scope(ctx))
}

/// Like [`resolve_config_traced`], but binds `item` to `item_json` (the current
/// item in a per-item run) instead of the first input item.
pub(crate) fn resolve_config_traced_for_item(
    ctx: &NodeContext,
    item_json: Value,
) -> (Value, Vec<crate::expr::NullResolution>) {
    resolve_config_traced_with_scope(ctx, expr_scope_for(ctx, item_json))
}

/// Shared body: resolve the node config against a pre-built `scope`, warning on
/// each null-resolved `=`-expression.
fn resolve_config_traced_with_scope(
    ctx: &NodeContext,
    scope: Value,
) -> (Value, Vec<crate::expr::NullResolution>) {
    let (cfg, misses) = crate::expr::resolve_traced(&ctx.node.config, &scope);
    for miss in &misses {
        tracing::warn!(
            node = %ctx.node.id,
            location = %miss.location,
            expression = %miss.expression,
            "config expression resolved to null; check the wiring (`nodes.<id>.item.<field>`)"
        );
    }
    (cfg, misses)
}

/// The outcome of executing a single node: the items it emits and (for branching
/// nodes) which output port to follow.
#[derive(Debug, Clone, Default)]
pub struct NodeOutput {
    /// The items this node emits. A node maps over its input and returns an array
    /// of output [`Item`]s (which may be empty).
    pub items: Vec<Item>,
    /// For branching nodes, the output port to follow (e.g. `"true"`); `None`
    /// means the default `"main"` port.
    pub port: Option<String>,
    /// Non-fatal data-binding diagnostics: every config `=`-expression that
    /// resolved to `null` during this execution (see
    /// [`crate::expr::resolve_traced`]). Surfaced on the run's
    /// [`ExecutionStep`](crate::observability::ExecutionStep) so a host can
    /// point at the exact unresolved wiring; failure policy stays with
    /// routing/`on_error`.
    pub diagnostics: Vec<crate::expr::NullResolution>,
    /// Extra keys to record alongside `items`/`port` in this node's run-state
    /// slot, for a node that must remember something across its own
    /// activations. `None` writes nothing.
    ///
    /// The only current user is [`NodeKind::Loop`](crate::model::NodeKind::Loop),
    /// which keeps its `iteration` count here. Slot state is the right home for
    /// it because the run state is what gets checkpointed, so an iteration
    /// count rides resume for free and is addressable from expressions as
    /// `=nodes.<id>.iteration` — whereas a counter held in the executor would
    /// be lost the moment the run paused, and a counter threaded through items
    /// would be destroyed by any node in the body that reshapes them.
    ///
    /// Must be a JSON object; anything else is ignored when the slot is built.
    pub meta: Option<Value>,
    /// A request to the engine for something other than "emit and move on".
    /// `None` — the overwhelmingly common case — means ordinary data flow.
    ///
    /// This is the channel that lets an executor return *control* rather than
    /// only data. Without it a node can express "here are my items" and "I
    /// failed", but not "pause the run here" or "ask me again shortly" — which
    /// is why, before this existed, a `sub_workflow` whose child paused at an
    /// approval gate had to fail the parent outright rather than pause it.
    pub control: Option<NodeControl>,
}

/// What a node asks the engine to do instead of simply emitting its items.
///
/// Both variants are lowered in [`crate::engine`]'s node handler, which is the
/// only place that can speak to the underlying super-step executor.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeControl {
    /// Pause the whole run here, surfacing `id` on the run's pending set.
    ///
    /// The engine turns this into a `tinyagents` interrupt, which **discards
    /// this activation's state update** — the node re-runs from the top when
    /// the run is resumed. An executor using this must therefore be safe to
    /// re-enter: record what it needs to recognise the resume (a ticket, a
    /// child thread id) in its slot on an *earlier* activation, not this one.
    Interrupt {
        /// Identifies the pause to the host, and addresses the resume value
        /// back to this node. Conventionally the node id.
        id: String,
        /// Host-facing description of what is being waited on.
        payload: Value,
    },
    /// Re-activate this same node in the next super-step, after a delay.
    ///
    /// State-driven waiting: the node's update *is* committed, so it can leave
    /// itself notes, and it is then re-run to look at the world again. This is
    /// how a gather or gate waits for lanes and tickets that settle in
    /// different super-steps.
    ///
    /// Each poll costs one super-step and one node visit against the run's
    /// budgets, so **every** user of this must carry its own bounded poll count
    /// rather than relying on the run-level backstop to stop it.
    Reenter {
        /// How long to wait before the next activation, in milliseconds.
        after_ms: u64,
    },
}

impl NodeOutput {
    /// Builds an output on the default `"main"` port.
    #[must_use]
    pub fn main(items: Vec<Item>) -> Self {
        Self {
            items,
            ..Self::default()
        }
    }

    /// Builds an output that routes to the named `port`.
    #[must_use]
    pub fn routed(items: Vec<Item>, port: impl Into<String>) -> Self {
        Self {
            items,
            port: Some(port.into()),
            ..Self::default()
        }
    }

    /// Builds an empty output on the default port (a node that produced no items).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Attaches data-binding diagnostics (null-resolved expressions) to this
    /// output.
    #[must_use]
    pub fn with_diagnostics(mut self, diagnostics: Vec<crate::expr::NullResolution>) -> Self {
        self.diagnostics = diagnostics;
        self
    }

    /// Attaches extra run-state slot keys to this output (see
    /// [`NodeOutput::meta`]).
    #[must_use]
    pub fn with_meta(mut self, meta: Value) -> Self {
        self.meta = Some(meta);
        self
    }

    /// Attaches a control request to this output (see [`NodeOutput::control`]).
    #[must_use]
    pub fn with_control(mut self, control: NodeControl) -> Self {
        self.control = Some(control);
        self
    }

    /// Builds an output that pauses the run, surfacing `id` on its pending set.
    ///
    /// Carries no items: an interrupt discards this activation's update, so
    /// anything set here would be thrown away.
    #[must_use]
    pub fn interrupt(id: impl Into<String>, payload: Value) -> Self {
        Self::empty().with_control(NodeControl::Interrupt {
            id: id.into(),
            payload,
        })
    }

    /// Builds an output that commits `meta` and asks to be re-run after
    /// `after_ms` milliseconds.
    #[must_use]
    pub fn reenter_after(after_ms: u64, meta: Value) -> Self {
        Self::empty()
            .with_meta(meta)
            .with_control(NodeControl::Reenter { after_ms })
    }
}

/// Executes one node kind.
#[async_trait]
pub trait NodeExecutor: Send + Sync {
    /// Runs the node and returns its output (or a routing decision).
    async fn execute(&self, ctx: NodeContext<'_>) -> Result<NodeOutput>;
}

/// A trigger node's executor: it echoes its input items through unchanged. The
/// engine seeds the trigger payload directly into the run state, so at runtime
/// the trigger is a passthrough; this executor makes the dispatch table total.
#[derive(Debug, Default, Clone)]
struct TriggerNode;

#[async_trait]
impl NodeExecutor for TriggerNode {
    async fn execute(&self, ctx: NodeContext<'_>) -> Result<NodeOutput> {
        Ok(NodeOutput::main(ctx.input.to_vec()))
    }
}

/// Returns the [`NodeExecutor`] for a given [`NodeKind`].
///
/// Native control-flow executors live in [`control_flow`]; capability-backed
/// ones in [`integration`]. The engine uses this to dispatch each graph node.
#[must_use]
pub(crate) fn executor_for(kind: &NodeKind) -> Box<dyn NodeExecutor> {
    match kind {
        NodeKind::Trigger => Box::new(TriggerNode),
        NodeKind::Agent => Box::new(integration::AgentNode),
        NodeKind::ToolCall => Box::new(integration::ToolCallNode),
        NodeKind::HttpRequest => Box::new(integration::HttpRequestNode),
        NodeKind::Code => Box::new(integration::CodeNode),
        NodeKind::Shell => Box::new(integration::ShellNode),
        NodeKind::OutputParser => Box::new(integration::OutputParserNode),
        NodeKind::SubWorkflow => Box::new(integration::SubWorkflowNode),
        NodeKind::Memory => Box::new(integration::MemoryNode),
        NodeKind::Condition => Box::new(control_flow::ConditionNode),
        NodeKind::Switch => Box::new(control_flow::SwitchNode),
        NodeKind::Merge => Box::new(control_flow::MergeNode),
        NodeKind::SplitOut => Box::new(control_flow::SplitOutNode),
        NodeKind::Transform => Box::new(control_flow::TransformNode),
        NodeKind::Dedup => Box::new(control_flow::DedupNode),
        NodeKind::Spawn => Box::new(integration::SpawnNode),
        NodeKind::Gate => Box::new(integration::GateNode),
        NodeKind::Loop => Box::new(control_flow::LoopNode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caps::mock::mock_capabilities;
    use crate::data::Item;
    use crate::model::Node;
    use serde_json::json;

    /// Every [`NodeKind`] variant, so the coverage below stays exhaustive.
    fn all_kinds() -> Vec<NodeKind> {
        use NodeKind::{
            Agent, Code, Condition, Dedup, HttpRequest, Loop, Memory, Merge, OutputParser, Shell,
            SplitOut, SubWorkflow, Switch, ToolCall, Transform, Trigger,
        };
        vec![
            Trigger,
            Agent,
            ToolCall,
            HttpRequest,
            Code,
            Shell,
            Condition,
            Switch,
            Merge,
            SplitOut,
            Transform,
            OutputParser,
            SubWorkflow,
            Memory,
            Dedup,
            Loop,
        ]
    }

    /// Minimal config that lets each kind execute successfully.
    fn config_for(kind: &NodeKind) -> Value {
        match kind {
            NodeKind::ToolCall => json!({ "slug": "demo" }),
            NodeKind::Shell => json!({ "source": "printf ok" }),
            NodeKind::SubWorkflow => json!({
                "workflow": { "nodes": [{ "id": "ct", "kind": "trigger", "name": "ct" }], "edges": [] }
            }),
            // `people` needs no `scope`/`query`, so it runs against the
            // default mock capabilities (which wire a `MemoryProvider`) with
            // the minimal config every other kind gets via `Value::Null`.
            NodeKind::Memory => json!({ "operation": "people" }),
            _ => Value::Null,
        }
    }

    fn node(kind: NodeKind, config: Value) -> Node {
        Node {
            id: "n".into(),
            kind,
            type_version: 1,
            name: "n".into(),
            config,
            ports: vec![],
            position: None,
        }
    }

    #[tokio::test]
    async fn executor_for_is_total_and_every_executor_runs() {
        let caps = mock_capabilities();
        let run = Value::Null;
        for kind in all_kinds() {
            let node = node(kind.clone(), config_for(&kind));
            let input = vec![Item::new(json!({ "x": 1 }))];
            let exec = executor_for(&kind);
            let out = exec
                .execute(NodeContext {
                    node: &node,
                    input: &input,
                    run: &run,
                    nodes: &Value::Null,
                    caps: &caps,
                    agents: &[],
                    observer: &crate::observability::NoopObserver,
                    token: crate::engine::CancellationToken::new(),
                    lane: None,
                    resume: None,
                    step: 0,
                })
                .await;
            assert!(
                out.is_ok(),
                "executor for {kind:?} should run: {:?}",
                out.err()
            );
        }
    }

    #[tokio::test]
    async fn trigger_executor_passes_input_through() {
        let caps = mock_capabilities();
        let run = Value::Null;
        let node = node(NodeKind::Trigger, Value::Null);
        let input = vec![Item::new(json!({ "a": 1 })), Item::new(json!({ "b": 2 }))];
        let out = executor_for(&NodeKind::Trigger)
            .execute(NodeContext {
                node: &node,
                input: &input,
                run: &run,
                nodes: &Value::Null,
                caps: &caps,
                agents: &[],
                observer: &crate::observability::NoopObserver,
                token: crate::engine::CancellationToken::new(),
                lane: None,
                resume: None,
                step: 0,
            })
            .await
            .expect("execute");
        assert_eq!(out.items, input);
        assert_eq!(out.port, None);
    }

    #[test]
    fn expr_scope_exposes_completed_nodes_keyed_by_id() {
        let caps = mock_capabilities();
        let run = Value::Null;
        let n = node(NodeKind::Transform, Value::Null);
        let input = vec![Item::new(json!({ "in": 1 }))];
        // Run-state shape: serialized `Item`s under each completed node's slot.
        let nodes_state = json!({
            "a": { "items": [
                { "json": { "x": 42 } },
                { "json": { "x": 43 }, "paired_item": 0 },
            ] },
            "b": { "items": [], "port": "true" },
            "broken": { "no_items": true },
        });
        let ctx = NodeContext {
            node: &n,
            input: &input,
            run: &run,
            nodes: &nodes_state,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            resume: None,
            step: 0,
        };
        let scope = expr_scope(&ctx);
        // Existing keys unchanged (back-compat).
        assert_eq!(scope["item"], json!({ "in": 1 }));
        assert_eq!(scope["items"], json!([{ "in": 1 }]));
        // `nodes.<id>` projects each slot's item `json` payloads.
        assert_eq!(scope["nodes"]["a"]["item"], json!({ "x": 42 }));
        assert_eq!(
            scope["nodes"]["a"]["items"],
            json!([{ "x": 42 }, { "x": 43 }])
        );
        // An empty slot yields a null `item` and empty `items`.
        assert_eq!(scope["nodes"]["b"]["item"], Value::Null);
        assert_eq!(scope["nodes"]["b"]["items"], json!([]));
        // A slot without an `items` array is skipped, not panicked on.
        assert!(scope["nodes"].get("broken").is_none());
    }

    #[test]
    fn expr_scope_with_null_nodes_state_is_empty_map() {
        let caps = mock_capabilities();
        let run = Value::Null;
        let n = node(NodeKind::Transform, Value::Null);
        let ctx = NodeContext {
            node: &n,
            input: &[],
            run: &run,
            nodes: &Value::Null,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            resume: None,
            step: 0,
        };
        let scope = expr_scope(&ctx);
        assert_eq!(scope["nodes"], json!({}));
    }

    #[test]
    fn node_output_constructors_have_expected_shapes() {
        let items = vec![Item::new(json!({ "a": 1 }))];

        let main = NodeOutput::main(items.clone());
        assert_eq!(main.port, None);
        assert_eq!(main.items, items);

        let routed = NodeOutput::routed(items.clone(), "true");
        assert_eq!(routed.port.as_deref(), Some("true"));
        assert_eq!(routed.items, items);

        let empty = NodeOutput::empty();
        assert!(empty.items.is_empty());
        assert_eq!(empty.port, None);
    }
}
