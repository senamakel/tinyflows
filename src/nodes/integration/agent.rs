//! The `agent` node: an LLM agent turn with optional sub-ports.

use async_trait::async_trait;
use serde_json::Value;

use crate::data::Item;
use crate::error::Result;
use crate::nodes::integration::schema;
use crate::nodes::{NodeContext, NodeExecutor, NodeOutput};

/// Runs an LLM agent turn, optionally composed with **sub-ports** that attach an
/// output parser and tools to the bare completion.
///
/// The node config is the completion request handed to the injected
/// [`LlmProvider`](crate::caps::LlmProvider). On top of that, two sub-ports are
/// wired (config-embedded, so a plain agent node with just a prompt still works
/// unchanged):
///
/// - **tool sub-port** (`config.tools`): the available tools are surfaced to the
///   model in the request. If the model's response elects to call one of the
///   *offered* tools — a `tool_call: { slug, args?, connection_ref? }` object in
///   the response — the agent invokes it once via
///   [`ToolInvoker`](crate::caps::ToolInvoker) and attaches the result under
///   `tool_result`. This is a **single hop** (no unbounded agent loop) — a full
///   multi-turn tool-use loop is a documented follow-up.
/// - **output-parser sub-port** (`config.output_parser`): after the completion
///   (and any tool hop), the resulting value is validated/repaired against
///   `config.output_parser.schema` using the shared [`schema`] routine
///   (validate → one LLM auto-fix → re-validate), honoring
///   `config.output_parser.auto_fix` (default `true`).
///
/// **Agent-kind selection** (`config.agent_ref`): when set and the host wired the
/// optional [`AgentRunner`](crate::caps::AgentRunner) capability, the node runs
/// that named agent to completion instead of issuing a bare completion.
/// `agent_ref` resolves against the graph's own
/// [`agents`](crate::model::WorkflowGraph::agents) registry first, then the
/// harness's, then passes through as a bare id. The resulting
/// [`AgentDefinition`](crate::model::AgentDefinition) is merged with the node's
/// overrides — **narrowing only** — and assembled into a typed
/// [`AgentRunRequest`](crate::caps::AgentRunRequest) carrying the resolved
/// instructions, model, provider, working directory, context blocks, tool
/// descriptors, limits, and metadata. See
/// [`agent_request`](super::agent_request) for the merge rules.
///
/// The inline tool sub-port is skipped on that path (the agent owns its own tool
/// loop, and re-invoking a tool it already called would duplicate the side
/// effect); the output-parser sub-port still applies. `agent_ref` is read from
/// trusted node config, never from model output.
///
/// With no `agent_ref` (or no `AgentRunner`) the node falls back to
/// [`LlmProvider`](crate::caps::LlmProvider) exactly as it always has — the one
/// addition being that a declared `context` is still resolved into blocks, so an
/// author's context is not silently dropped on a host without a harness.
///
/// **Output.** The agent-kind path emits `{ json, text, raw, meta }`, where
/// `meta.stop` is `"finished"` or `"limit_stop"` — so a downstream `condition`
/// can branch on whether the agent actually reached an answer. A `limit_stop`
/// payload is partial and skips the output parser. The degraded path emits the
/// original three-key envelope unchanged. A harness reporting
/// [`Paused`](crate::caps::StopReason::Paused) fails the node: resuming a paused
/// agent is not supported yet, and emitting a half-run agent's output as if it
/// were an answer is the failure this refuses to make.
///
/// Sub-ports **not** yet wired (documented follow-ups): a `chat_model` sub-port
/// (attached model selection beyond what the request already carries) and a
/// `memory` sub-port (conversation memory injected into the request / persisted
/// across turns). Those require attached-node wiring and/or `StateStore` plumbing
/// and are deliberately left out rather than stubbed.
#[derive(Debug, Default, Clone)]
pub struct AgentNode;

#[async_trait]
impl NodeExecutor for AgentNode {
    async fn execute(&self, ctx: NodeContext<'_>) -> Result<NodeOutput> {
        // Execution mode (default `once`): an agent turn is usually batch-level,
        // but `per_item` maps it over the input (one turn per item, config
        // re-resolved against each) for row-wise agent processing.
        let per_item =
            crate::nodes::execution_mode(&ctx.node.config, crate::nodes::ExecutionMode::Once)
                == crate::nodes::ExecutionMode::PerItem
                && !ctx.input.is_empty();

        if per_item {
            // Fan out: `config.concurrency` decides how many turns run at once
            // (default 1 — sequential, as this node has always behaved), and
            // `config.on_item_error` what a failing turn does to the batch.
            let opts = crate::nodes::map::map_options(&ctx.node.config, &ctx.node.id);
            let ctx = &ctx;
            let (items, diagnostics) = crate::nodes::map::map_items(
                ctx.input.len(),
                &ctx.node.id,
                ctx.observer,
                opts,
                move |index| async move {
                    let item_json = ctx.input[index].json.clone();
                    let (cfg, diags) =
                        crate::nodes::resolve_config_traced_for_item(ctx, item_json.clone());
                    // The same scope the config was resolved against, so an
                    // agent definition's own `=`-expressions see this item.
                    let scope = crate::nodes::expr_scope_for(ctx, item_json);
                    let item = run_turn_indexed(ctx, &cfg, &scope, Some(index)).await?;
                    Ok((item, diags))
                },
            )
            .await?;
            return Ok(NodeOutput::main(items).with_diagnostics(diagnostics));
        }

        // Single turn against the first-item scope (or empty input).
        let (cfg, diagnostics) = crate::nodes::resolve_config_traced(&ctx);
        let scope = crate::nodes::expr_scope(&ctx);
        let item = run_turn(&ctx, &cfg, &scope).await?;
        Ok(NodeOutput::main(vec![item]).with_diagnostics(diagnostics))
    }
}

/// Runs one agent turn against an already-resolved `cfg`: the completion (or
/// registered agent kind), the optional tool sub-port, the optional
/// output-parser sub-port, and finally the stable `{ json, text, raw }`
/// envelope. Returns the emitted [`Item`] (without pairing — the caller sets it).
async fn run_turn(ctx: &NodeContext<'_>, cfg: &Value, scope: &Value) -> Result<Item> {
    run_turn_indexed(ctx, cfg, scope, None).await
}

/// [`run_turn`], told which input item it is running for under `per_item`
/// execution, so the harness can attribute the run.
async fn run_turn_indexed(
    ctx: &NodeContext<'_>,
    cfg: &Value,
    scope: &Value,
    item_index: Option<usize>,
) -> Result<Item> {
    let conn = cfg.get("connection_ref").and_then(Value::as_str);

    // Agent-kind selection: a trusted `agent_ref` in config routes this node
    // to a host-registered agent (its own tools/model/sandbox) via the
    // optional `AgentRunner` capability, instead of a bare completion. The ref
    // comes from resolved node config — never from model output — so it can't
    // be steered by prompt injection. Falls back to `LlmProvider` when no
    // `agent_ref` is set or the host wired no agent registry.
    let agent_ref = super::agent_request::agent_ref_of(cfg);
    let via_agent_kind = agent_ref.is_some() && ctx.caps.agent.is_some();

    if let (Some(agent_ref), Some(runner)) = (agent_ref, ctx.caps.agent.as_ref()) {
        tracing::debug!(agent_ref, "agent node: running registered agent kind");
        let request =
            super::agent_request::assemble(ctx, cfg, agent_ref, scope, item_index).await?;
        let outcome = runner.run(request).await?;
        return finish_agent_run(ctx, cfg, conn, agent_ref, outcome).await;
    }

    // Degraded path: no agent kind selected, or no harness wired. The node
    // config *is* the completion request, exactly as it has always been — when
    // a `tools` sub-port is configured its descriptors ride along so the model
    // can elect to call one.
    //
    // The one addition: when the author declared `context`, its sources are
    // resolved and the key is replaced with the resulting blocks, so declared
    // context still reaches the model on a host with no `AgentRunner`. Nodes
    // that declare no context are untouched, so an existing graph's request is
    // byte-identical to what it has always been.
    let request = match cfg.get("context") {
        Some(raw) if !raw.is_null() => {
            let sources: Vec<crate::model::ContextSource> = serde_json::from_value(raw.clone())
                .map_err(|e| {
                    crate::error::EngineError::Capability(format!(
                        "agent node {}: invalid `context`: {e}",
                        ctx.node.id
                    ))
                })?;
            let identity = super::agent_request::identity_of(ctx, item_index);
            let blocks = super::agent_request::resolve_context(ctx, &sources, &identity).await?;
            let mut request = cfg.clone();
            request["context"] = serde_json::to_value(blocks).unwrap_or(Value::Null);
            request
        }
        _ => cfg.clone(),
    };
    let response = ctx.caps.llm.complete(request, conn).await?;

    // `text`/`raw` are derived from the untouched completion; `value` is the
    // structured payload we thread the sub-ports (tool hop, output parser)
    // through before it becomes the envelope's `json`.
    let text = response.as_str().map(str::to_string).or_else(|| {
        response
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let raw = response.clone();

    // Tool sub-port (single hop): honor a `tool_call` the model returned, but
    // only for a tool that was actually offered in `config.tools`. Skipped
    // when a registered agent kind ran — that agent drives its own tool loop.
    let mut value = response;
    let model_tool_call = if via_agent_kind {
        None
    } else {
        value.get("tool_call").cloned()
    };
    if let Some(tool_call) = model_tool_call {
        if let Some(slug) = tool_call.get("slug").and_then(Value::as_str) {
            // Only a tool actually offered in `config.tools` may be invoked;
            // keep the matched descriptor so its trusted `connection_ref` wins.
            let offered_tool = cfg
                .get("tools")
                .and_then(Value::as_array)
                .and_then(|tools| {
                    tools
                        .iter()
                        .find(|t| t.get("slug").and_then(Value::as_str) == Some(slug))
                });
            if let Some(offered_tool) = offered_tool {
                tracing::debug!(slug, "agent tool sub-port: invoking model-elected tool");
                let args = tool_call.get("args").cloned().unwrap_or(Value::Null);
                // Credentials come from trusted config only: the offered tool
                // descriptor's `connection_ref`, else the node's. The model's
                // `tool_call.connection_ref` is deliberately NOT trusted — a
                // prompt-injection could otherwise elect an arbitrary host
                // credential id for the call.
                let tool_conn = offered_tool
                    .get("connection_ref")
                    .and_then(Value::as_str)
                    .or(conn);
                let result = ctx.caps.tools.invoke(slug, args, tool_conn).await?;
                if let Value::Object(map) = &mut value {
                    map.insert("tool_result".to_string(), result);
                }
            } else {
                tracing::warn!(
                    slug,
                    "agent tool sub-port: model elected an un-offered tool; ignoring"
                );
            }
        }
    }

    // Output-parser sub-port: validate/repair the agent output against a schema.
    if let Some(parser) = cfg.get("output_parser").filter(|p| !p.is_null()) {
        if let Some(parser_schema) = parser.get("schema").filter(|s| !s.is_null()) {
            let auto_fix = parser
                .get("auto_fix")
                .and_then(Value::as_bool)
                .unwrap_or(true);
            let parser_conn = parser
                .get("connection_ref")
                .and_then(Value::as_str)
                .or(conn);
            value = schema::parse_and_validate(
                value,
                parser_schema,
                auto_fix,
                &ctx.caps.llm,
                parser_conn,
            )
            .await?;
        }
    }

    // Emit the stable envelope: `json` is the structured (tool/parser-threaded)
    // value, `text` the model's prose, `raw` the original completion — so a
    // downstream node reads `=item.text` / `=item.json.<field>` regardless of
    // which sub-ports fired. See [`super::envelope`].
    let json = match value {
        Value::Object(_) | Value::Array(_) => value,
        _ => Value::Null,
    };
    Ok(Item::new(super::envelope::from_parts(json, text, raw)))
}

/// Turns a harness's [`AgentRunOutcome`] into the emitted [`Item`]: honor the
/// stop reason, apply the output-parser sub-port, and publish the envelope.
///
/// The stop reason is read **before** the payload, which is the point of having
/// one. A bare value cannot distinguish an agent that answered from one that ran
/// out of budget one step short, and the workflow marches downstream with a
/// partial answer either way.
async fn finish_agent_run(
    ctx: &NodeContext<'_>,
    cfg: &Value,
    conn: Option<&str>,
    agent_ref: &str,
    outcome: crate::caps::AgentRunOutcome,
) -> Result<Item> {
    use crate::caps::StopReason;

    let mut meta = serde_json::json!({ "stop": outcome.stop.as_str(), "agent_ref": agent_ref });

    match &outcome.stop {
        StopReason::Finished => {}
        StopReason::LimitStop { limit } => {
            tracing::warn!(
                node = %ctx.node.id,
                agent_ref,
                limit = %limit,
                "agent node: the agent stopped on a limit; its output is partial"
            );
            meta["limit"] = Value::from(limit.clone());
        }
        StopReason::Paused { reason, .. } => {
            // A pause is resumable, not finished — and the engine cannot yet
            // route one into its checkpoint/resume machinery. Failing loudly
            // beats emitting a half-run agent's output as if it were an answer;
            // see `StopReason::Paused`.
            return Err(crate::error::EngineError::Capability(format!(
                "agent node {}: agent `{agent_ref}` paused ({reason}); resuming a paused agent \
                 is not supported yet, so the run cannot continue",
                ctx.node.id
            )));
        }
    }

    if let Some(usage) = outcome.usage {
        meta["usage"] = serde_json::to_value(usage).unwrap_or(Value::Null);
    }

    // Output-parser sub-port. Deliberately skipped on a limit stop: validating
    // a knowingly partial payload against the author's schema either fails for
    // the wrong reason or, with `auto_fix`, spends a model call inventing the
    // missing half.
    let mut value = outcome.json;
    if matches!(outcome.stop, StopReason::Finished)
        && let Some(parser) = cfg.get("output_parser").filter(|p| !p.is_null())
        && let Some(parser_schema) = parser.get("schema").filter(|s| !s.is_null())
    {
        let auto_fix = parser
            .get("auto_fix")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let parser_conn = parser
            .get("connection_ref")
            .and_then(Value::as_str)
            .or(conn);
        value =
            schema::parse_and_validate(value, parser_schema, auto_fix, &ctx.caps.llm, parser_conn)
                .await?;
    }

    let json = match value {
        Value::Object(_) | Value::Array(_) => value,
        _ => Value::Null,
    };
    Ok(Item::new(super::envelope::from_parts_with_meta(
        json,
        outcome.text,
        outcome.raw,
        meta,
    )))
}

#[cfg(test)]
mod tests {
    use crate::caps::mock::mock_capabilities;
    use crate::compiler::compile;
    use crate::engine::run;
    use crate::model::{Edge, Node, NodeKind, WorkflowGraph};
    use serde_json::{Value, json};

    fn wf(kind: NodeKind, config: Value) -> WorkflowGraph {
        WorkflowGraph {
            nodes: vec![
                Node {
                    id: "t".into(),
                    kind: NodeKind::Trigger,
                    type_version: 1,
                    name: "t".into(),
                    config: Value::Null,
                    ports: vec![],
                    position: None,
                },
                Node {
                    id: "n".into(),
                    kind,
                    type_version: 1,
                    name: "n".into(),
                    config,
                    ports: vec![],
                    position: None,
                },
            ],
            edges: vec![Edge {
                from_node: "t".into(),
                from_port: "main".into(),
                to_node: "n".into(),
                to_port: "main".into(),
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn agent_completes_config_request() {
        let graph = wf(NodeKind::Agent, json!({ "prompt": "hi" }));
        let compiled = compile(&graph).expect("compile");
        let out = run(&compiled, Value::Null, &mock_capabilities())
            .await
            .expect("run");
        assert_eq!(
            out.output["nodes"]["n"]["items"][0]["json"]["json"]["completion"]["prompt"],
            "hi"
        );
    }

    use super::AgentNode;
    use crate::data::Item;
    use crate::nodes::{NodeContext, NodeExecutor};

    fn agent_node(config: Value) -> Node {
        Node {
            id: "n".into(),
            kind: NodeKind::Agent,
            type_version: 1,
            name: "n".into(),
            config,
            ports: vec![],
            position: None,
        }
    }

    #[tokio::test]
    async fn defaults_to_once_but_per_item_maps_over_input() {
        // Agent defaults to `once` (a single turn regardless of input count)...
        let once = agent_node(json!({ "prompt": "=item.name" }));
        let input = vec![
            Item::new(json!({ "name": "A" })),
            Item::new(json!({ "name": "B" })),
        ];
        let caps = mock_capabilities();
        let run_meta = Value::Null;
        let out = AgentNode
            .execute(NodeContext {
                node: &once,
                input: &input,
                run: &run_meta,
                nodes: &Value::Null,
                caps: &caps,
                agents: &[],
                observer: &crate::observability::NoopObserver,
                token: crate::engine::CancellationToken::new(),
                lane: None,
                step: 0,
            })
            .await
            .expect("execute");
        assert_eq!(out.items.len(), 1, "once mode emits a single item");
        assert_eq!(out.items[0].json["json"]["completion"]["prompt"], "A");

        // ...but `execution: per_item` runs one turn per input item.
        let per_item = agent_node(json!({ "prompt": "=item.name", "execution": "per_item" }));
        let out = AgentNode
            .execute(NodeContext {
                node: &per_item,
                input: &input,
                run: &run_meta,
                nodes: &Value::Null,
                caps: &caps,
                agents: &[],
                observer: &crate::observability::NoopObserver,
                token: crate::engine::CancellationToken::new(),
                lane: None,
                step: 0,
            })
            .await
            .expect("execute");
        assert_eq!(out.items.len(), 2, "per_item emits one turn per input");
        assert_eq!(out.items[0].json["json"]["completion"]["prompt"], "A");
        assert_eq!(out.items[1].json["json"]["completion"]["prompt"], "B");
        assert_eq!(out.items[1].paired_item, Some(1));
    }

    #[tokio::test]
    async fn threads_connection_ref_and_echoes_config() {
        let node = agent_node(json!({ "prompt": "hi", "connection_ref": "acct_9" }));
        let input = vec![Item::new(json!({ "seed": 1 }))];
        let caps = mock_capabilities();
        let run_meta = Value::Null;
        let ctx = NodeContext {
            node: &node,
            input: &input,
            run: &run_meta,
            nodes: &Value::Null,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            step: 0,
        };
        let out = AgentNode.execute(ctx).await.expect("execute");
        assert_eq!(out.items.len(), 1);
        // The mock LLM echoes the whole config under `completion` and the conn
        // ref; under the envelope that structured payload is at `json.*`.
        assert_eq!(out.items[0].json["json"]["completion"]["prompt"], "hi");
        assert_eq!(out.items[0].json["json"]["connection"], "acct_9");
        // The raw completion is preserved verbatim under `raw`.
        assert_eq!(out.items[0].json["raw"]["completion"]["prompt"], "hi");
    }

    #[tokio::test]
    async fn resolves_expression_in_config_against_input() {
        // `prompt` is a `=`-expression bound to the input item's `name`; the mock
        // LLM echoes the resolved request under `completion`.
        let node = agent_node(json!({ "prompt": "=item.name" }));
        let input = vec![Item::new(json!({ "name": "X" }))];
        let caps = mock_capabilities();
        let run_meta = Value::Null;
        let ctx = NodeContext {
            node: &node,
            input: &input,
            run: &run_meta,
            nodes: &Value::Null,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            step: 0,
        };
        let out = AgentNode.execute(ctx).await.expect("execute");
        assert_eq!(out.items[0].json["json"]["completion"]["prompt"], "X");
    }

    #[tokio::test]
    async fn missing_connection_ref_is_null() {
        let node = agent_node(json!({ "prompt": "hi" }));
        let input = vec![];
        let caps = mock_capabilities();
        let run_meta = Value::Null;
        let ctx = NodeContext {
            node: &node,
            input: &input,
            run: &run_meta,
            nodes: &Value::Null,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            step: 0,
        };
        let out = AgentNode.execute(ctx).await.expect("execute");
        assert_eq!(out.items[0].json["json"]["connection"], Value::Null);
    }

    #[tokio::test]
    async fn emits_exactly_one_item_regardless_of_input_count() {
        // The agent turn is driven by config, not by mapping over input, so it
        // always emits a single completion item.
        let node = agent_node(json!({ "prompt": "hi" }));
        let input = vec![
            Item::new(json!({ "a": 1 })),
            Item::new(json!({ "b": 2 })),
            Item::new(json!({ "c": 3 })),
        ];
        let caps = mock_capabilities();
        let run_meta = Value::Null;
        let ctx = NodeContext {
            node: &node,
            input: &input,
            run: &run_meta,
            nodes: &Value::Null,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            step: 0,
        };
        let out = AgentNode.execute(ctx).await.expect("execute");
        assert_eq!(out.items.len(), 1);
        assert_eq!(out.port, None);
    }

    // --- sub-ports: tool + output_parser ---

    use crate::caps::{Capabilities, LlmProvider};
    use async_trait::async_trait;
    use std::sync::Arc;

    fn caps_with_llm(llm: Arc<dyn LlmProvider>) -> Capabilities {
        let mut caps = mock_capabilities();
        caps.llm = llm;
        caps
    }

    async fn run_agent(node: &Node, caps: &Capabilities) -> Value {
        let input: Vec<Item> = vec![];
        let run_meta = Value::Null;
        let ctx = NodeContext {
            node,
            input: &input,
            run: &run_meta,
            nodes: &Value::Null,
            caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            step: 0,
        };
        AgentNode
            .execute(ctx)
            .await
            .expect("execute")
            .items
            .remove(0)
            .json
    }

    /// An LLM that returns a fixed `tool_call` directive on the completion call.
    struct ToolCallingLlm(Value);

    #[async_trait]
    impl LlmProvider for ToolCallingLlm {
        async fn complete(
            &self,
            _request: Value,
            _conn: Option<&str>,
        ) -> crate::error::Result<Value> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn tool_sub_port_invokes_offered_tool_and_attaches_result() {
        // The model elects to call an offered tool; the agent invokes it once and
        // attaches the (mock) tool output under `tool_result`.
        let node = agent_node(json!({
            "prompt": "do it",
            "tools": [{ "slug": "slack.post" }]
        }));
        let llm = Arc::new(ToolCallingLlm(json!({
            "tool_call": { "slug": "slack.post", "args": { "text": "hi" } }
        })));
        let value = run_agent(&node, &caps_with_llm(llm)).await;
        // Mock ToolInvoker echoes the slug/args it was called with; the tool
        // result lives at the stable `json.tool_result` accessor.
        assert_eq!(value["json"]["tool_result"]["tool"], "slack.post");
        assert_eq!(value["json"]["tool_result"]["args"]["text"], "hi");
    }

    #[tokio::test]
    async fn tool_sub_port_ignores_unoffered_tool() {
        // The model tries to call a tool that was never offered; the agent leaves
        // the output untouched (no `tool_result`).
        let node = agent_node(json!({
            "prompt": "do it",
            "tools": [{ "slug": "slack.post" }]
        }));
        let llm = Arc::new(ToolCallingLlm(json!({
            "tool_call": { "slug": "danger.delete_all" }
        })));
        let value = run_agent(&node, &caps_with_llm(llm)).await;
        assert!(value["json"].get("tool_result").is_none());
    }

    #[tokio::test]
    async fn tool_sub_port_ignores_model_supplied_connection_ref() {
        // Security: a model-supplied `tool_call.connection_ref` must NOT be
        // trusted (prompt-injection could otherwise select an arbitrary host
        // credential). The credential comes from the offered tool descriptor's
        // `connection_ref` when present, else the node's `connection_ref`.
        let node = agent_node(json!({
            "prompt": "do it",
            "connection_ref": "node_acct",
            "tools": [{ "slug": "slack.post", "connection_ref": "trusted_acct" }]
        }));
        let llm = Arc::new(ToolCallingLlm(json!({
            "tool_call": {
                "slug": "slack.post",
                "args": { "text": "hi" },
                "connection_ref": "attacker_acct"
            }
        })));
        let value = run_agent(&node, &caps_with_llm(llm)).await;
        // The mock ToolInvoker echoes the `conn` it was invoked with: it must be
        // the offered descriptor's trusted id, never the model-supplied one.
        assert_eq!(value["json"]["tool_result"]["connection"], "trusted_acct");
    }

    #[tokio::test]
    async fn tool_sub_port_falls_back_to_node_connection_ref() {
        // When the offered tool descriptor carries no `connection_ref`, the node's
        // `connection_ref` is used — still never the model-supplied one.
        let node = agent_node(json!({
            "prompt": "do it",
            "connection_ref": "node_acct",
            "tools": [{ "slug": "slack.post" }]
        }));
        let llm = Arc::new(ToolCallingLlm(json!({
            "tool_call": {
                "slug": "slack.post",
                "args": { "text": "hi" },
                "connection_ref": "attacker_acct"
            }
        })));
        let value = run_agent(&node, &caps_with_llm(llm)).await;
        assert_eq!(value["json"]["tool_result"]["connection"], "node_acct");
    }

    /// An LLM that returns an invalid completion, but a schema-valid value when
    /// asked to coerce (the auto-fix call carries `task == "coerce_to_schema"`).
    struct ParserLlm {
        completion: Value,
        fixed: Value,
    }

    #[async_trait]
    impl LlmProvider for ParserLlm {
        async fn complete(
            &self,
            request: Value,
            _conn: Option<&str>,
        ) -> crate::error::Result<Value> {
            if request.get("task").and_then(Value::as_str) == Some("coerce_to_schema") {
                Ok(json!({ "value": self.fixed.clone() }))
            } else {
                Ok(self.completion.clone())
            }
        }
    }

    #[tokio::test]
    async fn output_parser_sub_port_repairs_agent_output() {
        // The completion is missing a required `name`; the output-parser sub-port
        // runs a one-shot auto-fix that supplies it.
        let node = agent_node(json!({
            "prompt": "hi",
            "output_parser": { "schema": { "type": "object", "required": ["name"] } }
        }));
        let llm = Arc::new(ParserLlm {
            completion: json!({ "wrong": 1 }),
            fixed: json!({ "name": "fixed" }),
        });
        let value = run_agent(&node, &caps_with_llm(llm)).await;
        // The schema-coerced value is the envelope's structured `json`.
        assert_eq!(value["json"], json!({ "name": "fixed" }));
    }

    #[tokio::test]
    async fn output_parser_sub_port_errors_when_unfixable() {
        let node = agent_node(json!({
            "prompt": "hi",
            "output_parser": { "schema": { "type": "object", "required": ["name"] } }
        }));
        // Completion invalid; "fix" still invalid → the node surfaces an error.
        let llm = Arc::new(ParserLlm {
            completion: json!({ "wrong": 1 }),
            fixed: json!({ "still": "wrong" }),
        });
        let input: Vec<Item> = vec![];
        let run_meta = Value::Null;
        let caps = caps_with_llm(llm);
        let ctx = NodeContext {
            node: &node,
            input: &input,
            run: &run_meta,
            nodes: &Value::Null,
            caps: &caps,
            agents: &[],
            observer: &crate::observability::NoopObserver,
            token: crate::engine::CancellationToken::new(),
            lane: None,
            step: 0,
        };
        let err = AgentNode
            .execute(ctx)
            .await
            .expect_err("unfixable output must error");
        assert!(matches!(err, crate::error::EngineError::Capability(_)));
    }

    // --- agent-kind selection (`agent_ref` -> AgentRunner) ---

    use crate::caps::mock::{MockAgentRunner, mock_capabilities_with_agent};

    #[tokio::test]
    async fn agent_ref_routes_to_the_registered_agent_kind() {
        // With an `agent_ref` and an AgentRunner wired, the node runs that named
        // agent (the mock echoes the ref/request) rather than a bare completion.
        let node = agent_node(json!({ "agent_ref": "code_executor", "prompt": "fix the bug" }));
        let caps = mock_capabilities_with_agent(MockAgentRunner);
        let value = run_agent(&node, &caps).await;
        assert_eq!(value["json"]["agent"], "code_executor");
        assert_eq!(value["json"]["request"]["prompt"], "fix the bug");
    }

    #[tokio::test]
    async fn agent_ref_is_ignored_without_a_runner() {
        // No AgentRunner in the bundle → fall back to the LlmProvider completion
        // even though `agent_ref` is present (host without an agent registry).
        let node = agent_node(json!({ "agent_ref": "researcher", "prompt": "hi" }));
        let value = run_agent(&node, &mock_capabilities()).await;
        // MockLlm echo shape, not the MockAgentRunner shape.
        assert_eq!(value["json"]["completion"]["prompt"], "hi");
        assert!(value["json"].get("agent").is_none());
    }

    #[tokio::test]
    async fn empty_agent_ref_falls_back_to_completion() {
        let node = agent_node(json!({ "agent_ref": "", "prompt": "hi" }));
        let caps = mock_capabilities_with_agent(MockAgentRunner);
        let value = run_agent(&node, &caps).await;
        assert_eq!(value["json"]["completion"]["prompt"], "hi");
    }

    #[tokio::test]
    async fn agent_kind_skips_inline_tool_subport() {
        // A registered agent owns its own tool loop, so a `tool_call` directive in
        // its response is NOT re-invoked by the inline sub-port. MockAgentRunner
        // echoes the request; even with `tools` offered, no `tool_result` appears.
        let node = agent_node(json!({
            "agent_ref": "researcher",
            "prompt": "do it",
            "tools": [{ "slug": "web.search" }]
        }));
        let caps = mock_capabilities_with_agent(MockAgentRunner);
        let value = run_agent(&node, &caps).await;
        assert_eq!(value["json"]["agent"], "researcher");
        assert!(value["json"].get("tool_result").is_none());
    }

    #[tokio::test]
    async fn prose_completion_populates_text_accessor() {
        // A model that answers in prose: the envelope exposes it at `text` so a
        // downstream node can bind `=item.text` reliably regardless of provider.
        let node = agent_node(json!({ "prompt": "hi" }));
        let llm = Arc::new(ToolCallingLlm(json!({ "text": "the answer is 42" })));
        let value = run_agent(&node, &caps_with_llm(llm)).await;
        assert_eq!(value["text"], "the answer is 42");
        assert_eq!(value["json"]["text"], "the answer is 42");
        assert_eq!(value["raw"]["text"], "the answer is 42");
    }

    #[tokio::test]
    async fn plain_agent_without_sub_ports_is_unchanged() {
        // Back-compat: no tools / output_parser configured ⇒ the completion is
        // emitted verbatim (the mock echoes the request under `completion`).
        let node = agent_node(json!({ "prompt": "hi" }));
        let value = run_agent(&node, &mock_capabilities()).await;
        assert_eq!(value["json"]["completion"]["prompt"], "hi");
        assert!(value["json"].get("tool_result").is_none());
    }

    // ---- configurable agents: registry, merge, context, tools, stop reasons --

    mod configurable {
        use super::{agent_node, run_agent};
        use crate::caps::mock::{
            MockAgentHarness, MockLimitedAgentRunner, MockPausingAgentRunner,
            mock_capabilities_with_agent,
        };
        use crate::caps::{AgentRunner, Capabilities};
        use crate::data::Item;
        use crate::model::{
            AgentDefinition, AgentLimits, ContextSource, ContextSourceKind, ToolGrant,
        };
        use crate::nodes::{NodeContext, NodeExecutor};
        use serde_json::{Value, json};

        fn triager() -> AgentDefinition {
            AgentDefinition {
                id: "triager".into(),
                instructions: Some("Be terse.".into()),
                model: Some("sonnet".into()),
                provider: Some("anthropic".into()),
                working_dir: Some("/srv/checkout".into()),
                limits: AgentLimits {
                    max_steps: Some(8),
                    max_tool_calls: Some(20),
                    agent_timeout_secs: Some(300),
                    tool_timeout_secs: Some(30),
                },
                tools: vec![
                    ToolGrant::new("github.search"),
                    ToolGrant {
                        slug: "github.label".into(),
                        connection_ref: Some("conn_definition".into()),
                    },
                ],
                metadata: json!({ "tier": "fast" }).as_object().unwrap().clone(),
                ..Default::default()
            }
        }

        /// Runs an `agent` node against an in-graph registry and a typed harness.
        async fn run_with_registry(
            config: Value,
            agents: &[AgentDefinition],
            caps: &Capabilities,
        ) -> Value {
            let node = agent_node(config);
            let input = vec![Item::new(json!({ "seed": 1 }))];
            let run_meta = json!({ "run_id": "run_7", "sub_workflow_depth": 2 });
            let out = super::super::AgentNode
                .execute(NodeContext {
                    node: &node,
                    input: &input,
                    run: &run_meta,
                    nodes: &Value::Null,
                    caps,
                    agents,
                    observer: &crate::observability::NoopObserver,
                    token: crate::engine::CancellationToken::new(),
                    lane: None,
                    step: 0,
                })
                .await
                .expect("execute");
            out.items[0].json.clone()
        }

        #[tokio::test]
        async fn an_in_graph_definition_reaches_the_harness_merged_with_node_overrides() {
            let caps = mock_capabilities_with_agent(MockAgentHarness::new());
            let value = run_with_registry(
                json!({
                    "agent_ref": "triager",
                    "prompt": "Triage it.",
                    "instructions": "Prefer `bug`.",
                    "model": "opus",
                    "working_dir": "/srv/other",
                    "tools": [{ "slug": "github.search" }],
                    "limits": { "max_steps": 4 },
                    "metadata": { "extra": true }
                }),
                &[triager()],
                &caps,
            )
            .await;
            let echo = &value["json"];

            assert_eq!(echo["agent"], "triager");
            assert_eq!(
                echo["instructions"], "Be terse.\n\nPrefer `bug`.",
                "node instructions append to the definition's"
            );
            assert_eq!(echo["model"], "opus", "the node overrides the model");
            assert_eq!(
                echo["provider"], "anthropic",
                "an un-overridden provider survives from the definition"
            );
            assert_eq!(echo["working_dir"], "/srv/other");
            assert_eq!(echo["prompt"], "Triage it.");
            assert_eq!(
                echo["data"][0]["seed"], 1,
                "input items ride along structurally"
            );
            assert_eq!(echo["limits"]["max_steps"], 4, "the node tightened it");
            assert_eq!(
                echo["limits"]["max_tool_calls"], 20,
                "un-narrowed bound survives"
            );
            assert_eq!(echo["limits"]["tool_timeout_secs"], 30);
            assert_eq!(echo["limits"]["agent_timeout_secs"], 300);
            assert_eq!(echo["metadata"]["tier"], "fast");
            assert_eq!(echo["metadata"]["extra"], true);
            assert_eq!(
                echo["tools"],
                json!(["github.search"]),
                "the node narrowed the definition's two grants to one"
            );
            assert_eq!(echo["identity"]["node_id"], "n");
            assert_eq!(echo["identity"]["run_id"], "run_7");
            assert_eq!(echo["identity"]["depth"], 2);
            assert_eq!(value["meta"]["stop"], "finished");
            assert_eq!(value["meta"]["agent_ref"], "triager");
            assert_eq!(value["meta"]["usage"]["steps"], 1);
        }

        #[tokio::test]
        async fn the_in_graph_registry_wins_over_the_harnesss() {
            let host_side = AgentDefinition {
                id: "triager".into(),
                model: Some("host-model".into()),
                ..Default::default()
            };
            let caps = mock_capabilities_with_agent(MockAgentHarness::new().with(host_side));
            let value =
                run_with_registry(json!({ "agent_ref": "triager" }), &[triager()], &caps).await;
            assert_eq!(
                value["json"]["model"], "sonnet",
                "the graph's definition wins"
            );
        }

        #[tokio::test]
        async fn the_harness_registry_answers_when_the_graph_does_not() {
            let caps = mock_capabilities_with_agent(MockAgentHarness::new().with(triager()));
            let value = run_with_registry(json!({ "agent_ref": "triager" }), &[], &caps).await;
            assert_eq!(value["json"]["model"], "sonnet");
            assert_eq!(value["json"]["provider"], "anthropic");
        }

        #[tokio::test]
        async fn an_unknown_ref_passes_through_as_a_bare_id() {
            // Not an error: the harness may resolve refs internally, which is
            // exactly what it did before a registry existed.
            let caps = mock_capabilities_with_agent(MockAgentHarness::new());
            let value = run_with_registry(json!({ "agent_ref": "mystery" }), &[], &caps).await;
            assert_eq!(value["json"]["agent"], "mystery");
            assert!(value["json"]["model"].is_null());
        }

        #[tokio::test]
        async fn context_sources_resolve_in_declaration_order() {
            let mut agent = triager();
            agent.context = vec![
                ContextSource::new(ContextSourceKind::Host {
                    source: "soul".into(),
                    params: json!({ "k": "v" }),
                }),
                ContextSource::new(ContextSourceKind::Memory {
                    scope: "user".into(),
                    query: "preferences".into(),
                    limit: Some(3),
                }),
            ];
            let caps = mock_capabilities_with_agent(MockAgentHarness::new());
            let value = run_with_registry(
                json!({
                    "agent_ref": "triager",
                    "context": [
                        { "kind": "text", "label": "Body", "text": "=item.seed" },
                        { "kind": "items" }
                    ]
                }),
                &[agent],
                &caps,
            )
            .await;
            let blocks = value["json"]["context"].as_array().expect("context blocks");

            assert_eq!(blocks.len(), 4, "definition blocks first, then the node's");
            assert_eq!(blocks[0]["kind"], "host");
            assert_eq!(blocks[0]["data"]["k"], "v");
            assert_eq!(blocks[1]["kind"], "memory");
            assert_eq!(blocks[2]["label"], "Body");
            assert_eq!(
                blocks[2]["text"], "1",
                "the =expression resolved against the item"
            );
            assert_eq!(blocks[3]["kind"], "items");
            assert_eq!(blocks[3]["data"][0]["seed"], 1);
            assert_eq!(
                blocks[3]["label"], "context_3",
                "an unlabelled block is numbered by its position in the ASSEMBLED list, \
                 not within the node's own `context` array"
            );
        }

        #[tokio::test]
        async fn an_unresolvable_context_source_fails_the_node_unless_optional() {
            let caps = mock_capabilities_with_agent(MockAgentHarness::new());
            let node = agent_node(json!({
                "agent_ref": "triager",
                "context": [{ "kind": "host", "source": "unknown" }]
            }));
            let input: Vec<Item> = vec![];
            let run_meta = Value::Null;
            let err = super::super::AgentNode
                .execute(NodeContext {
                    node: &node,
                    input: &input,
                    run: &run_meta,
                    nodes: &Value::Null,
                    caps: &caps,
                    agents: &[],
                    observer: &crate::observability::NoopObserver,
                    token: crate::engine::CancellationToken::new(),
                    lane: None,
                    step: 0,
                })
                .await
                .expect_err("an unresolved required block must fail the node");
            let message = err.to_string();
            assert!(message.contains("could not be resolved"), "{message}");
            assert!(message.contains("optional"), "{message}");

            // ...and marking it optional makes it survivable.
            let value = run_with_registry(
                json!({
                    "agent_ref": "triager",
                    "context": [{ "kind": "host", "source": "unknown", "optional": true }]
                }),
                &[],
                &caps,
            )
            .await;
            assert_eq!(value["json"]["context"], json!([]));
        }

        #[tokio::test]
        async fn tool_grants_are_expanded_by_the_harness() {
            let mut agent = triager();
            agent.tools = vec![ToolGrant::new("github.*")];
            let caps = mock_capabilities_with_agent(MockAgentHarness::new());
            let value = run_with_registry(json!({ "agent_ref": "triager" }), &[agent], &caps).await;
            assert_eq!(
                value["json"]["tools"],
                json!(["github.alpha", "github.beta"]),
                "the harness expanded the namespace pattern"
            );
        }

        #[tokio::test]
        async fn a_limit_stop_is_visible_and_skips_the_output_parser() {
            let caps = mock_capabilities_with_agent(MockLimitedAgentRunner);
            let value = run_with_registry(
                json!({
                    "agent_ref": "triager",
                    // A schema the partial payload could never satisfy: if the
                    // parser ran, this would fail the node instead of emitting.
                    "output_parser": {
                        "schema": { "type": "object", "required": ["definitely_absent"] },
                        "auto_fix": false
                    }
                }),
                &[],
                &caps,
            )
            .await;
            assert_eq!(value["meta"]["stop"], "limit_stop");
            assert_eq!(value["meta"]["limit"], "max_steps");
            assert_eq!(
                value["json"]["partial"], true,
                "the partial payload is kept"
            );
            assert_eq!(value["text"], "got as far as I could");
        }

        #[tokio::test]
        async fn a_paused_agent_fails_loudly_rather_than_looking_finished() {
            let caps = mock_capabilities_with_agent(MockPausingAgentRunner);
            let node = agent_node(json!({ "agent_ref": "triager" }));
            let input: Vec<Item> = vec![];
            let run_meta = Value::Null;
            let err = super::super::AgentNode
                .execute(NodeContext {
                    node: &node,
                    input: &input,
                    run: &run_meta,
                    nodes: &Value::Null,
                    caps: &caps,
                    agents: &[],
                    observer: &crate::observability::NoopObserver,
                    token: crate::engine::CancellationToken::new(),
                    lane: None,
                    step: 0,
                })
                .await
                .expect_err("a pause must not be reported as a finished answer");
            let message = err.to_string();
            assert!(message.contains("paused"), "{message}");
            assert!(message.contains("tool_approval"), "{message}");
        }

        #[tokio::test]
        async fn declared_context_still_resolves_without_a_harness() {
            // No `AgentRunner` wired: the node degrades to a completion, but the
            // author's declared context must not be silently dropped.
            let node = agent_node(json!({
                "prompt": "hi",
                "context": [{ "kind": "memory", "scope": "user", "query": "prefs" }]
            }));
            let value = run_agent(&node, &crate::caps::mock::mock_capabilities()).await;
            let blocks = &value["json"]["completion"]["context"];
            assert_eq!(blocks[0]["source_kind"], "memory");
            assert!(
                blocks[0]["data"].get("results").is_some(),
                "the memory capability resolved the block: {blocks}"
            );
        }

        #[tokio::test]
        async fn a_legacy_host_receives_the_byte_identical_config_it_always_did() {
            // THE non-breaking guarantee, end to end: `MockAgentRunner`
            // implements only `run_agent`, so the default `run` shim applies and
            // the host sees exactly the (agent_ref, resolved config, conn) it
            // received before the typed seam existed.
            let caps = mock_capabilities_with_agent(crate::caps::mock::MockAgentRunner);
            let config = json!({
                "agent_ref": "researcher",
                "prompt": "hi",
                "connection_ref": "acct_9"
            });
            let value = run_agent(&agent_node(config.clone()), &caps).await;
            assert_eq!(value["raw"]["agent"], "researcher");
            assert_eq!(value["raw"]["request"], config);
            assert_eq!(value["raw"]["connection"], "acct_9");
            assert_eq!(value["meta"]["stop"], "finished");
        }

        #[tokio::test]
        async fn list_agents_exposes_the_harness_catalogue() {
            let harness = MockAgentHarness::new().with(triager());
            assert_eq!(harness.list_agents().await.unwrap().len(), 1);
        }
    }
}
