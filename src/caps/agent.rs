//! The `agent` node's host capability: running a host-owned agent.
//!
//! tinyflows implements **no agentic loop**. The model↔tool loop, the
//! transcript, the budget, and the prompt assembly all belong to the embedding
//! harness. This crate's contribution is the *assembly*: turning an author's
//! declarative [`AgentDefinition`], context sources, and tool grants into one
//! inert, fully-resolved [`AgentRunRequest`], and turning the harness's
//! [`AgentRunOutcome`] back into the stable `{ json, text, raw, meta }` item
//! envelope.
//!
//! ```text
//!   graph `agents` registry ─┐
//!   node config             ─┼─► merge ─► resolve context ─► resolve tools
//!   AgentRunner::resolve_*  ─┘                                     │
//!                                                                  ▼
//!                        AgentRunOutcome ◄── AgentRunner::run(AgentRunRequest)
//! ```
//!
//! ## Everything new here is additive
//!
//! [`AgentRunner::run_agent`] is unchanged and remains the trait's only required
//! method, so a host written against the previous release compiles and behaves
//! identically: the default [`run`](AgentRunner::run) replays exactly the call
//! it made before. A harness opts into richer agents by overriding the defaulted
//! methods, one at a time.
//!
//! ## Absence is not failure
//!
//! [`resolve_agent`](AgentRunner::resolve_agent) and
//! [`resolve_context`](AgentRunner::resolve_context) return `Ok(None)` for an
//! id they do not recognize. That is a **normal outcome**, not a soft error: a
//! host's catalogue is data and legitimately names things absent from a given
//! build, and the caller has a fallback. `Err` is reserved for a genuine failure
//! to *answer* — a broken store, a lost connection.
//!
//! Every value type in this module is `serde` + `std` only, so a host can write
//! an adapter against it without linking the engine.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::error::Result;
use crate::model::{AgentDefinition, ToolGrant};

/// Which model, on which provider, an agent run should use.
///
/// Both fields are opaque to the engine and both may be `None`, meaning "no
/// author preference — the harness's default applies". They are carried
/// separately rather than as one composite string so a harness routing the same
/// model through different providers need not parse them apart.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentModelSelection {
    /// Model id, e.g. `"claude-opus-5"` or a host alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Provider id, e.g. `"anthropic"`, a gateway name, or a host routing key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
}

impl AgentModelSelection {
    /// Whether the author expressed no preference at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.model.is_none() && self.provider.is_none()
    }
}

/// What the agent is being asked to do.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct AgentInput {
    /// The task text (the node's `prompt`), with `=`-expressions resolved.
    ///
    /// An empty string is legal: a run driven entirely by `data` and context is
    /// a real case.
    #[serde(default)]
    pub text: String,
    /// The `json` payload of each of the node's input items, in order.
    ///
    /// Carried alongside `text` rather than stringified into it, so a harness
    /// with structured input support need not parse prose back apart, and one
    /// without it can serialize this however it likes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub data: Vec<Value>,
}

/// One resolved block of agent context, ready to hand to the harness.
///
/// The engine resolves each declared
/// [`ContextSource`](crate::model::ContextSource) in declaration order and
/// passes the list through. It never reorders, merges, de-duplicates, ranks, or
/// truncates blocks — composing them into a prompt is the harness's job,
/// because the harness owns the loop.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ContextBlock {
    /// The authored label, or the generated `"context_<n>"`.
    pub label: String,
    /// The wire name of the source kind this came from (`"text"`, `"items"`,
    /// `"memory"`, `"flavour"`, `"host"`), so a harness can format a memory
    /// recall differently from literal text without re-deriving where it came
    /// from.
    pub source_kind: String,
    /// Human-readable body, when the source produced prose.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// Structured body, when the source produced data — a memory `results`
    /// array, a host record. [`Value::Null`] when there is none.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub data: Value,
}

impl ContextBlock {
    /// A prose block.
    #[must_use]
    pub fn text(label: impl Into<String>, source_kind: &str, text: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            source_kind: source_kind.to_string(),
            text: Some(text.into()),
            data: Value::Null,
        }
    }

    /// A structured block.
    #[must_use]
    pub fn data(label: impl Into<String>, source_kind: &str, data: Value) -> Self {
        Self {
            label: label.into(),
            source_kind: source_kind.to_string(),
            text: None,
            data,
        }
    }
}

/// A concrete tool the harness may offer the model this run.
///
/// The discovery half [`ToolInvoker`](crate::caps::ToolInvoker) never had: a
/// slug plus enough metadata for a model to choose it. Produced from an
/// author's [`ToolGrant`]s by
/// [`AgentRunner::resolve_tools`](AgentRunner::resolve_tools).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ToolDescriptor {
    /// The slug the harness would pass to
    /// [`ToolInvoker::invoke`](crate::caps::ToolInvoker::invoke).
    ///
    /// Concrete when a harness expanded a grant; a harness that does not expand
    /// receives the author's grant verbatim, patterns included.
    pub slug: String,
    /// Display name, when the harness knows one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// What the tool does, for the model's benefit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema for the tool's arguments. `None` means "undescribed", not
    /// "takes no arguments".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<Value>,
    /// The trusted connection reference this tool must be called with, carried
    /// down from the grant or the node. Never model-supplied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<String>,
}

impl ToolDescriptor {
    /// The undescribed descriptor a [`ToolGrant`] becomes when no harness
    /// expands it: the slug and its trusted connection reference, nothing more.
    #[must_use]
    pub fn from_grant(grant: &ToolGrant, fallback_conn: Option<&str>) -> Self {
        Self {
            slug: grant.slug.clone(),
            name: None,
            description: None,
            input_schema: None,
            connection_ref: grant
                .connection_ref
                .clone()
                .or_else(|| fallback_conn.map(str::to_string)),
        }
    }
}

/// Who is running, for host-side tracing, budgeting, and rate limiting.
///
/// Informational to the engine — nothing here is enforced, and a harness may
/// ignore all of it. It exists because a harness that cannot attribute an agent
/// run to a workflow run cannot bill it, trace it, or cap it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentRunIdentity {
    /// The enclosing workflow run's id, when the run has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// The workflow's id, when the graph declares one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    /// The graph id of the `agent` node issuing this request. Stable across
    /// renames, so it is the right key for a per-node budget.
    pub node_id: String,
    /// `sub_workflow` nesting depth, `0` at the top level. A harness budgeting
    /// recursive workflows needs this to tell a runaway chain from a wide one.
    #[serde(default)]
    pub depth: u32,
    /// Index of the input item this run is for under `per_item` execution;
    /// `None` for a single `once` turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_index: Option<usize>,
}

/// One `agent` node's run request: everything the harness needs, fully
/// resolved.
///
/// By the time a harness sees one, every `=`-expression is evaluated, every
/// context block is materialized, and every tool grant has been offered for
/// expansion. The harness resolves nothing further except its own credentials.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunRequest {
    /// The merged, fully-resolved agent type: id, instructions, limits, and the
    /// grants and context sources it was defined with.
    pub agent: AgentDefinition,
    /// Which model, on which provider.
    #[serde(default)]
    pub model: AgentModelSelection,
    /// What this run is being asked to do.
    #[serde(default)]
    pub input: AgentInput,
    /// Resolved context, in declaration order — the definition's blocks first,
    /// then the node's. Empty is normal.
    #[serde(default)]
    pub context: Vec<ContextBlock>,
    /// The tools this run may use, already narrowed by any node-level
    /// allow-list.
    ///
    /// An empty vec means **no tools**, not "harness default"; a harness must
    /// not widen it.
    #[serde(default)]
    pub tools: Vec<ToolDescriptor>,
    /// The opaque, host-managed connection reference naming the account this
    /// run acts as.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_ref: Option<String>,
    /// Working directory the agent should run in, when it runs somewhere with a
    /// filesystem. `None` means the harness's default applies.
    ///
    /// Surfaced here as well as on
    /// [`agent.working_dir`](crate::model::AgentDefinition::working_dir)
    /// because a harness reaching for it should not have to know whether the
    /// definition or the node supplied it — this is the merged, effective value.
    ///
    /// **Untrusted authoring input**: the engine never touches the filesystem
    /// and never checks that this exists or is permitted. Validate it against
    /// your own allowed roots before handing it to a process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// Run identity, for host-side tracing and budgeting.
    #[serde(default)]
    pub identity: AgentRunIdentity,
    /// Free-form passthrough: the agent definition's `metadata` with the node's
    /// merged over it. The engine never interprets it.
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub metadata: Map<String, Value>,
    /// The schema the node's `output_parser` sub-port will validate against.
    ///
    /// **Advisory.** The engine still validates and repairs the outcome itself,
    /// so a harness may ignore this. It is forwarded because a harness with
    /// native structured output gets the right shape on the first try instead of
    /// paying for a repair round-trip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// The node's resolved config, verbatim.
    ///
    /// This is what the default [`AgentRunner::run`] forwards to
    /// [`run_agent`](AgentRunner::run_agent), which is how a host written
    /// against the previous release keeps receiving byte-identical input. It is
    /// also the escape hatch for any config key this struct does not model.
    #[serde(default)]
    pub config: Value,
}

/// Why a host-owned agent loop stopped.
///
/// The reason a typed outcome is worth having. With a bare `Value` return, an
/// agent that finished, one that stopped a step short of the answer, and one
/// waiting on a human are all indistinguishable — and the workflow marches
/// downstream with a partial answer in every case. Keeping the stop reason out
/// of the `Result` channel (rather than reporting a limit or a pause as an
/// error) is the same split [`ShellRunner`](crate::caps::ShellRunner) makes by
/// reporting a non-zero exit through `exit_code` instead of `Err`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "stop", rename_all = "snake_case")]
pub enum StopReason {
    /// The agent produced a final answer. The only reason a downstream node
    /// should treat the outcome as complete.
    Finished,
    /// The loop hit a cap and stopped cleanly, keeping what it produced. The
    /// outcome is **partial**: real, usable, and not the whole answer.
    LimitStop {
        /// Host-defined name of the cap that fired (`"max_steps"`,
        /// `"token_budget"`, `"wall_clock"`).
        ///
        /// A free string, not an enum: the engine branches on *whether* a limit
        /// fired, never on which, and an enum here would mint a taxonomy this
        /// crate cannot keep current with any harness's budget model.
        limit: String,
    },
    /// The loop latched a pause and is **resumable, not finished** — the
    /// harness still holds the transcript.
    ///
    /// The engine does not yet route a pause into its checkpoint/resume
    /// machinery: an `agent` node that receives this fails with a clear
    /// [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// naming the node and reason. The variant exists now so a harness can
    /// never *conflate* a pause with a finish, and so no wire type has to
    /// change when resume support lands.
    Paused {
        /// Opaque host handle for the paused run — a session id, a checkpoint
        /// key — which the harness will need echoed back to resume it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        /// Host-defined reason (`"tool_approval"`, `"clarifying_question"`),
        /// surfaced to whoever is being asked.
        reason: String,
        /// What the human must decide on. Opaque to the engine.
        #[serde(default, skip_serializing_if = "Value::is_null")]
        payload: Value,
    },
}

impl StopReason {
    /// The `snake_case` wire name of this reason, as it appears on the item
    /// envelope's `meta.stop`.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Finished => "finished",
            Self::LimitStop { .. } => "limit_stop",
            Self::Paused { .. } => "paused",
        }
    }
}

/// Token and step accounting for one agent run, as the harness reports it.
///
/// The engine neither aggregates nor enforces these; it forwards them onto the
/// item envelope's `meta.usage` for cost-aware workflows to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct AgentUsage {
    /// Prompt tokens consumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    /// Completion tokens produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    /// Model↔tool iterations the loop performed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub steps: Option<u32>,
    /// Tool invocations the loop performed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<u32>,
}

/// What a host-owned agent run produced.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentRunOutcome {
    /// Why the loop stopped. **Read this before the payload.**
    pub stop: StopReason,
    /// The agent's prose answer, when it produced one. Lands at the item
    /// envelope's `text`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    /// The structured result, when the agent produced one. Lands at the
    /// envelope's `json`, after the `output_parser` sub-port if one is
    /// configured. [`Value::Null`] when the agent answered only in prose.
    #[serde(default)]
    pub json: Value,
    /// The harness's native payload, verbatim. Lands at the envelope's `raw`,
    /// and is the escape hatch for anything this struct does not model.
    #[serde(default)]
    pub raw: Value,
    /// Optional usage figures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<AgentUsage>,
}

impl AgentRunOutcome {
    /// A [`Finished`](StopReason::Finished) outcome built from a harness's
    /// native `value`, deriving `json` and `text` the way the engine's envelope
    /// does: `json` is the value when it is an object or array, and `text` is
    /// the value when it is a string, else its `text` field.
    ///
    /// This is what the default [`AgentRunner::run`] wraps a legacy
    /// [`run_agent`](AgentRunner::run_agent) return in, and the shorthand a
    /// simple adapter wants.
    ///
    /// ```
    /// use tinyflows::caps::{AgentRunOutcome, StopReason};
    /// use serde_json::json;
    ///
    /// let outcome = AgentRunOutcome::finished(json!({ "text": "done", "n": 1 }));
    /// assert_eq!(outcome.stop, StopReason::Finished);
    /// assert_eq!(outcome.text.as_deref(), Some("done"));
    /// assert!(outcome.is_finished());
    /// ```
    #[must_use]
    pub fn finished(value: Value) -> Self {
        let text = match &value {
            Value::String(s) => Some(s.clone()),
            Value::Object(map) => map.get("text").and_then(Value::as_str).map(str::to_string),
            _ => None,
        };
        let json = match &value {
            Value::Object(_) | Value::Array(_) => value.clone(),
            _ => Value::Null,
        };
        Self {
            stop: StopReason::Finished,
            text,
            json,
            raw: value,
            usage: None,
        }
    }

    /// Whether the agent reached a final answer.
    ///
    /// Anything else means the payload is partial or absent — see
    /// [`StopReason`].
    #[must_use]
    pub fn is_finished(&self) -> bool {
        matches!(self.stop, StopReason::Finished)
    }
}

/// Runs a host-registered, multi-turn **agent** — the harness seam.
///
/// Where [`LlmProvider::complete`](crate::caps::LlmProvider::complete) is a
/// single chat call, an `AgentRunner` runs a *named agent kind* the host has
/// defined — a coding agent, a researcher, a crypto agent — each bringing its
/// own curated tool access, model, sandbox, and iteration policy. An `agent`
/// node selects one via a trusted `agent_ref` in its config; when the host wires
/// this capability, the node runs that agent to completion instead of issuing a
/// bare completion.
///
/// The engine stays host-agnostic: `agent_ref` is an opaque id the host resolves
/// against its own agent registry (or that the graph's own
/// [`agents`](crate::model::WorkflowGraph::agents) registry answers first). This
/// capability is **optional** — hosts without an agent registry leave
/// [`Capabilities::agent`](crate::caps::Capabilities::agent) `None`, and `agent`
/// nodes fall back to [`LlmProvider`](crate::caps::LlmProvider).
///
/// # Implementing it
///
/// Only [`run_agent`](Self::run_agent) is required, and it is unchanged from the
/// previous release. Every other method has a default that degrades to the
/// behavior a host had before richer agents existed, so an existing
/// implementation keeps compiling and keeps behaving identically. Override them
/// in roughly this order of value:
///
/// 1. [`run`](Self::run) — take the typed request, and report a real
///    [`StopReason`]. The default reports [`Finished`](StopReason::Finished) for
///    every run, which is exactly the conflation this trait exists to remove, so
///    a harness with a real loop should not leave it defaulted.
/// 2. [`resolve_agent`](Self::resolve_agent) — expose the host's own agent
///    catalogue to graphs.
/// 3. [`resolve_context`](Self::resolve_context) — expand `host` context
///    sources.
/// 4. [`resolve_tools`](Self::resolve_tools) — describe tools and expand
///    namespace patterns.
#[async_trait]
pub trait AgentRunner: Send + Sync {
    /// Runs the host-registered agent identified by `agent_ref` to completion.
    ///
    /// `request` is the (expression-resolved) node config — prompt/input plus
    /// any per-node overrides the host chooses to honor. `conn` is the same
    /// opaque, host-managed connection reference the other capabilities take.
    /// The returned JSON is treated exactly like a completion response by the
    /// `agent` node (enveloped into `{ json, text, raw }`), so downstream
    /// binding is identical to a plain agent turn.
    ///
    /// # Errors
    /// Returns an [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// when `agent_ref` is unknown or the agent run fails.
    async fn run_agent(&self, agent_ref: &str, request: Value, conn: Option<&str>)
    -> Result<Value>;

    /// Runs a fully-assembled [`AgentRunRequest`] until the loop finishes, hits
    /// a limit, or pauses.
    ///
    /// A limit stop and a pause are **not errors** — they are reported through
    /// [`AgentRunOutcome::stop`], so a harness's own failures (an unreachable
    /// model, a rejected credential) stay distinguishable from a loop that ran
    /// and deliberately stopped.
    ///
    /// The default implementation forwards
    /// [`request.config`](AgentRunRequest::config) — the node's resolved config,
    /// verbatim — to [`run_agent`](Self::run_agent) and reports the result as
    /// [`Finished`](StopReason::Finished). That is byte-for-byte the call a host
    /// received before this method existed, so leaving it defaulted preserves
    /// existing behavior exactly. It does mean the host cannot signal a partial
    /// or paused run, which is why a harness with a real loop should override
    /// it.
    ///
    /// # Errors
    /// Returns an [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// when the harness cannot run the request at all.
    async fn run(&self, request: AgentRunRequest) -> Result<AgentRunOutcome> {
        let value = self
            .run_agent(
                &request.agent.id,
                request.config.clone(),
                request.connection_ref.as_deref(),
            )
            .await?;
        Ok(AgentRunOutcome::finished(value))
    }

    /// Resolves `agent_ref` against the host's own agent catalogue.
    ///
    /// The **fallback** half of agent-kind resolution: a graph's own
    /// [`agents`](crate::model::WorkflowGraph::agents) registry is consulted
    /// first and wins on a collision, so a workflow stays portable between
    /// hosts. This method is how a harness adds curated kinds no graph should
    /// have to inline.
    ///
    /// Read-only on purpose. A directory the engine could write to would let a
    /// running workflow mint itself an agent with tools it was never granted.
    ///
    /// **`Ok(None)` for an unknown ref is a normal outcome, not a soft error** —
    /// the node then passes the ref through to [`run`](Self::run) as a bare
    /// `AgentDefinition { id: agent_ref, .. }`, which is what a harness that
    /// resolves refs internally wants. The default returns `Ok(None)` for
    /// everything, which is exactly that pass-through behavior.
    ///
    /// # Errors
    /// Returns an [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// only on a genuine failure to answer — a broken store, a lost connection.
    async fn resolve_agent(&self, agent_ref: &str) -> Result<Option<AgentDefinition>> {
        let _ = agent_ref;
        Ok(None)
    }

    /// Every agent kind the harness offers, for an editor's picker and for
    /// authoring surfaces. Order should be stable across calls.
    ///
    /// Not on the run path — nothing in [`crate::engine`] calls this.
    ///
    /// # Errors
    /// Returns an [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// when the catalogue cannot be listed.
    async fn list_agents(&self) -> Result<Vec<AgentDefinition>> {
        Ok(Vec::new())
    }

    /// Resolves an opaque
    /// [`ContextSourceKind::Host`](crate::model::ContextSourceKind::Host)
    /// source into a [`ContextBlock`] — identity/soul text, a description of the
    /// integrations this user has connected, a RAG index, learned context.
    ///
    /// This is deliberately *not* a prompt composer. The harness owns the loop
    /// and therefore owns the system prompt; a second prompt-composition seam
    /// here would duplicate or fight the harness's own. All this does is turn an
    /// author-declared source name into a block that rides along in the request.
    ///
    /// It also does not overlap [`MemoryProvider`](crate::caps::MemoryProvider):
    /// `memory` and `flavour` context sources route to that existing capability
    /// unchanged, so the memory `scope` contract keeps exactly one home.
    ///
    /// **`Ok(None)` for an unrecognized `source` is a normal outcome**; the node
    /// then applies the block's `optional` policy, failing unless the author
    /// marked the source optional. Returning `Ok(Some(_))` with an empty block
    /// is different and also normal: the source resolved and has nothing to
    /// contribute this run.
    ///
    /// # Errors
    /// Returns an [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// only on a genuine failure to answer.
    async fn resolve_context(
        &self,
        source: &str,
        params: &Value,
        identity: &AgentRunIdentity,
    ) -> Result<Option<ContextBlock>> {
        let _ = (source, params, identity);
        Ok(None)
    }

    /// Expands an agent's [`ToolGrant`]s into concrete [`ToolDescriptor`]s.
    ///
    /// The discovery half [`ToolInvoker`](crate::caps::ToolInvoker) lacks:
    /// `invoke(slug, args, conn)` can execute a tool but cannot say which tools
    /// exist, what they take, or what `"github.*"` expands to.
    ///
    /// `conn` is the node's connection reference, so a harness whose visible
    /// tool set depends on the account can narrow accordingly. A grant matching
    /// nothing contributes nothing — that is not an error, for the same
    /// build-variance reason an unknown agent id is not.
    ///
    /// The default returns one undescribed [`ToolDescriptor`] per grant, with
    /// patterns forwarded verbatim — appropriate for a harness that already
    /// knows its own tools and only needs to be told which ones are permitted.
    ///
    /// # Errors
    /// Returns an [`EngineError::Capability`](crate::error::EngineError::Capability)
    /// when the catalogue cannot be consulted.
    async fn resolve_tools(
        &self,
        grants: &[ToolGrant],
        conn: Option<&str>,
    ) -> Result<Vec<ToolDescriptor>> {
        Ok(grants
            .iter()
            .map(|grant| ToolDescriptor::from_grant(grant, conn))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A host written against the previous release: one method, JSON in and out.
    struct LegacyRunner;

    #[async_trait]
    impl AgentRunner for LegacyRunner {
        async fn run_agent(
            &self,
            agent_ref: &str,
            request: Value,
            conn: Option<&str>,
        ) -> Result<Value> {
            Ok(json!({ "agent": agent_ref, "request": request, "connection": conn }))
        }
    }

    fn request(config: Value) -> AgentRunRequest {
        AgentRunRequest {
            agent: AgentDefinition::new("researcher"),
            model: AgentModelSelection::default(),
            input: AgentInput::default(),
            context: Vec::new(),
            tools: Vec::new(),
            connection_ref: Some("conn_1".to_string()),
            working_dir: None,
            identity: AgentRunIdentity::default(),
            metadata: Map::new(),
            output_schema: None,
            config,
        }
    }

    #[tokio::test]
    async fn the_default_run_forwards_the_config_verbatim_to_run_agent() {
        // The non-breaking guarantee: a host that implements only `run_agent`
        // receives exactly the (agent_ref, config, conn) triple it always did.
        let config = json!({ "prompt": "hi", "agent_ref": "researcher" });
        let outcome = LegacyRunner.run(request(config.clone())).await.unwrap();

        assert_eq!(
            outcome.raw,
            json!({ "agent": "researcher", "request": config, "connection": "conn_1" })
        );
        assert_eq!(outcome.stop, StopReason::Finished);
    }

    #[tokio::test]
    async fn the_default_resolvers_report_absence_not_failure() {
        assert!(
            LegacyRunner
                .resolve_agent("anything")
                .await
                .unwrap()
                .is_none()
        );
        assert!(LegacyRunner.list_agents().await.unwrap().is_empty());
        assert!(
            LegacyRunner
                .resolve_context("soul", &Value::Null, &AgentRunIdentity::default())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn the_default_tool_resolver_passes_grants_through_undescribed() {
        let grants = vec![
            ToolGrant::new("github.*"),
            ToolGrant {
                slug: "slack.post".to_string(),
                connection_ref: Some("acct_slack".to_string()),
            },
        ];
        let tools = LegacyRunner
            .resolve_tools(&grants, Some("node_conn"))
            .await
            .unwrap();

        assert_eq!(tools[0].slug, "github.*", "patterns are forwarded verbatim");
        assert_eq!(
            tools[0].connection_ref.as_deref(),
            Some("node_conn"),
            "a grant without its own connection falls back to the node's"
        );
        assert_eq!(
            tools[1].connection_ref.as_deref(),
            Some("acct_slack"),
            "a grant's own connection wins"
        );
        assert!(tools[0].input_schema.is_none());
    }

    #[test]
    fn finished_derives_text_and_json_like_the_envelope() {
        let from_object = AgentRunOutcome::finished(json!({ "text": "done", "n": 1 }));
        assert_eq!(from_object.text.as_deref(), Some("done"));
        assert_eq!(from_object.json, json!({ "text": "done", "n": 1 }));

        let from_string = AgentRunOutcome::finished(json!("just prose"));
        assert_eq!(from_string.text.as_deref(), Some("just prose"));
        assert_eq!(
            from_string.json,
            Value::Null,
            "a scalar carries no structure"
        );
    }

    #[test]
    fn stop_reasons_have_stable_wire_names() {
        assert_eq!(StopReason::Finished.as_str(), "finished");
        assert_eq!(
            StopReason::LimitStop {
                limit: "max_steps".into()
            }
            .as_str(),
            "limit_stop"
        );
        assert_eq!(
            serde_json::to_value(StopReason::LimitStop {
                limit: "max_steps".into()
            })
            .unwrap(),
            json!({ "stop": "limit_stop", "limit": "max_steps" })
        );
    }
}
