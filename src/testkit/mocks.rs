//! Programmable, recording capability doubles.
//!
//! [`crate::caps::mock`] answers every call with the same canned echo. That is
//! enough to prove a graph *executes* and no help at all in proving one is
//! *correct*: there is no way to say "the third call fails", no way to make two
//! tools answer differently, and no way to ask afterwards what a capability was
//! actually handed. Every test that needed any of that wrote its own
//! `AtomicUsize`-counting impl, and there are more than a dozen such
//! one-offs in this repo's own suite.
//!
//! So: rules in, a call log out.
//!
//! ```no_run
//! use std::sync::Arc;
//! use tinyflows::testkit::{MockCaps, Respond};
//! use serde_json::json;
//!
//! // Shared behind an `Arc`, because each node activation is handed its own
//! // bundle over the same rules and the same log.
//! let mocks = Arc::new(MockCaps::new()
//!     .on_tool("slack.send", Respond::value(json!({ "ok": true })))
//!     // First call rate-limits, the retry succeeds — the shape of a flaky
//!     // dependency, without a flaky test.
//!     .on_tool(
//!         "gh.issues.*",
//!         Respond::sequence([
//!             Respond::error("429 rate limited"),
//!             Respond::value(json!({ "number": 7 })),
//!         ]),
//!     ));
//! let capabilities = mocks.capabilities();
//! ```
//!
//! Matching is first-rule-wins in declaration order, so a specific rule written
//! before a general one shadows it. A call matching no rule falls back to the
//! bundle's default behaviour, which is the same echo `caps::mock` gives — a
//! graph under test never fails because a capability was left unprogrammed.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::caps::{
    AgentRunner, Capabilities, CodeLanguage, CodeRunner, HttpClient, LlmProvider, MemoryProvider,
    ShellOutcome, ShellRequest, ShellRunner, StateStore, ToolInvoker, WorkflowResolver,
    sample_for_schema,
};
use crate::error::{EngineError, Result};
use crate::model::WorkflowGraph;

/// Which capability a call went to.
///
/// A plain string rather than an enum on the wire, so a recording written by a
/// build that knows a capability this one does not still parses.
pub mod capability {
    /// [`LlmProvider`](crate::caps::LlmProvider).
    pub const LLM: &str = "llm";
    /// [`ToolInvoker`](crate::caps::ToolInvoker).
    pub const TOOLS: &str = "tools";
    /// [`HttpClient`](crate::caps::HttpClient).
    pub const HTTP: &str = "http";
    /// [`CodeRunner`](crate::caps::CodeRunner).
    pub const CODE: &str = "code";
    /// [`ShellRunner`](crate::caps::ShellRunner).
    pub const SHELL: &str = "shell";
    /// [`AgentRunner`](crate::caps::AgentRunner).
    pub const AGENT: &str = "agent";
    /// [`MemoryProvider`](crate::caps::MemoryProvider).
    pub const MEMORY: &str = "memory";
    /// [`StateStore`](crate::caps::StateStore).
    pub const STATE: &str = "state";
}

/// How one capability call ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CallOutcome {
    /// The call returned a value.
    Ok(Value),
    /// The call failed, with this message.
    Err(String),
}

/// One capability call a run made.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapCall {
    /// Position in the run's single call sequence, from 0.
    ///
    /// One counter across *all* capabilities, so the log says what order things
    /// happened in — which per-capability counters cannot.
    pub seq: u64,
    /// Which capability — see the [`capability`] constants.
    pub capability: String,
    /// The trait method (`invoke`, `complete`, `request`, …).
    pub method: String,
    /// The node that made the call.
    ///
    /// `None` only when the call was made outside a node activation, which no
    /// engine path does today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// What identifies the target within the capability: a tool slug, an agent
    /// ref, an HTTP method and URL, a state key. Empty when the capability has
    /// no such notion.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub target: String,
    /// The arguments the call was made with.
    pub args: Value,
    /// What it returned.
    pub outcome: CallOutcome,
}

/// Every capability call a run made, in order.
///
/// Shared by every double in one [`MockCaps`], so the ordering across
/// capabilities is real rather than assembled afterwards from separate logs.
#[derive(Debug, Default)]
pub struct CallLog {
    calls: Mutex<Vec<CapCall>>,
    next_seq: AtomicU64,
}

impl CallLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a call, assigning it the next sequence number.
    fn record(
        &self,
        capability: &str,
        method: &str,
        node_id: Option<String>,
        target: String,
        args: Value,
        outcome: CallOutcome,
    ) {
        let call = CapCall {
            seq: self.next_seq.fetch_add(1, Ordering::SeqCst),
            capability: capability.to_string(),
            method: method.to_string(),
            node_id,
            target,
            args,
            outcome,
        };
        self.calls.lock().expect("call log poisoned").push(call);
    }

    /// Every call recorded so far, in sequence order.
    #[must_use]
    pub fn calls(&self) -> Vec<CapCall> {
        let mut calls = self.calls.lock().expect("call log poisoned").clone();
        calls.sort_by_key(|call| call.seq);
        calls
    }

    /// The calls matching a capability and an optional target glob.
    ///
    /// `capability` is one of the [`capability`] constants; `target` accepts the
    /// same `*` globbing the rules do, and `None` matches every target.
    #[must_use]
    pub fn matching(&self, capability: &str, target: Option<&str>) -> Vec<CapCall> {
        self.calls()
            .into_iter()
            .filter(|call| call.capability == capability)
            .filter(|call| target.is_none_or(|glob| glob_matches(glob, &call.target)))
            .collect()
    }

    /// How many calls match — the count an assertion usually wants.
    #[must_use]
    pub fn count(&self, capability: &str, target: Option<&str>) -> usize {
        self.matching(capability, target).len()
    }
}

/// What a matched rule answers with.
///
/// Construct these through the helpers ([`Respond::value`], [`Respond::error`],
/// …) rather than the variants directly; the variants are public so a host can
/// match on a loaded recording.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Respond {
    /// Return this value.
    Value(Value),
    /// Fail with this message, as an
    /// [`EngineError::Capability`](crate::error::EngineError::Capability).
    Error(String),
    /// Answer the nth matching call with the nth entry.
    ///
    /// Calls past the end repeat the last entry, so a sequence written for the
    /// first two calls does not start failing on the third for a reason the
    /// author never intended. An empty sequence falls through to the default.
    Sequence(Vec<Respond>),
    /// Wait, then answer. For exercising timeouts and genuine concurrency.
    Delay(Duration, Box<Respond>),
    /// Synthesize a value satisfying this JSON Schema.
    ///
    /// The auto-mock: a node declaring an `output_parser.schema` gets something
    /// that shape, so it does not fail validation for a reason unrelated to the
    /// graph. See [`sample_for_schema`].
    Schema(Value),
    /// Echo the request back, exactly as [`crate::caps::mock`] does.
    Echo,
}

impl Respond {
    /// Return `value`.
    #[must_use]
    pub fn value(value: Value) -> Self {
        Self::Value(value)
    }

    /// Fail with `message`.
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error(message.into())
    }

    /// Answer successive matching calls with successive entries.
    #[must_use]
    pub fn sequence(entries: impl IntoIterator<Item = Respond>) -> Self {
        Self::Sequence(entries.into_iter().collect())
    }

    /// Wait `delay`, then answer with `then`.
    #[must_use]
    pub fn after(delay: Duration, then: Respond) -> Self {
        Self::Delay(delay, Box::new(then))
    }

    /// Synthesize a value satisfying `schema`.
    #[must_use]
    pub fn schema(schema: Value) -> Self {
        Self::Schema(schema)
    }

    /// Resolve to a concrete answer for the `hit`-th matching call.
    ///
    /// `async` because [`Respond::Delay`] genuinely waits; every other variant
    /// resolves immediately.
    async fn answer(&self, hit: usize, request: &Value) -> Result<Value> {
        match self {
            Self::Value(value) => Ok(value.clone()),
            Self::Error(message) => Err(EngineError::Capability(message.clone())),
            Self::Sequence(entries) => match entries.last() {
                // Past the end, repeat the last entry rather than falling off
                // into a different behaviour the author never wrote.
                Some(last) => {
                    let entry = entries.get(hit).unwrap_or(last);
                    Box::pin(entry.answer(hit, request)).await
                }
                None => Ok(request.clone()),
            },
            Self::Delay(delay, then) => {
                futures_timer::Delay::new(*delay).await;
                Box::pin(then.answer(hit, request)).await
            }
            Self::Schema(schema) => Ok(sample_for_schema(schema)),
            Self::Echo => Ok(request.clone()),
        }
    }
}

/// Which calls a rule applies to.
#[derive(Debug, Clone)]
struct Matcher {
    capability: String,
    /// A glob over the call's target. `*` matches any run of characters.
    target: String,
    /// When set, the rule only applies to calls made by this node.
    node_id: Option<String>,
}

impl Matcher {
    fn matches(&self, capability: &str, target: &str, node_id: Option<&str>) -> bool {
        self.capability == capability
            && glob_matches(&self.target, target)
            && match self.node_id.as_deref() {
                Some(wanted) => node_id == Some(wanted),
                None => true,
            }
    }
}

/// Whether `glob` matches `value`, where `*` matches any run of characters.
///
/// Deliberately just `*`: tool slugs and URLs are the things being matched, and
/// a full regex dependency to express `gh.issues.*` would be a poor trade.
fn glob_matches(glob: &str, value: &str) -> bool {
    if glob == "*" {
        return true;
    }
    if !glob.contains('*') {
        return glob == value;
    }
    let parts: Vec<&str> = glob.split('*').collect();
    let mut rest = value;
    for (index, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        match index {
            // A leading literal must sit at the start.
            0 => match rest.strip_prefix(part) {
                Some(tail) => rest = tail,
                None => return false,
            },
            _ => match rest.find(part) {
                Some(at) => rest = &rest[at + part.len()..],
                None => return false,
            },
        }
    }
    // A glob with no trailing `*` must have consumed everything.
    parts
        .last()
        .is_none_or(|last| last.is_empty() || rest.is_empty())
}

/// One programmed rule and how many times it has answered.
#[derive(Debug)]
struct Rule {
    matcher: Matcher,
    respond: Respond,
    hits: AtomicU64,
}

/// A programmable, recording set of capability doubles.
///
/// Build one with the `on_*` methods, hand [`capabilities`](Self::capabilities)
/// to a run, then read [`log`](Self::log) afterwards.
#[derive(Debug, Default)]
pub struct MockCaps {
    rules: Vec<Rule>,
    log: Arc<CallLog>,
    workflows: HashMap<String, WorkflowGraph>,
}

impl MockCaps {
    /// A set of doubles with no rules: every call falls back to the echo
    /// behaviour, and every call is logged.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The call log these doubles write to.
    #[must_use]
    pub fn log(&self) -> Arc<CallLog> {
        self.log.clone()
    }

    /// Program a rule.
    #[must_use]
    fn rule(mut self, capability: &str, target: &str, respond: Respond) -> Self {
        self.rules.push(Rule {
            matcher: Matcher {
                capability: capability.to_string(),
                target: target.to_string(),
                node_id: None,
            },
            respond,
            hits: AtomicU64::new(0),
        });
        self
    }

    /// Answer `slug` (a glob) on the tool invoker.
    #[must_use]
    pub fn on_tool(self, slug: &str, respond: Respond) -> Self {
        self.rule(capability::TOOLS, slug, respond)
    }

    /// Answer HTTP requests whose URL matches `url` (a glob).
    #[must_use]
    pub fn on_http(self, url: &str, respond: Respond) -> Self {
        self.rule(capability::HTTP, url, respond)
    }

    /// Answer LLM completions.
    #[must_use]
    pub fn on_llm(self, respond: Respond) -> Self {
        self.rule(capability::LLM, "*", respond)
    }

    /// Answer the agent runner for `agent_ref` (a glob).
    #[must_use]
    pub fn on_agent(self, agent_ref: &str, respond: Respond) -> Self {
        self.rule(capability::AGENT, agent_ref, respond)
    }

    /// Answer code execution.
    #[must_use]
    pub fn on_code(self, respond: Respond) -> Self {
        self.rule(capability::CODE, "*", respond)
    }

    /// Answer shell execution.
    #[must_use]
    pub fn on_shell(self, respond: Respond) -> Self {
        self.rule(capability::SHELL, "*", respond)
    }

    /// Restrict the most recently programmed rule to calls made by `node_id`.
    ///
    /// This is what per-node mocking looks like: stub one node's tool calls and
    /// leave the rest of the graph alone.
    ///
    /// A no-op when no rule has been programmed yet.
    #[must_use]
    pub fn only_from(mut self, node_id: &str) -> Self {
        if let Some(rule) = self.rules.last_mut() {
            rule.matcher.node_id = Some(node_id.to_string());
        }
        self
    }

    /// Register a graph a `sub_workflow` node can resolve by id.
    #[must_use]
    pub fn with_workflow(mut self, id: impl Into<String>, graph: WorkflowGraph) -> Self {
        self.workflows.insert(id.into(), graph);
        self
    }

    /// Find the answer for a call, or `None` to fall back to the default.
    async fn respond_to(
        &self,
        capability: &str,
        target: &str,
        node_id: Option<&str>,
        request: &Value,
    ) -> Option<Result<Value>> {
        for rule in &self.rules {
            if rule.matcher.matches(capability, target, node_id) {
                let hit = rule.hits.fetch_add(1, Ordering::SeqCst) as usize;
                return Some(rule.respond.answer(hit, request).await);
            }
        }
        None
    }

    /// The capability bundle to hand a run.
    ///
    /// Every slot is filled, so a graph reaching a capability nobody programmed
    /// gets the echo rather than a "capability not configured" failure that
    /// would say nothing about the graph.
    #[must_use]
    pub fn capabilities(self: &Arc<Self>) -> Capabilities {
        let shared = self.clone();
        Capabilities {
            llm: Arc::new(Double::new(shared.clone(), None)),
            tools: Arc::new(Double::new(shared.clone(), None)),
            http: Arc::new(Double::new(shared.clone(), None)),
            code: Arc::new(Double::new(shared.clone(), None)),
            state: Arc::new(Double::new(shared.clone(), None)),
            resolver: Arc::new(Double::new(shared.clone(), None)),
            agent: Some(Arc::new(Double::new(shared.clone(), None))),
            shell: Some(Arc::new(Double::new(shared.clone(), None))),
            memory: Some(Arc::new(Double::new(shared.clone(), None))),
            tasks: Some(Arc::new(crate::caps::TokioTaskRunner::new())),
        }
    }

    /// The same bundle, with every call it makes attributed to `node_id`.
    ///
    /// Used through
    /// [`StepInterceptor::capabilities_for`](crate::interception::StepInterceptor::capabilities_for)
    /// so the log can say which node made which call.
    #[must_use]
    pub fn capabilities_for_node(self: &Arc<Self>, node_id: &str) -> Capabilities {
        let shared = self.clone();
        let node = Some(node_id.to_string());
        Capabilities {
            llm: Arc::new(Double::new(shared.clone(), node.clone())),
            tools: Arc::new(Double::new(shared.clone(), node.clone())),
            http: Arc::new(Double::new(shared.clone(), node.clone())),
            code: Arc::new(Double::new(shared.clone(), node.clone())),
            state: Arc::new(Double::new(shared.clone(), node.clone())),
            resolver: Arc::new(Double::new(shared.clone(), node.clone())),
            agent: Some(Arc::new(Double::new(shared.clone(), node.clone()))),
            shell: Some(Arc::new(Double::new(shared.clone(), node.clone()))),
            memory: Some(Arc::new(Double::new(shared.clone(), node))),
            tasks: Some(Arc::new(crate::caps::TokioTaskRunner::new())),
        }
    }
}

/// One capability double: it consults the rules, records the call, and answers.
///
/// A single type implementing every capability trait rather than nine, because
/// each implementation is the same three steps and nine copies of them would
/// drift.
struct Double {
    mocks: Arc<MockCaps>,
    /// The node this double was scoped to, stamped onto every call it logs.
    node_id: Option<String>,
    /// Backing map for the [`StateStore`] impl, which is the one capability
    /// whose whole job is to remember.
    state: Mutex<HashMap<String, Value>>,
}

impl Double {
    fn new(mocks: Arc<MockCaps>, node_id: Option<String>) -> Self {
        Self {
            mocks,
            node_id,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Consult the rules, log whatever happens, and return it.
    async fn dispatch(
        &self,
        capability: &str,
        method: &str,
        target: String,
        request: Value,
        default: impl FnOnce(&Value) -> Value,
    ) -> Result<Value> {
        let programmed = self
            .mocks
            .respond_to(capability, &target, self.node_id.as_deref(), &request)
            .await;
        let result = match programmed {
            Some(result) => result,
            None => Ok(default(&request)),
        };
        let outcome = match &result {
            Ok(value) => CallOutcome::Ok(value.clone()),
            Err(err) => CallOutcome::Err(err.to_string()),
        };
        self.mocks.log.record(
            capability,
            method,
            self.node_id.clone(),
            target,
            request,
            outcome,
        );
        result
    }
}

#[async_trait]
impl LlmProvider for Double {
    async fn complete(&self, request: Value, conn: Option<&str>) -> Result<Value> {
        let conn = conn.map(str::to_string);
        self.dispatch(
            capability::LLM,
            "complete",
            String::new(),
            request,
            |req| json!({ "completion": req, "connection": conn }),
        )
        .await
    }
}

#[async_trait]
impl ToolInvoker for Double {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> Result<Value> {
        let slug_owned = slug.to_string();
        let conn = conn.map(str::to_string);
        self.dispatch(
            capability::TOOLS,
            "invoke",
            slug.to_string(),
            args,
            move |args| json!({ "tool": slug_owned, "args": args, "connection": conn }),
        )
        .await
    }
}

#[async_trait]
impl HttpClient for Double {
    async fn request(&self, request: Value, conn: Option<&str>) -> Result<Value> {
        // The URL is what a rule globs on; a request without one still matches
        // a bare `*`.
        let url = request
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let conn = conn.map(str::to_string);
        self.dispatch(
            capability::HTTP,
            "request",
            url,
            request,
            |req| json!({ "status": 200, "request": req, "connection": conn }),
        )
        .await
    }
}

#[async_trait]
impl CodeRunner for Double {
    async fn run(&self, language: CodeLanguage, source: &str, input: Value) -> Result<Value> {
        let request = json!({
            "language": format!("{language:?}"),
            "source": source,
            "input": input,
        });
        self.dispatch(
            capability::CODE,
            "run",
            format!("{language:?}"),
            request,
            |req| json!({ "result": req.get("input").cloned().unwrap_or(Value::Null) }),
        )
        .await
    }
}

#[async_trait]
impl ShellRunner for Double {
    async fn run(&self, request: ShellRequest) -> Result<ShellOutcome> {
        let script = match &request.script {
            crate::caps::ShellScript::Inline(source) => source.clone(),
            crate::caps::ShellScript::Path(path) => path.clone(),
        };
        let encoded = json!({
            "interpreter": request.interpreter.as_str(),
            "script": script,
            "cwd": request.cwd,
            "env": request.env,
            "input": request.input,
        });
        let value = self
            .dispatch(
                capability::SHELL,
                "run",
                script.clone(),
                encoded,
                move |_req| json!({ "exit_code": 0, "stdout": script, "stderr": "" }),
            )
            .await?;
        // A programmed value may describe the whole outcome, or just be the
        // stdout a test cares about. Accept either rather than making a caller
        // spell out an exit code they do not care about.
        Ok(ShellOutcome {
            exit_code: value
                .get("exit_code")
                .and_then(Value::as_i64)
                .unwrap_or(0)
                .try_into()
                .unwrap_or(0),
            stdout: value
                .get("stdout")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| value.to_string()),
            stderr: value
                .get("stderr")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        })
    }
}

#[async_trait]
impl AgentRunner for Double {
    async fn run_agent(
        &self,
        agent_ref: &str,
        request: Value,
        conn: Option<&str>,
    ) -> Result<Value> {
        let name = agent_ref.to_string();
        let conn = conn.map(str::to_string);
        self.dispatch(
            capability::AGENT,
            "run_agent",
            agent_ref.to_string(),
            request,
            move |req| json!({ "agent": name, "request": req, "connection": conn }),
        )
        .await
    }
}

#[async_trait]
impl MemoryProvider for Double {
    async fn recall(&self, scope: &str, query: &str, opts: Value) -> Result<Value> {
        let request = json!({ "scope": scope, "query": query, "opts": opts });
        self.dispatch(
            capability::MEMORY,
            "recall",
            scope.to_string(),
            request,
            |_| json!({ "results": [] }),
        )
        .await
    }

    async fn flavour(&self, slug: &str) -> Result<Value> {
        let request = json!({ "slug": slug });
        self.dispatch(
            capability::MEMORY,
            "flavour",
            slug.to_string(),
            request,
            |_| json!({ "traits": {} }),
        )
        .await
    }

    async fn people(&self, query: Option<&str>) -> Result<Value> {
        let request = json!({ "query": query });
        self.dispatch(
            capability::MEMORY,
            "people",
            String::new(),
            request,
            |_| json!({ "people": [] }),
        )
        .await
    }

    async fn remember(&self, scope: &str, key: &str, value: Value) -> Result<()> {
        let request = json!({ "scope": scope, "key": key, "value": value });
        self.dispatch(
            capability::MEMORY,
            "remember",
            format!("{scope}/{key}"),
            request,
            |_| Value::Null,
        )
        .await
        .map(|_| ())
    }

    async fn forget(&self, scope: &str, key: &str) -> Result<()> {
        let request = json!({ "scope": scope, "key": key });
        self.dispatch(
            capability::MEMORY,
            "forget",
            format!("{scope}/{key}"),
            request,
            |_| Value::Null,
        )
        .await
        .map(|_| ())
    }
}

#[async_trait]
impl StateStore for Double {
    async fn load(&self, key: &str) -> Result<Option<Value>> {
        let stored = self
            .state
            .lock()
            .expect("mock state poisoned")
            .get(key)
            .cloned();
        // Logged like any other call, but the *store* is the source of truth:
        // a rule that overrode a load would make a stateful graph unreadable.
        self.mocks.log.record(
            capability::STATE,
            "load",
            self.node_id.clone(),
            key.to_string(),
            json!({ "key": key }),
            CallOutcome::Ok(stored.clone().unwrap_or(Value::Null)),
        );
        Ok(stored)
    }

    async fn store(&self, key: &str, value: Value) -> Result<()> {
        self.state
            .lock()
            .expect("mock state poisoned")
            .insert(key.to_string(), value.clone());
        self.mocks.log.record(
            capability::STATE,
            "store",
            self.node_id.clone(),
            key.to_string(),
            json!({ "key": key, "value": value }),
            CallOutcome::Ok(Value::Null),
        );
        Ok(())
    }
}

#[async_trait]
impl WorkflowResolver for Double {
    async fn resolve(&self, workflow_id: &str) -> Result<WorkflowGraph> {
        self.mocks
            .workflows
            .get(workflow_id)
            .cloned()
            .ok_or_else(|| {
                EngineError::Capability(format!(
                    "testkit: no workflow registered as {workflow_id:?}"
                ))
            })
    }
}

#[cfg(test)]
#[path = "mocks_tests.rs"]
mod tests;
