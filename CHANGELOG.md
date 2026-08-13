# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Configurable agents.** An `agent` node can now be given dynamic context,
  an explicit tool allow-list, a model and provider, a working directory,
  advisory limits, and arbitrary harness metadata — while the agent
  *implementation* stays entirely with the embedding harness. tinyflows runs no
  agentic loop: it assembles a typed `caps::AgentRunRequest` and the harness
  executes it.
  - `WorkflowGraph::agents`: a top-level registry of reusable
    `model::AgentDefinition`s (id, name, description, instructions, model,
    provider, working_dir, tools, context, limits, metadata), mirroring
    `inputs`. A node's `agent_ref` resolves here first, then against the
    harness's registry, then passes through as a bare id.
  - New `agent` node config keys, all optional: `instructions`, `model`,
    `provider`, `working_dir`, `context`, `limits`, `metadata` (`prompt`,
    `agent_ref`, `tools`, `output_parser`, `connection_ref` keep their meaning).
    A node may only **narrow** its agent definition — instructions append,
    context appends, tools intersect, limits take the lower bound.
  - `model::ContextSource`: declarative dynamic context — `text` (literal or
    `=`-expression), `items`, `memory` and `flavour` (via the existing
    `MemoryProvider`), and `host` (expanded by the harness). An unresolvable
    source fails the node unless it sets `optional: true`.
  - `model::AgentLimits`: `max_steps`, `max_tool_calls`, `agent_timeout_secs`
    (whole run) and `tool_timeout_secs` (per tool call).
  - `AgentRunner` gains four **defaulted** methods — `run`, `resolve_agent`,
    `list_agents`, `resolve_context`, `resolve_tools` — plus the value types
    `AgentRunRequest`, `AgentRunOutcome`, `StopReason`, `ContextBlock`,
    `ToolDescriptor`, `AgentRunIdentity`, `AgentModelSelection`, `AgentUsage`.
  - `StopReason` distinguishes `Finished` from `LimitStop` and `Paused`, so a
    partial or human-blocked run is no longer indistinguishable from an answer.
    Surfaced on the item envelope's new `meta` key as `=item.meta.stop`.
  - `validate::unresolved_agent_refs` reports refs the graph does not declare,
    for hosts that want author-time resolution against their own registry.
  - Author-time validation: duplicate agent ids, literal-only `agent_ref` /
    tool `slug` / tool `connection_ref` / memory `scope`, trailing-`.*`-only
    tool patterns, positive limits, and a node that tries to widen an in-graph
    agent's tool grants.
  - Mocks: `MockAgentHarness` (typed seam), `MockLimitedAgentRunner`,
    `MockPausingAgentRunner`.

  **This release is additive.** `AgentRunner::run_agent` is unchanged and
  remains the trait's only required method; the default `run` forwards the
  node's resolved config to it verbatim, so a host written against the previous
  release compiles untouched and behaves byte-identically. `Capabilities` gains
  no fields, and the item envelope's `json` / `text` / `raw` are unchanged. The
  only source-level change is the new `agents` field on `WorkflowGraph` (and
  `agents` on `NodeContext`): JSON round-trips unaffected, but code
  constructing either by exhaustive struct literal needs `..Default::default()`
  or the new field. To move off the shim, override `AgentRunner::run` and map
  your harness's real stop reason onto `StopReason` — leaving it defaulted
  reports every run as `Finished`.

- A `shell` node kind that runs a shell script — inline via `config.source` or
  from a file via `config.script_path` — with an optional `interpreter`
  (`sh`/`bash`), `cwd`, and `env`. A non-zero exit fails the step; a successful
  run emits `{ exit_code, stdout, stderr, stdout_json }`.
- A `ShellRunner` capability trait (`caps::shell`) and the optional
  `Capabilities::shell` slot behind it. The engine never resolves a script path,
  chooses an environment, or spawns a process: it hands the host a validated
  `ShellRequest` and the host decides what is reachable. `None` refuses `shell`
  nodes with a capability error.

- Versioned browser action contracts plus run/tab-bound `ChromeToolInvoker` and
  composable `RoutingToolInvoker` support for explicit `slug: "browser"` nodes.
- An authenticated loopback companion with pairing-secret rotation, explicit
  shared-tab/run binding, action correlation, timeouts, heartbeats, workflow
  listing/start/cancel controls, and native CLI commands.
- A locally bundled MV3 Chrome extension with debugger-based browser actions,
  visible tab-group consent, popup pairing, a workflow side panel, unit tests,
  Playwright coverage, and deterministic release packaging.

### Changed

- **Breaking:** `Capabilities` gained a `shell` field. Hosts constructing the
  struct literally add `shell: None` (or their own runner).

## [0.3.0] - YYYY-MM-DD

_Next (unreleased) minor._

### Added

- Integration nodes (`agent`, `tool_call`, `http_request`) now resolve `=`
  expressions in their config against the node's input, enabling inline
  data-binding from upstream output; new `expr::resolve` recursively evaluates
  `=`-expressions anywhere in a config value, and the binding scope is
  `{ item, items, run }` (the first input item, all input items, and the run
  payload). A minor bump is warranted because a config string starting with `=`
  now evaluates where it was previously carried through as a literal.

## [0.2.0] - YYYY-MM-DD

First functional release: the crate graduates from a skeleton to a working,
host-agnostic workflow engine.

### Added

- **Execution engine** (`engine::run`) that lowers a validated `WorkflowGraph`
  onto the [`tinyagents`](https://crates.io/crates/tinyagents) state-graph engine
  and drives it to completion, with an item-based data-flow contract passing
  lists of items between nodes.
- **Node catalog** with per-node executors:
  - Control-flow nodes: `condition`, `switch`, `merge`, `split_out`,
    `transform`.
  - Capability-backed nodes: `agent`, `tool_call`, `http_request`, `code`,
    `output_parser`, `sub_workflow` (nested graph execution).
- **Conditional routing** driven by node outputs, **parallel fan-out** to run
  branches concurrently, and a **merge fan-in barrier** that joins branches back
  together.
- **Per-node error handling**: configurable `on_error` behaviour, retry with
  backoff, and a dedicated error port for routing failures.
- **Run-level configuration**: overall run timeout and recursion-limit guards.
- **Observability**: `tracing` spans/events plus a `RunObserver` hook and
  structured `Run` / `ExecutionStep` records.
- **Human-in-the-loop approval gating**: workflows can pause with
  `pending_approvals` and continue via `engine::resume`.
- **Opaque `connection_ref` credentials** threaded through capability calls, so
  hosts resolve secrets without the crate ever seeing them.
- **Versioning and migration**: `schema_version` / `type_version` fields and a
  migration framework for evolving workflow definitions.
- **jq expression engine** backed by [`jaq`](https://crates.io/crates/jaq-core),
  with a dotted-path shorthand for simple field access.
- **Injectable checkpointer for durable, cross-process HITL resume**:
  `engine::run_with_checkpointer` / `resume_with_checkpointer` accept a
  host-implemented `Checkpointer<serde_json::Value>` keyed by a `thread_id`, so a
  run can pause at an approval gate, persist to the host's durable store, and
  resume later — even across a process restart. `Checkpointer`, `FileCheckpointer`,
  `InMemoryCheckpointer`, and `DurabilityMode` are re-exported from `tinyagents`.
  (The in-process `run_resumable` remains the simple path.)
- **`StateStore` wired into the `Capabilities` bundle**: the bundle now carries
  all five host capabilities (`llm`, `tools`, `http`, `code`, `state`), and nodes
  reach durable key/value state via `ctx.caps.state`.
- **Reference-workflow end-to-end test suite** and a runnable
  `hello_workflow` example.

## [0.1.1]

- Initial crate scaffold / skeleton release.
