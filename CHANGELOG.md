# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`RunInput::with_run_id`** — a host names a run with a durable,
  server-generated id, seeded into the run state as `run.id`. The engine's own
  run id is process-local and minted fresh on every call (a resume re-executes,
  so it changes between a run and its own resume), which makes it unusable for
  anything that must name *this* run across a pause. The `approval` node's
  `request_id` defaults to `"<run id>:<node id>"` and so is now unique per run
  **and** stable across resume — the property that makes one human review
  rather than a fresh card per resume. Seeded outside `run.trigger` on purpose:
  the trigger is caller-supplied and, on a webhook, attacker-influenced, and the
  review-de-duplication key must not be.

### Added

- **`approval` node kind + the `ApprovalProvider` capability** — a
  human-in-the-loop review step that carries what is being reviewed (a URL, a
  draft, any payload) and routes on the answer: `approved` / `rejected` ports,
  the verdict (reviewer, comment, any edit they made) emitted as an item and
  readable anywhere as `=nodes.<id>.decision.approved`. Reaches the human
  through the new optional `caps::ApprovalProvider`, whose `decide` is
  create-or-fetch on a stable `request_id` so a resume or a poll never notifies
  the reviewer twice. Hosts that wire no provider still get a working node: it
  pauses the run and is settled with `engine::resume`, exactly like a
  `requires_approval` gate. Waits by suspending by default, with an optional
  bounded `wait_mode: "poll"`; `on_reject` (`route` / `error` / `drop`) and
  `on_timeout` (`error` / `reject` / `route`) decide what a "no" does.
  Note for hosts that build `caps::Capabilities` with a struct literal: it gains
  an `approvals: Option<Arc<dyn ApprovalProvider>>` field, so add
  `approvals: None` (or a provider) to keep compiling.

- **`tinyflows::testkit` — testing, mocking, and live debugging for workflows.**
  Behind the default-off `testkit` feature; adds no dependencies.

  The problem it addresses: the engine will happily run a graph whose every
  binding resolved to `null`, whose agent node dispatched with an empty prompt,
  and whose failure was swallowed by an `on_error` policy — and report all of it
  as success, because each of those is a legal value rather than an error. What
  was missing was not execution but the means to interrogate an execution.

  - `testkit::mocks` — programmable, recording capability doubles.
    `MockCaps::new().on_tool("slack.send", Respond::value(…))`, with `*` globs,
    per-call sequences (`Respond::sequence`), injected failures, delays,
    schema-synthesized answers, and per-node scoping (`only_from`). Every call
    is logged in one sequence across all capabilities and attributed to the node
    that made it.
  - `testkit::trace` — a structured run record: each activation's input *and*
    output, every `=`-binding with the value it resolved to, and — when it
    resolved to nothing — the upstream node it was reading from. That last part
    turns "it produced null" into a pointer at the node that should have
    produced it.
  - `testkit::harness` — `TestHarness` plus named assertions, including
    `assert_no_null_bindings`, which catches the failure a green run hides.
  - `testkit::debug` — real breakpoints. Pause before or after a node, inspect
    what it was about to receive, override its output, skip it, fail it, patch
    the run state, or single-step. Conditions cover `on_error`, the nth
    activation of a loop, and arbitrary `=`-expressions. A `DebugSession` owns
    the spawned run, so it can be driven from another task.
  - `testkit::tools` — every one of the above as a named tool with a real JSON
    Schema and a JSON-in/JSON-out `TestkitRegistry::dispatch`, so a host can
    hand the whole module to an agent without writing an adapter. tinyflows
    registers nothing and talks to no model; these are descriptors and handlers,
    the same division `catalog` already draws for node kinds.

- **`tinyflows::interception` — the engine's one execution-*gating* hook.**
  Always compiled, and inert unless used. A `RunObserver`'s callbacks return
  `()`, so it can watch a run and never change one; a `StepInterceptor` returns
  a `StepAction` the engine obeys, which is what makes breakpoints and output
  overrides expressible at all. New entry point `engine::run_intercepted`. With
  no interceptor attached the engine builds no `StepFrame` and makes no call —
  asserted by a property test over generated graphs, not only by inspection.

- **`tinyflows::diagnostics` and `tinyflows::evidence`** — `Diagnosis`,
  `diagnose`, and the evidence-bounding helpers moved out from behind the
  `store` feature. Both are pure functions of engine records, and a trace or a
  tool reply needs them as much as a durable record does. Re-exported from
  `store::types`, so no downstream caller breaks.

- **`caps::sample_for_schema`** is no longer behind `host-caps`; the auto-mock
  needs it and should not have to pull in a process runner and an HTTP client to
  get it. Still re-exported from `caps::host::mocks`.

### Fixed

- A node that failed once and then **succeeded on retry** is no longer reported
  as failed to observers of an activation's settled state. The engine's retry
  loop keeps the last failed attempt's error even after a later attempt
  succeeds; surfacing it unconditionally showed a recovered node as a failed one
  and would have fired every on-error breakpoint on it.

### Changed

- **Breaking: the Chrome companion moved behind the `chrome-extension`
  feature.** `tinyflows::browser` and `tinyflows::companion` — previously part
  of the default public API at `0.6.1` — now compile only when the
  `chrome-extension` feature is enabled, and the `tinyflows` CLI binary requires
  that feature too. The core engine no longer pulls in the `axum`/`reqwest`
  listener and HTTP client stack by default. Downstream code that used
  `tinyflows::browser` or `tinyflows::companion` must enable the feature (e.g.
  `tinyflows = { version = "0.7", features = ["chrome-extension"] }`).
  Correspondingly released as a major semver change (`0.7.0`, breaking in `0.x`
  terms).

- **Removed the `tinyagents` dependency.** The state-graph runtime the engine
  lowers workflows onto now lives in-crate as `crate::graph` (builder, superstep
  executor, channels, reducers, commands/interrupts, checkpointing, run status,
  and the durable event journal), vendored out of `tinyagents` and trimmed to the
  surface tinyflows actually drives. Agents are a host concern reached through
  `caps`, so the crate no longer carries an agent-harness dependency, its
  vendored `vendor/tinyagents` submodule, or the `[patch.crates-io]` table that
  redirected it. Dropped along the way: the graph-level node retry policy
  (tinyflows applies its own `on_error`/retry inside each node handler, so a
  second retry loop would have multiplied the attempt budget), the SQLite
  checkpointer, the Langfuse exporter, and the store-backed event journal.
  `engine`'s re-exports (`Checkpointer`, `DurabilityMode`, `FileCheckpointer`,
  `InMemoryCheckpointer`, `GraphEventJournal`, `GraphObservation`,
  `InMemoryGraphEventJournal`) keep their names and now resolve to `crate::graph`,
  so host code that only used those re-exports is unaffected; anything naming
  `tinyagents::` types directly must switch to `tinyflows::graph::`.

### Added

- **New node kind: `void`, a terminal sink.** It accepts items on `main`,
  discards them, and activates nothing — the branch ends there, on purpose. A
  branch could always dead-end (a node with no outgoing edges terminates), but
  an unwired port reads exactly like a forgotten one, so there was no way to
  declare "this is a side effect and nothing waits on it". `spawn → void` is now
  the explicit spelling of a ticket no `gate` will collect; the abandon
  semantics are unchanged from leaving it unwired. It adds **no** concurrency:
  work upstream still runs inline in its own super-step, and only the result is
  dropped. Its slot is `{items: [], port: null, discarded: N}`, which keeps
  "never activated" (no slot at all), "activated with nothing to drop" and
  "dropped N items" distinguishable.

  Validation refuses a `void` with any outgoing edge — including the `error`
  edge `on_error: "route"` would otherwise demand, which is reported directly
  rather than as a `MissingErrorRoute` the next rule would then reject — and one
  with no incoming edge, since a node with no effect and no input declares
  nothing.

  The scatter-lane dead-end rule is relaxed accordingly: a lane branch ending in
  a `void` is now legal, making it the one dead end a lane may have. The rule
  exists to catch *accidental* invisibility (a lane activation never writes the
  node's top-level slot), and a `void` is the author declaring it. A `scatter`
  with no `gather` anywhere is still refused, void downstream or not.

  Not included, deliberately: no lint for a `spawn` with neither a `gate` nor a
  `void` downstream. `validate_all` has no severity tier, and making it a hard
  error would break the documented "fire-and-forget is legal" contract. A
  possible future addition if a warning channel ever lands.

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
