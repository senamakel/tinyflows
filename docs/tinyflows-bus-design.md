# tinyflows-bus design (openhuman#8055, tinyflows#3, tinyflows#4)

Goal: the OpenHuman host reaches workflow behaviour only through the
`tinyflows-bus` contract served by the compiled `tinyflows-module`. The host
keeps config, security policy and approvals, credentials, `Outcome<T>`
controllers and event-bus wiring. This document fixes the contract shape and
the slice order. Catalog CRUD and revision history are served by the module;
run execution remains a later slice.

## Contract surfaces

One bus name (`ai.tinyhumans.tinyflows.Authoring`), one object path and
interface per surface, each with its own `CONTRACT_VERSION`:

| Surface | Status | Members |
| --- | --- | --- |
| Authoring | served, v4 | `ValidateGraph`, `ImportGraph`, `BindingFailures`, `AuthoringFailures`, `UnresolvableToolArgs`, `BuilderFailures`, `WiringWarnings` (migrate, compat, graph analysis, import, gates, preflight, advisory wiring) |
| Schedule | served, v4 | scheduler start/stop/tick/drain, durable occurrence leases, queued `RunJobNow`, two-phase `Send`/terminal `Deliver`, captured-job `ScheduleTrigger`, `ScheduleStore`, delay and delivery validation, schedule maths and validation (`NextRun`, `ValidateSchedule`, `RunsCloserThan`, `NormalizeExpression`, `ScheduleCronExpression`), `DefaultDelivery`, `CheckOriginRequirements`, `CheckPatch`, `TruncateCronOutput` |
| Runs | served, v6 | `StartRun`, `ResumeRun`, `ExpireParked`, `BootSweep`, `CancelRun`, `PollRun`, `SandboxRun` |
| CapsHost | host callback, v2 | `LlmComplete`, `ToolInvoke`, `HttpRequest`, `CodeRun`, `ShellRun`, `MemoryRecall`, `MemoryFlavour`, `MemoryPeople`, `MemoryRemember`, `MemoryForget`, `AgentRun`, `AgentEvents`, `ResolveAgent`, `ListAgents`, `ResolveContext`, `ResolveTools`, `ResolveWorkdir`, `ApprovalDecide`, `ApprovalCancel`, `AgentCancel` |
| Catalog | served, v6 | `ListWorkflows`, `GetWorkflow`, `PutWorkflow` (create/update/duplicate), `DeleteWorkflow`, `SetEnabled`, `ListRevisions`, `GetRevision`, `CreateDraft`, `GetDraft`, `UpdateDraft`, `ListDrafts`, `DeleteDraft`, `PromoteDraft`, `UpsertSuggestions`, `ListSuggestions`, `SetSuggestionStatus`, `GetRun`, `ListRuns`, `ListAllRuns`, `PruneRuns`, `NodeKinds`, `ApplyGraphOps`, `ApprovalManifest`, `ImportLegacyBatch` |

Rules: contract crates stay synchronous and I/O-free; graphs and runs cross the
boundary as JSON; the host never links the engine. Adding a surface does not
change the other surfaces' versions; changing a served shape bumps that
surface's version and the registry records the pair.

### Validate / migrate / compat

`ValidateGraph` runs migration, structural validation and engine compatibility
in the module; the result carries the migrated graph even when invalid so
editors keep the author's work. A request may include the scope-bound
`AuthoringHost.ResolveWorkflow` callback. When present, the shared async
compatibility walk checks saved sub-workflow children; missing or unavailable
children retain the engine's existing runtime-diagnostic behavior.

For parseable graphs the v4 result also carries the module's trigger warnings,
compact authoring summary, and whether an actionable node is reachable from
the trigger. These values use the same graph helpers as execution and catalog
policy. Unparseable graphs leave summary and actionability absent.

`ImportGraph` normalizes native and n8n documents without persistence. `auto`
detects n8n exports, while `native`/`tinyflows` and `n8n` select a format
explicitly. Format names are trimmed and case-insensitive; an empty format is
the same as `auto`. The module returns importer warnings separately from
structural and engine compatibility errors. Its warnings include both
importer-specific diagnostics and the same module-generated trigger warnings
as `ValidateGraph`.

### Run / resume / cancel

`StartRun` accepts a required host-chosen opaque `run_id`, a trusted execution
context, a separately bound `CapsHost` and owner-scoped `CatalogStorage`, the
graph, a typed trigger kind plus its distinct payload, declared inputs, and a
required nonzero `timeout_ms`. The host chooses the budget; the module enforces
it cooperatively at engine node boundaries. There is no module default or
unbounded value. OpenHuman continues to supply its existing 600 second policy.
Compatibility, compilation and declared-input resolution complete before a run
row or live cancellation token is created. After preflight, row insertion uses
create-if-absent and the owner-scoped live token is registered before the id is
acknowledged. The exact storage callback address is the stable owner key for
live Poll/Cancel; owner ids never come from graph or trigger data.

The module drives the run on its executor. `CancelRun` signals a live run's
engine token; the actor persists its own terminal row and event. A parked or
orphaned run is cancelled through the row's compare-and-swap, and its checkpoint
is removed only if that transition wins. `PollRun(run_id, cursor)` returns a
bounded page with a monotonic next cursor. Step writes and their events are
queued out of synchronous observer callbacks and drained before terminal
settlement; graph observations and live agent progress join the same event
sequence. The final event follows the terminal row write, and `done` remains
false for `pending_approval`. Checkpoints and the exact executed graph snapshot
live in the owner-scoped document port. `ResumeRun` carries the expected flow
id, owner storage callback, a fresh flow-bound `CapsHost`, approval and
rejection ids, and a host-selected timeout. It requires a pending row for that
flow and at least one decision matching a pending gate; a gate cannot be both
approved and rejected. The module compares the current saved workflow hash
with the hash pinned when the run parked, then rebuilds execution from the
pinned graph and input snapshot. The parked-to-running transition and resumed
actor lease are one compare-and-swap, so a concurrent resume, expiry, or boot
sweep cannot replace the winner.

`PollRun` also carries the engine outcome needed by synchronous host
controllers: a `parked` event's data is `RunParked` (segment output, pending
approval ids, and the pinned graph hash), and a `finished` event's data is
`RunFinished` with an optional output and a deadline-cause flag. If an
output-bearing event exceeds the storage backend's document limit, the module
retries the same event without only the output field; the parked approval
metadata or finished status remains pollable. Recovery-only terminal events
also omit output because no engine outcome exists. These fields are additive
and default when reading events produced by an older module.

The resumed observer starts with the persisted step prefix, replaces a node's
parked observation with its resumed result, and seeds the event writer from
the last persisted sequence. Resumed execution uses a cancellable engine entry
point; `CancelRun` and the host-selected timeout reach the same cooperative
token as a fresh run.

`ExpireParked` changes only rows still in `pending_approval` whose parking time
is older than the 600-second TTL. `BootSweep` reconciles old `running` rows
only on a local backend. It is a no-op for shared backends and skips rows with
an active owner-scoped actor lease, including when the host callback is an
alias for the same document scope. Expiry and boot recovery emit a terminal
event only after their status compare-and-swap wins, settle tentative dedup
state from the stored execution snapshot, and remove the corresponding
checkpoint best-effort.

The host's CatalogStorage callback allowlist must also admit
`flows_graph_checkpoints`, `flows_graph_threads` and `flows_graph_writes` for
the module's DriverCheckpointer. It must apply the same owner scope to those
collections as it does to the workflow catalog collections.

`SandboxRun` accepts an unsaved inline graph and input, validates and compiles
it inside the module, then returns the same diagnostic buckets as the builder
dry-run. It does not admit a live actor or write run rows, events or graph
checkpoints. The host includes the policy-classified permission manifest in
the request; the module reports it without treating it as execution authority.
The module uses schema-aware deterministic mocks for both named
agent and plain LLM execution. Its only host callback is the exact
`CapsHost.ToolInvoke` address supplied for preflight. The module discards the
callback's return value after successful validation and produces the simulated
tool result with its own `MockTools`; hosts must not forward the callback to a
real integration. HTTP, code, shell, memory, state, approval and agent calls
remain in module-local mocks.

### Node kinds, graph_ops, gates, preflight

The canonical host-facing `NodeKindContract` data model, host-note overlay
method, and renderers live in `tinyflows-bus::node_kinds`. The engine retains
its source-compatible API snapshot because the bus crate is intentionally
unpublished; an exhaustive module parity test keeps every contract and
renderer aligned. The `NodeKinds` member returns the full ordered schema,
including config fields, ports, examples and portable notes, without requiring
hosts to link the engine. `ApplyGraphOps` applies
tagged edits sequentially to a private graph copy and returns either the full
result or the first indexed error, with no validation or persistence.

The approval manifest follows `tinyflows-catalog::approval_manifest`: the
module lists candidates in graph order, the scope-bound
`ApprovalManifestHost.ClassifyManifestCandidates` callback classifies every
candidate slot, and the module assembles the wire entries. A missing,
malformed, or failed callback response fails the operation closed. Host policy
still owns HTTP/code/native/connector classification and curated-read skips;
dynamic and agent rows have no host classification.

## Host callbacks for caps traits

Every host-owned `tinyflows::caps` method is served by the `CapsHost`
interface, whose exact callback address the trusted host binds to the flow,
run, approval requirement, and owner scope before run execution. The module
uses that address verbatim; graph and trigger JSON cannot select or replace it.
The module never sees credentials, and `conn` stays an opaque host reference:

| Trait | Members |
| --- | --- |
| `LlmProvider` | `LlmComplete(request, conn)` |
| `ToolInvoker` | `ToolInvoke(slug, args, conn)` |
| `HttpClient` | `HttpRequest(request, conn)` |
| `CodeRunner` / `ShellRunner` | `CodeRun`, `ShellRun` |
| `MemoryProvider` | `MemoryRecall/Flavour/People/Remember/Forget` |
| Module-owned state and catalogue reads | document port (`flows_kv`, saved workflow catalog) |
| `ApprovalProvider` | `ApprovalDecide`, `ApprovalCancel` |
| Module-owned tasks | Tokio task runner with the run's capability bundle |
| `AgentRunner` | `AgentRun`, `AgentEvents` |

The module constructs `StateStore` over the run's owner-scoped document
callback, with the trusted namespace `flow:<flow_id>`, and resolves saved
workflow ids through that same scoped catalog. The run manager supplies the
flow id separately from the graph and trigger. Spawn tasks execute inside the
module: tool and HTTP tasks call the same run capability bundle, and child
workflow tasks retain the module task runner through a weak reference to avoid
a runner/capabilities ownership cycle. Task start, poll, and cancel never
bounce through the host. Saved child graphs and inline workflow-task graphs pass
the engine compatibility guard before compilation; run-time resolution fails
closed when a legacy saved graph has a topology the current executor cannot run.
If a child workflow parks for approval, its task settles as a capability failure
instead of reporting partial output as success: TaskState has no parked/resume
state, and detached tasks are process-local rather than checkpointed run state.

Only `AgentRunner` streams progress. `AgentRun` returns an opaque handle, then
`AgentEvents(handle, cursor)` reads typed events and a typed terminal outcome.
Cursors start at zero and pages are contiguous, one-based sequence numbers;
replay, gaps, pages above 256 entries, and transcripts above 10,000 entries
fail as capability errors. Empty nonterminal pages are paced by the module, and
an empty terminal page is allowed. A terminal page has exactly one outcome;
the host's settled transcript is authoritative, with bounded progress entries
used only when that transcript is empty. Agent absence (`None`) and empty tool
grants stay ordinary absence. Workdir refusal remains explicit. Transport or
malformed-response errors are capability failures, never a pending result.

Approval callbacks use the existing create-or-fetch `request_id` contract:
`Pending` parks the node for engine resume, and a later call with the same id
reads the same request. Callback failures fail closed. `CodeRun` forwards the
existing source string and JSON input unchanged, including source using the
legacy function-body `return` convention; the module does not wrap it as
stdin/stdout script input. `ShellRun` uses the closed `sh`/`bash` interpreter
and typed inline/path request.

## Schedule contract v3: typed records and pure members

A host reaches the scheduler through `tinyflows-bus` alone: it links neither
`tinyflows-schedule` nor `tinyflows-drivers`, and redeclares no type. v3 makes
that possible:

- The wire records live in `tinyflows_bus::schedule::types` (`Schedule`,
  `CronJob`, `CronRun`, `CronJobPatch`, `JobOrigin`, `DeliveryConfig`,
  `DeliveryStatus`, `JobPolicy`, `JobType`, `SessionTarget`, `ActiveHours`,
  `delivery_mode`). `tinyflows_bus::schedule` exports them together with
  `AgentJobSpec`, `ImportedJob`, the callback records
  `Attempt`/`RunRecord`/`Completion`, `StoreOperation` (the typed `op` enum),
  `TooFrequent` and the constants `MIN_AGENT_JOB_INTERVAL`,
  `MAX_CRON_OUTPUT_BYTES`, `TRUNCATED_OUTPUT_MARKER`. `tinyflows-schedule`
  re-exports every one at its old path. JSON shapes are byte-identical, pinned
  by `wire_tests` in both crates. Adds `chrono` (no I/O) to the contract.
- New members (algorithms that need the `cron` crate run in the module):
  `NextRun(NextRunRequest)`, `ValidateSchedule(ValidateScheduleRequest)`
  (`agent: true` adds the cadence floor), `RunsCloserThan(RunsCloserThanRequest)`,
  `NormalizeExpression(string)`, `ScheduleCronExpression(Schedule)`,
  `DefaultDelivery(Option<JobOrigin>)`, `CheckOriginRequirements(CheckOriginRequest)`,
  `CheckPatch(CheckPatchRequest)`, `TruncateCronOutput(string)`.
- New `ScheduleStore` operations: `set_max_active_jobs`, `import_job`,
  `import_run`.

## Storage-port decision (`tinyflows-sqlite`)

Decision: the module does not own a SQLite backend. Schedule contract v2
already removed it. The catalog and run checkpoints use the document port
(`CatalogStorage`: ensure/get/put/delete/query/delete-where over scoped collections), backed on the
host by `tinystoragedrivers` (sqlite, file, mongodb) selected by
`OPENHUMAN_STORAGE_URL`. `tinyflows-drivers` keeps the document-port
implementation of catalog and checkpoint stores. Hosts own legacy source
discovery and raw readers; the module accepts bounded records through the
callback contract and applies TinyFlows-owned semantic writes. No
`tinyflows-sqlite` crate is created. Scope is the agent id, set by the host
when it binds the callback object.

The served Catalog v2 receives a `CatalogStorage` callback address on each
request. That object is created by the host for the acting agent, so the module
does not accept a scope string from workflow data. The module drives
`FlowCatalogDocuments` through the callback and allowlists its catalog
collections. `PutWorkflow` distinguishes create, update and duplicate; update
uses the previously observed `updated_at` as its optional expected version and
returns a structured conflict with the current workflow. Create enforces the
generic automatic-trigger and outbound-approval safeguards and reports whether
it born-disabled the workflow or forced approval. New and duplicated workflows
start at revision 1; only a committed graph update advances the number. Existing
documents without the sequence report one plus their visible retained history;
any older, already-pruned history cannot be reconstructed. The next successful
graph update persists the following number. Imported definitions remain
unseeded while their history is copied, so a history import does not masquerade
as a graph update. Once persisted, the durable counter does not reset when old
snapshots are pruned. List rows
include the latest revision, status, trigger kind, node count, update time and
skipped corrupt-row count; revisions are served by id and history order. The
CatalogStorage port advertises only ensure, get, put, delete, query and
delete-where: the current catalog driver does not use transaction batches or
collection counts.

Catalog v6 also serves the existing draft, suggestion, and run-read behavior
through that same scope-bound document port. Draft graphs remain raw JSON and
draft patches preserve the difference between an unchanged `flow_id`, clearing
it with `null`, and assigning an id. Draft promotion is generic orchestration:
the module loads the draft and selects create or update, calls a distinct
scope-bound `DraftPromotionHost.SavePromotedDraft` callback for the normal host
save path, then best-effort deletes the draft. The callback carries typed
workflow data and opaque host outcome metadata; a structured failure retains
its code, message, and details and leaves the draft in place. The module does
not own host validation, trigger binding, approval policy or event side effects.
Suggestion and run DTOs are mirrored in the serde-only bus crate so clients do
not need implementation-library dependencies.

`ImportLegacyBatch` carries at most 64 raw records and a 2 MiB serialized
request, always through the supplied scope-bound `CatalogStorage` callback.
SQLite and JSON/JSONL paths remain host-side; the module receives only bounded
row DTOs and file bytes. The owning module performs graph migration, typed
decode, idempotent catalog writes, checkpoint replay and per-record tallies.
The existing `import_flow_catalog`, `import_sqlite_checkpoints`, and
`import_file_checkpoints` APIs use the same raw reader and semantic writer.
The host must run all batches during exclusive migration startup, before run
execution or other writers can mutate that scope; the checkpointer's append
sequence is not an atomic transaction with concurrent live writes.

## Slice plan

1. Catalog CRUD, enablement, revisions and scoped document callbacks are
   served. `runs` remains vocabulary-only.
2. `NodeKinds` served; core `flows` store reads move behind
   the bus; delete the core copies and tests.
3. `ApplyGraphOps` and `ApprovalManifest` served (host classifies).
4. `CapsHost` callbacks and `StartRun`/`PollRun`/`CancelRun`; core run
   manager shrinks to the host adapter and event-bus projection.
5. `ResumeRun`, parked-run expiry, and boot sweep (Runs v6).
6. `SandboxRun` and the module artifact's mock-only preflight boundary.
7. Retire remaining direct `tinyflows` links from core; update
   `module-boundaries.json` and the boundary inventory.

Known blocker (not fixed here): modules that depend on tinybus through a
nested path cannot be co-linked into `openhuman-modules-linked`; tracked in the
tinybus epic.
