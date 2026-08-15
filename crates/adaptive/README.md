# tinyflows-adaptive

An adaptive loop over the tinyflows engine. It ingests a prompt, **selects a
stored workflow or authors one**, runs it on the engine, judges the result
against evidence, and learns — updating or replacing the workflow when the
graph itself was the problem.

The engine is not modified. This crate sits beside it and decides *which* graph
to run; `tinyflows` decides nothing and runs one graph.

```
prompt ─▶ INTAKE ──────────▶ Runner ──▶ engine::run ──▶ CLOSING ──▶ answer
          ├ goal              (local     (unmodified)   ├ judge
          ├ select, or        or remote)                ├ consolidate
          └ author                                      ├ score / promote
                     ▲                                  └ retry?
                     └──── rows + lessons ──────────────┘
```

Every phase of the plan below is built. What closes the loop is the bottom
edge: the next attempt sees what this episode already spent and what earlier
ones learned, so a retry is a different idea rather than the same one reworded.

## Why it is a separate crate

The engine's graph is **frozen at compile**: `CompiledWorkflow` is
`{ graph: WorkflowGraph }`, and nothing at run time adds, removes or rewires a
node. It is also persistence-free and has no concept of a goal. So it can
*repeat* — the `loop` node is real — but it cannot *re-decide*.

Re-deciding is this crate's whole job, and it is a different shape: the graph
changes **between** runs, from evidence, against a record of what has already
been ruled out. Keeping the two in separate packages is what makes a merge from
upstream a merge rather than a conflict resolution.

## The rule

> The engine may know about one run. Anything that spans runs lives here.

Ledger rows, lessons, workflow scoring, exclusion lists, promotion — none of it
crosses into `tinyflows`. That is not a discipline we maintain; `WorkflowRecord`
has nowhere to put it.

## What is ported, and what is not

Derived from medulla-v2 (Python). Most of it is **not** ported, because the
engine already does it better:

| medulla-v2 | here |
|---|---|
| `Step`, `depends_on`, wave scheduling, `_dispatch_child` | **dropped** — nodes, edges, fan-out and the merge barrier are upstream |
| worktree pool, harness adapters, stream reader | **dropped** — `AgentRunner` is the seam |
| `WorkflowStore` | **dropped** — `tinyflows::store` has `WorkflowRecord`, `RunRecord`, notes, proposals, rollback |
| `Verdict`, `Blocker`, `Budget`, stall rule, `advanced` | **ported** — `contracts.rs` |
| planner / evaluator / consolidator prompts | **ported**, planner split into *select* and *author* |
| ledger rows, scored lessons, `record_use` | **ported** — upstream has notes, not scored lessons |

What survives is exactly the loop.

## Plan

- [x] **0 · repo** — workspace member beside the engine; engine untouched.
- [x] **1a · contracts** — `Goal`, `Approach`, `Verdict`, `Blocker`, `Budget`.
- [x] **1b · ledger** — the `Ledger` trait plus two backends behind features,
      `sqlite` and `mongo`, both checked by one conformance suite. Kept separate
      from `WorkflowStore` so an upstream merge never contends with it.
- [x] **2 · intake** — `decide()`: select a stored workflow, else author one.
      Selection sees the catalogue with both score counters and never sees a
      workflow this episode already tried; authoring is grounded on the engine's
      generated node catalogue and validated before it returns.
- [x] **2b · host facts** — `HostFacts`: what this machine permits, rendered into
      the authoring prompt and checked after, plus the store's own
      `HostPolicy::check_graph`. An absent fact means unknown, never forbidden.
- [x] **3 · execute** — the `Runner` port: `Local` runs the graph in this
      process, `Remote` relays it to one elsewhere, and the loop cannot tell
      which. Both are `serve()` → `RunReport` → `into_ran()`, so there is no
      second path to drift. Thin on purpose — it holds no opinion, reads no
      history, and never returns an error, because an attempt that leaves no
      ledger row is one the next pass repeats. Not `run_with_checkpointer`: see
      the field notes.
- [x] **4 · judge** — evidence from three sources: the `RunOutcome`, the
      engine's own `Diagnosis` of what the steps did, and what changed outside
      the run. Mechanical evidence settles three verdicts before any model is
      asked; the judge never sees the ledger, so it cannot propose what to try
      next.
- [x] **5 · consolidate** — `close()` records the row **whatever the verdict**
      and scores the workflow that ran; `consolidate()` keeps only what a
      different task could act on, and only with rows cited; `repair()` turns a
      `GraphOp` batch into a **variant**, never an edit in place, and only when
      the diagnosis says the graph is the thing at fault.
- [x] **5b · promotion** — a repaired family collapses to **one** catalogue row,
      and which member holds it is decided on score. A variant is proven only
      after `MIN_TRIALS` runs; until then the root keeps the position, so an
      untested graph never displaces a 40/40 parent for everyone. Lineage lives
      in the ledger, because *this graph came from that one* spans runs.
- [x] **6 · retry edge** — both planners see this episode's rows *and* the
      lessons other episodes left. Closes the loop: `consolidate()` was
      write-only until now. Authored attempts are fingerprinted by graph shape,
      so two of them no longer fold into one exclusion-list entry.

## An instance is not a goal run

Two lifetimes, and putting them in one object is the mistake worth naming.

A **`Loop` is per tenant** — a scoped ledger, a store, capabilities, host facts,
a runner, a budget. Building one costs a database pool and an HTTP client, so it
is built once and shared.

A **goal run is an episode id**, not an object. Its state lives in the `Episode`
record: goal, status, attempt, stalled.

```rust
let engine = Loop { ledger: &ledger.for_tenant("user-7"), store, caps, .. };
let finished = engine.run("ep-9f2", &goal).await?;   // many of these, concurrently
```

That split buys both things at once. Many goal runs share one instance, because
the instance holds nothing per-episode. And an episode survives the process:
kill this one mid-run and `Loop::unfinished()` on the next boot hands back
everything that was in flight, each resumable by id.

Had the instance *been* the goal run, both would be false — config rebuilt per
goal, and a deploy losing every episode's counters while leaving its rows behind
to look like progress.

The record holds exactly what the rows cannot: the **goal** (unrecoverable), and
the **stall count** (recomputable only if `advanced` is stored, so it is, on the
row). `satisfied` is a field too — it used to be recoverable only by matching
`outcome == "satisfied"`, one reworded line from reporting every episode failed.

## Inference: the crate names the job, the host picks the model

Every request carries a `tier` — `select`, `author`, `judge`, `consolidate`,
`repair`. The crate never names a model, a vendor or a URL, which is the
host-agnostic rule it inherits; only the host knows what a job maps to.

That is what makes a tier sweep a config change rather than a code change.
Judging is the expensive opinion — a judge that says yes wrongly ends the
episode — and selecting is a cheap one; without a name on the request a host
cannot route them differently.

Called `tier` and not `role` because a chat request already has `role` on every
message. Five rather than medulla-v2's three: a host maps several tiers to one
model in a line of config and cannot split one tier into two at all.

## Where the engine runs

The loop and the engine may sit in one process or on opposite ends of a socket.
`Runner` is the seam; `execute::wire` is the contract.

```
     server                                   device
  ┌────────────┐   RunRequest{graph,inputs}  ┌────────────┐
  │  intake    │ ──────────────────────────▶ │  serve()   │
  │  closing   │ ◀────────────────────────── │  engine    │
  └────────────┘   RunReport{steps,…}        └────────────┘
```

**Steps cross, not `output`.** A run's final `output` is a lossy projection of
its steps: no status (so a swallowed error is invisible), no duration, no
null-binding diagnostics, a looped node collapsed to one entry — and a run that
returned `Err` has no `output` at all while its steps are all still there.
`Diagnosis` is not sent either: it is a pure function of the graph and the
steps, and the loop already has the graph.

**Bounding is per node, at two budgets.** `bounded_within` is whole-value and
non-recursive, so applied to a map of nodes one fat entry replaces every other
node's output with a string preview. `RECORD_BUDGET` (256 KiB) bounds each step
for the durable record; `PROMPT_BUDGET` (4 KiB) bounds each node again in the
projection the judge reads.

**Nothing about the episode crosses.** A runner sees one graph and its inputs —
no ledger, no lessons, no exclusion list, no verdict. It cannot reconstruct what
is being learned from it.

**A runner that never answers is still an attempt.** `Remote` synthesizes a
report rather than propagating an error, and deliberately does *not* report an
empty `changed`: empty means "the host looked and saw nothing", which settles
mechanically as `MissingEvidence` — terminal. `ExternalWait` is terminal too, so
there is no safe blocker to pick. Saying the result is unknown routes it to the
judge, which can reach a continuable verdict, so a socket blip cannot end an
episode.

## The contracts an external process touches

Two kinds, and they fail differently. A **wire type** breaks when a derive is
dropped or a field renamed — silently, in another repository. A **trait** breaks
at compile time in the host that implements it.

### Wire — serialized, crosses a boundary

| Contract | Types | Casing |
|---|---|---|
| Execute | `RunRequest`, `RunReport`, `StepRecord`, `StepOutcome` | camelCase |
| Nested in those | `WorkflowGraph`, `Node`, `Edge`, `WorkflowInput`, `NullResolution` | **snake_case** |
| Run diagnosis | `Diagnosis`, `NullBinding`, `HiddenError`, `NeverRan` | camelCase |
| Loop state | `Goal`, `Approach`, `Verdict`, `Blocker`, `Budget` | snake_case |
| Knowledge | `LedgerRow`, `Lesson`, `LessonKind`, `Score` | snake_case |
| Host & repair | `HostFacts`, `GraphOp`, `WorkflowRecord` | snake_case |

**One payload, two conventions.** The envelope this crate added is camelCase;
the engine's model types predate it and use serde's default, so the graph
*inside* a camelCase request stays snake_case:

```json
{ "attemptId": "ep-1/3",
  "graph": { "schema_version": 1,
             "nodes": [{ "type_version": 1, "kind": "trigger" }],
             "edges": [{ "from_node": "a", "from_port": "main" }] } }
```

Neither is wrong and changing either breaks something already shipped, so the
seam is asserted rather than tidied. A relay that assumes one convention
throughout produces a graph the engine refuses.

`tests/contracts_surface.rs` asserts all of it at compile time.

**Not serializable, deliberately:** `RunOutcome`, `ExecutionStep` and
`StepStatus` are `Debug + Clone` only upstream. That is why steps cross as
`StepRecord` and the outcome is rebuilt by `into_ran` rather than sent.

### Traits — implemented in-process by a host

| Trait | Who implements it |
|---|---|
| `Relay` | the service, to reach a runner elsewhere |
| `Workspace` | whatever can say what changed outside a run |
| `Ledger` | ships as `sqlite` and `mongo`; a third passes `conformance` |
| `Runner` | ships as `Local` and `Remote`; rarely custom |
| `LlmProvider`, `ToolInvoker`, `HttpClient`, `CodeRunner`, `StateStore`, `WorkflowResolver` | the engine's `Capabilities` bundle |
| `AgentRunner`, `MemoryProvider` | optional capabilities |
| `WorkflowStore`, `HostPolicy` | the engine's store seam — **synchronous** |

`WorkflowStore` being synchronous is the one to plan around: a hosted service
with an async driver cannot implement it without blocking, so load a per-episode
snapshot before the loop and flush after.

## Choosing a ledger backend

```toml
tinyflows-adaptive = "0.1"                       # sqlite, on by default
tinyflows-adaptive = { version = "0.1", default-features = false, features = ["mongo"] }
```

**sqlite is a default feature**, so the crate persists out of the box. A crate
whose whole value is that learning accumulates should not ship unable to
accumulate it. It costs a bundled SQLite build; a deployment that only wants
Mongo turns it off with `default-features = false`.

```rust
// The parent directory is created — `/var/lib/app/` on a first run need not exist.
let ledger = SqliteLedger::from_env_or("./adaptive.db")?;
```

`from_env_or` reads `TINYFLOWS_ADAPTIVE_DB` and uses the argument when it is
unset, so ops can move the file without a rebuild while the fallback stays
visible in your code. The library **does not invent a location on your disk** —
one that writes to a home directory nobody named surprises an operator once and
is distrusted afterwards, and the right place differs entirely between a CLI, a
container and a service with a mounted volume.

Three implementations, all checked by the same public
[`ledger::conformance`] suite — so "it works on sqlite" cannot quietly mean "it
works only on sqlite", and a host writing a fourth runs the identical cases.

`MemoryLedger` is always compiled: no feature, no driver, no C library, so the
crate is usable the moment it is added. It **forgets everything on restart**,
and it is deliberately never selected for you.

That last part is the design decision, not an oversight. A ledger silently
defaulting to memory is the worst failure this crate could have: the loop runs,
the exclusion list works within an episode, lessons are written and scored, the
tests pass — and every restart throws all of it away. Nobody notices, because
the only symptom is that it never gets better. So there is no `Default` impl
that would hand it to a host that did not ask, it is named for what it does, and
`sqlite` or `mongo` is the answer the moment learning is supposed to outlive a
process.

Workflow scores live here, not on `WorkflowRecord`: a score is a fact that spans
runs, and the engine's record is a fact about one document.

## Tenancy

The scope lives on the **handle**, not on every method, because the failure it
prevents is forgetting to pass it. `ledger.for_tenant("user-7")` at the edge of
a request is a thing a reviewer can see; six scope arguments threaded through
intake and closing is a thing that goes wrong once and leaks one tenant's
lessons into another's prompt. Nothing in the loop takes a tenant argument.

One rule everywhere: **writes go to this handle's bucket; reads return this
handle's bucket plus the global one.** An unscoped handle's bucket *is* global,
so a single-tenant deployment that never calls `for_tenant` reads back exactly
what it wrote.

This matters because a lesson is free text. Its `trigger` and `claim` are
written from one tenant's episode and can name their repositories, paths and
internals, and `consolidate()` renders every retrievable lesson into the
model's prompt. So `promote` stamps the handle's scope and **ignores whatever
the argument says** — a caller, or a model answer deserialized straight into a
`Lesson`, cannot publish into another bucket by asking.

Episode rows were never at risk: they are keyed by episode and `tried()` reads
one episode at a time. It is the knowledge plane — lessons and workflow
scores — that needed the key.

`ledger::conformance::run_tenants` is public alongside `run_all`, and takes
three handles onto one store because how a backend makes a scoped one is its
own business.

## Deliberately out of scope

- **Human-in-the-loop parking.** `StopReason::Paused` is not routed into the
  engine's checkpoint/resume machinery; an `agent` node that receives one fails.
  Wiring it is an upstream contribution, not a workaround here.
- **Scheduling.** Nine trigger kinds are accepted and stored; whether one
  dispatches unattended is a host concern, and on the hosts we run today only
  `manual` fires.

## Field notes

Things that cost a day each if met in production instead.

- **`resume` replays.** It re-executes the workflow with the merged approval
  set. Every node before the gate runs again. Our retry is a new run of a new
  graph, never the engine's resume.
- **There is no wait node.** A workflow cannot sleep. Long waits end the run and
  are re-triggered.
- **`RenameNode` does not rewrite bindings.** Edges are rewired;
  `=nodes.<old_id>…` inside other nodes' configs is not. Validation passes and
  the graph runs quietly wrong. An automated fixer must treat a rename as
  touching every expression in the graph.
- **The envelope.** `agent`, `tool_call` and `http_request` wrap output in
  `{json, text, raw}`. `=nodes.x.item.f` is null where `=nodes.x.item.json.f`
  was meant — compiles, validates, dry-runs green, runs empty.
- **Bindings are `=expr`, and there are no braces.** `CLAUDE.md` and one node
  doc said `={{ … }}`; the implementation never accepted it. `is_expression` is
  `starts_with('=')`, and the remainder is either a simple dotted path
  (`=nodes.fetch.item.json.body`) or a jq program (`=.items | length`). `{{ }}`
  is neither: it routes to jq, fails to compile, and a failed program is
  `Value::Null`. Measured, not reasoned about — `={{ nodes.fetch.item.json.body }}`
  evaluates to `null` against a scope where the dotted form yields `"hello"`.
  Both docs are corrected.
- **A dry run proves wiring, not work.** A `code` node's script and an `agent`
  node's real reply are both invisible to one.
- **`run_with_checkpointer` installs a `NoopObserver`.** No observer means no
  steps, and no steps means `diagnose` returns a blank `Diagnosis` — which is
  not "nothing was wrong", it is "nobody looked". The judge's findings, the
  three mechanical verdicts and `graph_is_suspect` all read it, so the durable
  entry point silently disables repair. `run_with_checkpointer_journaled_observed`
  keeps both, at the cost of a journal. We run observed and unpersisted: a
  checkpointer buys durable *resume*, and resume is out of scope.
- **`never_ran` only reports `agent`, `tool_call` and `http_request`.** A
  routed-past `transform` is not a surprise worth warning about, so it is
  omitted by design. A test that asserts on skipped control-flow nodes will
  fail against a correct engine.
