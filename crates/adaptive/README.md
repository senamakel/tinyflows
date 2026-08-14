# tinyflows-adaptive

An adaptive loop over the tinyflows engine. It ingests a prompt, **selects a
stored workflow or authors one**, runs it on the engine, judges the result
against evidence, and learns — updating or replacing the workflow when the
graph itself was the problem.

The engine is not modified. This crate sits beside it and decides *which* graph
to run; `tinyflows` decides nothing and runs one graph.

```
prompt ─▶ INTAKE ──────────────────────────▶ engine::run ──▶ CLOSING ──▶ answer
          ├ goal                             (unmodified)    ├ judge
          ├ select a stored workflow, or                     ├ consolidate
          └ author one when none fits                        ├ score / promote
                     ▲                                       └ retry?
                     └─────────── re-decide ─────────────────┘
```

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
- [ ] **5b · promotion** — a variant supersedes its parent on score, not on
      having been written.
- [ ] **6 · retry edge** — planner sees the ledger and the exclusion list.

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

## Choosing a ledger backend

```toml
tinyflows-adaptive = { version = "0.1", features = ["sqlite"] }   # single process
tinyflows-adaptive = { version = "0.1", features = ["mongo"] }    # hosted
```

Neither is compiled unless asked for. Both pass the same
[`ledger::conformance`] suite, which is public — a host writing a third backend
runs the identical cases against it.

Workflow scores live here, not on `WorkflowRecord`: a score is a fact that spans
runs, and the engine's record is a fact about one document.

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
