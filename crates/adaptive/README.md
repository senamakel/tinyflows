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
- [ ] **1b · stores** — bridge `tinyflows::store`; add the two tables it lacks:
      ledger rows and scored lessons.
- [ ] **2 · intake** — prompt → goal → *select* (catalogue with `applied`/`helped`
      shown; model picks and binds inputs, or says none fits) → *author*
      (grounded on the node catalogue, validated, dry-run before it counts).
- [ ] **3 · execute** — `run_with_checkpointer`, host capabilities.
- [ ] **4 · judge** — evidence from three sources: `RunOutcome`, the
      `RunRecord`'s null-resolving expressions, and the workspace diff.
- [ ] **5 · consolidate** — lessons; `record_use` on the workflow; a `GraphOp`
      batch as a **variant** when the graph is at fault; promotion behind an
      evidenced gate.
- [ ] **6 · retry edge** — planner sees the ledger and the exclusion list.

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
- **A dry run proves wiring, not work.** A `code` node's script and an `agent`
  node's real reply are both invisible to one.
