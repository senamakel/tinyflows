# Node Catalog

Every node has a `NodeKind`. Kind-specific settings live in the node's `config`
(free-form JSON, validated per kind). Ports carry the item arrays between nodes;
the default port is `main`.

## Trigger

Exactly one per workflow — the graph's entry node. Its firing mode is a
`TriggerKind` in config (`manual`, `schedule`, `webhook`, `app_event`, `form`,
`execute_by_workflow`, `chat_message`, `evaluation`, `system`). The host actually
fires it; tinyflows injects the trigger payload as the initial run state.

| Node | Purpose | Ports / config gist |
|------|---------|---------------------|
| `trigger` | Entry node that starts the run | Out `main`; config `trigger_kind` |

A workflow's typed **parameters** are not declared on the trigger. They live in
the graph's top-level `inputs` array, are validated before the run starts, and
are read from any node as `=inputs.<name>` — see
[Architecture → Workflow inputs](Architecture). The trigger payload stays at
`=run.trigger.<path>`.

## Control-flow nodes (native)

Native routing logic — no host capabilities required.

| Node | Purpose | Ports / config gist |
|------|---------|---------------------|
| `condition` | Two-way IF branch | Out `true` / `false`; config: boolean expression |
| `switch` | Multi-way branch keyed by an expression | Out one port per case (+ optional `default`); config `expression`, `cases` |
| `merge` | Fan-in barrier combining multiple inputs | Waits for all wired inputs; config `mode` (e.g. `append`) |
| `split_out` | Fan-out: one item per element of a list | Downstream runs per item; config `path` |
| `transform` | Pure, expression-based field mapping | Config `set` (field → `=`-expression map) |
| `void` | Terminal sink: discards its input, runs nothing downstream | In `main`, no out ports; no config |

### Fire-and-forget with `void`

A branch could always dead-end — a node with no outgoing edges terminates — but
an unwired port reads exactly like a forgotten one. `void` is how you *say* that
a branch is a side effect nothing waits on, so validation and the next reader
can both tell intent from an accident.

It adds no concurrency: work upstream of a `void` still runs inline in its own
super-step, and only the result is dropped. For work that should genuinely
overlap, use `spawn` and a `TaskRunner`.

Three places it earns its keep:

- **`spawn → void`** — the explicit spelling of a ticket no `gate` will collect.
  Same abandon semantics as leaving the spawn unwired, said out loud.
- **A loop-body side branch** — the void arm never joins the back-edge, so it
  cannot gate an iteration.
- **Inside a scatter lane** — it is the *one* dead end a lane may have. Every
  other lane branch must reach the `gather`, because a stranded lane's output is
  invisible rather than merely uncollected; a `void` makes that invisibility the
  contract instead of the accident.

Validation refuses a `void` with any outgoing edge (including an `error` edge
from `on_error: "route"`), and one with no incoming edge. Its slot is
`{items: [], port: null, discarded: N}` — a node that never ran has no slot at
all, so "never activated", "activated with nothing to drop" and "dropped N" stay
distinguishable. `discarded` counts *that* activation, so in a loop the last
iteration's value survives, and in a lane it lands under `lanes.<lane>`.

## Capability-backed nodes

Reach the outside world through the host-injected [capability
traits](Capability-Traits).

| Node | Purpose | Ports / config gist |
|------|---------|---------------------|
| `agent` | Runs an LLM agent turn | Sub-ports `chat_model` / `memory` / `tool` / `output_parser`; config `prompt`, `model`, `cwd`, … — via `LlmProvider` |
| `tool_call` | Invokes one specific integration action | Config `slug`, `args` — via `ToolInvoker` |
| `http_request` | Outbound HTTP request | Config `method`, `url`, `headers`, `query`, `body` — via `HttpClient` |
| `code` | Runs sandboxed user code | Config `language` (`javascript`/`python`), `source` — via `CodeRunner` |
| `shell` | Runs a shell script, inline or from a file | Config `source` **or** `script_path`, plus `interpreter` (`sh`/`bash`), `cwd`, `env` — via `ShellRunner` |
| `output_parser` | Parses/validates an agent's output into a structured shape | May use `LlmProvider` for auto-fixing; can nest as a sub-agent |
| `approval` | Puts a subject in front of a **human** and routes on approve/reject | Out `approved` / `rejected` / `timeout`; config `subject`, `subject_kind`, `title`, `prompt`, `assignees`, `wait_mode`, `on_reject`, `on_timeout` (`error` default / `reject` / `route` — `route` is required to reach the `timeout` port) — via `ApprovalProvider` |
| `sub_workflow` | Runs another workflow as a nested sub-graph | Config: exactly one of `workflow` (inline) / `workflow_id`; optional `inputs` map for the child's declared inputs, optional `workspace` to run the child elsewhere |

### Where a step runs

A run is pinned to one **workspace** — the trigger's `config.workspace`, or a
`workspace` key on the trigger payload for a host that pins one per run. Every
directory a node names resolves against it:

- an `agent` node's `cwd` (`working_dir` is the older spelling of the same key),
- a `shell` node's `cwd`, and a script step's `args.cwd`,
- a `sub_workflow` node's `workspace`, which re-pins the **child run** — the
  child inherits the parent's otherwise.

One rule for all of them: a relative path is joined to the workspace, an
absolute one is allowed only if it resolves inside it (symlinks followed), and a
directory that is missing or is not a directory **fails the step** rather than
falling back to the workspace. Every one of them is `=`-bindable, which is the
point — `"cwd": "=nodes.prepare.item.json.worktree"` runs the step in a
directory an earlier node created.

An expression that resolves to `null` — the upstream node failed, or the key
moved — **fails the step** too. It is not read as "no directory declared": that
would fall back to the agent definition's own `working_dir`, then to whatever
the harness defaults to, and the step would run in a different checkout without
saying so.

A run with **no** workspace resolves nothing: the string reaches the harness
verbatim, as it always has, because a host whose agents run in a remote sandbox
names directories this process has never heard of.

**Whose filesystem.** The shape of a declared directory — absolute vs relative,
`..` traversal — is decided by reading the string, so the engine always checks
it. Whether the path *exists*, what it canonicalizes to, and whether it is a
directory are outside-world effects, and they route through
`AgentRunner::resolve_workdir`. A harness whose agents run in a container or a
remote sandbox implements it and answers for its own filesystem; the default is
`WorkdirCheck::Unmanaged`, which checks the engine's own disk exactly as before.
The `shell` node reaches the same place by a different road: it hands `cwd` to
the `ShellRunner` untouched and the host's `ScriptPolicy` contains it.

A directory key a node does not read — `workdir` on an `agent` node, `cwd` on a
`tool_call` node — is a **validation error**, not a silent no-op. Being able to
write down where a step runs and have it ignored is the failure this whole seam
exists to remove.

The capability-backed integration nodes (`agent`, `tool_call`, `http_request`)
resolve `=` expressions anywhere in their config against the `{ item, items, run }`
scope before use, so their parameters can data-bind directly from upstream output
(e.g. `args: { "channel": "=item.channel" }`). Non-`=` values pass through as
literals.

All 12 node kinds plus the trigger are implemented and dispatched by the engine.
Per-node error handling (`on_error` stop/continue/route, `retry`, an `error`
port) and approval gating (`requires_approval`) are configured through the same
free-form `config`.

`approval` is not the same thing as the `requires_approval` flag. The flag holds
a node back until someone says go; it carries nothing and its answer is invisible
to the graph. The `approval` **kind** is the review itself — it carries what is
being reviewed, and the verdict (approved, who decided, their comment, any edit
they made) comes back as an item on the `approved` / `rejected` ports, readable
anywhere as `=nodes.<id>.decision.approved`. It waits by suspending the run by
default (`wait_mode: "suspend"`), which costs nothing while a card sits in
somebody's queue.

### Per-item fan-out

`agent`, `tool_call`, `http_request`, `memory`, and `sub_workflow` can map over
their input array instead of running once. Three config keys control it, and
they mean the same thing on every one of those kinds:

| Key | Values | Meaning |
|-----|--------|---------|
| `execution` | `once` \| `per_item` | Run once for the whole input array, or once per item. Defaults to `per_item` for `tool_call` / `http_request` / `memory`, and `once` for `agent` / `sub_workflow`. |
| `concurrency` | integer \| `"all"` | How many items run at a time: `1` (default) sequential, `n` at most n in flight, `0` or `"all"` unbounded. Clamped to 64. |
| `on_item_error` | `collect` \| `fail_fast` \| `skip` | What a failing item does to the batch. |

```jsonc
// one agent turn per topic, at most 8 concurrently
{ "id": "research", "kind": "agent", "name": "Research each",
  "config": {
    "execution": "per_item",
    "concurrency": 8,
    "agent_ref": "researcher",
    "prompt": "Research =item.name"
  } }
```

`sub_workflow` in `per_item` mode is the **multiplier**: one complete child run
per item, each seeded with just that item and resolving `workflow_id` against
it. The nesting-depth guard is per child run, so a fan-out widens a run without
deepening it — N siblings at depth d+1, never d+N.

Output items always come back in **input order** with `paired_item` set,
whatever the concurrency, so a fan-out never reorders data.

`on_item_error` defaults to `collect` when the node fans out (`concurrency`
other than `1`) and `fail_fast` when it runs sequentially. That split matters:
`tool_call`, `http_request`, and `memory` are `per_item` *by default*, so
collecting unconditionally would silently disable `on_error`, `retry`, and the
`error` port for the most ordinary nodes in the engine. Under `collect` a failed
item becomes `{ json: { error, failed: true } }` in its own slot, so the node
still emits one output per input and a downstream `condition` can branch on
`=item.json.failed`; under `skip` it is dropped, so the output array may be
shorter than the input.

These keys are rejected at validation time on a node that does not map over its
input — a fan-out knob that silently does nothing is worse than an error.

Each node kind's config keys and ports, along with the available trigger kinds,
are documented in the sections above.
